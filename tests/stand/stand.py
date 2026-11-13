"""Drive the stand (compose.yaml here) and judge it by the writer's log, not by what the agents say."""
import json
import sqlite3
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).parent
NODES = ("a", "b")
PEER = {"a": "b", "b": "a"}
DELAY = ("60ms", "20ms")  # per hop, so a round trip is about the tunnel between the servers


def compose(*args: str, check: bool = True, timeout: float | None = None) -> str:
    return subprocess.run(["docker", "compose", *args], cwd=HERE, check=check, capture_output=True, text=True,
                          timeout=timeout).stdout


def net_container(node: str) -> str:
    return "net-c" if node == "c" else f"node-{node}"


def up() -> None:
    compose("down", "-v", check=False)
    compose("up", "-d")
    netem()


def down() -> None:
    compose("down", "-v", check=False)


def netem() -> None:
    for node in ("a", "b", "c"):
        compose("exec", "-T", net_container(node), "tc", "qdisc", "replace", "dev", "eth0", "root", "netem",
                "delay", *DELAY, check=False)


def isolate(node: str, on: bool) -> None:
    rules = "iptables -I INPUT -i eth0 -j DROP; iptables -I OUTPUT -o eth0 -j DROP" if on else "iptables -F"
    compose("exec", "-T", net_container(node), "sh", "-c", rules, check=False)


def kill(node: str) -> None:
    """The whole server: its NATS and, sharing its network, its agent."""
    compose("kill", f"nats-{node}", net_container(node))


def start(node: str) -> None:
    compose("start", f"nats-{node}", net_container(node), check=False)
    netem()


def pause(node: str, on: bool) -> None:
    """A hung agent: its process stops, the network and NATS stay."""
    compose("pause" if on else "unpause", f"node-{node}", check=False)


def heal() -> None:
    for node in NODES:
        pause(node, False)
    for node in ("a", "b", "c"):
        start(node)
        isolate(node, False)


def request(node: str, check: bool = True) -> None:
    """Ask `node` to lead, from that node; a fault test may ask a node that cannot reach the cluster."""
    try:
        compose("exec", "-T", f"node-{node}", "failover", "request", f"/config/{node}.json", node, check=check,
                timeout=20)
    except subprocess.TimeoutExpired:
        if check:
            raise


@dataclass(frozen=True)
class Write:
    ts: float
    seq: int
    node: str
    run: str


def writes() -> list[Write]:
    rows = []
    for node in NODES:
        with tempfile.NamedTemporaryFile() as f:
            if subprocess.run(["docker", "compose", "cp", f"node-{node}:/data/writes.log", f.name], cwd=HERE,
                              capture_output=True).returncode:
                continue
            for line in Path(f.name).read_text().splitlines():
                ts, seq, n, run = line.split()
                rows.append(Write(float(ts), int(seq), n, run))
    return sorted(rows, key=lambda w: w.ts)


@dataclass(frozen=True)
class Stint:
    """One uninterrupted run of the service on one node."""

    node: str
    run: str
    first: Write
    last: Write
    seqs: frozenset


def stints(rows: list[Write]) -> list[Stint]:
    by_run: dict[str, list[Write]] = {}
    for w in rows:
        by_run.setdefault(w.run, []).append(w)
    return sorted((Stint(ws[0].node, run, ws[0], ws[-1], frozenset(w.seq for w in ws)) for run, ws in by_run.items()),
                  key=lambda s: s.first.ts)


@dataclass(frozen=True)
class Handover:
    before: Stint
    after: Stint

    @property
    def overlap(self) -> float:
        return self.before.last.ts - self.after.first.ts

    def concurrent(self, rows: list[Write]) -> list[Write]:
        """Rows the earlier stint wrote after the later one had started."""
        return [w for w in rows if w.run == self.before.run and w.ts >= self.after.first.ts]

    @property
    def gap(self) -> float:
        return self.after.first.ts - self.before.last.ts

    def lost(self, rows: list[Write]) -> list[Write]:
        """Rows committed before the later stint, by any earlier stint, that it does not start after."""
        return [w for w in rows if w.ts < self.after.first.ts and w.seq >= self.after.first.seq]

    def lost_seconds(self, rows: list[Write]) -> float:
        """How long before the last leader stopped the oldest lost row was committed: everything older must
        have reached the replica."""
        lost = self.lost(rows)
        return self.before.last.ts - min(w.ts for w in lost) if lost else 0.0


def handovers(rows: list[Write]) -> list[Handover]:
    runs = stints(rows)
    return [Handover(a, b) for a, b in zip(runs, runs[1:])]


def serving(window: float = 2.0) -> str | None:
    """The node whose writer committed in the last seconds, if exactly one did."""
    now = time.time()
    recent = {w.node for w in writes() if now - w.ts < window}
    return recent.pop() if len(recent) == 1 else None


def wait_serving(timeout: float, other_than: str | None = None) -> str | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        node = serving()
        if node and node != other_than:
            return node
        time.sleep(1)
    return None


def stream_leaders(via: str) -> set[str]:
    script = ("import json, urllib.request\n"
              "d = json.load(urllib.request.urlopen('http://127.0.0.1:8222/jsz?accounts=true&streams=true'))\n"
              "print(json.dumps([s.get('cluster', {}).get('leader') for a in d['account_details']"
              " for s in a['stream_detail']]))")
    return set(json.loads(compose("exec", "-T", net_container(via), "python3", "-c", script)))


def wait_streams_led_by(node: str, timeout: float) -> bool:
    """The leader pulls the stream leaders to its server; until then a test is not starting from the usual state."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if stream_leaders(via=node) == {node}:
            return True
        time.sleep(1)
    return False


def state(node: str) -> dict:
    return json.loads(compose("exec", "-T", f"node-{node}", "failover", "state", f"/config/{node}.json"))


FREEZE_GAP = 2.0  # the writer commits every 0.1s; a longer silence inside a stint is a frozen node


def effective_end(stint_rows: list[Write]) -> float:
    """When a stint really stopped: the last write before its first freeze, or its last write. A frozen node
    that wakes up writes a short burst before it fences; that burst counts as written after the end."""
    for a, b in zip(stint_rows, stint_rows[1:]):
        if b.ts - a.ts > FREEZE_GAP:
            return a.ts
    return stint_rows[-1].ts


def final_rows(node: str) -> set[tuple[int, str]]:
    """(seq, run) of every row the database on `node` holds now, WAL included."""
    with tempfile.TemporaryDirectory() as tmp:
        for suffix in ("", "-wal"):
            subprocess.run(["docker", "compose", "cp", f"node-{node}:/data/app.db{suffix}", f"{tmp}/app.db{suffix}"],
                           cwd=HERE, capture_output=True)
        conn = sqlite3.connect(f"{tmp}/app.db")
        try:
            return set(conn.execute("SELECT seq, run FROM log"))
        finally:
            conn.close()


def unexpected_losses(rows: list[Write], leader: str, allowance: float) -> list[Write]:
    """Rows missing from the leader's database that were committed more than `allowance` before their stint
    stopped: everything older must have reached the replica."""
    kept = final_rows(leader)
    by_run: dict[str, list[Write]] = {}
    for w in rows:
        by_run.setdefault(w.run, []).append(w)
    return [w for run, ws in by_run.items() for w in ws
            if (w.seq, w.run) not in kept and w.ts < effective_end(ws) - allowance]


WAKE_BURST = 1.0  # how long a node that wakes up from a freeze may write before its agent stops it


def split_brain(rows: list[Write]) -> list[str]:
    """Rows written while another node already served, except the burst right after a freeze."""
    problems = []
    for h in handovers(rows):
        both = h.concurrent(rows)
        if not both:
            continue
        stint = [w for w in rows if w.run == h.before.run]
        woke = next((b.ts for a, b in zip(stint, stint[1:]) if b.ts - a.ts > FREEZE_GAP), None)
        if woke is not None and all(woke <= w.ts <= woke + WAKE_BURST for w in both):
            continue
        problems.append(f"{h.before.node} wrote {len(both)} rows after {h.after.node} started, "
                        f"{both[0].ts - h.after.first.ts:.2f}s..{both[-1].ts - h.after.first.ts:.2f}s after it")
    return problems
