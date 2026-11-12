"""In-memory stand-ins with the contracts' semantics: revisions grow, writes compare revisions."""
from failover.agent import LeaseLost, RevisionMoved
from failover.domain import Desired, Position, Txids


class Cluster:
    """What the NATS KV buckets hold, shared by the nodes' elections."""

    def __init__(self):
        self.seq = 0
        self.lease: tuple[str, int] | None = None
        self.desired: Desired | None = None
        self.position: tuple[Position, int] | None = None
        self.history: list[str] = []
        self.down = False

    def next(self) -> int:
        self.seq += 1
        return self.seq


class Election:
    def __init__(self, cluster: Cluster, node: str):
        self.c, self.node = cluster, node

    def check(self):
        if self.c.down:
            raise TimeoutError("no quorum")

    async def acquire(self):
        self.check()
        if self.c.lease is not None:
            return None
        self.c.lease = (self.node, self.c.next())
        return self.c.lease[1]

    async def renew(self, revision):
        self.check()
        if self.c.lease is None or self.c.lease[0] != self.node:
            raise LeaseLost()
        if self.c.lease[1] != revision:
            raise RevisionMoved(self.c.lease[1])
        self.c.lease = (self.node, self.c.next())
        return self.c.lease[1]

    async def release(self, revision):
        self.check()
        if self.c.lease == (self.node, revision):
            self.c.lease = None

    async def release_mine(self):
        self.check()
        if self.c.lease and self.c.lease[0] == self.node:
            self.c.lease = None

    async def holder(self):
        self.check()
        return self.c.lease[0] if self.c.lease else None

    async def desired(self):
        self.check()
        return self.c.desired

    async def clear_desired(self, desired):
        if self.c.desired == desired:
            self.c.desired = None

    async def position(self):
        self.check()
        return (self.c.position[0], self.c.position[1]) if self.c.position else (None, 0)

    async def record_position(self, position, after):
        self.check()
        current = self.c.position[1] if self.c.position else 0
        if current != after:
            raise LeaseLost()
        self.c.position = (position, self.c.next())
        return self.c.position[1]

    async def record(self, event):
        self.c.history.append(f"{self.node}: {event}")


class Replica:
    def __init__(self, store: dict[str, Txids]):
        self.store = store  # the replica: shared by the nodes
        self.local_txids: Txids = {}
        self.standby_txids: Txids = {}
        self.replicating = self.following = False
        self.restores = self.promotions = 0
        self.short = False
        self.unreadable = False
        self.leaderless = False
        self.stuck = False
        self.replication_restarts = 0

    def local(self):
        return dict(self.local_txids)

    def standby(self):
        return dict(self.standby_txids)

    async def replicated(self):
        return dict(self.store.get("bucket", {}))

    async def restore(self, required):
        self.restores += 1
        bucket = self.store.get("bucket", {})
        if self.unreadable or not set(required) <= set(bucket):
            return None
        self.local_txids = {} if self.short else dict(bucket)
        return dict(self.local_txids)

    async def promote_standby(self, required):
        if not set(required) <= set(self.standby_txids):
            raise FileNotFoundError(f"no standby copy of {set(required) - set(self.standby_txids)}")
        self.promotions += 1
        self.local_txids = {**self.local_txids, **self.standby_txids}

    def stall_seconds(self):
        return 60.0 if self.stuck and self.replicating else 0.0

    async def ready(self):
        return not self.leaderless

    async def follow(self):
        self.following = True

    async def stop_following(self):
        self.following = False

    async def replicate(self):
        if self.stuck:
            self.replication_restarts += 1
            self.stuck = False
        self.replicating = True

    async def stop_replicating(self, graceful):
        self.replicating = False

    def write(self, db, txid):
        """The service commits and replication ships it."""
        self.local_txids[db] = txid
        if self.replicating:
            self.store.setdefault("bucket", {})[db] = txid


class Service:
    def __init__(self):
        self.up = False
        self.partial = False  # some of its parts stopped
        self.starts = 0
        self.stop_fails = False

    async def start(self):
        self.up, self.partial = True, False
        self.starts += 1

    async def stop(self):
        if self.stop_fails:
            raise RuntimeError("compose stop failed")
        self.up = self.partial = False

    async def running(self):
        return self.up

    async def complete(self):
        return self.up and not self.partial


class Dns:
    def __init__(self, record: dict, node: str):
        self.record, self.node = record, node

    async def point_here(self):
        self.record["name"] = self.node


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now
