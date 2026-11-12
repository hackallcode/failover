"""SQLite databases of one service replicated by Litestream, each into its own JetStream object store bucket
(Litestream's NATS replica has no path inside a bucket)."""
import asyncio
import base64
import json
import logging
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from nats.js import JetStreamContext
from nats.js.api import ObjectStoreConfig
from nats.js.errors import BucketNotFoundError

from failover.domain import Txids

log = logging.getLogger(__name__)

RESTORE_TIMEOUT = 120
STOP_TIMEOUT = 10
REPLICA_MAX_BYTES = 256 * 1024 * 1024
SNAPSHOTS = {"interval": "1h", "retention": "2h"}


@dataclass(frozen=True)
class Database:
    name: str
    path: Path

    def bucket(self, service: str) -> str:
        return f"replica-{service}-{self.name}"

    @property
    def standby(self) -> Path:
        return self.path.with_name(self.path.name + ".standby")

    @property
    def meta(self) -> Path:
        return self.path.with_name(f".{self.path.name}-litestream")


def parse_txid(text: str) -> int:
    return int(text.strip(), 16)


def newest_txid(names: list[str]) -> int | None:
    txids = [parse_txid(n.rsplit("-", 1)[1].removesuffix(".ltx")) for n in names
             if n.startswith("ltx/") and n.endswith(".ltx") and "-" in n]
    return max(txids, default=None)


def remove_database(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        Path(f"{path}{suffix}").unlink(missing_ok=True)


class Litestream:
    def __init__(self, binary: str, config: Path, databases: list[Database], js: JetStreamContext, service: str,
                 follow_interval: float = 10):
        self.binary, self.config, self.databases, self.js, self.service = binary, config, databases, js, service
        self.follow_interval = follow_interval
        self.replicating: asyncio.subprocess.Process | None = None
        self.following: dict[str, asyncio.subprocess.Process] = {}
        self.follow_started: dict[str, float] = {}
        self.progress: dict[str, tuple[int, int, float | None]] = {}

    @classmethod
    async def open(cls, js: JetStreamContext, binary: str, config: Path, databases: list[Database], service: str,
                   nats_url: str, replicas: int, credentials: dict | None = None, follow_interval: float = 10):
        for db in databases:
            name = db.bucket(service)
            try:
                await js.object_store(name)
            except BucketNotFoundError:
                await js.create_object_store(name, ObjectStoreConfig(bucket=name, replicas=replicas,
                                                                     max_bytes=REPLICA_MAX_BYTES))
                continue
            info = await js.stream_info(f"OBJ_{name}")
            if info.config.max_bytes != REPLICA_MAX_BYTES:
                info.config.max_bytes = REPLICA_MAX_BYTES
                await js.update_stream(info.config)
        write_config(config, databases, service, nats_url, credentials or {})
        return cls(binary, config, databases, js, service, follow_interval)

    def command(self, subcommand: str, *args: str) -> list[str]:
        """Flags go before the database path: Litestream stops reading flags at the first argument."""
        return [self.binary, subcommand, "-config", str(self.config), *args]

    async def run(self, *args: str) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(*self.command(*args), stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE)
        try:
            out, err = await asyncio.wait_for(proc.communicate(), RESTORE_TIMEOUT)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            return -1, ""
        if proc.returncode:
            log.warning("litestream %s: %s", args[0], err.decode().strip()[-500:])
        return proc.returncode, out.decode()

    def local(self) -> Txids:
        txids: Txids = {}
        for db in self.databases:
            level0 = db.meta / "ltx" / "0"
            if not db.path.exists() or not level0.is_dir():
                continue
            names = [f.name for f in level0.iterdir() if f.name.endswith(".ltx")]
            if names:
                txids[db.name] = max(parse_txid(n.removesuffix(".ltx").rsplit("-", 1)[1]) for n in names)
        return txids

    def stall_seconds(self) -> float:
        """Litestream stuck on NATS writes no LTX while its read lock keeps the WAL from being checkpointed."""
        local, now, stall = self.local(), time.monotonic(), 0.0
        for db in self.databases:
            wal = Path(f"{db.path}-wal")
            size = wal.stat().st_size if wal.exists() else 0
            txid = local.get(db.name, 0)
            seen = self.progress.get(db.name)
            if seen is None or seen[0] != txid or size < seen[1]:
                self.progress[db.name] = (txid, size, None)
            elif size > seen[1]:
                grew_at = seen[2] if seen[2] is not None else now
                self.progress[db.name] = (txid, seen[1], grew_at)
                stall = max(stall, now - grew_at)
        return stall

    def standby(self) -> Txids:
        txids: Txids = {}
        for db in self.databases:
            try:
                txids[db.name] = parse_txid(Path(f"{db.standby}-txid").read_text())
            except (OSError, ValueError):
                pass
        return txids

    async def replicated(self) -> Txids:
        """From the stream's subjects: listing the objects decodes each in Python and stalls the loop."""
        txids: Txids = {}
        for db in self.databases:
            bucket = db.bucket(self.service)
            prefix = f"$O.{bucket}.M."
            info = await self.js.stream_info(f"OBJ_{bucket}", subjects_filter=f"{prefix}>")
            names = [base64.urlsafe_b64decode(subject[len(prefix):] + "==").decode()
                     for subject in (info.state.subjects or {})]
            txid = newest_txid(names)
            if txid is not None:
                txids[db.name] = txid
        return txids

    async def ready(self) -> bool:
        for db in self.databases:
            info = await self.js.stream_info(f"OBJ_{db.bucket(self.service)}")
            if info.cluster is not None and not info.cluster.leader:
                return False
        return True

    async def restore(self, required: set[str]) -> Txids | None:
        restored: Txids = {}
        for db in self.databases:
            target = db.path.with_name(db.path.name + ".restore")
            remove_database(target)
            args = ["restore", "-json", "-o", str(target)]
            if db.name not in required:
                args.append("-if-replica-exists")
            code, out = await self.run(*args, str(db.path))
            if code != 0:
                return None
            if not target.exists():
                continue
            remove_database(db.path)
            shutil.rmtree(db.meta, ignore_errors=True)
            os.replace(target, db.path)
            restored[db.name] = parse_txid(json.loads(out)["txid"])
        return restored

    async def promote_standby(self, required: set[str]) -> None:
        missing = [db.name for db in self.databases if db.name in required and not db.standby.exists()]
        if missing:
            raise FileNotFoundError(f"no standby copy of {missing}")
        for db in self.databases:
            if not db.standby.exists():
                continue
            remove_database(db.path)
            shutil.rmtree(db.meta, ignore_errors=True)
            os.replace(db.standby, db.path)
            Path(f"{db.standby}-txid").unlink(missing_ok=True)

    async def follow(self) -> None:
        for db in self.databases:
            proc = self.following.get(db.name)
            if proc is not None and proc.returncode is None:
                continue
            if time.monotonic() - self.follow_started.get(db.name, float("-inf")) < self.follow_interval:
                continue
            if proc is not None:
                # It may not resume from what it left.
                remove_database(db.standby)
                Path(f"{db.standby}-txid").unlink(missing_ok=True)
            # Each poll lists every level of the bucket.
            self.follow_started[db.name] = time.monotonic()
            self.following[db.name] = await asyncio.create_subprocess_exec(*self.command(
                "restore", "-f", "-follow-interval", f"{self.follow_interval:g}s", "-if-replica-exists",
                "-o", str(db.standby), str(db.path)))

    async def stop_following(self) -> None:
        for proc in self.following.values():
            await stop(proc, graceful=False)
        self.following.clear()

    async def replicate(self) -> None:
        if self.replicating is None or self.replicating.returncode is not None:
            self.progress.clear()
            self.replicating = await asyncio.create_subprocess_exec(*self.command("replicate"))

    async def stop_replicating(self, graceful: bool) -> None:
        if self.replicating is not None:
            await stop(self.replicating, graceful)
            self.replicating = None


async def stop(proc: asyncio.subprocess.Process, graceful: bool) -> None:
    if proc.returncode is not None:
        return
    if graceful:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), STOP_TIMEOUT)
            return
        except TimeoutError:
            log.warning("litestream did not stop in %ss", STOP_TIMEOUT)
    proc.kill()
    await proc.wait()


def write_config(path: Path, databases: list[Database], service: str, url: str, credentials: dict) -> None:
    config = {"snapshot": SNAPSHOTS,
              "dbs": [{"path": str(db.path),
                       "replica": {"type": "nats", "url": f"{url}/{db.bucket(service)}", **credentials}}
                      for db in databases]}
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(config, indent=2))  # YAML is a superset of JSON
    os.replace(tmp, path)
