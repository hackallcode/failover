"""SQLite databases of one service replicated as whole compressed snapshots, one key each in a NATS KV bucket.

The databases are small and change rarely: a snapshot per change costs a few kilobytes, where shipping a log
through an object store lists the whole bucket on every operation. The version of a database is the revision of
its key; the leader writes with the revision it last wrote, so a node that lost the lease cannot overwrite a newer
snapshot.
"""
import asyncio
import logging
import os
import sqlite3
import tempfile
import time
import zlib
from dataclasses import dataclass
from pathlib import Path

from nats.js import JetStreamContext
from nats.js.api import KeyValueConfig
from nats.js.errors import BucketNotFoundError, KeyWrongLastSequenceError, NotFoundError
from nats.js.kv import KeyValue

from failover.domain import Txids

log = logging.getLogger(__name__)

CHECK_EVERY = 1.0
RESTORE_TIMEOUT = 30
BUCKET_MAX_BYTES = 64 * 1024 * 1024
# NATS's default max_payload: a snapshot travels as one message.
SNAPSHOT_MAX_BYTES = 1024 * 1024


@dataclass(frozen=True)
class Database:
    name: str
    path: Path

    @property
    def standby(self) -> Path:
        return self.path.with_name(self.path.name + ".standby")


def version_file(path: Path) -> Path:
    return Path(f"{path}.version")


def read_version(path: Path) -> int | None:
    try:
        return int(version_file(path).read_text())
    except (OSError, ValueError):
        return None


def write_atomically(path: Path, data: bytes) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def install(path: Path, data: bytes, version: int) -> None:
    """The database file, without the journal of the one it replaces, then its version."""
    for suffix in ("-wal", "-shm"):
        Path(f"{path}{suffix}").unlink(missing_ok=True)
    write_atomically(path, data)
    write_atomically(version_file(path), str(version).encode())


def snapshot(path: Path) -> bytes:
    """A consistent copy under a live writer; a read-only connection never checkpoints its journal."""
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as source, sqlite3.connect(":memory:") as copy:
        source.backup(copy)
        return zlib.compress(copy.serialize())


def fingerprint(path: Path) -> tuple:
    stats = []
    for name in (path, Path(f"{path}-wal")):
        try:
            s = name.stat()
            stats.append((s.st_mtime_ns, s.st_size))
        except FileNotFoundError:
            stats.append(None)
    return tuple(stats)


async def bucket(js: JetStreamContext, config: KeyValueConfig) -> KeyValue:
    try:
        return await js.key_value(config.bucket)
    except BucketNotFoundError:
        return await js.create_key_value(config=config)


class SnapshotReplica:
    def __init__(self, js: JetStreamContext, kv: KeyValue, name: str, databases: list[Database]):
        self.js, self.kv, self.name, self.databases = js, kv, name, databases
        self.stream = f"KV_{name}"
        self.uploading: asyncio.Task | None = None
        self.following: asyncio.Task | None = None
        self.unshipped_since: float | None = None
        self.shipped_revisions: dict[str, int] | None = None

    @classmethod
    async def open(cls, js: JetStreamContext, service: str, databases: list[Database], replicas: int):
        name = f"replica-{service}"
        kv = await bucket(js, KeyValueConfig(bucket=name, history=1, replicas=replicas, max_bytes=BUCKET_MAX_BYTES,
                                             max_value_size=SNAPSHOT_MAX_BYTES))
        return cls(js, kv, name, databases)

    async def latest(self, db: Database) -> tuple[bytes | None, int]:
        """From the stream leader: a direct get may come from a lagging replica."""
        try:
            msg = await self.js.get_msg(self.stream, subject=f"$KV.{self.name}.{db.name}", direct=False)
        except NotFoundError:
            return None, 0
        return msg.data or None, msg.seq

    def local(self) -> Txids:
        return {db.name: v for db in self.databases if db.path.exists() and (v := read_version(db.path)) is not None}

    def standby(self) -> Txids:
        return {db.name: v for db in self.databases
                if db.standby.exists() and (v := read_version(db.standby)) is not None}

    async def replicated(self) -> Txids:
        """While this node replicates, what it wrote: a CAS write admits no other writer, and reading the stream
        would fetch every snapshot whole."""
        if self.shipped_revisions is not None and self.uploading is not None and not self.uploading.done():
            return {name: revision for name, revision in self.shipped_revisions.items() if revision}
        txids: Txids = {}
        for db in self.databases:
            _, revision = await self.latest(db)
            if revision:
                txids[db.name] = revision
        return txids

    async def ready(self) -> bool:
        info = await self.js.stream_info(self.stream)
        return info.cluster is None or bool(info.cluster.leader)

    def stall_seconds(self) -> float:
        return time.monotonic() - self.unshipped_since if self.unshipped_since is not None else 0.0

    async def restore(self, required: set[str]) -> Txids | None:
        restored: Txids = {}
        try:
            for db in self.databases:
                data, revision = await asyncio.wait_for(self.latest(db), RESTORE_TIMEOUT)
                if data is None:
                    if db.name in required:
                        return None
                    continue
                await asyncio.to_thread(install, db.path, zlib.decompress(data), revision)
                restored[db.name] = revision
        except Exception as e:
            log.warning("restore: %r", e)
            return None
        return restored

    async def promote_standby(self, required: set[str]) -> None:
        missing = [db.name for db in self.databases if db.name in required and not db.standby.exists()]
        if missing:
            raise FileNotFoundError(f"no standby copy of {missing}")
        for db in self.databases:
            if not db.standby.exists():
                continue
            for suffix in ("-wal", "-shm"):
                Path(f"{db.path}{suffix}").unlink(missing_ok=True)
            os.replace(version_file(db.standby), version_file(db.path))
            os.replace(db.standby, db.path)

    async def follow(self) -> None:
        if self.following is None or self.following.done():
            self.following = asyncio.create_task(self.keep_standby())

    async def keep_standby(self) -> None:
        """The bucket pushes each new snapshot: a standby never polls."""
        paths = {db.name: db.standby for db in self.databases}
        watcher = await self.kv.watch(">")
        try:
            async for entry in watcher:
                if entry is None or entry.key not in paths or not entry.value:
                    continue
                await asyncio.to_thread(install, paths[entry.key], zlib.decompress(entry.value), entry.revision)
        finally:
            await watcher.stop()

    async def stop_following(self) -> None:
        await cancel(self.following)
        self.following = None

    async def replicate(self) -> None:
        if self.uploading is None or self.uploading.done():
            self.uploading = asyncio.create_task(self.ship_changes())

    async def ship_changes(self) -> None:
        shipped: dict[str, tuple] = {}
        self.shipped_revisions = None
        while True:
            try:
                if self.shipped_revisions is None:
                    self.shipped_revisions = {db.name: (await self.latest(db))[1] for db in self.databases}
                await self.ship_once(shipped, self.shipped_revisions)
            except KeyWrongLastSequenceError:
                raise
            except Exception as e:
                log.warning("ship: %r", e)
            await asyncio.sleep(CHECK_EVERY)

    async def ship_once(self, shipped: dict[str, tuple], revisions: dict[str, int]) -> None:
        pending = False
        for db in self.databases:
            if not db.path.exists():
                continue
            mark = fingerprint(db.path)
            if shipped.get(db.name) == mark:
                continue
            pending = True
            try:
                data = await asyncio.to_thread(snapshot, db.path)
                if len(data) > SNAPSHOT_MAX_BYTES:
                    raise ValueError(f"{db.name}: a {len(data)}-byte snapshot exceeds {SNAPSHOT_MAX_BYTES}")
                revision = await self.kv.update(db.name, data, last=revisions[db.name])
            except KeyWrongLastSequenceError:
                log.error("%s: another node wrote a newer snapshot; this one stops shipping", db.name)
                raise
            except Exception as e:
                log.warning("ship %s: %r", db.name, e)
                continue
            revisions[db.name], shipped[db.name] = revision, mark
            await asyncio.to_thread(write_atomically, version_file(db.path), str(revision).encode())
        unshipped = any(db.path.exists() and shipped.get(db.name) != fingerprint(db.path) for db in self.databases)
        if not unshipped:
            self.unshipped_since = None
        elif pending and self.unshipped_since is None:
            self.unshipped_since = time.monotonic()

    async def stop_replicating(self, graceful: bool) -> None:
        await cancel(self.uploading)
        self.uploading = None
        if graceful:
            revisions = {db.name: (await self.latest(db))[1] for db in self.databases}
            await self.ship_once({}, revisions)


async def cancel(task: asyncio.Task | None) -> None:
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
