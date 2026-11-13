"""SQLite databases of one service replicated through the NATS KV bucket as a base image and the pages changed since.

Each database has two keys: `<db>` holds a compressed image, `<db>.delta` the pages that differ from that image now,
so a change ships only its pages and a reader needs no history. Once the pages outweigh a share of the image, the
leader writes a new image. The version of a database is the revision of the last of its writes; every write compares
the key's revision, so a node that lost the lease cannot overwrite a newer one.
"""
import asyncio
import logging
import os
import sqlite3
import struct
import tempfile
import time
import zlib
from dataclasses import dataclass
from pathlib import Path

from nats.js import JetStreamContext
from nats.js.errors import KeyWrongLastSequenceError
from nats.js.kv import KeyValue

from failover.buckets import exact, stream
from failover.domain import Txids

log = logging.getLogger(__name__)

CHECK_EVERY = 1.0
RESTORE_TIMEOUT = 30
# NATS's default max_payload: an image or a delta travels as one message.
SNAPSHOT_MAX_BYTES = 1024 * 1024
# A delta past this share of its compressed image, or over an image this old, is replaced by a new image: a whole
# image now and then resyncs whatever the deltas missed.
REBASE_SHARE = 0.25
REBASE_EVERY = 24 * 3600
DELTA_HEADER = struct.Struct(">QII")
PAGE_NUMBER = struct.Struct(">I")


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
    """A consistent image under a live writer; a read-only connection never checkpoints its journal."""
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as source, sqlite3.connect(":memory:") as copy:
        source.backup(copy)
        return copy.serialize()


def page_size(image: bytes) -> int:
    size = int.from_bytes(image[16:18], "big")
    return 65536 if size == 1 else size


def diff(base: bytes, image: bytes, base_revision: int) -> bytes | None:
    """The pages of `image` that differ from `base`, or None when the page size changed."""
    size = page_size(image)
    if page_size(base) != size:
        return None
    parts = [DELTA_HEADER.pack(base_revision, size, len(image) // size)]
    for offset in range(0, len(image), size):
        page = image[offset:offset + size]
        if page != base[offset:offset + size]:
            parts += [PAGE_NUMBER.pack(offset // size), page]
    return zlib.compress(b"".join(parts))


def delta_base(delta: bytes) -> int:
    return DELTA_HEADER.unpack_from(zlib.decompress(delta))[0]


def apply(base: bytes, delta: bytes) -> bytes:
    raw = zlib.decompress(delta)
    _, size, pages = DELTA_HEADER.unpack_from(raw)
    image = bytearray(base[:pages * size].ljust(pages * size, b"\0"))
    for offset in range(DELTA_HEADER.size, len(raw), PAGE_NUMBER.size + size):
        number = PAGE_NUMBER.unpack_from(raw, offset)[0]
        start = offset + PAGE_NUMBER.size
        image[number * size:(number + 1) * size] = raw[start:start + size]
    return bytes(image)


@dataclass
class Base:
    """The image readers apply deltas to, as this node last read or wrote it."""
    image: bytes
    revision: int
    compressed: int
    since: float


def fingerprint(path: Path) -> tuple:
    stats = []
    for name in (path, Path(f"{path}-wal")):
        try:
            s = name.stat()
            stats.append((s.st_mtime_ns, s.st_size))
        except FileNotFoundError:
            stats.append(None)
    return tuple(stats)


class SnapshotReplica:
    def __init__(self, js: JetStreamContext, kv: KeyValue, bucket: str, service: str, databases: list[Database]):
        self.js, self.kv, self.bucket, self.databases = js, kv, bucket, databases
        self.prefix = f"{service}.snapshot."
        self.stream = stream(bucket)
        self.uploading: asyncio.Task | None = None
        self.following: asyncio.Task | None = None
        self.installing: asyncio.Task | None = None
        self.unshipped_since: float | None = None
        self.shipped_revisions: dict[str, int] | None = None
        self.bases: dict[str, Base | None] = {}
        self.image_revisions: dict[str, int] = {}
        self.delta_revisions: dict[str, int] = {}

    def key(self, db: Database) -> str:
        return f"{self.prefix}{db.name}"

    def delta_key(self, db: Database) -> str:
        return f"{self.prefix}{db.name}.delta"

    async def latest(self, db: Database) -> tuple[bytes | None, int]:
        return await exact(self.js, self.bucket, self.key(db))

    async def latest_delta(self, db: Database) -> tuple[bytes | None, int]:
        return await exact(self.js, self.bucket, self.delta_key(db))

    async def stored(self, db: Database) -> tuple[bytes | None, int, int]:
        """The image, the version it reaches with its delta, and the delta key's revision."""
        image, revision = await self.latest(db)
        delta, delta_revision = await self.latest_delta(db)
        if image is None:
            return None, 0, delta_revision
        image = zlib.decompress(image)
        if delta and delta_base(delta) == revision:
            return apply(image, delta), delta_revision, delta_revision
        return image, revision, delta_revision

    def local(self) -> Txids:
        return {db.name: v for db in self.databases if db.path.exists() and (v := read_version(db.path)) is not None}

    def standby(self) -> Txids:
        return {db.name: v for db in self.databases
                if db.standby.exists() and (v := read_version(db.standby)) is not None}

    async def replicated(self) -> Txids:
        """While this node replicates, what it wrote: a CAS write admits no other writer, and reading the stream
        would fetch every image whole."""
        if self.shipped_revisions is not None and self.uploading is not None and not self.uploading.done():
            return {name: revision for name, revision in self.shipped_revisions.items() if revision}
        txids: Txids = {}
        for db in self.databases:
            _, version, _ = await self.stored(db)
            if version:
                txids[db.name] = version
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
                image, version, _ = await self.stored(db)
                if image is None:
                    if db.name in required:
                        return None
                    continue
                await asyncio.to_thread(install, db.path, image, version)
                restored[db.name] = version
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
            os.replace(db.standby, db.path)
            os.replace(version_file(db.standby), version_file(db.path))

    async def follow(self) -> None:
        if self.following is None or self.following.done():
            self.following = asyncio.create_task(self.keep_standby())

    async def keep_standby(self) -> None:
        """The bucket pushes each image and delta: a standby never polls. An image arrives before its deltas, and a
        delta written over another image is skipped."""
        images = {self.key(db): db for db in self.databases}
        deltas = {self.delta_key(db): db for db in self.databases}
        bases: dict[str, tuple[bytes, int]] = {}
        watcher = await self.kv.watch(f"{self.prefix}>")
        try:
            async for entry in watcher:
                if entry is None or not entry.value:
                    continue
                if entry.key in images:
                    db = images[entry.key]
                    image = zlib.decompress(entry.value)
                    bases[db.name] = (image, entry.revision)
                elif entry.key in deltas:
                    db = deltas[entry.key]
                    base = bases.get(db.name)
                    if base is None or delta_base(entry.value) != base[1]:
                        continue
                    image = apply(base[0], entry.value)
                else:
                    continue
                self.installing = asyncio.create_task(asyncio.to_thread(install, db.standby, image, entry.revision))
                await asyncio.shield(self.installing)
        finally:
            await watcher.stop()

    async def stop_following(self) -> None:
        """A thread cannot be cancelled: the copy it is writing lands before the standby files are touched."""
        await cancel(self.following)
        self.following = None
        if self.installing is not None:
            await asyncio.wait([self.installing])
            self.installing = None

    async def replicate(self) -> None:
        if self.uploading is None or self.uploading.done():
            self.uploading = asyncio.create_task(self.ship_changes())

    async def load(self) -> None:
        """What the bucket holds, which this node's writes then build on."""
        self.shipped_revisions = {}
        for db in self.databases:
            image, revision = await self.latest(db)
            self.image_revisions[db.name] = revision
            delta, self.delta_revisions[db.name] = await self.latest_delta(db)
            self.bases[db.name] = (Base(zlib.decompress(image), revision, len(image), time.monotonic())
                                   if image else None)
            version = self.delta_revisions[db.name] if image and delta and delta_base(delta) == revision else revision
            self.shipped_revisions[db.name] = version

    async def ship_changes(self) -> None:
        shipped: dict[str, tuple] = {}
        self.shipped_revisions = None
        while True:
            try:
                if self.shipped_revisions is None:
                    await self.load()
                await self.ship_once(shipped)
            except KeyWrongLastSequenceError:
                raise
            except Exception as e:
                log.warning("ship: %r", e)
            await asyncio.sleep(CHECK_EVERY)

    async def ship_once(self, shipped: dict[str, tuple], strict: bool = False) -> None:
        pending = False
        for db in self.databases:
            if not db.path.exists():
                continue
            mark = fingerprint(db.path)
            if shipped.get(db.name) == mark:
                continue
            pending = True
            try:
                image = await asyncio.to_thread(snapshot, db.path)
                revision = await self.ship(db, image)
            except KeyWrongLastSequenceError:
                log.error("%s: another node wrote a newer snapshot; this one stops shipping", db.name)
                raise
            except Exception as e:
                if strict:
                    raise
                log.warning("ship %s: %r", db.name, e)
                continue
            self.shipped_revisions[db.name], shipped[db.name] = revision, mark
            await asyncio.to_thread(write_atomically, version_file(db.path), str(revision).encode())
        unshipped = any(db.path.exists() and shipped.get(db.name) != fingerprint(db.path) for db in self.databases)
        if not unshipped:
            self.unshipped_since = None
        elif pending and self.unshipped_since is None:
            self.unshipped_since = time.monotonic()

    async def ship(self, db: Database, image: bytes) -> int:
        """The changed pages while they stay a small share of the image, else a new image."""
        base = self.bases.get(db.name)
        delta = await asyncio.to_thread(diff, base.image, image, base.revision) if base else None
        if (delta is not None and len(delta) <= base.compressed * REBASE_SHARE
                and time.monotonic() - base.since < REBASE_EVERY):
            revision = await self.write(self.delta_key(db), delta, self.delta_revisions[db.name], base.revision)
            self.delta_revisions[db.name] = revision
            return revision
        compressed = await asyncio.to_thread(zlib.compress, image)
        if len(compressed) > SNAPSHOT_MAX_BYTES:
            raise ValueError(f"{db.name}: a {len(compressed)}-byte image exceeds {SNAPSHOT_MAX_BYTES}")
        revision = await self.write(self.key(db), compressed, self.image_revisions[db.name])
        self.bases[db.name] = Base(image, revision, len(compressed), time.monotonic())
        self.image_revisions[db.name] = revision
        return revision

    async def write(self, key: str, data: bytes, last: int, base_revision: int | None = None) -> int:
        """A write whose reply was lost has committed all the same: finding its own bytes there, the writer goes on.
        A delta over another image, left by a leader that lost the lease, gives way: no reader applies it."""
        try:
            return await self.kv.update(key, data, last=last)
        except KeyWrongLastSequenceError:
            stored, revision = await exact(self.js, self.bucket, key)
            if stored == data:
                return revision
            if base_revision is not None and stored and delta_base(stored) != base_revision:
                return await self.kv.update(key, data, last=revision)
            raise

    async def stop_replicating(self, graceful: bool) -> None:
        """Handing over, every change has to reach the bucket: a failure keeps the lease for another try."""
        await cancel(self.uploading)
        self.uploading = None
        if graceful:
            if self.shipped_revisions is None:
                await self.load()
            await self.ship_once({}, strict=True)


async def cancel(task: asyncio.Task | None) -> None:
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
