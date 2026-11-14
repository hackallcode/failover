"""The election and the shared state of one service in NATS JetStream KV.

Everything lives in one bucket: each replicated stream is another raft group heartbeating between the servers.
The lease is a key whose every write expires after the lease TTL: a holder that stops renewing loses it. Every
write compares the key's revision, so only the holder can renew it, and the term is the revision that created it.
A released or expired lease is an empty value or no key; both are free.
"""
import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass

from nats.js import JetStreamContext
from nats.js.api import KeyValueConfig
from nats.js.errors import APIError, KeyNotFoundError, KeyWrongLastSequenceError
from nats.js.kv import KeyValue

from failover.agent import LeaseLost, RevisionMoved
from failover.buckets import bucket, exact, stream
from failover.domain import Desired, Position

HISTORY_LENGTH = 50
WRONG_LAST_SEQUENCE = (10071, 10164)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Pushed:
    """A key's latest value as the bucket pushed it, and when it arrived."""
    value: bytes | None
    at: float


class NatsElection:
    BUCKET = "failover"

    def __init__(self, js: JetStreamContext, kv: KeyValue, service: str, node: str, lease_ttl: float | None = None):
        self.js, self.kv, self.node, self.lease_ttl = js, kv, node, lease_ttl
        self.pushed: dict[str, Pushed] = {}
        self.watching: asyncio.Task | None = None
        self.k_lease, self.k_desired, self.k_position, self.k_history = (
            f"{service}.lease", f"{service}.desired", f"{service}.position", f"{service}.history")

    @classmethod
    async def open(cls, js: JetStreamContext, service: str, node: str, lease_ttl: float, replicas: int):
        kv = await bucket(js, KeyValueConfig(bucket=cls.BUCKET, history=1, replicas=replicas, direct=True))
        election = cls(js, kv, service, node, lease_ttl)
        election.watch()
        return election

    def watch(self) -> None:
        """The lease and the switch request pushed as they change: any replica answers a read, so reading them every
        tick would cross a route each time."""
        self.pushed = {}
        self.watching = asyncio.create_task(self.keep_watching())

    async def keep_watching(self) -> None:
        await asyncio.gather(self.follow_key(self.k_lease), self.follow_key(self.k_desired))

    async def follow_key(self, key: str) -> None:
        watcher = await self.kv.watch(key)
        try:
            async for entry in watcher:
                if entry is not None:
                    self.pushed[key] = Pushed(entry.value, time.monotonic())
                elif key not in self.pushed:  # the marker after the current values: the key has none
                    self.pushed[key] = Pushed(None, time.monotonic())
        finally:
            await watcher.stop()

    def latest(self, key: str) -> Pushed | None:
        """None until the watch delivered the key; a watch that died says why and starts over."""
        if self.watching is not None and self.watching.done():
            if not self.watching.cancelled() and self.watching.exception():
                log.warning("watch: %r", self.watching.exception())
            self.watch()
        return self.pushed.get(key)

    @classmethod
    async def attach(cls, js: JetStreamContext, service: str, node: str):
        """For reading and requests: neither creates the bucket nor changes it, nor writes the lease."""
        return cls(js, await js.key_value(cls.BUCKET), service, node)

    @property
    def stream(self) -> str:
        return stream(self.BUCKET)

    async def exact(self, key: str) -> tuple[bytes | None, int]:
        return await exact(self.js, self.BUCKET, key)

    async def write_lease(self, value: bytes, last: int) -> int:
        """Each write carries the writer's TTL: the key outlives its holder's fencing whatever TTL others run with.
        NATS takes whole seconds, so the TTL is rounded up, never down below the fencing time."""
        try:
            ack = await self.js.publish(f"$KV.{self.BUCKET}.{self.k_lease}", value, msg_ttl=math.ceil(self.lease_ttl),
                                        headers={"Nats-Expected-Last-Subject-Sequence": str(last)})
        except APIError as e:
            if e.err_code in WRONG_LAST_SEQUENCE:
                raise KeyWrongLastSequenceError(description=e.description) from None
            raise
        return ack.seq

    async def acquire(self) -> int | None:
        value, revision = await self.exact(self.k_lease)
        if value:
            return None
        try:
            return await self.write_lease(self.node.encode(), revision)
        except KeyWrongLastSequenceError:
            return None

    async def renew(self, revision: int) -> int:
        try:
            return await self.write_lease(self.node.encode(), revision)
        except KeyWrongLastSequenceError:
            value, current = await self.exact(self.k_lease)
            if value == self.node.encode():
                raise RevisionMoved(current) from None
            raise LeaseLost() from None

    async def release(self, revision: int) -> None:
        try:
            await self.write_lease(b"", revision)
        except KeyWrongLastSequenceError:
            pass

    async def release_mine(self) -> None:
        value, revision = await self.exact(self.k_lease)
        if value == self.node.encode():
            await self.release(revision)

    async def holder(self) -> str | None:
        """A lease that went unrenewed for a TTL has expired: its key leaves without a word to the watch."""
        pushed = self.latest(self.k_lease)
        if pushed is not None:
            fresh = time.monotonic() - pushed.at < self.lease_ttl
            return pushed.value.decode() if pushed.value and fresh else None
        try:
            entry = await self.kv.get(self.k_lease)
        except KeyNotFoundError:
            return None
        return entry.value.decode() if entry.value else None

    async def desired(self) -> Desired | None:
        pushed = self.latest(self.k_desired)
        if pushed is not None:
            value = pushed.value
        else:
            try:
                value = (await self.kv.get(self.k_desired)).value
            except KeyNotFoundError:
                return None
        if not value:
            return None
        d = json.loads(value)
        return Desired(node=d["node"], at=d["at"])

    async def request(self, desired: Desired) -> None:
        await self.kv.put(self.k_desired, json.dumps({"node": desired.node, "at": desired.at}).encode())

    async def clear_desired(self, desired: Desired) -> None:
        value, revision = await self.exact(self.k_desired)
        if value and json.loads(value) == {"node": desired.node, "at": desired.at}:
            try:
                await self.kv.update(self.k_desired, b"", last=revision)
            except KeyWrongLastSequenceError:
                pass

    async def position(self) -> tuple[Position | None, int]:
        value, revision = await self.exact(self.k_position)
        if not value:
            return None, revision
        p = json.loads(value)
        return Position(txids=p["txids"], node=p["node"], term=p["term"]), revision

    async def record_position(self, position: Position, after: int) -> int:
        value = json.dumps({"txids": position.txids, "node": position.node, "term": position.term}).encode()
        try:
            if after:
                return await self.kv.update(self.k_position, value, last=after)
            return await self.kv.create(self.k_position, value)
        except KeyWrongLastSequenceError:
            stored, current = await self.exact(self.k_position)
            if stored == value:
                return current
            raise LeaseLost() from None

    async def history(self) -> list[dict]:
        value, _ = await self.exact(self.k_history)
        return json.loads(value) if value else []

    async def record(self, event: str) -> None:
        for _ in range(5):
            value, revision = await self.exact(self.k_history)
            entries = (json.loads(value) if value else []) + [
                {"at": int(time.time()), "node": self.node, "event": event}]
            try:
                if revision:
                    await self.kv.update(self.k_history, json.dumps(entries[-HISTORY_LENGTH:]).encode(),
                                            last=revision)
                else:
                    await self.kv.create(self.k_history, json.dumps(entries[-HISTORY_LENGTH:]).encode())
                return
            except KeyWrongLastSequenceError:
                continue


class StreamLeaders:
    """Renewals and replica writes then commit next to the leader, not on another, maybe busy, server."""

    def __init__(self, nc, stream_name: str, server: str):
        self.nc, self.stream, self.server = nc, stream_name, server

    async def pull_here(self) -> None:
        info = json.loads((await self.nc.request(f"$JS.API.STREAM.INFO.{self.stream}", b"", timeout=5)).data)
        leader = info.get("cluster", {}).get("leader")
        if leader and leader != self.server:
            await self.nc.request(f"$JS.API.STREAM.LEADER.STEPDOWN.{self.stream}",
                                  json.dumps({"placement": {"preferred": self.server}}).encode(), timeout=5)
