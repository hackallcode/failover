"""The election and the shared state of one service in NATS JetStream KV.

The lease is a key in a bucket whose max age is the lease TTL: a holder that stops renewing loses it. Every write
compares the key's revision, so only the holder can renew it, and the term is the revision that created it. A
released or expired lease is an empty value or no key; both are free.
"""
import json
import time

from nats.js import JetStreamContext
from nats.js.api import KeyValueConfig
from nats.js.errors import KeyNotFoundError, KeyWrongLastSequenceError, NotFoundError
from nats.js.kv import KeyValue

from failover.agent import LeaseLost, RevisionMoved
from failover.buckets import bucket
from failover.domain import Desired, Position

HISTORY_LENGTH = 50


class NatsElection:
    STATE_BUCKET = "failover"

    def __init__(self, js: JetStreamContext, leases: KeyValue, state: KeyValue, service: str, node: str):
        self.js, self.leases, self.state, self.service, self.node = js, leases, state, service, node
        self.lease_bucket = f"lease-{service}"
        self.k_desired, self.k_position, self.k_history = (f"{service}.desired", f"{service}.position",
                                                           f"{service}.history")

    @classmethod
    async def open(cls, js: JetStreamContext, service: str, node: str, lease_ttl: float, replicas: int):
        leases = await bucket(js, KeyValueConfig(bucket=f"lease-{service}", ttl=lease_ttl, history=1,
                                                 replicas=replicas, direct=True))
        # A shorter expiry than the TTL agents fence by would free the lease while its holder still serves.
        info = await js.stream_info(f"KV_lease-{service}")
        if info.config.max_age != lease_ttl:
            info.config.max_age = lease_ttl
            await js.update_stream(info.config)
        state = await bucket(js, KeyValueConfig(bucket=cls.STATE_BUCKET, history=1, replicas=replicas,
                                                         direct=True))
        return cls(js, leases, state, service, node)

    @classmethod
    async def attach(cls, js: JetStreamContext, service: str, node: str):
        """For reading and requests: neither creates the buckets nor changes them."""
        return cls(js, await js.key_value(f"lease-{service}"), await js.key_value(cls.STATE_BUCKET), service, node)

    async def exact(self, name: str, key: str) -> tuple[bytes | None, int]:
        """Value and revision from the stream leader: a direct get may come from a lagging replica."""
        try:
            msg = await self.js.get_msg(f"KV_{name}", subject=f"$KV.{name}.{key}", direct=False)
        except NotFoundError:
            return None, 0
        return msg.data or None, msg.seq

    async def acquire(self) -> int | None:
        value, revision = await self.exact(self.lease_bucket, self.service)
        if value:
            return None
        try:
            if revision:
                return await self.leases.update(self.service, self.node.encode(), last=revision)
            return await self.leases.create(self.service, self.node.encode())
        except KeyWrongLastSequenceError:
            return None

    async def renew(self, revision: int) -> int:
        try:
            return await self.leases.update(self.service, self.node.encode(), last=revision)
        except KeyWrongLastSequenceError:
            value, current = await self.exact(self.lease_bucket, self.service)
            if value == self.node.encode():
                raise RevisionMoved(current) from None
            raise LeaseLost() from None

    async def release(self, revision: int) -> None:
        try:
            await self.leases.update(self.service, b"", last=revision)
        except KeyWrongLastSequenceError:
            pass

    async def release_mine(self) -> None:
        value, revision = await self.exact(self.lease_bucket, self.service)
        if value == self.node.encode():
            await self.release(revision)

    async def holder(self) -> str | None:
        try:
            entry = await self.leases.get(self.service)
        except KeyNotFoundError:
            return None
        return entry.value.decode() if entry.value else None

    async def desired(self) -> Desired | None:
        try:
            entry = await self.state.get(self.k_desired)
        except KeyNotFoundError:
            return None
        if not entry.value:
            return None
        d = json.loads(entry.value)
        return Desired(node=d["node"], at=d["at"])

    async def request(self, desired: Desired) -> None:
        await self.state.put(self.k_desired, json.dumps({"node": desired.node, "at": desired.at}).encode())

    async def clear_desired(self, desired: Desired) -> None:
        value, revision = await self.exact(self.STATE_BUCKET, self.k_desired)
        if value and json.loads(value) == {"node": desired.node, "at": desired.at}:
            try:
                await self.state.update(self.k_desired, b"", last=revision)
            except KeyWrongLastSequenceError:
                pass

    async def position(self) -> tuple[Position | None, int]:
        value, revision = await self.exact(self.STATE_BUCKET, self.k_position)
        if not value:
            return None, revision
        p = json.loads(value)
        return Position(txids=p["txids"], node=p["node"], term=p["term"]), revision

    async def record_position(self, position: Position, after: int) -> int:
        value = json.dumps({"txids": position.txids, "node": position.node, "term": position.term}).encode()
        try:
            if after:
                return await self.state.update(self.k_position, value, last=after)
            return await self.state.create(self.k_position, value)
        except KeyWrongLastSequenceError:
            stored, current = await self.exact(self.STATE_BUCKET, self.k_position)
            if stored == value:
                return current
            raise LeaseLost() from None

    async def history(self) -> list[dict]:
        value, _ = await self.exact(self.STATE_BUCKET, self.k_history)
        return json.loads(value) if value else []

    async def record(self, event: str) -> None:
        for _ in range(5):
            value, revision = await self.exact(self.STATE_BUCKET, self.k_history)
            entries = (json.loads(value) if value else []) + [
                {"at": int(time.time()), "node": self.node, "event": event}]
            try:
                if revision:
                    await self.state.update(self.k_history, json.dumps(entries[-HISTORY_LENGTH:]).encode(),
                                            last=revision)
                else:
                    await self.state.create(self.k_history, json.dumps(entries[-HISTORY_LENGTH:]).encode())
                return
            except KeyWrongLastSequenceError:
                continue


class StreamLeaders:
    """Renewals and replica writes then commit next to the leader, not on another, maybe busy, server."""

    def __init__(self, nc, streams: list[str], server: str):
        self.nc, self.streams, self.server = nc, streams, server

    async def pull_here(self) -> None:
        for stream in self.streams:
            info = json.loads((await self.nc.request(f"$JS.API.STREAM.INFO.{stream}", b"", timeout=5)).data)
            leader = info.get("cluster", {}).get("leader")
            if leader and leader != self.server:
                await self.nc.request(f"$JS.API.STREAM.LEADER.STEPDOWN.{stream}",
                                      json.dumps({"placement": {"preferred": self.server}}).encode(), timeout=5)
