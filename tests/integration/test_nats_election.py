import asyncio
import uuid

import pytest
from nats.js.errors import BucketNotFoundError, NotFoundError

from failover.agent import LeaseLost, RevisionMoved
from failover.domain import Desired, Position
from failover.nats_election import NatsElection


async def pair(js, ttl=30):
    service = f"svc{uuid.uuid4().hex[:8]}"
    a = await NatsElection.open(js, service, "a", lease_ttl=ttl, replicas=1)
    b = await NatsElection.open(js, service, "b", lease_ttl=ttl, replicas=1)
    return a, b


async def test_one_holder_and_terms_grow(js):
    a, b = await pair(js)
    term = await a.acquire()
    assert term and await b.acquire() is None
    assert await b.holder() == "a"
    revision = await a.renew(term)
    assert revision > term
    await a.release(revision)
    assert await b.holder() is None
    term2 = await b.acquire()
    assert term2 > revision and await b.holder() == "b"


async def test_renew_tells_a_moved_revision_from_a_lost_lease(js):
    a, b = await pair(js)
    term = await a.acquire()
    moved = await a.renew(term)  # as if this reply was lost
    with pytest.raises(RevisionMoved) as e:
        await a.renew(term)
    assert e.value.revision == moved
    await a.release(moved)
    await b.acquire()
    with pytest.raises(LeaseLost):
        await a.renew(moved)


async def test_release_is_a_no_op_for_a_stale_revision(js):
    a, b = await pair(js)
    term = await a.acquire()
    await a.release(term)
    term2 = await b.acquire()
    await a.release(term)  # late release after b took it
    assert await b.holder() == "b"
    assert await b.renew(term2)


async def test_lease_expires_without_renewals(js):
    a, b = await pair(js, ttl=2)
    await a.acquire()
    assert await b.acquire() is None
    await asyncio.sleep(3)
    assert await b.acquire()


async def test_position_writes_compare_revisions(js):
    a, b = await pair(js)
    assert await a.position() == (None, 0)
    p1 = Position(txids={"main": 5}, node="a", term=3)
    r1 = await a.record_position(p1, 0)
    assert await b.position() == (p1, r1)
    p2 = Position(txids={"main": 5}, node="b", term=9)
    r2 = await b.record_position(p2, r1)
    with pytest.raises(LeaseLost):
        await a.record_position(Position(txids={"main": 6}, node="a", term=3), r1)
    assert await a.position() == (p2, r2)


async def test_desired_is_cleared_only_if_unchanged(js):
    a, b = await pair(js)
    first = Desired("b", at=100)
    await a.request(first)
    assert await b.desired() == first
    second = Desired("a", at=200)
    await a.request(second)
    await b.clear_desired(first)
    assert await b.desired() == second
    await a.clear_desired(second)
    assert await b.desired() is None


async def test_history_keeps_the_newest(js):
    a, b = await pair(js)
    await a.record("became leader")
    await b.record("standby")
    assert [h["event"] for h in await a.history()] == ["became leader", "standby"]


async def test_reopening_with_another_ttl_moves_the_lease_expiry(js):
    service = f"svc{uuid.uuid4().hex[:8]}"
    await NatsElection.open(js, service, "a", lease_ttl=2, replicas=1)
    await NatsElection.open(js, service, "a", lease_ttl=5, replicas=1)
    info = await js.stream_info(f"KV_lease-{service}")
    assert info.config.max_age == 5


async def test_position_write_with_a_lost_reply_is_still_mine(js):
    a, b = await pair(js)
    p1 = Position(txids={"main": 5}, node="a", term=3)
    r1 = await a.record_position(p1, 0)
    p2 = Position(txids={"main": 6}, node="a", term=3)
    r2 = await a.record_position(p2, r1)  # as if this reply was lost
    assert await a.record_position(p2, r1) == r2
    with pytest.raises(LeaseLost):
        await a.record_position(Position(txids={"main": 7}, node="a", term=3), r1)


async def test_attach_neither_creates_nor_changes_buckets(js):
    service = f"svc{uuid.uuid4().hex[:8]}"
    with pytest.raises(BucketNotFoundError):
        await NatsElection.attach(js, service, "a")
    with pytest.raises(NotFoundError):
        await js.stream_info(f"KV_lease-{service}")


async def test_release_mine_frees_only_this_nodes_lease(js):
    a, b = await pair(js)
    await a.acquire()
    await b.release_mine()
    assert await a.holder() == "a"
    await a.release_mine()
    assert await b.acquire()
