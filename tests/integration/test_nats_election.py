import asyncio
import uuid

import pytest
from nats.js.api import KeyValueConfig
from nats.js.errors import BucketNotFoundError, NotFoundError

from failover.agent import LeaseLost, RevisionMoved
from failover.domain import Desired, Position
from failover.nats_election import NatsElection


async def soon(read, expected, timeout=5):
    """Pushed values arrive a moment after the write."""
    deadline = asyncio.get_running_loop().time() + timeout
    while (value := await read()) != expected:
        assert asyncio.get_running_loop().time() < deadline, f"{value!r} != {expected!r}"
        await asyncio.sleep(0.05)


async def watching(*elections, timeout=5):
    deadline = asyncio.get_running_loop().time() + timeout
    while not all(len(e.pushed) == 2 for e in elections):
        assert asyncio.get_running_loop().time() < deadline, "the watch never answered"
        await asyncio.sleep(0.05)


async def pair(js, ttl=30):
    service = f"svc{uuid.uuid4().hex[:8]}"
    a = await NatsElection.open(js, service, "a", lease_ttl=ttl, replicas=1)
    b = await NatsElection.open(js, service, "b", lease_ttl=ttl, replicas=1)
    await watching(a, b)
    return a, b


async def test_one_holder_and_terms_grow(js):
    a, b = await pair(js)
    term = await a.acquire()
    assert term and await b.acquire() is None
    await soon(b.holder, "a")
    revision = await a.renew(term)
    assert revision > term
    await a.release(revision)
    await soon(b.holder, None)
    term2 = await b.acquire()
    assert term2 > revision
    await soon(b.holder, "b")


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
    await soon(b.desired, first)
    second = Desired("a", at=200)
    await a.request(second)
    await b.clear_desired(first)
    await soon(b.desired, second)
    await a.clear_desired(second)
    await soon(b.desired, None)


async def test_history_keeps_the_newest(js):
    a, b = await pair(js)
    await a.record("became leader")
    await b.record("standby")
    assert [h["event"] for h in await a.history()] == ["became leader", "standby"]


async def test_a_lease_lives_by_its_writers_ttl(js):
    service = f"svc{uuid.uuid4().hex[:8]}"
    long = await NatsElection.open(js, service, "a", lease_ttl=5, replicas=1)
    short = await NatsElection.open(js, service, "b", lease_ttl=1, replicas=1)
    await long.acquire()
    await asyncio.sleep(2)
    assert await short.acquire() is None
    await asyncio.sleep(4)
    assert await short.acquire()


async def test_position_write_with_a_lost_reply_is_still_mine(js):
    a, b = await pair(js)
    p1 = Position(txids={"main": 5}, node="a", term=3)
    r1 = await a.record_position(p1, 0)
    p2 = Position(txids={"main": 6}, node="a", term=3)
    r2 = await a.record_position(p2, r1)  # as if this reply was lost
    assert await a.record_position(p2, r1) == r2
    with pytest.raises(LeaseLost):
        await a.record_position(Position(txids={"main": 7}, node="a", term=3), r1)


async def without_bucket(js):
    try:
        await js.delete_key_value(NatsElection.BUCKET)
    except NotFoundError:
        pass


async def test_attach_neither_creates_nor_changes_the_bucket(js):
    await without_bucket(js)
    with pytest.raises(BucketNotFoundError):
        await NatsElection.attach(js, "svc", "a")
    with pytest.raises(NotFoundError):
        await js.stream_info(f"KV_{NatsElection.BUCKET}")


async def test_release_mine_frees_only_this_nodes_lease(js):
    a, b = await pair(js)
    await a.acquire()
    await b.release_mine()
    assert await a.holder() == "a"
    await a.release_mine()
    assert await b.acquire()


async def test_a_bucket_of_an_earlier_agent_gets_direct_reads(js):
    await without_bucket(js)
    await js.create_key_value(config=KeyValueConfig(bucket=NatsElection.BUCKET, history=1))
    election = await NatsElection.open(js, f"svc{uuid.uuid4().hex[:8]}", "a", lease_ttl=30, replicas=1)
    await watching(election)
    assert (await js.stream_info(f"KV_{NatsElection.BUCKET}")).config.allow_direct
    assert await election.acquire()
    await soon(election.holder, "a")


async def test_the_lease_and_the_request_are_pushed_not_read(js, monkeypatch):
    a, b = await pair(js)
    await a.acquire()
    await a.request(Desired("b", at=100))
    await soon(b.holder, "a")
    await soon(b.desired, Desired("b", at=100))

    async def no_reads(*args, **kwargs):
        raise AssertionError("read the bucket")

    monkeypatch.setattr(b.kv, "get", no_reads)
    for _ in range(20):
        assert await b.holder() == "a" and await b.desired() == Desired("b", at=100)


async def test_an_unrenewed_lease_reads_free_after_a_ttl_but_is_not_taken_early(js):
    service = f"svc{uuid.uuid4().hex[:8]}"
    holder = await NatsElection.open(js, service, "a", lease_ttl=30, replicas=1)
    reader = await NatsElection.open(js, service, "b", lease_ttl=1, replicas=1)
    await watching(holder, reader)
    await holder.acquire()
    await soon(reader.holder, "a")
    await asyncio.sleep(1.1)
    assert await reader.holder() is None
    assert await reader.acquire() is None


async def test_a_dead_watch_starts_over(js):
    a, b = await pair(js)
    b.watching.cancel()
    await asyncio.sleep(0)
    await a.acquire()
    await soon(b.holder, "a")
    assert not b.watching.done()


async def test_a_restarted_agent_sees_the_lease_it_left(js):
    service = f"svc{uuid.uuid4().hex[:8]}"
    old = await NatsElection.open(js, service, "a", lease_ttl=30, replicas=1)
    await watching(old)
    await old.acquire()
    old.watching.cancel()
    new = await NatsElection.open(js, service, "a", lease_ttl=30, replicas=1)
    await watching(new)
    assert await new.holder() == "a"
