import asyncio
import sqlite3
import uuid

import pytest
from nats.js.api import KeyValueConfig
from nats.js.errors import KeyWrongLastSequenceError

from failover.buckets import bucket
from failover.snapshots import Database, SnapshotReplica


def database(path, rows):
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE IF NOT EXISTS t (n INTEGER)")
        db.executemany("INSERT INTO t VALUES (?)", [(n,) for n in rows])


def rows(path):
    with sqlite3.connect(path) as db:
        return [n for (n,) in db.execute("SELECT n FROM t ORDER BY n")]


async def node(js, service, directory):
    directory.mkdir(exist_ok=True)
    kv = await bucket(js, KeyValueConfig(bucket="failover", history=1, direct=True))
    return SnapshotReplica(js, kv, "failover", service, [Database("main", directory / "main.db")])


async def eventually(check, timeout=10):
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        assert asyncio.get_running_loop().time() < deadline, "timed out"
        await asyncio.sleep(0.1)


async def test_a_leader_ships_each_change_and_another_node_restores_it(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    leader, other = await node(js, service, tmp_path / "a"), await node(js, service, tmp_path / "b")
    database(leader.databases[0].path, [1, 2])
    await leader.replicate()
    await eventually(lambda: leader.local().get("main"))
    first = leader.local()["main"]
    database(leader.databases[0].path, [3])
    await eventually(lambda: leader.local()["main"] > first)
    await leader.stop_replicating(graceful=False)

    restored = await other.restore({"main"})
    assert restored == leader.local() == await other.replicated()
    assert rows(other.databases[0].path) == [1, 2, 3]


async def test_an_unchanged_database_is_not_shipped_again(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    leader = await node(js, service, tmp_path / "a")
    database(leader.databases[0].path, [1])
    await leader.replicate()
    await eventually(lambda: leader.local().get("main"))
    shipped = leader.local()["main"]
    await asyncio.sleep(2.5)
    await leader.stop_replicating(graceful=False)
    assert (await leader.replicated())["main"] == shipped


async def test_a_node_that_lost_the_lease_cannot_overwrite_a_newer_snapshot(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    old, new = await node(js, service, tmp_path / "a"), await node(js, service, tmp_path / "b")
    database(old.databases[0].path, [1])
    await old.replicate()
    await eventually(lambda: old.local().get("main"))
    database(new.databases[0].path, [2])
    await new.replicate()
    await eventually(lambda: new.local().get("main"))
    database(old.databases[0].path, [3])
    with pytest.raises(KeyWrongLastSequenceError):
        await asyncio.wait_for(old.uploading, 10)
    await new.stop_replicating(graceful=False)
    await new.restore({"main"})
    assert rows(new.databases[0].path) == [2]


async def test_a_standby_receives_each_snapshot_without_polling(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    leader, standby = await node(js, service, tmp_path / "a"), await node(js, service, tmp_path / "b")
    await standby.follow()
    database(leader.databases[0].path, [1])
    await leader.replicate()
    await eventually(lambda: standby.standby().get("main") and standby.standby() == leader.local())
    database(leader.databases[0].path, [2])
    await eventually(lambda: standby.standby() == leader.local() and leader.local()["main"] > 0
                     and rows(standby.databases[0].standby) == [1, 2])
    await leader.stop_replicating(graceful=False)
    await standby.stop_following()
    await standby.promote_standby({"main"})
    assert rows(standby.databases[0].path) == [1, 2] and standby.local() == leader.local()


async def test_restore_of_a_required_database_that_was_never_shipped_fails(js, tmp_path):
    replica = await node(js, f"svc{uuid.uuid4().hex[:8]}", tmp_path / "a")
    assert await replica.restore({"main"}) is None
    assert await replica.restore(set()) == {}


async def test_a_leader_handing_over_ships_its_last_change(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    leader, other = await node(js, service, tmp_path / "a"), await node(js, service, tmp_path / "b")
    database(leader.databases[0].path, [1])
    await leader.stop_replicating(graceful=True)
    await other.restore({"main"})
    assert rows(other.databases[0].path) == [1]


async def test_a_replicating_leader_knows_its_revisions_without_fetching_snapshots(js, tmp_path, monkeypatch):
    service = f"svc{uuid.uuid4().hex[:8]}"
    leader = await node(js, service, tmp_path / "a")
    database(leader.databases[0].path, [1])
    await leader.replicate()
    await eventually(lambda: leader.local().get("main"))
    fetched = []
    get_msg = js.get_msg

    async def counting(*args, **kwargs):
        fetched.append(args)
        return await get_msg(*args, **kwargs)

    monkeypatch.setattr(js, "get_msg", counting)
    assert await leader.replicated() == leader.local()
    assert not fetched
    await leader.stop_replicating(graceful=False)
