import asyncio
import os
import sqlite3
import time
import uuid
import zlib

import pytest
from nats.js.api import KeyValueConfig
from nats.js.errors import KeyWrongLastSequenceError

from failover.buckets import bucket
from failover.snapshots import REBASE_EVERY, Database, SnapshotReplica, snapshot


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


async def test_a_snapshot_whose_reply_was_lost_is_taken_as_written(js, tmp_path):
    leader = await node(js, f"svc{uuid.uuid4().hex[:8]}", tmp_path / "a")
    database(leader.databases[0].path, [1])
    await leader.load()
    await leader.ship_once({})
    written = leader.shipped_revisions["main"]

    leader.bases["main"], leader.image_revisions["main"] = None, 0
    await leader.ship_once({})
    assert leader.shipped_revisions["main"] == written


async def test_a_hand_over_fails_when_its_last_change_cannot_be_shipped(js, tmp_path):
    leader = await node(js, f"svc{uuid.uuid4().hex[:8]}", tmp_path / "a")
    leader.databases[0].path.write_bytes(b"not a database")
    with pytest.raises(sqlite3.DatabaseError):
        await leader.stop_replicating(graceful=True)


def padded(path, rows):
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE IF NOT EXISTS t (n INTEGER, pad TEXT)")
        db.executemany("INSERT INTO t VALUES (?, ?)", [(n, os.urandom(400).hex()) for n in rows])


async def test_a_change_ships_only_its_pages_and_a_standby_applies_them(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    leader, standby = await node(js, service, tmp_path / "a"), await node(js, service, tmp_path / "b")
    path = leader.databases[0].path
    padded(path, range(300))
    await standby.follow()
    await leader.replicate()
    await eventually(lambda: leader.local().get("main"))
    image, image_revision = await leader.latest(leader.databases[0])

    with sqlite3.connect(path) as db:
        db.execute("UPDATE t SET n = -1 WHERE n = 7")
    await eventually(lambda: leader.local()["main"] > image_revision and standby.standby() == leader.local())
    delta, _ = await leader.latest_delta(leader.databases[0])
    assert (await leader.latest(leader.databases[0]))[1] == image_revision
    assert len(delta) * 10 < len(image)

    changed = leader.local()["main"]
    with sqlite3.connect(path) as db:
        db.execute("UPDATE t SET n = 7 WHERE n = -1")
    await eventually(lambda: leader.local()["main"] > changed and standby.standby() == leader.local())
    await leader.stop_replicating(graceful=False)
    await standby.stop_following()
    assert rows(standby.databases[0].standby) == rows(path) == list(range(300))


async def test_pages_outgrowing_their_image_ship_a_new_image(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    leader = await node(js, service, tmp_path / "a")
    path = leader.databases[0].path
    padded(path, range(50))
    await leader.replicate()
    await eventually(lambda: leader.local().get("main"))
    _, image_revision = await leader.latest(leader.databases[0])

    padded(path, range(50, 400))
    await eventually(lambda: leader.local()["main"] > image_revision)
    await leader.stop_replicating(graceful=False)
    assert (await leader.latest(leader.databases[0]))[1] == leader.local()["main"]


async def test_a_delta_over_another_image_is_neither_restored_nor_in_the_way(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    old, new = await node(js, service, tmp_path / "a"), await node(js, service, tmp_path / "b")
    padded(old.databases[0].path, range(100))
    await old.load()
    await old.ship_once({})
    padded(new.databases[0].path, range(1000, 1100))
    await new.load()
    new.bases["main"] = None
    await new.ship_once({})

    padded(old.databases[0].path, [100])
    await old.ship_once({})
    assert (await old.restore({"main"})) is not None and rows(old.databases[0].path) == list(range(1000, 1100))

    padded(new.databases[0].path, [1100])
    await new.ship_once({})
    restored = await old.restore({"main"})
    assert restored == {"main": new.shipped_revisions["main"]}
    assert rows(old.databases[0].path) == list(range(1000, 1101))


async def test_an_image_a_day_old_is_replaced_by_the_next_change(js, tmp_path, monkeypatch):
    leader = await node(js, f"svc{uuid.uuid4().hex[:8]}", tmp_path / "a")
    path = leader.databases[0].path
    padded(path, range(100))
    await leader.load()
    await leader.ship_once({})
    _, image_revision = await leader.latest(leader.databases[0])

    later = time.monotonic() + REBASE_EVERY
    monkeypatch.setattr(time, "monotonic", lambda: later)
    padded(path, [100])
    await leader.ship_once({})
    assert (await leader.latest(leader.databases[0]))[1] == leader.shipped_revisions["main"] > image_revision


async def test_shipping_starts_over_after_the_bucket_failed_to_answer(js, tmp_path, monkeypatch):
    leader = await node(js, f"svc{uuid.uuid4().hex[:8]}", tmp_path / "a")
    database(leader.databases[0].path, [1])
    latest_delta = leader.latest_delta

    async def times_out(db):
        monkeypatch.setattr(leader, "latest_delta", latest_delta)
        raise TimeoutError()

    monkeypatch.setattr(leader, "latest_delta", times_out)
    await leader.replicate()
    await eventually(lambda: leader.local().get("main"))
    await leader.stop_replicating(graceful=False)


async def test_a_standby_that_cannot_write_its_copy_says_why_and_follows_again(js, tmp_path, caplog):
    service = f"svc{uuid.uuid4().hex[:8]}"
    leader, standby = await node(js, service, tmp_path / "a"), await node(js, service, tmp_path / "b")
    database(leader.databases[0].path, [1])
    await leader.replicate()
    await eventually(lambda: leader.local().get("main"))
    standby.databases[0].path.parent.rmdir()

    await standby.follow()
    await eventually(lambda: standby.following.done())
    await standby.follow()
    assert "standby: FileNotFoundError" in caplog.text
    standby.databases[0].path.parent.mkdir()
    await eventually(lambda: standby.standby() == leader.local())
    await standby.stop_following()
    await leader.stop_replicating(graceful=False)


async def test_a_hand_over_ships_past_a_write_the_cancel_cut_off(js, tmp_path):
    leader = await node(js, f"svc{uuid.uuid4().hex[:8]}", tmp_path / "a")
    path = leader.databases[0].path
    padded(path, range(100))
    await leader.replicate()
    await eventually(lambda: leader.local().get("main"))
    await leader.stop_replicating(graceful=False)
    await leader.write(leader.key(leader.databases[0]), zlib.compress(snapshot(path)), leader.image_revisions["main"])
    padded(path, [100])

    await leader.stop_replicating(graceful=True)
    await leader.restore({"main"})
    assert rows(path) == list(range(101))
