import pytest

from failover.litestream import Database, Litestream


def replica_of(tmp_path, *names, binary="litestream", follow_interval=10.0):
    databases = [Database(name, tmp_path / f"{name}.db") for name in names]
    return Litestream(binary, tmp_path / "litestream.yml", databases, None, "app", follow_interval), databases


def test_litestream_flags_come_before_the_database_path(tmp_path):
    replica, _ = replica_of(tmp_path, "main")
    argv = replica.command("restore", "-json", "-o", "/tmp/out", "/data/db")
    assert argv == ["litestream", "restore", "-config", str(tmp_path / "litestream.yml"), "-json", "-o", "/tmp/out",
                    "/data/db"]


async def test_promote_replaces_only_databases_with_a_standby_copy(tmp_path):
    replica, (main, panel) = replica_of(tmp_path, "main", "panel")
    main.path.write_text("old main")
    main.standby.write_text("new main")
    panel.path.write_text("panel")
    await replica.promote_standby({"main"})
    assert main.path.read_text() == "new main" and panel.path.read_text() == "panel"


async def test_promote_touches_nothing_when_a_required_copy_is_missing(tmp_path):
    replica, (main, panel) = replica_of(tmp_path, "main", "panel")
    main.path.write_text("old main")
    main.standby.write_text("new main")
    with pytest.raises(FileNotFoundError):
        await replica.promote_standby({"main", "panel"})
    assert main.path.read_text() == "old main"


async def test_a_follower_that_exits_is_not_restarted_before_the_follow_interval(tmp_path):
    starts = tmp_path / "starts"
    binary = tmp_path / "litestream"
    binary.write_text(f"#!/bin/sh\necho started >> {starts}\n")
    binary.chmod(0o755)
    replica, _ = replica_of(tmp_path, "main", binary=str(binary), follow_interval=60)
    await replica.follow()
    await replica.following["main"].wait()
    await replica.follow()
    await replica.stop_following()
    assert starts.read_text().count("started") == 1


def test_a_stall_counts_from_when_the_wal_grows_without_new_ltx(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("failover.litestream.time.monotonic", lambda: clock[0])
    replica, (main,) = replica_of(tmp_path, "main")
    main.path.write_text("db")
    level0 = main.meta / "ltx" / "0"
    level0.mkdir(parents=True)
    (level0 / "0000000000000001-0000000000000001.ltx").write_text("x")
    wal = tmp_path / "main.db-wal"
    wal.write_bytes(b"0" * 100)
    assert replica.stall_seconds() == 0
    clock[0] += 60
    assert replica.stall_seconds() == 0  # nothing written: an idle service
    wal.write_bytes(b"0" * 200)
    assert replica.stall_seconds() == 0
    clock[0] += 31
    assert replica.stall_seconds() == 31
    (level0 / "0000000000000002-0000000000000002.ltx").write_text("x")
    assert replica.stall_seconds() == 0
