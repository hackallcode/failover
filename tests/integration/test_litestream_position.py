import json
import uuid

from nats.js.api import ObjectStoreConfig

from failover.litestream import Database, Litestream


async def test_replicated_reads_the_newest_txid_from_object_names(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    db = Database("main", tmp_path / "db.sqlite")
    store = await js.create_object_store(db.bucket(service), ObjectStoreConfig(bucket=db.bucket(service)))
    replica = Litestream("litestream", tmp_path / "litestream.yml", [db], js, service)
    assert await replica.replicated() == {}
    for name in ("ltx/0/0000000000000009-0000000000000009.ltx", "ltx/1/0000000000000001-000000000000000a.ltx",
                 "ltx/0/000000000000000b-000000000000000b.ltx"):
        await store.put(name, b"x")
    await store.delete("ltx/0/000000000000000b-000000000000000b.ltx")  # compacted away: still was replicated
    assert await replica.replicated() == {"main": 0xB}


async def test_open_caps_every_replica_bucket_and_shortens_snapshots(js, tmp_path):
    from failover.litestream import REPLICA_MAX_BYTES

    service = f"svc{uuid.uuid4().hex[:8]}"
    old, new = Database("old", tmp_path / "old.sqlite"), Database("new", tmp_path / "new.sqlite")
    await js.create_object_store(old.bucket(service), ObjectStoreConfig(bucket=old.bucket(service)))
    config = tmp_path / "litestream.yml"
    await Litestream.open(js, "litestream", config, [old, new], service, "nats://127.0.0.1:4222", replicas=1)
    for db in (old, new):
        info = await js.stream_info(f"OBJ_{db.bucket(service)}")
        assert info.config.max_bytes == REPLICA_MAX_BYTES
    assert json.loads(config.read_text())["snapshot"] == {"interval": "1h", "retention": "2h"}


async def test_a_bucket_on_a_single_server_is_ready(js, tmp_path):
    service = f"svc{uuid.uuid4().hex[:8]}"
    db = Database("main", tmp_path / "db.sqlite")
    await js.create_object_store(db.bucket(service), ObjectStoreConfig(bucket=db.bucket(service)))
    assert await Litestream("litestream", tmp_path / "litestream.yml", [db], js, service).ready()
