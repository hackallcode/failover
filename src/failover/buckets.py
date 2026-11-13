from nats.js import JetStreamContext
from nats.js.api import KeyValueConfig
from nats.js.errors import NotFoundError
from nats.js.kv import KeyValue


def stream(bucket_name: str) -> str:
    return f"KV_{bucket_name}"


async def bucket(js: JetStreamContext, config: KeyValueConfig) -> KeyValue:
    """Direct reads on, whoever created the bucket: the nearest replica answers, not the stream leader on another
    server."""
    try:
        info = await js.stream_info(stream(config.bucket))
    except NotFoundError:
        return await js.create_key_value(config=config)
    if bool(info.config.allow_direct) != bool(config.direct):
        info.config.allow_direct = bool(config.direct)
        await js.update_stream(info.config)
    return await js.key_value(config.bucket)


async def exact(js: JetStreamContext, bucket_name: str, key: str) -> tuple[bytes | None, int]:
    """Value and revision from the stream leader: a direct get may come from a lagging replica."""
    try:
        msg = await js.get_msg(stream(bucket_name), subject=f"$KV.{bucket_name}.{key}", direct=False)
    except NotFoundError:
        return None, 0
    return msg.data or None, msg.seq
