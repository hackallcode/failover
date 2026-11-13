from nats.js import JetStreamContext
from nats.js.api import KeyValueConfig
from nats.js.errors import NotFoundError
from nats.js.kv import KeyValue


async def bucket(js: JetStreamContext, config: KeyValueConfig) -> KeyValue:
    """A bucket created before direct reads were wanted gets them too: a direct read is answered by the nearest
    replica, not by the stream leader on another server."""
    try:
        info = await js.stream_info(f"KV_{config.bucket}")
    except NotFoundError:
        return await js.create_key_value(config=config)
    if bool(info.config.allow_direct) != bool(config.direct):
        info.config.allow_direct = bool(config.direct)
        await js.update_stream(info.config)
    return await js.key_value(config.bucket)
