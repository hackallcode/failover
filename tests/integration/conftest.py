import socket
import subprocess
import time
import uuid

import nats
import pytest

NATS_IMAGE = "nats:2.15.2-alpine"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def nats_url():
    port, name = free_port(), f"failover-test-{uuid.uuid4().hex[:8]}"
    subprocess.run(["docker", "run", "-d", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:4222", NATS_IMAGE,
                    "-js"], check=True, capture_output=True)
    url = f"nats://127.0.0.1:{port}"
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                break
        except OSError:
            time.sleep(0.2)
    yield url
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)


@pytest.fixture
async def js(nats_url):
    nc = await nats.connect(nats_url)
    yield nc.jetstream(timeout=5)
    await nc.close()
