import os

import pytest

from failover.compose import ComposeService


@pytest.fixture
def slow_up(tmp_path, monkeypatch):
    docker = tmp_path / "docker"
    docker.write_text("#!/bin/sh\nsleep 0.5\n")
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    return tmp_path


async def test_up_runs_until_the_start_timeout(slow_up):
    await ComposeService([slow_up], start_timeout=5).start()
    with pytest.raises(RuntimeError):
        await ComposeService([slow_up], start_timeout=0.1).start()
