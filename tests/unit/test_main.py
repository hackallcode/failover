from types import SimpleNamespace

import nats
import pytest

from failover import main
from failover.agent import Action, Status
from failover.config import load
from tests.unit.config_files import write


def test_a_held_leader_still_counts_as_the_leader(tmp_path):
    config = load(write(tmp_path))
    agent = SimpleNamespace(status=Status(action=Action.HELD, leader="a"))
    assert 'failover_leader{service="app"} 1' in main.metrics(agent, config)


def test_a_slow_start_does_not_count_as_a_hung_agent(tmp_path):
    config = load(write(tmp_path, start_timeout=300))
    agent = SimpleNamespace(status=Status(action=Action.SERVING), starting_service=True)
    assert main.stalled_after(agent, config) > 300


async def test_a_command_retries_a_cluster_busy_electing(monkeypatch):
    async def no_wait(_):
        pass

    monkeypatch.setattr(main.asyncio, "sleep", no_wait)
    answers = iter([nats.errors.TimeoutError(), nats.errors.TimeoutError(), "state"])

    async def action():
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    assert await main.retried(action) == "state"
    with pytest.raises(nats.errors.TimeoutError):
        await main.retried(always_busy)


async def always_busy():
    raise nats.errors.TimeoutError()
