from types import SimpleNamespace

import nats
import pytest

from failover import main
from failover.agent import Action, Status
from failover.config import load
from tests.unit.config_files import write


def test_a_held_leader_still_counts_as_the_leader(tmp_path):
    config = load(write(tmp_path))
    assert 'failover_leader{service="app"} 1' in main.metrics(Status(action=Action.HELD, leader="a"), config)


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


async def test_an_agent_out_of_the_quorum_runs_and_says_so(tmp_path, monkeypatch):
    config = load(write(tmp_path, metrics_dir=str(tmp_path)))
    sent, answers = [], iter([nats.js.errors.ServiceUnavailableError(), "election"])

    async def open_election(_config, _js):
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return SimpleNamespace(kv=None)

    async def check_metrics(_):
        sent.append((tmp_path / "failover_app.prom").read_text())

    monkeypatch.setattr(main, "notify", sent.append)
    monkeypatch.setattr(main, "open_election", open_election)
    monkeypatch.setattr(main.asyncio, "sleep", check_metrics)
    monkeypatch.setattr(main, "SnapshotReplica", lambda *args: "replica")
    assert await main.set_up(config, js=None) == (SimpleNamespace(kv=None), "replica")
    assert sent[:2] == ["READY=1", "WATCHDOG=1"]
    assert 'failover_has_leader{service="app"} 0' in sent[2] and 'failover_error{service="app"} 1' in sent[2]
