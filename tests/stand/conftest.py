import os

import pytest

from tests.stand import stand

LOG_LINES_ON_FAILURE = 300


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    report = (yield).get_result()
    if report.when == "call" and report.failed:
        logs = stand.compose("logs", "--no-color", "node-a", "node-b", check=False).splitlines()
        agents = [line for line in logs if "GET /" not in line][-LOG_LINES_ON_FAILURE:]
        report.sections.append(("agent logs", "\n".join(agents)))


@pytest.fixture
def cluster():
    """A fresh stand per test: NATS ×3 (a, b, c), agents on a and b, one serving."""
    stand.up()
    leader = stand.wait_serving(90)
    assert leader, "nobody started serving"
    yield leader
    if not os.environ.get("KEEP_STAND"):
        stand.down()
