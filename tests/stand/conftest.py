import os

import pytest

from tests.stand import stand


@pytest.fixture
def cluster():
    """A fresh stand per test: NATS ×3 (a, b, c), agents on a and b, one serving."""
    stand.up()
    leader = stand.wait_serving(90)
    assert leader, "nobody started serving"
    yield leader
    if not os.environ.get("KEEP_STAND"):
        stand.down()
