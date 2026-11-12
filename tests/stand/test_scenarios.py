"""One fault at a time; every test checks that no two nodes ever wrote at once."""
import time

from tests.stand import stand

TTL = 10
FENCE = TTL * 0.7
RECOVERY = 60
SYNC = 2.0  # Litestream's sync interval and some slack


def check_single_writer(rows):
    problems = stand.split_brain(rows)
    assert not problems, problems


def last_handover(rows):
    hs = stand.handovers(rows)
    assert hs, "no handover happened"
    return hs[-1]


def test_kill_the_leader(cluster):
    stand.kill(cluster)
    new = stand.wait_serving(RECOVERY, other_than=cluster)
    assert new == stand.PEER[cluster]
    rows = stand.writes()
    check_single_writer(rows)
    assert last_handover(rows).lost_seconds(rows) <= SYNC


def test_isolate_the_leader(cluster):
    stand.isolate(cluster, True)
    new = stand.wait_serving(RECOVERY, other_than=cluster)
    assert new == stand.PEER[cluster]
    stand.isolate(cluster, False)
    time.sleep(TTL)
    rows = stand.writes()
    check_single_writer(rows)
    assert last_handover(rows).lost_seconds(rows) <= FENCE + SYNC


def test_hung_agent_is_not_the_service(cluster):
    """A paused agent stops renewing; its writer is paused with it, so nothing writes without the lease."""
    stand.pause(cluster, True)
    new = stand.wait_serving(RECOVERY, other_than=cluster)
    assert new == stand.PEER[cluster]
    stand.pause(cluster, False)
    time.sleep(TTL)
    check_single_writer(stand.writes())


def test_manual_switch_loses_nothing(cluster):
    stand.request(stand.PEER[cluster])
    assert stand.wait_serving(RECOVERY, other_than=cluster) == stand.PEER[cluster]
    rows = stand.writes()
    check_single_writer(rows)
    assert last_handover(rows).lost(rows) == []


def test_witness_down_keeps_the_leader(cluster):
    stand.kill("c")
    time.sleep(3 * TTL)
    assert stand.serving() == cluster
    rows = stand.writes()
    check_single_writer(rows)
    for h in stand.handovers(rows):
        assert h.lost_seconds(rows) <= SYNC


def test_witness_and_leader_down_nobody_serves(cluster):
    stand.kill("c")
    time.sleep(3)
    stand.kill(cluster)
    assert stand.wait_serving(3 * TTL, other_than=cluster) is None
    stand.start("c")
    assert stand.wait_serving(RECOVERY) == stand.PEER[cluster]
    check_single_writer(stand.writes())


def test_quorum_loss_fences_the_leader(cluster):
    stand.isolate("c", True)
    stand.isolate(stand.PEER[cluster], True)
    time.sleep(FENCE + 5)
    assert stand.serving() is None
    stand.isolate("c", False)
    stand.isolate(stand.PEER[cluster], False)
    assert stand.wait_serving(RECOVERY)
    check_single_writer(stand.writes())


def test_standby_down_for_a_while_then_leader_dies(cluster):
    standby = stand.PEER[cluster]
    stand.kill(standby)
    time.sleep(60)
    stand.start(standby)
    time.sleep(20)
    stand.kill(cluster)
    assert stand.wait_serving(RECOVERY, other_than=cluster) == standby
    rows = stand.writes()
    check_single_writer(rows)
    assert last_handover(rows).lost_seconds(rows) <= SYNC
