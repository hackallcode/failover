import pytest

from failover.agent import Action, Agent, Settings
from failover.domain import Desired, Position
from tests.unit import fakes

TTL = 10


class Node:
    def __init__(self, name, cluster, store, dns, clock, held=False):
        self.election = fakes.Election(cluster, name)
        self.replica = fakes.Replica(store)
        self.service = fakes.Service()
        settings = Settings(node=name, candidate=True, lease_ttl=TTL, tick=1, desired_valid=120, yield_for=15,
                            held=lambda: held)
        self.agent = Agent(settings, self.election, self.replica, self.service, fakes.Dns(dns, name),
                           clock=clock, wall=clock)


@pytest.fixture
def world():
    cluster, store, dns, clock = fakes.Cluster(), {}, {}, fakes.Clock()
    nodes = {n: Node(n, cluster, store, dns, clock) for n in ("a", "b")}
    return cluster, store, dns, clock, nodes


async def test_first_leader_starts_on_its_own_data_and_claims_the_position(world):
    cluster, _, dns, _, nodes = world
    await nodes["a"].agent.tick()
    assert nodes["a"].service.up and nodes["a"].replica.replicating
    assert nodes["a"].replica.restores == 0
    assert dns["name"] == "a"
    assert cluster.position[0] == Position(txids={}, node="a", term=nodes["a"].agent.status.term)
    await nodes["b"].agent.tick()
    assert not nodes["b"].service.up and nodes["b"].replica.following


async def test_leader_records_what_was_replicated(world):
    cluster, _, _, _, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    assert cluster.position[0].txids == {"main": 5}


async def test_takeover_restores_the_replica(world):
    cluster, store, dns, clock, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    cluster.lease = None  # a died, its lease expired
    await b.agent.tick()
    assert b.service.up and b.replica.restores == 1
    assert b.replica.local() == {"main": 5}
    assert dns["name"] == "b"


async def test_short_restore_does_not_serve_and_frees_the_lease(world):
    cluster, _, _, _, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    cluster.lease = None
    b.replica.short = True  # a lagging stream replica answered "no backups"
    await b.agent.tick()
    assert not b.service.up and not b.agent.leading
    assert cluster.lease is None
    assert any("did not take over" in e for e in cluster.history)


async def test_takeover_uses_a_caught_up_standby(world):
    cluster, _, _, _, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    b.replica.standby_txids = {"main": 5}
    cluster.lease = None
    await b.agent.tick()
    assert b.service.up and b.replica.promotions == 1 and b.replica.restores == 0


async def test_re_elected_leader_keeps_its_newer_local_database(world):
    cluster, store, _, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    a.replica.write("main", 6)  # committed locally, not shipped before the fence
    clock.now += TTL
    await a.agent.check_fence()
    assert not a.service.up
    cluster.lease = None
    await a.agent.tick()
    assert a.service.up and a.replica.restores == 0
    assert a.replica.local() == {"main": 6}


async def test_fence_when_renewals_fail_for_most_of_the_ttl(world):
    cluster, _, _, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    cluster.down = True
    clock.now += TTL * 0.6
    await a.agent.renew()
    await a.agent.check_fence()
    assert a.service.up
    clock.now += TTL * 0.2
    await a.agent.check_fence()
    assert not a.service.up and not a.replica.replicating
    assert a.agent.status.action is Action.FENCED


async def test_fence_when_another_node_holds_the_lease(world):
    cluster, _, _, _, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    cluster.lease = ("b", cluster.next())
    await a.agent.renew()
    assert not a.service.up and not a.agent.leading


async def test_old_leader_cannot_record_after_a_takeover(world):
    cluster, _, _, _, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    cluster.lease = None  # a is cut off but has not noticed yet
    await b.agent.tick()
    assert b.service.up
    a.replica.write("main", 7)
    await a.agent.tick()
    assert not a.service.up and not a.agent.leading
    assert cluster.position[0].node == "b"


async def test_manual_switch_hands_over_and_the_old_leader_waits(world):
    cluster, _, dns, clock, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    a.replica.write("main", 3)
    cluster.desired = Desired("b", at=clock.now)
    await a.agent.tick()
    assert not a.service.up and cluster.lease is None
    assert cluster.position[0].txids == {"main": 3}
    await a.agent.tick()
    assert not a.agent.leading  # yields to the requested node
    await b.agent.tick()
    assert b.service.up and dns["name"] == "b"
    assert cluster.desired is None


async def test_held_leader_neither_hands_over_nor_restarts(world):
    cluster, store, dns, clock, _ = world
    a = Node("a", cluster, store, dns, clock, held=True)
    a.agent.term, a.agent.revision, a.agent.confirmed_sent = 1, 1, clock.now
    cluster.lease = ("a", 1)
    cluster.desired = Desired("b", at=clock.now)
    await a.agent.tick()
    assert a.agent.status.action is Action.HELD
    assert not a.service.up and cluster.lease == ("a", 1)


async def test_renewal_with_a_lost_reply_keeps_the_lease_without_extending_it(world):
    """An adopted revision is no confirmation: only a renewal that got its reply moves the confirmed time."""
    cluster, _, _, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    sent = a.agent.confirmed_sent
    cluster.lease = ("a", cluster.next())  # an earlier renewal committed, its reply was lost
    real = a.election.renew

    async def renew(revision):
        if revision == cluster.lease[1]:
            raise TimeoutError("no reply")
        return await real(revision)

    a.election.renew = renew
    clock.now += 1
    await a.agent.renew()
    assert a.agent.leading and a.agent.revision == cluster.lease[1]
    assert a.agent.confirmed_sent == sent
    a.election.renew = real
    clock.now += 1
    await a.agent.renew()
    assert a.agent.confirmed_sent == clock.now


async def test_position_is_listed_at_most_every_few_seconds_but_at_once_on_hand_over(world):
    cluster, _, _, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.replica.write("main", 1)
    await a.agent.tick()
    assert cluster.position[0].txids == {"main": 1}
    a.replica.write("main", 2)
    await a.agent.tick()
    assert cluster.position[0].txids == {"main": 1}
    clock.now += a.agent.s.position_every
    await a.agent.tick()
    assert cluster.position[0].txids == {"main": 2}
    a.replica.write("main", 3)
    cluster.desired = Desired("b", at=clock.now)
    await a.agent.tick()
    assert cluster.position[0].txids == {"main": 3}


async def test_leader_pulls_its_streams_close_now_and_then(world):
    cluster, store, dns, clock, _ = world

    class Placement:
        calls = 0

        async def pull_here(self):
            Placement.calls += 1

    a = Node("a", cluster, store, dns, clock)
    a.agent.placement = Placement()
    await a.agent.tick()
    await a.agent.tick()
    await a.agent.tick()
    assert Placement.calls == 1
    clock.now += a.agent.s.placement_every
    await a.agent.tick()
    assert Placement.calls == 2


async def test_one_slow_reply_does_not_fence_the_leader(world):
    """A renewal whose reply comes too late (it committed) must not cost the leader its lease: renewals run every
    TTL/5 after the previous one returned, a timeout takes the request timeout, a fence check runs all along."""
    cluster, _, _, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    real = a.election.renew
    slow = {"left": 1}

    async def renew(revision):
        result = await real(revision)
        if slow["left"]:
            slow["left"] -= 1
            clock.now += 2  # waited for the reply
            raise TimeoutError("reply lost after commit")
        return result

    a.election.renew = renew
    for _ in range(6):
        clock.now += TTL / 5
        await a.agent.check_fence()
        await a.agent.renew()
        await a.agent.check_fence()
    assert a.agent.leading and a.service.up


async def test_cluster_ok_follows_completed_ticks(world):
    cluster, _, _, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    assert a.agent.cluster_ok()
    cluster.down = True
    clock.now += 10
    try:
        await a.agent.tick()
    except TimeoutError as e:
        a.agent.status.error = repr(e)  # what the run loop records
    assert not a.agent.cluster_ok()
    cluster.down = False
    await a.agent.tick()
    assert a.agent.cluster_ok()


async def test_leader_restarts_a_part_of_the_service_that_stopped(world):
    """A deploy stops one compose project of the service; the leader brings it back."""
    _, _, _, _, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    starts = a.service.starts
    a.service.partial = True
    await a.agent.tick()
    assert a.service.starts == starts + 1 and not a.service.partial


async def test_a_node_that_could_not_restore_lets_another_take_the_lease(world):
    cluster, _, _, clock, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    cluster.lease = None
    b.replica.short = True
    await b.agent.tick()
    assert not b.agent.leading and cluster.lease is None
    await b.agent.tick()
    assert b.replica.restores == 1  # waits instead of grabbing the lease again
    await a.agent.tick()
    assert a.agent.leading and a.service.up


async def test_a_failed_take_over_frees_the_lease_and_never_serves_stale_data(world):
    cluster, _, _, clock, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    cluster.lease = None
    real = b.election.position

    async def position():
        raise TimeoutError("no reply")

    b.election.position = position
    with pytest.raises(TimeoutError):
        await b.agent.tick()
    assert not b.agent.leading and cluster.lease is None
    b.election.position = real
    await b.agent.tick()
    assert not b.service.up and not b.replica.replicating
    clock.now += 20
    await b.agent.tick()
    assert b.service.up and b.replica.local() == {"main": 5}


async def test_an_empty_position_still_takes_what_reached_the_replica(world):
    cluster, store, _, clock, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    a.replica.write("main", 5)
    assert cluster.position[0].txids == {}
    cluster.lease = None

    async def short_listing():
        return {}

    b.replica.replicated = short_listing
    b.replica.unreadable = True
    await b.agent.tick()
    assert not b.service.up and not b.agent.leading
    b.replica.unreadable = False
    clock.now += 20
    await b.agent.tick()
    assert b.service.up and b.replica.local() == {"main": 5}
    assert cluster.position[0].txids == {"main": 5}


async def test_a_fence_stops_replicating_even_when_the_service_does_not_stop(world):
    _, _, _, _, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.service.stop_fails = True
    with pytest.raises(RuntimeError):
        await a.agent.fence("the lease is running out")
    assert not a.replica.replicating


async def test_a_restarted_leader_takes_its_lease_back_at_once(world):
    cluster, store, dns, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    restarted = Node("a", cluster, store, dns, clock)
    restarted.replica.local_txids = dict(a.replica.local_txids)
    await restarted.agent.tick()
    assert restarted.agent.leading and restarted.service.up


async def test_a_request_for_the_leader_itself_is_cleared(world):
    cluster, _, _, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    cluster.desired = Desired("a", at=clock.now)
    await a.agent.tick()
    assert cluster.desired is None and a.agent.leading


async def test_a_failed_take_over_clears_the_request_for_it(world):
    cluster, _, _, clock, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    cluster.desired = Desired("b", at=clock.now)
    await a.agent.tick()
    b.replica.short = True
    await b.agent.tick()
    assert not b.agent.leading and cluster.desired is None


async def test_a_stale_request_is_not_reported(world):
    cluster, _, _, clock, nodes = world
    a = nodes["a"]
    cluster.desired = Desired("b", at=clock.now - 1000)
    await a.agent.tick()
    assert a.agent.status.desired is None


async def test_a_database_outside_the_goal_does_not_block_a_take_over(world):
    cluster, _, _, _, nodes = world
    a, b = nodes["a"], nodes["b"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    b.replica.standby_txids = {"main": 5}
    cluster.lease = None
    await b.agent.tick()
    assert b.agent.leading and b.replica.promotions == 1


async def test_the_position_never_moves_back(world):
    cluster, store, _, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.replica.write("main", 7)
    a.replica.write("panel", 3)
    await a.agent.tick()
    store["bucket"] = {"main": 5, "panel": 4}
    clock.now += 10
    await a.agent.tick()
    assert cluster.position[0].txids == {"main": 7, "panel": 4}


async def test_a_fence_while_the_leader_restarts_its_service_stops_it(world):
    _, _, _, _, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.service.partial = True
    start = a.service.start

    async def fenced_while_starting():
        await a.agent.fence("the lease is running out")
        await start()

    a.service.start = fenced_while_starting
    await a.agent.tick()
    assert not a.service.up and not a.replica.replicating


async def test_a_standby_a_poll_behind_is_waited_for_instead_of_a_restore(world, monkeypatch):
    cluster, store, dns, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    b = Node("b", cluster, store, dns, clock)
    b.agent.s = Settings(**{**b.agent.s.__dict__, "standby_wait": 2})
    b.replica.standby_txids = {"main": 4}

    async def follower_catches_up(_):
        b.replica.standby_txids = {"main": 5}

    monkeypatch.setattr("failover.agent.asyncio.sleep", follower_catches_up)
    cluster.lease = None
    await b.agent.tick()
    assert b.agent.leading and b.replica.promotions == 1 and b.replica.restores == 0


async def test_no_replication_starts_before_the_replica_stream_has_a_leader(world):
    cluster, _, _, clock, nodes = world
    a = nodes["a"]
    a.replica.leaderless = True
    await a.agent.tick()
    assert not a.service.up and not a.replica.replicating and cluster.lease is None
    a.replica.leaderless = False
    clock.now += 20
    await a.agent.tick()
    assert a.service.up and a.replica.replicating


async def test_a_stalled_litestream_is_restarted(world):
    _, _, _, _, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.replica.stuck = True
    await a.agent.tick()
    assert a.replica.replication_restarts == 1 and a.replica.replicating and a.service.up


async def test_the_leader_reports_how_long_its_writes_are_not_replicated(world):
    _, store, _, clock, nodes = world
    a = nodes["a"]
    await a.agent.tick()
    a.replica.write("main", 5)
    await a.agent.tick()
    a.replica.local_txids["main"] = 9  # written locally, not shipped
    clock.now += 10
    await a.agent.tick()
    clock.now += 100
    await a.agent.tick()
    assert a.agent.status.replication_lag == 100
    store["bucket"]["main"] = 9
    clock.now += 10
    await a.agent.tick()
    assert a.agent.status.replication_lag == 0
