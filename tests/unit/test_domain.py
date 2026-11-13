from failover.domain import (Desired, Position, Source, fence_due, hand_over_to, may_campaign, restore_serves,
                             takeover_source)

TTL = 10


def test_fence_after_seventy_percent_of_ttl_since_confirmed_send():
    assert not fence_due(now=107.0, confirmed_sent=100.0, ttl=TTL)
    assert fence_due(now=107.1, confirmed_sent=100.0, ttl=TTL)


def test_hand_over_only_to_another_node_on_a_fresh_request():
    assert hand_over_to(Desired("b", at=100), "a", now=150, valid_for=120) == "b"
    assert hand_over_to(Desired("a", at=100), "a", now=150, valid_for=120) is None
    assert hand_over_to(Desired("b", at=100), "a", now=221, valid_for=120) is None
    assert hand_over_to(None, "a", now=150, valid_for=120) is None


def test_campaign_waits_for_the_requested_node_and_after_handing_over():
    assert may_campaign(None, "a", now=100, valid_for=120, yielded_at=None, yield_for=15)
    assert not may_campaign(Desired("b", at=90), "a", now=100, valid_for=120, yielded_at=None, yield_for=15)
    assert may_campaign(Desired("a", at=90), "a", now=100, valid_for=120, yielded_at=None, yield_for=15)
    assert may_campaign(Desired("b", at=-100), "a", now=100, valid_for=120, yielded_at=None, yield_for=15)
    assert not may_campaign(None, "a", now=100, valid_for=120, yielded_at=90, yield_for=15)
    assert may_campaign(None, "a", now=106, valid_for=120, yielded_at=90, yield_for=15)


def test_takeover_source():
    pos = Position(txids={"main": 50, "panel": 9}, node="a", term=7)
    assert takeover_source(None, "b", local={}, standby={}, replicated={}) is Source.EMPTY
    # the newest position is mine and nothing was written after it elsewhere
    assert takeover_source(pos, "a", local={"main": 52, "panel": 9}, standby={}, replicated={}) is Source.LOCAL
    # mine, but one database is behind it (an older copy) - not local
    assert takeover_source(pos, "a", local={"main": 52, "panel": 8}, standby={}, replicated={}) is Source.RESTORE
    # another node wrote it: my database diverged even with higher txids
    assert takeover_source(pos, "b", local={"main": 90, "panel": 90}, standby={}, replicated={}) is Source.RESTORE
    assert takeover_source(pos, "b", local={}, standby={"main": 50, "panel": 9}, replicated={}) is Source.STANDBY
    assert takeover_source(pos, "b", local={}, standby={"main": 50}, replicated={}) is Source.RESTORE


def test_restore_must_reach_the_position():
    pos = Position(txids={"main": 50, "panel": 9}, node="a", term=7)
    assert restore_serves(None, {})
    assert restore_serves(pos.txids, {"main": 50, "panel": 9})
    assert not restore_serves(pos.txids, {"main": 49, "panel": 9})
    # "no matching backups" from a lagging replica listing
    assert not restore_serves(pos.txids, {})


def test_takeover_target_includes_what_the_bucket_already_holds():
    from failover.domain import target

    pos = Position(txids={"main": 50}, node="a", term=7)
    assert target(pos, {"main": 53, "panel": 2}) == {"main": 53, "panel": 2}
    assert target(None, {}) is None
    assert target(None, {"main": 4}) == {"main": 4}
    # a standby that matches the stale position but not the bucket is not good enough
    assert takeover_source(pos, "b", local={}, standby={"main": 50}, replicated={"main": 53}) is Source.RESTORE
    # nothing recorded but the bucket has data (state lost): restore, never start empty
    assert takeover_source(None, "b", local={}, standby={}, replicated={"main": 4}) is Source.RESTORE
    # an empty claimed position does not let any copy pass
    empty = Position(txids={}, node="a", term=7)
    assert takeover_source(empty, "b", local={}, standby={}, replicated={"main": 9}) is Source.RESTORE
    assert not restore_serves(target(empty, {"main": 9}), {})


def test_a_service_that_had_a_leader_restores_even_with_an_empty_position():
    from failover.domain import target

    empty = Position(txids={}, node="a", term=7)
    # the first leader claimed before anything was replicated and the listing came back short
    assert takeover_source(empty, "b", local={}, standby={}, replicated={}) is Source.RESTORE
    assert takeover_source(empty, "a", local={}, standby={}, replicated={}) is Source.RESTORE
    assert restore_serves(target(empty, {}), {})
    assert takeover_source(None, "a", local={}, standby={}, replicated={}) is Source.EMPTY
