"""Decisions of one failover service: pure functions over plain data, no I/O."""
from dataclasses import dataclass
from enum import Enum

# A leader stops serving once this share of the lease TTL passed since its last confirmed renewal was sent:
# the lease expires on the server no earlier than TTL after that send, so the rest is the margin.
FENCE_RATIO = 0.7


Txids = dict[str, int]


@dataclass(frozen=True)
class Position:
    txids: Txids
    node: str
    term: int


@dataclass(frozen=True)
class Desired:
    node: str
    at: float


class Source(Enum):
    """Where a new leader takes the databases from."""

    EMPTY = "empty"
    LOCAL = "local"
    STANDBY = "standby"
    RESTORE = "restore"


def fence_due(now: float, confirmed_sent: float, ttl: float) -> bool:
    return now - confirmed_sent > ttl * FENCE_RATIO


def desired_fresh(desired: Desired | None, now: float, valid_for: float) -> bool:
    return desired is not None and now - desired.at <= valid_for


def hand_over_to(desired: Desired | None, me: str, now: float, valid_for: float) -> str | None:
    if desired_fresh(desired, now, valid_for) and desired.node != me:
        return desired.node
    return None


def may_campaign(desired: Desired | None, me: str, now: float, valid_for: float,
                 yielded_at: float | None, yield_for: float) -> bool:
    if yielded_at is not None and now - yielded_at < yield_for:
        return False
    return not (desired_fresh(desired, now, valid_for) and desired.node != me)


def highest(*txids: Txids) -> Txids:
    merged: Txids = {}
    for view in txids:
        for db, txid in view.items():
            merged[db] = max(merged.get(db, 0), txid)
    return merged


def target(position: Position | None, replicated: Txids) -> Txids | None:
    """The position trails the replica, so a new leader must also reach what the replica shows."""
    if position is None and not replicated:
        return None
    return highest(position.txids if position else {}, replicated)


def reaches(goal: Txids, txids: Txids) -> bool:
    """A missing database fails: a lagging stream replica may list an empty or short replica."""
    return all(txids.get(db, 0) >= txid for db, txid in goal.items())


def takeover_source(position: Position | None, me: str, local: Txids, standby: Txids,
                    replicated: Txids) -> Source:
    goal = target(position, replicated)
    if goal is None:
        return Source.EMPTY
    if not goal:
        return Source.RESTORE
    if position is not None and position.node == me and reaches(goal, local):
        return Source.LOCAL
    if reaches(goal, standby):
        return Source.STANDBY
    return Source.RESTORE


def restore_serves(goal: Txids | None, restored: Txids) -> bool:
    return not goal or reaches(goal, restored)
