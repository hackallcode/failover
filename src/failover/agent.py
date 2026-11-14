"""One failover service on one node: hold the lease while leading, follow otherwise, never serve without it."""
import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from failover.domain import (Desired, Position, Source, Txids, desired_fresh, fence_due, hand_over_to,
                             may_campaign, highest, reaches, restore_serves, takeover_source, target)

log = logging.getLogger(__name__)

RENEW_ATTEMPTS = 3
STANDBY_POLL = 0.5
REPLICATION_STALL = 30


class LeaseLost(Exception):
    """Another node holds the lease now."""


class RevisionMoved(Exception):
    """The lease is still mine at a newer revision: an earlier renewal committed and its reply was lost."""

    def __init__(self, revision: int):
        super().__init__(revision)
        self.revision = revision


class Election(Protocol):
    async def acquire(self) -> int | None:
        """The term, a revision that only grows, or None if the lease is held."""

    async def renew(self, revision: int) -> int: ...

    async def release(self, revision: int) -> None: ...

    async def release_mine(self) -> None:
        """Frees a lease held under this node's name that no running process leads, e.g. after a restart."""

    async def holder(self) -> str | None: ...

    async def desired(self) -> Desired | None: ...

    async def clear_desired(self, desired: Desired) -> None: ...

    async def position(self) -> tuple[Position | None, int]: ...

    async def record_position(self, position: Position, after: int) -> int:
        """LeaseLost if someone wrote since `after`."""

    async def record(self, event: str) -> None: ...


class Replica(Protocol):
    def local(self) -> Txids: ...

    def standby(self) -> Txids: ...

    async def replicated(self) -> Txids:
        """A lower bound: the replica may hold more by now."""

    def stall_seconds(self) -> float:
        """How long the database has had changes that did not reach the replica."""

    async def ready(self) -> bool:
        """The replica's stream has a leader, so writes can commit."""

    async def restore(self, required: set[str]) -> Txids | None: ...

    async def promote_standby(self, required: set[str]) -> None: ...

    async def follow(self) -> None: ...

    async def stop_following(self) -> None: ...

    async def replicate(self) -> None: ...

    async def stop_replicating(self, graceful: bool) -> None: ...


class Service(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def running(self) -> bool:
        """Any part of it runs."""

    async def complete(self) -> bool: ...


class Dns(Protocol):
    async def point_here(self) -> None: ...


class Placement(Protocol):
    async def pull_here(self) -> None: ...


@dataclass(frozen=True)
class Settings:
    node: str
    candidate: bool
    lease_ttl: float
    tick: float
    desired_valid: float
    yield_for: float
    held: Callable[[], bool]
    standby_wait: float = 0
    position_every: float = 5
    placement_every: float = 30


class Action(Enum):
    STARTING = "starting"
    STANDBY = "standby"
    TAKING_OVER = "taking over"
    SERVING = "serving"
    HELD = "held"
    HANDING_OVER = "handing over"
    FENCED = "fenced"


@dataclass
class Status:
    action: Action = Action.STARTING
    leader: str | None = None
    term: int | None = None
    desired: str | None = None
    position: Position | None = None
    error: str | None = None
    replication_lag: float = 0


class Agent:
    def __init__(self, settings: Settings, election: Election, replica: Replica, service: Service, dns: Dns,
                 clock: Callable[[], float] = time.monotonic, wall: Callable[[], float] = time.time,
                 placement: Placement | None = None):
        self.s, self.election, self.replica, self.service, self.dns = settings, election, replica, service, dns
        self.placement = placement
        self.placed_at = float("-inf")
        self.clock, self.wall = clock, wall
        self.status = Status()
        self.term: int | None = None
        self.revision = 0
        self.confirmed_sent = 0.0
        self.position_revision = 0
        self.position_checked = float("-inf")
        self.yielded_at: float | None = None
        self.lease_may_be_from_an_earlier_run = True
        self.behind_since: float | None = None
        self.ticked_at = clock()
        self.starting_service = False
        self.lock = asyncio.Lock()

    @property
    def leading(self) -> bool:
        return self.term is not None

    def cluster_ok(self) -> bool:
        return self.status.error is None and self.clock() - self.ticked_at < 3 * self.s.tick + 2

    async def run(self) -> None:
        await self.stop_serving()
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.forever(self.tick, self.s.tick))
            tasks.create_task(self.forever(self.renew, self.s.lease_ttl / 5))
            tasks.create_task(self.forever(self.check_fence, 0.2))

    async def forever(self, step, every: float) -> None:
        while True:
            try:
                await step()
            except Exception as e:  # one failed step must not end the agent: it would stop fencing
                self.status.error = repr(e)
                log.warning("%s: %r", step.__name__, e)
            await asyncio.sleep(every)

    async def check_fence(self) -> None:
        if self.leading and fence_due(self.clock(), self.confirmed_sent, self.s.lease_ttl):
            await self.fence("the lease is running out")

    async def fence(self, why: str) -> None:
        if self.term is None:
            return
        term, self.term = self.term, None
        self.yielded_at = self.wall()
        self.status.action = Action.FENCED
        log.warning("fence (term %s): %s", term, why)
        await self.stop_serving()
        await self.try_record(f"stopped: {why}")

    async def stop_serving(self) -> None:
        try:
            await self.replica.stop_replicating(graceful=False)
        finally:
            await self.service.stop()

    async def start_service(self) -> None:
        """Holds up the tick for as long as the service takes to start; the watchdog allows for it meanwhile."""
        self.starting_service = True
        try:
            await self.service.start()
        finally:
            self.starting_service = False

    async def serving(self) -> bool:
        return await self.service.running()

    async def renew(self) -> None:
        """A late reply is retried at once, not a TTL/5 later: one slow reply must not eat the fencing margin."""
        term, revision = self.term, self.revision
        for _ in range(RENEW_ATTEMPTS):
            if term is None or self.term != term:
                return
            sent = self.clock()
            try:
                revision = await self.election.renew(revision)
            except LeaseLost:
                if self.term == term:
                    await self.fence("another node holds the lease")
                return
            except RevisionMoved as e:
                # Committed by an earlier send of unknown time: no confirmation.
                revision = e.revision
                if self.term == term:
                    self.revision = revision
                continue
            except Exception as e:
                log.warning("renew: %r", e)
                continue
            if self.term == term:
                self.revision, self.confirmed_sent = revision, sent
            return

    async def tick(self) -> None:
        async with self.lock:
            desired = await self.election.desired()
            fresh = desired_fresh(desired, self.wall(), self.s.desired_valid)
            self.status.desired = desired.node if fresh else None
            if self.leading:
                await self.lead(desired)
            else:
                await self.stand_by(desired)
        self.ticked_at = self.clock()
        self.status.error = None

    async def stand_by(self, desired: Desired | None) -> None:
        self.status.action = Action.STANDBY
        self.status.replication_lag, self.behind_since = 0, None
        if await self.service.running():
            await self.stop_serving()
        await self.replica.follow()
        self.status.leader = await self.election.holder()
        if self.status.leader == self.s.node and self.lease_may_be_from_an_earlier_run:
            await self.election.release_mine()
            self.status.leader = None
        self.lease_may_be_from_an_earlier_run = False
        if self.status.leader is not None or not self.s.candidate:
            return
        if not may_campaign(desired, self.s.node, self.wall(), self.s.desired_valid, self.yielded_at,
                            self.s.yield_for):
            return
        sent = self.clock()
        term = await self.election.acquire()
        if term is None:
            return
        self.term, self.revision, self.confirmed_sent = term, term, sent
        self.status.leader, self.status.term = self.s.node, term
        log.info("elected, term %s", term)
        try:
            await self.take_over(term, desired)
        except Exception as e:
            if self.term == term:
                await self.give_up(term, f"take-over failed: {e!r}")
            raise

    async def take_over(self, term: int, desired: Desired | None) -> None:
        self.status.action = Action.TAKING_OVER
        position, self.position_revision = await self.election.position()
        replicated = await self.replica.replicated()
        goal = target(position, replicated)
        source = takeover_source(position, self.s.node, self.replica.local(), self.replica.standby(), replicated)
        if source is Source.RESTORE and goal and self.replica.standby():
            await self.wait_for_standby(goal)
            source = takeover_source(position, self.s.node, self.replica.local(), self.replica.standby(), replicated)
        log.info("taking over from %s, goal %s", source.value, goal)
        await self.replica.stop_following()
        restored: Txids = {}
        if source is Source.STANDBY:
            await self.replica.promote_standby(set(goal or {}))
        elif source is Source.RESTORE:
            restored = await self.replica.restore(set(goal or {}))
            if restored is None or not restore_serves(goal, restored):
                await self.give_up(term, f"restore got {restored}, short of {goal}")
                return
        if self.term != term:
            return
        held = highest(goal or {}, restored, self.replica.local() if source is Source.LOCAL else {})
        if not await self.replica.ready():
            await self.give_up(term, "the replica has no stream leader yet")
            return
        if not await self.claim_position(held, term):
            await self.give_up(term, "another node recorded a position meanwhile")
            return
        if self.term != term:
            return
        await self.dns.point_here()
        await self.replica.replicate()
        await self.start_service()
        if self.term != term:
            await self.stop_serving()
            return
        self.status.action = Action.SERVING
        await self.try_record(f"became leader (term {term}, from {source.value})")
        await self.clear_request_for_me(desired)

    async def clear_request_for_me(self, desired: Desired | None) -> None:
        if not desired or desired.node != self.s.node:
            return
        try:
            await self.election.clear_desired(desired)
        except Exception as e:
            log.warning("clear desired: %r", e)

    async def wait_for_standby(self, goal: Txids) -> None:
        """A standby a poll or two behind catches up sooner than a restore from scratch."""
        for _ in range(int(self.s.standby_wait / STANDBY_POLL)):
            if reaches(goal, self.replica.standby()):
                return
            await asyncio.sleep(STANDBY_POLL)

    async def claim_position(self, txids: Txids, term: int) -> bool:
        """Writing over the last leader's position makes its late writes fail."""
        claimed = Position(txids=txids, node=self.s.node, term=term)
        try:
            self.position_revision = await self.election.record_position(claimed, self.position_revision)
        except LeaseLost:
            return False
        self.status.position = claimed
        return True

    async def give_up(self, term: int, why: str) -> None:
        self.term = None
        self.yielded_at = self.wall()
        self.status.action = Action.STANDBY
        await self.stop_serving()
        await self.try_record(f"did not take over: {why}")
        try:
            await self.election.release(self.revision)
        except Exception as e:
            log.warning("release: %r", e)
        log.error("did not take over (term %s): %s", term, why)

    async def lead(self, desired: Desired | None) -> None:
        if self.s.held():
            self.status.action = Action.HELD
            return
        to = hand_over_to(desired, self.s.node, self.wall(), self.s.desired_valid)
        if to is not None:
            await self.hand_over(to)
            return
        await self.clear_request_for_me(desired)
        if not await self.service.complete() and await self.replica.ready():
            term = self.term
            await self.replica.replicate()
            await self.start_service()
            if self.term != term:
                await self.stop_serving()
                return
        self.status.action = Action.SERVING
        stall = self.replica.stall_seconds()
        if stall > REPLICATION_STALL:
            log.warning("replication stalled for %.0fs: restarting it", stall)
            await self.replica.stop_replicating(graceful=False)
            await self.replica.replicate()
        await self.advance_position()
        behind = self.clock() - self.behind_since if self.behind_since is not None else 0
        self.status.replication_lag = max(stall, behind)
        await self.place()

    async def place(self) -> None:
        if self.placement is None or self.clock() - self.placed_at < self.s.placement_every:
            return
        self.placed_at = self.clock()
        try:
            await self.placement.pull_here()
        except Exception as e:
            log.warning("placement: %r", e)

    async def advance_position(self, now: bool = False) -> None:
        if not now and self.clock() - self.position_checked < self.s.position_every:
            return
        self.position_checked = self.clock()
        term = self.term
        replicated = await self.replica.replicated()
        if reaches(self.replica.local(), replicated):
            self.behind_since = None
        elif self.behind_since is None:
            self.behind_since = self.clock()
        current = self.status.position.txids if self.status.position else {}
        if not replicated or reaches(replicated, current) or self.term != term:
            return
        position = Position(txids=highest(current, replicated), node=self.s.node, term=term)
        try:
            self.position_revision = await self.election.record_position(position, self.position_revision)
        except LeaseLost:
            await self.fence("another node recorded a position")
            return
        self.status.position = position

    async def hand_over(self, to: str) -> None:
        self.status.action = Action.HANDING_OVER
        term = self.term
        await self.service.stop()
        await self.replica.stop_replicating(graceful=True)
        await self.advance_position(now=True)
        if self.term != term:
            return
        self.term = None
        self.yielded_at = self.wall()
        await self.election.release(self.revision)
        await self.try_record(f"handed over to {to} on request")

    async def try_record(self, event: str) -> None:
        try:
            await self.election.record(event)
        except Exception as e:
            log.warning("history: %r", e)
