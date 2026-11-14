"""Composition root and command line: failover {run,stop,state,request} <config.json> [node]."""
import argparse
import asyncio
import json
import logging
import os
import socket
import ssl
import time
import urllib.request
from pathlib import Path

import nats
from nats.js.errors import ServiceUnavailableError

from failover.agent import Action, Agent, Settings
from failover.api import Api
from failover.cloudflare import CloudflareRecord, Records
from failover.compose import ComposeService
from failover.process import ProcessService
from failover.config import Config, load
from failover.domain import Desired
from failover.snapshots import RESTORE_TIMEOUT, Database, SnapshotReplica
from failover.nats_election import NatsElection, StreamLeaders

log = logging.getLogger("failover")

# A hold file older than this was left behind by a dead deploy and no longer counts.
HOLD_MAX = 300
# A standby receives each snapshot as it is written: a few seconds cover one in flight.
STANDBY_WAIT = 5


class HttpRecord:
    """PUT <url>/<name> with the address: a stand-in DNS for tests."""

    def __init__(self, url: str, name: str, ip: str):
        self.url, self.name, self.ip = url, name, ip

    async def point_here(self) -> None:
        req = urllib.request.Request(f"{self.url}/{self.name}", data=self.ip.encode(), method="PUT")
        await asyncio.to_thread(lambda: urllib.request.urlopen(req, timeout=5).close())


def dns(config: Config) -> Records:
    d = config.dns
    if d.provider == "cloudflare":
        token = d.token_file.read_text().strip()
        return Records([CloudflareRecord(token, d.zone, name, d.ip) for name in d.names])
    return Records([HttpRecord(d.url, name, d.ip) for name in d.names])


def held(config: Config):
    def check() -> bool:
        try:
            return config.hold_file is not None and time.time() - config.hold_file.stat().st_mtime < HOLD_MAX
        except OSError:
            return False
    return check


async def connect(config: Config):
    options = {}
    if config.nats.user:
        options.update(user=config.nats.user, password=config.nats.password_file.read_text().strip())
    if config.nats.tls:
        context = ssl.create_default_context(cafile=str(config.nats.tls.ca))
        context.load_cert_chain(str(config.nats.tls.cert), str(config.nats.tls.key))
        options.update(tls=context)
    nc = await nats.connect(servers=config.nats.servers, dont_randomize=True, connect_timeout=2,
                            max_reconnect_attempts=-1, reconnect_time_wait=1, ping_interval=2, max_outstanding_pings=2,
                            **options)
    return nc, nc.jetstream(timeout=2)


async def open_election(config: Config, js) -> NatsElection:
    return await NatsElection.open(js, config.service, config.node, config.lease_ttl, config.nats.replicas)


def notify(message: str) -> None:
    """sd_notify without libsystemd: READY=1 and WATCHDOG=1 for a Type=notify unit with WatchdogSec."""
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
        s.sendto(message.encode(), address)


def metrics(agent: Agent, config: Config) -> str:
    s, labels = agent.status, f'{{service="{config.service}"}}'
    values = {"failover_leader": int(s.leader == config.node and s.action in (Action.SERVING, Action.HELD)),
              "failover_has_leader": int(s.leader is not None),
              "failover_error": int(bool(s.error)),
              "failover_replication_lag_seconds": round(s.replication_lag),
              "failover_last_tick_timestamp_seconds": int(time.time())}
    for db, txid in (s.position.txids if s.position else {}).items():
        values[f'failover_position_txid{{service="{config.service}",database="{db}"}}'] = txid
    return "".join(f"{k if '{' in k else k + labels} {v}\n" for k, v in values.items())


def stalled_after(agent: Agent, config: Config) -> float:
    seconds = max(10 * config.tick, config.lease_ttl / 2)
    if agent.status.action is Action.TAKING_OVER:
        seconds += RESTORE_TIMEOUT * len(config.databases)
    if agent.starting_service:
        seconds += config.start_timeout * len(config.projects)
    return seconds


async def supervise(agent: Agent, config: Config) -> None:
    """Report liveness only while ticks complete, so a hung agent is killed and its service stopped."""
    notify("READY=1")
    while True:
        if time.monotonic() - agent.ticked_at < stalled_after(agent, config):
            notify("WATCHDOG=1")
        if config.metrics_dir:
            path = config.metrics_dir / f"failover_{config.service}.prom"
            try:
                tmp = path.with_suffix(".tmp")
                tmp.write_text(metrics(agent, config))
                os.replace(tmp, path)
            except OSError as e:
                log.warning("metrics: %r", e)
        await asyncio.sleep(config.tick)


async def run(config: Config) -> None:
    nc, js = await connect(config)
    while True:
        try:
            election = await open_election(config, js)
            replica = SnapshotReplica(js, election.kv, NatsElection.BUCKET, config.service,
                                      [Database(name, path) for name, path in config.databases.items()])
            break
        except Exception as e:  # the cluster may still be electing its leaders
            log.warning("setup: %r", e)
            await asyncio.sleep(2)
    settings = Settings(node=config.node, candidate=config.candidate, lease_ttl=config.lease_ttl, tick=config.tick,
                        desired_valid=config.desired_valid, yield_for=config.yield_for, held=held(config),
                        standby_wait=STANDBY_WAIT)
    agent = Agent(settings, election, replica, service(config), dns(config),
                  placement=StreamLeaders(nc, election.stream, config.nats.server_name or config.node)
                  if config.place_streams else None)
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(agent.run())
        tasks.create_task(supervise(agent, config))
        if config.api:
            api = Api(agent, election, config.service, config.api.token_file.read_text().strip(), config.dns.ip,
                      config.node, config.candidate)
            tasks.create_task(api.serve(config.api.listen))


def service(config: Config) -> ComposeService | ProcessService:
    return ProcessService(config.command) if config.command else ComposeService(config.projects, config.start_timeout)


async def stop(config: Config) -> None:
    await service(config).stop()


CLI_ATTEMPTS = 5
TRANSIENT = (nats.errors.TimeoutError, nats.errors.NoRespondersError, ServiceUnavailableError)


async def retried(action):
    """A one-off command meets the cluster mid-election as often as the agent does."""
    for attempt in range(CLI_ATTEMPTS):
        try:
            return await action()
        except TRANSIENT:
            if attempt == CLI_ATTEMPTS - 1:
                raise
            await asyncio.sleep(1)


async def read_state(js, config: Config) -> dict:
    election = await NatsElection.attach(js, config.service, config.node)
    position, _ = await election.position()
    desired = await election.desired()
    return {"leader": await election.holder(), "desired": desired.node if desired else None,
            "position": position.txids if position else None, "history": await election.history()}


async def show(config: Config) -> None:
    nc, js = await connect(config)
    try:
        print(json.dumps(await retried(lambda: read_state(js, config)), indent=2))
    finally:
        await nc.close()


async def request(config: Config, node: str) -> None:
    nc, js = await connect(config)

    async def ask() -> None:
        election = await NatsElection.attach(js, config.service, config.node)
        await election.request(Desired(node=node, at=time.time()))

    try:
        await retried(ask)
    finally:
        await nc.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="failover")
    parser.add_argument("command", choices=["run", "stop", "state", "request"])
    parser.add_argument("config", type=Path)
    parser.add_argument("node", nargs="?")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    config = load(args.config)
    if args.command == "request" and not args.node:
        parser.error("request needs the node to lead")
    commands = {"run": lambda: run(config), "stop": lambda: stop(config), "state": lambda: show(config),
                "request": lambda: request(config, args.node)}
    asyncio.run(commands[args.command]())
