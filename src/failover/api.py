"""HTTP API for the panel: GET /state, POST /takeover (a request for this node to lead), bearer token."""
import asyncio
import hmac
import json
import logging
import time
from dataclasses import asdict
from typing import Protocol

from failover.agent import Agent
from failover.domain import Desired, Position

log = logging.getLogger(__name__)


class Shared(Protocol):
    async def position(self) -> tuple[Position | None, int]: ...

    async def history(self) -> list[dict]: ...

    async def request(self, desired: Desired) -> None: ...


def authorized(header: str | None, token: str) -> bool:
    return bool(token) and hmac.compare_digest(header or "", f"Bearer {token}")


async def position(agent: Agent, election: Shared) -> dict | None:
    """From KV, not this node's view: a standby has none of its own."""
    try:
        recorded, _ = await election.position()
    except Exception as e:
        log.warning("position: %r", e)
        recorded = agent.status.position
    return asdict(recorded) if recorded else None


async def history(election: Shared) -> list[dict]:
    """The state answers without it when NATS is slow."""
    try:
        return await election.history()
    except Exception as e:
        log.warning("history: %r", e)
        return []


class Api:
    def __init__(self, agent: Agent, election: Shared, service: str, token: str, address: str, node: str,
                 candidate: bool):
        self.agent, self.election, self.service, self.token, self.address = agent, election, service, token, address
        self.node, self.candidate = node, candidate

    async def state(self) -> dict:
        s = self.agent.status
        return {"service": self.service, "node": self.node, "address": self.address, "candidate": self.candidate,
                "cluster_ok": self.agent.cluster_ok(),
                "action": s.action.value, "leader": s.leader, "term": s.term, "desired": s.desired,
                "position": await position(self.agent, self.election), "error": s.error,
                "serving": await self.agent.serving(), "history": await history(self.election)}

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = (await reader.readline()).decode().split()
            headers = {}
            while (line := (await reader.readline()).decode().strip()):
                name, _, value = line.partition(":")
                headers[name.strip().lower()] = value.strip()
            code, body = await self.route(request[:2], headers.get("authorization"))
        except Exception as e:
            log.warning("api: %r", e)
            code, body = 500, {"error": "internal error"}
        data = json.dumps(body).encode()
        writer.write(f"HTTP/1.1 {code} -\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\n"
                     f"Connection: close\r\n\r\n".encode() + data)
        try:
            await writer.drain()
        finally:
            writer.close()

    async def route(self, request: list[str], authorization: str | None) -> tuple[int, dict]:
        if request not in (["GET", "/state"], ["POST", "/takeover"]):
            return 404, {"error": "not found"}
        if not authorized(authorization, self.token):
            return 401, {"error": "unauthorized"}
        if request[0] == "GET":
            return 200, await self.state()
        await self.election.request(Desired(node=self.node, at=time.time()))
        return 202, {"takeover_requested": True}

    async def serve(self, listen: str) -> None:
        """The tailnet address may come up after the agent: keep trying to bind."""
        host, _, port = listen.rpartition(":")
        while True:
            try:
                server = await asyncio.start_server(self.handle, host, int(port))
                break
            except OSError as e:
                log.warning("api bind %s: %r", listen, e)
                await asyncio.sleep(5)
        async with server:
            await server.serve_forever()
