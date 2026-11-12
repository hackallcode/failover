"""The service as docker compose projects, started in order and stopped in reverse."""
import asyncio
import logging
from pathlib import Path

log = logging.getLogger(__name__)

COMPOSE_TIMEOUT = 120


async def compose(directory: Path, *args: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec("docker", "compose", *args, cwd=directory,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), COMPOSE_TIMEOUT)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return -1, ""
    if proc.returncode:
        log.warning("docker compose %s in %s: %s", args[0], directory, err.decode().strip()[-500:])
    return proc.returncode, out.decode()


class ComposeService:
    def __init__(self, projects: list[Path]):
        self.projects = projects

    async def start(self) -> None:
        for project in self.projects:
            code, _ = await compose(project, "up", "-d")
            if code:
                raise RuntimeError(f"{project} did not start")

    async def stop(self) -> None:
        failed = [p for p in reversed(self.projects) if (await compose(p, "stop", "--timeout", "1"))[0]]
        if failed:
            raise RuntimeError(f"could not stop {failed}")

    async def running(self) -> bool:
        for project in self.projects:
            code, out = await compose(project, "ps", "--status", "running", "--quiet")
            if code or out.strip():
                return True  # unknown counts as running: the caller stops it
        return False

    async def complete(self) -> bool:
        for project in self.projects:
            code, out = await compose(project, "ps", "--status", "running", "--quiet")
            if code or not out.strip():
                return False
        return True
