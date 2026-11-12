"""The service as one child process of the agent."""
import asyncio


class ProcessService:
    def __init__(self, command: list[str]):
        self.command = command
        self.proc: asyncio.subprocess.Process | None = None

    async def start(self) -> None:
        if not await self.running():
            self.proc = await asyncio.create_subprocess_exec(*self.command)

    async def stop(self) -> None:
        if await self.running():
            self.proc.kill()  # it must not write once the lease may be gone
            await self.proc.wait()

    async def running(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    async def complete(self) -> bool:
        return await self.running()
