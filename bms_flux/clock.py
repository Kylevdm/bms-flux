"""Provide the bridge's controllable clock seam and the real asyncio clock."""

import asyncio
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
