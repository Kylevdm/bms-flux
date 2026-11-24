"""Byte-chunk bridge test doubles and CRC-valid frame builders."""

import asyncio
import json
import struct


class ManualClock:
    def __init__(self) -> None:
        self.time = 0.0
        self.waiters: list[tuple[float, asyncio.Future[None]]] = []

    def now(self) -> float:
        return self.time

    async def sleep(self, seconds: float) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.waiters.append((self.time + seconds, future))
        await future

    def advance(self, seconds: float) -> None:
        self.time += seconds
        for deadline, future in self.waiters:
            if deadline <= self.time and not future.done():
                future.set_result(None)


class FakeTransport:
    def __init__(self) -> None:
        self.chunks: asyncio.Queue[bytes] = asyncio.Queue()

    async def read(self) -> bytes:
        return await self.chunks.get()


class MemoryMQTT:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, object], bool]] = []

    async def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        self.messages.append((topic, json.loads(payload), retain))


def frame(address: int = 1, *, current: int = -1259) -> bytes:
    registers = [5177, current & 0xFFFF, 8764, 28000, 0, 313, 982, 177, 3235] + [0] * 9
    return block(4, struct.pack(">18H", *registers), address)


def block(function: int, payload: bytes, address: int = 1) -> bytes:
    data = bytes((address, function, len(payload))) + payload
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return data + crc.to_bytes(2, "little")


async def settle() -> None:
    for _ in range(30):
        await asyncio.sleep(0)
