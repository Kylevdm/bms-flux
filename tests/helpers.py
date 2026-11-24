"""Byte-chunk bridge test doubles and CRC-valid frame builders."""

import asyncio
import json
import struct
from typing import Any

from bms_flux.clock import Clock


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
        self.messages: list[tuple[str, Any, bool]] = []
        self.retained: list[tuple[str, str]] = []
        self.subscriptions: list[str] = []
        self.qos: list[int] = []

    async def read_retained(self, topic_filter: str, clock: Clock) -> list[tuple[str, str]]:
        self.subscriptions.append(topic_filter)
        return self.retained.copy()

    async def publish(
        self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
    ) -> None:
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            decoded = payload
        self.messages.append((topic, decoded, retain))
        self.qos.append(qos)


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


def pack_snapshots(sink: MemoryMQTT) -> list[tuple[str, Any, bool]]:
    """Select pack snapshots, excluding bus and bank state topics."""
    return [
        message
        for message in sink.messages
        if message[0].endswith("/state") and message[0].split("/")[1] not in ("bus", "bank")
    ]


def discard_snapshots(sink: MemoryMQTT) -> None:
    """Discard startup recovery snapshots before asserting later publications."""
    snapshots = pack_snapshots(sink)
    sink.messages[:] = [message for message in sink.messages if message not in snapshots]


async def settle() -> None:
    for _ in range(30):
        await asyncio.sleep(0)
