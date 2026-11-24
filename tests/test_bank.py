"""Bus and bank behaviour through the byte-chunk bridge seam."""

import asyncio
import struct
from pathlib import Path
from typing import Any

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, block, settle


def full_set(address: int = 1, *, soc: int = 700, capacity: int = 28000) -> bytes:
    registers = [5200, 100, 20000, capacity, 50, soc, 980, 100] + [0] * 10
    return (
        block(4, struct.pack(">18H", *registers), address)
        + block(4, bytes(52), address)
        + block(1, bytes(18), address)
    )


def values(sink: MemoryMQTT, topic: str) -> list[Any]:
    return [p for t, p, _ in sink.messages if t == topic]


@pytest.mark.asyncio
async def test_zero_full_capacity_keeps_bank_unavailable() -> None:
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(
        Bridge(transport, SeplosV3Decoder(), sink, [Pack(address=1, serial="A")], clock).run()
    )
    try:
        await settle()
        transport.chunks.put_nowait(full_set(capacity=0))
        await settle()
        clock.advance(1)
        await settle()
        assert values(sink, "bms_flux/A/link")[-1] == "ok"
        assert values(sink, "bms_flux/bank/availability") == ["offline"]
        assert not values(sink, "bms_flux/bank/state")
        transport.chunks.put_nowait(full_set())
        await settle()
        assert values(sink, "bms_flux/bank/availability") == ["offline", "online"]
        assert values(sink, "bms_flux/bank/state")[-1]["reported_soc"] == 70
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_bank_discovery_and_complete_capacity_weighted_snapshot(tmp_path: Path) -> None:
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    packs = [Pack(address=1, serial="A"), Pack(address=2, serial="B")]
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        config = values(sink, "homeassistant/device/bms_flux_bridge/config")[-1]
        for name in ("bank_current", "bank_power", "reported_soc", "soc_spread"):
            assert config["components"][name]["availability_mode"] == "all"
        assert (
            values(sink, "homeassistant/device/bms_flux_A/config")[-1]["device"]["via_device"]
            == "bms_flux_bridge"
        )
        assert values(sink, "bms_flux/bank/availability") == ["offline"]
        transport.chunks.put_nowait(full_set())
        await settle()
        assert not values(sink, "bms_flux/bank/state")
        transport.chunks.put_nowait(full_set(2, soc=900, capacity=14000))
        await settle()
        snapshot = values(sink, "bms_flux/bank/state")[-1]
        assert snapshot == pytest.approx(
            {"bank_current": 2, "bank_power": 104, "reported_soc": 230 / 3, "soc_spread": 20}
        )
        clock.advance(10)
        await settle()
        assert values(sink, "bms_flux/bank/availability")[-1] == "offline"
        count = len(values(sink, "bms_flux/bank/state"))
        transport.chunks.put_nowait(full_set())
        await settle()
        assert len(values(sink, "bms_flux/bank/state")) == count
        transport.chunks.put_nowait(full_set(2))
        await settle()
        assert values(sink, "bms_flux/bank/availability")[-1] == "online"
        # A fingerprint mismatch also invalidates the whole bank, not just one pack.
        registers = [5200, 100, 20000, 28000, 49, 700, 980, 99] + [0] * 10
        transport.chunks.put_nowait(block(4, struct.pack(">18H", *registers)))
        await settle()
        assert values(sink, "bms_flux/bank/availability")[-1] == "offline"
        assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
