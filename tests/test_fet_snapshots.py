"""FET changes publish merged snapshots without waiting for the interval."""

import asyncio
import struct

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, block, frame, settle


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mask, name", [(1, "discharge_fet"), (2, "charge_fet"), (4, "current_limit_fet")]
)
async def test_fet_changes_publish_full_snapshot_and_clear(mask: int, name: str) -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    try:
        await settle()
        components = sink.messages[0][1]["components"]
        assert isinstance(components, dict)
        for entity in (name, "alarm", "protection", "balancing_active", "cell_1_balancing"):
            assert components[entity]["payload_on"] == "ON"
            assert components[entity]["payload_off"] == "OFF"
        for entity in ("alarm", "protection"):
            assert components[entity]["device_class"] == "problem"
        transport.chunks.put_nowait(frame())
        transport.chunks.put_nowait(block(4, struct.pack(">26H", *([3200] * 16 + [2931] * 10))))
        transport.chunks.put_nowait(block(1, bytes(18)))
        await settle()
        clock.advance(2)
        payload = bytearray(18)
        payload[15] = mask
        transport.chunks.put_nowait(block(1, bytes(payload)))
        await settle()
        snapshots = [p for t, p, _ in sink.messages if t.endswith("/state")]
        assert len(snapshots) == 1
        assert snapshots[0][name] is True
        assert snapshots[0]["cell_16_voltage"] == 3.2
        assert snapshots[0]["voltage"] == 51.77
        clock.advance(1)
        transport.chunks.put_nowait(block(1, bytes(18)))
        await settle()
        snapshots = [p for t, p, _ in sink.messages if t.endswith("/state")]
        assert len(snapshots) == 2
        assert snapshots[-1][name] is False
        clock.advance(7)
        await settle()
        assert len([t for t, _, _ in sink.messages if t.endswith("/state")]) == 2
        clock.advance(3)
        await settle()
        assert len([t for t, _, _ in sink.messages if t.endswith("/state")]) == 3
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_balancing_and_mode_changes_do_not_trigger_urgent_snapshot() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(block(1, bytes(18)))
        await settle()
        payload = bytearray(18)
        payload[6] = 1
        payload[8] = 2
        payload[16] = 2
        transport.chunks.put_nowait(block(1, bytes(payload)))
        await settle()
        assert not [t for t, _, _ in sink.messages if t.endswith("/state")]
        clock.advance(10)
        await settle()
        snapshot = [p for t, p, _ in sink.messages if t.endswith("/state")][-1]
        assert snapshot["system_state"] == "charge"
        assert snapshot["balancing_active"] is True
        assert snapshot["alarm"] is False
        assert snapshot["protection"] is False
        assert snapshot["event_flags"] == {"intermittent_charge": True}
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
