"""Named protocol fields and urgent snapshots exercised through the bridge seam."""

import asyncio
import struct

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import (
    FakeTransport,
    ManualClock,
    MemoryMQTT,
    block,
    frame,
    pack_snapshots,
    settle,
)


@pytest.mark.asyncio
async def test_pia_diagnostics_have_vendor_scales_and_discovery() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        registers = [
            5200,
            100,
            20000,
            28000,
            42,
            700,
            980,
            123,
            3250,
            2931,
            3300,
            3200,
            2981,
            2881,
            0,
            150,
            100,
            999,
        ]
        transport.chunks.put_nowait(block(4, struct.pack(">18H", *registers)))
        transport.chunks.put_nowait(block(4, bytes(52)) + block(1, bytes(18)))
        await settle()
        clock.advance(10)
        await settle()
        snapshot = pack_snapshots(sink)[-1][1]
        assert (
            snapshot.items()
            >= {
                "suggested_discharge_current": 150,
                "suggested_charge_current": 100,
                "precharge": 99.9,
                "max_temperature": 25.0,
                "min_temperature": 15.0,
                "max_cell_voltage": 3.3,
                "min_cell_voltage": 3.2,
                "cell_delta": 100,
                "lifetime_discharge": 420,
            }.items()
        )
        components = sink.messages[0][1]["components"]
        assert isinstance(components, dict)
        for name, unit in (
            ("suggested_discharge_current", "A"),
            ("suggested_charge_current", "A"),
            ("precharge", "%"),
        ):
            assert components[name]["enabled_by_default"] is False
            assert components[name]["entity_category"] == "diagnostic"
            assert components[name]["unit_of_measurement"] == unit
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "byte, mask, name, alarm, protection",
    [
        (2, 64, "cell_7_high_voltage", True, False),
        (4, 128, "cell_8_low_temperature", True, False),
        (9, 2, "cell_overvoltage_protection", False, True),
        (10, 1, "charge_high_temperature_alarm", True, False),
        (11, 32, "mosfet_high_temperature_protection", False, True),
        (12, 64, "output_short_circuit", False, True),
        (13, 4, "second_level_charge_latch", False, True),
        (14, 8, "soc_protection", False, True),
        (16, 32, "under_soc_protection", False, True),
        (17, 128, "aerosol_fault", False, True),
    ],
)
async def test_named_events_and_rollups(
    byte: int, mask: int, name: str, alarm: bool, protection: bool
) -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(frame())
        transport.chunks.put_nowait(block(4, bytes(52)) + block(1, bytes(18)))
        await settle()
        clock.advance(3)
        payload = bytearray(18)
        payload[byte] = mask
        transport.chunks.put_nowait(block(1, bytes(payload)))
        await settle()
        snapshot = pack_snapshots(sink)[-1][1]
        assert snapshot[name] is True
        assert snapshot["alarm"] is alarm
        assert snapshot["protection"] is protection
        assert snapshot["events"] == 1
        assert snapshot["event_flags"] == {name: True}
        assert snapshot["voltage"] == 51.77
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
