"""Full block decoding and per-pack snapshot behaviour through the bridge seam."""

import asyncio
import json
import struct
from pathlib import Path

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, block, frame, settle


@pytest.mark.asyncio
async def test_pib_and_every_pic_bit_merge_without_losing_pia(tmp_path: Path) -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(frame())
        transport.chunks.put_nowait(block(4, struct.pack(">26H", *([3201] * 16 + [2931] * 10))))
        transport.chunks.put_nowait(block(1, bytes(18)))
        await settle()
        clock.advance(10)
        await settle()
        snapshot = [p for t, p, _ in sink.messages if t.endswith("/state")][-1]
        assert snapshot["voltage"] == 51.77
        assert snapshot["power"] == pytest.approx(51.77 * -12.59)
        for cell in range(1, 17):
            assert snapshot[f"cell_{cell}_voltage"] == 3.201
            assert snapshot[f"cell_{cell}_balancing"] is False
        for cell in range(1, 5):
            assert snapshot[f"cell_{cell}_temperature"] == 20.0
        assert snapshot["ambient_temperature"] == 20.0
        assert snapshot["mosfet_temperature"] == 20.0
        for bit in range(144):
            if not 48 <= bit < 64:
                assert snapshot[f"bit_{bit}"] is False
        assert snapshot["balancing_active"] is False
        # The bridge has no persistence dependency for telemetry.
        assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_pic_bits_are_lsb_first_and_changes_restart_only_that_pack_timer() -> None:
    packs = [Pack(address=1, serial="ONE"), Pack(address=2, serial="TWO")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    try:
        await settle()
        for address in (1, 2):
            transport.chunks.put_nowait(frame(address))
            transport.chunks.put_nowait(block(1, bytes(18), address))
        await settle()
        clock.advance(4)
        payload = bytearray(18)
        payload[0] = 0x80
        payload[6] = 1
        payload[17] = 0x80
        transport.chunks.put_nowait(block(1, bytes(payload)))
        await settle()
        snapshots = [(t, p) for t, p, _ in sink.messages if t.endswith("/state")]
        assert len(snapshots) == 1
        assert snapshots[0][0] == "bms_flux/ONE/state"
        assert snapshots[0][1]["bit_7"] is True
        assert snapshots[0][1]["bit_0"] is False
        assert snapshots[0][1]["bit_143"] is True
        assert snapshots[0][1]["cell_1_balancing"] is True
        assert snapshots[0][1]["balancing_active"] is True
        transport.chunks.put_nowait(block(1, bytes(payload)))
        await settle()
        assert len([t for t, _, _ in sink.messages if t.endswith("/state")]) == 1
        clock.advance(6)
        await settle()
        assert [t for t, _, _ in sink.messages if t.endswith("/state")] == [
            "bms_flux/ONE/state",
            "bms_flux/TWO/state",
        ]
        clock.advance(4)
        await settle()
        assert [t for t, _, _ in sink.messages if t.endswith("/state")][-1] == "bms_flux/ONE/state"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_discovery_includes_measurements_and_diagnostic_balancing() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    try:
        await settle()
        components = sink.messages[0][1]["components"]
        assert isinstance(components, dict)
        assert components["lifetime_discharge"]["state_class"] == "total_increasing"
        for name in (
            "power",
            "soh",
            "remaining_capacity",
            "full_capacity",
            "cycles",
            "max_temperature",
            "min_temperature",
            "ambient_temperature",
            "mosfet_temperature",
        ):
            assert components[name]["platform"] == "sensor"
        for name in ["avg_cell_voltage", "max_cell_voltage", "min_cell_voltage"] + [
            f"cell_{i}_voltage" for i in range(1, 17)
        ]:
            assert components[name]["suggested_display_precision"] == 3
            assert components[name]["unit_of_measurement"] == "V"
        for cell in range(1, 17):
            component = components[f"cell_{cell}_balancing"]
            assert component["platform"] == "binary_sensor"
            assert component["enabled_by_default"] is False
            assert component["entity_category"] == "diagnostic"
        assert (
            components["events"]["json_attributes_template"]
            == "{{ value_json.event_flags | tojson }}"
        )
        assert components["system_state"]["device_class"] == "enum"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_raw_probe_playback_has_all_pib_and_pic_fields_for_both_packs() -> None:
    packs = [Pack(address=1, serial="ONE"), Pack(address=2, serial="TWO")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    try:
        await settle()
        raw = await asyncio.to_thread(Path("probe/fixtures/sniff-19200.raw.jsonl").read_text)
        chunks = [json.loads(line) for line in raw.splitlines()]
        start = float(chunks[0]["t"])
        previous = start
        for chunk in chunks:
            timestamp = float(chunk["t"])
            if timestamp - start > 0.4:
                break
            clock.advance(timestamp - previous)
            transport.chunks.put_nowait(bytes.fromhex(chunk["hex"]))
            await settle()
            previous = timestamp
        clock.advance(10)
        await settle()
        for serial, first_cell, ambient, mosfet in (
            ("ONE", 3.237, 24.6, 22.9),
            ("TWO", 3.241, 22.9, 20.7),
        ):
            snapshot = [p for t, p, _ in sink.messages if t == f"bms_flux/{serial}/state"][-1]
            assert snapshot["cell_1_voltage"] == first_cell
            assert snapshot["ambient_temperature"] == ambient
            assert snapshot["mosfet_temperature"] == mosfet
            assert snapshot["system_state"] == "discharge"
            assert snapshot["fet_state"] == 0x43
            assert snapshot["charge_fet"] is True
            assert snapshot["discharge_fet"] is True
            assert snapshot["current_limit_fet"] is False
            assert set(snapshot) >= {
                "voltage",
                "current",
                "power",
                "reported_soc",
                "soh",
                "remaining_capacity",
                "full_capacity",
                "cycles",
                "lifetime_discharge",
                "avg_cell_voltage",
                "max_cell_voltage",
                "min_cell_voltage",
                "cell_delta",
                "max_temperature",
                "min_temperature",
                "suggested_charge_current",
                "suggested_discharge_current",
                "precharge",
                "alarm",
                "protection",
                "events",
                "event_flags",
                "balancing_active",
            }
            for name in ("lifetime_discharge", "cell_delta"):
                value = snapshot[name]
                assert isinstance(value, int)
                assert value > 0
            for cell in range(1, 17):
                value = snapshot[f"cell_{cell}_voltage"]
                assert isinstance(value, float)
                assert 3.2 < value < 3.3
                assert snapshot[f"cell_{cell}_balancing"] is False
            for cell in range(1, 5):
                value = snapshot[f"cell_{cell}_temperature"]
                assert isinstance(value, float)
                assert 19 < value < 23
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_corrupt_pib_and_pic_cannot_change_snapshot_or_trigger_publication() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(frame())
        transport.chunks.put_nowait(block(1, bytes(18)))
        await settle()
        for function, payload in ((4, bytes([255] * 52)), (1, bytes([255] * 18))):
            corrupt = bytearray(block(function, payload))
            corrupt[-1] ^= 1
            transport.chunks.put_nowait(bytes(corrupt))
        await settle()
        assert not [t for t, _, _ in sink.messages if t.endswith("/state")]
        clock.advance(10)
        await settle()
        snapshot = [p for t, p, _ in sink.messages if t.endswith("/state")][-1]
        assert "cell_1_voltage" not in snapshot
        assert snapshot["bit_0"] is False
        assert snapshot["events"] == 0
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
@pytest.mark.parametrize("active_bit", range(144))
async def test_each_pic_coil_survives_as_one_lsb_first_flag(active_bit: int) -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    try:
        await settle()
        payload = bytearray(18)
        payload[active_bit // 8] = 1 << (active_bit % 8)
        transport.chunks.put_nowait(block(1, bytes(payload)))
        await settle()
        clock.advance(10)
        await settle()
        snapshot = [p for t, p, _ in sink.messages if t.endswith("/state")][-1]
        for bit in range(144):
            name = f"cell_{bit - 47}_balancing" if 48 <= bit < 64 else f"bit_{bit}"
            assert snapshot[name] is (bit == active_bit)
        assert snapshot["balancing_active"] is (48 <= active_bit < 64)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
