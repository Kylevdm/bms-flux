"""Exercise the bridge with byte chunks, a manual clock and an MQTT sink."""

import asyncio
import json
from pathlib import Path

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, frame, settle


@pytest.mark.asyncio
async def test_pia_becomes_retained_discovery_and_ten_second_snapshot() -> None:
    packs = [Pack(address=1, serial="SP112B2410100228")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock)
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        transport.chunks.put_nowait(frame())
        await settle()
        assert not [m for m in sink.messages if m[0].endswith("/state")]
        clock.advance(10)
        await settle()
        snapshots = [m for m in sink.messages if m[0].endswith("/state")]
        assert len(snapshots) == 1
        assert snapshots[0][0] == "bms_flux/SP112B2410100228/state"
        assert snapshots[0][2] is False
        assert (
            snapshots[0][1].items()
            >= {
                "voltage": 51.77,
                "current": -12.59,
                "remaining_capacity": 87.64,
                "full_capacity": 280.0,
                "reported_soc": 31.3,
                "soh": 98.2,
                "cycles": 177,
                "avg_cell_voltage": 3.235,
            }.items()
        )
        topic, discovery, retained = sink.messages[0]
        assert topic == "homeassistant/device/bms_flux_SP112B2410100228/config"
        assert retained
        assert discovery["device"] == {
            "identifiers": ["bms_flux_SP112B2410100228"],
            "name": "SP112B2410100228",
            "manufacturer": "Seplos",
            "model": "V3",
            "serial_number": "SP112B2410100228",
        }
        components = discovery["components"]
        assert isinstance(components, dict)
        assert set(components) >= {"voltage", "current", "reported_soc"}
        for name in ("voltage", "current", "reported_soc"):
            component = components[name]
            assert component["unique_id"] == f"bms_flux_SP112B2410100228_{name}"
            assert component["default_entity_id"] == f"sensor.bms_flux_sp112b2410100228_{name}"
            assert component["value_template"] == "{{ value_json." + name + " }}"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_bad_crc_and_incomplete_junk_resync_without_publishing_unmapped_packs() -> None:
    packs = [Pack(address=1, serial="SP112B2410100228")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    try:
        await settle()
        broken = bytearray(frame(current=300))
        broken[-1] ^= 1
        transport.chunks.put_nowait(bytes(broken))
        transport.chunks.put_nowait(frame(address=2))
        await settle()
        clock.advance(10)
        await settle()
        assert not [m for m in sink.messages if m[0].endswith("/state")]
        # A plausible but incomplete response in noise must not hide a later valid frame.
        transport.chunks.put_nowait(b"\x01\x04\xfa" + frame(current=425)[:7])
        await settle()
        transport.chunks.put_nowait(frame(current=425)[7:])
        await settle()
        clock.advance(10)
        await settle()
        snapshots = [m for m in sink.messages if m[0].endswith("/state")]
        assert len(snapshots) == 1
        assert snapshots[0][1]["current"] == 4.25
        assert all("SP18B2311040070" not in topic for topic, _, _ in sink.messages)
        clock.advance(10)
        await settle()
        assert len([m for m in sink.messages if m[0].endswith("/state")]) == 2
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_raw_probe_chunks_publish_both_packs_with_probe_values() -> None:
    packs = [
        Pack(address=1, serial="SP112B2410100228"),
        Pack(address=2, serial="SP18B2311040070"),
    ]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(packs), sink, packs, clock).run())
    raw = await asyncio.to_thread(Path("probe/fixtures/sniff-19200.raw.jsonl").read_text)
    chunks = [json.loads(line) for line in raw.splitlines()]
    try:
        await settle()
        previous = float(chunks[0]["t"])
        # The first two PIA responses are before the next 200 ms bus cycle.
        first_cycle = True
        start = previous
        for chunk in chunks:
            timestamp = float(chunk["t"])
            if first_cycle and timestamp - start >= 0.20:
                clock.advance(10)
                await settle()
                first_cycle = False
            clock.advance(timestamp - previous)
            transport.chunks.put_nowait(bytes.fromhex(chunk["hex"]))
            await settle()
            previous = timestamp
        clock.advance(10)
        await settle()
        for pack, expected in zip(
            packs,
            [
                {
                    "voltage": 51.77,
                    "current": -12.59,
                    "remaining_capacity": 87.64,
                    "full_capacity": 280.0,
                    "reported_soc": 31.3,
                    "soh": 98.2,
                    "cycles": 177,
                    "avg_cell_voltage": 3.235,
                },
                {
                    "voltage": 51.78,
                    "current": -10.61,
                    "remaining_capacity": 106.96,
                    "full_capacity": 280.0,
                    "reported_soc": 38.2,
                    "soh": 94.6,
                    "cycles": 535,
                    "avg_cell_voltage": 3.236,
                },
            ],
            strict=True,
        ):
            snapshots = [
                payload
                for topic, payload, _ in sink.messages
                if topic == f"bms_flux/{pack.serial}/state"
            ]
            assert len(snapshots) >= 3
            assert snapshots[0].items() >= expected.items()
            for snapshot in snapshots:
                assert snapshot["reported_soc"] == expected["reported_soc"]
                assert snapshot["cycles"] == expected["cycles"]
                assert snapshot["full_capacity"] == 280.0
        transport.chunks.put_nowait(b"")
        await task
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
