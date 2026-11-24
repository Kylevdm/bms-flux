"""Pack identity and link state exercised through timestamped bridge byte chunks."""

import asyncio
import struct
from pathlib import Path

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, block, pack_snapshots, settle


def pia(cycles: int = 100, discharge: int = 50, address: int = 1) -> bytes:
    registers = [5200, 100, 20000, 28000, discharge, 700, 980, cycles] + [0] * 10
    return block(4, struct.pack(">18H", *registers), address)


def full_set(address: int = 1) -> bytes:
    return pia(address=address) + block(4, bytes(52), address) + block(1, bytes(18), address)


def payloads(sink: MemoryMQTT, topic: str) -> list[object]:
    return [p for t, p, _ in sink.messages if t == topic]


@pytest.mark.asyncio
async def test_startup_silent_and_recovery_snapshot(tmp_path: Path) -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        assert payloads(sink, "bms_flux/PACK/link") == ["silent"]
        assert payloads(sink, "bms_flux/PACK/availability") == ["offline"]
        transport.chunks.put_nowait(pia() + block(4, bytes(52)))
        await settle()
        assert not payloads(sink, "bms_flux/PACK/state")
        transport.chunks.put_nowait(block(1, bytes(18)))
        await settle()
        assert payloads(sink, "bms_flux/PACK/link") == ["silent", "ok"]
        assert payloads(sink, "bms_flux/PACK/availability") == ["offline", "online"]
        snapshot = payloads(sink, "bms_flux/PACK/state")[-1]
        assert isinstance(snapshot, dict)
        assert snapshot["cycles"] == 100
        assert snapshot["cell_1_voltage"] == 0.0
        assert snapshot["alarm"] is False
        assert payloads(sink, "bms_flux/status") == ["online"]
        assert all(r for t, _, r in sink.messages if t.endswith(("/availability", "/status")))
        assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
@pytest.mark.parametrize("cycles, discharge", [(99, 50), (100, 49)])
async def test_counter_fall_latches_mismatch_and_never_publishes_wrong_serial(
    cycles: int, discharge: int
) -> None:
    packs = [Pack(address=1, serial="PACK"), Pack(address=2, serial="OTHER")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(full_set() + full_set(2))
        await settle()
        baseline = payloads(sink, "bms_flux/PACK/state")
        assert len(baseline) == 1
        transport.chunks.put_nowait(pia(cycles, discharge))
        await settle()
        assert payloads(sink, "bms_flux/PACK/link")[-1] == "fingerprint_mismatch"
        assert payloads(sink, "bms_flux/PACK/availability")[-1] == "offline"
        transport.chunks.put_nowait(
            pia(101, 51) + block(4, bytes(52)) + block(1, bytes([255] * 18))
        )
        transport.chunks.put_nowait(full_set(2))
        await settle()
        clock.advance(20)
        transport.chunks.put_nowait(full_set(2))
        await settle()
        assert payloads(sink, "bms_flux/PACK/state") == baseline
        assert payloads(sink, "bms_flux/OTHER/link")[-1] == "ok"
        assert payloads(sink, "bms_flux/PACK/link")[-1] == "fingerprint_mismatch"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_default_silence_is_ten_seconds_and_bad_crc_cannot_refresh_blocks() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        clock.advance(9)
        corrupt = bytearray(pia())
        corrupt[-1] ^= 1
        transport.chunks.put_nowait(bytes(corrupt))
        await settle()
        assert payloads(sink, "bms_flux/PACK/link")[-1] == "ok"
        clock.advance(1)
        await settle()
        assert payloads(sink, "bms_flux/PACK/link")[-1] == "silent"
        assert payloads(sink, "bms_flux/PACK/availability")[-1] == "offline"
        assert len(payloads(sink, "bms_flux/PACK/state")) == 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["PIA", "PIB", "PIC"])
async def test_each_block_expires_and_only_fresh_full_set_recovers(missing: str) -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(
        Bridge(transport, SeplosV3Decoder(), sink, packs, clock, silent_after_s=2).run()
    )
    blocks = {"PIA": pia(), "PIB": block(4, bytes(52)), "PIC": block(1, bytes(18))}
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        clock.advance(1)
        transport.chunks.put_nowait(b"".join(b for name, b in blocks.items() if name != missing))
        await settle()
        clock.advance(1)
        await settle()
        assert payloads(sink, "bms_flux/PACK/link") == ["silent", "ok", "silent"]
        assert payloads(sink, "bms_flux/PACK/availability")[-1] == "offline"
        count = len(payloads(sink, "bms_flux/PACK/state"))
        clock.advance(10)
        await settle()
        assert len(payloads(sink, "bms_flux/PACK/state")) == count
        transport.chunks.put_nowait(blocks[missing])
        await settle()
        assert payloads(sink, "bms_flux/PACK/link")[-1] == "silent"
        transport.chunks.put_nowait(b"".join(b for name, b in blocks.items() if name != missing))
        await settle()
        assert payloads(sink, "bms_flux/PACK/link")[-1] == "ok"
        assert len(payloads(sink, "bms_flux/PACK/state")) == count + 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_increasing_fingerprint_is_accepted_but_crc_failure_cannot_trip_guard() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        clock.advance(1)
        transport.chunks.put_nowait(pia(101, 51))
        await settle()
        corrupt = bytearray(pia(1, 1))
        corrupt[-1] ^= 1
        transport.chunks.put_nowait(bytes(corrupt))
        await settle()
        assert payloads(sink, "bms_flux/PACK/link") == ["silent", "ok"]
        # Keep every block fresh up to the interval publication.
        for _ in range(9):
            clock.advance(1)
            transport.chunks.put_nowait(pia(101, 51) + block(4, bytes(52)) + block(1, bytes(18)))
            await settle()
        snapshots = payloads(sink, "bms_flux/PACK/state")
        assert len(snapshots) == 2
        latest = snapshots[-1]
        assert isinstance(latest, dict)
        assert latest["cycles"] == 101
        assert latest["lifetime_discharge"] == 510
        # A fall from the new high-water counters must also latch a mismatch.
        transport.chunks.put_nowait(pia(100, 50))
        await settle()
        assert payloads(sink, "bms_flux/PACK/link")[-1] == "fingerprint_mismatch"
        assert payloads(sink, "bms_flux/PACK/state") == snapshots
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_label_changes_only_device_name_and_link_follows_bridge() -> None:
    discoveries: list[dict[str, object]] = []
    for label in (None, "Garage", "Renamed"):
        packs = [Pack(address=1, serial="PACK", label=label)]
        transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
        transport.chunks.put_nowait(b"")
        await Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run()
        config = sink.messages[0][1]
        assert config["device"]["name"] == (label or "PACK")
        components = config["components"]
        for name, component in components.items():
            if name == "link":
                assert component["availability"] == [{"topic": "bms_flux/status"}]
                assert component["options"] == ["ok", "silent", "fingerprint_mismatch"]
            else:
                assert component["availability"] == [
                    {"topic": "bms_flux/status"},
                    {"topic": "bms_flux/PACK/availability"},
                ]
                assert component["availability_mode"] == "all"
        config["device"].pop("name")
        discoveries.append(config)
    assert discoveries[0] == discoveries[1] == discoveries[2]


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [False, True])
async def test_unmapped_addresses_are_reported_but_never_become_packs(configured: bool) -> None:
    packs = [Pack(address=1, serial="PACK")] if configured else []
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(full_set(2))
        await settle()
        clock.advance(10)
        await settle()
        assert not pack_snapshots(sink)
        assert payloads(sink, "bms_flux/unmapped_addresses")[-1] == {"unmapped_addresses": [2]}
        configs = [t for t, _, _ in sink.messages if t.startswith("homeassistant/")]
        assert configs == (["homeassistant/device/bms_flux_PACK/config"] if configured else []) + [
            "homeassistant/device/bms_flux_bridge/config"
        ]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class StallingMQTT(MemoryMQTT):
    """A sink whose recovery availability publish stalls past silent_after."""

    def __init__(self, clock: ManualClock) -> None:
        super().__init__()
        self._clock = clock

    async def publish(
        self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
    ) -> None:
        if topic.endswith("/availability") and payload == "online":
            self._clock.advance(11)
        await super().publish(topic, payload, retain=retain, qos=qos)


@pytest.mark.asyncio
async def test_recovery_snapshot_is_not_published_once_blocks_expire_mid_publish() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, clock = FakeTransport(), ManualClock()
    sink = StallingMQTT(clock)
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        assert not payloads(sink, "bms_flux/PACK/state")
        assert payloads(sink, "bms_flux/PACK/link")[-1] == "silent"
        assert payloads(sink, "bms_flux/PACK/availability")[-1] == "offline"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
