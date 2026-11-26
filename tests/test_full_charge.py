"""Full-charge findings exercised through the bridge's byte-chunk seam."""

import asyncio
import logging
import struct
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import DriftFullCharge, Pack
from tests.helpers import (
    FakeTransport,
    ManualClock,
    MemoryMQTT,
    await_saved_finding,
    block,
    settle,
)


class PlaybackTransport(FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.ready = asyncio.Event()

    async def read(self) -> bytes:
        self.ready.set()
        return await super().read()

    async def play(self, data: bytes) -> None:
        self.ready.clear()
        self.chunks.put_nowait(data)
        async with asyncio.timeout(2):
            await self.ready.wait()
        # The core loop no longer joins consumer queues after each frame, so a
        # played chunk counts as processed only once the consumers have drained.
        await settle()


def full_set(
    *,
    soc: int = 940,
    current: int = 100,
    avg: int = 3450,
    capacity: int = 28000,
    address: int = 1,
) -> bytes:
    registers = [5200, current & 0xFFFF, 20000, capacity, 50, soc, 980, 100, avg] + [0] * 9
    return (
        block(4, struct.pack(">18H", *registers), address)
        + block(4, bytes(52), address)
        + block(1, bytes(18), address)
    )


def states(sink: MemoryMQTT, serial: str = "PACK") -> list[dict[str, object]]:
    return [p for t, p, _ in sink.messages if t == f"bms_flux/{serial}/drift_full_charge"]


@asynccontextmanager
async def running(
    path: Path,
    *,
    enabled: bool = True,
) -> AsyncIterator[tuple[Bridge, PlaybackTransport, MemoryMQTT, ManualClock]]:
    transport, sink, clock = PlaybackTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="PACK"), Pack(address=2, serial="OTHER")],
        clock,
        drift_full_charge=DriftFullCharge(hold_min=1, enabled=enabled),
        state_dir=path,
    )
    task = asyncio.create_task(bridge.run())
    try:
        async with asyncio.timeout(2):
            await transport.ready.wait()
        yield bridge, transport, sink, clock
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


async def play(
    transport: PlaybackTransport,
    clock: ManualClock,
    seconds: int,
    **values: int,
) -> None:
    for _ in range(seconds // 5):
        clock.advance(5)
        await transport.play(full_set(**values))


@pytest.mark.asyncio
async def test_full_charge_hold_evidence_and_bridge_availability(tmp_path: Path) -> None:
    async with running(tmp_path) as (bridge, transport, sink, clock):
        config = sink.messages[0][1]["components"]["drift_full_charge"]
        assert config["device_class"] == "problem"
        assert config["availability"] == [
            {"topic": "bms_flux/status"},
            {"topic": "bms_flux/PACK/finding_availability"},
        ]
        assert config["availability_mode"] == "all"
        transport.chunks.put_nowait(full_set())
        await settle()
        await play(transport, clock, 55)
        assert states(sink)[-1]["active"] is False
        await play(transport, clock, 5)
    assert states(sink)[-1] == {
        "active": True,
        "anchor_soc": 100.0,
        "reported_soc": 94.0,
        "gap": 6.0,
        "detected_at": 60.0,
        "thresholds": {
            "cell_avg_v": 3.45,
            "max_current_c": 0.05,
            "hold_min": 1,
            "soc_below_pct": 95.0,
        },
    }
    clock.advance(10)
    await bridge.republish_findings()
    assert states(sink)[-1]["active"] is True
    assert states(sink, "OTHER") == [{"active": False}, {"active": False}]


@pytest.mark.asyncio
async def test_latch_survives_restart_and_clears_only_at_new_agreeing_anchor(
    tmp_path: Path,
) -> None:
    async with running(tmp_path) as (_, transport, _, clock):
        transport.chunks.put_nowait(full_set())
        await settle()
        await play(transport, clock, 60)
        await await_saved_finding(tmp_path, "drift_full_charge", "PACK", True)
    async with running(tmp_path) as (bridge, transport, sink, clock):
        assert states(sink)[-1]["active"] is True
        clock.advance(10)
        await settle()
        assert ("bms_flux/PACK/availability", "offline", True) in sink.messages
        await bridge.republish_findings()
        assert states(sink)[-1]["active"] is True
        transport.chunks.put_nowait(full_set(soc=1000, avg=3400))
        await settle()
        await play(transport, clock, 60, soc=1000, avg=3400)
        assert states(sink)[-1]["active"] is True
        transport.chunks.put_nowait(full_set(soc=950))
        await settle()
        await play(transport, clock, 55, soc=950)
        assert states(sink)[-1]["active"] is True
        await play(transport, clock, 5, soc=950)
        await await_saved_finding(tmp_path, "drift_full_charge", "PACK", False)
    assert states(sink)[-1]["active"] is False
    async with running(tmp_path) as (_, _, sink, _):
        assert states(sink)[-1]["active"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "current,avg,capacity",
    [(1400, 3450, 28000), (-1400, 3450, 28000), (600, 3450, 10000), (100, 3449, 28000)],
)
async def test_anchor_gates_reset_hold_and_use_pack_capacity(
    tmp_path: Path,
    current: int,
    avg: int,
    capacity: int,
) -> None:
    async with running(tmp_path) as (_, transport, sink, clock):
        transport.chunks.put_nowait(full_set())
        await settle()
        await play(transport, clock, 55)
        await play(transport, clock, 5, current=current, avg=avg, capacity=capacity)
        await play(transport, clock, 60)
        assert states(sink)[-1]["active"] is False
        await play(transport, clock, 5)
    assert states(sink)[-1]["active"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["all", "PIB", "PIC"])
async def test_gap_resets_hold_even_when_pia_keeps_arriving(tmp_path: Path, missing: str) -> None:
    async with running(tmp_path) as (_, transport, sink, clock):
        transport.chunks.put_nowait(full_set())
        await settle()
        await play(transport, clock, 50)
        for _ in range(2):
            clock.advance(5)
            if missing != "all":
                data = full_set()
                transport.chunks.put_nowait(
                    data[:41] + (data[98:] if missing == "PIB" else data[41:98])
                )
            await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        await play(transport, clock, 55)
        assert states(sink)[-1]["active"] is False
        await play(transport, clock, 5)
    assert states(sink)[-1]["active"] is True


@pytest.mark.asyncio
async def test_disabled_rule_does_not_publish_or_persist(tmp_path: Path) -> None:
    async with running(tmp_path, enabled=False) as (_, transport, sink, clock):
        transport.chunks.put_nowait(full_set())
        await settle()
        await play(transport, clock, 65)
    assert states(sink) == []
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
async def test_corrupt_state_starts_empty_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (tmp_path / "findings.json").write_text('{"drift_full_charge": {"PACK": {"active": true}}}')
    async with running(tmp_path) as (_, _, sink, _):
        assert states(sink) == [{"active": False}]
    assert "state unreadable or corrupt" in caplog.text


@pytest.mark.asyncio
async def test_missing_state_starts_empty_without_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="bms_flux.drift.findings")
    async with running(tmp_path) as (_, _, sink, _):
        assert states(sink) == [{"active": False}]
    assert "No saved findings yet" in caplog.text
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]
