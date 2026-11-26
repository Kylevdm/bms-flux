"""Rested OCV findings exercised through timestamped bridge byte playback."""

import asyncio
import struct
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import DriftFullCharge, DriftRestedOCV, OCVPoint, Pack
from tests.helpers import (
    ManualClock,
    MemoryMQTT,
    await_saved_finding,
    block,
)
from tests.test_full_charge import PlaybackTransport


def full_set(
    *,
    soc: int = 300,
    current: int = 100,
    avg: int = 3209,
    capacity: int = 28000,
    temperature: int = 2831,
    address: int = 1,
) -> bytes:
    registers = [5200, current & 0xFFFF, 20000, capacity, 50, soc, 980, 100, avg]
    registers += [0, 0, 0, 0, temperature, 0, 0, 0, 0]
    return (
        block(4, struct.pack(">18H", *registers), address)
        + block(4, bytes(52), address)
        + block(1, bytes(18), address)
    )


def states(sink: MemoryMQTT, serial: str = "PACK") -> list[dict[str, object]]:
    return [p for t, p, _ in sink.messages if t == f"bms_flux/{serial}/drift_rested_ocv"]


@asynccontextmanager
async def running(
    path: Path,
    *,
    settings: DriftRestedOCV | None = None,
    table: list[OCVPoint] | None = None,
    full_charge: bool = False,
) -> AsyncIterator[tuple[Bridge, PlaybackTransport, MemoryMQTT, ManualClock]]:
    transport, sink, clock = PlaybackTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="PACK"), Pack(address=2, serial="OTHER")],
        clock,
        drift_full_charge=DriftFullCharge(hold_min=1) if full_charge else None,
        drift_rested_ocv=settings or DriftRestedOCV(),
        ocv_table=table,
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
async def test_rested_ocv_two_hour_hold_evidence_and_discovery(tmp_path: Path) -> None:
    async with running(tmp_path) as (bridge, transport, sink, clock):
        config = sink.messages[0][1]["components"]["drift_rested_ocv"]
        assert config["device_class"] == "problem"
        assert config["availability"] == [
            {"topic": "bms_flux/status"},
            {"topic": "bms_flux/PACK/finding_availability"},
        ]
        assert config["availability_mode"] == "all"
        await transport.play(full_set())
        await play(transport, clock, 7195)
        assert states(sink)[-1] == {"active": False}
        await play(transport, clock, 5)
        assert states(sink)[-1] == {
            "active": True,
            "anchor_soc": 10.0,
            "reported_soc": 30.0,
            "gap": 20.0,
            "detected_at": 7200.0,
            "thresholds": {
                "max_current_c": 0.02,
                "rest_h": 2,
                "ocv_soc_below_pct": 20,
                "gap_pts": 10,
                "min_temp_c": 10,
            },
        }
        await bridge.republish_findings()
        assert states(sink)[-1]["active"] is True
        assert states(sink, "OTHER") == [{"active": False}, {"active": False}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "values",
    [
        {"current": 560},
        {"current": -560},
        {"current": 200, "capacity": 10000},
        {"temperature": 2830},
    ],
)
async def test_rested_ocv_gates_reset_rest_using_pack_capacity(
    tmp_path: Path,
    values: dict[str, int],
) -> None:
    async with running(tmp_path, settings=DriftRestedOCV(rest_h=1)) as (_, transport, sink, clock):
        await transport.play(full_set())
        await play(transport, clock, 3595)
        await play(transport, clock, 5, **values)
        await play(transport, clock, 3600)
        assert states(sink)[-1]["active"] is False
        await play(transport, clock, 5)
        assert states(sink)[-1]["active"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["all", "PIB", "PIC"])
async def test_rested_ocv_gap_resets_rest(tmp_path: Path, missing: str) -> None:
    async with running(tmp_path, settings=DriftRestedOCV(rest_h=1)) as (_, transport, sink, clock):
        await transport.play(full_set())
        await play(transport, clock, 3590)
        for _ in range(2):
            clock.advance(5)
            if missing != "all":
                data = full_set()
                await transport.play(data[:41] + (data[98:] if missing == "PIB" else data[41:98]))
        await transport.play(full_set())
        await play(transport, clock, 3595)
        assert states(sink)[-1]["active"] is False
        await play(transport, clock, 5)
        assert states(sink)[-1]["active"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("avg,soc", [(3251, 500), (3455, 100), (3209, 200), (3212, 210)])
async def test_rested_ocv_strict_window_no_upper_band_and_gap_boundary(
    tmp_path: Path,
    avg: int,
    soc: int,
) -> None:
    async with running(tmp_path, settings=DriftRestedOCV(rest_h=1)) as (_, transport, sink, clock):
        await transport.play(full_set(avg=avg, soc=soc))
        await play(transport, clock, 3600, avg=avg, soc=soc)
        assert states(sink) == [{"active": False}]


@pytest.mark.asyncio
@pytest.mark.parametrize("avg,expected", [(3209, 10.0), (3212, 11.0), (2500, 1.0)])
async def test_rested_ocv_default_table_interpolates_and_clamps(
    tmp_path: Path,
    avg: int,
    expected: float,
) -> None:
    async with running(tmp_path, settings=DriftRestedOCV(rest_h=1)) as (_, transport, sink, clock):
        await transport.play(full_set(avg=avg))
        await play(transport, clock, 3600, avg=avg)
        assert states(sink)[-1]["anchor_soc"] == pytest.approx(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("avg,expected", [(3100, 10.0), (3050, 5.0), (2900, 0.0), (3500, 100.0)])
async def test_rested_ocv_override_table_and_configured_thresholds(
    tmp_path: Path,
    avg: int,
    expected: float,
) -> None:
    table = [
        OCVPoint(cell_v=v, soc_pct=s)
        for v, s in [(3.0, 0), (3.1, 10), (3.2, 20), (3.3, 50), (3.4, 100)]
    ]
    settings = DriftRestedOCV(rest_h=1, ocv_soc_below_pct=100, gap_pts=1, min_temp_c=15)
    async with running(tmp_path, settings=settings, table=table) as (_, transport, sink, clock):
        await transport.play(full_set(avg=avg, temperature=2881))
        await play(transport, clock, 3600, avg=avg, temperature=2881)
        if expected == 100:
            assert states(sink) == [{"active": False}]
        else:
            assert states(sink)[-1]["anchor_soc"] == pytest.approx(expected)
            thresholds = states(sink)[-1]["thresholds"]
            assert isinstance(thresholds, dict)
            assert thresholds["gap_pts"] == 1


@pytest.mark.asyncio
async def test_rested_ocv_latches_across_restart_and_clears_at_new_agreeing_anchor(
    tmp_path: Path,
) -> None:
    settings = DriftRestedOCV(rest_h=1)
    async with running(tmp_path, settings=settings) as (_, transport, _, clock):
        await transport.play(full_set())
        await play(transport, clock, 3600)
        # Agreement during the same rest is not a new anchor.
        await play(transport, clock, 5, soc=100)
        await await_saved_finding(tmp_path, "drift_rested_ocv", "PACK", True)
    async with running(tmp_path, settings=settings) as (bridge, transport, sink, clock):
        assert states(sink)[-1]["active"] is True
        await bridge.republish_findings()
        assert states(sink)[-1]["active"] is True
        # Neither a warm upper-band rest nor a cold low-end rest clears the finding.
        await transport.play(full_set(avg=3455, soc=990))
        await play(transport, clock, 3600, avg=3455, soc=990)
        await play(transport, clock, 3600, temperature=2800, soc=100)
        assert states(sink)[-1]["active"] is True
        await play(transport, clock, 3600, soc=200)
        assert states(sink)[-1]["active"] is True
        await play(transport, clock, 5, soc=200)
        await await_saved_finding(tmp_path, "drift_rested_ocv", "PACK", False)
        assert states(sink)[-1]["active"] is False
    async with running(tmp_path, settings=settings) as (_, _, sink, _):
        assert states(sink)[-1]["active"] is False


@pytest.mark.asyncio
async def test_rested_ocv_disabled_rule_does_not_publish_or_persist(tmp_path: Path) -> None:
    async with running(tmp_path, settings=DriftRestedOCV(enabled=False)) as (
        _,
        transport,
        sink,
        clock,
    ):
        await transport.play(full_set())
        await play(transport, clock, 7200)
        assert states(sink) == []
        assert "drift_rested_ocv" not in sink.messages[0][1]["components"]
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
async def test_rested_ocv_and_full_charge_are_independent_per_pack(tmp_path: Path) -> None:
    async with running(
        tmp_path,
        settings=DriftRestedOCV(rest_h=1),
        full_charge=True,
    ) as (_, transport, sink, clock):
        components = sink.messages[0][1]["components"]
        assert "drift_full_charge" in components
        assert "drift_rested_ocv" in components
        await transport.play(full_set(avg=3450, soc=900))
        await transport.play(full_set(avg=3226, soc=0, address=2))
        for _ in range(720):
            clock.advance(5)
            await transport.play(full_set(avg=3450, soc=900))
            await transport.play(full_set(avg=3226, soc=0, address=2))
        assert states(sink, "PACK") == [{"active": False}]
        assert states(sink, "OTHER")[-1]["active"] is True
        assert states(sink, "OTHER")[-1]["gap"] == 15.0
        full_charge_states = [
            p for t, p, _ in sink.messages if t == "bms_flux/PACK/drift_full_charge"
        ]
        assert full_charge_states[-1]["active"] is True
