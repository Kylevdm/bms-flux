"""Divergence drift rule exercised through the bridge seam."""

import asyncio
import contextlib
import struct
from pathlib import Path

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import DriftDivergence, Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, block, settle


def full_set(
    address: int = 1,
    *,
    soc: int = 700,
    capacity: int = 28000,
    current: int = 0,
    cycles: int = 100,
    discharge: int = 50,
) -> bytes:
    registers = [5200, current & 0xFFFF, 20000, capacity, discharge, soc, 980, cycles, 3200] + [
        0
    ] * 9
    return (
        block(4, struct.pack(">18H", *registers), address)
        + block(4, bytes(52), address)
        + block(1, bytes(18), address)
    )


async def play(
    transport: FakeTransport,
    clock: ManualClock,
    seconds: int,
    a_soc: int,
    b_soc: int,
) -> None:
    for _ in range(seconds // 5):
        clock.advance(5)
        transport.chunks.put_nowait(full_set(1, soc=a_soc) + full_set(2, soc=b_soc))
        await settle()


def states(
    sink: MemoryMQTT, topic: str = "bms_flux/bank/drift_divergence"
) -> list[dict[str, object]]:
    return [payload for t, payload, _ in sink.messages if t == topic]


def bank_configs(sink: MemoryMQTT) -> list[dict[str, object]]:
    return [
        payload
        for t, payload, _ in sink.messages
        if t == "homeassistant/device/bms_flux_bridge/config"
    ]


def availability(sink: MemoryMQTT) -> list[object]:
    return [payload for t, payload, _ in sink.messages if t == "bms_flux/bank/availability"]


async def running_bridge(
    tmp_path: Path,
    *,
    enabled: bool = True,
    hold_min: int = 1,
    spread_on_pts: float = 5,
    spread_off_pts: float = 3,
) -> tuple[Bridge, FakeTransport, MemoryMQTT, ManualClock, asyncio.Task[None]]:
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="A"), Pack(address=2, serial="B")],
        clock,
        snapshot_interval_s=10,
        silent_after_s=10,
        drift_divergence=DriftDivergence(
            enabled=enabled,
            spread_on_pts=spread_on_pts,
            spread_off_pts=spread_off_pts,
            hold_min=hold_min,
        ),
        state_dir=tmp_path,
    )
    task = asyncio.create_task(bridge.run())
    await settle()
    return bridge, transport, sink, clock, task


async def cleanup(
    transport: FakeTransport,
    task: asyncio.Task[None],
) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_divergence_discovery_component(tmp_path: Path) -> None:
    _bridge, transport, sink, _clock, task = await running_bridge(tmp_path)
    try:
        config = bank_configs(sink)[-1]
        assert isinstance(config, dict)
        components = config["components"]
        assert isinstance(components, dict)
        component = components["drift_divergence"]
        assert component["platform"] == "binary_sensor"
        assert component["device_class"] == "problem"
        assert component["state_topic"] == "bms_flux/bank/drift_divergence"
        assert component["json_attributes_topic"] == "bms_flux/bank/drift_divergence"
        assert component["availability"] == [
            {"topic": "bms_flux/status"},
            {"topic": "bms_flux/bank/availability"},
        ]
    finally:
        await cleanup(transport, task)


@pytest.mark.asyncio
async def test_divergence_fires_and_clears(tmp_path: Path) -> None:
    _bridge, transport, sink, clock, task = await running_bridge(tmp_path)
    try:
        transport.chunks.put_nowait(full_set(1, soc=300) + full_set(2, soc=400))
        await settle()
        await play(transport, clock, 55, 300, 400)
        assert states(sink)[-1]["active"] is False

        await play(transport, clock, 10, 300, 400)
        fired = states(sink)[-1]
        assert fired["active"] is True
        assert fired["spread"] == 10.0
        assert fired["hold_start"] == 0.0

        await play(transport, clock, 55, 300, 305)
        assert states(sink)[-1]["active"] is True

        await play(transport, clock, 10, 300, 305)
        cleared = states(sink)[-1]
        assert cleared["active"] is False
        assert cleared["spread"] == 0.5
        assert cleared["hold_start"] is None
    finally:
        await cleanup(transport, task)


@pytest.mark.asyncio
async def test_gap_resets_divergence_timer(tmp_path: Path) -> None:
    _bridge, transport, sink, clock, task = await running_bridge(tmp_path)
    try:
        transport.chunks.put_nowait(full_set(1, soc=300) + full_set(2, soc=400))
        await settle()
        await play(transport, clock, 55, 300, 400)
        assert states(sink)[-1]["active"] is False

        clock.advance(15)
        transport.chunks.put_nowait(full_set(1, soc=300))
        await settle()
        assert availability(sink)[-1] == "offline"
        reset = states(sink)[-1]
        assert reset["active"] is False
        assert reset["hold_start"] is None

        clock.advance(5)
        transport.chunks.put_nowait(full_set(1, soc=300) + full_set(2, soc=400))
        await settle()
        await play(transport, clock, 55, 300, 400)
        assert states(sink)[-1]["active"] is False

        await play(transport, clock, 10, 300, 400)
        assert states(sink)[-1]["active"] is True
    finally:
        await cleanup(transport, task)


@pytest.mark.asyncio
async def test_disabled_divergence_does_not_publish(tmp_path: Path) -> None:
    _bridge, transport, sink, clock, task = await running_bridge(tmp_path, enabled=False)
    try:
        config = bank_configs(sink)[-1]
        assert isinstance(config, dict)
        components = config["components"]
        assert isinstance(components, dict)
        assert "drift_divergence" not in components
        transport.chunks.put_nowait(full_set(1, soc=300) + full_set(2, soc=400))
        await settle()
        await play(transport, clock, 65, 300, 400)
        assert states(sink) == []
    finally:
        await cleanup(transport, task)


@pytest.mark.asyncio
async def test_republish_divergence_after_reconnect(tmp_path: Path) -> None:
    bridge, transport, sink, clock, task = await running_bridge(tmp_path)
    try:
        transport.chunks.put_nowait(full_set(1, soc=300) + full_set(2, soc=400))
        await settle()
        await play(transport, clock, 65, 300, 400)
        before = len(states(sink))
        await bridge.republish_findings()
        after = states(sink)
        assert len(after) == before + 1
        assert after[-1]["active"] is True
    finally:
        await cleanup(transport, task)


@pytest.mark.asyncio
async def test_broker_reconnect_resets_the_divergence_hold(tmp_path: Path) -> None:
    bridge, transport, sink, clock, task = await running_bridge(tmp_path, hold_min=30)
    try:
        await play(transport, clock, 20 * 60, 700, 500)
        first_start = states(sink)[-1]["hold_start"]
        assert first_start is not None
        await bridge.republish()
        await settle()
        # The hold restarted at the reconnect rather than carrying over.
        assert states(sink)[-1]["hold_start"] == clock.now() != first_start
        # The hold restarts from the reconnect, so 20 more minutes is not enough.
        await play(transport, clock, 20 * 60, 700, 500)
        assert states(sink)[-1]["active"] is False
    finally:
        await cleanup(transport, task)
