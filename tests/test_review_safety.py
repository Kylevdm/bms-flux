"""Whole-batch safety regressions through public bridge and transport seams."""

import asyncio
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from bms_flux.bridge import Bridge
from bms_flux.cli import ConfigurationError, probe
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import DriftFullCharge, MQTTSettings, Pack, Settings
from bms_flux.transport import SerialTransport
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, settle
from tests.test_identity import full_set, pia


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bus", ["rfc2217://localhost:1234", "socket://localhost:1234", "loop://", "relative"]
)
async def test_probe_rejects_unsupported_bus_before_open(
    bus: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("invalid bus reached transport")

    monkeypatch.setattr("bms_flux.cli.SupervisedTransport", forbidden)
    with pytest.raises(ConfigurationError, match="bus must be"):
        await probe(bus)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bus",
    ["rfc2217://localhost:1234", "loop://", "tcp://localhost:1234", "relative", "/dev/../tmp/port"],
)
async def test_serial_transport_refuses_non_device_paths(
    bus: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("unsafe bus reached pyserial")

    monkeypatch.setattr(
        "bms_flux.transport.serial_asyncio_fast.create_serial_connection", forbidden
    )
    with pytest.raises(ValueError, match="/dev/"):
        await SerialTransport.open(bus)


@pytest.mark.asyncio
async def test_mismatched_findings_are_unavailable_and_not_republished(tmp_path: Path) -> None:
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="PACK")],
        clock,
        state_dir=tmp_path,
        drift_full_charge=DriftFullCharge(),
    )
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        assert ("bms_flux/PACK/finding_availability", "online", True) in sink.messages
        clock.advance(11)
        await settle()
        assert ("bms_flux/PACK/finding_availability", "offline", True) not in sink.messages
        transport.chunks.put_nowait(pia(cycles=1))
        await settle()
        assert ("bms_flux/PACK/finding_availability", "offline", True) in sink.messages
        sink.messages.clear()
        await bridge.republish()
        assert not any(t == "bms_flux/PACK/drift_full_charge" for t, _, _ in sink.messages)
        assert ("bms_flux/PACK/finding_availability", "offline", True) in sink.messages
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class StallingSink(MemoryMQTT):
    """A sink whose stream publications stall until released."""

    def __init__(self) -> None:
        super().__init__()
        self.stalled = asyncio.Event()
        self.release = asyncio.Event()

    async def publish(
        self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
    ) -> None:
        if topic.endswith("/stream"):
            self.stalled.set()
            await self.release.wait()
        await super().publish(topic, payload, retain=retain, qos=qos)


async def drained(count: int) -> None:
    for _ in range(count):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_stalled_consumer_never_stops_the_decoder(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    sink = StallingSink()
    transport, clock = FakeTransport(), ManualClock()
    task = asyncio.create_task(
        Bridge(
            transport,
            SeplosV3Decoder(),
            sink,
            [Pack(address=1, serial="PACK")],
            clock,
            state_dir=tmp_path,
        ).run()
    )
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await asyncio.wait_for(sink.stalled.wait(), 1)
        # The stream consumer is stuck in its publish, yet the decoder keeps
        # running: link state and snapshots still reach the broker.
        transport.chunks.put_nowait(full_set())
        await settle()
        assert ("bms_flux/PACK/link", "ok", True) in sink.messages
        assert any(t == "bms_flux/PACK/state" for t, _, _ in sink.messages)
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_stalled_consumer_drops_its_oldest_queued_readings(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    sink = StallingSink()
    transport, clock = FakeTransport(), ManualClock()
    task = asyncio.create_task(
        Bridge(
            transport,
            SeplosV3Decoder(),
            sink,
            [Pack(address=1, serial="PACK")],
            clock,
            state_dir=tmp_path,
        ).run()
    )
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await asyncio.wait_for(sink.stalled.wait(), 1)
        # Fill the stalled stream consumer's bounded queue well past its limit.
        for _ in range(305):
            transport.chunks.put_nowait(pia())
            await drained(5)
        # Drops are logged on the first and every 100th; the count itself is pinned below.
        assert "dropped the oldest reading (total 1)" in caplog.text
        assert "(total 7)" not in caplog.text
        sink.release.set()
        await drained(2000)
        streams = [payload for t, payload, _ in sink.messages if t == "bms_flux/PACK/stream"]
        # The in-flight reading plus the 300 the queue retained; 7 were dropped.
        assert len(streams) == 301
        assert streams[-1]["block"] == "PIA"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_raw_api_fault_is_contained_like_the_mqtt_raw_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def broken(self: object, frame: bytes, ts: float) -> None:
        raise RuntimeError("api raw fault")

    monkeypatch.setattr("bms_flux.api.LocalAPI.publish_raw", broken)
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(
        Bridge(
            transport,
            SeplosV3Decoder(),
            sink,
            [Pack(address=1, serial="PACK")],
            clock,
            state_dir=tmp_path,
            raw_frames=True,
            api_token="test-only-token",
        ).run()
    )
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        assert "Consumer failed (RuntimeError)" in caplog.text
        assert not task.done()
        # The MQTT raw stream and the ordinary consumers are unaffected.
        assert any(t == "bms_flux/raw" for t, _, _ in sink.messages)
        assert any(t == "bms_flux/PACK/state" for t, _, _ in sink.messages)
        transport.chunks.put_nowait(full_set())
        await settle()
        assert not task.done()
        assert len([t for t, _, _ in sink.messages if t == "bms_flux/raw"]) > 3
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_reopened_transport_cannot_complete_a_partial_frame(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(
        Bridge(
            transport,
            SeplosV3Decoder(),
            sink,
            [Pack(address=1, serial="PACK")],
            clock,
            state_dir=tmp_path,
        ).run()
    )
    try:
        await settle()
        # One connection supplies a frame prefix, then the link reopens and the
        # new connection supplies bytes that would complete it with a valid CRC.
        transport.chunks.put_nowait(pia()[:10])
        await settle()
        transport.generation += 1
        transport.chunks.put_nowait(pia()[10:] + full_set())
        await settle()
        streams = [payload for t, payload, _ in sink.messages if t == "bms_flux/PACK/stream"]
        assert len(streams) == 3
        assert [p["block"] for p in streams] == ["PIA", "PIB", "PIC"]
        # The decoder resynced on the reopened stream and still publishes.
        transport.chunks.put_nowait(pia(cycles=101))
        await settle()
        streams = [payload for t, payload, _ in sink.messages if t == "bms_flux/PACK/stream"]
        assert len(streams) == 4
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_concurrent_finding_saves_are_serialised(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from bms_flux.drift.findings import Finding, FindingStore
    from bms_flux.identity import PackIdentity

    store = FindingStore(tmp_path, MemoryMQTT(), PackIdentity([Pack(address=1, serial="A")], 10))
    original = asyncio.to_thread
    active = 0
    peak = 0

    async def tracked(func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0)
        try:
            return await original(func, *args, **kwargs)
        finally:
            active -= 1

    monkeypatch.setattr(asyncio, "to_thread", tracked)
    finding = Finding(
        active=True,
        anchor_soc=95.0,
        reported_soc=80.0,
        gap=15.0,
        detected_at=1.0,
        thresholds={"gap_pts": 10.0},
    )
    await asyncio.gather(store.set("rule_one", "A", finding), store.set("rule_two", "A", finding))
    assert peak == 1
    saved = json.loads(await asyncio.to_thread((tmp_path / "findings.json").read_text))
    assert set(saved) == {"rule_one", "rule_two"}
    assert not await asyncio.to_thread(lambda: list(tmp_path.glob("*.tmp")))


@pytest.mark.asyncio
async def test_addon_mode_detection_runs_off_the_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import threading

    from bms_flux import cli

    threads: list[int] = []

    def fake_mode() -> bool:
        threads.append(threading.get_ident())
        return False

    monkeypatch.setattr("bms_flux.cli.addon.is_addon_mode", fake_mode)
    settings = Settings(
        bus="/dev/test",
        state_dir=tmp_path,
        mqtt=MQTTSettings(host="localhost"),
    )
    monkeypatch.setattr("bms_flux.cli.load_settings", lambda: settings)

    class Client:
        async def __aenter__(self) -> "Client":
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

        async def publish(
            self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
        ) -> None:
            pass

    class Transport(FakeTransport):
        def close(self) -> None:
            pass

    async def fake_open(bus: str) -> Transport:
        return Transport()

    monkeypatch.setattr("bms_flux.cli.aiomqtt.Client", lambda **kwargs: Client())
    monkeypatch.setattr("bms_flux.transport.SerialTransport.open", staticmethod(fake_open))
    task = asyncio.create_task(cli.run())
    try:
        for _ in range(100):
            await asyncio.sleep(0.001)
            if threads:
                break
        assert threads and threads[0] != threading.get_ident()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
