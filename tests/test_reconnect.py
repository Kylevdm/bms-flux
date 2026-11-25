"""Broker outages and consumer faults exercised through the bridge seam."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import aiomqtt
import pytest

from bms_flux.bridge import Bridge
from bms_flux.mqtt import BrokerConnection, ReconnectingSink
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.readings import DecodedBlock, ReadingStream
from bms_flux.settings import DriftDivergence, DriftFullCharge, Pack
from bms_flux.stream import StreamPublisher
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, block, frame, settle
from tests.test_divergence import full_set as divergence_full_set


@pytest.mark.asyncio
async def test_publish_failure_does_not_stop_decoding(tmp_path: Path) -> None:
    class FailingSink(MemoryMQTT):
        async def publish(
            self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
        ) -> None:
            if topic.endswith("/stream"):
                raise aiomqtt.MqttError("offline")
            await super().publish(topic, payload, retain=retain, qos=qos)

    transport, clock, sink = FakeTransport(), ManualClock(), FailingSink()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="PACK")],
        clock,
        state_dir=tmp_path,
        silent_after_s=120,
    )
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        transport.chunks.put_nowait(frame() + block(4, bytes(52)) + block(1, bytes(18)))
        await settle()
        transport.chunks.put_nowait(frame(current=100))
        await settle()
        clock.advance(5)
        transport.chunks.put_nowait(frame(current=200))
        await settle()
        clock.advance(5)
        await settle()
        assert not task.done()
        states = [payload for topic, payload, _ in sink.messages if topic == "bms_flux/PACK/state"]
        assert states[-1]["current"] == 2.0
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("loss", ["monitor", "publish"])
async def test_reconnect_restores_current_state_without_replaying_stream(
    tmp_path: Path, loss: str
) -> None:
    clock, transport = ManualClock(), FakeTransport()

    class Connection(MemoryMQTT):
        available = False
        disconnected = asyncio.Event()

        async def wait_disconnected(self) -> None:
            await self.disconnected.wait()
            raise aiomqtt.MqttError("offline")

        async def publish(
            self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
        ) -> None:
            if not self.available:
                raise aiomqtt.MqttError("offline")
            await super().publish(topic, payload, retain=retain, qos=qos)

    connection = Connection()
    attempts: list[float] = []

    @asynccontextmanager
    async def connect() -> AsyncIterator[BrokerConnection]:
        attempts.append(clock.now())
        if not connection.available:
            raise aiomqtt.MqttError("offline")
        connection.disconnected.clear()
        yield connection

    sink = ReconnectingSink(connect, clock)
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="PACK")],
        clock,
        silent_after_s=120,
        state_dir=tmp_path,
        drift_full_charge=DriftFullCharge(cell_avg_v=3.2, hold_min=1),
    )
    task = asyncio.create_task(bridge.run())
    try:
        # Start disconnected: drift rules must still evaluate the trusted readings.
        for _ in range(100):
            await asyncio.sleep(0.001)
            if attempts:
                break
        transport.chunks.put_nowait(frame() + block(4, bytes(52)) + block(1, bytes(18)))
        await settle()
        transport.chunks.put_nowait(frame(current=100))
        await settle()
        # hold_min has a lower bound of 1, so the second qualifying PIA, a minute
        # after the first, is what latches the finding while the broker is down.
        clock.advance(60)
        transport.chunks.put_nowait(frame(current=100))
        for _ in range(100):
            await asyncio.sleep(0.001)
            if await asyncio.to_thread(lambda: list(tmp_path.glob("*.json"))):
                break
        assert not connection.messages
        assert attempts == [0, 60]
        connection.available = True
        clock.advance(2)
        await settle()
        assert attempts == [0, 60, 62]
        topics = [topic for topic, _, _ in connection.messages]
        assert "homeassistant/device/bms_flux_PACK/config" in topics
        assert "homeassistant/device/bms_flux_bridge/config" in topics
        assert ("bms_flux/status", "online", True) in connection.messages
        assert ("bms_flux/PACK/availability", "online", True) in connection.messages
        assert ("bms_flux/PACK/link", "ok", True) in connection.messages
        assert "bms_flux/bank/state" in topics
        states = [p for t, p, _ in connection.messages if t == "bms_flux/PACK/state"]
        assert states[-1]["current"] == 1.0
        findings = [p for t, p, _ in connection.messages if t == "bms_flux/PACK/drift_full_charge"]
        assert findings[-1]["active"] is True
        assert "bms_flux/PACK/stream" not in topics

        # A live connection can fail too. Reconnect must reset availability caches.
        connection.messages.clear()
        connection.available = False
        if loss == "monitor":
            connection.disconnected.set()
        await settle()
        transport.chunks.put_nowait(frame(current=200))
        await settle()
        assert not connection.messages
        connection.available = True
        clock.advance(1)
        await settle()
        assert attempts[-1] == 63
        assert ("bms_flux/PACK/availability", "online", True) in connection.messages
        states = [p for t, p, _ in connection.messages if t == "bms_flux/PACK/state"]
        assert states[-1]["current"] == 2.0
        assert not any(t.endswith("/stream") for t, _, _ in connection.messages)
        transport.chunks.put_nowait(frame(current=300))
        await settle()
        streams = [p for t, p, _ in connection.messages if t.endswith("/stream")]
        assert len(streams) == 1
        assert streams[0]["current"] == 3.0
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("before_get", [False, True])
async def test_consumer_exception_skips_reading_and_keeps_decoder_running(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    before_get: bool,
) -> None:
    original = StreamPublisher.consume
    failed = False

    async def consume(self: StreamPublisher, stream: ReadingStream) -> None:
        nonlocal failed
        if not failed:
            if not before_get:
                await stream.get()
            failed = True
            raise RuntimeError("consumer fault")
        await original(self, stream)

    monkeypatch.setattr(StreamPublisher, "consume", consume)
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
        transport.chunks.put_nowait(frame() + block(4, bytes(52)) + block(1, bytes(18)))
        await settle()
        transport.chunks.put_nowait(frame(current=100))
        await settle()
        streams = [p for t, p, _ in sink.messages if t.endswith("/stream")]
        assert streams[-1]["current"] == 1.0
        assert "Consumer failed (RuntimeError)" in caplog.text
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_unexpected_core_fault_propagates(tmp_path: Path) -> None:
    class BrokenDecoder(SeplosV3Decoder):
        def decode(self, frame: bytes, ts: float) -> DecodedBlock | None:
            raise RuntimeError("core fault")

    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    transport.chunks.put_nowait(frame())
    with pytest.raises(ExceptionGroup, match="TaskGroup") as caught:
        await Bridge(
            transport,
            BrokenDecoder(),
            sink,
            [Pack(address=1, serial="PACK")],
            clock,
            state_dir=tmp_path,
        ).run()
    assert isinstance(caught.value.exceptions[0], RuntimeError)


@pytest.mark.asyncio
async def test_broker_backoff_doubles_and_caps_while_bus_keeps_running(tmp_path: Path) -> None:
    clock, transport = ManualClock(), FakeTransport()
    attempts: list[float] = []

    @asynccontextmanager
    async def connect() -> AsyncIterator[BrokerConnection]:
        attempts.append(clock.now())
        raise aiomqtt.MqttError("offline")
        yield MemoryMQTT()

    task = asyncio.create_task(
        Bridge(
            transport,
            SeplosV3Decoder(),
            ReconnectingSink(connect, clock),
            [],
            clock,
            state_dir=tmp_path,
        ).run()
    )
    try:
        await settle()
        for delay in [1, 2, 4, 8, 16, 30, 30]:
            transport.chunks.put_nowait(frame())
            clock.advance(delay)
            await settle()
        assert attempts == [0, 1, 3, 7, 15, 31, 61, 91]
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_consumer_failing_before_get_cannot_strand_fanout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def broken(self: StreamPublisher, stream: ReadingStream) -> None:
        raise RuntimeError("consumer cannot start")

    monkeypatch.setattr(StreamPublisher, "consume", broken)
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
        transport.chunks.put_nowait(frame() + block(4, bytes(52)) + block(1, bytes(18)))
        await settle()
        assert ("bms_flux/PACK/availability", "online", True) in sink.messages
        assert any(t == "bms_flux/PACK/state" for t, _, _ in sink.messages)
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_core_fault_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    from bms_flux.cli import main

    async def broken_run() -> None:
        raise RuntimeError("core fault")

    monkeypatch.setattr("bms_flux.cli.run", broken_run)
    monkeypatch.setattr("sys.argv", ["bms-flux", "run"])
    with pytest.raises(SystemExit) as caught:
        main()
    assert caught.value.code == 1


@pytest.mark.asyncio
async def test_broker_reconnect_resets_the_hold_before_live_consumers_publish(
    tmp_path: Path,
) -> None:
    clock, transport = ManualClock(), FakeTransport()
    stalled, release = asyncio.Event(), asyncio.Event()

    class Connection(MemoryMQTT):
        available = True
        disconnected = asyncio.Event()
        stall_restore = False

        async def wait_disconnected(self) -> None:
            await self.disconnected.wait()
            raise aiomqtt.MqttError("offline")

        async def publish(
            self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
        ) -> None:
            if self.stall_restore and topic.startswith("homeassistant/"):
                # The reconnection's restoration stalls, as a slow broker round-trip would.
                stalled.set()
                await release.wait()
            await super().publish(topic, payload, retain=retain, qos=qos)

    connection = Connection()

    @asynccontextmanager
    async def connect() -> AsyncIterator[BrokerConnection]:
        yield connection

    sink = ReconnectingSink(connect, clock)
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="A"), Pack(address=2, serial="B")],
        clock,
        silent_after_s=120,
        state_dir=tmp_path,
        drift_divergence=DriftDivergence(hold_min=1),
    )
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        # Latch an active divergence finding from a spread that has held for an hour.
        for _ in range(13):
            clock.advance(5)
            transport.chunks.put_nowait(
                divergence_full_set(1, soc=300) + divergence_full_set(2, soc=400)
            )
            await settle()
        first_start = next(
            payload["hold_start"]
            for topic, payload, _ in connection.messages
            if topic == "bms_flux/bank/drift_divergence" and payload["active"]
        )
        # The broker drops and returns while the packs stay fresh.
        connection.available = False
        connection.disconnected.set()
        await settle()
        connection.disconnected.clear()
        connection.available = True
        connection.stall_restore = True
        connection.messages.clear()
        clock.advance(1)
        await asyncio.wait_for(stalled.wait(), 1)
        # A concurrent bank evaluation publishes while restoration is still stalled:
        # the new connection is live, so the divergence hold must already be reset.
        clock.advance(10)
        await settle()
        drift = [
            payload
            for topic, payload, _ in connection.messages
            if topic == "bms_flux/bank/drift_divergence"
        ]
        assert drift
        assert drift[0]["active"] is False
        assert drift[0]["hold_start"] != first_start
        release.set()
        await settle()
        assert not task.done()
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
