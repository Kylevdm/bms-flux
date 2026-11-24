"""The bridge keeps its availability while transport faults change the bus enum."""

import asyncio
import errno

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from bms_flux.transport import BusState, SupervisedTransport
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, block, frame, settle


async def test_bus_enum_tracks_transport_faults_without_bridge_offline() -> None:
    class Transport(FakeTransport):
        state: BusState = "connected"

    transport = Transport()
    sink = MemoryMQTT()
    clock = ManualClock()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="PACK")],
        clock,
        bus_state=lambda: transport.state,
    )
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        discovery = next(
            payload
            for topic, payload, _ in sink.messages
            if topic == "homeassistant/device/bms_flux_bridge/config"
        )
        component = discovery["components"]["bus"]
        assert component["device_class"] == "enum"
        assert component["options"] == ["connected", "reconnecting", "port_busy"]
        assert component["availability"] == [{"topic": "bms_flux/status"}]
        assert component["state_topic"] == "bms_flux/bus/state"
        for state in ("connected", "port_busy", "reconnecting", "connected"):
            transport.state = state
            clock.advance(1)
            await settle()
            states = [
                payload for topic, payload, _ in sink.messages if topic == "bms_flux/bus/state"
            ]
            assert states[-1]["bus"] == state
        assert [payload for topic, payload, _ in sink.messages if topic == "bms_flux/status"] == [
            "online"
        ]
        assert ("bms_flux/PACK/link", "silent", True) in sink.messages
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_transport_failures_leave_packs_silent_and_recover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Endpoint(FakeTransport):
        async def read(self) -> bytes:
            data = await super().read()
            if not data:
                raise OSError(errno.EBUSY, "held by another sniffer")
            return data

        def close(self) -> None:
            pass

    endpoint = Endpoint()
    failure: OSError | None = None

    async def open_serial(bus: str) -> Endpoint:
        if failure is not None:
            raise failure
        return endpoint

    monkeypatch.setattr("bms_flux.transport.SerialTransport.open", open_serial)
    sink, clock = MemoryMQTT(), ManualClock()
    transport = SupervisedTransport("/dev/test", clock)
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="PACK")],
        clock,
        bus_state=lambda: transport.state,
    )
    task = asyncio.create_task(bridge.run())

    def latest(topic: str) -> object:
        return [payload for name, payload, _ in sink.messages if name == topic][-1]

    full_set = frame() + block(4, bytes(52)) + block(1, bytes(18))
    try:
        endpoint.chunks.put_nowait(full_set)
        await settle()
        assert latest("bms_flux/PACK/link") == "ok"
        endpoint.chunks.put_nowait(b"")
        failure = OSError(errno.EBUSY, "held by another sniffer")
        await settle()
        clock.advance(1)
        await settle()
        health = latest("bms_flux/bus/state")
        assert isinstance(health, dict)
        assert health["bus"] == "port_busy"
        failure = OSError("adapter absent")
        clock.advance(2)
        await settle()
        assert str(transport.state) == "reconnecting"
        clock.advance(10)
        await settle()
        assert latest("bms_flux/PACK/link") == "silent"
        assert latest("bms_flux/PACK/availability") == "offline"
        failure = None
        clock.advance(8)
        endpoint.chunks.put_nowait(full_set)
        await settle()
        assert latest("bms_flux/PACK/link") == "ok"
        assert latest("bms_flux/PACK/availability") == "online"
        assert latest("bms_flux/status") == "online"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        transport.close()
