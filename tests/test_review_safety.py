"""Whole-batch safety regressions through public bridge and transport seams."""

import asyncio
from pathlib import Path

import pytest

from bms_flux.bridge import Bridge
from bms_flux.cli import ConfigurationError, probe
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import DriftFullCharge, Pack
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
