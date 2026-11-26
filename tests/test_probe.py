"""The console probe listens through the bridge framing/decoder seam."""

import asyncio

import pytest

from bms_flux.cli import probe
from tests.helpers import FakeTransport, ManualClock, block, settle
from tests.test_identity import pia


@pytest.mark.asyncio
async def test_probe_lists_unmapped_addresses_with_cycles_and_reported_soc(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class ProbeTransport(FakeTransport):
        closed = False

        def close(self) -> None:
            self.closed = True

    transport, clock = ProbeTransport(), ManualClock()

    async def open_transport(bus: str) -> ProbeTransport:
        assert bus == "/dev/test"
        return transport

    monkeypatch.setattr("bms_flux.transport.SerialTransport.open", open_transport)
    task = asyncio.create_task(probe("/dev/test", 10, clock=clock))
    await settle()
    broken = bytearray(pia(address=3))
    broken[-1] ^= 1
    transport.chunks.put_nowait(pia(100, address=2)[:9])
    transport.chunks.put_nowait(pia(100, address=2)[9:] + bytes(broken))
    transport.chunks.put_nowait(pia(177, address=1) + block(4, bytes(52), address=4))
    await settle()
    assert not task.done()
    clock.advance(10)
    await task
    output = capsys.readouterr().out
    assert "bus address 1: cycles=177, reported SoC=70.0%" in output
    assert "bus address 2: cycles=100, reported SoC=70.0%" in output
    assert "bus address 4: cycles=unknown, reported SoC=unknown" in output
    assert "bus address 3:" not in output
    assert transport.closed
