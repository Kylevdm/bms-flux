"""Verify serial opening uses fixed settings and exposes only bus reading."""

import asyncio
from collections.abc import Callable

import pytest

from bms_flux.transport import ReadTransport, SerialTransport


class SerialEndpoint(asyncio.Transport):
    """Any write would raise via the base class; closing only records cleanup."""

    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    def close(self) -> None:
        self.closed = True


async def test_serial_is_exclusive_fixed_19200_8n1_and_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = SerialEndpoint()

    async def open_serial(
        loop: asyncio.AbstractEventLoop,
        factory: Callable[[], asyncio.Protocol],
        bus: str,
        **kwargs: object,
    ) -> tuple[asyncio.Transport, asyncio.Protocol]:
        assert loop is asyncio.get_running_loop()
        assert bus == "/dev/serial/by-id/example"
        assert kwargs == {
            "baudrate": 19200,
            "bytesize": 8,
            "parity": "N",
            "stopbits": 1,
            "exclusive": True,
        }
        protocol = factory()
        assert isinstance(protocol, asyncio.StreamReaderProtocol)
        protocol.connection_made(endpoint)
        protocol.data_received(b"bus bytes")
        return endpoint, protocol

    monkeypatch.setattr("serial_asyncio_fast.create_serial_connection", open_serial)
    transport = await SerialTransport.open("/dev/serial/by-id/example")
    seam: ReadTransport = transport
    assert await seam.read() == b"bus bytes"
    for name in ("write", "send", "drain"):
        assert not hasattr(seam, name)
        assert not hasattr(ReadTransport, name)
    transport.close()
    assert endpoint.closed
