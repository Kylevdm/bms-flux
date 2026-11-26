"""Exercise read-only TCP recovery and serial exclusivity at the transport seam."""

import asyncio
import logging
import os

import pytest

from bms_flux.transport import SerialTransport, SupervisedTransport, TCPTransport
from tests.helpers import ManualClock, settle


async def test_tcp_reads_without_sending_and_closes() -> None:
    received: list[bytes] = []
    finished = asyncio.Event()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b"bus bytes")
        await writer.drain()
        received.append(await reader.read())
        writer.close()
        await writer.wait_closed()
        finished.set()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    async with server:
        port = server.sockets[0].getsockname()[1]
        transport = await TCPTransport.open(f"tcp://127.0.0.1:{port}")
        assert await transport.read() == b"bus bytes"
        for name in ("write", "send", "drain"):
            assert not hasattr(transport, name)
        transport.close()
        await asyncio.wait_for(finished.wait(), 1)
    assert received == [b""]


async def test_serial_pty_is_exclusive_and_read_only() -> None:
    master, slave = os.openpty()
    transport = await SerialTransport.open(os.ttyname(slave))
    try:
        with pytest.raises(OSError, match="exclusively lock"):
            await SerialTransport.open(os.ttyname(slave))
        os.write(master, b"heard")
        assert await asyncio.wait_for(transport.read(), 1) == b"heard"
        os.set_blocking(master, False)
        with pytest.raises(BlockingIOError):
            os.read(master, 4096)
    finally:
        transport.close()
        await settle()
        os.close(master)
        os.close(slave)


async def test_tcp_idle_reopens_and_drop_backs_off(caplog: pytest.LogCaptureFixture) -> None:
    clock = ManualClock()
    peers: list[asyncio.StreamWriter] = []
    closed: list[bytes] = []

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peers.append(writer)
        closed.append(await reader.read())
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    transport = SupervisedTransport(f"tcp://127.0.0.1:{port}", clock)
    task = asyncio.create_task(transport.read())
    caplog.set_level(logging.INFO)
    try:
        await settle()
        assert transport.state == "connected"
        clock.advance(30)
        await settle()
        assert closed == [b""]
        assert str(transport.state) == "reconnecting"
        clock.advance(1)
        await settle()
        assert len(peers) == 2
        peers[-1].write(b"heard")
        await peers[-1].drain()
        assert await task == b"heard"
        assert transport.state == "connected"
        assert "recovered" in caplog.text
        peers[-1].close()
        await peers[-1].wait_closed()
        server.close()
        await server.wait_closed()
        task = asyncio.create_task(transport.read())
        await settle()
        for delay in (1, 2, 4, 8, 16, 30, 30):
            assert any(
                deadline == clock.now() + delay and not f.done() for deadline, f in clock.waiters
            )
            clock.advance(delay)
            await settle()
        assert str(transport.state) == "reconnecting"
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 3  # idle, drop, then one minute into the drop
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        transport.close()
        server.close()
        await server.wait_closed()
        await settle()


async def test_serial_idle_closes_and_reopens(monkeypatch: pytest.MonkeyPatch) -> None:
    master, slave = os.openpty()
    clock = ManualClock()
    transport = SupervisedTransport(os.ttyname(slave), clock)
    endpoints: list[SerialTransport] = []
    opened = asyncio.Event()
    open_serial = SerialTransport.open

    async def track_open(bus: str) -> SerialTransport:
        endpoint = await open_serial(bus)
        endpoints.append(endpoint)
        opened.set()
        return endpoint

    monkeypatch.setattr(SerialTransport, "open", track_open)
    task = asyncio.create_task(transport.read())
    try:
        await asyncio.wait_for(opened.wait(), 1)
        await settle()
        opened.clear()
        clock.advance(29)
        await settle()
        assert len(endpoints) == 1
        assert transport.state == "connected"
        clock.advance(1)
        await settle()
        assert await asyncio.wait_for(endpoints[0].read(), 1) == b""
        clock.advance(1)
        await asyncio.wait_for(opened.wait(), 1)
        assert len(endpoints) == 2
        os.write(master, b"after idle")
        assert await asyncio.wait_for(task, 1) == b"after idle"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        transport.close()
        if endpoints:
            await asyncio.wait_for(endpoints[-1].read(), 1)
        os.close(master)
        os.close(slave)


async def test_busy_port_retries_then_recovers(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    master, slave = os.openpty()
    bus = os.ttyname(slave)
    held = await SerialTransport.open(bus)
    opened = asyncio.Event()
    attempted = asyncio.Event()
    open_serial = SerialTransport.open

    async def track_open(bus: str) -> SerialTransport:
        try:
            endpoint = await open_serial(bus)
            opened.set()
            return endpoint
        finally:
            attempted.set()

    monkeypatch.setattr(SerialTransport, "open", track_open)
    clock = ManualClock()
    transport = SupervisedTransport(bus, clock)
    task = asyncio.create_task(transport.read())
    caplog.set_level(logging.INFO)
    try:
        await asyncio.wait_for(attempted.wait(), 1)
        assert transport.state == "port_busy"
        assert "port busy, is bms_ble or another sniffer holding it?" in caplog.text
        held.close()
        assert await asyncio.wait_for(held.read(), 1) == b""
        clock.advance(1)
        await asyncio.wait_for(opened.wait(), 1)
        os.write(master, b"recovered")
        assert await asyncio.wait_for(task, 1) == b"recovered"
        assert str(transport.state) == "connected"
        assert "recovered" in caplog.text
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        held.close()
        transport.close()
        await settle()
        os.close(master)
        os.close(slave)
