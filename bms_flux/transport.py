"""Supervise read-only serial (exclusive 19200 8N1) and raw TCP byte streams."""

import asyncio
import errno
import logging
from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import urlsplit

import serial_asyncio_fast

from bms_flux.clock import Clock
from bms_flux.settings import IDLE_REOPEN_S, RECONNECT_BACKOFF_MAX_S, RECONNECT_BACKOFF_MIN_S

_LOGGER = logging.getLogger(__name__)
BusState = Literal["connected", "reconnecting", "port_busy"]

BAUD_RATE = 19200
DATA_BITS = 8
PARITY = "N"
STOP_BITS = 1


class ReadTransport(Protocol):
    """A read-only byte stream; ``generation`` counts opens of the underlying link."""

    generation: int

    async def read(self) -> bytes: ...


class SerialTransport:
    def __init__(self, reader: asyncio.StreamReader, transport: asyncio.Transport) -> None:
        self._reader = reader
        self._transport = transport
        self.generation = 0

    @classmethod
    async def open(cls, bus: str) -> "SerialTransport":
        if not (
            bus.startswith("/dev/") and len(Path(bus).parts) >= 3 and ".." not in Path(bus).parts
        ):
            raise ValueError("serial bus must be an absolute /dev/... path")
        reader = asyncio.StreamReader()
        transport, _ = await serial_asyncio_fast.create_serial_connection(
            asyncio.get_running_loop(),
            lambda: asyncio.StreamReaderProtocol(reader),
            bus,
            baudrate=BAUD_RATE,
            bytesize=DATA_BITS,
            parity=PARITY,
            stopbits=STOP_BITS,
            exclusive=True,
        )
        return cls(reader, transport)

    async def read(self) -> bytes:
        return await self._reader.read(4096)

    def close(self) -> None:
        self._transport.close()


class TCPTransport:
    """Listen to a raw transparent gateway without sending any application bytes."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self.generation = 0

    @classmethod
    async def open(cls, bus: str) -> "TCPTransport":
        target = urlsplit(bus)
        reader, writer = await asyncio.open_connection(target.hostname, target.port)
        return cls(reader, writer)

    async def read(self) -> bytes:
        return await self._reader.read(4096)

    def close(self) -> None:
        self._writer.close()


class SupervisedTransport:
    """Retry transport faults forever; an empty read is a drop, never bridge EOF."""

    def __init__(self, bus: str, clock: Clock) -> None:
        self._bus = bus
        self._clock = clock
        self._transport: SerialTransport | TCPTransport | None = None
        self.state: BusState = "reconnecting"
        self.generation = 0
        self._delay = RECONNECT_BACKOFF_MIN_S
        self._last_warning: float | None = None

    async def _read(self) -> bytes:
        assert self._transport is not None
        read = asyncio.create_task(self._transport.read())
        idle = asyncio.create_task(self._clock.sleep(IDLE_REOPEN_S))
        try:
            done, _ = await asyncio.wait((read, idle), return_when=asyncio.FIRST_COMPLETED)
            if read in done:
                return read.result()
            raise TimeoutError("no bus bytes for 30 s")
        finally:
            read.cancel()
            idle.cancel()
            await asyncio.gather(read, idle, return_exceptions=True)

    async def read(self) -> bytes:
        while True:
            try:
                if self._transport is None:
                    async with asyncio.timeout(IDLE_REOPEN_S):
                        if self._bus.startswith("tcp://"):
                            self._transport = await TCPTransport.open(self._bus)
                        else:
                            self._transport = await SerialTransport.open(self._bus)
                    self.state = "connected"
                    self.generation += 1
                data = await self._read()
                if not data:
                    raise ConnectionError("bus transport closed")
            except OSError as error:
                self.close()
                busy = not self._bus.startswith("tcp://") and (
                    error.errno in (errno.EBUSY, errno.EAGAIN)
                    or "exclusively lock" in str(error).lower()
                )
                self.state = "port_busy" if busy else "reconnecting"
                now = self._clock.now()
                if self._last_warning is None or now - self._last_warning >= 60:
                    cause = (
                        "port busy, is bms_ble or another sniffer holding it?"
                        if busy
                        else str(error)
                    )
                    _LOGGER.warning("Bus transport unavailable: %s", cause)
                    self._last_warning = now
                await self._clock.sleep(self._delay)
                self._delay = min(self._delay * 2, RECONNECT_BACKOFF_MAX_S)
            else:
                if self._last_warning is not None:
                    _LOGGER.info("Bus transport recovered")
                self._last_warning = None
                self._delay = RECONNECT_BACKOFF_MIN_S
                return data

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None
