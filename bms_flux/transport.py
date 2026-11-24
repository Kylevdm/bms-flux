"""Own a read-only async byte-stream seam and an exclusive 19200 8N1 serial port."""

import asyncio
from typing import Protocol

import serial_asyncio_fast

BAUD_RATE = 19200
DATA_BITS = 8
PARITY = "N"
STOP_BITS = 1


class ReadTransport(Protocol):
    async def read(self) -> bytes: ...


class SerialTransport:
    def __init__(self, reader: asyncio.StreamReader, transport: asyncio.Transport) -> None:
        self._reader = reader
        self._transport = transport

    @classmethod
    async def open(cls, bus: str) -> "SerialTransport":
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
