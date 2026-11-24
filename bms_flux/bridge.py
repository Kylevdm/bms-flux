"""Wire the read-only transport through framing and decoding to one reading stream."""

import asyncio
import json

from bms_flux.bank import BankPublisher
from bms_flux.clock import Clock
from bms_flux.frames import FrameSplitter
from bms_flux.ha import SnapshotPublisher
from bms_flux.identity import PackIdentity
from bms_flux.mqtt import MQTTSink
from bms_flux.protocol import ProtocolDecoder
from bms_flux.readings import DecodedBlock, ReadingStream
from bms_flux.settings import Pack
from bms_flux.transport import ReadTransport


async def probe_blocks(
    transport: ReadTransport, decoder: ProtocolDecoder, clock: Clock, seconds: int
) -> dict[int, DecodedBlock]:
    """Listen for a bounded duration, retaining PIA diagnostics for heard addresses."""
    heard: dict[int, DecodedBlock] = {}

    async def listen() -> None:
        splitter = FrameSplitter()
        while True:
            data = await transport.read()
            for frame in splitter.feed(data, final=not data):
                if frame.kind == "resp":
                    block = decoder.decode(frame.data, clock.now())
                    if block is not None and (block.block == "PIA" or block.address not in heard):
                        heard[block.address] = block
            if not data:
                return

    listener = asyncio.create_task(listen())
    deadline = asyncio.create_task(clock.sleep(seconds))
    try:
        done, _ = await asyncio.wait((listener, deadline), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        listener.cancel()
        deadline.cancel()
        await asyncio.gather(listener, deadline, return_exceptions=True)
    return heard


class Bridge:
    def __init__(
        self,
        transport: ReadTransport,
        decoder: ProtocolDecoder,
        sink: MQTTSink,
        packs: list[Pack],
        clock: Clock,
        snapshot_interval_s: int = 10,
        silent_after_s: int = 10,
    ) -> None:
        self._transport = transport
        self._decoder = decoder
        self._clock = clock
        self._sink = sink
        self._identity = PackIdentity(packs, silent_after_s)
        self._unmapped: set[int] = set()
        self._lock = asyncio.Lock()
        self._bank = BankPublisher(sink, clock, self._identity, self._lock, snapshot_interval_s)
        self._publisher = SnapshotPublisher(
            sink, packs, clock, self._identity, self._lock, snapshot_interval_s
        )

    async def run(self) -> None:
        stream = ReadingStream()
        bank_stream = ReadingStream()
        splitter = FrameSplitter()
        await self._bank.clean_discovery()
        await self._publisher.discover()
        await self._bank.discover()
        await self._bank.sync()
        await self._bank.publish_health()
        await self._sink.publish(
            "bms_flux/unmapped_addresses", json.dumps({"unmapped_addresses": []})
        )
        await self._publisher.sync_links()
        await self._sink.publish("bms_flux/status", "online", retain=True)
        async with asyncio.TaskGroup() as tasks:
            consumer = tasks.create_task(self._publisher.consume(stream))
            publisher = tasks.create_task(self._publisher.publish_snapshots())
            bank_consumer = tasks.create_task(self._bank.consume(bank_stream))
            bank_publisher = tasks.create_task(self._bank.publish_snapshots())
            try:
                while True:
                    data = await self._transport.read()
                    frames = splitter.feed(data, final=not data)
                    self._bank.record_frames(frames, splitter.crc_errors)
                    await self._bank.publish_health()
                    for frame in frames:
                        if frame.kind == "resp":
                            async with self._lock:
                                self._identity.expire(self._clock.now())
                                await self._publisher.sync_links()
                                block = self._decoder.decode(frame.data, self._clock.now())
                                if block is not None:
                                    if (
                                        block.address not in self._identity.serials
                                        and block.address not in self._unmapped
                                    ):
                                        self._unmapped.add(block.address)
                                        await self._sink.publish(
                                            "bms_flux/unmapped_addresses",
                                            json.dumps(
                                                {"unmapped_addresses": sorted(self._unmapped)}
                                            ),
                                        )
                                    for reading in self._identity.accept(block, self._clock.now()):
                                        await stream.put(reading)
                                        await bank_stream.put(reading)
                                    await stream.join()
                                    await bank_stream.join()
                                    await self._publisher.sync_links()
                                    await self._bank.sync()
                    if not data:
                        await stream.join()
                        break
            finally:
                consumer.cancel()
                publisher.cancel()
                bank_consumer.cancel()
                bank_publisher.cancel()
