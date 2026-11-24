"""Wire the read-only transport through framing and decoding to one reading stream."""

import asyncio

from bms_flux.clock import Clock
from bms_flux.frames import FrameSplitter
from bms_flux.ha import SnapshotPublisher
from bms_flux.mqtt import MQTTSink
from bms_flux.protocol import ProtocolDecoder
from bms_flux.readings import ReadingStream
from bms_flux.settings import Pack
from bms_flux.transport import ReadTransport


class Bridge:
    def __init__(
        self,
        transport: ReadTransport,
        decoder: ProtocolDecoder,
        sink: MQTTSink,
        packs: list[Pack],
        clock: Clock,
    ) -> None:
        self._transport = transport
        self._decoder = decoder
        self._clock = clock
        self._publisher = SnapshotPublisher(sink, packs, clock)

    async def run(self) -> None:
        stream = ReadingStream()
        splitter = FrameSplitter()
        await self._publisher.discover()
        async with asyncio.TaskGroup() as tasks:
            consumer = tasks.create_task(self._publisher.consume(stream))
            publisher = tasks.create_task(self._publisher.publish_snapshots())
            try:
                while True:
                    data = await self._transport.read()
                    for frame in splitter.feed(data, final=not data):
                        if frame.kind == "resp":
                            reading = self._decoder.decode(frame.data, self._clock.now())
                            if reading is not None:
                                await stream.put(reading)
                    if not data:
                        await stream.join()
                        break
            finally:
                consumer.cancel()
                publisher.cancel()
