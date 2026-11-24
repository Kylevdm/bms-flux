"""Fan out trusted readings from the read-only transport to independent consumers."""

import asyncio
import json
from pathlib import Path

from bms_flux.clock import Clock
from bms_flux.drift.findings import FindingStore
from bms_flux.drift.full_charge import RULE, FullCharge
from bms_flux.frames import FrameSplitter
from bms_flux.ha import SnapshotPublisher
from bms_flux.identity import PackIdentity
from bms_flux.mqtt import MQTTSink
from bms_flux.protocol import ProtocolDecoder
from bms_flux.readings import DecodedBlock, ReadingStream
from bms_flux.settings import DriftFullCharge, Pack
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
        drift_full_charge: DriftFullCharge | None = None,
        state_dir: Path = Path("state"),
    ) -> None:
        self._transport = transport
        self._decoder = decoder
        self._clock = clock
        self._sink = sink
        self._identity = PackIdentity(packs, silent_after_s)
        self._unmapped: set[int] = set()
        self._lock = asyncio.Lock()
        self._packs = packs
        self._store = FindingStore(state_dir, sink)
        self._full_charge = (
            FullCharge(drift_full_charge, self._store, self._identity, silent_after_s)
            if drift_full_charge is not None and drift_full_charge.enabled
            else None
        )
        self._publisher = SnapshotPublisher(
            sink,
            packs,
            clock,
            self._identity,
            self._lock,
            snapshot_interval_s,
            finding_rules=[RULE] if self._full_charge is not None else [],
        )

    async def republish_findings(self) -> None:
        """Republish all configured finding states after broker reconnect."""
        if self._full_charge is not None:
            for pack in self._packs:
                await self._store.publish(RULE, pack.serial)

    async def run(self) -> None:
        stream = ReadingStream()
        splitter = FrameSplitter()
        finding_stream = ReadingStream()
        if self._full_charge is not None:
            await asyncio.to_thread(self._store.load)
        await self._publisher.discover()
        await self.republish_findings()
        await self._sink.publish(
            "bms_flux/unmapped_addresses", json.dumps({"unmapped_addresses": []})
        )
        await self._publisher.sync_links()
        await self._sink.publish("bms_flux/status", "online", retain=True)
        async with asyncio.TaskGroup() as tasks:
            consumer = tasks.create_task(self._publisher.consume(stream))
            publisher = tasks.create_task(self._publisher.publish_snapshots())
            findings = (
                tasks.create_task(self._full_charge.consume(finding_stream))
                if self._full_charge is not None
                else None
            )
            try:
                while True:
                    data = await self._transport.read()
                    for frame in splitter.feed(data, final=not data):
                        if frame.kind == "resp":
                            async with self._lock:
                                self._identity.expire(self._clock.now())
                                await self._publisher.sync_links()
                                if self._full_charge is not None:
                                    self._full_charge.reset_unavailable()
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
                                        if findings is not None:
                                            await finding_stream.put(reading)
                                    await stream.join()
                                    await finding_stream.join()
                                    await self._publisher.sync_links()
                    if not data:
                        await stream.join()
                        break
            finally:
                consumer.cancel()
                publisher.cancel()
                if findings is not None:
                    findings.cancel()
