"""Fan out trusted readings from the read-only transport to independent consumers."""

import asyncio
import json
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

from bms_flux.api import LocalAPI
from bms_flux.bank import BankPublisher
from bms_flux.clock import Clock
from bms_flux.drift.findings import FindingStore
from bms_flux.drift.full_charge import RULE as FULL_CHARGE_RULE
from bms_flux.drift.full_charge import FullCharge
from bms_flux.drift.rested_ocv import RULE as RESTED_OCV_RULE
from bms_flux.drift.rested_ocv import RestedOCV
from bms_flux.frames import FrameSplitter
from bms_flux.ha import SnapshotPublisher
from bms_flux.identity import PackIdentity
from bms_flux.mqtt import MQTTSink
from bms_flux.protocol import ProtocolDecoder
from bms_flux.readings import DecodedBlock, ReadingStream
from bms_flux.settings import DriftFullCharge, DriftRestedOCV, OCVPoint, Pack
from bms_flux.stream import StreamPublisher
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
        drift_rested_ocv: DriftRestedOCV | None = None,
        ocv_table: list[OCVPoint] | None = None,
        stream: bool = True,
        raw_frames: bool = False,
        api_token: str = "",
    ) -> None:
        self._transport = transport
        self._decoder = decoder
        self._clock = clock
        self._sink = sink
        self._identity = PackIdentity(packs, silent_after_s)
        self.api = LocalAPI(api_token, self._identity, raw_frames=raw_frames) if api_token else None
        self._unmapped: set[int] = set()
        self._lock = asyncio.Lock()
        self._packs = packs
        self._store = FindingStore(state_dir, sink)
        self._rules: dict[str, FullCharge | RestedOCV] = {}
        if drift_full_charge is not None and drift_full_charge.enabled:
            self._rules[FULL_CHARGE_RULE] = FullCharge(
                drift_full_charge, self._store, self._identity, silent_after_s
            )
        if drift_rested_ocv is not None and drift_rested_ocv.enabled:
            self._rules[RESTED_OCV_RULE] = RestedOCV(
                drift_rested_ocv, self._store, self._identity, silent_after_s, ocv_table or []
            )
        self._bank = BankPublisher(sink, clock, self._identity, self._lock, snapshot_interval_s)
        self._publisher = SnapshotPublisher(
            sink,
            packs,
            clock,
            self._identity,
            self._lock,
            snapshot_interval_s,
            finding_rules=list(self._rules),
        )
        self._raw_frames = raw_frames
        self._stream_publisher = StreamPublisher(sink, clock, stream=stream, raw_frames=raw_frames)
        # Every consumer of trusted readings gets its own queue (the fan-out).
        self._consumers: list[Callable[[ReadingStream], Coroutine[Any, Any, None]]] = [
            self._publisher.consume,
            self._bank.consume,
        ]
        self._consumers.extend(rule.consume for rule in self._rules.values())
        if stream:
            self._consumers.append(self._stream_publisher.consume)
            if self.api is not None:
                self._consumers.append(self.api.consume)

    async def republish_findings(self) -> None:
        """Republish all configured finding states after broker reconnect."""
        for rule in self._rules:
            for pack in self._packs:
                await self._store.publish(rule, pack.serial)

    async def run(self) -> None:
        queues = [ReadingStream() for _ in self._consumers]
        splitter = FrameSplitter()
        if self._rules:
            await asyncio.to_thread(self._store.load)
        await self._bank.clean_discovery()
        await self._publisher.discover()
        await self._bank.discover()
        await self._bank.sync()
        await self._bank.publish_health()
        await self.republish_findings()
        await self._sink.publish(
            "bms_flux/unmapped_addresses", json.dumps({"unmapped_addresses": []})
        )
        await self._publisher.sync_links()
        await self._sink.publish("bms_flux/status", "online", retain=True)
        async with asyncio.TaskGroup() as tasks:
            running: list[asyncio.Task[None]] = [
                tasks.create_task(consume(queue))
                for consume, queue in zip(self._consumers, queues, strict=True)
            ]
            running.append(tasks.create_task(self._publisher.publish_snapshots()))
            running.append(tasks.create_task(self._bank.publish_snapshots()))
            try:
                while True:
                    data = await self._transport.read()
                    frames = splitter.feed(data, final=not data)
                    self._bank.record_frames(frames, splitter.crc_errors)
                    await self._bank.publish_health()
                    for frame in frames:
                        if self._raw_frames:
                            ts = self._clock.now()
                            if self.api is not None:
                                self.api.publish_raw(frame.data, ts)
                            await self._stream_publisher.publish_raw(frame.data, ts=ts)
                        if frame.kind == "resp":
                            async with self._lock:
                                self._identity.expire(self._clock.now())
                                await self._publisher.sync_links()
                                for rule in self._rules.values():
                                    rule.reset_unavailable()
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
                                        for queue in queues:
                                            await queue.put(reading)
                                    for queue in queues:
                                        await queue.join()
                                    await self._publisher.sync_links()
                                    await self._bank.sync()
                    if not data:
                        for queue in queues:
                            await queue.join()
                        break
            finally:
                for task in running:
                    task.cancel()
