"""Fan out trusted readings from the read-only transport to independent consumers."""

import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from functools import partial
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
from bms_flux.mqtt import ContainedSink, MQTTSink, ReconnectingSink
from bms_flux.protocol import ProtocolDecoder
from bms_flux.readings import DecodedBlock, Reading, ReadingStream
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


_LOG = logging.getLogger(__name__)


class _ConsumerStream(ReadingStream):
    """Track the in-flight reading so a failed consumer cannot strand join()."""

    active = False

    async def get(self) -> Reading:
        reading = await super().get()
        self.active = True
        return reading

    def task_done(self) -> None:
        self.active = False
        super().task_done()


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
        self._reconnecting = sink if isinstance(sink, ReconnectingSink) else None
        sink = ContainedSink(sink)
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

    async def republish(self) -> None:
        """Restore discovery and current state without replaying the stream."""
        await self._contain(self._bank.clean_discovery, restart=False)
        await self._contain(self._publisher.discover, restart=False)
        await self._contain(self._bank.discover, restart=False)
        async with self._lock:
            await self._republish()

    async def _republish(self) -> None:
        await self._sink.publish("bms_flux/status", "online", retain=True)
        await self._contain(self._publisher.republish, restart=False)
        await self._contain(self._bank.republish, restart=False)
        await self._contain(self.republish_findings, restart=False)
        await self._sink.publish(
            "bms_flux/unmapped_addresses",
            json.dumps({"unmapped_addresses": sorted(self._unmapped)}),
        )

    async def _contain(
        self,
        operation: Callable[[], Coroutine[Any, Any, None]],
        queue: _ConsumerStream | None = None,
        *,
        restart: bool = True,
    ) -> None:
        while True:
            try:
                await operation()
                return
            except Exception as error:
                _LOG.exception("Consumer failed (%s)", type(error).__name__)
                if queue is not None:
                    if not queue.active:
                        await queue.get()
                    queue.task_done()
                if not restart:
                    return
                await asyncio.sleep(0)

    async def run(self) -> None:
        if self._rules:
            await asyncio.to_thread(self._store.load)
        if self._reconnecting is None:
            await self.republish()
            await self._run()
            return
        async with asyncio.TaskGroup() as tasks:
            broker = tasks.create_task(self._reconnecting.run(self.republish))
            try:
                await self._run()
                await self._reconnecting.ready.wait()
            finally:
                broker.cancel()

    async def _run(self) -> None:
        queues = [_ConsumerStream() for _ in self._consumers]
        splitter = FrameSplitter()
        async with asyncio.TaskGroup() as tasks:
            running: list[asyncio.Task[None]] = [
                tasks.create_task(self._contain(partial(consume, queue), queue))
                for consume, queue in zip(self._consumers, queues, strict=True)
            ]
            running.append(tasks.create_task(self._contain(self._publisher.publish_snapshots)))
            running.append(tasks.create_task(self._contain(self._bank.publish_snapshots)))
            try:
                while True:
                    data = await self._transport.read()
                    frames = splitter.feed(data, final=not data)
                    self._bank.record_frames(frames, splitter.crc_errors)
                    await self._contain(self._bank.publish_health, restart=False)
                    for frame in frames:
                        if self._raw_frames:
                            ts = self._clock.now()
                            if self.api is not None:
                                self.api.publish_raw(frame.data, ts)
                            await self._contain(
                                partial(self._stream_publisher.publish_raw, frame.data, ts=ts),
                                restart=False,
                            )
                        if frame.kind == "resp":
                            async with self._lock:
                                self._identity.expire(self._clock.now())
                                await self._contain(self._publisher.sync_links, restart=False)
                                for rule in self._rules.values():
                                    try:
                                        rule.reset_unavailable()
                                    except Exception as error:
                                        _LOG.exception(
                                            "Drift rule failed (%s)", type(error).__name__
                                        )
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
                                    await self._contain(self._publisher.sync_links, restart=False)
                                    await self._contain(self._bank.sync, restart=False)
                    if not data:
                        for queue in queues:
                            await queue.join()
                        break
            finally:
                for task in running:
                    task.cancel()
