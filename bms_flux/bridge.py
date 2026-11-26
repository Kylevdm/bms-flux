"""Fan out trusted readings from the read-only transport to independent consumers.

The core loop never waits on a consumer: each one owns a bounded queue, and a
full queue drops its oldest reading. Quarantine is enforced at emission time:
no output publishes under a pack serial whose link is fingerprint_mismatch.
"""

import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from contextlib import suppress
from functools import partial
from pathlib import Path
from typing import Any

from bms_flux.api import LocalAPI
from bms_flux.bank import BankPublisher
from bms_flux.clock import Clock
from bms_flux.drift.divergence import Divergence
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
from bms_flux.settings import DriftDivergence, DriftFullCharge, DriftRestedOCV, OCVPoint, Pack
from bms_flux.stream import StreamPublisher
from bms_flux.transport import BusState, ReadTransport


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

# Ten seconds of bus readings at the sniffed rate of about 30 blocks/s.
CONSUMER_QUEUE_LIMIT = 300


class _ConsumerQueue(ReadingStream):
    """One consumer's bounded fan-out queue; a stall drops the oldest reading."""

    def __init__(self) -> None:
        super().__init__(maxsize=CONSUMER_QUEUE_LIMIT)
        self.dropped = 0

    def offer(self, reading: Reading) -> None:
        try:
            self.put_nowait(reading)
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                self.get_nowait()
            self.dropped += 1
            # A stalled consumer drops ~30 readings/s: log the first drop and every 100th.
            if self.dropped % 100 == 1:
                _LOG.warning(
                    "Consumer queue full; dropped the oldest reading (total %d)", self.dropped
                )
            self.put_nowait(reading)


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
        drift_divergence: DriftDivergence | None = None,
        state_dir: Path = Path("state"),
        drift_rested_ocv: DriftRestedOCV | None = None,
        ocv_table: list[OCVPoint] | None = None,
        stream: bool = True,
        raw_frames: bool = False,
        api_token: str = "",
        bus_state: Callable[[], BusState] = lambda: "connected",
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
        self._store = FindingStore(state_dir, sink, self._identity)
        self._divergence: Divergence | None = None
        if drift_divergence is not None and drift_divergence.enabled:
            self._divergence = Divergence(drift_divergence, sink)
        self._rules: dict[str, FullCharge | RestedOCV] = {}
        if drift_full_charge is not None and drift_full_charge.enabled:
            self._rules[FULL_CHARGE_RULE] = FullCharge(
                drift_full_charge, self._store, self._identity, silent_after_s
            )
        if drift_rested_ocv is not None and drift_rested_ocv.enabled:
            self._rules[RESTED_OCV_RULE] = RestedOCV(
                drift_rested_ocv, self._store, self._identity, silent_after_s, ocv_table or []
            )
        self._bank = BankPublisher(
            sink,
            clock,
            self._identity,
            self._lock,
            snapshot_interval_s,
            bus_state,
            divergence=self._divergence,
        )
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
        self._stream_publisher = StreamPublisher(
            sink, clock, self._identity, stream=stream, raw_frames=raw_frames
        )
        # Every consumer of trusted readings gets its own queue (the fan-out);
        # the bank and snapshot publishers read identity under the core lock instead.
        self._consumers: list[Callable[[ReadingStream], Coroutine[Any, Any, None]]] = [
            self._publisher.consume,
        ]
        self._consumers.extend(rule.consume for rule in self._rules.values())
        if stream:
            self._consumers.append(self._stream_publisher.consume)
            if self.api is not None:
                self._consumers.append(self.api.consume)

    async def republish_findings(self) -> None:
        """Republish all configured finding states after broker reconnect."""
        if self._divergence is not None:
            await self._divergence.publish()
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
        if self._divergence is not None:
            # A broker outage is a gap: the divergence hold starts over.
            self._divergence.reset()
        await self._sink.publish("bms_flux/status", "online", retain=True)
        await self._contain(self._publisher.republish, restart=False)
        await self._contain(self._bank.republish, restart=False)
        await self._contain(self.republish_findings, restart=False)
        await self._sink.publish(
            "bms_flux/unmapped_addresses",
            json.dumps({"unmapped_addresses": sorted(self._unmapped)}),
        )

    async def _contain(
        self, operation: Callable[[], Coroutine[Any, Any, None]], *, restart: bool = True
    ) -> None:
        while True:
            try:
                await operation()
                return
            except Exception as error:
                _LOG.exception("Consumer failed (%s)", type(error).__name__)
                if not restart:
                    return
                await asyncio.sleep(0)

    async def _reset_outage_state(self) -> None:
        """Reset outage-sensitive state before a broker reconnect becomes publishable."""
        if self._divergence is not None:
            self._divergence.reset()

    async def run(self) -> None:
        if self._rules:
            await asyncio.to_thread(self._store.load)
        if self._reconnecting is None:
            await self.republish()
            await self._run()
            return
        async with asyncio.TaskGroup() as tasks:
            broker = tasks.create_task(
                self._reconnecting.run(self.republish, self._reset_outage_state)
            )
            try:
                await self._run()
                await self._reconnecting.ready.wait()
            finally:
                broker.cancel()

    async def _run(self) -> None:
        queues = [_ConsumerQueue() for _ in self._consumers]
        splitter = FrameSplitter()
        generation: int | None = None
        async with asyncio.TaskGroup() as tasks:
            running: list[asyncio.Task[None]] = [
                tasks.create_task(self._contain(partial(consume, queue)))
                for consume, queue in zip(self._consumers, queues, strict=True)
            ]
            running.append(tasks.create_task(self._contain(self._publisher.publish_snapshots)))
            running.append(tasks.create_task(self._contain(self._bank.publish_snapshots)))
            try:
                while True:
                    data = await self._transport.read()
                    if not data:
                        raise ConnectionError("unsupervised transport closed")
                    if self._transport.generation != generation:
                        # A reopened transport must not complete another connection's
                        # partial frame: bytes from different connections never combine.
                        generation = self._transport.generation
                        splitter.reset()
                    frames = splitter.feed(data)
                    self._bank.record_frames(frames, splitter.crc_errors)
                    await self._contain(self._bank.publish_health, restart=False)
                    # One turn per chunk, so consumers keep up with a buffered burst
                    # without the decoder ever waiting on any of them.
                    await asyncio.sleep(0)
                    for frame in frames:
                        if self._raw_frames:
                            ts = self._clock.now()
                            if self.api is not None:
                                await self._contain(
                                    partial(self.api.publish_raw, frame.data, ts), restart=False
                                )
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
                                            queue.offer(reading)
                                    await self._contain(self._publisher.sync_links, restart=False)
                                    await self._contain(self._bank.sync, restart=False)
            finally:
                for task in running:
                    task.cancel()
