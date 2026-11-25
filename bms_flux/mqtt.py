"""Expose MQTT publication and retained discovery reads through one broker seam.

MQTT has no end-of-retained marker. Startup collects retained discovery for one
second using the Clock seam, then unsubscribes before publishing discovery.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Protocol

import aiomqtt

from bms_flux.clock import Clock
from bms_flux.settings import RECONNECT_BACKOFF_MAX_S, RECONNECT_BACKOFF_MIN_S

_LOG = logging.getLogger(__name__)


class MQTTSink(Protocol):
    async def read_retained(self, topic_filter: str, clock: Clock) -> list[tuple[str, str]]: ...

    async def publish(
        self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
    ) -> None: ...


class BrokerConnection(MQTTSink, Protocol):
    async def wait_disconnected(self) -> None: ...


class ContainedSink:
    """Keep a publication failure out of the core read loop."""

    def __init__(self, sink: MQTTSink) -> None:
        self._sink = sink

    async def read_retained(self, topic_filter: str, clock: Clock) -> list[tuple[str, str]]:
        try:
            return await self._sink.read_retained(topic_filter, clock)
        except Exception as error:
            _LOG.error("MQTT retained read failed (%s)", type(error).__name__)
            return []

    async def publish(
        self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
    ) -> None:
        try:
            await self._sink.publish(topic, payload, retain=retain, qos=qos)
        except Exception as error:
            _LOG.error("MQTT publication failed (%s)", type(error).__name__)


class ReconnectingSink:
    """Drop disconnected publications; restore current state, never replay readings."""

    def __init__(
        self, connect: Callable[[], AbstractAsyncContextManager[BrokerConnection]], clock: Clock
    ) -> None:
        self._connect = connect
        self._clock = clock
        self._sink: BrokerConnection | None = None
        self._failed = asyncio.Event()
        self.ready = asyncio.Event()

    async def publish(
        self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
    ) -> None:
        if self._sink is None:
            return
        try:
            await self._sink.publish(topic, payload, retain=retain, qos=qos)
        except aiomqtt.MqttError:
            self._sink = None
            self._failed.set()

    async def read_retained(self, topic_filter: str, clock: Clock) -> list[tuple[str, str]]:
        if self._sink is None:
            return []
        return await self._sink.read_retained(topic_filter, clock)

    async def run(
        self,
        restore: Callable[[], Awaitable[None]],
        reset: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """Reconnect forever; ``reset`` runs before the new connection is publishable."""
        backoff = RECONNECT_BACKOFF_MIN_S
        while True:
            try:
                async with self._connect() as sink:
                    self._failed.clear()
                    if reset is not None:
                        # Outage-sensitive state resets before the new connection
                        # becomes publishable to live consumers.
                        await reset()
                    self._sink = sink
                    await restore()
                    self.ready.set()
                    if self._failed.is_set():
                        raise aiomqtt.MqttError("publication failed")
                    backoff = RECONNECT_BACKOFF_MIN_S
                    monitor = asyncio.create_task(sink.wait_disconnected())
                    failed = asyncio.create_task(self._failed.wait())
                    try:
                        done, _ = await asyncio.wait(
                            (monitor, failed), return_when=asyncio.FIRST_COMPLETED
                        )
                        for task in done:
                            task.result()
                    finally:
                        monitor.cancel()
                        failed.cancel()
                        await asyncio.gather(monitor, failed, return_exceptions=True)
            except Exception as error:
                _LOG.warning("MQTT unavailable; reconnecting (%s)", type(error).__name__)
            finally:
                self._sink = None
                self.ready.set()
            await self._clock.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_S)


class BrokerSink:
    def __init__(self, client: aiomqtt.Client) -> None:
        self._client = client

    async def wait_disconnected(self) -> None:
        async for _ in self._client.messages:
            pass

    async def read_retained(self, topic_filter: str, clock: Clock) -> list[tuple[str, str]]:
        retained: list[tuple[str, str]] = []
        await self._client.subscribe(topic_filter)

        async def collect() -> None:
            async for message in self._client.messages:
                if message.retain:
                    payload = message.payload
                    text = (
                        bytes(payload).decode("utf-8", errors="replace")
                        if isinstance(payload, (bytes, bytearray))
                        else str(payload)
                    )
                    retained.append((str(message.topic), text))

        collector = asyncio.create_task(collect())
        deadline = asyncio.create_task(clock.sleep(1))
        try:
            done, _ = await asyncio.wait((collector, deadline), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            collector.cancel()
            deadline.cancel()
            await asyncio.gather(collector, deadline, return_exceptions=True)
            await self._client.unsubscribe(topic_filter)
        return retained

    async def publish(
        self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
    ) -> None:
        await self._client.publish(topic, payload, qos=qos, retain=retain)
