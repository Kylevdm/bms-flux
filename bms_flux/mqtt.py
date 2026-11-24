"""Expose MQTT publication and retained discovery reads through one broker seam.

MQTT has no end-of-retained marker. Startup collects retained discovery for one
second using the Clock seam, then unsubscribes before publishing discovery.
"""

import asyncio
from typing import Protocol

import aiomqtt

from bms_flux.clock import Clock


class MQTTSink(Protocol):
    async def read_retained(self, topic_filter: str, clock: Clock) -> list[tuple[str, str]]: ...

    async def publish(self, topic: str, payload: str, *, retain: bool = False) -> None: ...


class BrokerSink:
    def __init__(self, client: aiomqtt.Client) -> None:
        self._client = client

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

    async def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        await self._client.publish(topic, payload, retain=retain)
