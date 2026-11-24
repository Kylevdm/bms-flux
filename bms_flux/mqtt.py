"""Expose an MQTT publish seam and the real asynchronous broker sink."""

from typing import Protocol

import aiomqtt


class MQTTSink(Protocol):
    async def publish(self, topic: str, payload: str, *, retain: bool = False) -> None: ...


class BrokerSink:
    def __init__(self, client: aiomqtt.Client) -> None:
        self._client = client

    async def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
        await self._client.publish(topic, payload, retain=retain)
