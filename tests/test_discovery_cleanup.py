"""Startup discovery cleanup stays within the bms-flux device prefix."""

import asyncio
from collections.abc import AsyncIterator
from typing import cast

import aiomqtt
import pytest

from bms_flux.bridge import Bridge
from bms_flux.mqtt import BrokerSink
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, settle


@pytest.mark.asyncio
async def test_retained_collection_waits_one_clock_second_before_cleanup() -> None:
    sink, clock, transport = MemoryMQTT(), ManualClock(), FakeTransport()
    incoming: asyncio.Queue[aiomqtt.Message] = asyncio.Queue()
    unsubscribed: list[str] = []

    class Client:
        async def subscribe(self, topic_filter: str) -> None:
            sink.subscriptions.append(topic_filter)

        async def unsubscribe(self, topic_filter: str) -> None:
            unsubscribed.append(topic_filter)

        @property
        def messages(self) -> AsyncIterator[aiomqtt.Message]:
            async def messages() -> AsyncIterator[aiomqtt.Message]:
                while True:
                    yield await incoming.get()

            return messages()

        async def publish(self, topic: str, payload: str, *, retain: bool = False) -> None:
            await sink.publish(topic, payload, retain=retain)

    task = asyncio.create_task(
        Bridge(
            transport,
            SeplosV3Decoder(),
            BrokerSink(cast(aiomqtt.Client, Client())),
            [],
            clock,
        ).run()
    )
    try:
        await settle()
        topic = "homeassistant/device/bms_flux_OLD/config"
        incoming.put_nowait(aiomqtt.Message(topic, b"{}", 0, True, 1, None))
        incoming.put_nowait(
            aiomqtt.Message("homeassistant/device/bms_flux_LIVE/config", b"{}", 0, False, 2, None)
        )
        await settle()
        clock.advance(0.99)
        await settle()
        assert not sink.messages
        assert not unsubscribed
        clock.advance(0.01)
        await settle()
        assert unsubscribed == ["homeassistant/device/+/config"]
        assert [(t, retain) for t, p, retain in sink.messages if p == ""] == [(topic, True)]
        assert ("bms_flux/status", "online", True) in sink.messages
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [True, False])
async def test_retained_cleanup_only_removes_unconfigured_pack_devices(configured: bool) -> None:
    sink = MemoryMQTT()
    topics = [
        "homeassistant/device/bms_flux_OLD/config",
        "homeassistant/device/bms_flux_KEEP/config",
        "homeassistant/device/bms_flux_bridge/config",
        "homeassistant/device/other_OLD/config",
        "other/device/bms_flux_OLD/config",
        "homeassistant/device/bms_flux_OLD/not_config",
        "homeassistant/device/bms_flux_OLD/extra/config",
    ]
    sink.retained = [(topic, "{}") for topic in topics]
    packs = [Pack(address=1, serial="KEEP")] if configured else []
    task = asyncio.create_task(
        Bridge(FakeTransport(), SeplosV3Decoder(), sink, packs, ManualClock()).run()
    )
    try:
        await settle()
        assert sink.subscriptions == ["homeassistant/device/+/config"]
        cleared = [(topic, retain) for topic, payload, retain in sink.messages if payload == ""]
        expected = [(topics[0], True)]
        if not configured:
            expected.append((topics[1], True))
        assert cleared == expected
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
