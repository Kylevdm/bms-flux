"""Full-rate MQTT stream: one message per reading and opt-in raw frames."""

import asyncio
import struct

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, block, frame, settle
from tests.test_identity import full_set, pia


@pytest.mark.asyncio
async def test_stream_publishes_each_mapped_reading_with_envelope() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(transport, SeplosV3Decoder(), sink, packs, clock, silent_after_s=120)
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        stream_messages = [
            (topic, payload, retain, sink.qos[i])
            for i, (topic, payload, retain) in enumerate(sink.messages)
            if topic.endswith("/stream")
        ]
        assert len(stream_messages) == 3
        assert sorted(payload["block"] for _, payload, _, _ in stream_messages) == [
            "PIA",
            "PIB",
            "PIC",
        ]
        for topic, payload, retain, qos in stream_messages:
            assert topic == "bms_flux/PACK/stream"
            assert retain is False
            assert qos == 0
            assert payload["v"] == 1
            assert payload["serial"] == "PACK"
            assert payload["address"] == 1
            assert isinstance(payload["ts"], str)
            assert payload["ts"].endswith("Z")
        pic = next(payload for _, payload, _, _ in stream_messages if payload["block"] == "PIC")
        assert isinstance(pic["alarm"], bool)
        assert isinstance(pic["event_flags"], dict)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_stream_is_not_published_when_disabled() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        packs,
        clock,
        silent_after_s=120,
        stream=False,
    )
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        assert not [t for t, _, _ in sink.messages if t.endswith("/stream")]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_unmapped_and_doubt_packs_are_not_streamed() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(transport, SeplosV3Decoder(), sink, packs, clock, silent_after_s=120)
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        transport.chunks.put_nowait(full_set(address=2))
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        stream_messages = [
            (topic, payload) for topic, payload, _ in sink.messages if topic.endswith("/stream")
        ]
        assert all(payload["serial"] == "PACK" for _, payload in stream_messages)
        assert not any(payload["address"] == 2 for _, payload in stream_messages)

        # Fingerprint mismatch stops the stream for the mapped pack.
        count_before = len(stream_messages)
        transport.chunks.put_nowait(pia(cycles=50))
        await settle()
        transport.chunks.put_nowait(full_set())
        await settle()
        stream_messages = [
            (topic, payload) for topic, payload, _ in sink.messages if topic.endswith("/stream")
        ]
        assert len(stream_messages) == count_before
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_raw_frames_publish_crc_valid_frames_including_requests() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        packs,
        clock,
        silent_after_s=120,
        stream=False,
        raw_frames=True,
    )
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        request = block(4, struct.pack(">HH", 0x1000, 0x0012), address=1)
        response = frame(address=1)
        transport.chunks.put_nowait(request + response)
        await settle()
        raw_messages = [
            (topic, payload, retain, sink.qos[i])
            for i, (topic, payload, retain) in enumerate(sink.messages)
            if topic == "bms_flux/raw"
        ]
        assert len(raw_messages) == 2
        assert raw_messages[0][1]["address"] == 1
        assert raw_messages[0][1]["hex"] == request.hex()
        assert raw_messages[1][1]["hex"] == response.hex()
        for _, payload, retain, qos in raw_messages:
            assert payload["v"] == 1
            assert isinstance(payload["ts"], str)
            assert payload["ts"].endswith("Z")
            assert retain is False
            assert qos == 0
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_failed_crc_frames_are_not_published_raw() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        packs,
        clock,
        silent_after_s=120,
        stream=False,
        raw_frames=True,
    )
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        broken = bytearray(frame())
        broken[-1] ^= 1
        valid = frame(current=300)
        transport.chunks.put_nowait(bytes(broken) + valid)
        await settle()
        raw_hexes = {
            payload["hex"] for topic, payload, _ in sink.messages if topic == "bms_flux/raw"
        }
        assert broken.hex() not in raw_hexes
        assert valid.hex() in raw_hexes
        assert len(raw_hexes) == 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_raw_frames_are_disabled_by_default() -> None:
    packs = [Pack(address=1, serial="PACK")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(transport, SeplosV3Decoder(), sink, packs, clock, silent_after_s=120)
    task = asyncio.create_task(bridge.run())
    try:
        await settle()
        transport.chunks.put_nowait(frame())
        await settle()
        assert not [t for t, _, _ in sink.messages if t == "bms_flux/raw"]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
