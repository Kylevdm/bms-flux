"""Rolling bus health is observable without giving corrupt bytes to packs."""

import asyncio

import pytest

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT
from tests.test_bank import full_set, values


async def settle() -> None:
    # Large batches must pass through both reading queues before the next chunk.
    for _ in range(10000):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_bus_health_crc_hysteresis_window_and_pack_isolation() -> None:
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(
        Bridge(transport, SeplosV3Decoder(), sink, [Pack(address=1, serial="A")], clock).run()
    )
    try:
        await settle()
        config = values(sink, "homeassistant/device/bms_flux_bridge/config")[-1]
        assert config["components"]["bus_problem"]["device_class"] == "problem"
        assert values(sink, "bms_flux/bus/state")[-1]["bus_problem"] is True
        transport.chunks.put_nowait(full_set() * 100)
        await settle()
        healthy = values(sink, "bms_flux/bus/state")[-1]
        assert healthy["frames_per_second"] == 5
        assert healthy["crc_errors"] == 0
        assert healthy["master_pack_seen"] is True
        assert healthy["bus_problem"] is False
        # Corrupt one complete PIA, in separate byte chunks. Count it once, not
        # once per feed call or once for each junk byte encountered in resync.
        corrupt = bytearray(full_set()[:41])
        corrupt[-1] ^= 1
        for _ in range(16):
            for byte in corrupt:
                transport.chunks.put_nowait(bytes([byte]))
        transport.chunks.put_nowait(b"\xff" * 50 + full_set())
        await settle()
        unhealthy = values(sink, "bms_flux/bus/state")[-1]
        assert unhealthy["crc_errors"] == 16
        assert unhealthy["bus_problem"] is True  # 16 / 319 > 5 %
        assert values(sink, "bms_flux/A/link") == ["silent", "ok"]
        transport.chunks.put_nowait(full_set() * 100)
        await settle()
        assert values(sink, "bms_flux/bus/state")[-1]["bus_problem"] is True  # hysteresis
        transport.chunks.put_nowait(full_set() * 400)
        await settle()
        assert values(sink, "bms_flux/bus/state")[-1]["bus_problem"] is False
        clock.advance(60)
        await settle()
        idle = values(sink, "bms_flux/bus/state")[-1]
        assert idle["frames_per_second"] == 0
        assert idle["bus_problem"] is True
        assert idle["master_pack_seen"] is False
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_bus_health_is_not_republished_on_every_read() -> None:
    from bms_flux.bridge import Bridge
    from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
    from bms_flux.settings import Pack
    from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, frame, settle

    packs = [Pack(address=1, serial="A")]
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    task = asyncio.create_task(Bridge(transport, SeplosV3Decoder(), sink, packs, clock).run())
    try:
        await settle()
        transport.chunks.put_nowait(frame())
        await settle()
        before = len([m for m in sink.messages if m[0] == "bms_flux/bus/state"])
        for _ in range(20):
            clock.advance(0.1)
            transport.chunks.put_nowait(frame())
            await settle()
        after = len([m for m in sink.messages if m[0] == "bms_flux/bus/state"])
        assert after - before <= 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
