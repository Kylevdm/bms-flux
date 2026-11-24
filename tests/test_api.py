"""Local API behaviour through CRC-valid byte chunks at the bridge seam."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from aiohttp import WSMsgType, web
from aiohttp.test_utils import TestClient, TestServer

from bms_flux.bridge import Bridge
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import Pack
from tests.helpers import FakeTransport, ManualClock, MemoryMQTT, frame, settle
from tests.test_identity import full_set, pia

AUTH = {"Authorization": "Bearer test-only-token"}


@asynccontextmanager
async def running_api(
    tmp_path: Path, *, stream: bool = True, raw_frames: bool = False
) -> AsyncIterator[
    tuple[TestClient[web.Request, web.Application], FakeTransport, MemoryMQTT, ManualClock]
]:
    transport, sink, clock = FakeTransport(), MemoryMQTT(), ManualClock()
    bridge = Bridge(
        transport,
        SeplosV3Decoder(),
        sink,
        [Pack(address=1, serial="PACK"), Pack(address=2, serial="OTHER")],
        clock,
        state_dir=tmp_path,
        stream=stream,
        raw_frames=raw_frames,
        api_token="test-only-token",
    )
    assert bridge.api is not None
    async with TestClient(TestServer(bridge.api.app)) as client:
        task = asyncio.create_task(bridge.run())
        try:
            await settle()
            yield client, transport, sink, clock
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


@pytest.mark.asyncio
async def test_api_requires_bearer_token_on_every_request(tmp_path: Path) -> None:
    async with running_api(tmp_path) as (client, _, _, _):
        for path in ("/packs", "/stream", "/unknown"):
            for headers in (
                {},
                {"Authorization": "Bearer wrong"},
                {"Authorization": "test-only-token"},
            ):
                response = await client.get(path, headers=headers)
                assert response.status == 401
        assert (await client.get("/packs", headers=AUTH)).status == 200
        assert (await client.post("/packs", headers=AUTH)).status == 405
        assert (await client.get("/v1/packs", headers=AUTH)).status == 404


@pytest.mark.asyncio
async def test_packs_keeps_original_readings_when_silent(tmp_path: Path) -> None:
    async with running_api(tmp_path) as (client, transport, sink, clock):
        initial = await (await client.get("/packs", headers=AUTH)).json()
        assert initial["OTHER"] == {"link": "silent", "last_seen": None, "readings": {}}
        clock.advance(1)
        transport.chunks.put_nowait(full_set())
        await settle()
        packs = await (await client.get("/packs", headers=AUTH)).json()
        readings = {p["block"]: p for t, p, _ in sink.messages if t.endswith("/stream")}
        assert packs["PACK"] == {
            "link": "ok",
            "last_seen": readings["PIC"]["ts"],
            "readings": readings,
        }
        clock.advance(11)
        await settle()
        silent = await (await client.get("/packs", headers=AUTH)).json()
        assert silent["PACK"] == {**packs["PACK"], "link": "silent"}
        assert "OTHER" in silent


@pytest.mark.asyncio
async def test_websocket_matches_mqtt_bytes_and_filters(tmp_path: Path) -> None:
    async with (
        running_api(tmp_path) as (client, transport, sink, clock),
        client.ws_connect("/stream", headers=AUTH) as all_readings,
        client.ws_connect("/stream?serial=PACK&block=pia,pib", headers=AUTH) as filtered,
    ):
        clock.advance(2)
        transport.chunks.put_nowait(full_set() + full_set(address=2))
        await settle()
        expected = [p for t, p, _ in sink.messages if t.endswith("/stream")]
        for payload in expected:
            received = await all_readings.receive(timeout=1)
            assert received.type == WSMsgType.TEXT
            assert received.data == json.dumps(payload)
        for payload in expected[:2]:
            assert (await filtered.receive(timeout=1)).data == json.dumps(payload)
        # A regressed fingerprint must not reach either API or MQTT stream.
        transport.chunks.put_nowait(pia(cycles=1))
        await settle()
        packs = await (await client.get("/packs", headers=AUTH)).json()
        assert packs["PACK"]["link"] == "fingerprint_mismatch"
        with pytest.raises(TimeoutError):
            await filtered.receive(timeout=0.02)


@pytest.mark.asyncio
async def test_websocket_raw_is_refused_when_disabled(tmp_path: Path) -> None:
    async with (
        running_api(tmp_path) as (client, _, _, _),
        client.ws_connect("/stream?raw=1", headers=AUTH) as ws,
    ):
        close = await ws.receive(timeout=1)
        assert close.type == WSMsgType.CLOSE
        assert close.extra is not None and "raw_frames" in close.extra


@pytest.mark.asyncio
async def test_websocket_raw_matches_mqtt_and_excludes_bad_crc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with running_api(tmp_path, raw_frames=True) as (client, transport, sink, clock):

        def ticking_now() -> float:
            clock.time += 0.001
            return clock.time

        monkeypatch.setattr(clock, "now", ticking_now)
        async with client.ws_connect("/stream?raw=1", headers=AUTH) as ws:
            broken = bytearray(frame())
            broken[-1] ^= 1
            transport.chunks.put_nowait(bytes(broken) + full_set())
            await settle()
            expected = [
                p for t, p, _ in sink.messages if t.endswith("/stream") or t == "bms_flux/raw"
            ]
            received = [(await ws.receive(timeout=1)).data for _ in expected]
            # The independent consumers may interleave raw frames and decoded readings.
            assert sorted(received) == sorted(json.dumps(p) for p in expected)
            assert all(p.get("hex") != broken.hex() for p in expected)


@pytest.mark.asyncio
async def test_stream_switch_does_not_disable_resync_or_raw(tmp_path: Path) -> None:
    async with (
        running_api(tmp_path, stream=False, raw_frames=True) as (client, transport, sink, _),
        client.ws_connect("/stream?raw=1", headers=AUTH) as ws,
    ):
        transport.chunks.put_nowait(full_set())
        await settle()
        raw = [p for t, p, _ in sink.messages if t == "bms_flux/raw"]
        for payload in raw:
            assert (await ws.receive(timeout=1)).data == json.dumps(payload)
        packs = await (await client.get("/packs", headers=AUTH)).json()
        assert set(packs["PACK"]["readings"]) == {"PIA", "PIB", "PIC"}
        with pytest.raises(TimeoutError):
            await ws.receive(timeout=0.02)


@pytest.mark.asyncio
async def test_slow_client_is_closed_without_blocking_bridge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_send = web.WebSocketResponse.send_str
    stalled: web.WebSocketResponse | None = None
    started = asyncio.Event()
    never = asyncio.Event()

    async def blocked_send(
        ws: web.WebSocketResponse, data: str, compress: int | None = None
    ) -> None:
        nonlocal stalled
        if stalled is None:
            stalled = ws
        if ws is stalled:
            started.set()
            await never.wait()
        await original_send(ws, data, compress=compress)

    # Model socket backpressure, without depending on OS socket buffer sizes.
    monkeypatch.setattr(web.WebSocketResponse, "send_str", blocked_send)
    async with (
        running_api(tmp_path) as (client, transport, sink, clock),
        client.ws_connect("/stream", headers=AUTH) as slow,
    ):
        transport.chunks.put_nowait(full_set())
        await asyncio.wait_for(started.wait(), timeout=1)
        async with client.ws_connect("/stream", headers=AUTH) as healthy:
            transport.chunks.put_nowait(full_set() * 60)
            for _ in range(180):
                assert (await healthy.receive(timeout=1)).type == WSMsgType.TEXT
            close = await slow.receive(timeout=1)
            assert close.type == WSMsgType.CLOSE
            assert close.extra == "too slow"
            assert len([t for t, _, _ in sink.messages if t.endswith("/stream")]) == 183
            before = len([t for t, _, _ in sink.messages if t.endswith("/state")])
            clock.advance(10)
            transport.chunks.put_nowait(full_set())
            await settle()
            assert len([t for t, _, _ in sink.messages if t.endswith("/state")]) > before
            packs = await (await client.get("/packs", headers=AUTH)).json()
            assert packs["PACK"]["last_seen"] == "1970-01-01T00:00:10.000Z"


def test_api_is_absent_without_token(tmp_path: Path) -> None:
    bridge = Bridge(
        FakeTransport(), SeplosV3Decoder(), MemoryMQTT(), [], ManualClock(), state_dir=tmp_path
    )
    assert bridge.api is None
