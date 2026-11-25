"""Token-gated, read-only local resync and full-rate stream API.

/packs is keyed by pack serial; each entry has link, last_seen (UTC ISO-8601
or null), and readings keyed by block. Reading timestamps remain unchanged.
Raw frames opt in independently of block filters; serial filters also apply to
raw frames using the owner's bus address map.
"""

import asyncio
import hmac
import json
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web

from bms_flux.identity import PackIdentity
from bms_flux.readings import ReadingStream
from bms_flux.stream import _iso_ts, format_raw_frame, format_reading


@dataclass(eq=False)
class _Client:
    ws: web.WebSocketResponse
    serials: set[str]
    blocks: set[str]
    raw: bool
    queue: asyncio.Queue[tuple[str | None, str]]
    overflow: asyncio.Event = field(default_factory=asyncio.Event)


class LocalAPI:
    def __init__(self, token: str, identity: PackIdentity, *, raw_frames: bool = False) -> None:
        self._identity = identity
        self._raw_frames = raw_frames
        self._clients: set[_Client] = set()
        expected = f"Bearer {token}".encode()

        @web.middleware
        async def authorize(
            request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
        ) -> web.StreamResponse:
            if not hmac.compare_digest(request.headers.get("Authorization", "").encode(), expected):
                raise web.HTTPUnauthorized(headers={"WWW-Authenticate": "Bearer"})
            return await handler(request)

        self.app = web.Application(middlewares=[authorize])
        self.app.router.add_get("/packs", self.packs)
        self.app.router.add_get("/stream", self.stream)
        self.app.on_shutdown.append(self.shutdown)

    async def packs(self, request: web.Request) -> web.Response:
        result: dict[str, Any] = {}
        for serial, link in self._identity.links.items():
            readings = {r.block: format_reading(r) for r in self._identity.readings(serial)}
            result[serial] = {
                "link": link,
                "last_seen": (
                    _iso_ts(last_seen)
                    if (last_seen := self._identity.last_seen[serial]) is not None
                    else None
                ),
                "readings": readings,
            }
        return web.json_response(result)

    async def consume(self, stream: ReadingStream) -> None:
        """Fan out without awaiting any WebSocket client."""
        while True:
            reading = await stream.get()
            # One turn per reading: a producer burst must not overflow the bounded
            # queue of a client whose sender is actively draining it.
            await asyncio.sleep(0)
            self._offer(format_reading(reading))
            stream.task_done()

    async def publish_raw(self, frame: bytes, ts: float) -> None:
        """Offer a CRC-valid raw frame without waiting for clients."""
        if self._raw_frames:
            self._offer(format_raw_frame(frame[0], frame, ts))

    def _offer(self, payload: dict[str, Any]) -> None:
        text = json.dumps(payload)
        raw = "hex" in payload
        serial = self._identity.serials.get(payload["address"]) if raw else payload["serial"]
        for client in self._clients:
            if client.overflow.is_set() or (raw and not client.raw):
                continue
            if client.serials and serial not in client.serials:
                continue
            if not raw and client.blocks and payload["block"] not in client.blocks:
                continue
            try:
                client.queue.put_nowait((None if raw else serial, text))
            except asyncio.QueueFull:
                client.overflow.set()

    async def stream(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(timeout=1)
        await ws.prepare(request)
        raw = request.query.get("raw") == "1"
        if raw and not self._raw_frames:
            await ws.close(code=1008, message=b"raw_frames is disabled")
            return ws
        client = _Client(
            ws,
            set(filter(None, request.query.get("serial", "").split(","))),
            set(filter(None, request.query.get("block", "").upper().split(","))),
            raw,
            # About five seconds at 30 readings/s, plus raw requests and responses.
            asyncio.Queue(maxsize=450 if raw else 150),
        )
        self._clients.add(client)

        async def send() -> None:
            while True:
                serial, text = await client.queue.get()
                # Only a fingerprint mismatch quarantines a serial; silence does not.
                if serial is None or self._identity.links[serial] != "fingerprint_mismatch":
                    await ws.send_str(text)

        async def receive() -> None:
            async for _ in ws:
                pass  # Client messages never control the bridge or reach the bus.

        sender = asyncio.create_task(send())
        receiver = asyncio.create_task(receive())
        overflow = asyncio.create_task(client.overflow.wait())
        try:
            done, _ = await asyncio.wait(
                (sender, receiver, overflow), return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()
        except ConnectionError:
            pass
        finally:
            self._clients.discard(client)
            for task in (sender, receiver, overflow):
                task.cancel()
            await asyncio.gather(sender, receiver, overflow, return_exceptions=True)
            with suppress(ConnectionError):
                await ws.close(
                    code=1008 if client.overflow.is_set() else 1000,
                    message=b"too slow" if client.overflow.is_set() else b"",
                )
        return ws

    async def shutdown(self, app: web.Application) -> None:
        await asyncio.gather(
            *(client.ws.close(code=1001, message=b"shutdown") for client in list(self._clients))
        )
