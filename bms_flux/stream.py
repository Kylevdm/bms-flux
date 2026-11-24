"""Format and publish the full-rate stream of readings and the opt-in raw-frame stream."""

import json
from datetime import UTC, datetime
from typing import Any

from bms_flux.clock import Clock
from bms_flux.mqtt import MQTTSink
from bms_flux.readings import Reading, ReadingStream

STREAM_VERSION = 1
RAW_VERSION = 1


def _iso_ts(ts: float) -> str:
    """UTC ISO-8601 timestamp with millisecond precision."""
    dt = datetime.fromtimestamp(ts, tz=UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def format_reading(reading: Reading) -> dict[str, Any]:
    """Build the JSON payload for one decoded reading on the stream."""
    return {
        "v": STREAM_VERSION,
        "ts": _iso_ts(reading.ts),
        "serial": reading.serial,
        "address": reading.address,
        "block": reading.block,
        **reading.fields,
    }


def format_raw_frame(address: int, frame: bytes, ts: float) -> dict[str, Any]:
    """Build the JSON payload for one raw, CRC-valid frame."""
    return {
        "v": RAW_VERSION,
        "ts": _iso_ts(ts),
        "address": address,
        "hex": frame.hex(),
    }


class StreamPublisher:
    """Publish decoded readings to per-pack stream topics and raw frames to bms_flux/raw."""

    def __init__(
        self,
        sink: MQTTSink,
        clock: Clock,
        *,
        stream: bool = True,
        raw_frames: bool = False,
    ) -> None:
        self._sink = sink
        self._clock = clock
        self._stream = stream
        self._raw_frames = raw_frames

    async def consume(self, stream: ReadingStream) -> None:
        """Consume one fan-out queue of trusted readings."""
        while True:
            reading = await stream.get()
            await self._sink.publish(
                f"bms_flux/{reading.serial}/stream",
                json.dumps(format_reading(reading)),
                retain=False,
                qos=0,
            )
            stream.task_done()

    async def publish_raw(self, frame: bytes, *, ts: float | None = None) -> None:
        """Publish a single CRC-valid raw frame, including requests."""
        if not self._raw_frames:
            return
        payload = json.dumps(
            format_raw_frame(frame[0], frame, self._clock.now() if ts is None else ts)
        )
        await self._sink.publish("bms_flux/raw", payload, retain=False, qos=0)
