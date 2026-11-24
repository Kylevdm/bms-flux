"""Define per-pack readings and the bridge's internal reading stream."""

import asyncio
from dataclasses import dataclass

FieldValue = float | int | str | dict[str, bool]


@dataclass(frozen=True)
class Reading:
    serial: str
    address: int
    ts: float
    block: str
    fields: dict[str, FieldValue]


class ReadingStream(asyncio.Queue[Reading]):
    """The single internal stream consumed by the snapshot publisher."""
