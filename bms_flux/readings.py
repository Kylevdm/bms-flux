"""Define per-pack readings and the bridge's internal reading stream."""

import asyncio
from dataclasses import dataclass

FieldValue = float | int | str | dict[str, bool]


@dataclass(frozen=True)
class DecodedBlock:
    """A decoded block keyed only by bus address, not yet a trusted reading."""

    address: int
    ts: float
    block: str
    fields: dict[str, FieldValue]


@dataclass(frozen=True)
class Reading(DecodedBlock):
    serial: str


class ReadingStream(asyncio.Queue[Reading]):
    """The single internal stream consumed by the snapshot publisher."""
