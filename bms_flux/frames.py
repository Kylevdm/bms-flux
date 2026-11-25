"""Split a byte stream into structurally valid, CRC-checked Modbus RTU frames."""

from dataclasses import dataclass
from typing import Literal


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


@dataclass(frozen=True)
class Frame:
    kind: Literal["req", "resp", "exc"]
    data: bytes


class FrameSplitter:
    def __init__(self) -> None:
        self._buffer = bytearray()
        self.crc_errors = 0
        self.junk_bytes = 0
        self._position = 0
        self._failed_until = 0

    def reset(self) -> None:
        """Discard partial framing state; a reopened transport starts a fresh stream."""
        self._buffer.clear()
        self._position = 0
        self._failed_until = 0

    def feed(self, data: bytes, *, final: bool = False) -> list[Frame]:
        self._buffer.extend(data)
        frames: list[Frame] = []
        offset = 0
        earliest_incomplete: int | None = None
        failures: dict[int, int] = {}
        while offset + 2 <= len(self._buffer):
            candidates: list[tuple[Literal["req", "resp", "exc"], int]] = []
            address, function = self._buffer[offset : offset + 2]
            if address <= 247 and function in (1, 2, 3, 4):
                candidates.append(("req", 8))
                if offset + 3 <= len(self._buffer):
                    count = self._buffer[offset + 2]
                    if 1 <= count <= 250 and (function in (1, 2) or count % 2 == 0):
                        candidates.append(("resp", count + 5))
            elif address <= 247 and function in (0x81, 0x82, 0x83, 0x84):
                candidates.append(("exc", 5))
            incomplete = False
            for kind, length in candidates:
                if len(self._buffer) - offset < length:
                    incomplete = True
                    continue
                candidate = bytes(self._buffer[offset : offset + length])
                if kind == "req":
                    count = int.from_bytes(candidate[4:6], "big")
                    if not 1 <= count <= (2000 if function in (1, 2) else 125):
                        continue
                if crc16(candidate[:-2]) == int.from_bytes(candidate[-2:], "little"):
                    frames.append(Frame(kind, candidate))
                    self.junk_bytes += offset
                    self._position += offset + length
                    failures.clear()
                    del self._buffer[: offset + length]
                    offset = 0
                    earliest_incomplete = None
                    break
                failures[offset] = max(failures.get(offset, 0), length)
            else:
                # A valid alternative at this offset wins. Delay incomplete
                # candidates, and suppress overlapping guesses inside a bad frame.
                if (
                    offset in failures
                    and not incomplete
                    and self._position + offset >= self._failed_until
                ):
                    self.crc_errors += 1
                    self._failed_until = self._position + offset + failures[offset]
                if incomplete and earliest_incomplete is None:
                    earliest_incomplete = offset
                offset += 1
        if final:
            self.junk_bytes += len(self._buffer)
            self._position += len(self._buffer)
            self._buffer.clear()
        else:
            # Keep a possible chunk-split frame, but never let junk hide a later CRC-valid one.
            keep = earliest_incomplete if earliest_incomplete is not None else offset
            self.junk_bytes += keep
            self._position += keep
            del self._buffer[:keep]
        return frames
