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

    def feed(self, data: bytes, *, final: bool = False) -> list[Frame]:
        self._buffer.extend(data)
        frames: list[Frame] = []
        offset = 0
        earliest_incomplete: int | None = None
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
                    del self._buffer[: offset + length]
                    offset = 0
                    earliest_incomplete = None
                    break
            else:
                if incomplete and earliest_incomplete is None:
                    earliest_incomplete = offset
                offset += 1
        if final:
            self._buffer.clear()
        else:
            # Keep a possible chunk-split frame, but never let junk hide a later CRC-valid one.
            keep = earliest_incomplete if earliest_incomplete is not None else offset
            del self._buffer[:keep]
        return frames
