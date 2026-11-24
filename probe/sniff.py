"""Listen-only sniff (transmits nothing). Saves raw USB chunks with timestamps,
then splits the byte stream into Modbus RTU frames by structure + CRC.

usage: sniff.py BAUD SECONDS OUT_PREFIX
writes OUT_PREFIX.raw.jsonl (every chunk) and OUT_PREFIX.frames.jsonl (parsed)
"""
import json
import sys
import time

import serial

PORT = "/dev/serial/by-id/usb-1a86_USB_Serial-if00-port0"


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


def ok(fr: bytes) -> bool:
    return len(fr) >= 4 and crc16(fr[:-2]) == int.from_bytes(fr[-2:], "little")


def split(stream: bytes):
    """Yield (offset, kind, frame) for CRC-valid frames; skip junk bytewise."""
    i, n = 0, len(stream)
    while i < n:
        found = None
        if i + 2 <= n and stream[i + 1] in (0x01, 0x02, 0x03, 0x04):
            # read request: addr fc reg(2) count(2) crc(2) = 8
            if i + 8 <= n and ok(stream[i:i + 8]):
                found = ("req", stream[i:i + 8])
            # read response: addr fc bc data[bc] crc(2)
            elif i + 3 <= n:
                ln = 3 + stream[i + 2] + 2
                if i + ln <= n and ok(stream[i:i + ln]):
                    found = ("resp", stream[i:i + ln])
        elif i + 2 <= n and stream[i + 1] & 0x80 and i + 5 <= n and ok(stream[i:i + 5]):
            found = ("exc", stream[i:i + 5])
        if found:
            yield i, found[0], found[1]
            i += len(found[1])
        else:
            yield i, "junk", stream[i:i + 1]
            i += 1


def main() -> None:
    baud, seconds, prefix = int(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
    ser = serial.Serial(PORT, baud, bytesize=8, parity="N", stopbits=1, timeout=0.05)
    ser.reset_input_buffer()
    chunks, t_end = [], time.time() + seconds
    while time.time() < t_end:
        c = ser.read(4096)
        if c:
            chunks.append((time.time(), c))
    ser.close()
    with open(prefix + ".raw.jsonl", "w") as f:
        for t, c in chunks:
            f.write(json.dumps({"t": round(t, 4), "dir": "rx", "baud": baud, "hex": c.hex(" ")}) + "\n")
    # byte -> timestamp of its chunk
    stream, stamps = bytearray(), []
    for t, c in chunks:
        stream += c
        stamps += [t] * len(c)
    frames, junk = [], 0
    for off, kind, fr in split(bytes(stream)):
        if kind == "junk":
            junk += 1
            continue
        frames.append({"t": round(stamps[off], 4), "kind": kind, "addr": fr[0], "fc": fr[1],
                       "hex": fr.hex(" ")})
    with open(prefix + ".frames.jsonl", "w") as f:
        for fr in frames:
            f.write(json.dumps(fr) + "\n")
    print(f"baud={baud} bytes={len(stream)} frames={len(frames)} junk_bytes={junk}")


if __name__ == "__main__":
    main()
