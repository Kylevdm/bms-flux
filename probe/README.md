# Probe: 2026-11-24, read-only

Captured on the dev machine through the CH340 adapter (`/dev/serial/by-id/usb-1a86_USB_Serial-if00-port0`) plugged into the bank's internal RS485 bus. **Listen only: no byte was transmitted.**

## Findings

- **Protocol:** Seplos V3, plain Modbus RTU, **19200 8N1**. A 30 s sniff gave 1,346 CRC-valid frames; the only junk was 18 bytes at the start of the capture. At 9600 the bus reads as noise (`listen-9600.noise.jsonl`).
- **Bus addresses:** only `0x01` and `0x02`.
  - `0x01` is the master pack. It polls `0x02` with FC04 `0x1000`×0x12 (PIA), FC04 `0x1100`×0x1A (PIB) and FC01 `0x1200`×0x90 (PIC), and also sends its own PIA, PIB and PIC as responses nobody requested. Each block arrives about every 200 ms (150 per 30 s). Bus utilisation is about 70%.
  - Nothing on the bus reads the serial (`0x1715`) or EIA.
- **Address to pack serial** (matched by cycle count with the owner, 2026-11-24):

  | Bus address | Pack serial | Cycles | SoH | SoC at capture |
  |---|---|---|---|---|
  | `0x01` (master) | `SP112B2410100228` | 177 | 98.2% | 31.3% |
  | `0x02` | `SP18B2311040070` | 535 | 94.6% | 38.2% |

  Both packs were at 51.77–51.78 V, discharging at −12.6 A and −10.6 A.

## PIA decode used for the match (FC04 0x1000, 18 registers)

| Reg | Field | Scale | 0x01 | 0x02 |
|---|---|---|---|---|
| 0x1000 | pack voltage | 10 mV | 51.77 V | 51.78 V |
| 0x1001 | current (signed) | 10 mA | −12.59 A | −10.61 A |
| 0x1002 | remaining capacity | 10 mAh | 87.64 Ah | 106.96 Ah |
| 0x1003 | total capacity | 10 mAh | 280.00 Ah | 280.00 Ah |
| 0x1005 | SoC | 0.1 % | 31.3 | 38.2 |
| 0x1006 | SoH | 0.1 % | 98.2 | 94.6 |
| 0x1007 | cycles | 1 | 177 | 535 |
| 0x1008 | average cell voltage | mV | 3235 | ~3236 |
| 0x1009 | average cell temperature | 0.1 K | 293.6 K | – |

## Files

- `fixtures/sniff-19200.raw.jsonl`: every USB read chunk with its wall-clock timestamp. The CH340 delivers bytes in chunks, so frames are split across chunk boundaries.
- `fixtures/sniff-19200.frames.jsonl`: the same byte stream split into Modbus frames by structure and CRC (`kind`: req/resp).
- `fixtures/listen-9600.noise.jsonl`: 3 s at 9600, which is noise.
- `sniff.py`: the throwaway capture script (`sniff.py BAUD SECONDS OUT_PREFIX`).
