# Seplos V3 (BMS 3.0) protocol: framing, read-only register map, multi-pack addressing

Research for issue #3. Scope is reading only. Write registers (0x10, 0x0F, the SCA control block) are left out.

## Sources (by trust)

| Tag | Source | Kind |
|---|---|---|
| **P01** | *SEPLOS-3.0 RS485 BMS Modbus RTU Protocol* V0.1, 2023-02-09 ([PDF mirror](https://raw.githubusercontent.com/DpunktS/seplos_v3_sniffer/main/Dokumente/SEPLOS-3.0RS485BMS.Modbus.RTU.Protocol.pdf), same file as the [ferelarg mirror](https://github.com/ferelarg/Seplos3MQTT/tree/main/docs), md5 `49c6dfcc…`) | Vendor protocol document |
| **P03** | *SEPLOS BMS Modbus RTU Protocol* V0.3, 2024-10-15, "English, official" ([PDF in syssi/esphome-seplos-bms](https://github.com/syssi/esphome-seplos-bms/tree/main/docs), file `XZH BMS Modbus-RTU Protocol V0.3 (English, official).pdf`) | Vendor protocol document (newer) |
| **S3** | *SEPLOS Smart BMS 3.0 16S100A Specification* ([PDF, akkudoktor.net](https://akkudoktor.net/uploads/short-url/wvcI75WIhJljXGzUgrw1e7AosHU.pdf)) | Vendor hardware spec |
| **S2** | *SEPLOS 48V 200A BMS user manual* (V2 era) ([PDF, akkudoktor.net](https://akkudoktor.net/uploads/short-url/yn1ldRctrqKy8ia9wlawEwJ9fdJ.pdf)) | Vendor manual, previous generation |
| **A** | `aiobmsble` Seplos V3 plugin [`seplos_bms.py`](https://github.com/patman15/aiobmsble/blob/main/aiobmsble/bms/seplos_bms.py) and V2 plugin [`seplos_v2_bms.py`](https://github.com/patman15/aiobmsble/blob/main/aiobmsble/bms/seplos_v2_bms.py). These are the plugin library behind `patman15/BMS_BLE-HA`; the plugins moved out of that repo. | Working decoder (BLE) |
| **Y** | syssi/esphome-seplos-bms: [V3 RS485 example](https://github.com/syssi/esphome-seplos-bms/blob/main/esp32-seplos-v3-example.yaml), [README (V2 ASCII)](https://github.com/syssi/esphome-seplos-bms#protocol), [V3 frame captures](https://github.com/syssi/esphome-seplos-bms/tree/main/docs/pdus/v3) | Working decoder and real captures |
| **D** | DpunktS [seplos_v3_sniffer](https://github.com/DpunktS/seplos_v3_sniffer) (ESPHome) and [ioBroker.seplos-v3-sniffer](https://github.com/DpunktS/ioBroker.seplos-v3-sniffer) | Working passive multi-pack decoder |
| **F** | ferelarg [Seplos3MQTT](https://github.com/ferelarg/Seplos3MQTT) | Working passive multi-pack decoder |
| **R** | rolandfiege69-svg [seplos-v3-ha-modbus](https://github.com/rolandfiege69-svg/seplos-v3-ha-modbus) | Working single-pack active poller |
| **I80** | [syssi/esphome-seplos-bms#80](https://github.com/syssi/esphome-seplos-bms/issues/80) | User reports |

I re-computed the vendor example frames' CRCs (P01 §"Communication demonstration") and they match standard Modbus CRC-16.

---

## 1. Line settings and framing

- **19200 baud, 8 data bits, no parity, 1 stop bit (8N1).** Source: P01 §1.1 and P03 §1.1. S3 §4.4.2 also gives "baud rate for BMS internal communication 19200". All of A/Y/D/F/R use 19200 8N1.
- **Plain Modbus RTU**, binary, with no ASCII wrapper. Each frame is `ADDR, FC, …, CRC`. The CRC is Modbus CRC-16 (poly 0xA001, init 0xFFFF), sent **low byte first**. Data registers are **big-endian** (MSB first). Sources: P01 §2.2–2.6, verified against the example frames.
- **Function codes** (P01 §2.1.1):
  - `0x04` Read Input Registers: blocks PIA, PIB, EIA, EIB, PCT. P03 adds SPA, HIA and VIA.
  - `0x01` Read Coils: blocks PIC and EIC. P03 adds SFA.
  - `0x10` and `0x0F` are writes and are out of scope.
- **Request** (FC 0x04 and 0x01): `ADDR FC startHi startLo countHi countLo crcLo crcHi`, 8 bytes in total.
  - For FC 0x01, `count` is a number of **bits**. The reply `Length` is in **bytes**, and unused bits are zero-padded (P01 §2.4, Chinese note).
- **Response**: `ADDR FC byteCount data… crcLo crcHi`.
- **Exception response**: `ADDR FC|0x80 errCode crcLo crcHi`, 5 bytes (P01 §2.6). The codes are standard Modbus 0x01–0x06, 0x08, 0x0A and 0x0B, plus one vendor code, **0x81 "No history record"**.
  - A real capture shows `00 84 02 93 01`: illegal data address, returned for FC 0x04 at device 0 (Y captures, `SP05B2312190075-error-responses.txt`).
- **Port behaviour** (P01 §1.2): "RS485: BMS response which is self address only."

Vendor example requests (P01/P03). All are read-only and safe to send:

```
PIA:  00 04 10 00 00 12 75 16   -> 00 04 24 <36 bytes> CRC   (18 regs; table lists 17 + 1 reserve)
PIB:  00 04 11 00 00 1A 75 2C   -> 00 04 34 <52 bytes> CRC   (26 regs)
PIC:  00 01 12 00 00 90 38 CF   -> 00 01 12 <18 bytes> CRC   (0x90 = 144 coils = 18 bytes)
```

## 2. Device IDs (address space)

From P01 §2.1.2. P03 is the same but lists more blocks per device.

| Device | ID | Blocks |
|---|---|---|
| BMS (pack) | **0x00–0x7F** | PIA, PIB, PIC (P03 adds SPA/SCA/HIA/VIA/SFA) |
| EMS | 0xB0–0xBF | (none listed) |
| ECU (system aggregate) | **0xC0** | EIA, EIB, EIC |
| 2.4/5/7" LCD | 0xE0 | PIA/PIB/PIC (+EIx, PCT in P03) |
| Bluetooth | 0xE0 / 0x00–0x10 / 0xC0 | all of the above |

## 3. Read-only register map (per pack)

All rows are from P01 TA01. Where P03 differs, the row says so. In the R/W column, R means read-only.

### PIA: Pack Info A, FC 0x04, start 0x1000

| Reg | Field | Type | Unit / scale | Notes |
|---|---|---|---|---|
| 0x1000 | Pack voltage | UINT16 | 10 mV | |
| 0x1001 | Current | **INT16** | 10 mA | Positive means charging (R). **P03 rev V0.2: on 300 A/400 A BMS the unit is 0.1 A; "no change below 200 A".** |
| 0x1002 | Remaining capacity | UINT16 | 10 mAh | |
| 0x1003 | Total (full) capacity | UINT16 | 10 mAh | |
| 0x1004 | Total discharge capacity | UINT16 | **10 Ah** | |
| 0x1005 | SoC | UINT16 | 0.1 % | |
| 0x1006 | SoH | UINT16 | 0.1 % | |
| 0x1007 | Cycle count | UINT16 | 1 | |
| 0x1008 | Average cell voltage | UINT16 | 1 mV | |
| 0x1009 | Average cell temperature | UINT16 | 0.1 K | |
| 0x100A / 0x100B | Max / min cell voltage | UINT16 | 1 mV | |
| 0x100C / 0x100D | Max / min cell temperature | UINT16 | 0.1 K | |
| 0x100E | reserved | | | |
| 0x100F | Max discharge current (suggested) | UINT16 | 1 A | |
| 0x1010 | Max charge current (suggested) | UINT16 | 1 A | |
| 0x1011 | Precharge percent | UINT16 | 0.1 % | P03 only. P01's example returns this slot as "reserve" = 0x03E8. |

Temperatures are in Kelvin×10. The vendor examples subtract **2731**, for example `0x0B80 = 2944 → 21.3 °C`. Y uses 2731.5 and D uses 2731.5 too (273.15 K). Kelvin keeps the values positive, so UINT16 is safe.

### PIB: Pack Info B, FC 0x04, start 0x1100, 26 registers (0x1A)

| Reg | Field | Type | Unit |
|---|---|---|---|
| 0x1100–0x110F | Cell 1–16 voltage | UINT16 | 1 mV |
| 0x1110–0x1113 | Cell temperature 1–4 | UINT16 | 0.1 K |
| 0x1114–0x1117 | reserved | | | 
| 0x1118 | **Environment** (ambient) temperature | UINT16 | 0.1 K |
| 0x1119 | **Power** (MOSFET) temperature | UINT16 | 0.1 K |

On the reserved registers 0x1114–0x1117: the vendor example returns `0x0AAB`, and a real capture returns `0x5CAB` (Y capture). Ignore them.

### PIC: Pack Info C, FC 0x01 (coils), start 0x1200, 144 bits = 18 bytes

Each byte holds 8 flags. Coil address = 0x1200 + 8·byteIndex + bit. Bit 0 is the LSB of the byte.

| Byte | Coil base | Content | Table |
|---|---|---|---|
| 0 | 0x1200 | Cell 1–8 low-voltage alarm (bit n = cell n+1) | |
| 1 | 0x1208 | Cell 9–16 low-voltage alarm | |
| 2 | 0x1210 | Cell 1–8 high-voltage alarm | |
| 3 | 0x1218 | Cell 9–16 high-voltage alarm | |
| 4 | 0x1220 | Cell temp 1–8 low alarm | |
| 5 | 0x1228 | Cell temp 1–8 high alarm | |
| 6 | 0x1230 | **Cell 1–8 balancing (equalization) on** | |
| 7 | 0x1238 | **Cell 9–16 balancing on** | |
| 8 | 0x1240 | System state | TB09 |
| 9 | 0x1248 | Voltage events | TB02 |
| 10 | 0x1250 | Cell temperature events | TB03 |
| 11 | 0x1258 | Environment/power temperature events | TB04 |
| 12 | 0x1260 | Current events 1 | TB05 |
| 13 | 0x1268 | Current events 2 (latch-ups) | TB16 |
| 14 | 0x1270 | Residual capacity events | TB06 |
| 15 | 0x1278 | FET state | TB07 |
| 16 | 0x1280 | Equalization (balancer) state | TB08 |
| 17 | 0x1288 | Hard faults | TB15 |

Bit tables (P01, P03):

- **TB09, system state:**
  - b0 Discharge
  - b1 Charge
  - b2 Floating charge
  - b3 Full charge
  - b4 Standby
  - b5 Turn off
- **TB02, voltage:**
  - b0 cell high alarm
  - b1 cell OV protection
  - b2 cell low alarm
  - b3 cell UV protection
  - b4 pack high alarm
  - b5 pack OV protection
  - b6 pack low alarm
  - b7 pack UV protection. P03 renames b7 to "Temp-Curt Adjust" (see §6).
- **TB03, cell temperature:**
  - b0/b1 charge high alarm / protection
  - b2/b3 charge low alarm / protection
  - b4/b5 discharge high alarm / protection
  - b6/b7 discharge low alarm / protection
- **TB04, environment and power temperature:**
  - b0/b1 environment high alarm / protection
  - b2/b3 environment low alarm / protection
  - b4/b5 power high alarm / protection
  - b6 cell low-temperature heating
- **TB05, current:**
  - b0/b1 charge alarm / protection
  - b2 charge 2nd-level protection
  - b3/b4 discharge alarm / protection
  - b5 discharge 2nd-level protection
  - b6 output short circuit
- **TB16, latch-ups:**
  - b0 short-circuit latch
  - b2 2nd-level charge latch
  - b3 2nd-level discharge latch
- **TB06, residual capacity:**
  - b2 SoC alarm
  - b3 SoC protection
  - b4 cell-difference alarm
- **TB07, FET state:**
  - b0 discharge FET on
  - b1 charge FET on
  - b2 current-limit FET on
  - b3 heating on
- **TB08, equalization state** (the name says balancer, but the bits are mostly modes):
  - b0 low SoC alarm
  - b1 intermittent charge
  - b2 external switch control
  - b3 static standby/sleep
  - b4 history recording
  - b5 under-SoC protection
  - b6 active current limiting
  - b7 passive current limiting
- **TB15, hard faults:**
  - b0 NTC
  - b1 AFE
  - b2 charge MOSFET
  - b3 discharge MOSFET
  - b4 cell
  - b5 break line
  - b6 key
  - b7 aerosol

Per-cell balancing therefore comes from PIC bytes 6–7. It does not come from TB08.

### VIA: Version Info, FC 0x04, start 0x1700. This block holds the serial number. P03 only.

| Reg | Field | Type | Length |
|---|---|---|---|
| 0x1700 | Factory name | ASCII | 20 bytes (10 regs) |
| 0x170A | Device name | ASCII | 20 bytes |
| 0x1714 | Firmware version | ASCII | 2 bytes |
| 0x1715 | **BMS SN** ("BMS QR-code info") | ASCII | 30 bytes (15 regs) |
| 0x1724 | **Pack SN** ("Pack QR-code info") | ASCII | 30 bytes |

P03 adds a note on this block: "内填写小端在前", meaning "little-endian first". Byte order within each register may therefore be swapped relative to the numeric blocks. The decoder should accept both orders and strip NUL padding. Example read: `00 04 17 15 00 0F A5 AF`. This block is absent from P01, and none of the community decoders I read actually decode it. **Unverified on real hardware.**

### System aggregate blocks (EIA / EIB / EIC)

These come from the ECU device 0xC0 per P01. Over BLE, A reads them at device **0x00**. A real capture also has device 0x00 answering them.

- **EIA, FC 0x04, 0x2000, 26 regs.** Fields:
  - pack V (UINT32, 10 mV)
  - current (**INT32, 100 mA**)
  - remaining, total and rated capacity (UINT32, 10 mAh)
  - total discharge (UINT32, 10 Ah)
  - online-pack bitmap 0x200C
  - protected-pack bitmap 0x200E
  - max discharge/charge current (UINT32, 100 mA)
  - suggested OV/UV (100 mV)
  - **0x2016 System Pack No. (pack count)**
  - 0x2017 average cycles
  - 0x2018 SoC, 0x2019 SoH (0.1 %)

  **UINT32 values are word-swapped (low word first):** the capture `14 A6 00 00` = 0x000014A6 = 53.18 V (Y capture). A does the same with `swap32`.
- **EIB, FC 0x04, 0x2100, 22 regs.** System max/min cell V and their IDs, max/min pack V, temperatures, pack SoC extremes. **P01 and P03 disagree on layout and units.** P03 has temperatures in 0.1 °C and SoC/cycle/SoH at 0x210D–0x2110. A real capture matches P03: `0x2108 = 0x00E5` = 22.9 °C.
- **EIC, FC 0x01, 0x2200, 80 bits.** Same TB tables as PIC bytes 8–17. P01 lists this block from 0x2200, without the per-cell bytes.

## 4. Multi-pack addressing (two daisy-chained, auto-addressed packs)

What the sources say:

1. **V3 auto-addresses.** S3 §4.5 and §8.10: "BMS has the function of automatic address assignment without dialing (the dialing switch reserved on BMS is only a decoration…)". It works only if the packs are linked through the **internal-485** parallel ports with ordinary straight network cables.
   - **V2 is different.** It used DIP switches 1–4 as the RS485 address, with all switches OFF for a single pack (S2 §9.4).
   - A third-party summary of a Seplos 3.0 manual says BMS 2.0 and 3.0 packs cannot be paralleled. I have not confirmed this in a primary source.
2. **In a V3 parallel group, the first pack becomes the Modbus master** on the internal RS485 link and polls the others (D, F, I80). The DpunktS README says the first BMS "acts as a Modbus master while all other BMS act as slaves… it is no longer possible to access the BMS from a third device via Modbus, since two master devices cannot exist." The master cycles PIA/PIB/PIC per pack roughly **every 200 ms** (D).
   - The sniffers accept responses from **0x01–0x10** (D: `buffer[0] >= 0x01 && <= 0x10`).
   - They key on the response shapes: FC 0x04 with lengths 0x24 (PIA) or 0x34 (PIB), and FC 0x01 with length 0x12 (PIC).
   - Packs therefore sit at **1..N**, where N ≤ 16. Seplos specifies up to 16 in parallel (S3, A caps `pack_count` at 0x10).
3. **A single, un-paralleled pack answers on address 0x00** (R; Y has `address: 0x00` with `allow_broadcast_read: true`). Address 0 is the Modbus broadcast address, so many Modbus libraries refuse to read from it. The probe must send raw frames or explicitly allow it.
   - R's README also says "slave address = DIP address". That conflicts with S3's statement that the DIP switch is decorative. See §6.
4. **Over BLE the master relays.** A queries EIA/EIB/EIC at device 0x00, reads `pack_count` from EIA 0x2016, then reads PIA/PIB at devices **1..pack_count** through the same connection.
   - P01's device table lists Bluetooth as 0xE0 / 0x00–0x10 / 0xC0.
   - So the BLE module, on the master, works as a gateway. P01 error codes 0x0A/0x0B (gateway path unavailable / no response) fit this.
   - Whether an external RS485 master can use the same relaying is **not documented**.
5. **Ports.**
   - The V3 BMS has an **"Inverter CAN/485"** RJ45, which speaks an inverter protocol (Pylon/Victron/… selected in software, PCT block 0x1800).
   - It also has **internal 485** RJ45s for the parallel link (S3 figure §4.3).
   - R uses the port labelled **RS485-1** on a single pack and calls RS485-2 the daisy-chain port. Their RJ45 pinout is pin 1 = B−, pin 2 = A+, pins 3/6 = GND.
   - D uses pins 1/8 = B, 2/7 = A, 5 = GND on the parallel port. For a lone pack, D says "connect pin 6 to pin 5 (GND) so that the master can send data independently". This is a strap that makes a lone pack act as master and emit its poll cycle.
   - The CAN port reaches only the master (I80).
   - On the V2 manual (S2), DIP switch 5 selected CAN. Solar-Assistant notes that with CAN selected, the master is not readable on RS485.

**What this means for two auto-addressed packs on one RS485 port.** Plugged into the free internal-485 port of the chain (the master's second port or the last slave's port), the probe shares a bus that the master pack already drives. It should **listen first**: frames to and from 0x01 and 0x02 will appear every ~200 ms. Transmitting into that bus risks collisions. The proven approach for multi-pack V3 is passive sniffing (D, F). Active polling of individual packs in a running chain is reported as unreliable (I80). Active polling works for a single pack at 0x00.

**Address range a probe should scan:** **0x00, then 0x01–0x10.** Optionally add 0xC0 for the EIx aggregate. The protocol allows 0x00–0x7F, but no source places a pack above 0x10.

## 5. Telling V3 Modbus from V2 ASCII on the wire

| | V2 (Seplos "1101-SPxx", 10E V2, V16) | V3 (BMS 3.0, "SPxxB" models) |
|---|---|---|
| Line | RS485 19200 8N1 (S2). The RS232 console runs at 9600 (Y). | RS485 19200 8N1 |
| Frame | ASCII hex between `~` (0x7E) and CR (0x0D) | Binary Modbus RTU, no delimiters |
| Example request | `~20004642E00200FD37\r`: ver 0x20, addr 0x00, CID1 0x46, CID2 0x42 (telemetry), LENGTH with its checksum, ASCII checksum (Y) | `00 04 10 00 00 12 75 16` |
| Integrity | 16-bit two's-complement sum of the ASCII chars (Y/V2 docs). Over BLE the V2 plugin uses binary 0x7E…0x0D with CRC-XMODEM (A). | CRC-16/Modbus, little-endian on the wire |
| Addressing | DIP switch 1–4 (S2) | Automatic. A lone pack answers on 0x00. |
| BLE name (A matchers) | `BP[0-2][0-9]*` | `SP??B*`, `SP1??B*`, `XZHX*`, `CSY*`, `ST*` |

How to tell them apart:

- **Passively:**
  - A first byte of 0x7E followed by ASCII hex digits and a CR terminator means V2.
  - Binary bytes that start with an address of 0x00–0x10 and FC 0x01 or 0x04, and pass the Modbus CRC, mean V3.
- **Actively:**
  - Send the V3 PIA read to 0x00. A CRC-valid `00 04 24 …` reply (or `00 84 xx`) confirms V3.
  - R reports that "BMS 3.0 firmware does not speak the old Seplos ASCII protocol… it silently ignores those frames."
  - Sending the V2 `~…\r` frame is the converse check.

**What SP18B2311040070 implies.**

- The **`SPxxB` prefix is the V3 model family**: A's V3 BLE matcher `SP??B*` matches `SP18B…`, and the V2 matcher `BP…` does not.
- The other known V3 units fit this pattern:
  - `BMS16S200A-SP05B`, a V3.0 "Type C" with serial `SP05B2312190075` (Y)
  - `SG16S200A-SP57B-A` (Y)
- V2 units report `1101-SP05`, `1101-SP15`, etc., with no `B` suffix (Y).
- S3 §3 gives the QR/serial scheme as `<customer model>-<date>-<mfg date>-<serial no.>`. Read that way, `SP18B 231104 0070` is model/customer code **SP18B**, date **2023-11-04**, unit **0070**. The decoding is my inference; the parallel `SP05B 231219 0075` fits it.
- **"18" is a model code, not a cell count.** The V3 decoders (A, D, Y) all read 16 cells, and the ticket's 16-cell expectation stands.
- The BLE-advertised name is most likely the same string as VIA 0x1715 "BMS SN". This is inferred and needs a check on hardware.

## 6. Disagreements and open uncertainties

1. **PIA current scale.** It is 10 mA in P01. P03 (rev V0.2) says 0.1 A on 300 A/400 A BMS models.
   - A, D and R all assume 10 mA.
   - The SP18B current rating is unknown, so cross-check: pack V × I should be plausible, and EIA current (100 mA) should agree with PIA current when only one pack is present.
2. **PIA length.** The table runs 0x1000–0x1010 (17 regs) and P03 adds 0x1011. The vendor example reads 0x12 (18). A reads 0x11.
   - Both work. Reading 0x12 is the vendor's own example.
3. **0x1118 vs 0x1119.** P01, P03, A and Y say 0x1118 = environment and 0x1119 = power/MOSFET. R swaps them. Trust the vendor.
4. **Kelvin offset.** The vendor uses 2731; Y and D use 2731.5. The difference is at most 0.05 °C.
5. **EIB layout.** P01 and P03 differ in register order and in temperature unit (1 °C vs 0.1 °C). A capture matches P03.
6. **PIC addressing in P03.** P03's table relabels PIC as 0x1200–0x1208 (one register = 2 flag bytes). Its own example still uses FC 0x01 with 0x90 coils from 0x1200. Use the coil form.
7. **TB02 bit 7.** P01 has "Pack under voltage protection"; P03 has "Temp-Curt Adjust". Firmware-dependent.
8. **A's "balancer" field** is TB07 b2 (current-limit FET), which A itself comments as `# limit FET`. True per-cell balancing is in PIC bytes 6–7.
9. **DIP vs auto-address.**
   - S3 (vendor V3 spec) says the DIP switch is decorative and that addresses are auto-assigned.
   - R reports "slave address = DIP address", but tested only a single pack with all switches OFF → 0x00.
   - I80 has syssi saying 0x00 is "master mode", followed by "I don't know".
   - Treat addresses as discovered, not configured.
10. **Can an external master poll slaves in a running V3 chain, or ask the master to relay over RS485?**
    - No source shows either working.
    - The BLE path does relay (A).
    - D and F settled on passive sniffing because a second master conflicts.
    - This is the biggest open question for a two-pack install, and only a hardware test settles it.
11. **VIA (serial number) block** is P03-only and its byte order note is ambiguous. No decoder I read uses it, so it needs verification on real hardware.
