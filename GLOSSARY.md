# bms-flux

bms-flux owns the RS485 link to a bank of Seplos battery packs and reports their state, and any doubt about their state of charge, to Home Assistant.

## Language

### Hardware

**Bank**:
All the packs on one bus, wired in parallel and reported on together.
_Avoid_: Group, array, system

**Pack**:
One Seplos battery with its own BMS and serial number. A bank holds several packs wired in parallel.
_Avoid_: Battery, BMS, module

**Pack serial**:
The serial number a pack's BMS reports (e.g. `SP18B2311040070`). It is the pack's identity; bus addresses are not.
_Avoid_: Pack ID, device ID

**Pack label**:
The owner's display name for a pack (e.g. "Pack A"). It never identifies the pack; the pack serial does.
_Avoid_: Name, alias

**Bus**:
The daisy-chained RS485 link that joins the packs and the adapter.
_Avoid_: Chain, line

**Master pack**:
The pack that wins auto-addressing and polls every pack on the bus, itself included. bms-flux reads the bus by listening to it.
_Avoid_: Primary, leader, host

**Bus address**:
The number a pack answers to on the bus. It is assigned automatically and may change, so it never identifies a pack.
_Avoid_: Slave ID, unit ID

**Fingerprint**:
The counters a pack reports that only ever rise, its cycle count and lifetime discharge, used to check that a bus address still belongs to the same pack.
_Avoid_: Signature, checksum

**Reading**:
One decoded block of a pack's data at one moment, keyed by pack serial.
_Avoid_: Sample, message, frame

**Transport**:
The way bms-flux reaches the bus: a local serial adapter, or a network gateway that carries the bus over TCP.
_Avoid_: Connection, link

### Reporting

**Snapshot**:
The latest reading of each pack, published to Home Assistant on an interval or as soon as an alarm, protection or FET state changes.
_Avoid_: Sample, update, poll

**Stream**:
Every reading of every mapped pack, at the rate it crosses the bus, offered to apps other than Home Assistant.
_Avoid_: Feed, firehose, telemetry

**Link state**:
Whether bms-flux can currently vouch for a pack's readings, and if not, why (the pack has gone silent, or its fingerprint no longer matches).
_Avoid_: Status, connection

### State of charge

**Reported SoC**:
The state of charge a pack's BMS claims for itself.
_Avoid_: SoC (unqualified), battery level

**Drift**:
The gap between a pack's reported SoC and its true state of charge, which the BMS's coulomb counting accumulates over time.
_Avoid_: Error, offset, miscalibration

**Drift rule**:
One named test of independent evidence that reported SoC has drifted, in one pack or somewhere in the bank.
_Avoid_: Check, detector, alarm

**Drift finding**:
A drift rule's verdict that reported SoC has drifted, together with its evidence. An anchor finding names one pack; a divergence finding names the bank, because it cannot tell which pack drifted. A finding never corrects anything.
_Avoid_: Alert, correction, warning

**Divergence**:
Packs in one bank reporting SoCs further apart than parallel packs can truly be. Evidence that some pack has drifted, but not which.
_Avoid_: Imbalance, mismatch

**Anchor**:
A battery condition whose true state of charge is known independently of the BMS (full charge, or rested open-circuit voltage near the ends).
_Avoid_: Reference point, calibration point

**Latched finding**:
A drift finding that stays raised after its evidence has passed, until the same rule reaches its anchor again and finds agreement.
_Avoid_: Sticky alert, persistent alarm

**SoC correction**:
An owner-commanded write that sets a pack's reported SoC. Not part of V1.
_Avoid_: Recalibration, reset, sync

### Tooling

**Probe**:
A read-only run that discovers how the bus speaks (protocol, baud rate, which addresses answer) and saves every raw frame.
_Avoid_: Scan, discovery, test

**Parity check**:
The acceptance comparison of bms-flux's values against bms_ble's for the same packs, over one unbroken week, testing both that fields decode alike and that bms-flux stays up.
_Avoid_: Soak test, validation, burn-in
