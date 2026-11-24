# bms-flux

bms-flux owns the RS485 link to a bank of Seplos battery packs and reports their state, and any doubt about their state of charge, to Home Assistant.

## Language

### Hardware

**Pack**:
One Seplos battery with its own BMS and serial number. A bank holds several packs wired in parallel.
_Avoid_: Battery, BMS, module

**Pack serial**:
The serial number a pack's BMS reports (e.g. `SP18B2311040070`). It is the pack's identity; bus addresses are not.
_Avoid_: Pack ID, device ID

**Bus**:
The daisy-chained RS485 link that joins the packs and the adapter.
_Avoid_: Chain, line

**Bus address**:
The number a pack answers to on the bus. It is assigned automatically and may change, so it never identifies a pack.
_Avoid_: Slave ID, unit ID

**Transport**:
The way bms-flux reaches the bus: a local serial adapter, or a network gateway that carries the bus over TCP.
_Avoid_: Connection, link

### State of charge

**Reported SoC**:
The state of charge a pack's BMS claims for itself.
_Avoid_: SoC (unqualified), battery level

**Drift**:
The gap between a pack's reported SoC and its true state of charge, which the BMS's coulomb counting accumulates over time.
_Avoid_: Error, offset, miscalibration

**Drift rule**:
One named test of independent evidence that a pack's reported SoC has drifted.
_Avoid_: Check, detector, alarm

**Drift finding**:
A drift rule's verdict that a pack has drifted, together with its evidence. A finding never corrects anything.
_Avoid_: Alert, correction, warning

**Anchor**:
A battery condition whose true state of charge is known independently of the BMS (full charge, or rested open-circuit voltage near the ends).
_Avoid_: Reference point, calibration point

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
