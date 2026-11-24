# bms-flux only listens to the bus and never transmits

The master pack already reads PIA, PIB and PIC from every pack about every 200 ms, which keeps the bus about 70% busy (probe, 2026-11-24). A second Modbus master would collide with it, and the collisions could disturb the packs' own parallel coordination. bms-flux therefore splits and decodes the master's traffic without sending anything. This is stricter than "never sends a write frame".

## Considered Options

- **Active master** (bms-flux polls each pack): rejected because of the collision risk on a bus that is already busy.
- **Hybrid** (sniff, plus rare reads for the serial and EIA in quiet gaps): deferred. It is the only way to read pack serials or EIA, and it could come back as its own decision.

## Consequences

- Pack serials and EIA (the system aggregate) can't be read, so the pack serial behind each bus address must come from somewhere else.
- bms-flux sees data only while a master pack is polling.
- A network gateway must pass raw bytes through (transparent TCP), not act as a Modbus TCP master.
