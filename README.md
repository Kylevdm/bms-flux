# bms-flux

A read-only asyncio bridge from a bank of Seplos V3 packs to Home Assistant over
MQTT device discovery. bms-flux listens to the master pack's traffic; it never
transmits on the bus. Serial settings are fixed at **19200 8N1**, and the port is
opened exclusively.

## What V1 does

- **Passive sniff**: the master pack polls every pack on the bus roughly every
  200 ms; bms-flux decodes those PIA/PIB/PIC responses and never speaks. A
  hand-rolled frame splitter resyncs through noise and trusts only frames that
  pass the CRC-16/Modbus check.
- **Pack identity**: the owner maps bus addresses to pack serials in the config.
  A fingerprint guard (cycle count and lifetime discharge never fall) catches an
  address swap; a swapped pack goes `fingerprint_mismatch` and is never
  published under another serial. A `link` sensor per pack reports
  `ok` / `silent` / `fingerprint_mismatch`.
- **Home Assistant**: one MQTT device per pack (voltage, current, reported SoC,
  cells, temperatures, FET and alarm states), plus a bms-flux device that
  carries the bank (bank current and power, capacity-weighted reported SoC,
  SoC spread), bus health (`bus` enum, `bus_problem`, CRC error rate,
  unmapped addresses). Snapshots publish every 10 s and immediately when an
  alarm, protection or FET state changes.
- **Drift findings**: three rules raise `problem` binary sensors with their
  evidence — divergence (pack SoC spread held apart), the full-charge anchor
  and the rested-OCV anchor. Anchor findings latch across restarts until the
  next anchor agrees; each rule can be switched off or tuned.
- **Full-rate data**: every reading also goes to `bms_flux/<pack serial>/stream`
  as JSON, and (when `api_token` is set) to a token-gated local read-only API
  (`GET /packs`, WebSocket `/stream`) on port 8765. Raw frames are opt-in with
  `raw_frames`.
- **Resilience**: transport faults and a busy port are retried forever with
  backoff; the broker reconnects and republishes discovery and state without
  replaying the stream; a failing consumer is contained and never stops the
  decoder.

## Configure and run

Install the package with Python 3.11 or newer, then name a YAML settings file
(daemon/pip install). The same keys are the add-on's options:

```yaml
bus: /dev/serial/by-id/usb-1a86_USB_Serial-if00-port0
packs:
  - address: 1
    serial: SP112B2410100228
  - address: 2
    serial: SP18B2311040070
mqtt:
  host: localhost
  port: 1883
```

The optional MQTT keys are `username` and `password`. Keep credentials out of
version control. The owner must supply the correct bus address → pack serial
mapping: pack serials cannot be learned passively. Leave `packs` empty to run a
discovery mode that reports bus health and unmapped addresses only. A
`tcp://gateway:port` bus addresses a Waveshare RS485-to-Ethernet gateway in raw
transparent mode.

Run the daemon:

```sh
BMS_FLUX_CONFIG=/path/to/settings.yaml bms-flux run
```

To fill in the map, listen first (never transmitting) and list the addresses it
hears with cycles and reported SoC:

```sh
bms-flux probe --bus /dev/serial/by-id/usb-1a86_USB_Serial-if00-port0
```

Home Assistant users install the add-on from this repository instead; see
`bms_flux_addon/DOCS.md`. Compose users build the same image with
`docker compose build` (set `BMS_FLUX_VERSION` to the release tag).

## Development

```sh
pip install -e '.[dev]'
ruff check .
ruff format --check .
mypy
pytest -q
```

Tests use fake transports and MQTT sinks, a controllable clock and recorded
probe fixtures, not hardware or a broker. The read-only reference capture script
under `probe/` is excluded from Ruff; production code and tests are checked in
CI on Python 3.11 and 3.12.

The 7-day parity check against bms_ble runs with `tools/parity.py`; see its
module docstring.
