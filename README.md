# bms-flux

A read-only asyncio bridge from a bank of Seplos V3 packs to Home Assistant over
MQTT device discovery. bms-flux listens to the master pack's traffic; it never
transmits on the bus. Serial settings are fixed at **19200 8N1**, and the port is
opened exclusively.

## Run the tracer bullet

Install the package with Python 3.11 or newer, then name a YAML settings file:

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
mapping: pack serials cannot be learned passively.

```sh
BMS_FLUX_CONFIG=/path/to/settings.yaml bms-flux run
```

This first slice decodes PIA readings and discovers voltage, current and reported
SoC sensors on one HA device per configured pack. Positive current means charging.
Discovery is retained at `homeassistant/device/bms_flux_<pack serial>/config`;
JSON snapshots are not retained and publish every 10 seconds at
`bms_flux/<pack serial>/state`. No snapshot is sent until a reading is available.
Unmapped bus addresses never become packs.

Fingerprint guarding, link state, reconnect behaviour, other blocks and sensors,
TCP transport, the full-rate API, and packaging are subsequent tickets. This
slice has no persisted state and does not yet guard against a bus address swap.

## Development

```sh
pip install -e '.[dev]'
ruff check .
ruff format --check .
mypy
pytest -q
```

Tests use fake transports and MQTT sinks, a controllable clock and recorded probe
fixtures, not hardware or a broker. The read-only reference capture script under
`probe/` is excluded from Ruff; production code and tests are checked in CI on
Python 3.11 and 3.12.
