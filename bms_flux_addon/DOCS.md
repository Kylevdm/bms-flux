# bms-flux add-on

bms-flux listens passively to a bank of Seplos BMS packs on RS485 and publishes their telemetry to Home Assistant through MQTT discovery. It never transmits on the bus.

## Required Home Assistant services

- **MQTT**: add the Mosquitto broker (or any broker registered with the Supervisor MQTT service). The add-on reads host, port, username and password automatically from the Supervisor.
- **UART**: the CH340 RS485 adapter must be plugged into the HAOS host. With `uart: true` the Supervisor maps `/dev/serial/by-id/...` into the add-on.

## Configuration

| Option | Description |
|--------|-------------|
| `bus` | Absolute serial path (e.g. `/dev/serial/by-id/usb-1a86_USB_Serial-if00-port0`) or `tcp://gateway:port` for a Waveshare RS485-to-Ethernet gateway in raw transparent mode. |
| `packs` | Address → pack serial map. Leave empty to start discovery mode. Each entry needs `address` (1–16) and `serial` (`[A-Z0-9]+`); `label` is optional. |
| `snapshot_interval_s` | Seconds between HA snapshot publishes. |
| `silent_after_s` | Seconds without a block before a pack is marked silent. |
| `log_level` | `debug`, `info`, `warning` or `error`. |
| `stream` | Publish the full-rate stream to `bms_flux/<serial>/stream`. |
| `raw_frames` | Include raw Modbus frames in the stream. |
| `api_token` | Bearer token for the local read-only API. Leave empty to disable the API. |
| `drift_divergence` | Bank-level drift rule: spread that raises/clears a finding and how long it must hold. |
| `drift_full_charge` | Pack-level anchor finding for a pack that is not full when it should be. |
| `drift_rested_ocv` | Pack-level anchor finding based on rested open-circuit voltage. |
| `ocv_table` | Optional override of the built-in LFP OCV-to-SoC table. Empty means use the CALCE A123 midpoint table. |

The local API always listens on port `8765` inside the container. Map it to a host port only if a LAN app needs it.

## State

Add-on state is stored in `/data`.

## Release procedure

1. Update `bms_flux_addon/config.yaml` `version` and add an entry to `bms_flux_addon/CHANGELOG.md`.
2. Commit the bump.
3. Tag the commit `vX.Y.Z` and push the branch and the tag.
4. Open/merge the PR to `main`.

The `BUILD_VERSION` argument passed to the Dockerfile must equal the pushed `vX.Y.Z` tag, because the image installs the package from that tag.
