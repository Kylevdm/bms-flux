# Changelog

## 0.1.0

Initial add-on release for bms-flux V1.
- Passive RS485 listener for Seplos V3 Modbus RTU at 19200 8N1.
- MQTT discovery for one Home Assistant device per mapped pack.
- Drift rules: divergence, full-charge anchor and rested-OCV anchor.
- Full-rate stream over MQTT and a token-gated local read-only API on port 8765.
