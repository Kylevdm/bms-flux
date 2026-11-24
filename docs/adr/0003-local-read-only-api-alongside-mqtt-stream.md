# The stream is offered over a local read-only API as well as MQTT

The handoff said bms-flux has no inbound surface and that ha-spark reads only through Home Assistant. The stream of every reading at full rate (about 5 per second per pack) is too fast for HA entities, so it needs its own way out. MQTT alone would have kept bms-flux outbound-only. We also open a small local API, because ha-spark should reach bms-flux either through HA entities or through bms-flux's own API, never by subscribing to its broker topics directly. The API is read-only. Nothing a client sends can reach the bus or change bms-flux's state.

## Considered Options

- **MQTT only** (`bms_flux/<pack serial>/stream`): rejected as the only transport, although it still carries the stream for other apps. It would make ha-spark depend on bms-flux's topic layout and broker credentials.
- **API only**: rejected. MQTT costs almost nothing, since the broker connection already exists, and any LAN app can subscribe without a token or a port mapping.

## Consequences

- Both transports carry the same JSON messages: one per reading, with an envelope of `v`, `ts`, `serial`, `address` and `block`. The integer `v` is the only version marker. Paths and topics are unversioned.
- The API is `GET /stream` (WebSocket, filtered by `serial`, `block` and `raw`) and `GET /packs` (the latest readings and link state). It has no write or control endpoints, and no history or replay.
- The API exists only when `api_token` is set, and every request needs that bearer token. In the add-on, the port is declared but not mapped to the host, so other add-ons reach it on the internal network. The owner maps it for LAN apps. The connection is plain HTTP.
- A slow client is closed when its bounded queue (about 5 s) fills. It never holds back the decoder, the snapshots or the drift rules.
- The handoff's "no inbound surface" rule no longer holds. The boundary is now that nothing a client sends ever reaches the bus.
