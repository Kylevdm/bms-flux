# bms-flux ships as an HA add-on and a standalone daemon, not an HA integration

A custom integration looks like the more natural way into Home Assistant, and add-ons were thought unable to reach the RS485 adapter. They can: with `uart: true`, the Supervisor maps the host's serial devices into the add-on, `/dev/serial/by-id/...` included. Where an add-on can't see the adapter (because it's plugged into another machine), an integration can't either. So bms-flux runs as its own process: the add-on on HAOS, or the same package as a compose daemon or a pip install elsewhere. It talks to HA only through MQTT discovery.

## Considered Options

- **HA custom integration**: rejected.
  - The bus listener would live inside HA's process, so every HA restart or update would leave gaps in the readings that drift rules depend on.
  - It could not share one package and Dockerfile with the daemon and pip install.
  - It would hide the full-rate stream inside HA, away from other apps.
  - It would replace the MQTT discovery schema with HA entity classes.

## Consequences

- The add-on declares `uart: true` and takes the bus as a by-id path in its options. Because one process must own the port, bms-flux opens it exclusively rather than trusting the add-on's device mapping.
- bms-flux depends on an MQTT broker (`core_mosquitto` in add-on mode, via `services: [mqtt:need]`).
- If the adapter can't sit on the HA box, the answer is the daemon on that machine or the Waveshare gateway in raw TCP mode, never an integration.
