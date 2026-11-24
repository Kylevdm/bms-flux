"""Publish pack device discovery and ten-second Home Assistant snapshots."""

import json

from bms_flux.clock import Clock
from bms_flux.mqtt import MQTTSink
from bms_flux.readings import Reading, ReadingStream
from bms_flux.settings import Pack

SNAPSHOT_INTERVAL = 10


class SnapshotPublisher:
    def __init__(self, sink: MQTTSink, packs: list[Pack], clock: Clock) -> None:
        self._sink = sink
        self._packs = packs
        self._clock = clock
        self._latest: dict[str, Reading] = {}

    async def discover(self) -> None:
        for pack in self._packs:
            identifier = f"bms_flux_{pack.serial}"
            components = {}
            for name, unit, device_class in (
                ("voltage", "V", "voltage"),
                ("current", "A", "current"),
                ("reported_soc", "%", "battery"),
            ):
                components[name] = {
                    "platform": "sensor",
                    "name": name.replace("_", " ").capitalize(),
                    "unique_id": f"{identifier}_{name}",
                    "default_entity_id": f"sensor.{identifier.lower()}_{name}",
                    "unit_of_measurement": unit,
                    "device_class": device_class,
                    "state_class": "measurement",
                    "value_template": "{{ value_json." + name + " }}",
                }
            payload = {
                "device": {
                    "identifiers": [identifier],
                    "name": pack.serial,
                    "manufacturer": "Seplos",
                    "model": "V3",
                    "serial_number": pack.serial,
                },
                "origin": {"name": "bms-flux"},
                "state_topic": f"bms_flux/{pack.serial}/state",
                "components": components,
            }
            await self._sink.publish(
                f"homeassistant/device/{identifier}/config", json.dumps(payload), retain=True
            )

    async def consume(self, stream: ReadingStream) -> None:
        while True:
            reading = await stream.get()
            self._latest[reading.serial] = reading
            stream.task_done()

    async def publish_snapshots(self) -> None:
        while True:
            await self._clock.sleep(SNAPSHOT_INTERVAL)
            for serial, reading in self._latest.copy().items():
                await self._sink.publish(f"bms_flux/{serial}/state", json.dumps(reading.fields))
