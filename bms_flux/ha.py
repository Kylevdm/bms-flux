"""Publish pack device discovery and interval-based Home Assistant snapshots."""

import asyncio
import json

from bms_flux.clock import Clock
from bms_flux.drift.findings import finding_component
from bms_flux.identity import LinkState, PackIdentity
from bms_flux.mqtt import MQTTSink
from bms_flux.protocol.seplos_v3 import SYSTEM_STATES
from bms_flux.readings import FieldValue, ReadingStream
from bms_flux.settings import Pack


class SnapshotPublisher:
    def __init__(
        self,
        sink: MQTTSink,
        packs: list[Pack],
        clock: Clock,
        identity: PackIdentity,
        lock: asyncio.Lock,
        snapshot_interval_s: int = 10,
        finding_rules: list[str] | None = None,
    ) -> None:
        self._finding_rules = finding_rules or []
        self._lock = lock
        self._identity = identity
        self._links: dict[str, LinkState] = {}
        self._sink = sink
        self._packs = packs
        self._clock = clock
        self._snapshot_interval_s = snapshot_interval_s
        self._latest: dict[str, dict[str, FieldValue]] = {}
        self._pic: dict[str, dict[str, FieldValue]] = {}
        self._deadlines = {pack.serial: clock.now() + snapshot_interval_s for pack in packs}

    async def discover(self) -> None:
        for pack in self._packs:
            identifier = f"bms_flux_{pack.serial}"
            components: dict[str, dict[str, object]] = {}
            sensors = [
                ("voltage", "V", "voltage"),
                ("current", "A", "current"),
                ("reported_soc", "%", "battery"),
                ("power", "W", "power"),
                ("soh", "%", None),
                ("remaining_capacity", "Ah", None),
                ("full_capacity", "Ah", None),
                ("cycles", None, None),
                ("lifetime_discharge", "Ah", None),
                ("avg_cell_voltage", "V", "voltage"),
                ("max_cell_voltage", "V", "voltage"),
                ("min_cell_voltage", "V", "voltage"),
                ("cell_delta", "mV", "voltage"),
                ("max_temperature", "°C", "temperature"),
                ("min_temperature", "°C", "temperature"),
                ("ambient_temperature", "°C", "temperature"),
                ("mosfet_temperature", "°C", "temperature"),
                ("suggested_charge_current", "A", "current"),
                ("suggested_discharge_current", "A", "current"),
                ("precharge", "%", None),
            ]
            sensors += [(f"cell_{i}_voltage", "V", "voltage") for i in range(1, 17)]
            sensors += [(f"cell_{i}_temperature", "°C", "temperature") for i in range(1, 5)]
            for name, unit, device_class in sensors:
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
                component = components[name]
                if unit is None:
                    del component["unit_of_measurement"]
                if device_class is None:
                    del component["device_class"]
                if name in ("suggested_charge_current", "suggested_discharge_current", "precharge"):
                    component["entity_category"] = "diagnostic"
                    component["enabled_by_default"] = False
                if name == "lifetime_discharge":
                    component["state_class"] = "total_increasing"
                if unit == "V" and name != "voltage":
                    component["suggested_display_precision"] = 3
            for name in [
                "balancing_active",
                "charge_fet",
                "discharge_fet",
                "current_limit_fet",
                "alarm",
                "protection",
            ] + [f"cell_{i}_balancing" for i in range(1, 17)]:
                component = {
                    "platform": "binary_sensor",
                    "name": name.replace("_", " ").capitalize(),
                    "unique_id": f"{identifier}_{name}",
                    "default_entity_id": f"binary_sensor.{identifier.lower()}_{name}",
                    "value_template": "{{ 'ON' if value_json." + name + " else 'OFF' }}",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                }
                if name in ("alarm", "protection"):
                    component["device_class"] = "problem"
                if name.startswith("cell_"):
                    component["entity_category"] = "diagnostic"
                    component["enabled_by_default"] = False
                components[name] = component
            for name in ("events", "system_state"):
                components[name] = {
                    "platform": "sensor",
                    "name": name.replace("_", " ").capitalize(),
                    "unique_id": f"{identifier}_{name}",
                    "default_entity_id": f"sensor.{identifier.lower()}_{name}",
                    "value_template": "{{ value_json." + name + " }}",
                }
            components["events"]["json_attributes_topic"] = f"bms_flux/{pack.serial}/state"
            components["events"]["json_attributes_template"] = (
                "{{ value_json.event_flags | tojson }}"
            )
            components["system_state"]["device_class"] = "enum"
            components["system_state"]["options"] = [*SYSTEM_STATES, "unknown"]
            for component in components.values():
                component["availability"] = [
                    {"topic": "bms_flux/status"},
                    {"topic": f"bms_flux/{pack.serial}/availability"},
                ]
                component["availability_mode"] = "all"
            components["link"] = {
                "platform": "sensor",
                "name": "Link",
                "unique_id": f"{identifier}_link",
                "default_entity_id": f"sensor.{identifier.lower()}_link",
                "device_class": "enum",
                "options": ["ok", "silent", "fingerprint_mismatch"],
                "state_topic": f"bms_flux/{pack.serial}/link",
                "availability": [{"topic": "bms_flux/status"}],
            }
            for rule in self._finding_rules:
                components[rule] = finding_component(rule, pack.serial)
            payload = {
                "device": {
                    "identifiers": [identifier],
                    "name": pack.label if pack.label is not None else pack.serial,
                    "manufacturer": "Seplos",
                    "model": "V3",
                    "serial_number": pack.serial,
                    "via_device": "bms_flux_bridge",
                },
                "origin": {"name": "bms-flux"},
                "state_topic": f"bms_flux/{pack.serial}/state",
                "components": components,
            }
            await self._sink.publish(
                f"homeassistant/device/{identifier}/config", json.dumps(payload), retain=True
            )

    async def sync_links(self) -> None:
        # Publishing awaits, and time moves on meanwhile: re-expire and repeat until the
        # published link states match identity, so a stall can't leave a stale "ok" behind.
        changed = True
        while changed:
            changed = False
            self._identity.expire(self._clock.now())
            for serial, link in list(self._identity.links.items()):
                if self._links.get(serial) == link:
                    continue
                changed = True
                self._links[serial] = link
                await self._sink.publish(f"bms_flux/{serial}/link", link, retain=True)
                await self._sink.publish(
                    f"bms_flux/{serial}/availability",
                    "online" if link == "ok" else "offline",
                    retain=True,
                )
                if link == "ok":
                    await self._publish(serial)

    async def consume(self, stream: ReadingStream) -> None:
        while True:
            reading = await stream.get()
            latest = self._latest.setdefault(reading.serial, {})
            latest.update(reading.fields)
            if reading.block == "PIC":
                previous = self._pic.get(reading.serial)
                self._pic[reading.serial] = reading.fields
                if (
                    self._links.get(reading.serial) == "ok"
                    and previous is not None
                    and any(
                        previous.get(name) != value
                        for name, value in reading.fields.items()
                        if name.endswith(("_alarm", "_protection", "_fault", "_latch", "_fet"))
                        or name == "output_short_circuit"
                        or (name.startswith("cell_") and not name.endswith("_balancing"))
                    )
                ):
                    await self._publish(reading.serial)
            stream.task_done()

    async def _publish(self, serial: str) -> None:
        # Freshness is rechecked here, after any awaits that led to this publish.
        self._identity.expire(self._clock.now())
        if self._identity.links[serial] != "ok":
            return
        await self._sink.publish(f"bms_flux/{serial}/state", json.dumps(self._latest[serial]))
        self._deadlines[serial] = self._clock.now() + self._snapshot_interval_s

    async def publish_snapshots(self) -> None:
        while True:
            delay = min(
                self._deadlines.values(), default=self._clock.now() + self._snapshot_interval_s
            )
            delay = min(delay, self._identity.next_expiry(self._clock.now()))
            await self._clock.sleep(max(0, delay - self._clock.now()))
            async with self._lock:
                self._identity.expire(self._clock.now())
                await self.sync_links()
                for serial, deadline in self._deadlines.copy().items():
                    if deadline <= self._clock.now():
                        if self._identity.links[serial] == "ok" and serial in self._latest:
                            await self._publish(serial)
                        else:
                            self._deadlines[serial] = self._clock.now() + self._snapshot_interval_s
