"""Publish the bms-flux device and all-or-nothing bank snapshots."""

import asyncio
import json
from collections import deque
from collections.abc import Callable

from bms_flux.clock import Clock
from bms_flux.drift.divergence import Divergence
from bms_flux.frames import Frame
from bms_flux.identity import PackIdentity
from bms_flux.mqtt import MQTTSink
from bms_flux.readings import FieldValue, ReadingStream
from bms_flux.settings import (
    BUS_HEALTH_CLEAR_RATIO,
    BUS_HEALTH_PROBLEM_RATIO,
    BUS_HEALTH_WINDOW_S,
    DISCOVERY_PREFIX,
)
from bms_flux.transport import BusState


class BankPublisher:
    def __init__(
        self,
        sink: MQTTSink,
        clock: Clock,
        identity: PackIdentity,
        lock: asyncio.Lock,
        snapshot_interval_s: int,
        bus_state: Callable[[], BusState] = lambda: "connected",
        divergence: Divergence | None = None,
    ) -> None:
        self._sink = sink
        self._bus_state = bus_state
        self._clock = clock
        self._identity = identity
        self._lock = lock
        self._interval = snapshot_interval_s
        self._divergence = divergence
        self._deadline = clock.now() + snapshot_interval_s
        self._available: bool | None = None
        self._latest: dict[str, dict[str, FieldValue]] = {}
        self._frames: deque[tuple[float, int, int]] = deque()
        self._crc_errors = 0
        self._master_seen: float | None = None
        self._problem = True
        self._health: dict[str, object] = {}
        self._health_deadline = clock.now()

    async def clean_discovery(self) -> None:
        configured = set(self._identity.serials.values())
        prefix = f"{DISCOVERY_PREFIX}/device/bms_flux_"
        for topic, _ in await self._sink.read_retained(
            f"{DISCOVERY_PREFIX}/device/+/config", self._clock
        ):
            if not topic.startswith(prefix) or not topic.endswith("/config"):
                continue
            serial = topic[len(prefix) : -len("/config")]
            if serial and "/" not in serial and serial != "bridge" and serial not in configured:
                await self._sink.publish(topic, "", retain=True)

    async def discover(self) -> None:
        components: dict[str, dict[str, object]] = {}
        for name, unit in (
            ("bank_current", "A"),
            ("bank_power", "W"),
            ("reported_soc", "%"),
            ("soc_spread", "%"),
        ):
            components[name] = {
                "platform": "sensor",
                "name": name.replace("_", " ").capitalize(),
                "unique_id": f"bms_flux_{name}",
                "default_entity_id": f"sensor.bms_flux_{name}",
                "unit_of_measurement": unit,
                "state_class": "measurement",
                "state_topic": "bms_flux/bank/state",
                "value_template": "{{ value_json." + name + " }}",
                "availability": [
                    {"topic": "bms_flux/status"},
                    {"topic": "bms_flux/bank/availability"},
                ],
                "availability_mode": "all",
            }
        for name, platform in (
            ("frames_per_second", "sensor"),
            ("crc_errors", "sensor"),
            ("master_pack_seen", "binary_sensor"),
            ("bus_problem", "binary_sensor"),
        ):
            component: dict[str, object] = {
                "platform": platform,
                "name": name.replace("_", " ").capitalize(),
                "unique_id": f"bms_flux_{name}",
                "default_entity_id": f"{platform}.bms_flux_{name}",
                "state_topic": "bms_flux/bus/state",
                "availability": [{"topic": "bms_flux/status"}],
                "value_template": "{{ value_json." + name + " }}",
            }
            if platform == "binary_sensor":
                component["value_template"] = "{{ 'ON' if value_json." + name + " else 'OFF' }}"
            if name == "bus_problem":
                component["device_class"] = "problem"
            elif name == "frames_per_second":
                component["unit_of_measurement"] = "frames/s"
                component["state_class"] = "measurement"
            elif name == "crc_errors":
                component["state_class"] = "total_increasing"
            components[name] = component
        if self._divergence is not None:
            components["drift_divergence"] = {
                "platform": "binary_sensor",
                "name": "Divergence",
                "unique_id": "bms_flux_drift_divergence",
                "default_entity_id": "binary_sensor.bms_flux_drift_divergence",
                "device_class": "problem",
                "state_topic": "bms_flux/bank/drift_divergence",
                "value_template": "{{ 'ON' if value_json.active else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                "json_attributes_topic": "bms_flux/bank/drift_divergence",
                "availability": [
                    {"topic": "bms_flux/status"},
                    {"topic": "bms_flux/bank/availability"},
                ],
                "availability_mode": "all",
            }
        components["bus"] = {
            "platform": "sensor",
            "name": "Bus",
            "unique_id": "bms_flux_bus",
            "default_entity_id": "sensor.bms_flux_bus",
            "device_class": "enum",
            "options": ["connected", "reconnecting", "port_busy"],
            "state_topic": "bms_flux/bus/state",
            "value_template": "{{ value_json.bus }}",
            "availability": [{"topic": "bms_flux/status"}],
        }
        components["unmapped_addresses"] = {
            "platform": "sensor",
            "name": "Unmapped addresses",
            "unique_id": "bms_flux_unmapped_addresses",
            "default_entity_id": "sensor.bms_flux_unmapped_addresses",
            "state_topic": "bms_flux/unmapped_addresses",
            "value_template": "{{ value_json.unmapped_addresses | join(', ') }}",
            "availability": [{"topic": "bms_flux/status"}],
        }
        await self._sink.publish(
            "homeassistant/device/bms_flux_bridge/config",
            json.dumps(
                {
                    "device": {"identifiers": ["bms_flux_bridge"], "name": "bms-flux"},
                    "origin": {"name": "bms-flux"},
                    "components": components,
                }
            ),
            retain=True,
        )

    def record_frames(self, frames: list[Frame], crc_errors: int) -> None:
        now = self._clock.now()
        errors = crc_errors - self._crc_errors
        self._crc_errors = crc_errors
        if frames or errors:
            self._frames.append((now, len(frames), errors))
        if any(frame.kind == "resp" and frame.data[0] == 1 for frame in frames):
            self._master_seen = now

    async def publish_health(self) -> None:
        now = self._clock.now()
        while self._frames and self._frames[0][0] <= now - BUS_HEALTH_WINDOW_S:
            self._frames.popleft()
        good = sum(count for _, count, _ in self._frames)
        bad = sum(count for _, _, count in self._frames)
        ratio = bad / (good + bad) if good + bad else 0
        if good == 0 or ratio > BUS_HEALTH_PROBLEM_RATIO:
            self._problem = True
        elif ratio < BUS_HEALTH_CLEAR_RATIO:
            self._problem = False
        health: dict[str, object] = {
            "bus": self._bus_state(),
            "frames_per_second": good / BUS_HEALTH_WINDOW_S,
            "crc_errors": self._crc_errors,
            "master_pack_seen": self._master_seen is not None
            and now - self._master_seen < BUS_HEALTH_WINDOW_S,
            "bus_problem": self._problem,
        }
        # frames/s moves on almost every read; publish on the snapshot interval, and at
        # once only when a state HA acts on (bus_problem, master seen) flips.
        flipped = any(
            health[key] != self._health.get(key)
            for key in ("bus_problem", "master_pack_seen", "bus")
        )
        if flipped or now >= self._health_deadline:
            self._health = health
            self._health_deadline = now + self._interval
            await self._sink.publish("bms_flux/bus/state", json.dumps(health))

    async def consume(self, stream: ReadingStream) -> None:
        while True:
            reading = await stream.get()
            if reading.block == "PIA":
                self._latest[reading.serial] = reading.fields
            stream.task_done()

    async def republish(self) -> None:
        """Restore bank availability, snapshot and bus health after reconnect."""
        self._available = None
        self._deadline = self._clock.now()
        self._health_deadline = self._clock.now()
        await self.sync()
        await self.publish_health()

    async def sync(self) -> None:
        self._identity.expire(self._clock.now())
        available = bool(self._identity.links) and all(
            link == "ok" and serial in self._latest for serial, link in self._identity.links.items()
        )
        fields = list(self._latest.values())
        capacities = [self._number(f, "full_capacity") for f in fields]
        capacity = sum(capacities)
        # A zero reported full capacity cannot define a weighted reported SoC.
        available = available and capacity != 0
        spread = 0.0
        if fields:
            socs = [self._number(f, "reported_soc") for f in fields]
            spread = max(socs) - min(socs)
        if self._divergence is not None:
            await self._divergence.evaluate(available, spread, self._clock.now())
        recovered = available and not self._available
        if available != self._available:
            self._available = available
            await self._sink.publish(
                "bms_flux/bank/availability", "online" if available else "offline", retain=True
            )
        if available and (recovered or self._clock.now() >= self._deadline):
            currents = [self._number(f, "current") for f in fields]
            powers = [self._number(f, "power") for f in fields]
            socs = [self._number(f, "reported_soc") for f in fields]
            await self._sink.publish(
                "bms_flux/bank/state",
                json.dumps(
                    {
                        "bank_current": sum(currents),
                        "bank_power": sum(powers),
                        "reported_soc": sum(s * c for s, c in zip(socs, capacities, strict=True))
                        / capacity,
                        "soc_spread": max(socs) - min(socs),
                    }
                ),
            )
            self._deadline = self._clock.now() + self._interval

    @staticmethod
    def _number(fields: dict[str, FieldValue], name: str) -> float:
        value = fields[name]
        assert isinstance(value, (int, float))
        return float(value)

    async def publish_snapshots(self) -> None:
        while True:
            await self._clock.sleep(1)
            async with self._lock:
                await self.sync()
                await self.publish_health()
