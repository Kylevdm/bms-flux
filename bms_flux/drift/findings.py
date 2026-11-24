"""Store latched findings by rule and pack serial and publish their evidence."""

import asyncio
import json
import logging
from pathlib import Path
from typing import TypedDict

from bms_flux.mqtt import MQTTSink

_LOG = logging.getLogger(__name__)


class Finding(TypedDict):
    active: bool
    anchor_soc: float
    reported_soc: float
    gap: float
    detected_at: float
    thresholds: dict[str, float]


class FindingStore:
    def __init__(self, state_dir: Path, sink: MQTTSink) -> None:
        self._path = state_dir / "findings.json"
        self._sink = sink
        self._states: dict[str, dict[str, Finding]] = {}

    def load(self) -> None:
        try:
            data = json.loads(self._path.read_text())
            if not isinstance(data, dict):
                raise ValueError("invalid findings")
            states: dict[str, dict[str, Finding]] = {}
            for rule, packs in data.items():
                if not isinstance(packs, dict):
                    raise ValueError("invalid findings")
                states[rule] = {}
                for serial, finding in packs.items():
                    if (
                        not isinstance(finding, dict)
                        or not isinstance(finding.get("active"), bool)
                        or not all(
                            isinstance(finding.get(key), (int, float))
                            for key in ("anchor_soc", "reported_soc", "gap", "detected_at")
                        )
                        or not isinstance(finding.get("thresholds"), dict)
                        or not all(
                            isinstance(key, str) and isinstance(value, (int, float))
                            for key, value in finding["thresholds"].items()
                        )
                    ):
                        raise ValueError("invalid finding")
                    states[rule][serial] = Finding(
                        active=finding["active"],
                        anchor_soc=finding["anchor_soc"],
                        reported_soc=finding["reported_soc"],
                        gap=finding["gap"],
                        detected_at=finding["detected_at"],
                        thresholds=finding["thresholds"],
                    )
            self._states = states
        except (OSError, ValueError):
            _LOG.warning("Finding state missing or corrupt; starting empty")
            self._states = {}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self._states, allow_nan=False))
        temporary.replace(self._path)

    async def set(self, rule: str, serial: str, finding: Finding) -> None:
        previous = self._states.get(rule, {}).get(serial)
        if previous is not None and previous["active"] == finding["active"]:
            return
        if previous is None and not finding["active"]:
            return
        self._states.setdefault(rule, {})[serial] = finding
        await asyncio.to_thread(self._save)
        await self.publish(rule, serial)

    async def publish(self, rule: str, serial: str) -> None:
        state = self._states.get(rule, {}).get(serial)
        await self._sink.publish(
            f"bms_flux/{serial}/{rule}",
            json.dumps(state if state is not None else {"active": False}),
            retain=True,
        )


def finding_component(rule: str, serial: str) -> dict[str, object]:
    identifier = f"bms_flux_{serial}_{rule}"
    topic = f"bms_flux/{serial}/{rule}"
    return {
        "platform": "binary_sensor",
        "name": rule.replace("_", " ").capitalize(),
        "unique_id": identifier,
        "default_entity_id": f"binary_sensor.{identifier.lower()}",
        "device_class": "problem",
        "state_topic": topic,
        "value_template": "{{ 'ON' if value_json.active else 'OFF' }}",
        "payload_on": "ON",
        "payload_off": "OFF",
        "json_attributes_topic": topic,
        "availability": [{"topic": "bms_flux/status"}],
    }
