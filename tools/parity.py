"""Compare bms-flux against bms_ble from Home Assistant recorder history.

The parity check reads both sides from HA's history REST API for one window,
aligns every bms_ble update with bms-flux's last snapshot at or before it, and
writes a Markdown report with separate decode and stability verdicts.

This module is a repo tool, not part of the shipped package::

    HA_URL=http://homeassistant.local:8123 HA_TOKEN=... \
        python tools/parity.py --start 2026-11-24T00:00:00Z \
        --purge-keep-days 10 --output parity.md

Pass ``--packs`` in ascending bus-address order to confirm the array order
explicitly; without it the master pack must still be array slot 0.

The token is read from HA_TOKEN and is never printed or put in the report.
"""

from __future__ import annotations

import argparse
import bisect
import itertools
import json
import os
import statistics
import sys
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

BMS_BLE_MASTER_SERIAL = "SP112B2410100228"
PARITY_DAYS = 7
PARITY_WINDOW_S = PARITY_DAYS * 24 * 60 * 60

# bms_ble polls about every 31 s, so a longer hole on both sides means HA or the
# host was down rather than bms-flux or the bus.
RESTART_GAP_S = 60.0
MAX_SNAPSHOT_AGE_S = 10.0
LINK_OK_RATIO = 0.995
TEMPERATURE_LAYOUT_EPSILON_C = 0.5
BLE_MERGE_S = 1.0

BLE_ATTRIBUTES = (
    "pack_voltage",
    "pack_current",
    "pack_battery_level",
    "pack_cycles",
    "cell_voltages",
    "temperature_sensors",
)

PACK_FIELDS = (
    "voltage",
    "current",
    "reported_soc",
    "cycles",
    *(f"cell_{cell}_voltage" for cell in range(1, 17)),
    *(f"cell_{cell}_temperature" for cell in range(1, 5)),
    "ambient_temperature",
    "mosfet_temperature",
)

TEMPERATURE_FIELDS = (
    "cell_1_temperature",
    "cell_2_temperature",
    "cell_3_temperature",
    "cell_4_temperature",
    "ambient_temperature",
    "mosfet_temperature",
)

UNAVAILABLE = frozenset({"", "unknown", "unavailable", "none"})


@dataclass(frozen=True)
class FieldSpec:
    tolerance: float
    relative: float = 0.0
    required_ratio: float = 0.99

    def limit(self, reference: float) -> float:
        if self.relative:
            return max(self.tolerance, abs(reference) * self.relative)
        return self.tolerance


FIELD_SPECS: dict[str, FieldSpec] = {
    "voltage": FieldSpec(0.15, required_ratio=0.95),
    "current": FieldSpec(1.0, relative=0.05, required_ratio=0.95),
    "reported_soc": FieldSpec(0.2),
    "cycles": FieldSpec(0.0),
    **{f"cell_{cell}_voltage": FieldSpec(0.005) for cell in range(1, 17)},
    **{field: FieldSpec(0.2) for field in TEMPERATURE_FIELDS},
    "bank_current": FieldSpec(1.0, relative=0.05, required_ratio=0.95),
}


@dataclass(frozen=True)
class Point:
    ts: float
    state: str
    attributes: dict[str, Any]


@dataclass
class Series:
    points: list[Point] = field(default_factory=list)
    timestamps: list[float] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.points.sort(key=lambda point: point.ts)
        self.timestamps = [point.ts for point in self.points]

    def last_at(self, ts: float) -> Point | None:
        index = bisect.bisect_right(self.timestamps, ts) - 1
        return self.points[index] if index >= 0 else None


@dataclass
class PackHistory:
    serial: str
    fields: dict[str, Series]
    link: Series | None

    def snapshot(self, ts: float) -> tuple[float, dict[str, float]] | None:
        values: dict[str, float] = {}
        newest = float("-inf")
        for name, series in self.fields.items():
            point = series.last_at(ts)
            if point is None:
                return None
            value = _number(point.state)
            if value is None:
                return None
            values[name] = value
            newest = max(newest, point.ts)
        if len(values) != len(PACK_FIELDS):
            return None
        return newest, values


@dataclass(frozen=True)
class BleObservation:
    ts: float
    pack_voltage: list[float]
    pack_current: list[float]
    pack_battery_level: list[float]
    pack_cycles: list[float]
    cell_voltages: list[float]
    temperature_sensors: list[float]


@dataclass(frozen=True)
class Pair:
    ts: float
    ble: float
    flux: float
    diff: float
    limit: float
    within: bool


@dataclass
class Comparison:
    pack: str
    field: str
    pairs: int
    within: int
    median_signed_diff: float
    median_limit: float
    required_ratio: float
    worst: list[Pair]

    @property
    def ratio(self) -> float:
        return self.within / self.pairs if self.pairs else 0.0

    @property
    def passed(self) -> bool:
        if not self.pairs:
            return False
        return (
            self.ratio >= self.required_ratio
            and abs(self.median_signed_diff) <= self.median_limit / 2
        )


@dataclass
class PackStability:
    pack: str
    window_s: float
    ok_s: float
    fingerprint_mismatch: bool
    core_fault: bool

    @property
    def ratio(self) -> float:
        return self.ok_s / self.window_s if self.window_s else 0.0

    @property
    def passed(self) -> bool:
        return (
            self.window_s > 0
            and self.ratio >= LINK_OK_RATIO
            and not self.fingerprint_mismatch
            and not self.core_fault
        )


@dataclass(frozen=True)
class Interval:
    start: float
    end: float

    @property
    def duration(self) -> float:
        return self.end - self.start


@dataclass
class ParityReport:
    window_start: float
    window_end: float
    comparisons: list[Comparison]
    stability: list[PackStability]
    restarts: list[Interval]
    core_faults: list[Interval]
    warnings: list[str]
    decode_errors: list[str]
    stability_errors: list[str]
    pack_order_confirmed: bool
    pack_order_note: str
    temperature_layout_confirmed: bool
    temperature_layout_note: str
    purge_keep_days: int | None

    @property
    def decode_passed(self) -> bool:
        return (
            not self.decode_errors
            and self.pack_order_confirmed
            and self.temperature_layout_confirmed
            and bool(self.comparisons)
            and all(comparison.passed for comparison in self.comparisons)
        )

    @property
    def stability_passed(self) -> bool:
        return (
            not self.stability_errors
            and bool(self.stability)
            and all(stability.passed for stability in self.stability)
        )

    @property
    def passed(self) -> bool:
        return self.decode_passed and self.stability_passed

    def to_markdown(self) -> str:
        verdict = "PASS" if self.passed else "FAIL"
        lines = [
            "# bms-flux / bms_ble parity check",
            "",
            f"Window: {_iso(self.window_start)} to {_iso(self.window_end)} "
            f"({(self.window_end - self.window_start) / 86400:.2f} days)",
            "",
            f"**Verdict: {verdict}**",
            "",
            "## Checks",
            "",
            "| Check | Result |",
            "| --- | --- |",
            f"| Decode | {'PASS' if self.decode_passed else 'FAIL'} |",
            f"| Stability | {'PASS' if self.stability_passed else 'FAIL'} |",
            "",
            "## Assumptions confirmed",
            "",
            f"- Pack order: {'PASS' if self.pack_order_confirmed else 'FAIL'} — "
            f"{self.pack_order_note}",
            f"- Temperature layout: "
            f"{'PASS' if self.temperature_layout_confirmed else 'FAIL'} — "
            f"{self.temperature_layout_note}",
        ]
        if self.warnings:
            lines += ["", "## Warnings", ""]
            lines += [f"- {warning}" for warning in self.warnings]
        if self.decode_errors or self.stability_errors:
            lines += ["", "## Errors", ""]
            lines += [f"- {error}" for error in self.decode_errors]
            lines += [f"- {error}" for error in self.stability_errors]
        if self.comparisons:
            lines += [
                "",
                "## Decode",
                "",
                "| Pack | Field | Pairs | Within | Median signed diff | Median limit | Result |",
                "| --- | --- | ---: | ---: | ---: | ---: | --- |",
            ]
            for comparison in self.comparisons:
                lines.append(
                    f"| {comparison.pack} | {comparison.field} | {comparison.pairs} | "
                    f"{comparison.within} ({comparison.ratio:.2%}) | "
                    f"{comparison.median_signed_diff:.4f} | "
                    f"{comparison.median_limit / 2:.4f} | "
                    f"{'PASS' if comparison.passed else 'FAIL'} |"
                )
            failing = [item for item in self.comparisons if not item.passed]
            if failing:
                lines += ["", "### Failing fields", ""]
                for comparison in failing:
                    lines += [
                        f"#### {comparison.pack}/{comparison.field}",
                        "",
                        f"{comparison.within} of {comparison.pairs} pairs within tolerance; "
                        f"median signed difference {comparison.median_signed_diff:.4f}, "
                        f"limit {comparison.median_limit / 2:.4f}.",
                        "",
                    ]
                    if comparison.worst:
                        lines += [
                            "Worst pairs:",
                            "",
                            "| Time | bms_ble | bms-flux | Diff | Tolerance | Within |",
                            "| --- | ---: | ---: | ---: | ---: | --- |",
                        ]
                        for pair in comparison.worst:
                            lines.append(
                                f"| {_iso(pair.ts)} | {pair.ble:.5g} | {pair.flux:.5g} | "
                                f"{pair.diff:+.5g} | {pair.limit:.5g} | "
                                f"{'yes' if pair.within else 'no'} |"
                            )
                        lines.append("")
        if self.stability:
            lines += [
                "",
                "## Stability",
                "",
                "| Pack | Link ok | Window (excl. restarts) | Fingerprint mismatch | Result |",
                "| --- | ---: | ---: | --- | --- |",
            ]
            for stability in self.stability:
                lines.append(
                    f"| {stability.pack} | {stability.ratio:.3%} | "
                    f"{stability.window_s / 3600:.2f} h | "
                    f"{'fingerprint_mismatch' if stability.fingerprint_mismatch else 'none'} | "
                    f"{'PASS' if stability.passed else 'FAIL'} |"
                )
        lines += ["", "### Excluded restarts", ""]
        if self.restarts:
            lines += [
                "| Start | End | Reason |",
                "| --- | --- | --- |",
            ]
            lines += [
                f"| {_iso(restart.start)} | {_iso(restart.end)} | HA or host restart |"
                for restart in self.restarts
            ]
        else:
            lines.append("None.")
        if self.core_faults:
            lines += ["", "### bms-flux gaps while bms_ble continued", ""]
            lines += [
                f"- {_iso(gap.start)} to {_iso(gap.end)} "
                "(core-fault exit suspected, or a transport outage)"
                for gap in self.core_faults
            ]
        lines.append("")
        return "\n".join(lines)


def analyze(
    history: Mapping[str, Sequence[Mapping[str, Any]]] | Iterable[Sequence[Mapping[str, Any]]],
    *,
    start: datetime | float | str | None = None,
    end: datetime | float | str | None = None,
    purge_keep_days: int | None = None,
    bms_ble_master: str = BMS_BLE_MASTER_SERIAL,
    address_order: Sequence[str] | None = None,
) -> ParityReport:
    """Analyse one HA history response and return the parity report."""
    points = _normalise(history)
    all_timestamps = [point.ts for records in points.values() for point in records]
    start_ts = _as_timestamp(start) if start is not None else None
    end_ts = _as_timestamp(end) if end is not None else None
    if start_ts is None:
        start_ts = min(all_timestamps, default=0.0)
    if end_ts is None:
        end_ts = max(all_timestamps, default=start_ts)

    warnings: list[str] = []
    if purge_keep_days is None:
        warnings.append(
            "recorder purge_keep_days was not provided; cannot confirm it is at least 7"
        )
    elif purge_keep_days < PARITY_DAYS:
        warnings.append(
            f"recorder purge_keep_days is {purge_keep_days}, less than the "
            f"{PARITY_DAYS}-day parity window; history may be incomplete"
        )
    if end_ts - start_ts < PARITY_WINDOW_S:
        warnings.append("window is shorter than 7 days; this is a smoke test only")

    # Keep states from before the window: the link and the last snapshot at the
    # window start are the state at or before the first bms_ble update.
    points = {
        entity_id: [point for point in records if point.ts <= end_ts]
        for entity_id, records in points.items()
    }
    points = {entity_id: records for entity_id, records in points.items() if records}

    packs, _links, entity_errors = _flux_packs(points)
    observations, observation_errors = _ble_observations(points, bms_ble_master)
    observations = [
        observation for observation in observations if start_ts <= observation.ts <= end_ts
    ]
    decode_errors = [*entity_errors, *observation_errors]
    stability_errors: list[str] = []

    if not packs:
        decode_errors.append("no bms-flux pack entities found in the history")
    if not observations:
        decode_errors.append("no bms_ble array updates found in the history")

    aligned = _align(packs, observations)
    if observations and not aligned and packs:
        decode_errors.append("no bms_ble update had a bms-flux snapshot within 10 s")

    assignment: dict[str, int] | None = None
    pack_order_confirmed = False
    pack_order_note = "not checked"
    if packs and aligned and not observation_errors:
        assignment, order_errors, pack_order_note = _assign_packs(
            packs, aligned, bms_ble_master, address_order
        )
        if order_errors:
            decode_errors.extend(order_errors)
        else:
            pack_order_confirmed = True

    comparisons: list[Comparison] = []
    temperature_layout_confirmed = False
    temperature_layout_note = "not checked"
    if assignment is not None:
        comparisons = _compare(aligned, assignment)
        if not comparisons:
            decode_errors.append("no aligned bms_ble / bms-flux pairs to compare")
        layout_ok, layout_errors, temperature_layout_note = _confirm_temperature_layout(
            aligned, assignment
        )
        temperature_layout_confirmed = layout_ok
        decode_errors.extend(layout_errors)

    flux_times = sorted(
        {
            point.ts
            for pack in packs.values()
            for series in pack.fields.values()
            for point in series.points
            if start_ts <= point.ts <= end_ts
        }
    )
    ble_times = sorted(observation.ts for observation in observations)
    restarts, core_faults = _gaps(flux_times, ble_times, start_ts, end_ts)
    stability: list[PackStability] = []
    for serial, pack in sorted(packs.items()):
        if pack.link is None:
            stability_errors.append(f"{serial}: no link history")
            continue
        stability.append(
            _stability(
                serial,
                pack.link,
                start_ts,
                end_ts,
                restarts,
                core_faults,
            )
        )
    for gap in core_faults:
        stability_errors.append(
            f"bms-flux gap from {_iso(gap.start)} to {_iso(gap.end)} while bms_ble "
            "continued (core-fault exit suspected)"
        )

    return ParityReport(
        window_start=start_ts,
        window_end=end_ts,
        comparisons=comparisons,
        stability=stability,
        restarts=restarts,
        core_faults=core_faults,
        warnings=warnings,
        decode_errors=decode_errors,
        stability_errors=stability_errors,
        pack_order_confirmed=pack_order_confirmed,
        pack_order_note=pack_order_note,
        temperature_layout_confirmed=temperature_layout_confirmed,
        temperature_layout_note=temperature_layout_note,
        purge_keep_days=purge_keep_days,
    )


def _normalise(
    history: Mapping[str, Sequence[Mapping[str, Any]]] | Iterable[Sequence[Mapping[str, Any]]],
) -> dict[str, list[Point]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    if isinstance(history, Mapping):
        for entity_id, records in history.items():
            grouped.setdefault(str(entity_id), []).extend(records)
    else:
        for item in history:
            if isinstance(item, Mapping):
                _add_record(grouped, item)
            else:
                for record in item:
                    _add_record(grouped, record)

    points: dict[str, list[Point]] = {}
    for entity_id, records in grouped.items():
        parsed: list[Point] = []
        for record in records:
            timestamp = _record_timestamp(record)
            if timestamp is None:
                continue
            attributes = record.get("attributes", {})
            parsed.append(
                Point(
                    timestamp,
                    str(record.get("state", "")),
                    dict(attributes) if isinstance(attributes, Mapping) else {},
                )
            )
        points[entity_id] = parsed
    return points


def _add_record(grouped: dict[str, list[Mapping[str, Any]]], record: Mapping[str, Any]) -> None:
    entity_id = record.get("entity_id")
    if isinstance(entity_id, str):
        grouped.setdefault(entity_id, []).append(record)


def _record_timestamp(record: Mapping[str, Any]) -> float | None:
    for key in ("last_updated", "last_changed", "last_reported", "ts"):
        value = record.get(key)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                continue
            return parsed.timestamp()
    return None


def _as_timestamp(value: datetime | float | str) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str):
        parsed = _record_timestamp({"ts": value})
        if parsed is None:
            raise ValueError(f"invalid timestamp: {value}")
        return parsed
    return float(value)


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _float_list(value: Any) -> list[float] | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return None
    numbers = [_number(item) for item in value]
    if any(number is None for number in numbers):
        return None
    return [number for number in numbers if number is not None]


def _flux_packs(
    points: Mapping[str, list[Point]],
) -> tuple[dict[str, PackHistory], dict[str, Series], list[str]]:
    fields: dict[str, dict[str, Series]] = {}
    links: dict[str, Series] = {}
    errors: list[str] = []
    expected = set(PACK_FIELDS)
    for entity_id, records in points.items():
        if not entity_id.startswith("sensor.bms_flux_"):
            continue
        body = entity_id.removeprefix("sensor.bms_flux_")
        serial_lower, separator, name = body.partition("_")
        if not separator or not serial_lower.isalnum():
            continue
        if serial_lower in {"bank", "bridge"}:
            continue
        serial = serial_lower.upper()
        if name == "link":
            links[serial] = Series(list(records))
        elif name in expected:
            fields.setdefault(serial, {})[name] = Series(list(records))
    if not fields:
        return {}, links, errors
    packs: dict[str, PackHistory] = {}
    for serial, pack_fields in fields.items():
        missing = sorted(expected - pack_fields.keys())
        if missing:
            errors.append(f"{serial}: missing bms-flux entities for {', '.join(missing)}")
        packs[serial] = PackHistory(serial, pack_fields, links.get(serial))
    return packs, links, errors


def _ble_observations(
    points: Mapping[str, list[Point]], master: str
) -> tuple[list[BleObservation], list[str]]:
    prefix = f"sensor.{master.lower()}_"
    attribute_points: dict[str, list[Point]] = {name: [] for name in BLE_ATTRIBUTES}
    entity_states: dict[str, Series] = {}
    relevant_entities: set[str] = set()
    for entity_id, records in points.items():
        if not entity_id.startswith(prefix):
            continue
        entity_states[entity_id] = Series(list(records))
        for point in records:
            if point.state.strip().lower() in UNAVAILABLE:
                continue
            for name in BLE_ATTRIBUTES:
                numbers = _float_list(point.attributes.get(name))
                if numbers is None:
                    continue
                attribute_points[name].append(Point(point.ts, "", {name: numbers}))
                relevant_entities.add(entity_id)
    errors: list[str] = []
    missing = [name for name in BLE_ATTRIBUTES if not attribute_points[name]]
    if missing:
        errors.append(f"bms_ble history is missing attributes: {', '.join(missing)}")
        return [], errors
    attribute_series = {name: Series(points) for name, points in attribute_points.items()}

    times = sorted({point.ts for series in attribute_series.values() for point in series.points})
    grouped = _group_times(times)
    observations: list[BleObservation] = []
    for timestamp in grouped:
        if any(
            (latest := entity_states[entity_id].last_at(timestamp)) is not None
            and latest.state.strip().lower() in UNAVAILABLE
            for entity_id in relevant_entities
        ):
            continue
        arrays: dict[str, list[float]] = {}
        for name in BLE_ATTRIBUTES:
            latest = attribute_series[name].last_at(timestamp)
            if latest is None:
                continue
            value = _float_list(latest.attributes.get(name))
            if value is None:
                continue
            arrays[name] = value
        if set(arrays) != set(BLE_ATTRIBUTES):
            continue
        lengths = {
            len(arrays["pack_voltage"]),
            len(arrays["pack_current"]),
            len(arrays["pack_battery_level"]),
            len(arrays["pack_cycles"]),
        }
        if len(lengths) != 1:
            errors.append("bms_ble per-pack arrays have different lengths")
            continue
        count = lengths.pop()
        if count == 0:
            errors.append("bms_ble per-pack arrays are empty")
            continue
        if len(arrays["cell_voltages"]) != count * 16:
            errors.append(
                f"bms_ble cell_voltages has {len(arrays['cell_voltages'])} values; "
                f"expected {count * 16}"
            )
            continue
        if len(arrays["temperature_sensors"]) != count * 6:
            errors.append(
                f"bms_ble temperature_sensors has "
                f"{len(arrays['temperature_sensors'])} values; expected {count * 6}"
            )
            continue
        observations.append(
            BleObservation(
                timestamp,
                arrays["pack_voltage"],
                arrays["pack_current"],
                arrays["pack_battery_level"],
                arrays["pack_cycles"],
                arrays["cell_voltages"],
                arrays["temperature_sensors"],
            )
        )
    return observations, errors


def _group_times(times: Sequence[float]) -> list[float]:
    grouped: list[float] = []
    for timestamp in times:
        if not grouped or timestamp - grouped[-1] > BLE_MERGE_S:
            grouped.append(timestamp)
        else:
            grouped[-1] = timestamp
    return grouped


@dataclass(frozen=True)
class Aligned:
    observation: BleObservation
    flux: dict[str, tuple[float, dict[str, float]]]


def _align(
    packs: Mapping[str, PackHistory], observations: Sequence[BleObservation]
) -> list[Aligned]:
    aligned: list[Aligned] = []
    for observation in observations:
        snapshots: dict[str, tuple[float, dict[str, float]]] = {}
        for serial, pack in packs.items():
            snapshot = pack.snapshot(observation.ts)
            if snapshot is None:
                continue
            snapshot_ts, values = snapshot
            if observation.ts - snapshot_ts > MAX_SNAPSHOT_AGE_S:
                continue
            snapshots[serial] = (snapshot_ts, values)
        if snapshots:
            aligned.append(Aligned(observation, snapshots))
    return aligned


def _assign_packs(
    packs: Mapping[str, PackHistory],
    aligned: Sequence[Aligned],
    master: str,
    address_order: Sequence[str] | None,
) -> tuple[dict[str, int] | None, list[str], str]:
    serials = sorted(packs)
    count = len(serials)
    if count == 0:
        return None, ["no bms-flux packs to map"], "not checked"
    slots = {len(item.observation.pack_cycles) for item in aligned} if aligned else {0}
    if slots != {count}:
        return (
            None,
            [f"bms_ble arrays have {sorted(slots)} entries but bms-flux has {count} packs"],
            "not checked",
        )

    matches = {serial: [0] * count for serial in serials}
    voltage_differences: dict[str, list[list[float]]] = {
        serial: [[] for _ in range(count)] for serial in serials
    }
    for item in aligned:
        for serial, (_, values) in item.flux.items():
            for slot in range(count):
                if _same(values["cycles"], item.observation.pack_cycles[slot]):
                    matches[serial][slot] += 1
                voltage_differences[serial][slot].append(
                    abs(values["voltage"] - item.observation.pack_voltage[slot])
                )

    def score(assignment: tuple[int, ...]) -> tuple[int, float]:
        total_matches = sum(
            matches[serial][assignment[index]] for index, serial in enumerate(serials)
        )
        total_voltage = 0.0
        for index, serial in enumerate(serials):
            differences = voltage_differences[serial][assignment[index]]
            if differences:
                total_voltage += statistics.median(differences)
        return total_matches, -total_voltage

    candidates = list(itertools.permutations(range(count)))
    scored = sorted(candidates, key=score, reverse=True)
    best = scored[0]
    if len(scored) > 1 and score(scored[1]) == score(best):
        return (
            None,
            ["cannot confirm pack order: cycles and voltage do not distinguish the packs"],
            "cannot confirm pack order",
        )
    assignment = {serial: best[index] for index, serial in enumerate(serials)}
    for serial, slot in assignment.items():
        if matches[serial][slot] == 0:
            return (
                None,
                [
                    f"cannot confirm pack order: {serial} never matches bms_ble "
                    f"slot {slot} by cycle count"
                ],
                "cannot confirm pack order",
            )

    master_serial = master.upper()
    if master_serial not in assignment:
        return (
            None,
            [f"bms_ble master {master} is not among the bms-flux packs"],
            "cannot confirm pack order",
        )
    if assignment[master_serial] != 0:
        return (
            None,
            [
                f"bms_ble arrays are not ordered by bus address: master "
                f"{master} is slot {assignment[master_serial]}, not slot 0"
            ],
            "arrays are not ordered by bus address",
        )
    if address_order is not None:
        for expected_slot, serial in enumerate(address_order):
            serial_upper = serial.upper()
            if assignment.get(serial_upper) != expected_slot:
                return (
                    None,
                    [
                        f"bms_ble arrays are not ordered by bus address: "
                        f"{serial_upper} should be slot {expected_slot}"
                    ],
                    "arrays are not ordered by bus address",
                )

    note = ", ".join(
        f"slot {slot} = {serial}" for serial, slot in sorted(assignment.items(), key=lambda i: i[1])
    )
    return assignment, [], note


def _same(left: float, right: float) -> bool:
    return abs(left - right) <= 1e-9


def _compare(aligned: Sequence[Aligned], assignment: Mapping[str, int]) -> list[Comparison]:
    pairs: dict[tuple[str, str], list[Pair]] = {}
    for serial, _slot in assignment.items():
        for name in PACK_FIELDS:
            pairs[(serial, name)] = []
    bank: list[Pair] = []

    for item in aligned:
        observation = item.observation
        currents: dict[str, tuple[float, float]] = {}
        for serial, slot in assignment.items():
            snapshot = item.flux.get(serial)
            if snapshot is None:
                continue
            _, values = snapshot
            for name, reference in _ble_values(observation, slot).items():
                spec = FIELD_SPECS[name]
                limit = spec.limit(reference)
                difference = values[name] - reference
                pairs[(serial, name)].append(
                    Pair(
                        observation.ts,
                        reference,
                        values[name],
                        difference,
                        limit,
                        abs(difference) <= limit + 1e-9,
                    )
                )
            currents[serial] = (values["current"], observation.pack_current[slot])
        if len(currents) == len(assignment):
            flux_bank = sum(flux for flux, _ in currents.values())
            ble_bank = sum(ble for _, ble in currents.values())
            spec = FIELD_SPECS["bank_current"]
            limit = spec.limit(ble_bank)
            difference = flux_bank - ble_bank
            bank.append(
                Pair(
                    observation.ts,
                    ble_bank,
                    flux_bank,
                    difference,
                    limit,
                    abs(difference) <= limit + 1e-9,
                )
            )

    comparisons = [
        _comparison(pack, name, field_pairs, FIELD_SPECS[name])
        for (pack, name), field_pairs in pairs.items()
    ]
    comparisons.append(_comparison("bank", "bank_current", bank, FIELD_SPECS["bank_current"]))
    return comparisons


def _ble_values(observation: BleObservation, slot: int) -> dict[str, float]:
    values: dict[str, float] = {
        "voltage": observation.pack_voltage[slot],
        "current": observation.pack_current[slot],
        "reported_soc": observation.pack_battery_level[slot],
        "cycles": observation.pack_cycles[slot],
    }
    for cell in range(16):
        values[f"cell_{cell + 1}_voltage"] = observation.cell_voltages[slot * 16 + cell]
    for index, name in enumerate(TEMPERATURE_FIELDS):
        values[name] = observation.temperature_sensors[slot * 6 + index]
    return values


def _comparison(pack: str, name: str, field_pairs: Sequence[Pair], spec: FieldSpec) -> Comparison:
    differences = [pair.diff for pair in field_pairs]
    limits = [pair.limit for pair in field_pairs]
    return Comparison(
        pack=pack,
        field=name,
        pairs=len(field_pairs),
        within=sum(pair.within for pair in field_pairs),
        median_signed_diff=statistics.median(differences) if differences else 0.0,
        median_limit=statistics.median(limits) if limits else spec.tolerance,
        required_ratio=spec.required_ratio,
        worst=sorted(field_pairs, key=lambda pair: abs(pair.diff), reverse=True)[:5],
    )


def _confirm_temperature_layout(
    aligned: Sequence[Aligned], assignment: Mapping[str, int]
) -> tuple[bool, list[str], str]:
    errors: list[str] = []
    notes: list[str] = []
    for serial, slot in sorted(assignment.items()):
        flux_temperatures: list[dict[str, float]] = []
        for item in aligned:
            snapshot = item.flux.get(serial)
            if snapshot is None:
                continue
            flux_temperatures.append(snapshot[1])
        if not flux_temperatures:
            errors.append(f"{serial}: no aligned temperatures to confirm the layout")
            continue
        ble_medians = [
            statistics.median(
                item.observation.temperature_sensors[slot * 6 + index]
                for item in aligned
                if serial in item.flux
            )
            for index in range(6)
        ]
        flux_medians = [
            statistics.median(values[name] for values in flux_temperatures)
            for name in TEMPERATURE_FIELDS
        ]

        def cost(
            permutation: tuple[int, ...],
            ble_medians: list[float] = ble_medians,
            flux_medians: list[float] = flux_medians,
        ) -> float:
            return sum(
                abs(ble_medians[index] - flux_medians[permutation[index]]) for index in range(6)
            )

        best = min(itertools.permutations(range(6)), key=cost)
        expected = tuple(range(6))
        if cost(best) < cost(expected) - TEMPERATURE_LAYOUT_EPSILON_C:
            errors.append(
                f"{serial}: temperature layout is not cells 1-4, ambient, MOSFET "
                f"(best order is {best})"
            )
            notes.append(f"{serial}: cells 1-4, ambient, MOSFET not confirmed")
        else:
            notes.append(f"{serial}: cells 1-4, ambient, MOSFET")
    confirmed = not errors
    if not notes:
        notes.append("cells 1-4, ambient, MOSFET")
    return confirmed, errors, "; ".join(notes)


def _gaps(
    flux_times: Sequence[float],
    ble_times: Sequence[float],
    start: float,
    end: float,
) -> tuple[list[Interval], list[Interval]]:
    restarts: list[Interval] = []
    core_faults: list[Interval] = []
    if not flux_times:
        return restarts, core_faults

    def classify(left: float, right: float) -> None:
        if right - left <= RESTART_GAP_S:
            return
        # Ignore updates within one snapshot of either edge: a bms-flux snapshot
        # and a bms_ble update in the same poll need not share a timestamp.
        index = bisect.bisect_right(ble_times, left + MAX_SNAPSHOT_AGE_S)
        ble_continued = index < len(ble_times) and ble_times[index] < right - MAX_SNAPSHOT_AGE_S
        if ble_continued:
            core_faults.append(Interval(left, right))
        else:
            restarts.append(Interval(left, right))

    classify(start, flux_times[0])
    for left, right in itertools.pairwise(flux_times):
        classify(left, right)
    classify(flux_times[-1], end)
    return restarts, core_faults


def _stability(
    serial: str,
    link: Series,
    start: float,
    end: float,
    restarts: Sequence[Interval],
    core_faults: Sequence[Interval],
) -> PackStability:
    link_points = [point for point in link.points if start <= point.ts <= end]
    states = [point.state.strip().lower() for point in link_points]
    initial = link.last_at(start)
    if initial is not None:
        states.append(initial.state.strip().lower())
    fingerprint_mismatch = "fingerprint_mismatch" in states
    boundaries = {start, end}
    boundaries.update(point.ts for point in link_points if start < point.ts < end)
    for interval in [*restarts, *core_faults]:
        boundaries.add(max(start, interval.start))
        boundaries.add(min(end, interval.end))
    ordered = sorted(boundaries)

    window = 0.0
    ok = 0.0
    for left, right in itertools.pairwise(ordered):
        if right <= left:
            continue
        middle = (left + right) / 2
        if any(interval.start <= middle <= interval.end for interval in restarts):
            continue
        window += right - left
        if any(interval.start <= middle <= interval.end for interval in core_faults):
            continue
        point = link.last_at(middle)
        if point is not None and point.state.strip().lower() == "ok":
            ok += right - left
    return PackStability(serial, window, ok, fingerprint_mismatch, bool(core_faults))


def _iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat(timespec="seconds")


def fetch_history(
    base_url: str,
    token: str,
    entity_ids: Sequence[str],
    start: float,
    end: float,
    *,
    minimal: bool,
) -> list[list[dict[str, Any]]]:
    """Read HA history for entity_ids, in chunks that keep the URL short."""
    responses: list[list[dict[str, Any]]] = []
    for offset in range(0, len(entity_ids), 20):
        chunk = entity_ids[offset : offset + 20]
        query = {
            "end_time": _iso(end),
            "filter_entity_id": ",".join(chunk),
        }
        if minimal:
            query["minimal_response"] = "1"
            query["no_attributes"] = "1"
        url = (
            f"{base_url.rstrip('/')}/api/history/period/"
            f"{urllib.parse.quote(_iso(start), safe='')}?"
            f"{urllib.parse.urlencode(query)}"
        )
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(request) as response:
            payload = json.load(response)
        if isinstance(payload, list):
            responses.extend(item for item in payload if isinstance(item, list))
    return responses


def discover_entities(base_url: str, token: str) -> list[str]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/states",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request) as response:
        states = json.load(response)
    if not isinstance(states, list):
        return []
    return [
        state["entity_id"]
        for state in states
        if isinstance(state, Mapping) and isinstance(state.get("entity_id"), str)
    ]


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="window start, ISO-8601")
    parser.add_argument("--end", help="window end, ISO-8601 (default: now)")
    parser.add_argument("--url", default=os.environ.get("HA_URL"), help="HA base URL")
    parser.add_argument(
        "--bms-ble-serial", default=BMS_BLE_MASTER_SERIAL, help="bms_ble master serial"
    )
    parser.add_argument(
        "--purge-keep-days",
        type=int,
        default=os.environ.get("HA_PURGE_KEEP_DAYS"),
        help="recorder purge_keep_days (warns when below 7)",
    )
    parser.add_argument(
        "--packs",
        nargs="*",
        help="pack serials in ascending bus-address order",
    )
    parser.add_argument("--output", default="parity-report.md", help="Markdown path")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    url = args.url or os.environ.get("HA_URL")
    token = os.environ.get("HA_TOKEN")
    if not url or not token:
        print("parity: set HA_URL and HA_TOKEN", file=sys.stderr)
        return 2
    start = _as_timestamp(args.start)
    end = _as_timestamp(args.end) if args.end else datetime.now(UTC).timestamp()
    entities = discover_entities(url, token)
    flux_entities = [entity for entity in entities if entity.startswith("sensor.bms_flux_")]
    ble_prefix = f"sensor.{args.bms_ble_serial.lower()}_"
    ble_entities = [entity for entity in entities if entity.startswith(ble_prefix)]
    history: list[list[dict[str, Any]]] = []
    if flux_entities:
        history.extend(fetch_history(url, token, flux_entities, start, end, minimal=True))
    if ble_entities:
        history.extend(fetch_history(url, token, ble_entities, start, end, minimal=False))
    report = analyze(
        history,
        start=start,
        end=end,
        purge_keep_days=args.purge_keep_days,
        bms_ble_master=args.bms_ble_serial,
        address_order=args.packs,
    )
    markdown = report.to_markdown()
    if args.output == "-":
        print(markdown)
    else:
        with open(args.output, "w", encoding="utf-8") as output:
            output.write(markdown)
        print(f"parity: wrote {args.output}: {'PASS' if report.passed else 'FAIL'}")
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
