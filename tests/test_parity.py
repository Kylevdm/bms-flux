"""The parity check turns recorded HA history into a report and a verdict."""

from datetime import UTC, datetime
from typing import Any

from tools import parity

MASTER = "SP112B2410100228"
OTHER = "SP18B2311040070"
START = 1_764_000_000.0
INTERVAL = 30.0


def iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat()


def record(
    entity_id: str, timestamp: float, state: float | str, **attributes: Any
) -> dict[str, Any]:
    return {
        "entity_id": entity_id,
        "state": str(state),
        "last_changed": iso(timestamp),
        "last_updated": iso(timestamp),
        "attributes": attributes,
    }


class History:
    """A tiny recorder-shaped history keyed by entity id."""

    def __init__(self) -> None:
        self.entities: dict[str, list[dict[str, Any]]] = {}

    def add(self, entity_id: str, timestamp: float, state: float | str, **attributes: Any) -> None:
        self.entities.setdefault(entity_id, []).append(
            record(entity_id, timestamp, state, **attributes)
        )

    def flux_pack(self, serial: str, timestamp: float, fields: dict[str, Any]) -> None:
        for field, value in fields.items():
            self.add(f"sensor.bms_flux_{serial.lower()}_{field}", timestamp, value)

    def link(self, serial: str, timestamp: float, state: str) -> None:
        self.add(f"sensor.bms_flux_{serial.lower()}_link", timestamp, state)

    def ble(
        self,
        timestamp: float,
        arrays: dict[str, list[float]],
        *,
        state: float | str = 0.001,
    ) -> None:
        self.add(
            "sensor.sp112b2410100228_delta_cell_voltage",
            timestamp,
            state,
            **arrays,
        )

    def as_ha_response(self) -> list[list[dict[str, Any]]]:
        return list(self.entities.values())


def cells(base: float, index: int) -> list[float]:
    return [base + cell * 0.001 + index * 0.0001 for cell in range(16)]


def master_fields(index: int) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "voltage": 51.70 + index * 0.001,
        "current": -1.0 - index * 0.01,
        "reported_soc": 31.3,
        "cycles": 177,
        "ambient_temperature": 24.0,
        "mosfet_temperature": 30.0,
    }
    for cell, value in enumerate(cells(3.230, index), start=1):
        fields[f"cell_{cell}_voltage"] = value
    for cell, value in enumerate([25.0, 25.1, 25.2, 25.3], start=1):
        fields[f"cell_{cell}_temperature"] = value
    return fields


def other_fields(index: int) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "voltage": 51.80 + index * 0.001,
        "current": -2.0 - index * 0.01,
        "reported_soc": 38.2,
        "cycles": 535,
        "ambient_temperature": 25.0,
        "mosfet_temperature": 31.0,
    }
    for cell, value in enumerate(cells(3.240, index), start=1):
        fields[f"cell_{cell}_voltage"] = value
    for cell, value in enumerate([26.0, 26.1, 26.2, 26.3], start=1):
        fields[f"cell_{cell}_temperature"] = value
    return fields


def build(count: int = 20) -> tuple[History, float, float]:
    history = History()
    history.link(MASTER, START, "ok")
    history.link(OTHER, START, "ok")
    last = START
    for index in range(count):
        timestamp = START + index * INTERVAL
        last = timestamp
        master = master_fields(index)
        other = other_fields(index)
        history.flux_pack(MASTER, timestamp - 2, master)
        history.flux_pack(OTHER, timestamp - 2, other)
        history.ble(
            timestamp,
            {
                "pack_voltage": [master["voltage"], other["voltage"]],
                "pack_current": [master["current"], other["current"]],
                "pack_battery_level": [master["reported_soc"], other["reported_soc"]],
                "pack_cycles": [master["cycles"], other["cycles"]],
                "cell_voltages": [master[f"cell_{cell}_voltage"] for cell in range(1, 17)]
                + [other[f"cell_{cell}_voltage"] for cell in range(1, 17)],
                "temperature_sensors": [
                    master["cell_1_temperature"],
                    master["cell_2_temperature"],
                    master["cell_3_temperature"],
                    master["cell_4_temperature"],
                    master["ambient_temperature"],
                    master["mosfet_temperature"],
                    other["cell_1_temperature"],
                    other["cell_2_temperature"],
                    other["cell_3_temperature"],
                    other["cell_4_temperature"],
                    other["ambient_temperature"],
                    other["mosfet_temperature"],
                ],
            },
        )
    return history, START, last + INTERVAL


def analyze(history: History, start: float, end: float, **kwargs: Any) -> parity.ParityReport:
    return parity.analyze(
        history.as_ha_response(),
        start=start,
        end=end,
        purge_keep_days=kwargs.pop("purge_keep_days", 7),
        **kwargs,
    )


def test_matching_history_passes_and_confirms_array_layout() -> None:
    history, start, end = build()
    report = analyze(history, start, end)
    markdown = report.to_markdown()

    assert report.passed
    assert report.decode_passed
    assert report.stability_passed
    assert "PASS" in markdown
    assert "Pack order" in markdown
    assert "slot 0" in markdown
    assert "cells 1-4, ambient, MOSFET" in markdown
    assert "SP112B2410100228" in markdown
    assert "voltage" in markdown
    assert "bank_current" in markdown
    assert "purge_keep_days" not in markdown


def test_bad_field_fails_and_names_its_worst_pairs() -> None:
    history, start, end = build()
    for point in history.entities["sensor.sp112b2410100228_delta_cell_voltage"]:
        attributes = point["attributes"]
        attributes["cell_voltages"][0] += 0.020
    report = analyze(history, start, end)
    markdown = report.to_markdown()

    assert not report.decode_passed
    failing = [comparison for comparison in report.comparisons if not comparison.passed]
    assert [comparison.field for comparison in failing] == ["cell_1_voltage"]
    assert "SP112B2410100228/cell_1_voltage" in markdown
    assert "Worst pairs" in markdown
    assert "FAIL" in markdown


def test_alignment_drops_stale_snapshots_and_unavailable_bms_ble() -> None:
    history = History()
    history.link(MASTER, START, "ok")
    history.link(OTHER, START, "ok")
    master, other = master_fields(0), other_fields(0)
    arrays = {
        "pack_voltage": [master["voltage"], other["voltage"]],
        "pack_current": [master["current"], other["current"]],
        "pack_battery_level": [master["reported_soc"], other["reported_soc"]],
        "pack_cycles": [master["cycles"], other["cycles"]],
        "cell_voltages": [master[f"cell_{cell}_voltage"] for cell in range(1, 17)]
        + [other[f"cell_{cell}_voltage"] for cell in range(1, 17)],
        "temperature_sensors": [
            *[25.0, 25.1, 25.2, 25.3, 24.0, 30.0],
            *[26.0, 26.1, 26.2, 26.3, 25.0, 31.0],
        ],
    }
    history.flux_pack(MASTER, START, master)
    history.flux_pack(OTHER, START, other)
    history.ble(START, arrays)
    history.ble(START + 5, arrays)
    history.ble(START + 20, arrays)
    history.ble(START + 35, arrays, state="unavailable")

    report = analyze(history, START, START + 40)

    assert report.comparisons
    voltage = next(
        comparison
        for comparison in report.comparisons
        if comparison.pack == MASTER and comparison.field == "voltage"
    )
    assert voltage.pairs == 2
    assert all(comparison.pairs == 2 for comparison in report.comparisons)


def test_swapped_pack_arrays_fail_the_order_confirmation() -> None:
    history, start, end = build()
    for point in history.entities["sensor.sp112b2410100228_delta_cell_voltage"]:
        attributes = point["attributes"]
        for name in (
            "pack_voltage",
            "pack_current",
            "pack_battery_level",
            "pack_cycles",
        ):
            attributes[name] = list(reversed(attributes[name]))
        attributes["cell_voltages"] = (
            attributes["cell_voltages"][16:] + attributes["cell_voltages"][:16]
        )
        temperatures = attributes["temperature_sensors"]
        attributes["temperature_sensors"] = temperatures[6:] + temperatures[:6]
    report = analyze(history, start, end)
    markdown = report.to_markdown()

    assert not report.passed
    assert not report.decode_passed
    assert "ordered by bus address" in markdown


def test_ambiguous_pack_order_fails_loudly() -> None:
    history, start, end = build()
    for index in range(20):
        master = master_fields(index)
        other = other_fields(index)
        other["cycles"] = master["cycles"]
        other["voltage"] = master["voltage"]
        history.flux_pack(OTHER, START + index * INTERVAL - 2, other)
        point = history.entities["sensor.sp112b2410100228_delta_cell_voltage"][index]
        point["attributes"]["pack_cycles"] = [master["cycles"], master["cycles"]]
        point["attributes"]["pack_voltage"] = [master["voltage"], master["voltage"]]
    report = analyze(history, start, end)
    markdown = report.to_markdown()

    assert not report.decode_passed
    assert "cannot confirm" in markdown.lower()


def test_swapped_temperature_layout_fails_confirmation() -> None:
    history, start, end = build()
    for point in history.entities["sensor.sp112b2410100228_delta_cell_voltage"]:
        temperatures = point["attributes"]["temperature_sensors"]
        temperatures[4], temperatures[5] = temperatures[5], temperatures[4]
    report = analyze(history, start, end)
    markdown = report.to_markdown()

    assert not report.decode_passed
    assert "temperature layout" in markdown.lower()


def test_ha_restart_is_excluded_and_listed() -> None:
    history, start, end = build(count=10)
    # A gap on both sides is an HA/host restart, not a bms-flux fault.
    for index in list(range(10, 14)) + list(range(16, 20)):
        timestamp = START + index * INTERVAL
        end = timestamp + INTERVAL
        master = master_fields(index)
        other = other_fields(index)
        history.flux_pack(MASTER, timestamp - 2, master)
        history.flux_pack(OTHER, timestamp - 2, other)
        history.ble(
            timestamp,
            {
                "pack_voltage": [master["voltage"], other["voltage"]],
                "pack_current": [master["current"], other["current"]],
                "pack_battery_level": [master["reported_soc"], other["reported_soc"]],
                "pack_cycles": [master["cycles"], other["cycles"]],
                "cell_voltages": [master[f"cell_{cell}_voltage"] for cell in range(1, 17)]
                + [other[f"cell_{cell}_voltage"] for cell in range(1, 17)],
                "temperature_sensors": [
                    *[25.0, 25.1, 25.2, 25.3, 24.0, 30.0],
                    *[26.0, 26.1, 26.2, 26.3, 25.0, 31.0],
                ],
            },
        )
    report = analyze(history, start, end)
    markdown = report.to_markdown()

    assert report.stability_passed
    assert report.restarts
    assert "Excluded restarts" in markdown


def test_silent_link_and_fingerprint_mismatch_fail_stability() -> None:
    history, start, end = build(count=10)
    history.link(MASTER, START + 150, "silent")
    report = analyze(history, start, end)
    assert not report.stability_passed

    history, start, end = build(count=10)
    history.link(OTHER, START + 150, "fingerprint_mismatch")
    report = analyze(history, start, end)
    markdown = report.to_markdown()
    assert not report.stability_passed
    assert "fingerprint_mismatch" in markdown


def test_bms_flux_gap_while_bms_ble_continues_fails_stability() -> None:
    history = History()
    history.link(MASTER, START, "ok")
    history.link(OTHER, START, "ok")
    for index in range(20):
        timestamp = START + index * INTERVAL
        master = master_fields(index)
        other = other_fields(index)
        if not 5 <= index < 9:
            history.flux_pack(MASTER, timestamp - 2, master)
            history.flux_pack(OTHER, timestamp - 2, other)
        history.ble(
            timestamp,
            {
                "pack_voltage": [master["voltage"], other["voltage"]],
                "pack_current": [master["current"], other["current"]],
                "pack_battery_level": [master["reported_soc"], other["reported_soc"]],
                "pack_cycles": [master["cycles"], other["cycles"]],
                "cell_voltages": [master[f"cell_{cell}_voltage"] for cell in range(1, 17)]
                + [other[f"cell_{cell}_voltage"] for cell in range(1, 17)],
                "temperature_sensors": [
                    *[25.0, 25.1, 25.2, 25.3, 24.0, 30.0],
                    *[26.0, 26.1, 26.2, 26.3, 25.0, 31.0],
                ],
            },
        )
    report = analyze(history, START, START + 20 * INTERVAL)
    markdown = report.to_markdown()

    assert not report.stability_passed
    assert "core-fault" in markdown.lower()


def test_bank_current_failure_is_reported() -> None:
    history, start, end = build()
    for point in history.entities["sensor.sp112b2410100228_delta_cell_voltage"]:
        point["attributes"]["pack_current"] = [-2.0, -3.0]
    report = analyze(history, start, end)

    bank = next(
        comparison for comparison in report.comparisons if comparison.field == "bank_current"
    )
    assert not bank.passed
    assert not report.decode_passed


def test_purge_keep_days_under_seven_warns() -> None:
    history, start, end = build()
    report = analyze(history, start, end, purge_keep_days=3)
    markdown = report.to_markdown()

    assert any("purge_keep_days" in warning for warning in report.warnings)
    assert "purge_keep_days" in markdown


def test_address_order_option_confirms_the_bus_order() -> None:
    history, start, end = build(count=10)
    report = analyze(history, start, end, address_order=[MASTER, OTHER])
    assert report.pack_order_confirmed

    history, start, end = build(count=10)
    report = analyze(history, start, end, address_order=[OTHER, MASTER])
    assert not report.pack_order_confirmed
    assert "ordered by bus address" in report.to_markdown()


def test_link_state_from_before_the_window_is_used() -> None:
    history, start, end = build(count=10)
    report = analyze(history, start + 100, end)

    assert report.stability_passed
    assert all(stability.ratio == 1 for stability in report.stability)
