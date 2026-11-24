"""Decode Seplos V3 blocks using the owner's address-to-serial mapping.

PIC flags without a bit definition in the protocol decision retain their absolute,
zero-based coil number. No meaning is inferred from the captured flag values.
"""

import struct

from bms_flux.readings import FieldValue, Reading
from bms_flux.settings import Pack

# Byte 8 is TB09, followed by TB02/03/04/05/16/06/07/08/15.
PIC_TABLES: dict[int, tuple[str, ...]] = {
    8: ("discharge", "charge", "floating_charge", "full_charge", "standby", "turn_off"),
    9: (
        "cell_high_voltage_alarm",
        "cell_overvoltage_protection",
        "cell_low_voltage_alarm",
        "cell_undervoltage_protection",
        "pack_high_voltage_alarm",
        "pack_overvoltage_protection",
        "pack_low_voltage_alarm",
    ),
    10: (
        "charge_high_temperature_alarm",
        "charge_high_temperature_protection",
        "charge_low_temperature_alarm",
        "charge_low_temperature_protection",
        "discharge_high_temperature_alarm",
        "discharge_high_temperature_protection",
        "discharge_low_temperature_alarm",
        "discharge_low_temperature_protection",
    ),
    11: (
        "ambient_high_temperature_alarm",
        "ambient_high_temperature_protection",
        "ambient_low_temperature_alarm",
        "ambient_low_temperature_protection",
        "mosfet_high_temperature_alarm",
        "mosfet_high_temperature_protection",
        "cell_low_temperature_heating",
    ),
    12: (
        "charge_current_alarm",
        "charge_current_protection",
        "second_level_charge_protection",
        "discharge_current_alarm",
        "discharge_current_protection",
        "second_level_discharge_protection",
        "output_short_circuit",
    ),
    13: (
        "short_circuit_latch",
        "bit_105",
        "second_level_charge_latch",
        "second_level_discharge_latch",
    ),
    14: ("bit_112", "bit_113", "soc_alarm", "soc_protection", "cell_difference_alarm"),
    15: ("discharge_fet", "charge_fet", "current_limit_fet", "heating"),
    16: (
        "low_soc_alarm",
        "intermittent_charge",
        "external_switch_control",
        "static_standby",
        "history_recording",
        "under_soc_protection",
        "active_current_limiting",
        "passive_current_limiting",
    ),
    17: (
        "ntc_fault",
        "afe_fault",
        "charge_mosfet_fault",
        "discharge_mosfet_fault",
        "cell_fault",
        "break_line_fault",
        "key_fault",
        "aerosol_fault",
    ),
}
SYSTEM_STATES = ["discharge", "charge", "floating_charge", "full_charge", "standby", "turn_off"]


class SeplosV3Decoder:
    def __init__(self, packs: list[Pack]) -> None:
        self._serials = {pack.address: pack.serial for pack in packs}

    def decode(self, frame: bytes, timestamp: float) -> Reading | None:
        if len(frame) < 5:
            return None
        serial = self._serials.get(frame[0])
        if serial is None:
            return None
        fields: dict[str, FieldValue]
        if len(frame) == 41 and frame[1:3] == b"\x04\x24":
            block = "PIA"
            registers = struct.unpack(">18H", frame[3:-2])
            current = int.from_bytes(frame[5:7], "big", signed=True)
            fields = {
                "voltage": registers[0] / 100,
                "current": current / 100,
                "power": registers[0] * current / 10000,
                "remaining_capacity": registers[2] / 100,
                "full_capacity": registers[3] / 100,
                "lifetime_discharge": registers[4] * 10,
                "reported_soc": registers[5] / 10,
                "soh": registers[6] / 10,
                "cycles": registers[7],
                "avg_cell_voltage": registers[8] / 1000,
                "avg_cell_temperature": (registers[9] - 2731) / 10,
                "max_cell_voltage": registers[10] / 1000,
                "min_cell_voltage": registers[11] / 1000,
                "cell_delta": registers[10] - registers[11],
                "max_temperature": (registers[12] - 2731) / 10,
                "min_temperature": (registers[13] - 2731) / 10,
            }
            fields["suggested_discharge_current"] = registers[15]
            fields["suggested_charge_current"] = registers[16]
            fields["precharge"] = registers[17] / 10
        elif len(frame) == 57 and frame[1:3] == b"\x04\x34":
            block = "PIB"
            registers = struct.unpack(">26H", frame[3:-2])
            fields = {f"cell_{i + 1}_voltage": registers[i] / 1000 for i in range(16)}
            for index in range(4):
                fields[f"cell_{index + 1}_temperature"] = (registers[16 + index] - 2731) / 10
            fields["ambient_temperature"] = (registers[24] - 2731) / 10
            fields["mosfet_temperature"] = (registers[25] - 2731) / 10
        elif len(frame) == 23 and frame[1:3] == b"\x01\x12":
            block = "PIC"
            fields = {}
            for bit in range(144):
                name = f"cell_{bit - 47}_balancing" if 48 <= bit < 64 else f"bit_{bit}"
                fields[name] = bool(frame[3 + bit // 8] & (1 << (bit % 8)))
            fields["balancing_active"] = bool(frame[9] or frame[10])
            events: dict[str, bool] = {}
            alarm = protection = False
            for bit in range(144):
                byte, offset = divmod(bit, 8)
                if 48 <= bit < 64:
                    continue
                name = f"bit_{bit}"
                is_alarm = is_protection = False
                if byte < 4:
                    cell = bit + 1 if byte < 2 else bit - 15
                    name = f"cell_{cell}_{'low' if byte < 2 else 'high'}_voltage"
                    is_alarm = True
                elif byte < 6:
                    name = f"cell_{offset + 1}_{'low' if byte == 4 else 'high'}_temperature"
                    is_alarm = True
                elif byte in PIC_TABLES and offset < len(PIC_TABLES[byte]):
                    name = PIC_TABLES[byte][offset]
                    is_alarm = name.endswith("_alarm")
                    is_protection = name.endswith(("_protection", "_fault", "_latch")) or (
                        name == "output_short_circuit"
                    )
                active = bool(frame[3 + byte] & (1 << offset))
                fields[name] = active
                alarm |= active and is_alarm
                protection |= active and is_protection
                if active and byte not in (8, 15):
                    events[name] = True
            fields["alarm"] = alarm
            fields["protection"] = protection
            state = frame[11]
            fields["system_state"] = (
                SYSTEM_STATES[state.bit_length() - 1]
                if state in (1, 2, 4, 8, 16, 32)
                else "unknown"
            )
            fields["fet_state"] = frame[18]
            fields["events"] = len(events)
            fields["event_flags"] = events
        else:
            return None
        return Reading(serial, frame[0], timestamp, block, fields)
