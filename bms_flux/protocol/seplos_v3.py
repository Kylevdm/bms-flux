"""Decode Seplos V3 PIA responses using the owner's address-to-serial mapping."""

import struct

from bms_flux.readings import Reading
from bms_flux.settings import Pack


class SeplosV3Decoder:
    def __init__(self, packs: list[Pack]) -> None:
        self._serials = {pack.address: pack.serial for pack in packs}

    def decode(self, frame: bytes, timestamp: float) -> Reading | None:
        if len(frame) != 41 or frame[1:3] != b"\x04\x24":
            return None
        serial = self._serials.get(frame[0])
        if serial is None:
            return None
        registers = struct.unpack(">18H", frame[3:-2])
        current = int.from_bytes(frame[5:7], "big", signed=True)
        return Reading(
            serial,
            frame[0],
            timestamp,
            "PIA",
            {
                "voltage": registers[0] / 100,
                "current": current / 100,
                "remaining_capacity": registers[2] / 100,
                "full_capacity": registers[3] / 100,
                "reported_soc": registers[5] / 10,
                "soh": registers[6] / 10,
                "cycles": registers[7],
                "avg_cell_voltage": registers[8] / 1000,
            },
        )
