"""Attach owner-mapped pack serials and gate readings on fingerprint and freshness.

Fingerprints are high-water counters within this process run. A fall in either
counter latches fingerprint_mismatch until restart, even if later counters rise.
The decisions do not define a reset or persistent baseline; restart establishes a
new baseline, so the owner must verify the address-to-serial map before restarting.
This guard detects counter regressions, not every possible address swap.
"""

from typing import Literal

from bms_flux.readings import DecodedBlock, Reading
from bms_flux.settings import Pack

LinkState = Literal["ok", "silent", "fingerprint_mismatch"]


class PackIdentity:
    def __init__(self, packs: list[Pack], silent_after_s: int) -> None:
        self.serials = {pack.address: pack.serial for pack in packs}
        self.links: dict[str, LinkState] = {pack.serial: "silent" for pack in packs}
        self._silent_after_s = silent_after_s
        self._blocks: dict[str, dict[str, Reading]] = {pack.serial: {} for pack in packs}
        self._fingerprints: dict[str, tuple[float, float]] = {}
        self.last_seen: dict[str, float | None] = {pack.serial: None for pack in packs}

    def readings(self, serial: str) -> tuple[Reading, ...]:
        """Return the latest accepted blocks, including those of a silent pack."""
        return tuple(self._blocks[serial].values())

    def next_expiry(self, now: float) -> float:
        return min(
            (
                reading.ts + self._silent_after_s
                for serial, blocks in self._blocks.items()
                if self.links[serial] == "ok"
                for reading in blocks.values()
            ),
            default=now + self._silent_after_s,
        )

    def expire(self, now: float) -> None:
        for serial, blocks in self._blocks.items():
            if self.links[serial] == "ok" and any(
                now - reading.ts >= self._silent_after_s for reading in blocks.values()
            ):
                self.links[serial] = "silent"

    def accept(self, block: DecodedBlock, now: float) -> list[Reading]:
        self.expire(now)
        serial = self.serials.get(block.address)
        if serial is None or self.links[serial] == "fingerprint_mismatch":
            return []
        if block.block == "PIA":
            cycles = block.fields["cycles"]
            discharge = block.fields["lifetime_discharge"]
            assert isinstance(cycles, (int, float)) and isinstance(discharge, (int, float))
            fingerprint = (float(cycles), float(discharge))
            previous = self._fingerprints.get(serial)
            if previous is not None and any(
                a < b for a, b in zip(fingerprint, previous, strict=True)
            ):
                self.links[serial] = "fingerprint_mismatch"
                self._blocks[serial].clear()
                return []
            self._fingerprints[serial] = fingerprint
        reading = Reading(block.address, block.ts, block.block, block.fields, serial)
        blocks = self._blocks[serial]
        blocks[block.block] = reading
        self.last_seen[serial] = reading.ts
        if self.links[serial] == "ok":
            return [reading]
        if all(
            name in blocks and now - blocks[name].ts < self._silent_after_s
            for name in ("PIA", "PIB", "PIC")
        ):
            self.links[serial] = "ok"
            return [blocks[name] for name in ("PIA", "PIB", "PIC")]
        return []
