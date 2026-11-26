"""Reach a full-charge anchor from continuous qualifying PIA readings."""

import logging

from bms_flux.drift.findings import Finding, FindingStore
from bms_flux.identity import PackIdentity
from bms_flux.readings import Reading, ReadingStream
from bms_flux.settings import DriftFullCharge

_LOG = logging.getLogger(__name__)
RULE = "drift_full_charge"


class FullCharge:
    def __init__(
        self,
        settings: DriftFullCharge,
        store: FindingStore,
        identity: PackIdentity,
        silent_after_s: int,
    ) -> None:
        self._settings = settings
        self._store = store
        self._identity = identity
        self._silent_after_s = silent_after_s
        self._starts: dict[str, float] = {}
        self._last: dict[str, float] = {}
        self._reached: set[str] = set()

    def reset_unavailable(self) -> None:
        for serial, link in self._identity.links.items():
            if link != "ok":
                self._reset(serial)

    def _reset(self, serial: str) -> None:
        self._starts.pop(serial, None)
        self._reached.discard(serial)

    async def consume(self, stream: ReadingStream) -> None:
        while True:
            reading = await stream.get()
            try:
                await self._evaluate(reading)
            except Exception:
                _LOG.exception("Full-charge drift rule failed")
            finally:
                stream.task_done()

    async def _evaluate(self, reading: Reading) -> None:
        if reading.block != "PIA":
            return
        serial, ts = reading.serial, reading.ts
        if ts - self._last.get(serial, ts) >= self._silent_after_s:
            self._reset(serial)
        self._last[serial] = ts
        fields = reading.fields
        avg, current, capacity, soc = (
            fields[name]
            for name in ("avg_cell_voltage", "current", "full_capacity", "reported_soc")
        )
        assert isinstance(avg, (float, int)) and isinstance(current, (float, int))
        assert isinstance(capacity, (float, int)) and isinstance(soc, (float, int))
        settings = self._settings
        if avg < settings.cell_avg_v or abs(current) >= settings.max_current_c * capacity:
            self._reset(serial)
            return
        start = self._starts.setdefault(serial, ts)
        if ts - start < settings.hold_min * 60 or serial in self._reached:
            return
        self._reached.add(serial)
        await self._store.set(
            RULE,
            serial,
            Finding(
                active=soc < settings.soc_below_pct,
                anchor_soc=100.0,
                reported_soc=float(soc),
                gap=100.0 - soc,
                detected_at=ts,
                thresholds={
                    "cell_avg_v": settings.cell_avg_v,
                    "max_current_c": settings.max_current_c,
                    "hold_min": settings.hold_min,
                    "soc_below_pct": settings.soc_below_pct,
                },
            ),
        )
