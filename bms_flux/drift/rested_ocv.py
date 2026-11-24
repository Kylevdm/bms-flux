"""Reach a low-end OCV anchor after continuous rest in a warm pack."""

import logging
from decimal import Decimal

from bms_flux.drift.findings import Finding, FindingStore
from bms_flux.identity import PackIdentity
from bms_flux.readings import Reading, ReadingStream
from bms_flux.settings import DriftRestedOCV, OCVPoint

_LOG = logging.getLogger(__name__)
RULE = "drift_rested_ocv"

# CALCE A123 charge/discharge midpoint at 25 °C: proposed default table in
# docs/research/lfp-ocv-soc-table.md (research for issue #10).
DEFAULT_OCV_TABLE = (
    (2.790, 1.0),
    (2.915, 2.0),
    (2.995, 3.0),
    (3.100, 5.0),
    (3.165, 7.0),
    (3.193, 8.0),
    (3.209, 10.0),
    (3.215, 12.0),
    (3.226, 15.0),
    (3.237, 17.0),
    (3.251, 20.0),
    (3.269, 25.0),
    (3.283, 30.0),
    (3.303, 40.0),
    (3.307, 50.0),
    (3.310, 60.0),
    (3.325, 70.0),
    (3.346, 80.0),
    (3.352, 90.0),
    (3.361, 94.0),
    (3.366, 95.0),
    (3.374, 96.0),
    (3.386, 97.0),
    (3.410, 98.0),
    (3.455, 99.0),
    (3.490, 100.0),
)


class RestedOCV:
    def __init__(
        self,
        settings: DriftRestedOCV,
        store: FindingStore,
        identity: PackIdentity,
        silent_after_s: int,
        table: list[OCVPoint],
    ) -> None:
        self._settings = settings
        self._store = store
        self._identity = identity
        self._silent_after_s = silent_after_s
        self._table = tuple((point.cell_v, point.soc_pct) for point in table) or DEFAULT_OCV_TABLE
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

    def _soc(self, voltage: float) -> float:
        if voltage <= self._table[0][0]:
            return self._table[0][1]
        for (low_v, low_soc), (high_v, high_soc) in zip(
            self._table[:-1], self._table[1:], strict=True
        ):
            if voltage <= high_v:
                return low_soc + (voltage - low_v) / (high_v - low_v) * (high_soc - low_soc)
        return self._table[-1][1]

    async def consume(self, stream: ReadingStream) -> None:
        while True:
            reading = await stream.get()
            try:
                await self._evaluate(reading)
            except Exception:
                _LOG.exception("Rested OCV drift rule failed")
            finally:
                stream.task_done()

    async def _evaluate(self, reading: Reading) -> None:
        if reading.block != "PIA":
            return
        serial, ts = reading.serial, reading.ts
        if ts - self._last.get(serial, ts) >= self._silent_after_s:
            self._reset(serial)
        self._last[serial] = ts
        avg, current, capacity, soc, temperature = (
            reading.fields[name]
            for name in (
                "avg_cell_voltage",
                "current",
                "full_capacity",
                "reported_soc",
                "min_temperature",
            )
        )
        assert isinstance(avg, (float, int)) and isinstance(current, (float, int))
        assert isinstance(capacity, (float, int)) and isinstance(soc, (float, int))
        assert isinstance(temperature, (float, int))
        settings = self._settings
        # Compare decimal measurements exactly at the strict C-rate boundary.
        current_limit = Decimal(str(settings.max_current_c)) * Decimal(str(capacity))
        if abs(Decimal(str(current))) >= current_limit or temperature < settings.min_temp_c:
            self._reset(serial)
            return
        start = self._starts.setdefault(serial, ts)
        if ts - start < settings.rest_h * 3600 or serial in self._reached:
            return
        anchor_soc = self._soc(avg)
        if anchor_soc >= settings.ocv_soc_below_pct:
            return
        self._reached.add(serial)
        gap = abs(anchor_soc - soc)
        await self._store.set(
            RULE,
            serial,
            Finding(
                active=gap > settings.gap_pts,
                anchor_soc=anchor_soc,
                reported_soc=float(soc),
                gap=gap,
                detected_at=ts,
                thresholds={
                    "max_current_c": settings.max_current_c,
                    "rest_h": settings.rest_h,
                    "ocv_soc_below_pct": settings.ocv_soc_below_pct,
                    "gap_pts": settings.gap_pts,
                    "min_temp_c": settings.min_temp_c,
                },
            ),
        )
