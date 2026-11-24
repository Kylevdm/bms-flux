"""Bank-wide SoC divergence drift rule.

The rule fires when the pack reported SoC spread stays above *spread_on_pts*
for *hold_min* minutes, and clears when it stays below *spread_off_pts* for the
same hold. Any gap in bank availability resets the timer; the finding does not
latch and is not persisted.
"""

import json
import logging

from bms_flux.mqtt import MQTTSink
from bms_flux.settings import DriftDivergence

RULE = "drift_divergence"
_LOG = logging.getLogger(__name__)


class Divergence:
    def __init__(self, settings: DriftDivergence, sink: MQTTSink) -> None:
        self._settings = settings
        self._sink = sink
        self._active = False
        self._hold_start: float | None = None
        self._clear_start: float | None = None
        self._last_spread: float | None = None

    async def evaluate(self, available: bool, spread: float, ts: float) -> None:
        """Evaluate the rule against one bank snapshot."""
        self._last_spread = spread
        if not available:
            if self._active or self._hold_start is not None or self._clear_start is not None:
                self.reset()
                await self._publish()
            return

        settings = self._settings
        if not self._active:
            self._clear_start = None
            if spread > settings.spread_on_pts:
                if self._hold_start is None:
                    self._hold_start = ts
                    await self._publish()
                elif ts - self._hold_start >= settings.hold_min * 60:
                    self._active = True
                    await self._publish()
            elif self._hold_start is not None:
                self._hold_start = None
                await self._publish()
        else:
            if spread < settings.spread_off_pts:
                if self._clear_start is None:
                    self._clear_start = ts
                    await self._publish()
                elif ts - self._clear_start >= settings.hold_min * 60:
                    self._active = False
                    self._hold_start = None
                    self._clear_start = None
                    await self._publish()
            elif self._clear_start is not None:
                self._clear_start = None
                await self._publish()

    def reset(self) -> None:
        self._active = False
        self._hold_start = None
        self._clear_start = None

    async def publish(self) -> None:
        """Republish the current finding state, e.g. after broker reconnect."""
        await self._publish()

    async def _publish(self) -> None:
        payload: dict[str, object] = {
            "active": self._active,
            "spread": self._last_spread if self._last_spread is not None else 0.0,
            "hold_start": self._hold_start,
        }
        await self._sink.publish(f"bms_flux/bank/{RULE}", json.dumps(payload), retain=True)
