"""The protocol-decoder seam: validated frame bytes in, mapped readings out."""

from typing import Protocol

from bms_flux.readings import Reading


class ProtocolDecoder(Protocol):
    def decode(self, frame: bytes, timestamp: float) -> Reading | None:
        """Decode a validated response, or ignore an unknown block or bus address."""
        ...
