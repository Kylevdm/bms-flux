"""The protocol-decoder seam: validated frame bytes in, bus-address blocks out."""

from typing import Protocol

from bms_flux.readings import DecodedBlock


class ProtocolDecoder(Protocol):
    def decode(self, frame: bytes, timestamp: float) -> DecodedBlock | None:
        """Decode a validated response, or ignore an unknown block."""
        ...
