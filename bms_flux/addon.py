"""Add-on helpers: detect add-on mode and read the MQTT service from the Supervisor."""

import json
import os
import urllib.request
from pathlib import Path
from typing import Any

from bms_flux.settings import MQTTSettings, Settings, load_settings


def _supervisor_token() -> str | None:
    return os.environ.get("SUPERVISOR_TOKEN") or None


def is_addon_mode(options_path: Path = Path("/data/options.json")) -> bool:
    """Return True when running inside the Home Assistant Supervisor as an add-on."""
    return _supervisor_token() is not None and options_path.exists()


def fetch_mqtt(base_url: str = "http://supervisor") -> MQTTSettings:
    """Fetch broker details from the Supervisor MQTT service.

    The Supervisor token is read from the ``SUPERVISOR_TOKEN`` environment
    variable and is never logged.
    """
    token = _supervisor_token()
    if token is None:
        raise RuntimeError("SUPERVISOR_TOKEN is not set")
    request = urllib.request.Request(
        f"{base_url}/services/mqtt",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload: dict[str, Any] = json.loads(response.read().decode("utf-8"))
    data = payload.get("data", {})
    password = data.get("password")
    return MQTTSettings(
        host=data["host"],
        port=int(data.get("port", 1883)),
        username=data.get("username") or None,
        password=password if password else None,
    )


def load_addon_settings(options_path: Path = Path("/data/options.json")) -> Settings:
    """Load settings in add-on mode, with MQTT and state_dir supplied by the Supervisor."""
    mqtt = fetch_mqtt()
    return load_settings(
        options_path=options_path,
        mqtt=mqtt,
        state_dir=Path("/data"),
        api_port=8765,
    )
