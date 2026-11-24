"""Check add-on detection, Supervisor MQTT fetch and options-schema sync."""

import io
import json
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import BaseModel
from pydantic_core import PydanticUndefined

from bms_flux import addon
from bms_flux.settings import (
    DAEMON_ONLY_KEYS,
    MQTTSettings,
    OCVPoint,
    Pack,
    Settings,
    load_settings,
)

REQUIRED_PLACEHOLDERS: dict[str, Any] = {"bus": ""}


def _flatten_options(options: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in options.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                result[f"{key}.{subkey}"] = subvalue
        else:
            result[key] = value
    return result


def _flatten_schema(schema: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for key, value in schema.items():
        keys.add(key)
        if isinstance(value, dict):
            for subkey in value:
                keys.add(f"{key}.{subkey}")
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            for subkey in value[0]:
                keys.add(f"{key}.{subkey}")
    return keys


def _default_value(field: Any) -> Any:
    value = field.default_factory() if field.default_factory else field.default
    return None if value is PydanticUndefined else value


def _settings_keys_and_defaults() -> tuple[set[str], dict[str, Any]]:
    keys: set[str] = set()
    defaults: dict[str, Any] = {}
    for name, field in Settings.model_fields.items():
        if name in DAEMON_ONLY_KEYS:
            continue
        keys.add(name)
        value = _default_value(field)
        if isinstance(value, BaseModel):
            for subname, subfield in type(value).model_fields.items():
                keys.add(f"{name}.{subname}")
                subvalue = _default_value(subfield)
                if subvalue is not None:
                    defaults[f"{name}.{subname}"] = subvalue
        elif name in ("packs", "ocv_table"):
            defaults[name] = value
            item_model = Pack if name == "packs" else OCVPoint
            for subname, subfield in item_model.model_fields.items():
                keys.add(f"{name}.{subname}")
                subvalue = _default_value(subfield)
                if subvalue is not None:
                    defaults[f"{name}.{subname}"] = subvalue
        else:
            defaults[name] = value
    return keys, defaults


def test_addon_options_sync_with_settings_model() -> None:
    config_path = Path("bms_flux_addon/config.yaml")
    config = yaml.safe_load(config_path.read_text())
    options = _flatten_options(config["options"])
    schema_keys = _flatten_schema(config["schema"])
    settings_keys, settings_defaults = _settings_keys_and_defaults()

    # SecretStr defaults must be compared as plain strings.
    for key, value in settings_defaults.items():
        if hasattr(value, "get_secret_value"):
            settings_defaults[key] = value.get_secret_value()

    assert schema_keys == settings_keys

    for key, value in options.items():
        if key in REQUIRED_PLACEHOLDERS:
            assert value == REQUIRED_PLACEHOLDERS[key]
        elif key in settings_defaults:
            assert value == settings_defaults[key], f"default mismatch for {key}"
        else:
            pytest.fail(f"{key} in add-on options but has no Settings default")

    for key in settings_defaults:
        assert key in options, f"{key} has Settings default but is missing from add-on options"


def test_is_addon_mode_requires_token_and_options_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options = tmp_path / "options.json"
    absent = tmp_path / "absent.json"
    monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)
    assert not addon.is_addon_mode(options_path=options)
    options.write_text("{}")
    assert not addon.is_addon_mode(options_path=options)
    monkeypatch.setenv("SUPERVISOR_TOKEN", "test-token")
    assert not addon.is_addon_mode(options_path=absent)
    assert addon.is_addon_mode(options_path=options)


def test_fetch_mqtt_reads_supervisor_response_and_hides_password(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    captured: dict[str, Any] = {}

    def fake_urlopen(request: urllib.request.Request, **kwargs: object) -> io.BytesIO:
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        response = {
            "data": {
                "host": "core-mosquitto",
                "port": 1883,
                "username": "addons",
                "password": "secret-broker-password",
                "ssl": False,
            }
        }
        return io.BytesIO(json.dumps(response).encode("utf-8"))

    monkeypatch.setenv("SUPERVISOR_TOKEN", "test-token")
    monkeypatch.setattr("bms_flux.addon.urllib.request.urlopen", fake_urlopen)
    mqtt = addon.fetch_mqtt()
    assert mqtt.host == "core-mosquitto"
    assert mqtt.port == 1883
    assert mqtt.username == "addons"
    assert mqtt.password is not None
    assert mqtt.password.get_secret_value() == "secret-broker-password"
    assert captured["url"] == "http://supervisor/services/mqtt"
    assert captured["headers"].get("Authorization") == "Bearer test-token"
    assert "secret-broker-password" not in caplog.text


def test_fetch_mqtt_without_token_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="SUPERVISOR_TOKEN"):
        addon.fetch_mqtt()


def test_load_addon_settings_supplies_daemon_only_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options = tmp_path / "options.json"
    options.write_text(json.dumps({"bus": "/dev/example"}))
    monkeypatch.setenv("SUPERVISOR_TOKEN", "test-token")

    def fake_urlopen(request: urllib.request.Request, **kwargs: object) -> io.BytesIO:
        return io.BytesIO(
            json.dumps({"data": {"host": "core-mosquitto", "port": 1883}}).encode("utf-8")
        )

    monkeypatch.setattr("bms_flux.addon.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setenv("BMS_FLUX_STATE_DIR", "/wrong")
    settings = addon.load_addon_settings(options_path=options)
    assert settings.bus == "/dev/example"
    assert settings.mqtt.host == "core-mosquitto"
    assert settings.state_dir == Path("/data")
    assert settings.api_port == 8765


@pytest.mark.asyncio
async def test_cli_run_uses_addon_settings_in_supervisor_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options = tmp_path / "options.json"
    options.write_text(json.dumps({"bus": "/dev/example"}))
    monkeypatch.setenv("SUPERVISOR_TOKEN", "test-token")

    monkeypatch.setattr("bms_flux.addon.is_addon_mode", lambda: True)

    addon_calls: list[Path] = []

    def fake_load_addon_settings(options_path: Path = Path("/data/options.json")) -> Settings:
        addon_calls.append(options_path)
        return Settings(
            bus="/dev/example",
            mqtt=MQTTSettings(host="core-mosquitto", port=1883),
        )

    monkeypatch.setattr("bms_flux.addon.load_addon_settings", fake_load_addon_settings)

    @asynccontextmanager
    async def fake_mqtt_client(*args: Any, **kwargs: Any) -> Any:
        class FakeClient:
            async def publish(self, *args: Any, **kwargs: Any) -> None:
                pass

        yield FakeClient()

    monkeypatch.setattr("bms_flux.cli.aiomqtt.Client", fake_mqtt_client)

    class FakeTransport:
        def close(self) -> None:
            pass

    async def fake_open(bus: str) -> FakeTransport:
        return FakeTransport()

    monkeypatch.setattr("bms_flux.transport.SerialTransport.open", staticmethod(fake_open))

    class FakeBridge:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def run(self) -> None:
            pass

    monkeypatch.setattr("bms_flux.cli.Bridge", FakeBridge)

    from bms_flux.cli import run

    await run()
    assert addon_calls == [Path("/data/options.json")]


def test_init_kwargs_override_env_and_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options = tmp_path / "options.json"
    options.write_text(json.dumps({"bus": "/dev/options", "snapshot_interval_s": 20}))
    config = tmp_path / "settings.yaml"
    config.write_text("bus: /dev/yaml\nmqtt: {host: yaml}\n")
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    monkeypatch.setenv("BMS_FLUX_BUS", "tcp://localhost:1234")
    monkeypatch.setenv("BMS_FLUX_MQTT__HOST", "env")
    mqtt = MQTTSettings(host="init", port=1883)
    settings = load_settings(
        options_path=options,
        mqtt=mqtt,
        state_dir=Path("/data"),
        api_port=8765,
    )
    assert settings.bus == "tcp://localhost:1234"
    assert settings.mqtt.host == "init"
    assert settings.state_dir == Path("/data")
    assert settings.api_port == 8765
