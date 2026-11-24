"""Check the minimal YAML settings seam without serial hardware or a broker."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from bms_flux.cli import main
from bms_flux.settings import load_settings


def test_yaml_settings_from_named_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text(
        "bus: /dev/serial/by-id/example\n"
        "packs:\n  - address: 1\n    serial: SP112B2410100228\n"
        "mqtt:\n  host: localhost\n  password: test-only-password\n"
    )
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    settings = load_settings()
    assert settings.bus == "/dev/serial/by-id/example"
    assert settings.packs[0].serial == "SP112B2410100228"
    assert settings.mqtt.port == 1883
    assert "test-only-password" not in repr(settings)


def test_cli_configuration_error_does_not_expose_mqtt_password(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text("mqtt: {host: localhost, password: test-only-password}\n")
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    monkeypatch.setattr("sys.argv", ["bms-flux", "run"])
    with pytest.raises(SystemExit) as result:
        main()
    assert result.value.code == 2
    output = capsys.readouterr()
    assert "configuration" in output.err
    assert "test-only-password" not in output.err + output.out


@pytest.mark.parametrize(
    "packs",
    [
        "[{address: 1, serial: first}, {address: 1, serial: second}]",
        "[{address: 1, serial: same}, {address: 2, serial: same}]",
    ],
)
def test_ambiguous_pack_mapping_is_rejected(
    packs: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text(f"bus: /dev/example\npacks: {packs}\nmqtt: {{host: localhost}}\n")
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    with pytest.raises(ValidationError, match="unique"):
        load_settings()
