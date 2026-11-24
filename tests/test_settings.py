"""Check settings sources and validation without serial hardware or a broker."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from bms_flux.cli import main
from bms_flux.settings import DAEMON_ONLY_KEYS, load_settings


def test_source_precedence_and_nested_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options = tmp_path / "options.json"
    options.write_text(
        json.dumps(
            {
                "bus": "/dev/options",
                "snapshot_interval_s": 20,
                "mqtt": {"host": "options", "username": "owner", "password": "options-secret"},
            }
        )
    )
    config = tmp_path / "settings.yaml"
    config.write_text("bus: /dev/yaml\nmqtt: {host: yaml}\nsilent_after_s: 15\n")
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    monkeypatch.setenv("BMS_FLUX_BUS", "tcp://localhost:1234")
    monkeypatch.setenv("BMS_FLUX_MQTT__PASSWORD", "env-secret")
    monkeypatch.setenv("BMS_FLUX_DRIFT_DIVERGENCE__HOLD_MIN", "40")
    settings = load_settings(options_path=options)
    assert settings.bus == "tcp://localhost:1234"
    assert settings.snapshot_interval_s == 20
    assert settings.silent_after_s == 15
    assert settings.mqtt.host == "yaml"
    assert settings.mqtt.username == "owner"
    assert settings.mqtt.password is not None
    assert settings.mqtt.password.get_secret_value() == "env-secret"
    assert settings.drift_divergence.hold_min == 40
    assert "env-secret" not in repr(settings)


def test_shared_defaults_and_daemon_allowlist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("BMS_FLUX_CONFIG", raising=False)
    monkeypatch.setenv("BMS_FLUX_BUS", "/dev/example")
    monkeypatch.setenv("BMS_FLUX_MQTT__HOST", "localhost")
    settings = load_settings(options_path=tmp_path / "absent.json")
    assert settings.packs == []
    assert settings.snapshot_interval_s == settings.silent_after_s == 10
    assert settings.log_level == "info"
    assert settings.stream and not settings.raw_frames
    assert settings.api_token.get_secret_value() == ""
    assert settings.ocv_table == []
    assert settings.state_dir == Path("./state")
    assert settings.api_port == 8765
    assert {"mqtt", "state_dir", "api_port"} == DAEMON_ONLY_KEYS
    assert settings.drift_divergence.model_dump() == {
        "enabled": True,
        "spread_on_pts": 5,
        "spread_off_pts": 3,
        "hold_min": 30,
    }
    assert settings.drift_full_charge.model_dump() == {
        "enabled": True,
        "cell_avg_v": 3.45,
        "max_current_c": 0.05,
        "hold_min": 15,
        "soc_below_pct": 95,
    }
    assert settings.drift_rested_ocv.model_dump() == {
        "enabled": True,
        "max_current_c": 0.02,
        "rest_h": 2,
        "ocv_soc_below_pct": 20,
        "gap_pts": 10,
        "min_temp_c": 10,
    }


@pytest.mark.parametrize(
    "fragment, location",
    [
        ("bus: relative/path", "bus"),
        ("bus: /tmp/port", "bus"),
        ("bus: tcp://localhost", "bus"),
        ("bus: tcp://localhost:0", "bus"),
        ("bus: tcp://localhost:70000", "bus"),
        ("bus: tcp://owner:secret@localhost:1234", "bus"),
        ("packs: [{address: 0, serial: SP1}]", "packs.0.address"),
        ("packs: [{address: 17, serial: SP1}]", "packs.0.address"),
        ("packs: [{address: 1, serial: lower}]", "packs.0.serial"),
        ("snapshot_interval_s: 0", "snapshot_interval_s"),
        ("snapshot_interval_s: 301", "snapshot_interval_s"),
        ("snapshot_interval_s: 10.0", "snapshot_interval_s"),
        ("snapshot_interval_s: '10.0'", "snapshot_interval_s"),
        ("silent_after_s: true", "silent_after_s"),
        ("silent_after_s: 1", "silent_after_s"),
        ("silent_after_s: 121", "silent_after_s"),
        ("log_level: trace", "log_level"),
        ("api_token: short", "api_token"),
        ("drift_divergence: {spread_off_pts: 5}", "drift_divergence.spread_off_pts"),
        ("drift_full_charge: {hold_min: 1.5}", "drift_full_charge.hold_min"),
        ("drift_rested_ocv: {rest_h: true}", "drift_rested_ocv.rest_h"),
        ("drift_rested_ocv: {ocv_soc_above_pct: 90}", "drift_rested_ocv.ocv_soc_above_pct"),
        ("ocv_table: [{soc_pct: 0, cell_v: 2.5}]", "ocv_table"),
        ("api_port: 65536", "api_port"),
        ("baud_rate: 9600", "baud_rate"),
        ("discovery_prefix: other", "discovery_prefix"),
        ("drift_divergence: {spread_on_pts: .nan}", "drift_divergence.spread_on_pts"),
        ("ocv_table: [{soc_pct: .inf, cell_v: 2.5}]", "ocv_table.0.soc_pct"),
    ],
)
def test_invalid_options_report_the_field(
    fragment: str, location: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text("bus: /dev/example\nmqtt: {host: localhost}\n" + fragment + "\n")
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    with pytest.raises(ValidationError) as result:
        load_settings(options_path=tmp_path / "absent.json")
    assert location in {".".join(map(str, error["loc"])) for error in result.value.errors()}


def test_all_independent_problems_are_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text(
        "bus: invalid\nsnapshot_interval_s: 0\napi_token: short\n"
        "packs:\n  - {address: 0, serial: lower, label: same}\n"
        "  - {address: 0, serial: lower, label: same}\n"
        "drift_divergence: {spread_off_pts: 6, hold_min: nope}\n"
        "mqtt: {port: 0, password: test-only-password}\n"
        "ocv_table: [{soc_pct: 0, cell_v: 2.5}]\nunknown: true\n"
    )
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    with pytest.raises(ValidationError) as result:
        load_settings(options_path=tmp_path / "absent.json")
    errors = result.value.errors()
    locations = {".".join(map(str, error["loc"])) for error in errors}
    assert {
        "bus",
        "snapshot_interval_s",
        "api_token",
        "packs.0.address",
        "packs.0.serial",
        "packs.1.address",
        "packs.1.serial",
        "packs.address",
        "packs.serial",
        "packs.label",
        "drift_divergence.spread_off_pts",
        "drift_divergence.hold_min",
        "mqtt.host",
        "mqtt.port",
        "ocv_table",
        "unknown",
    } <= locations


@pytest.mark.parametrize("key", ["soc_pct", "cell_v"])
def test_ocv_table_must_be_strictly_rising(
    key: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = [{"soc_pct": n * 20, "cell_v": 2.5 + n * 0.1} for n in range(5)]
    table[3][key] = table[2][key]
    config = tmp_path / "settings.json"
    config.write_text(
        json.dumps({"bus": "/dev/example", "mqtt": {"host": "localhost"}, "ocv_table": table})
    )
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    with pytest.raises(ValidationError, match="strictly rising"):
        load_settings(options_path=tmp_path / "absent.json")


def test_complete_valid_configuration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text(
        "bus: tcp://[::1]:1234\nmqtt: {host: localhost}\n"
        "packs: [{address: 1, serial: SP1, label: First}, {address: 16, serial: SP2}]\n"
        "snapshot_interval_s: 300\nsilent_after_s: 120\nlog_level: debug\n"
        "stream: false\nraw_frames: true\napi_token: test-only-token-1234\n"
        "drift_divergence: {enabled: false, spread_on_pts: 7, spread_off_pts: 4}\n"
        "drift_full_charge: {enabled: false, cell_avg_v: 3.5}\n"
        "drift_rested_ocv: {enabled: false, rest_h: 3}\n"
        "ocv_table: [{soc_pct: 0, cell_v: 2.5}, {soc_pct: 10, cell_v: 2.8}, "
        "{soc_pct: 20, cell_v: 3.0}, {soc_pct: 50, cell_v: 3.2}, {soc_pct: 100, cell_v: 3.5}]\n"
        "state_dir: /tmp/state\napi_port: 8766\n"
    )
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    settings = load_settings(options_path=tmp_path / "absent.json")
    assert settings.packs[0].label == "First"
    assert settings.packs[1].label is None
    assert settings.snapshot_interval_s == 300
    assert not settings.stream and settings.raw_frames
    assert len(settings.ocv_table) == 5
    assert settings.api_port == 8766
    assert "test-only-token-1234" not in repr(settings)


@pytest.mark.parametrize("source", ["malformed", "nonmapping", "missing"])
def test_source_errors_are_collected_with_validation_errors(
    source: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options = tmp_path / "options.json"
    options.write_text("{bad-json-secret")
    config = tmp_path / "settings.yaml"
    if source == "malformed":
        config.write_text("mqtt: [yaml-secret")
    elif source == "nonmapping":
        config.write_text("- yaml-secret\n")
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    monkeypatch.setenv("BMS_FLUX_SNAPSHOT_INTERVAL_S", "0")
    monkeypatch.setenv("BMS_FLUX_PACKS", "not-json-secret")
    monkeypatch.setenv("BMS_FLUX_TYPO", "unknown-secret")
    with pytest.raises(ValidationError) as result:
        load_settings(options_path=options)
    locations = {".".join(map(str, error["loc"])) for error in result.value.errors()}
    assert {
        "options.json",
        "BMS_FLUX_CONFIG",
        "packs",
        "snapshot_interval_s",
        "typo",
        "bus",
    } <= locations


def test_cli_lists_all_problems_once_without_secret_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text(
        "bus: invalid\nsnapshot_interval_s: 0\napi_token: short-secret\n"
        "mqtt: {password: mqtt-secret}\n"
    )
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    monkeypatch.setattr("sys.argv", ["bms-flux", "run"])
    with pytest.raises(SystemExit) as result:
        main()
    assert result.value.code == 2
    output = capsys.readouterr().err
    for location in ("bus", "snapshot_interval_s", "api_token", "mqtt.host"):
        assert f"  {location}:" in output
    assert output.count("configuration") == 1
    assert "short-secret" not in output
    assert "mqtt-secret" not in output


@pytest.mark.parametrize("key", ["address", "serial", "label"])
def test_duplicate_pack_mapping_values(
    key: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packs: list[dict[str, object]] = [
        {"address": 1, "serial": "SP1", "label": "First"},
        {"address": 2, "serial": "SP2", "label": "Second"},
    ]
    packs[1][key] = packs[0][key]
    options = tmp_path / "options.json"
    options.write_text(
        json.dumps({"bus": "/dev/example", "mqtt": {"host": "localhost"}, "packs": packs})
    )
    monkeypatch.delenv("BMS_FLUX_CONFIG", raising=False)
    with pytest.raises(ValidationError, match="unique"):
        load_settings(options_path=options)


def test_bus_address_uniqueness_uses_validated_integers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text(
        "bus: /dev/example\nmqtt: {host: localhost}\n"
        "packs: [{address: 1, serial: SP1}, {address: '1', serial: SP2}]\n"
    )
    monkeypatch.setenv("BMS_FLUX_CONFIG", str(config))
    with pytest.raises(ValidationError, match="unique"):
        load_settings(options_path=tmp_path / "absent.json")


def test_options_json_without_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    options = tmp_path / "options.json"
    options.write_text('{"bus": "/dev/example", "mqtt": {"host": "localhost"}}')
    monkeypatch.delenv("BMS_FLUX_CONFIG", raising=False)
    assert load_settings(options_path=options).bus == "/dev/example"


def test_unknown_nested_environment_key_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("BMS_FLUX_CONFIG", raising=False)
    monkeypatch.setenv("BMS_FLUX_BUS", "/dev/example")
    monkeypatch.setenv("BMS_FLUX_MQTT__HOST", "localhost")
    monkeypatch.setenv("BMS_FLUX_MQTT__TLS", "true")
    with pytest.raises(ValidationError) as result:
        load_settings(options_path=tmp_path / "absent.json")
    assert ("mqtt", "tls") in {error["loc"] for error in result.value.errors()}


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
