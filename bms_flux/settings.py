"""Load and validate owner settings from add-on options, YAML and environment."""

import json
import os
from contextvars import ContextVar
from pathlib import Path
from typing import Annotated, Any, Literal, Self
from urllib.parse import urlsplit

import yaml
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    SecretStr,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic.fields import FieldInfo
from pydantic_core import InitErrorDetails
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

DAEMON_ONLY_KEYS = frozenset({"mqtt", "state_dir", "api_port"})
DISCOVERY_PREFIX = "homeassistant"
RECONNECT_BACKOFF_MIN_S = 1
RECONNECT_BACKOFF_MAX_S = 30
IDLE_REOPEN_S = 30
BUS_HEALTH_PROBLEM_RATIO = 0.05
BUS_HEALTH_CLEAR_RATIO = 0.01
BUS_HEALTH_WINDOW_S = 60

_options_path: ContextVar[Path] = ContextVar("options_path", default=Path("/data/options.json"))
_source_errors: ContextVar[list[InitErrorDetails] | None] = ContextVar(
    "source_errors", default=None
)


def _integer(value: Any) -> Any:
    if isinstance(value, (bool, float)):
        raise ValueError("must be an integer")
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            raise ValueError("must be an integer") from None
    return value


Duration = Annotated[int, BeforeValidator(_integer)]


def _problem(location: tuple[str | int, ...], message: str) -> InitErrorDetails:
    return {
        "type": "value_error",
        "loc": location,
        "input": None,
        "ctx": {"error": ValueError(message)},
    }


def _validation_details(error: ValidationError) -> list[InitErrorDetails]:
    return [
        InitErrorDetails(
            type=detail["type"],
            loc=detail["loc"],
            input=detail.get("input"),
            ctx=detail.get("ctx", {}),
        )
        for detail in error.errors(include_url=False)
    ]


def _source_problem(location: str, message: str) -> None:
    problem = _problem((location,), message)
    errors = _source_errors.get()
    if errors is None:
        raise ValidationError.from_exception_data("Settings", [problem])
    errors.append(problem)


class Pack(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: int = Field(ge=1, le=16)
    serial: str = Field(pattern=r"^[A-Z0-9]+$")
    label: str | None = None


class MQTTSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str
    port: int = Field(default=1883, ge=1, le=65535)
    username: str | None = None
    password: SecretStr | None = None


class DriftDivergence(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    enabled: bool = True
    spread_on_pts: float = 5
    spread_off_pts: float = 3
    hold_min: Duration = 30


class DriftFullCharge(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    enabled: bool = True
    cell_avg_v: float = 3.45
    max_current_c: float = 0.05
    hold_min: Duration = 15
    soc_below_pct: float = 95


class DriftRestedOCV(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    enabled: bool = True
    max_current_c: float = 0.02
    rest_h: Duration = 2
    ocv_soc_below_pct: float = 20
    gap_pts: float = 10
    min_temp_c: float = 10


class OCVPoint(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    soc_pct: float
    cell_v: float


class _FileSource(PydanticBaseSettingsSource):
    def __init__(self, settings_cls: type[BaseSettings], *, json_file: bool) -> None:
        super().__init__(settings_cls)
        self._json_file = json_file

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        if self._json_file:
            path = _options_path.get()
            location = "options.json"
        else:
            name = os.environ.get("BMS_FLUX_CONFIG")
            if name is None:
                return {}
            path = Path(name)
            location = "BMS_FLUX_CONFIG"
        try:
            text = path.read_text()
            data = json.loads(text) if self._json_file else yaml.safe_load(text)
        except FileNotFoundError:
            if not self._json_file:
                _source_problem(location, "configuration file does not exist")
            return {}
        except (OSError, ValueError, yaml.YAMLError):
            _source_problem(location, "cannot read or parse configuration file")
            return {}
        if data is None:
            return {}
        if not isinstance(data, dict):
            _source_problem(location, "configuration source must be a mapping")
            return {}
        return data


class _EnvSource(EnvSettingsSource):
    def prepare_field_value(
        self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool
    ) -> Any:
        try:
            return super().prepare_field_value(field_name, field, value, value_is_complex)
        except ValueError:
            _source_problem(field_name, "invalid JSON in environment value")
            return None

    def __call__(self) -> dict[str, Any]:
        data = super().__call__()
        for key in self.env_vars:
            if key.startswith("bms_flux_"):
                name = key.removeprefix("bms_flux_").split("__", 1)[0]
                if name != "config" and name not in self.settings_cls.model_fields:
                    data[name] = None
        return data


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="forbid", env_prefix="BMS_FLUX_", env_nested_delimiter="__"
    )

    bus: str
    packs: list[Pack] = Field(default_factory=list)
    snapshot_interval_s: Duration = Field(default=10, ge=1, le=300)
    silent_after_s: Duration = Field(default=10, ge=2, le=120)
    log_level: Literal["debug", "info", "warning", "error"] = "info"
    stream: bool = True
    raw_frames: bool = False
    api_token: SecretStr = SecretStr("")
    drift_divergence: DriftDivergence = Field(default_factory=DriftDivergence)
    drift_full_charge: DriftFullCharge = Field(default_factory=DriftFullCharge)
    drift_rested_ocv: DriftRestedOCV = Field(default_factory=DriftRestedOCV)
    ocv_table: list[OCVPoint] = Field(default_factory=list)
    mqtt: MQTTSettings
    state_dir: Path = Path("./state")
    api_port: int = Field(default=8765, ge=1, le=65535)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            _EnvSource(settings_cls),
            _FileSource(settings_cls, json_file=False),
            _FileSource(settings_cls, json_file=True),
        )

    @field_validator("bus")
    @classmethod
    def valid_bus(cls, bus: str) -> str:
        if bus.startswith("/dev/") and len(Path(bus).parts) >= 3 and ".." not in Path(bus).parts:
            return bus
        try:
            url = urlsplit(bus)
            if (
                url.scheme == "tcp"
                and url.hostname
                and url.port
                and not url.username
                and not url.password
                and not url.path
                and not url.query
                and not url.fragment
            ):
                return bus
        except ValueError:
            pass
        raise ValueError("bus must be an absolute /dev/... path or tcp://host:port")

    @field_validator("api_token")
    @classmethod
    def valid_token(cls, token: SecretStr) -> SecretStr:
        if 0 < len(token.get_secret_value()) < 16:
            raise ValueError("api_token must be empty or at least 16 characters")
        return token

    @model_validator(mode="wrap")
    @classmethod
    def all_problems(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        errors: list[InitErrorDetails] = []
        result: Self | None = None
        try:
            result = handler(data)
        except ValidationError as error:
            errors.extend(_validation_details(error))
        if isinstance(data, dict):
            packs = data.get("packs", [])
            if isinstance(packs, list):
                normalized: list[Any] = []
                for pack in packs:
                    try:
                        normalized.append(Pack.model_validate(pack))
                    except ValidationError:
                        normalized.append(pack)
                for key in ("address", "serial", "label"):
                    values = [
                        pack.get(key) if isinstance(pack, dict) else getattr(pack, key, None)
                        for pack in normalized
                    ]
                    given = [value for value in values if value is not None]
                    if any(value in given[:index] for index, value in enumerate(given)):
                        errors.append(_problem(("packs", key), f"{key} values must be unique"))
            divergence = data.get("drift_divergence", {})
            if isinstance(divergence, dict):
                try:
                    on = TypeAdapter(float).validate_python(divergence.get("spread_on_pts", 5))
                    off = TypeAdapter(float).validate_python(divergence.get("spread_off_pts", 3))
                except ValidationError:
                    pass  # Field validation already reports invalid thresholds.
                else:
                    if off >= on:
                        errors.append(
                            _problem(
                                ("drift_divergence", "spread_off_pts"),
                                "spread_off_pts must be less than spread_on_pts",
                            )
                        )
            table = data.get("ocv_table", [])
            if isinstance(table, list) and table:
                if len(table) < 5:
                    errors.append(_problem(("ocv_table",), "ocv_table needs at least 5 points"))
                for index in range(1, len(table)):
                    try:
                        previous = OCVPoint.model_validate(table[index - 1])
                        current = OCVPoint.model_validate(table[index])
                    except ValidationError:
                        continue  # Field validation reports malformed points.
                    for key in ("soc_pct", "cell_v"):
                        if getattr(current, key) <= getattr(previous, key):
                            errors.append(
                                _problem(
                                    ("ocv_table", index, key), f"{key} must be strictly rising"
                                )
                            )
        if errors:
            raise ValidationError.from_exception_data(cls.__name__, errors)
        assert result is not None
        return result


def load_settings(
    *, options_path: Path = Path("/data/options.json"), **init_kwargs: Any
) -> Settings:
    token = _options_path.set(options_path)
    errors: list[InitErrorDetails] = []
    error_token = _source_errors.set(errors)
    try:
        settings: Settings | None = None
        try:
            settings = Settings(**init_kwargs)
        except ValidationError as error:
            errors.extend(_validation_details(error))
        if errors:
            raise ValidationError.from_exception_data("Settings", errors)
        assert settings is not None
        return settings
    finally:
        _source_errors.reset(error_token)
        _options_path.reset(token)
