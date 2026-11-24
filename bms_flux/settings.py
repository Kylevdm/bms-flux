"""Load the tracer bullet's bus, pack mapping and MQTT settings from YAML."""

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Pack(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: int = Field(ge=0, le=16)
    serial: str = Field(pattern=r"^[A-Za-z0-9_-]+$")


class MQTTSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str
    port: int = Field(default=1883, ge=1, le=65535)
    username: str | None = None
    password: SecretStr | None = None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="forbid", env_prefix="BMS_FLUX_")

    bus: str = Field(min_length=1)
    packs: list[Pack]
    mqtt: MQTTSettings

    @field_validator("packs")
    @classmethod
    def unique_mapping(cls, packs: list[Pack]) -> list[Pack]:
        if len({pack.address for pack in packs}) != len(packs) or len(
            {pack.serial for pack in packs}
        ) != len(packs):
            raise ValueError("bus addresses and pack serials must be unique")
        return packs


def load_settings() -> Settings:
    path = Path(os.environ["BMS_FLUX_CONFIG"])
    return Settings.model_validate(yaml.safe_load(path.read_text()))
