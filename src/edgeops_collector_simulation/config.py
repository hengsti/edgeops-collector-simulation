import os
from functools import lru_cache
from pathlib import Path

from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="SIMULATION_COLLECTOR_",
        extra="ignore",
        hide_input_in_errors=True,
    )

    api_key: SecretStr
    admin_api_key: SecretStr
    source_host: str = "local-simulation"
    seed: int = 42
    profile_path: Path = Path("config/simulation-profile.json")
    scenario_path: Path = Path("config/scenarios")
    device_id: str = "esp32-simulated-01"
    allowed_services: str = (
        "nanomq,ingestion-service,influxdb,telegraf,grafana,device-management,"
        "control-ui,homekit-api"
    )

    @model_validator(mode="after")
    def validate_distinct_keys(self) -> "Settings":
        if self.api_key.get_secret_value() == self.admin_api_key.get_secret_value():
            raise ValueError("simulation read and admin API keys must be different")
        return self

    @property
    def allowed_service_names(self) -> frozenset[str]:
        return frozenset(item.strip() for item in self.allowed_services.split(",") if item.strip())


def _dotenv_keys(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    result: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip().removeprefix("export ").strip()
            result.add(key)
    return result


def validate_environment(env_file: Path = Path(".env")) -> None:
    supported = {f"SIMULATION_COLLECTOR_{name.upper()}" for name in Settings.model_fields}
    names = set(os.environ) | _dotenv_keys(env_file)
    unsupported = sorted(
        name
        for name in names
        if (name.startswith("SIMULATION_COLLECTOR_") and name not in supported)
        or name == "COLLECTOR_MODE"
    )
    if unsupported:
        names = ", ".join(unsupported)
        raise ValueError(f"Unsupported simulation collector environment variable(s): {names}")


@lru_cache
def get_settings() -> Settings:
    validate_environment()
    return Settings()
