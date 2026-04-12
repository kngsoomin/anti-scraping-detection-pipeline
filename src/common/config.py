from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value

    return result


@dataclass
class AppConfig:
    raw: dict[str, Any]

    @property
    def project_name(self) -> str:
        return self.raw["project"]["name"]

    @property
    def runtime(self) -> dict[str, Any]:
        return self.raw["runtime"]

    @property
    def spark(self) -> dict[str, Any]:
        return self.raw["spark"]

    @property
    def paths(self) -> dict[str, Any]:
        return self.raw["paths"]

    @property
    def sessionization(self) -> dict[str, Any]:
        return self.raw["sessionization"]

    @property
    def detection(self) -> dict[str, Any]:
        return self.raw["detection"]


def load_config(env_name: str) -> AppConfig:
    config_dir = Path("config")
    base_path = config_dir / "base.yml"
    env_path = config_dir / f"{env_name}.yml"

    if not base_path.exists():
        raise FileNotFoundError(f"Missing base config: {base_path}")
    if not env_path.exists():
        raise FileNotFoundError(f"Missing env config: {env_path}")

    with base_path.open("r", encoding="utf-8") as f:
        base_cfg = yaml.safe_load(f) or {}

    with env_path.open("r", encoding="utf-8") as f:
        env_cfg = yaml.safe_load(f) or {}

    merged = _deep_merge(base_cfg, env_cfg)
    return AppConfig(raw=merged)























