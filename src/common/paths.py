from __future__ import annotations

from src.common.config import AppConfig


def resolve_output_path(config: AppConfig, key: str) -> str:
    base_uri = config.paths["base_uri"].rstrip("/")
    suffix = config.paths[key].rstrip("/")
    return f"{base_uri}/{suffix}"


def resolve_input_path(config: AppConfig, key: str) -> str:
    return str(config.paths[key])


def resolve_dt_path(base_path: str, dt: str) -> str:
    return f"{base_path}/dt={dt}"