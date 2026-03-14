from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .errors import ConfigError


def _parse_yaml_like_text(raw_text: str) -> dict[str, Any]:
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(raw_text)
        if not isinstance(data, dict):
            raise ConfigError("Config content must be an object.")
        return data
    except ModuleNotFoundError:
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise ConfigError(
                "PyYAML is not installed. Please provide JSON-compatible YAML."
            ) from exc
        if not isinstance(data, dict):
            raise ConfigError("Config content must be an object.")
        return data


def _load_dotenv(project_root: Path) -> None:
    env_path = project_root / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#") or "=" not in cleaned:
            continue
        key, value = cleaned.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def load_config(project_root: Path) -> dict[str, Any]:
    _load_dotenv(project_root)
    config_path = project_root / "config" / "config.yaml"
    if not config_path.exists():
        raise ConfigError(f"Missing config file: {config_path}")
    config = _parse_yaml_like_text(config_path.read_text(encoding="utf-8"))
    config["project_root"] = str(project_root)

    ai_cfg = config.setdefault("ai", {})
    ai_cfg.setdefault("api_key", os.getenv("OPENAI_API_KEY", ""))
    ai_cfg.setdefault("base_url", os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"))
    ai_cfg["model"] = os.getenv("OPENAI_MODEL", ai_cfg.get("model", "gpt-4.1-mini"))
    ai_cfg["base_url"] = os.getenv("OPENAI_BASE_URL", ai_cfg.get("base_url"))

    return config
