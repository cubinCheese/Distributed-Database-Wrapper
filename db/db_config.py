"""Database configuration helpers.

Loads config from a YAML file (config.yaml) if present. Provides helper to
retrieve connection strings. This file intentionally keeps dependencies small
(uses PyYAML if available).
"""
from typing import Dict, Any
import os

try:
    import yaml
except Exception:  # pragma: no cover - yaml optional in dev
    yaml = None

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")


def load_config() -> Dict[str, Any]:
    if yaml and os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def get_db_uri(default: str = "sqlite:///:memory:") -> str:
    cfg = load_config()
    return cfg.get("db_uri", default)
