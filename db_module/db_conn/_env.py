"""Shared env-loading helpers for connector modules.

`load_dotenv` runs once on import so downstream code just calls `require()` or
`get()`. Treats empty strings as missing so a half-filled `.env` fails loudly
instead of silently connecting to nothing.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(REPO_ROOT / ".env", override=False)


class ConfigError(RuntimeError):
    """Raised when a required environment variable is missing or empty."""


def get(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def require(name: str) -> str:
    value = get(name)
    if value is None:
        raise ConfigError(
            f"Environment variable {name!r} is not set. "
            f"Copy .env.example to .env and fill it in."
        )
    return value


def resolve_path(raw: str) -> Path:
    """Turn a possibly-relative path into an absolute one anchored at repo root."""
    p = Path(raw)
    return p if p.is_absolute() else REPO_ROOT / p
