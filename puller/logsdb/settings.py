"""Names, defaults and the env file that holds the keys.

Keys live in a file outside every repo and are never printed. A missing key is
reported by name only.
"""

from __future__ import annotations

import os
from pathlib import Path

from .common import ConfigError

ACCOUNT = "HugsLibRecordKeeper"
API = "https://api.github.com"
RAW_HOST = "gist.githubusercontent.com"
USER_AGENT = "rimworld-logs-db/0.1"
LOG_FILENAME = "output_log.txt"
RELEASE_1_6 = "2025-07-11"

DEFAULT_DATA = Path.home() / ".local" / "share" / "rimworld-logs-db"
DEFAULT_ENV = Path.home() / ".config" / "rimworld-logs-db" / "env"
DATA_VARIABLE = "RIMWORLD_LOGS_DB_DATA"
ENV_VARIABLE = "RIMWORLD_LOGS_DB_ENV"

R2_KEYS = ("R2_ENDPOINT", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
SUPABASE_KEYS = ("SUPABASE_URL", "SUPABASE_SECRET_KEY")


def load_env() -> dict[str, str]:
    """Settings from the env file: KEY=value lines, comments and blanks skipped."""
    path = Path(os.environ.get(ENV_VARIABLE) or DEFAULT_ENV)
    try:
        lines = path.read_text().splitlines()
    except OSError as exc:
        raise ConfigError(f"cannot read {path}: {exc.strerror}") from exc
    values = {}
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def require(env: dict[str, str], keys) -> None:
    """Stop with a ConfigError naming any of these keys that is unset."""
    missing = [key for key in keys if not env.get(key)]
    if missing:
        raise ConfigError("not set in the env file: " + ", ".join(missing))
