"""Messages, timestamps and the errors that end a command."""

from __future__ import annotations

import datetime as dt
import sys

TIMESTAMP = "%Y-%m-%dT%H:%M:%SZ"


class ConfigError(Exception):
    """A missing or unusable setting, reported without its value."""


class UploadError(Exception):
    """The bucket or Supabase did not take what was sent."""


class GiveUp(Exception):
    """A request that kept failing after every retry."""


def say(message: str) -> None:
    """Progress goes to stderr, so stdout carries only a command's results."""
    print(message, file=sys.stderr, flush=True)


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime(TIMESTAMP)


def parse_time(iso: str) -> dt.datetime:
    """A GitHub timestamp as an aware UTC datetime."""
    return dt.datetime.strptime(iso, TIMESTAMP).replace(tzinfo=dt.timezone.utc)


def week_of(iso: str) -> str:
    """ISO week of a GitHub timestamp, e.g. 2026-W47."""
    year, week, _ = parse_time(iso).isocalendar()
    return f"{year}-W{week:02d}"
