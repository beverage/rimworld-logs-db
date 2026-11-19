"""The command line: one subcommand per step, plus `run` for all of them.

Exit codes: 0 ran; 1 a configuration, IO or upload error; 2 stopped early
because the raw host kept refusing requests.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
from pathlib import Path

from . import bucket, download, listing, packing, pipeline, removal, supabase
from .common import ConfigError, OutOfDate, UploadError, say
from .settings import DATA_VARIABLE, DEFAULT_DATA, RELEASE_1_6


def iso_date(text: str) -> str:
    """An argparse type for a YYYY-MM-DD date."""
    try:
        dt.date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not a date in YYYY-MM-DD form: {text}") from exc
    return text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rimworld-logs-db", description="Builds the RimWorld Logs DB archive.")
    parser.add_argument(
        "--data",
        type=Path,
        default=Path(os.environ.get(DATA_VARIABLE) or DEFAULT_DATA),
        help="data folder, kept outside every repo",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    index = commands.add_parser("index", help="list uploads into the local index")
    index.add_argument(
        "--since", type=iso_date, default=RELEASE_1_6, help="oldest upload date (default: the 1.6 release)"
    )
    index.add_argument("--new", action="store_true", help="stop at the first page with nothing new")
    index.add_argument("--start-page", type=int, default=1, help="list page to start from, to resume a walk")
    index.add_argument("--max-pages", type=int, default=0, help="read at most this many pages")
    index.add_argument(
        "--workers", type=int, default=1, help="list pages read at once; see listing.py before raising it"
    )
    index.add_argument("--no-supabase", action="store_true", help="list without first fetching the removed list")
    index.set_defaults(
        run=lambda a: listing.index(a.data, a.since, a.new, a.start_page, a.max_pages, a.workers, a.no_supabase)
    )

    fetch = commands.add_parser("fetch", help="download pending logs into staging")
    fetch.add_argument("--limit", type=int, default=0, help="download at most this many logs")
    fetch.add_argument("--week", help="only this ISO week, e.g. 2026-W46")
    fetch.add_argument("--workers", type=int, default=4, help="downloads in flight at once")
    fetch.add_argument("--rps", type=float, default=6.0, help="requests a second across all workers")
    fetch.add_argument("--max-mb", type=int, default=64, help="skip logs larger than this")
    fetch.add_argument("--retry-errors", action="store_true", help="queue failed downloads again first")
    fetch.set_defaults(
        run=lambda a: download.fetch(a.data, a.week, a.limit, a.workers, a.rps, a.max_mb, a.retry_errors)
    )

    pack = commands.add_parser("pack", help="pack finished weeks into Parquet")
    pack.add_argument("--week", help="only this ISO week")
    pack.add_argument("--include-open", action="store_true", help="also pack the week still in progress")
    pack.add_argument("--partial", action="store_true", help="pack a week that still has pending logs")
    pack.add_argument("--keep-staging", action="store_true", help="leave the staged gzip files in place")
    pack.add_argument("--level", type=int, default=9, help="zstd level")
    pack.set_defaults(run=lambda a: packing.pack(a.data, a.week, a.include_open, a.partial, a.keep_staging, a.level))

    push = commands.add_parser("push", help="upload packed parts to the bucket")
    push.add_argument("--keep-local", action="store_true", help="leave the uploaded files on disk")
    push.add_argument("--state", action="store_true", help="also copy the index database to the bucket")
    push.set_defaults(run=lambda a: bucket.push(a.data, a.keep_local, a.state))

    sync = commands.add_parser("sync", help="upsert packed logs into the Supabase logs table")
    sync.set_defaults(run=lambda a: supabase.sync(a.data))

    run = commands.add_parser("run", help="fetch, pack, push and sync week by week, newest first")
    run.add_argument("--weeks", type=int, default=0, help="stop after this many weeks")
    run.add_argument("--since", type=iso_date, help="leave weeks older than the one containing this date")
    run.add_argument("--workers", type=int, default=4, help="downloads in flight at once")
    run.add_argument("--rps", type=float, default=6.0, help="requests a second across all workers")
    run.add_argument("--max-mb", type=int, default=64, help="skip logs larger than this")
    run.add_argument("--level", type=int, default=9, help="zstd level")
    run.add_argument("--no-supabase", action="store_true", help="skip Supabase; `sync` can catch up later")
    run.add_argument("--backup-every", type=int, default=8, help="copy the index to the bucket every N weeks")
    run.set_defaults(
        run=lambda a: pipeline.run(
            a.data, a.weeks, a.workers, a.rps, a.max_mb, a.level, a.no_supabase, a.backup_every, a.since
        )
    )

    remove = commands.add_parser("remove", help="take logs out of the archive, by gist link or id")
    remove.add_argument("gists", nargs="+", metavar="LINK", help="a gist link, a raw log link, or a gist id")
    remove.add_argument("--dry-run", action="store_true", help="report what would be dropped and change nothing")
    remove.add_argument("--same-content", action="store_true", help="also remove other uploads of the same log")
    remove.add_argument("--no-supabase", action="store_true", help="leave Supabase alone")
    remove.add_argument("--level", type=int, default=9, help="zstd level for the rewritten files")
    remove.set_defaults(
        run=lambda a: removal.remove(a.data, a.gists, a.dry_run, a.same_content, a.no_supabase, a.level)
    )

    status = commands.add_parser("status", help="counts by state and disk use")
    status.set_defaults(run=lambda a: pipeline.status(a.data))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.data = args.data.expanduser()
    try:
        return args.run(args)
    except (OSError, sqlite3.Error, ConfigError, UploadError, OutOfDate) as exc:
        say(f"error: {exc}")
        return 1
    except Exception as exc:
        # boto3 raises its own exception types; report them like any other upload failure.
        if type(exc).__module__.split(".")[0] in ("botocore", "boto3", "s3transfer"):
            say(f"R2 error: {exc}")
            return 1
        raise
