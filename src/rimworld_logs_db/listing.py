"""index: list the account's gists into the local index.

GitHub's REST list endpoint returns 100 gists a page, newest first, with each
log's size and download address. Walking it costs one API request per 100
logs, and it reaches back to the account's first upload in 2016.

Two things shape the walk. About one request in seven returns a 502 and
passes on retry, so every request is retried. And a request for this account
takes about 10 seconds, while GitHub's secondary rate limit allows about 90
seconds of request time a minute, so pages are read one at a time. Two years
is about 2,450 pages, some six hours. A week run can start meanwhile, because
the walk goes newest first and stays ahead of it.

Before listing anything the removed list is copied down from Supabase, so a
log taken out on request is refused even by an index built from nothing.
"""

from __future__ import annotations

import concurrent.futures as cf
import http.client
import json
import os
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from . import supabase
from .common import ConfigError, GiveUp, UploadError, say, week_of
from .index_db import open_db
from .settings import ACCOUNT, API, LOG_FILENAME, USER_AGENT, load_env

ATTEMPTS = 8
LOWEST_BUDGET_PER_WORKER = 4


def github_token() -> str | None:
    """The token for API calls: the environment first, then the gh CLI."""
    for name in ("GITHUB_TOKEN", "GH_TOKEN"):
        if os.environ.get(name):
            return os.environ[name]
    try:
        out = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=15)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return out.stdout.strip() or None


def limit_wait(headers, fallback: float) -> float:
    """Seconds to wait after a 403 or 429, in the order GitHub documents."""
    retry_after = headers.get("retry-after")
    if retry_after and retry_after.isdigit():
        return int(retry_after) + 1
    if headers.get("x-ratelimit-remaining") == "0":
        reset = int(headers.get("x-ratelimit-reset", "0"))
        return max(1.0, reset - time.time() + 2)
    return max(60.0, fallback)


def api_get(path: str, token: str | None):
    """GET an API path as JSON, retrying server errors and waiting out limits.

    Returns the decoded body and the response headers.
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    delay, last = 2.0, "no attempt made"
    for _ in range(ATTEMPTS):
        request = urllib.request.Request(API + path, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read()), response.headers
        except urllib.error.HTTPError as exc:
            last = f"HTTP {exc.code}"
            if exc.code in (403, 429):
                wait = limit_wait(exc.headers, delay)
                say(f"  API refused {path} ({last}), waiting {wait:.0f} s")
                time.sleep(wait)
            elif 500 <= exc.code < 600:
                time.sleep(delay)
            else:
                raise GiveUp(f"{path}: {last}") from exc
        except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as exc:
            last = repr(exc)
            time.sleep(delay)
        delay = min(delay * 2, 900.0)
    raise GiveUp(f"{path}: still failing after {ATTEMPTS} attempts, last {last}")


def log_file(gist: dict) -> dict | None:
    """The log inside a gist: output_log.txt, else its largest file."""
    files = gist.get("files") or {}
    if LOG_FILENAME in files:
        return files[LOG_FILENAME]
    return max(files.values(), key=lambda f: f.get("size") or 0, default=None)


def uploader_of(description: str | None) -> str:
    """Which tool uploaded a gist, from the description it wrote."""
    text = description or ""
    if "Standalone Log Publisher" in text:
        return "standalone"
    if "HugsLib" in text:
        return "hugslib"
    return "other"


def add_gist(con: sqlite3.Connection, gist: dict, entry: dict) -> int:
    """Index one gist unless it is already known or on the removed list. Returns 1 if added."""
    return con.execute(
        "INSERT OR IGNORE INTO gists (id, created_at, week, size, raw_url, uploader)"
        " SELECT ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM removed WHERE id = ?)",
        (
            gist["id"],
            gist["created_at"],
            week_of(gist["created_at"]),
            entry["size"],
            entry["raw_url"],
            uploader_of(gist.get("description")),
            gist["id"],
        ),
    ).rowcount


def add_page(con: sqlite3.Connection, gists: list[dict], since: str) -> tuple[int, int]:
    """Index one list page, skipping uploads older than the cut-off. Returns how many were new and how many known."""
    new = known = 0
    for gist in gists:
        entry = log_file(gist)
        if gist["created_at"] < since or entry is None:
            continue
        added = add_gist(con, gist, entry)
        new += added
        known += 1 - added
    return new, known


@dataclass
class Walk:
    """Where a listing has got to."""

    pages: int = 0
    last_page: int = 0
    new: int = 0
    known: int = 0
    noted: int = 0

    def __str__(self) -> str:
        return f"pages={self.pages} last_page={self.last_page} new={self.new} known={self.known}"


def copy_removed_list(con: sqlite3.Connection, data: Path) -> bool:
    """Bring the removed list down before listing. False if it could not be read."""
    try:
        learned, sent = supabase.sync_removed(con, load_env(), data)
    except (ConfigError, UploadError) as exc:
        say(f"cannot read the removed list, so nothing was listed: {exc}")
        say("  --no-supabase lists without it, at the risk of indexing a removed log")
        return False
    if learned or sent:
        say(f"removed list: {learned} learned, {sent} sent")
    return True


def index(
    data: Path,
    since: str,
    new_only: bool = False,
    start_page: int = 1,
    max_pages: int = 0,
    workers: int = 1,
    no_supabase: bool = False,
) -> int:
    """Walk the list newest first until it passes `since`, a date as YYYY-MM-DD.

    `new_only` stops at the first page that adds nothing, for a daily top-up.
    `start_page` resumes a walk that was interrupted.
    """
    con = open_db(data)
    token = github_token()
    if not token:
        say("no GitHub token found: unauthenticated calls are limited to 60 an hour")
    if not no_supabase and not copy_removed_list(con, data):
        return 1

    cutoff = f"{since}T00:00:00Z"
    walk, page, finished = Walk(), start_page, False
    try:
        with cf.ThreadPoolExecutor(max_workers=workers) as pool:
            while not finished:
                batch = [p for p in range(page, page + workers) if not max_pages or p < start_page + max_pages]
                if not batch:
                    break
                paths = [f"/users/{ACCOUNT}/gists?per_page=100&page={p}" for p in batch]
                results = list(pool.map(lambda path: api_get(path, token), paths))
                for number, (gists, _) in zip(batch, results, strict=True):
                    new, known = add_page(con, gists, cutoff)
                    walk.pages, walk.last_page = walk.pages + 1, number
                    walk.new, walk.known = walk.new + new, walk.known + known
                    passed_cutoff = not gists or gists[-1]["created_at"] < cutoff
                    if passed_cutoff or (new_only and new == 0):
                        finished = True
                        break
                con.commit()
                page += len(batch)
                if walk.pages // 100 > walk.noted:
                    walk.noted = walk.pages // 100
                    say(f"  page {walk.last_page}: {walk.new} new, {walk.known} already known")
                if not finished:
                    wait_for_budget([headers for _, headers in results], workers)
    except GiveUp as exc:
        con.commit()
        say(f"stopped: {exc}")
        print(f"RESULT index failed {walk}")
        return 1
    except KeyboardInterrupt:
        con.commit()
        print(f"RESULT index interrupted {walk}")
        return 1
    total = con.execute("SELECT COUNT(*) FROM gists").fetchone()[0]
    print(f"RESULT index ok {walk} total={total}")
    return 0


def wait_for_budget(headers: list, workers: int) -> None:
    """Sleep until the hourly API allowance resets, if these responses show it nearly spent."""
    remaining = min(int(h.get("x-ratelimit-remaining", "5000")) for h in headers)
    if remaining >= LOWEST_BUDGET_PER_WORKER * workers:
        return
    reset = max(int(h.get("x-ratelimit-reset", "0")) for h in headers)
    wait = max(1.0, reset - time.time() + 2)
    say(f"  API budget nearly spent, waiting {wait:.0f} s for the reset")
    time.sleep(wait)
