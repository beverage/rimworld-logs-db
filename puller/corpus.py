#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=17", "boto3>=1.36"]
# ///
"""hugslib-corpus - keep a private, queryable copy of the public HugsLib logs.

HugsLib and the standalone Log Publisher upload every shared log as a public
gist under one GitHub account, HugsLibRecordKeeper. This tool lists those
gists, downloads the logs, and packs them into weekly Parquet files that
DuckDB can query, and uploads them to a private R2 bucket. index, fetch, sync
and status are stdlib only; pack needs pyarrow and uploads need boto3, which
`uv run` installs from the header above.

WHY IT IS SHAPED THIS WAY

  index   The REST list endpoint returns 100 gists a page, newest first, with
          each log's size and raw URL. Walking it costs one API request per
          100 logs. About one list request in seven returns a 502 and passes
          on retry. A request takes about 10 seconds on this account, and
          GitHub allows about 90 seconds of request time a minute, so pages
          are read one at a time: two years is some six hours. A week run can
          start meanwhile, because the walk goes newest first and stays ahead.

  fetch   Bodies come from gist.githubusercontent.com. The API's hourly
          budget does not cover that host and GitHub publishes no limits for
          it; the API's own gist endpoint truncates files at 1 MB. Requests
          go out unauthenticated at a capped rate. A 403 or 429 pauses every
          worker, and five refusals in a row end the run, because pushing on
          while limited is what gets a client banned.

  disk    The raw host sends no compression and the full set is larger as
          text than this machine's free disk, so each log is gzipped as it
          arrives and never written raw.

  pack    A week with nothing left to fetch becomes one Parquet file, zstd
          compressed, one row per log holding the text and the facts read off
          the HugsLib header. That week's staged copies are then deleted.

  patches The same step writes the log's Harmony patch block as four tables
          under patches/: HugslibLogs, HarmonyPatches, HarmonyPatchNamespaces
          and ActivePatches (one row per log and patch). A log averages over
          4,000 patches, so ActivePatches is the big one; sorted and
          delta-encoded it takes well under a byte a row, which only works
          because patch ids are small consecutive numbers handed out as each
          patch is first seen. The index database keeps that dictionary.

  push    This machine has no room for the set, so it is a pass-through. A
          packed part goes to the bucket, its stored size is checked against
          the file, and only then is the local copy deleted. sync upserts one
          row per log into the Supabase logs table, which holds the index and
          the facts but never the text.

  run     Walks the weeks newest first: fetch one, pack it, push it, sync it.
          At most one week of logs is on disk at a time, and a run stopped at
          any point picks up where it left off.

  remove  Takes one log out on request. The week's files are rewritten
          without it, its Supabase row is deleted, and its id and content
          hash go on a removed list, so a later pull does not bring it back.
          The list's home is the Supabase table removed_logs, because an
          index can be rebuilt from nothing and must still honour it: index
          and run copy the list down before they list anything, and fetch
          discards a download whose hash is on it, which catches the same
          log uploaded again under a new id. The patch dictionary is left
          alone: it names mods' methods, not people, and its numbers must
          not move.

  Secrets come from an env file outside every repo, by default
  ~/.config/hugslib-corpus/env, and are never printed.

DATA RULES

  The copy stays outside every repo. These are anonymous uploads made to get
  help: publish counts and rates, never a log id or an excerpt tied to a
  finding. Log text is untrusted input, written by any mod or player.

USAGE

  corpus.py index                     list every upload since the 1.6 release
  corpus.py index --since 2026-01-01  list back to another date
  corpus.py index --new               stop at the first page with nothing new
  corpus.py fetch                     download everything pending, newest first
  corpus.py fetch --limit 300         download a slice
  corpus.py fetch --retry-errors      put failed downloads back in the queue
  uv run corpus.py pack               pack every finished week
  uv run corpus.py pack --include-open --partial   pack what is staged now
  uv run corpus.py run                fetch, pack, push and sync every week
  uv run corpus.py run --weeks 1      do one week and stop
  uv run corpus.py push               upload packed parts still on disk
  corpus.py sync                      upsert packed logs into Supabase
  uv run corpus.py remove LINK        take one log out, by gist link or id
  uv run corpus.py remove LINK --dry-run   show what that would drop
  corpus.py status                    counts by state and disk use

  --data DIR goes before the command, or set HUGSLIB_CORPUS_DIR. The default
  is ~/Code/Apps/rimworld-log-corpus.

EXIT CODES

  0  ran
  1  configuration or IO error
  2  stopped early: the raw host kept refusing requests
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import contextlib
import datetime as dt
import gzip
import hashlib
import http.client
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path

ACCOUNT = "HugsLibRecordKeeper"
API = "https://api.github.com"
RAW_HOST = "gist.githubusercontent.com"
USER_AGENT = "hugslib-corpus/0.1"
RELEASE_1_6 = "2025-07-11"
LOG_FILENAME = "output_log.txt"
DEFAULT_DATA = Path.home() / "Code" / "Apps" / "rimworld-log-corpus"
DEFAULT_ENV = Path.home() / ".config" / "hugslib-corpus" / "env"
R2_KEYS = ("R2_ENDPOINT", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
SUPABASE_KEYS = ("SUPABASE_URL", "SUPABASE_SECRET_KEY")

# pending -> fetched -> packed, or one of the terminal states too_large, gone, error.
SCHEMA = """
CREATE TABLE IF NOT EXISTS gists (
    id          TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    week        TEXT NOT NULL,
    size        INTEGER NOT NULL,
    raw_url     TEXT NOT NULL,
    uploader    TEXT NOT NULL,
    state       TEXT NOT NULL DEFAULT 'pending',
    http_status INTEGER,
    bytes       INTEGER,
    sha256      TEXT,
    attempts    INTEGER NOT NULL DEFAULT 0,
    fetched_at  TEXT,
    shard       TEXT,
    error       TEXT,
    log_no      INTEGER
);
CREATE INDEX IF NOT EXISTS gists_state_week ON gists (state, week);
CREATE TABLE IF NOT EXISTS facts (
    id           TEXT PRIMARY KEY,
    game_version TEXT,
    game_rev     INTEGER,
    mod_count    INTEGER NOT NULL,
    dlcs         TEXT NOT NULL,
    trimmed      INTEGER NOT NULL,
    line_count   INTEGER NOT NULL,
    utf8_ok      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS patch_ids (
    key INTEGER PRIMARY KEY,
    id  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS namespace_ids (
    name TEXT PRIMARY KEY,
    id   INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS parts (
    shard       TEXT PRIMARY KEY,
    logs        INTEGER NOT NULL,
    uploaded_at TEXT
);
CREATE TABLE IF NOT EXISTS removed (
    id         TEXT PRIMARY KEY,
    removed_at TEXT NOT NULL,
    sha256     TEXT
);
"""


class GiveUp(Exception):
    """A request that kept failing after every retry."""


def say(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def week_of(iso: str) -> str:
    """ISO week of a GitHub timestamp, e.g. 2026-W47."""
    year, week, _ = dt.datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").isocalendar()
    return f"{year}-W{week:02d}"


def open_db(data: Path) -> sqlite3.Connection:
    data.mkdir(parents=True, exist_ok=True)
    # The long wait lets an index run and a week run share the database.
    con = sqlite3.connect(data / "index.sqlite", timeout=600)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(SCHEMA)
    present = {column[1] for column in con.execute("PRAGMA table_info(gists)")}
    for name, kind in (("log_no", "INTEGER"), ("synced", "INTEGER NOT NULL DEFAULT 0")):
        if name not in present:
            con.execute(f"ALTER TABLE gists ADD COLUMN {name} {kind}")
    if "sha256" not in {column[1] for column in con.execute("PRAGMA table_info(removed)")}:
        con.execute("ALTER TABLE removed ADD COLUMN sha256 TEXT")
    return con


# --------------------------------------------------------------------- index


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
    """Seconds to wait after a 403 or 429, by GitHub's documented order."""
    retry_after = headers.get("retry-after")
    if retry_after and retry_after.isdigit():
        return int(retry_after) + 1
    if headers.get("x-ratelimit-remaining") == "0":
        reset = int(headers.get("x-ratelimit-reset", "0"))
        return max(1.0, reset - time.time() + 2)
    return max(60.0, fallback)


def api_get(path: str, token: str | None):
    """GET an API path as JSON, retrying server errors and waiting out limits."""
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    delay, last = 2.0, "no attempt made"
    for _ in range(8):
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
    raise GiveUp(f"{path}: still failing after 8 attempts, last {last}")


def log_file(gist: dict) -> dict | None:
    """The log inside a gist: output_log.txt, else its largest file."""
    files = gist.get("files") or {}
    if LOG_FILENAME in files:
        return files[LOG_FILENAME]
    return max(files.values(), key=lambda f: f.get("size") or 0, default=None)


def uploader_of(description: str | None) -> str:
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


def cmd_index(args) -> int:
    con = open_db(args.data)
    token = github_token()
    if not token:
        say("no GitHub token found: unauthenticated calls are limited to 60 an hour")
    since = f"{args.since}T00:00:00Z"
    dt.datetime.strptime(since, "%Y-%m-%dT%H:%M:%SZ")
    if not args.no_supabase:
        try:
            learned, sent = sync_removed(con, load_env(), args.data)
        except (ConfigError, UploadError) as exc:
            say(f"cannot read the removed list, so nothing was listed: {exc}")
            say("  --no-supabase lists without it, at the risk of indexing a removed log")
            return 1
        if learned or sent:
            say(f"removed list: {learned} learned, {sent} sent")
    page, last_page, pages, new, known, noted, finished = args.start_page, 0, 0, 0, 0, 0, False
    try:
        with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
            while not finished:
                batch = list(range(page, page + args.workers))
                if args.max_pages:
                    batch = [p for p in batch if p < args.start_page + args.max_pages]
                if not batch:
                    break
                paths = [f"/users/{ACCOUNT}/gists?per_page=100&page={p}" for p in batch]
                results = list(pool.map(lambda path: api_get(path, token), paths))
                remaining, reset = 5000, 0
                for number, (gists, headers) in zip(batch, results):
                    remaining = min(remaining, int(headers.get("x-ratelimit-remaining", "5000")))
                    reset = max(reset, int(headers.get("x-ratelimit-reset", "0")))
                    if finished:
                        continue
                    pages, last_page, page_new = pages + 1, number, 0
                    for gist in gists:
                        entry = log_file(gist)
                        if gist["created_at"] < since or entry is None:
                            continue
                        inserted = add_gist(con, gist, entry)
                        new, known, page_new = new + inserted, known + (1 - inserted), page_new + inserted
                    reached_since = not gists or gists[-1]["created_at"] < since
                    finished = reached_since or (args.new and page_new == 0)
                con.commit()
                page += len(batch)
                if pages // 100 > noted:
                    noted = pages // 100
                    say(f"  page {last_page}: {new} new, {known} already known")
                if remaining < 4 * args.workers and not finished:
                    wait = max(1.0, reset - time.time() + 2)
                    say(f"  API budget nearly spent, waiting {wait:.0f} s for the reset")
                    time.sleep(wait)
    except GiveUp as exc:
        con.commit()
        say(f"stopped: {exc}")
        print(f"RESULT index failed pages={pages} last_page={last_page} new={new} known={known}")
        return 1
    except KeyboardInterrupt:
        con.commit()
        print(f"RESULT index interrupted pages={pages} last_page={last_page} new={new} known={known}")
        return 1
    total = con.execute("SELECT COUNT(*) FROM gists").fetchone()[0]
    print(f"RESULT index ok pages={pages} last_page={last_page} new={new} known={known} total={total}")
    return 0


# --------------------------------------------------------------------- fetch


class Throttle:
    """Shared pacing: a request-rate cap, and a pause every worker honours."""

    def __init__(self, rps: float) -> None:
        self.interval = 1.0 / rps
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.pause_until = 0.0
        self.refusals = 0

    def wait(self, stop: threading.Event) -> None:
        """Hold this worker until its turn, or until the run is stopped."""
        with self.lock:
            now = time.monotonic()
            at = max(now, self.next_at, self.pause_until)
            self.next_at = at + self.interval
        if at > now:
            stop.wait(at - now)

    def refused(self, retry_after: str | None) -> int:
        """Record a 403 or 429 and pause everyone. Returns refusals in a row.

        Requests already in flight when a pause starts come back refused too;
        only a refusal that arrives after the pause ran out counts as another.
        """
        with self.lock:
            now = time.monotonic()
            if now >= self.pause_until:
                self.refusals += 1
                if retry_after and retry_after.isdigit():
                    wait = int(retry_after) + 1.0
                else:
                    wait = min(60.0 * 2 ** (self.refusals - 1), 900.0)
                self.pause_until = now + wait
                say(f"  raw host refused a request, pausing {wait:.0f} s ({self.refusals} in a row)")
            return self.refusals

    def succeeded(self) -> None:
        with self.lock:
            self.refusals = 0


MAX_REFUSALS = 5
_thread = threading.local()


def raw_connection() -> http.client.HTTPSConnection:
    """One kept-alive connection to the raw host per worker thread."""
    conn = getattr(_thread, "conn", None)
    if conn is None:
        conn = _thread.conn = http.client.HTTPSConnection(RAW_HOST, timeout=60)
    return conn


def drop_connection() -> None:
    conn = getattr(_thread, "conn", None)
    if conn is not None:
        conn.close()
        _thread.conn = None


def fetch_one(row, staging: Path, throttle: Throttle, stop: threading.Event, max_bytes: int) -> dict:
    """Download one log to staging as gzip. Returns the row's new state."""
    gist_id, raw_url, week, expect = row
    if expect > max_bytes:
        return {"id": gist_id, "state": "too_large"}
    path = urllib.parse.urlsplit(raw_url).path
    dest = staging / week / f"{gist_id}.txt.gz"
    part = dest.with_name(dest.name + ".part")
    status, error, backoff, failures = None, None, 2.0, 0
    while failures < 4:
        throttle.wait(stop)
        if stop.is_set():
            return {"id": gist_id, "state": None}
        try:
            conn = raw_connection()
            conn.request("GET", path, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"})
            response = conn.getresponse()
            status = response.status
            if status == 200:
                dest.parent.mkdir(parents=True, exist_ok=True)
                digest, received = hashlib.sha256(), 0
                with gzip.open(part, "wb", compresslevel=6) as out:
                    while chunk := response.read(1 << 16):
                        digest.update(chunk)
                        received += len(chunk)
                        out.write(chunk)
                throttle.succeeded()
                if received != expect:
                    part.unlink(missing_ok=True)
                    return {
                        "id": gist_id,
                        "state": "error",
                        "status": status,
                        "bytes": received,
                        "error": f"got {received} of {expect} bytes",
                    }
                os.replace(part, dest)
                return {
                    "id": gist_id,
                    "state": "fetched",
                    "status": status,
                    "bytes": received,
                    "sha256": digest.hexdigest(),
                }
            response.read()
            if status in (403, 429):
                if throttle.refused(response.getheader("retry-after")) >= MAX_REFUSALS:
                    stop.set()
                continue
            if status in (404, 410):
                return {"id": gist_id, "state": "gone", "status": status}
            error = f"HTTP {status}"
        except (http.client.HTTPException, OSError) as exc:
            drop_connection()
            part.unlink(missing_ok=True)
            error = repr(exc)
        failures += 1
        stop.wait(backoff)
        backoff *= 2
    return {"id": gist_id, "state": "error", "status": status, "error": error}


def cmd_fetch(args) -> int:
    con = open_db(args.data)
    staging = args.data / "staging"
    if args.retry_errors:
        con.execute("UPDATE gists SET state = 'pending' WHERE state = 'error'")
        con.commit()
    query, params = "SELECT id, raw_url, week, size FROM gists WHERE state = 'pending'", []
    if args.week:
        query += " AND week = ?"
        params.append(args.week)
    query += " ORDER BY created_at DESC"
    if args.limit:
        query += " LIMIT ?"
        params.append(args.limit)
    rows = con.execute(query, params).fetchall()
    total = len(rows)
    if not total:
        print("RESULT fetch ok nothing pending")
        return 0

    throttle, stop = Throttle(args.rps), threading.Event()
    counts, received, started, last_note, interrupted = Counter(), 0, time.monotonic(), time.monotonic(), False
    removed_hashes = {sha for (sha,) in con.execute("SELECT sha256 FROM removed WHERE sha256 IS NOT NULL")}
    week_of_gist = {row[0]: row[2] for row in rows}

    def record(result: dict) -> None:
        nonlocal received
        if result["state"] is None:
            return
        staged = staging / week_of_gist[result["id"]] / f"{result['id']}.txt.gz"
        result = drop_if_removed(con, staged, result, removed_hashes)
        counts[result["state"]] += 1
        received += result.get("bytes") or 0
        con.execute(
            "UPDATE gists SET state = ?, http_status = ?, bytes = ?, sha256 = ?,"
            " attempts = attempts + 1, fetched_at = ?, error = ? WHERE id = ?",
            (
                result["state"],
                result.get("status"),
                result.get("bytes"),
                result.get("sha256"),
                now_iso(),
                result.get("error"),
                result["id"],
            ),
        )

    queue = iter(rows)
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending: set[cf.Future] = set()

        def top_up() -> None:
            while len(pending) < args.workers * 2 and not stop.is_set():
                row = next(queue, None)
                if row is None:
                    return
                pending.add(pool.submit(fetch_one, row, staging, throttle, stop, args.max_mb << 20))

        try:
            top_up()
            while pending:
                done, pending = cf.wait(pending, return_when=cf.FIRST_COMPLETED)
                for future in done:
                    record(future.result())
                top_up()
                # Committed every time round: an open write would hold up an
                # index run sharing the database.
                con.commit()
                finished = sum(counts.values())
                if time.monotonic() - last_note >= 10:
                    last_note = time.monotonic()
                    rate = finished / (last_note - started)
                    left = (total - finished) / rate if rate else 0
                    say(
                        f"  {finished}/{total}  {received / 1e6:.0f} MB  {rate:.1f}/s"
                        f"  about {dt.timedelta(seconds=int(left))} left"
                    )
        except KeyboardInterrupt:
            interrupted = True
            stop.set()
    con.commit()

    elapsed = time.monotonic() - started
    summary = " ".join(f"{state}={count}" for state, count in sorted(counts.items()))
    outcome = "interrupted" if interrupted else "stopped_refused" if stop.is_set() else "ok"
    print(f"RESULT fetch {outcome} asked={total} {summary} mb={received / 1e6:.1f} seconds={elapsed:.0f}")
    if interrupted:
        return 1
    return 2 if stop.is_set() else 0


# ---------------------------------------------------------------------- pack

# "Name(packageId)[mv:1.2]: assemblies". The name may hold parentheses of its
# own, so the packageId is the last parenthesised token before the colon.
MOD_LINE = re.compile(r"^(?P<name>.*)\((?P<package>[^()\s]+)\)(?:\[[^\]]*\])?: ")
MOD_BLOCK_END = re.compile(r"\r?\n[ \t]*\r?\n")
GAME_VERSION = re.compile(r"RimWorld (\d+\.\d+\.\d+) rev(\d+)")
DLC_PREFIX = "ludeon.rimworld."
TRIM_MARK = "(log trimmed to"


def parse_log(text: str) -> dict:
    """Facts read off a HugsLib log: its mod list, DLC, game version and size."""
    mods: list[str] = []
    start = text.find("Loaded mods:")
    if start != -1:
        block = MOD_BLOCK_END.split(text[start:], maxsplit=1)[0]
        for line in block.splitlines()[1:]:
            match = MOD_LINE.match(line)
            if match:
                mods.append(match["package"])
    version = GAME_VERSION.search(text)
    return {
        "mods": mods,
        "mod_count": len(mods),
        "dlcs": sorted(m[len(DLC_PREFIX):] for m in mods if m.lower().startswith(DLC_PREFIX)),
        "game_version": version[1] if version else None,
        "game_rev": int(version[2]) if version else None,
        "trimmed": TRIM_MARK in text,
        "line_count": text.count("\n") + 1,
    }


PATCH_TABLES = ("HugslibLogs", "ActivePatches", "HarmonyPatches", "HarmonyPatchNamespaces")
# Sorted ids delta-encode to a fraction of a byte a row; a dictionary would undo that.
ACTIVE_PATCHES_ENCODING = {
    "use_dictionary": False,
    "column_encoding": {"HugslibLogId": "DELTA_BINARY_PACKED", "HarmonyPatchId": "DELTA_BINARY_PACKED"},
}
PATCH_SECTION = re.compile(r"(?:^| )(PRE|post|TRANS): ")
PATCH_PRIORITY = re.compile(r"^\[(-?\d+)\]")
PATCH_KINDS = {"PRE": "prefix", "post": "postfix", "TRANS": "transpiler"}


def parse_patches(text: str) -> list[tuple]:
    """Unique (target, kind, priority, method) entries from the Harmony patch block.

    HugsLib prints one line per patched method, "Type.Method: PRE: a, b post: c
    TRANS: d", naming each patch by its full method name with "[priority]" in
    front when the priority is not the default. It prints no finalizers, and a
    target carries neither namespace nor signature, so overloads share a name.
    """
    start = text.find("Active Harmony patches:")
    if start == -1:
        return []
    block = MOD_BLOCK_END.split(text[start:], maxsplit=1)[0]
    found = set()
    for line in block.splitlines()[1:]:
        target, separator, rest = line.partition(": ")
        if not separator or target == "Harmony versions present":
            continue
        pieces = PATCH_SECTION.split(rest)
        for label, names in zip(pieces[1::2], pieces[2::2]):
            for name in names.split(", "):
                name = name.strip()
                priority = PATCH_PRIORITY.match(name)
                if priority:
                    name = name[priority.end():]
                if name:
                    found.add((target, PATCH_KINDS[label], int(priority[1]) if priority else None, name))
    return list(found)


def patch_key(entry: tuple) -> int:
    """A patch's dictionary key: 64 bits of a hash over its four printed parts."""
    target, kind, priority, method = entry
    digest = hashlib.blake2b(f"{target}\x1f{kind}\x1f{priority}\x1f{method}".encode("utf-8"), digest_size=8)
    return int.from_bytes(digest.digest(), "big", signed=True)


def namespace_of(method: str) -> str:
    """Namespace of a patch method printed as Namespace.Class.Method, '' when it has none."""
    parts = method.rsplit(".", 2)
    return parts[0] if len(parts) == 3 else ""


def known_patch_ids(con: sqlite3.Connection, keys) -> dict[int, int]:
    """The ids already assigned to these patch keys."""
    con.execute("DELETE FROM wanted")
    con.executemany("INSERT INTO wanted (key) VALUES (?)", ((key,) for key in keys))
    return dict(con.execute("SELECT p.key, p.id FROM wanted w JOIN patch_ids p ON p.key = w.key"))


def cmd_pack(args) -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        say("pack needs pyarrow: run it as `uv run corpus.py pack`")
        return 1

    schemas = {
        "logs": pa.schema(
            [
                ("hugslib_log_id", pa.int32()),
                ("id", pa.string()),
                ("created_at", pa.timestamp("s", tz="UTC")),
                ("size", pa.int64()),
                ("uploader", pa.string()),
                ("sha256", pa.string()),
                ("utf8_ok", pa.bool_()),
                ("game_version", pa.string()),
                ("game_rev", pa.int32()),
                ("mod_count", pa.int32()),
                ("dlcs", pa.list_(pa.string())),
                ("mods", pa.list_(pa.string())),
                ("trimmed", pa.bool_()),
                ("line_count", pa.int32()),
                ("text", pa.large_string()),
            ]
        ),
        "HugslibLogs": pa.schema(
            [("HugslibLogId", pa.int32()), ("GUID", pa.string()), ("UploadTime", pa.timestamp("s", tz="UTC"))]
        ),
        "ActivePatches": pa.schema([("HugslibLogId", pa.int32()), ("HarmonyPatchId", pa.int32())]),
        "HarmonyPatches": pa.schema(
            [
                ("HarmonyPatchId", pa.int32()),
                ("Target", pa.string()),
                ("Kind", pa.string()),
                ("Priority", pa.int32()),
                ("Method", pa.string()),
                ("HarmonyPatchNamespaceId", pa.int32()),
            ]
        ),
        "HarmonyPatchNamespaces": pa.schema([("HarmonyPatchNamespaceId", pa.int32()), ("Namespace", pa.string())]),
    }
    delta = ACTIVE_PATCHES_ENCODING
    con = open_db(args.data)
    con.execute("CREATE TEMP TABLE IF NOT EXISTS wanted (key INTEGER PRIMARY KEY)")
    staging, shards, patches = args.data / "staging", args.data / "shards", args.data / "patches"
    open_week = week_of(now_iso())
    weeks = [w for (w,) in con.execute("SELECT DISTINCT week FROM gists WHERE state = 'fetched' ORDER BY week")]
    namespace_ids = dict(con.execute("SELECT name, id FROM namespace_ids"))
    next_patch = (con.execute("SELECT MAX(id) FROM patch_ids").fetchone()[0] or 0) + 1
    next_log = (con.execute("SELECT MAX(log_no) FROM gists").fetchone()[0] or 0) + 1
    packed_weeks = packed_logs = raw_bytes = shard_bytes = patch_bytes = skipped = missing = 0
    patch_rows = new_patches = collisions = 0

    for week in weeks:
        if args.week and week != args.week:
            continue
        waiting = con.execute(
            "SELECT COUNT(*) FROM gists WHERE week = ? AND state = 'pending'", (week,)
        ).fetchone()[0]
        if (week == open_week and not args.include_open) or (waiting and not args.partial):
            skipped += 1
            continue
        rows = con.execute(
            "SELECT id, created_at, size, uploader, sha256 FROM gists"
            " WHERE week = ? AND state = 'fetched' ORDER BY created_at",
            (week,),
        ).fetchall()
        # The part number comes from the index, so a part left behind by a
        # crashed run is overwritten rather than duplicated.
        part = con.execute(
            "SELECT COUNT(DISTINCT shard) FROM gists WHERE week = ? AND state = 'packed'", (week,)
        ).fetchone()[0]
        shard = f"week={week}/part-{part:03d}.parquet"
        outputs = {"logs": shards / shard, **{name: patches / name / shard for name in PATCH_TABLES}}
        temps = {name: path.with_name(path.name + ".tmp") for name, path in outputs.items()}
        for path in outputs.values():
            path.parent.mkdir(parents=True, exist_ok=True)
        batch, batch_bytes, done, facts, week_patch_rows = [], 0, [], [], 0

        with contextlib.ExitStack() as stack:
            writers = {
                name: stack.enter_context(
                    pq.ParquetWriter(
                        temps[name],
                        schemas[name],
                        compression="zstd",
                        compression_level=args.level,
                        **(delta if name == "ActivePatches" else {}),
                    )
                )
                for name in outputs
            }

            def flush() -> None:
                nonlocal batch, batch_bytes, next_patch, new_patches, collisions, week_patch_rows
                if not batch:
                    return
                writers["logs"].write_table(pa.Table.from_pylist([row for row, _ in batch], schema=schemas["logs"]))
                writers["HugslibLogs"].write_table(
                    pa.Table.from_pylist(
                        [
                            {"HugslibLogId": row["hugslib_log_id"], "GUID": row["id"], "UploadTime": row["created_at"]}
                            for row, _ in batch
                        ],
                        schema=schemas["HugslibLogs"],
                    )
                )

                wanted, per_log = {}, []
                for row, entries in batch:
                    keys = []
                    for entry in entries:
                        key = patch_key(entry)
                        if wanted.setdefault(key, entry) != entry:
                            collisions += 1
                        keys.append(key)
                    per_log.append((row["hugslib_log_id"], keys))
                ids = known_patch_ids(con, wanted)

                # New patches take the next ids in method order, which keeps
                # one mod's patches on neighbouring ids.
                fresh = sorted(
                    (key for key in wanted if key not in ids),
                    key=lambda key: (wanted[key][3], wanted[key][0], wanted[key][1]),
                )
                patch_table, namespace_table = [], []
                for key in fresh:
                    target, kind, priority, method = wanted[key]
                    namespace = namespace_of(method)
                    if namespace not in namespace_ids:
                        namespace_ids[namespace] = len(namespace_ids) + 1
                        namespace_table.append(
                            {"HarmonyPatchNamespaceId": namespace_ids[namespace], "Namespace": namespace}
                        )
                    ids[key] = next_patch
                    patch_table.append(
                        {
                            "HarmonyPatchId": next_patch,
                            "Target": target,
                            "Kind": kind,
                            "Priority": priority,
                            "Method": method,
                            "HarmonyPatchNamespaceId": namespace_ids[namespace],
                        }
                    )
                    next_patch += 1
                con.executemany("INSERT INTO patch_ids (key, id) VALUES (?, ?)", [(key, ids[key]) for key in fresh])
                con.executemany(
                    "INSERT INTO namespace_ids (name, id) VALUES (?, ?)",
                    [(row["Namespace"], row["HarmonyPatchNamespaceId"]) for row in namespace_table],
                )
                if patch_table:
                    writers["HarmonyPatches"].write_table(
                        pa.Table.from_pylist(patch_table, schema=schemas["HarmonyPatches"])
                    )
                if namespace_table:
                    writers["HarmonyPatchNamespaces"].write_table(
                        pa.Table.from_pylist(namespace_table, schema=schemas["HarmonyPatchNamespaces"])
                    )

                log_column, patch_column = [], []
                for log_no, keys in per_log:
                    mine = sorted({ids[key] for key in keys})
                    log_column += [log_no] * len(mine)
                    patch_column += mine
                writers["ActivePatches"].write_table(
                    pa.table(
                        {"HugslibLogId": pa.array(log_column, pa.int32()), "HarmonyPatchId": pa.array(patch_column, pa.int32())},
                        schema=schemas["ActivePatches"],
                    )
                )
                new_patches += len(fresh)
                week_patch_rows += len(patch_column)
                batch, batch_bytes = [], 0

            for gist_id, created_at, size, uploader, sha256 in rows:
                staged = staging / week / f"{gist_id}.txt.gz"
                try:
                    with gzip.open(staged, "rb") as handle:
                        raw = handle.read()
                except FileNotFoundError:
                    con.execute("UPDATE gists SET state = 'pending' WHERE id = ?", (gist_id,))
                    missing += 1
                    continue
                try:
                    text, utf8_ok = raw.decode("utf-8"), True
                except UnicodeDecodeError:
                    text, utf8_ok = raw.decode("utf-8", errors="replace"), False
                parsed = parse_log(text)
                batch.append(
                    (
                        {
                            "hugslib_log_id": next_log,
                            "id": gist_id,
                            "created_at": dt.datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").replace(
                                tzinfo=dt.timezone.utc
                            ),
                            "size": size,
                            "uploader": uploader,
                            "sha256": sha256,
                            "utf8_ok": utf8_ok,
                            "text": text,
                            **parsed,
                        },
                        parse_patches(text),
                    )
                )
                facts.append(
                    (
                        gist_id,
                        parsed["game_version"],
                        parsed["game_rev"],
                        parsed["mod_count"],
                        ",".join(parsed["dlcs"]),
                        int(parsed["trimmed"]),
                        parsed["line_count"],
                        int(utf8_ok),
                    )
                )
                done.append((gist_id, staged, next_log))
                next_log += 1
                batch_bytes += len(raw)
                raw_bytes += len(raw)
                if len(batch) >= 64 or batch_bytes >= 64 << 20:
                    flush()
            flush()

        if not done:
            for temp in temps.values():
                temp.unlink(missing_ok=True)
            con.commit()
            continue
        written = pq.ParquetFile(temps["logs"]).metadata.num_rows
        active = pq.ParquetFile(temps["ActivePatches"]).metadata.num_rows
        if written != len(done) or active != week_patch_rows:
            con.rollback()
            for temp in temps.values():
                temp.unlink(missing_ok=True)
            say(
                f"{week}: wrote {written} log rows for {len(done)} logs and {active} patch rows"
                f" for {week_patch_rows}, leaving the week staged"
            )
            return 1
        for name, temp in temps.items():
            os.replace(temp, outputs[name])
        con.executemany("INSERT OR REPLACE INTO facts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", facts)
        con.executemany(
            "UPDATE gists SET state = 'packed', shard = ?, log_no = ? WHERE id = ?",
            [(shard, log_no, gist_id) for gist_id, _, log_no in done],
        )
        con.execute("INSERT OR REPLACE INTO parts (shard, logs, uploaded_at) VALUES (?, ?, NULL)", (shard, len(done)))
        con.commit()
        if not args.keep_staging:
            for _, staged, _ in done:
                staged.unlink(missing_ok=True)
            try:
                (staging / week).rmdir()
            except OSError:
                pass
        packed_weeks += 1
        packed_logs += len(done)
        patch_rows += week_patch_rows
        shard_bytes += outputs["logs"].stat().st_size
        tables_bytes = sum(outputs[name].stat().st_size for name in PATCH_TABLES)
        patch_bytes += tables_bytes
        say(
            f"  {week}: {len(done)} logs -> {shard} ({outputs['logs'].stat().st_size / 1e6:.1f} MB),"
            f" {week_patch_rows} patch rows ({tables_bytes / 1e6:.1f} MB)"
        )

    print(
        f"RESULT pack ok weeks={packed_weeks} logs={packed_logs} skipped_weeks={skipped} missing={missing}"
        f" text_mb={raw_bytes / 1e6:.1f} parquet_mb={shard_bytes / 1e6:.1f} patch_rows={patch_rows}"
        f" new_patches={new_patches} patches_mb={patch_bytes / 1e6:.1f} key_collisions={collisions}"
    )
    return 0


# -------------------------------------------------------------------- upload


class ConfigError(Exception):
    """A missing or unusable setting, reported without its value."""


class UploadError(Exception):
    """The bucket or Supabase did not take what was sent."""


def load_env() -> dict[str, str]:
    """Settings from the env file: KEY=value lines, comments and blanks skipped."""
    path = Path(os.environ.get("HUGSLIB_CORPUS_ENV") or DEFAULT_ENV)
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
    missing = [key for key in keys if not env.get(key)]
    if missing:
        raise ConfigError("not set in the env file: " + ", ".join(missing))


def r2_client(env: dict[str, str]):
    """An S3 client for the R2 bucket named in the env file."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise ConfigError("uploads need boto3: run it as `uv run corpus.py ...`") from exc
    require(env, R2_KEYS)
    return boto3.client(
        "s3",
        endpoint_url=env["R2_ENDPOINT"],
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def part_files(data: Path, shard: str) -> dict[str, Path]:
    """Bucket key to local file, for the five files of one packed part."""
    files = {f"logs/{shard}": data / "shards" / shard}
    files.update({f"patches/{name}/{shard}": data / "patches" / name / shard for name in PATCH_TABLES})
    return files


def push_parts(con: sqlite3.Connection, data: Path, client, bucket: str, keep_local: bool) -> tuple[int, int]:
    """Upload every packed part the bucket lacks, then drop the local copy."""
    pushed = sent = 0
    for (shard,) in con.execute("SELECT shard FROM parts WHERE uploaded_at IS NULL ORDER BY shard").fetchall():
        files = part_files(data, shard)
        for key, path in files.items():
            size = path.stat().st_size
            client.upload_file(str(path), bucket, key)
            stored = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
            if stored != size:
                raise UploadError(f"{key}: the bucket holds {stored} bytes, the file has {size}")
            sent += size
        con.execute("UPDATE parts SET uploaded_at = ? WHERE shard = ?", (now_iso(), shard))
        con.commit()
        if not keep_local:
            for path in files.values():
                path.unlink(missing_ok=True)
                with contextlib.suppress(OSError):
                    path.parent.rmdir()
        pushed += 1
        say(f"  pushed {shard}")
    return pushed, sent


def backup_state(con: sqlite3.Connection, data: Path, client, bucket: str) -> None:
    """Copy the index database to the bucket, so another machine can resume."""
    copy = data / "index.backup.sqlite"
    target = sqlite3.connect(copy)
    try:
        con.backup(target)
    finally:
        target.close()
    client.upload_file(str(copy), bucket, "state/index.sqlite")
    copy.unlink()


def supabase_request(env: dict[str, str], method: str, path: str, body=None, prefer: str | None = None) -> bytes:
    """One PostgREST call with the secret key, which goes on the apikey header only."""
    require(env, SUPABASE_KEYS)
    headers = {"apikey": env["SUPABASE_SECRET_KEY"], "User-Agent": USER_AGENT}
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if prefer:
        headers["Prefer"] = prefer
    request = urllib.request.Request(
        env["SUPABASE_URL"].rstrip("/") + path, data=payload, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read()[:300].decode("utf-8", "replace")
        if exc.code == 404 and "PGRST205" in detail:
            raise ConfigError("a Supabase table is missing: apply schema.sql in the project's SQL editor") from exc
        raise UploadError(f"Supabase answered HTTP {exc.code}: {detail}") from exc


def sync_logs(con: sqlite3.Connection, env: dict[str, str]) -> int:
    """Upsert every packed log the Supabase logs table lacks. Returns the count."""
    synced = 0
    while True:
        rows = con.execute(
            "SELECT g.id, g.log_no, g.created_at, g.size, g.uploader, g.week, g.shard, g.sha256,"
            " f.game_version, f.game_rev, f.mod_count, f.dlcs, f.trimmed, f.line_count"
            " FROM gists g JOIN facts f ON f.id = g.id WHERE g.state = 'packed' AND g.synced = 0 LIMIT 500"
        ).fetchall()
        if not rows:
            return synced
        body = [
            {
                "id": row[0],
                "hugslib_log_id": row[1],
                "created_at": row[2],
                "size_bytes": row[3],
                "uploader": row[4],
                "week": row[5],
                "shard": row[6],
                "sha256": row[7],
                "game_version": row[8],
                "game_rev": row[9],
                "mod_count": row[10],
                "dlcs": row[11].split(",") if row[11] else [],
                "trimmed": bool(row[12]),
                "line_count": row[13],
            }
            for row in rows
        ]
        supabase_request(env, "POST", "/rest/v1/logs?on_conflict=id", body, UPSERT)
        con.executemany("UPDATE gists SET synced = 1 WHERE id = ?", [(row[0],) for row in rows])
        con.commit()
        synced += len(rows)


UPSERT = "resolution=merge-duplicates,return=minimal"


def sync_removed(con: sqlite3.Connection, env: dict[str, str], data: Path) -> tuple[int, int]:
    """Merge the removed list with its copy in Supabase, both ways.

    Returns how many removals this index learned and how many it sent up. A
    log learned this way is marked removed here; whoever removed it has
    already rewritten the files in the bucket.
    """
    remote, offset = {}, 0
    while True:
        page = json.loads(
            supabase_request(
                env, "GET", f"/rest/v1/removed_logs?select=id,sha256,removed_at&order=id&limit=1000&offset={offset}"
            )
        )
        remote.update({row["id"]: row for row in page})
        if len(page) < 1000:
            break
        offset += 1000
    local = {row[0]: row for row in con.execute("SELECT id, removed_at, sha256 FROM removed")}
    learned = [row for gist_id, row in remote.items() if gist_id not in local]
    for row in learned:
        staged = con.execute("SELECT week FROM gists WHERE id = ? AND state = 'fetched'", (row["id"],)).fetchone()
        if staged:
            (data / "staging" / staged[0] / f"{row['id']}.txt.gz").unlink(missing_ok=True)
        con.execute(
            "INSERT OR IGNORE INTO removed (id, removed_at, sha256) VALUES (?, ?, ?)",
            (row["id"], row["removed_at"], row.get("sha256")),
        )
        con.execute("UPDATE gists SET state = 'removed', synced = 0 WHERE id = ? AND state != 'removed'", (row["id"],))
    con.commit()
    unsent = [
        {"id": gist_id, "removed_at": row[1], "sha256": row[2]} for gist_id, row in local.items() if gist_id not in remote
    ]
    for start in range(0, len(unsent), 500):
        supabase_request(env, "POST", "/rest/v1/removed_logs?on_conflict=id", unsent[start : start + 500], UPSERT)
    return len(learned), len(unsent)


def drop_if_removed(con: sqlite3.Connection, staged: Path, result: dict, hashes: set) -> dict:
    """Discard a download whose content is on the removed list, whatever id it came under."""
    if result.get("state") != "fetched" or result.get("sha256") not in hashes:
        return result
    staged.unlink(missing_ok=True)
    con.execute(
        "INSERT OR IGNORE INTO removed (id, removed_at, sha256) VALUES (?, ?, ?)",
        (result["id"], now_iso(), result["sha256"]),
    )
    return {**result, "state": "removed"}


def cmd_push(args) -> int:
    env = load_env()
    client = r2_client(env)
    con = open_db(args.data)
    pushed, sent = push_parts(con, args.data, client, env["R2_BUCKET"], args.keep_local)
    if args.state:
        backup_state(con, args.data, client, env["R2_BUCKET"])
    print(f"RESULT push ok parts={pushed} mb={sent / 1e6:.1f}")
    return 0


def cmd_sync(args) -> int:
    con = open_db(args.data)
    print(f"RESULT sync ok rows={sync_logs(con, load_env())}")
    return 0


def cmd_run(args) -> int:
    env = load_env()
    client = r2_client(env)
    bucket = env["R2_BUCKET"]
    client.head_bucket(Bucket=bucket)
    use_supabase = not args.no_supabase
    if use_supabase:
        supabase_request(env, "GET", "/rest/v1/logs?select=id&limit=1")
    con = open_db(args.data)
    if use_supabase:
        sync_removed(con, env, args.data)
    push_parts(con, args.data, client, bucket, keep_local=False)

    finished = pushed = synced = 0
    while not args.weeks or finished < args.weeks:
        # Asked afresh each time round, so weeks an index run adds meanwhile are picked up.
        newest = con.execute(
            "SELECT week, SUM(state = 'pending') FROM gists WHERE state IN ('pending', 'fetched') AND week != ?"
            " GROUP BY week ORDER BY week DESC LIMIT 1",
            (week_of(now_iso()),),
        ).fetchone()
        if newest is None:
            break
        week, waiting = newest
        say(f"week {week}: {waiting} logs to fetch")
        code = cmd_fetch(
            argparse.Namespace(
                data=args.data,
                limit=0,
                week=week,
                workers=args.workers,
                rps=args.rps,
                max_mb=args.max_mb,
                retry_errors=False,
            )
        )
        if code == 0:
            code = cmd_pack(
                argparse.Namespace(
                    data=args.data, week=week, include_open=False, partial=False, keep_staging=False, level=args.level
                )
            )
        if code != 0:
            print(f"RESULT run stopped week={week} weeks_done={finished} parts={pushed} synced={synced}")
            return code
        pushed += push_parts(con, args.data, client, bucket, keep_local=False)[0]
        if use_supabase:
            synced += sync_logs(con, env)
        finished += 1
        if finished % args.backup_every == 0:
            backup_state(con, args.data, client, bucket)
    backup_state(con, args.data, client, bucket)
    print(f"RESULT run ok weeks_done={finished} parts={pushed} synced={synced}")
    return 0


# -------------------------------------------------------------------- remove

GIST_LINK = re.compile(r"gist\.github(?:usercontent)?\.com/(?:[^/\s]+/)?([0-9a-f]{20,40})(?:[/?#.]|$)")
GIST_ID = re.compile(r"[0-9a-f]{20,40}")


def gist_id_of(text: str) -> str | None:
    """The gist id in a gist link, a raw log link, or a bare id."""
    text = text.strip()
    link = GIST_LINK.search(text)
    if link:
        return link[1]
    return text if GIST_ID.fullmatch(text) else None


def rewrite_without(pq, source: Path, dest: Path, column: str, value, **options) -> tuple[int, int]:
    """Copy a Parquet file, leaving out rows where column equals value. Returns rows kept and dropped."""
    import pyarrow.compute as pc

    file = pq.ParquetFile(source)
    kept = dropped = 0
    with pq.ParquetWriter(dest, file.schema_arrow, compression="zstd", **options) as writer:
        for group in range(file.num_row_groups):
            table = file.read_row_group(group)
            rest = table.filter(pc.not_equal(table[column], value))
            dropped += table.num_rows - rest.num_rows
            kept += rest.num_rows
            if rest.num_rows:
                writer.write_table(rest)
    return kept, dropped


def remove_from_part(pq, con, args, client, bucket, gist_id: str, shard: str, log_no: int) -> int:
    """Rewrite the three files of a part that hold a log's rows. Returns rows dropped."""
    uploaded = con.execute("SELECT uploaded_at FROM parts WHERE shard = ?", (shard,)).fetchone()
    in_bucket = bool(uploaded and uploaded[0])
    local = part_files(args.data, shard)
    targets = {
        f"logs/{shard}": ("id", gist_id, {}),
        f"patches/HugslibLogs/{shard}": ("HugslibLogId", log_no, {}),
        f"patches/ActivePatches/{shard}": ("HugslibLogId", log_no, ACTIVE_PATCHES_ENCODING),
    }
    work = args.data / "removal"
    work.mkdir(parents=True, exist_ok=True)
    total = 0
    for key, (column, value, options) in targets.items():
        source = work / key.replace("/", "_") if in_bucket else local[key]
        rewritten = source.with_name(source.name + ".new")
        if in_bucket:
            client.download_file(bucket, key, str(source))
        kept, dropped = rewrite_without(pq, source, rewritten, column, value, compression_level=args.level, **options)
        say(f"  {key}: {dropped} rows dropped, {kept} kept")
        total += dropped
        if args.dry_run:
            rewritten.unlink()
        elif in_bucket:
            client.upload_file(str(rewritten), bucket, key)
            stored = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
            if stored != rewritten.stat().st_size:
                raise UploadError(f"{key}: the bucket holds {stored} bytes, the rewritten file has {rewritten.stat().st_size}")
            rewritten.unlink()
        else:
            os.replace(rewritten, source)
        if in_bucket:
            source.unlink()
    with contextlib.suppress(OSError):
        work.rmdir()
    return total


def cmd_remove(args) -> int:
    ids = [gist_id_of(item) for item in args.gists]
    if None in ids:
        say("not a gist link or id: " + args.gists[ids.index(None)])
        return 1
    con = open_db(args.data)
    env = client = pq = None
    removed = touched_bucket = 0
    queue = list(dict.fromkeys(ids))
    while queue:
        gist_id = queue.pop(0)
        row = con.execute("SELECT state, week, shard, log_no, sha256 FROM gists WHERE id = ?", (gist_id,)).fetchone()
        state, week, shard, log_no, sha256 = row or (None, None, None, None, None)
        if sha256:
            twins = [
                twin
                for (twin,) in con.execute(
                    "SELECT id FROM gists WHERE sha256 = ? AND id != ? AND state != 'removed'", (sha256, gist_id)
                )
                if twin not in queue
            ]
            if twins and args.same_content:
                queue += twins
            elif twins:
                say(f"  {len(twins)} other uploads hold the same content; --same-content removes them as well")
        say(f"{gist_id}: {state or 'not in the index'}")
        if state == "packed":
            if pq is None:
                try:
                    import pyarrow.parquet as pq
                except ImportError:
                    say("removing a packed log needs pyarrow: run it as `uv run corpus.py remove ...`")
                    return 1
            in_bucket = con.execute("SELECT uploaded_at FROM parts WHERE shard = ?", (shard,)).fetchone()
            if in_bucket and in_bucket[0] and client is None:
                env = load_env()
                client = r2_client(env)
            remove_from_part(pq, con, args, client, env and env["R2_BUCKET"], gist_id, shard, log_no)
            touched_bucket += bool(client)
            if not args.dry_run and not args.no_supabase:
                supabase_request(env or load_env(), "DELETE", f"/rest/v1/logs?id=eq.{gist_id}")
        elif state == "fetched" and not args.dry_run:
            (args.data / "staging" / week / f"{gist_id}.txt.gz").unlink(missing_ok=True)
        if args.dry_run:
            continue
        entry = {"id": gist_id, "removed_at": now_iso(), "sha256": sha256}
        if not args.no_supabase:
            env = env or load_env()
            supabase_request(env, "POST", "/rest/v1/removed_logs?on_conflict=id", [entry], UPSERT)
        con.execute(
            "INSERT OR REPLACE INTO removed (id, removed_at, sha256) VALUES (?, ?, ?)",
            (gist_id, entry["removed_at"], sha256),
        )
        con.execute("UPDATE gists SET state = 'removed', synced = 0, error = NULL WHERE id = ?", (gist_id,))
        con.execute("DELETE FROM facts WHERE id = ?", (gist_id,))
        if state == "packed":
            con.execute("UPDATE parts SET logs = logs - 1 WHERE shard = ?", (shard,))
        con.commit()
        removed += 1
    if touched_bucket and not args.dry_run:
        backup_state(con, args.data, client, env["R2_BUCKET"])
    print(f"RESULT remove {'dry run' if args.dry_run else 'ok'} asked={len(ids)} removed={removed}")
    return 0


# -------------------------------------------------------------------- status


def tree_bytes(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file()) if root.exists() else 0


def cmd_status(args) -> int:
    con = open_db(args.data)
    oldest, newest = con.execute("SELECT MIN(created_at), MAX(created_at) FROM gists").fetchone()
    print(f"data     {args.data}")
    print(f"span     {oldest} to {newest}")
    for state, count, size in con.execute(
        "SELECT state, COUNT(*), COALESCE(SUM(size), 0) FROM gists GROUP BY state ORDER BY state"
    ):
        print(f"{state:<9}{count:>8} logs {size / 1e9:>8.2f} GB as text")
    print(f"staging  {tree_bytes(args.data / 'staging') / 1e9:.2f} GB on disk")
    print(f"shards   {tree_bytes(args.data / 'shards') / 1e9:.2f} GB on disk")
    print(f"patches  {tree_bytes(args.data / 'patches') / 1e9:.2f} GB on disk")
    distinct = con.execute("SELECT COUNT(*) FROM patch_ids").fetchone()[0]
    namespaces = con.execute("SELECT COUNT(*) FROM namespace_ids").fetchone()[0]
    print(f"distinct patches {distinct}, namespaces {namespaces}")
    waiting = con.execute("SELECT COUNT(*) FROM parts WHERE uploaded_at IS NULL").fetchone()[0]
    uploaded = con.execute("SELECT COUNT(*) FROM parts WHERE uploaded_at IS NOT NULL").fetchone()[0]
    unsynced = con.execute("SELECT COUNT(*) FROM gists WHERE state = 'packed' AND synced = 0").fetchone()[0]
    print(f"parts    {uploaded} in the bucket, {waiting} waiting to upload; {unsynced} packed logs not yet in Supabase")
    return 0


# ---------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="corpus.py", description="Private copy of the public HugsLib log gists.")
    parser.add_argument(
        "--data",
        type=Path,
        default=Path(os.environ.get("HUGSLIB_CORPUS_DIR") or DEFAULT_DATA),
        help="corpus folder, kept outside every repo",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    index = commands.add_parser("index", help="list uploads into the local index")
    index.add_argument("--since", default=RELEASE_1_6, help="oldest upload date, YYYY-MM-DD (default: 1.6 release)")
    index.add_argument("--new", action="store_true", help="stop at the first page with nothing new")
    index.add_argument("--start-page", type=int, default=1, help="list page to start from, to resume a walk")
    index.add_argument("--max-pages", type=int, default=0, help="read at most this many pages")
    index.add_argument("--workers", type=int, default=1, help="list pages read at once (see WHY: index)")
    index.add_argument("--no-supabase", action="store_true", help="list without first fetching the removed list")
    index.set_defaults(run=cmd_index)

    fetch = commands.add_parser("fetch", help="download pending logs into staging")
    fetch.add_argument("--limit", type=int, default=0, help="download at most this many logs")
    fetch.add_argument("--week", help="only this ISO week, e.g. 2026-W46")
    fetch.add_argument("--workers", type=int, default=4, help="downloads in flight at once")
    fetch.add_argument("--rps", type=float, default=6.0, help="requests a second across all workers")
    fetch.add_argument("--max-mb", type=int, default=64, help="skip logs larger than this")
    fetch.add_argument("--retry-errors", action="store_true", help="queue failed downloads again first")
    fetch.set_defaults(run=cmd_fetch)

    pack = commands.add_parser("pack", help="pack finished weeks into Parquet")
    pack.add_argument("--week", help="only this ISO week")
    pack.add_argument("--include-open", action="store_true", help="also pack the week still in progress")
    pack.add_argument("--partial", action="store_true", help="pack a week that still has pending logs")
    pack.add_argument("--keep-staging", action="store_true", help="leave the staged gzip files in place")
    pack.add_argument("--level", type=int, default=9, help="zstd level")
    pack.set_defaults(run=cmd_pack)

    push = commands.add_parser("push", help="upload packed parts to the bucket")
    push.add_argument("--keep-local", action="store_true", help="leave the uploaded files on disk")
    push.add_argument("--state", action="store_true", help="also copy the index database to the bucket")
    push.set_defaults(run=cmd_push)

    sync = commands.add_parser("sync", help="upsert packed logs into the Supabase logs table")
    sync.set_defaults(run=cmd_sync)

    run = commands.add_parser("run", help="fetch, pack, push and sync week by week, newest first")
    run.add_argument("--weeks", type=int, default=0, help="stop after this many weeks")
    run.add_argument("--workers", type=int, default=4, help="downloads in flight at once")
    run.add_argument("--rps", type=float, default=6.0, help="requests a second across all workers")
    run.add_argument("--max-mb", type=int, default=64, help="skip logs larger than this")
    run.add_argument("--level", type=int, default=9, help="zstd level")
    run.add_argument("--no-supabase", action="store_true", help="skip the Supabase sync; `sync` can catch up later")
    run.add_argument("--backup-every", type=int, default=8, help="copy the index to the bucket every N weeks")
    run.set_defaults(run=cmd_run)

    remove = commands.add_parser("remove", help="take logs out of the archive, by gist link or id")
    remove.add_argument("gists", nargs="+", metavar="LINK", help="a gist link, a raw log link, or a gist id")
    remove.add_argument("--dry-run", action="store_true", help="report what would be dropped and change nothing")
    remove.add_argument("--same-content", action="store_true", help="also remove other uploads of the same log")
    remove.add_argument("--no-supabase", action="store_true", help="leave the Supabase row alone")
    remove.add_argument("--level", type=int, default=9, help="zstd level for the rewritten files")
    remove.set_defaults(run=cmd_remove)

    status = commands.add_parser("status", help="counts by state and disk use")
    status.set_defaults(run=cmd_status)

    args = parser.parse_args(argv)
    args.data = args.data.expanduser()
    try:
        return args.run(args)
    except (OSError, sqlite3.Error, ConfigError, UploadError) as exc:
        say(f"error: {exc}")
        return 1
    except Exception as exc:
        if type(exc).__module__.split(".")[0] in ("botocore", "boto3", "s3transfer"):
            say(f"R2 error: {exc}")
            return 1
        raise


if __name__ == "__main__":
    sys.exit(main())
