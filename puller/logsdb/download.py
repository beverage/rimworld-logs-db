"""fetch: download each indexed log once, compressed as it arrives.

Log bodies come from gist.githubusercontent.com, GitHub's raw file host. The
API's own gist endpoint truncates a file at 1 MB, and about a third of recent
logs are larger. The raw host is not covered by the API's hourly allowance,
and GitHub publishes no limits for it, so this step sets its own: requests go
out without a token at a capped rate, a 403 or 429 pauses every worker for as
long as the response asks, and five refusals in a row end the run. Pushing on
while refused is what gets a client banned.

The raw host sends no compression, and the full set is larger as text than
the disk of the machine that builds it. Each log is gzipped as it arrives and
is never written to disk as plain text.
"""

from __future__ import annotations

import concurrent.futures as cf
import datetime as dt
import gzip
import hashlib
import http.client
import os
import sqlite3
import threading
import time
import urllib.parse
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path

from .common import now_iso, say
from .index_db import open_db
from .layout import staged_path
from .settings import RAW_HOST, USER_AGENT

MAX_REFUSALS = 5
MAX_FAILURES = 4
CHUNK = 1 << 16


@dataclass
class Download:
    """What became of one log's download.

    `state` is the gist's new state in the index: fetched, gone, too_large,
    error or removed. It is None when the run was stopped before this log was
    tried, which leaves the gist pending.
    """

    id: str
    state: str | None
    status: int | None = None
    bytes: int | None = None
    sha256: str | None = None
    error: str | None = None


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


def fetch_one(row, staging: Path, throttle: Throttle, stop: threading.Event, max_bytes: int) -> Download:
    """Download one log into staging. `row` is (gist id, raw URL, week, expected size)."""
    gist_id, raw_url, week, expect = row
    if expect > max_bytes:
        return Download(gist_id, "too_large")
    path = urllib.parse.urlsplit(raw_url).path
    dest = staged_path(staging, week, gist_id)
    part = dest.with_name(dest.name + ".part")
    status, error, backoff, failures = None, None, 2.0, 0
    while failures < MAX_FAILURES:
        throttle.wait(stop)
        if stop.is_set():
            return Download(gist_id, None)
        try:
            conn = raw_connection()
            conn.request("GET", path, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"})
            response = conn.getresponse()
            status = response.status
            if status == 200:
                dest.parent.mkdir(parents=True, exist_ok=True)
                digest, received = hashlib.sha256(), 0
                with gzip.open(part, "wb", compresslevel=6) as out:
                    while chunk := response.read(CHUNK):
                        digest.update(chunk)
                        received += len(chunk)
                        out.write(chunk)
                throttle.succeeded()
                if received != expect:
                    part.unlink(missing_ok=True)
                    return Download(gist_id, "error", status, received, error=f"got {received} of {expect} bytes")
                os.replace(part, dest)
                return Download(gist_id, "fetched", status, received, digest.hexdigest())
            response.read()
            if status in (403, 429):
                if throttle.refused(response.getheader("retry-after")) >= MAX_REFUSALS:
                    stop.set()
                continue
            if status in (404, 410):
                return Download(gist_id, "gone", status)
            error = f"HTTP {status}"
        except (http.client.HTTPException, OSError) as exc:
            drop_connection()
            part.unlink(missing_ok=True)
            error = repr(exc)
        failures += 1
        stop.wait(backoff)
        backoff *= 2
    return Download(gist_id, "error", status, error=error)


def drop_if_removed(con: sqlite3.Connection, staged: Path, result: Download, hashes: set) -> Download:
    """Discard a download whose content is on the removed list, whatever id it came under."""
    if result.state != "fetched" or result.sha256 not in hashes:
        return result
    staged.unlink(missing_ok=True)
    con.execute(
        "INSERT OR IGNORE INTO removed (id, removed_at, sha256) VALUES (?, ?, ?)",
        (result.id, now_iso(), result.sha256),
    )
    return replace(result, state="removed")


def pending_rows(con: sqlite3.Connection, week: str | None, limit: int) -> list:
    """Logs still to download, newest first: (gist id, raw URL, week, size)."""
    query, params = "SELECT id, raw_url, week, size FROM gists WHERE state = 'pending'", []
    if week:
        query += " AND week = ?"
        params.append(week)
    query += " ORDER BY created_at DESC"
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    return con.execute(query, params).fetchall()


def record(con: sqlite3.Connection, result: Download) -> None:
    """Write one download's outcome to the index."""
    con.execute(
        "UPDATE gists SET state = ?, http_status = ?, bytes = ?, sha256 = ?,"
        " attempts = attempts + 1, fetched_at = ?, error = ? WHERE id = ?",
        (result.state, result.status, result.bytes, result.sha256, now_iso(), result.error, result.id),
    )


def fetch(
    data: Path,
    week: str | None = None,
    limit: int = 0,
    workers: int = 4,
    rps: float = 6.0,
    max_mb: int = 64,
    retry_errors: bool = False,
) -> int:
    """Download every pending log, or one week's, or the newest `limit`.

    Returns 0 when it ran to the end, 1 when interrupted, and 2 when the raw
    host kept refusing and the run stopped itself.
    """
    con = open_db(data)
    staging = data / "staging"
    if retry_errors:
        con.execute("UPDATE gists SET state = 'pending' WHERE state = 'error'")
        con.commit()
    rows = pending_rows(con, week, limit)
    total = len(rows)
    if not total:
        print("RESULT fetch ok nothing pending")
        return 0

    throttle, stop = Throttle(rps), threading.Event()
    counts, received, interrupted = Counter(), 0, False
    started = last_note = time.monotonic()
    removed_hashes = {sha for (sha,) in con.execute("SELECT sha256 FROM removed WHERE sha256 IS NOT NULL")}
    week_of_gist = {row[0]: row[2] for row in rows}
    queue = iter(rows)

    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
        in_flight: set[cf.Future] = set()

        def top_up() -> None:
            """Keep a short queue ahead of the workers, so a stop takes effect quickly."""
            while len(in_flight) < workers * 2 and not stop.is_set():
                row = next(queue, None)
                if row is None:
                    return
                in_flight.add(pool.submit(fetch_one, row, staging, throttle, stop, max_mb << 20))

        try:
            top_up()
            while in_flight:
                done, in_flight = cf.wait(in_flight, return_when=cf.FIRST_COMPLETED)
                for future in done:
                    result = future.result()
                    if result.state is None:
                        continue
                    staged = staged_path(staging, week_of_gist[result.id], result.id)
                    result = drop_if_removed(con, staged, result, removed_hashes)
                    record(con, result)
                    counts[result.state] += 1
                    received += result.bytes or 0
                top_up()
                # Committed every time round: an open write would hold up an
                # index run sharing the database.
                con.commit()
                if time.monotonic() - last_note >= 10:
                    last_note = time.monotonic()
                    finished = sum(counts.values())
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
