"""sync: keep two small tables in Supabase.

`logs` has one row per packed log: its id, upload time and parsed facts, never
its text. It is small enough for a free project and gives a place to look up
one log without downloading anything.

`removed_logs` is the list of logs taken out on request, and this is its home.
The local index can be lost and rebuilt from nothing, and a rebuilt index must
still refuse those logs, so the list is merged both ways before anything is
listed: removals made elsewhere are learned, and removals made here are sent.

Both tables are reached through Supabase's REST interface with the project's
secret key, which has to travel on the `apikey` header.
"""

from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path

from .common import ConfigError, UploadError
from .index_db import open_db
from .layout import staged_path
from .settings import SUPABASE_KEYS, USER_AGENT, load_env, require

UPSERT = "resolution=merge-duplicates,return=minimal"
BATCH = 500
PAGE = 1000


def request(env: dict[str, str], method: str, path: str, body=None, prefer: str | None = None) -> bytes:
    """One REST call. Returns the response body."""
    require(env, SUPABASE_KEYS)
    headers = {"apikey": env["SUPABASE_SECRET_KEY"], "User-Agent": USER_AGENT}
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if prefer:
        headers["Prefer"] = prefer
    call = urllib.request.Request(env["SUPABASE_URL"].rstrip("/") + path, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(call, timeout=60) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read()[:300].decode("utf-8", "replace")
        if exc.code == 404 and "PGRST205" in detail:
            raise ConfigError("a Supabase table is missing: apply schema.sql in the project's SQL editor") from exc
        raise UploadError(f"Supabase answered HTTP {exc.code}: {detail}") from exc


def sync_logs(con: sqlite3.Connection, env: dict[str, str]) -> int:
    """Upsert every packed log the `logs` table lacks. Returns how many were sent."""
    synced = 0
    while True:
        rows = con.execute(
            "SELECT g.id, g.log_no, g.created_at, g.size, g.uploader, g.week, g.shard, g.sha256,"
            " f.game_version, f.game_rev, f.mod_count, f.dlcs, f.trimmed, f.line_count"
            " FROM gists g JOIN facts f ON f.id = g.id WHERE g.state = 'packed' AND g.synced = 0 LIMIT ?",
            (BATCH,),
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
        request(env, "POST", "/rest/v1/logs?on_conflict=id", body, UPSERT)
        con.executemany("UPDATE gists SET synced = 1 WHERE id = ?", [(row[0],) for row in rows])
        con.commit()
        synced += len(rows)


def remote_removed(env: dict[str, str]) -> dict[str, dict]:
    """The whole `removed_logs` table, by gist id."""
    listed, offset = {}, 0
    while True:
        page = json.loads(
            request(
                env, "GET", f"/rest/v1/removed_logs?select=id,sha256,removed_at&order=id&limit={PAGE}&offset={offset}"
            )
        )
        listed.update({row["id"]: row for row in page})
        if len(page) < PAGE:
            return listed
        offset += PAGE


def sync_removed(con: sqlite3.Connection, env: dict[str, str], data: Path) -> tuple[int, int]:
    """Merge the removed list with its copy in Supabase, both ways.

    Returns how many removals this index learned and how many it sent up. A
    log learned this way is marked removed here and its staged copy deleted;
    whoever removed it has already rewritten the files in the bucket.
    """
    remote = remote_removed(env)
    local = {row[0]: row for row in con.execute("SELECT id, removed_at, sha256 FROM removed")}
    learned = [row for gist_id, row in remote.items() if gist_id not in local]
    for row in learned:
        staged = con.execute("SELECT week FROM gists WHERE id = ? AND state = 'fetched'", (row["id"],)).fetchone()
        if staged:
            staged_path(data / "staging", staged[0], row["id"]).unlink(missing_ok=True)
        con.execute(
            "INSERT OR IGNORE INTO removed (id, removed_at, sha256) VALUES (?, ?, ?)",
            (row["id"], row["removed_at"], row.get("sha256")),
        )
        con.execute("UPDATE gists SET state = 'removed', synced = 0 WHERE id = ? AND state != 'removed'", (row["id"],))
    con.commit()
    unsent = [
        {"id": gist_id, "removed_at": row[1], "sha256": row[2]}
        for gist_id, row in local.items()
        if gist_id not in remote
    ]
    for start in range(0, len(unsent), BATCH):
        request(env, "POST", "/rest/v1/removed_logs?on_conflict=id", unsent[start : start + BATCH], UPSERT)
    return len(learned), len(unsent)


def sync(data: Path) -> int:
    """Send every packed log not yet in the `logs` table."""
    con = open_db(data)
    print(f"RESULT sync ok rows={sync_logs(con, load_env())}")
    return 0
