"""run: the whole pipeline, one week at a time, newest first. Also status.

For each week: download its logs, pack them, upload the part, record its rows
in Supabase. At most one week of logs is on disk at a time, and a run stopped
at any point picks up where it left off, because every step reads its work
from the index.

The newest week with work left is looked up afresh each time round, so weeks
that a listing adds while a run is under way are picked up. The week still in
progress is left alone: uploads are still arriving for it.
"""

from __future__ import annotations

from pathlib import Path

from . import bucket, download, packing, supabase
from .common import now_iso, say, week_of
from .index_db import open_db
from .settings import load_env


def next_week(con) -> tuple[str, int] | None:
    """The newest finished week with logs still to download or pack, and how many are waiting."""
    return con.execute(
        "SELECT week, SUM(state = 'pending') FROM gists WHERE state IN ('pending', 'fetched') AND week != ?"
        " GROUP BY week ORDER BY week DESC LIMIT 1",
        (week_of(now_iso()),),
    ).fetchone()


def run(
    data: Path,
    weeks: int = 0,
    workers: int = 4,
    rps: float = 6.0,
    max_mb: int = 64,
    level: int = 9,
    no_supabase: bool = False,
    backup_every: int = 8,
) -> int:
    """Process weeks until none is left, or until `weeks` of them are done.

    Returns 0 when it ran to the end, and otherwise the code of the step that
    stopped it: 2 means the raw host kept refusing downloads.
    """
    env = load_env()
    client = bucket.r2_client(env)
    bucket_name = env["R2_BUCKET"]
    client.head_bucket(Bucket=bucket_name)
    if not no_supabase:
        supabase.request(env, "GET", "/rest/v1/logs?select=id&limit=1")
    con = open_db(data)
    if not no_supabase:
        supabase.sync_removed(con, env, data)
    # A part packed by an earlier run that stopped before uploading it.
    bucket.push_parts(con, data, client, bucket_name, keep_local=False)

    finished = pushed = synced = 0
    while not weeks or finished < weeks:
        newest = next_week(con)
        if newest is None:
            break
        week, waiting = newest
        say(f"week {week}: {waiting} logs to fetch")
        code = download.fetch(data, week=week, workers=workers, rps=rps, max_mb=max_mb)
        if code == 0:
            code = packing.pack(data, week=week, level=level)
        if code != 0:
            print(f"RESULT run stopped week={week} weeks_done={finished} parts={pushed} synced={synced}")
            return code
        pushed += bucket.push_parts(con, data, client, bucket_name, keep_local=False)[0]
        if not no_supabase:
            synced += supabase.sync_logs(con, env)
        finished += 1
        if finished % backup_every == 0:
            bucket.backup_state(con, data, client, bucket_name)
    bucket.backup_state(con, data, client, bucket_name)
    print(f"RESULT run ok weeks_done={finished} parts={pushed} synced={synced}")
    return 0


def tree_bytes(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file()) if root.exists() else 0


def status(data: Path) -> int:
    """Print where every log stands and what is waiting to upload or sync."""
    con = open_db(data)
    oldest, newest = con.execute("SELECT MIN(created_at), MAX(created_at) FROM gists").fetchone()
    print(f"data     {data}")
    print(f"span     {oldest} to {newest}")
    for state, count, size in con.execute(
        "SELECT state, COUNT(*), COALESCE(SUM(size), 0) FROM gists GROUP BY state ORDER BY state"
    ):
        print(f"{state:<9}{count:>8} logs {size / 1e9:>8.2f} GB as text")
    for folder in ("staging", "shards", "patches"):
        print(f"{folder:<9}{tree_bytes(data / folder) / 1e9:.2f} GB on disk")
    distinct = con.execute("SELECT COUNT(*) FROM patch_ids").fetchone()[0]
    namespaces = con.execute("SELECT COUNT(*) FROM namespace_ids").fetchone()[0]
    print(f"distinct patches {distinct}, namespaces {namespaces}")
    waiting = con.execute("SELECT COUNT(*) FROM parts WHERE uploaded_at IS NULL").fetchone()[0]
    uploaded = con.execute("SELECT COUNT(*) FROM parts WHERE uploaded_at IS NOT NULL").fetchone()[0]
    unsynced = con.execute("SELECT COUNT(*) FROM gists WHERE state = 'packed' AND synced = 0").fetchone()[0]
    print(f"parts    {uploaded} in the bucket, {waiting} waiting to upload; {unsynced} packed logs not yet in Supabase")
    return 0
