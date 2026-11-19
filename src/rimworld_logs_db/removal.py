"""remove: take one log out of the archive on request.

Anyone can ask for their log to be removed by sending its gist link. Removal
has to reach everywhere the log is, and has to last:

1. The week's log file, HugslibLogs and ActivePatches are rewritten without
   the log and uploaded over the old files.
2. Its row is deleted from the Supabase `logs` table.
3. Its id and content hash go on the removed list, in Supabase and in the
   local index. Listing refuses that id from then on, and a download with
   that content is discarded, which covers the same log uploaded twice.
4. A fresh copy of the index goes to the bucket.

The patch dictionary is not rewritten. It names mods' patch methods, which
say nothing about a player, and the numbers it gives out must not move.
"""

from __future__ import annotations

import contextlib
import os
import re
import sqlite3
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import bucket, supabase
from .common import now_iso, say
from .index_db import open_db
from .layout import LOGS, bucket_key, part_paths, staged_path
from .packing import ACTIVE_PATCHES_ENCODING
from .settings import load_env

GIST_LINK = re.compile(r"gist\.github(?:usercontent)?\.com/(?:[^/\s]+/)?([0-9a-f]{20,40})(?:[/?#.]|$)")
GIST_ID = re.compile(r"[0-9a-f]{20,40}")


def gist_id_of(text: str) -> str | None:
    """The gist id in a gist link, a raw log link, or a bare id."""
    text = text.strip()
    link = GIST_LINK.search(text)
    if link:
        return link[1]
    return text if GIST_ID.fullmatch(text) else None


def rewrite_without(source: Path, dest: Path, column: str, value, **options) -> tuple[int, int]:
    """Copy a Parquet file, leaving out rows where column equals value. Returns rows kept and dropped."""
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


class Removal:
    """One run of `remove`: the connections it opens, and what it has touched."""

    def __init__(self, con: sqlite3.Connection, data: Path, dry_run: bool, no_supabase: bool, level: int) -> None:
        self.con, self.data = con, data
        self.dry_run, self.no_supabase, self.level = dry_run, no_supabase, level
        self.env: dict[str, str] | None = None
        self.client = None
        self.removed = 0

    def settings(self) -> dict[str, str]:
        """The env file, read the first time something needs a key."""
        if self.env is None:
            self.env = load_env()
        return self.env

    def twins(self, gist_id: str, queued: list[str]) -> list[str]:
        """Other uploads of the same log, by content hash, not already queued for removal."""
        row = self.con.execute("SELECT sha256 FROM gists WHERE id = ?", (gist_id,)).fetchone()
        if not row or not row[0]:
            return []
        same = self.con.execute(
            "SELECT id FROM gists WHERE sha256 = ? AND id != ? AND state != 'removed'", (row[0], gist_id)
        )
        return [twin for (twin,) in same if twin not in queued]

    def remove(self, gist_id: str) -> None:
        """Take one log out of wherever it has got to: staging, a packed part, or nowhere yet."""
        row = self.con.execute(
            "SELECT state, week, shard, log_no, sha256 FROM gists WHERE id = ?", (gist_id,)
        ).fetchone()
        state, week, shard, log_no, sha256 = row or (None, None, None, None, None)
        say(f"{gist_id}: {state or 'not in the index'}")
        if state == "packed":
            self.rewrite_part(gist_id, shard, log_no)
            if not self.dry_run and not self.no_supabase:
                supabase.request(self.settings(), "DELETE", f"/rest/v1/logs?id=eq.{gist_id}")
        elif state == "fetched" and not self.dry_run:
            staged_path(self.data / "staging", week, gist_id).unlink(missing_ok=True)
        if not self.dry_run:
            self.record(gist_id, sha256, was_packed=state == "packed", shard=shard)

    def rewrite_part(self, gist_id: str, shard: str, log_no: int) -> None:
        """Rewrite the three files of a part that hold the log's rows, in the bucket or on disk."""
        uploaded = self.con.execute("SELECT uploaded_at FROM parts WHERE shard = ?", (shard,)).fetchone()
        in_bucket = bool(uploaded and uploaded[0])
        if in_bucket and self.client is None:
            self.client = bucket.r2_client(self.settings())
        local = part_paths(self.data, shard)
        targets = {
            LOGS: ("id", gist_id, {}),
            "HugslibLogs": ("HugslibLogId", log_no, {}),
            "ActivePatches": ("HugslibLogId", log_no, ACTIVE_PATCHES_ENCODING),
        }
        work = self.data / "removal"
        work.mkdir(parents=True, exist_ok=True)
        for table, (column, value, options) in targets.items():
            key = bucket_key(table, shard)
            source = work / key.replace("/", "_") if in_bucket else local[table]
            rewritten = source.with_name(source.name + ".new")
            if in_bucket:
                self.client.download_file(self.settings()["R2_BUCKET"], key, str(source))
            kept, dropped = rewrite_without(source, rewritten, column, value, compression_level=self.level, **options)
            say(f"  {key}: {dropped} rows dropped, {kept} kept")
            if self.dry_run:
                rewritten.unlink()
            elif in_bucket:
                bucket.upload_checked(self.client, self.settings()["R2_BUCKET"], rewritten, key)
                rewritten.unlink()
            else:
                os.replace(rewritten, source)
            if in_bucket:
                source.unlink()
        with contextlib.suppress(OSError):
            work.rmdir()

    def record(self, gist_id: str, sha256: str | None, was_packed: bool, shard: str | None) -> None:
        """Put the log on the removed list, here and in Supabase, and mark it in the index."""
        entry = {"id": gist_id, "removed_at": now_iso(), "sha256": sha256}
        if not self.no_supabase:
            supabase.request(self.settings(), "POST", "/rest/v1/removed_logs?on_conflict=id", [entry], supabase.UPSERT)
        con = self.con
        con.execute(
            "INSERT OR REPLACE INTO removed (id, removed_at, sha256) VALUES (?, ?, ?)",
            (gist_id, entry["removed_at"], sha256),
        )
        con.execute("UPDATE gists SET state = 'removed', synced = 0, error = NULL WHERE id = ?", (gist_id,))
        con.execute("DELETE FROM facts WHERE id = ?", (gist_id,))
        if was_packed:
            con.execute("UPDATE parts SET logs = logs - 1 WHERE shard = ?", (shard,))
        con.commit()
        self.removed += 1

    def finish(self) -> None:
        """Back the index up if the bucket was changed, so the copy there matches."""
        if self.client is not None and not self.dry_run:
            bucket.backup_state(self.con, self.data, self.client, self.settings()["R2_BUCKET"])


def remove(
    data: Path,
    gists: list[str],
    dry_run: bool = False,
    same_content: bool = False,
    no_supabase: bool = False,
    level: int = 9,
) -> int:
    """Remove logs named by gist link or id.

    `dry_run` reports what would be dropped and changes nothing. `same_content`
    also removes other uploads of an identical log. `level` is the zstd level
    of the rewritten files.
    """
    ids = [gist_id_of(item) for item in gists]
    if None in ids:
        say("not a gist link or id: " + gists[ids.index(None)])
        return 1
    job = Removal(open_db(data), data, dry_run, no_supabase, level)
    queue = list(dict.fromkeys(ids))
    while queue:
        gist_id = queue.pop(0)
        twins = job.twins(gist_id, queue)
        if twins and same_content:
            queue += twins
        elif twins:
            say(f"  {len(twins)} other uploads hold the same content; --same-content removes them as well")
        job.remove(gist_id)
    job.finish()
    print(f"RESULT remove {'dry run' if dry_run else 'ok'} asked={len(ids)} removed={job.removed}")
    return 0
