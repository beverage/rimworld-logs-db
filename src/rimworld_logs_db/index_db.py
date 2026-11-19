"""The local index: one SQLite file that every step reads and updates.

It is working state, not the archive. It records each gist and how far it has
got, the number given to each Harmony patch, which parts have been uploaded,
and a copy of the removed list. A step stopped at any point resumes from it.

A gist moves through these states:

    pending -> fetched -> packed
    or ends in one of: too_large, gone, error, removed
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

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

# Columns added after the first index files were written.
ADDED_COLUMNS = (
    ("gists", "log_no", "INTEGER"),
    ("gists", "synced", "INTEGER NOT NULL DEFAULT 0"),
    ("removed", "sha256", "TEXT"),
)


def open_db(data: Path) -> sqlite3.Connection:
    """Open the index in a data folder, creating it and bringing it up to date."""
    data.mkdir(parents=True, exist_ok=True)
    # The long wait lets an index run and a week run share the database.
    con = sqlite3.connect(data / "index.sqlite", timeout=600)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(SCHEMA)
    for table, column, kind in ADDED_COLUMNS:
        if column not in {row[1] for row in con.execute(f"PRAGMA table_info({table})")}:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {kind}")
    return con
