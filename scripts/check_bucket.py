"""Check what the bucket holds against the local index, reading the tables straight from R2.

Prints counts only: no keys, no log ids, no log text. Run it between weeks:
while a week is being fetched or packed, its files are on disk and the
"nothing is left on disk" check fails.

Run:  uv run python scripts/check_bucket.py [env file] [data folder]
"""

import os
import sqlite3
import sys
from pathlib import Path

import duckdb

from rimworld_logs_db import bucket, settings
from rimworld_logs_db.layout import LOGS, PATCH_TABLES, bucket_key

failed = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(("PASS " if ok else "FAIL ") + name + (f" ({detail})" if detail else ""))
    if not ok:
        failed.append(name)


def bucket_objects(s3, name: str) -> dict[str, int]:
    """Every key in the bucket and its size."""
    objects = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=name):
        for item in page.get("Contents", []):
            objects[item["Key"]] = item["Size"]
    return objects


def main() -> int:
    if len(sys.argv) > 1:
        os.environ[settings.ENV_VARIABLE] = sys.argv[1]
    env = settings.load_env()
    data = Path(sys.argv[2]).expanduser() if len(sys.argv) > 2 else settings.DEFAULT_DATA
    name = env["R2_BUCKET"]

    objects = bucket_objects(bucket.r2_client(env), name)
    groups = {}
    for key, size in objects.items():
        prefix = "/".join(key.split("/")[:2]) if key.startswith("patches/") else key.split("/")[0]
        count, total = groups.get(prefix, (0, 0))
        groups[prefix] = (count + 1, total + size)
    print("-- bucket contents (files, MB):", {k: (c, round(t / 1e6, 2)) for k, (c, t) in sorted(groups.items())})

    con = sqlite3.connect(data / "index.sqlite")
    parts = con.execute("SELECT shard, logs, uploaded_at FROM parts ORDER BY shard").fetchall()
    uploaded = [part for part in parts if part[2]]
    check("every packed part is marked uploaded", len(uploaded) == len(parts), f"{len(uploaded)} of {len(parts)}")
    missing = [
        key
        for shard, _, _ in uploaded
        for key in (bucket_key(table, shard) for table in (LOGS, *PATCH_TABLES))
        if key not in objects
    ]
    check("all five files of each uploaded part are in the bucket", not missing, f"{len(missing)} missing")
    left = [p for folder in ("staging", "shards", "patches") for p in (data / folder).rglob("*") if p.is_file()]
    check("nothing is left on disk but the index", not left, f"{len(left)} files")
    packed = con.execute("SELECT COUNT(*) FROM gists WHERE state = 'packed'").fetchone()[0]
    states = dict(
        con.execute(
            "SELECT state, COUNT(*) FROM gists"
            " WHERE week IN (SELECT DISTINCT week FROM gists WHERE state = 'packed') GROUP BY state"
        )
    )
    print("-- index, logs by state in the packed weeks:", states)

    db = duckdb.connect()
    db.execute("INSTALL httpfs")
    db.execute("LOAD httpfs")
    db.execute(
        "CREATE SECRET bucket_check (TYPE S3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [env["R2_ACCESS_KEY_ID"], env["R2_SECRET_ACCESS_KEY"], env["R2_ENDPOINT"].split("//", 1)[1].rstrip("/")],
    )
    logs = f"read_parquet('s3://{name}/logs/*/*.parquet')"
    for table in PATCH_TABLES:
        db.execute(f"create view {table} as select * from read_parquet('s3://{name}/patches/{table}/*/*.parquet')")

    count, first, last = db.execute(f"select count(*), min(created_at), max(created_at) from {logs}").fetchone()
    check("logs in the bucket equal packed logs in the index", count == packed, f"{count} vs {packed}")
    print(f"-- logs span {first} to {last}")
    print(
        "-- by game version (top 6):",
        db.execute(f"select game_version, count(*) from {logs} group by 1 order by 2 desc limit 6").fetchall(),
    )
    rows, with_patches = db.execute("select count(*), count(distinct HugslibLogId) from ActivePatches").fetchone()
    patches = db.execute("select count(*) from HarmonyPatches").fetchone()[0]
    namespaces = db.execute("select count(*) from HarmonyPatchNamespaces").fetchone()[0]
    print(f"-- ActivePatches {rows} rows over {with_patches} logs; {patches} distinct patches; {namespaces} namespaces")
    check("HugslibLogs matches the log files", db.execute("select count(*) from HugslibLogs").fetchone()[0] == count)
    joined = db.execute(
        "select count(*) from HugslibLogs join ActivePatches using (HugslibLogId)"
        " join HarmonyPatches using (HarmonyPatchId) join HarmonyPatchNamespaces using (HarmonyPatchNamespaceId)"
    ).fetchone()[0]
    check("every ActivePatches row joins through all four tables", joined == rows, f"{joined} of {rows}")
    check(
        "log ids agree between the log files and HugslibLogs",
        db.execute(
            f"select count(*) from {logs} l join HugslibLogs h on h.HugslibLogId = l.hugslib_log_id and h.GUID = l.id"
        ).fetchone()[0]
        == count,
    )
    active_bytes = sum(size for key, size in objects.items() if key.startswith("patches/ActivePatches/"))
    print(f"-- ActivePatches {active_bytes / max(rows, 1):.2f} bytes a row in the bucket")
    print(f"RESULT bucket {'ok' if not failed else 'failed'} failed={len(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
