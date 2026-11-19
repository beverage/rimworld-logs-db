"""push: upload a packed part to the R2 bucket and drop the local copy.

The machine that builds the archive has no room to keep it, so it is a
pass-through. A part's five files go to the bucket, each stored size is
checked against the file, and only then are the local copies deleted.

The bucket is S3-compatible. A bucket created in R2's EU jurisdiction is
served only from the `.eu.` endpoint host; through the default host every call
to it returns "access denied".
"""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path

import boto3
from botocore.config import Config

from .common import UploadError, now_iso, say
from .index_db import open_db
from .layout import STATE_KEY, part_files
from .settings import R2_KEYS, load_env, require


def r2_client(env: dict[str, str]):
    """An S3 client for the R2 bucket named in the env file."""
    require(env, R2_KEYS)
    return boto3.client(
        "s3",
        endpoint_url=env["R2_ENDPOINT"],
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            # Send and check checksums only where the S3 API requires them.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def upload_checked(client, bucket: str, path: Path, key: str) -> int:
    """Upload one file and confirm the bucket stored all of it. Returns its size."""
    size = path.stat().st_size
    client.upload_file(str(path), bucket, key)
    stored = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if stored != size:
        raise UploadError(f"{key}: the bucket holds {stored} bytes, the file has {size}")
    return size


def push_parts(con: sqlite3.Connection, data: Path, client, bucket: str, keep_local: bool) -> tuple[int, int]:
    """Upload every packed part the bucket lacks, then drop the local copy.

    Returns how many parts went up and how many bytes.
    """
    pushed = sent = 0
    for (shard,) in con.execute("SELECT shard FROM parts WHERE uploaded_at IS NULL ORDER BY shard").fetchall():
        files = part_files(data, shard)
        for key, path in files.items():
            sent += upload_checked(client, bucket, path, key)
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
    client.upload_file(str(copy), bucket, STATE_KEY)
    copy.unlink()


def push(data: Path, keep_local: bool = False, state: bool = False) -> int:
    """Upload whatever is packed and not yet in the bucket. `state` also backs up the index."""
    env = load_env()
    client = r2_client(env)
    con = open_db(data)
    pushed, sent = push_parts(con, data, client, env["R2_BUCKET"], keep_local)
    if state:
        backup_state(con, data, client, env["R2_BUCKET"])
    print(f"RESULT push ok parts={pushed} mb={sent / 1e6:.1f}")
    return 0
