"""push: upload a packed part to the R2 bucket and drop the local copy.

The machine that builds the archive has no room to keep it, so it is a
pass-through. A part's five files go to the bucket, each stored size is
checked against the file, and only then are the local copies deleted.

The bucket is S3-compatible. A bucket created in R2's EU jurisdiction is
served only from the `.eu.` endpoint host; through the default host every call
to it returns "access denied".

Only one index may build the archive. Log numbers, patch ids and part names
all come from the index, so a second index, whether new or an old copy, would
hand out the same ones again and write its parts over the first one's. Two
checks stop that. Before anything is written, `ensure_current` compares the
bucket's parts with the index's and refuses an index that is behind. And a
push never replaces an object that is not the file it packed.
"""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .common import OutOfDate, UploadError, now_iso, say
from .index_db import open_db
from .layout import LOGS, STATE_KEY, bucket_key, part_files
from .settings import R2_KEYS, load_env, require

MISSING = ("404", "NoSuchKey", "NotFound")


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


def stored_size(client, bucket: str, key: str) -> int | None:
    """The size of an object in the bucket, or None when the bucket has no such key."""
    try:
        return client.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in MISSING:
            return None
        raise


def ensure_current(con: sqlite3.Connection, client, bucket: str) -> None:
    """Stop with OutOfDate if the bucket holds parts this index has no record of.

    That is the sign of a second index: a new one, or a copy left behind when
    the work moved to another machine. Run before anything is written.
    """
    known = {shard for (shard,) in con.execute("SELECT shard FROM parts")}
    prefix = bucket_key(LOGS, "")
    unknown = sorted(
        item["Key"][len(prefix) :]
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
        if item["Key"][len(prefix) :] not in known
    )
    if unknown:
        raise OutOfDate(
            f"the bucket holds {len(unknown)} parts this index has no record of, the newest {unknown[-1]}."
            " Another machine is building the archive, or this index is an old copy."
            f" Nothing was written. Use the current index: its last backup is {STATE_KEY} in the bucket."
        )


def upload_checked(client, bucket: str, path: Path, key: str) -> int:
    """Upload one file, replacing any object under its key, and confirm the bucket stored all of it.

    Returns its size.
    """
    size = path.stat().st_size
    client.upload_file(str(path), bucket, key)
    stored = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if stored != size:
        raise UploadError(f"{key}: the bucket holds {stored} bytes, the file has {size}")
    return size


def already_stored(client, bucket: str, path: Path, key: str) -> bool:
    """True when the bucket already holds this file, as it does after a push that was cut short.

    An object of another size under the same key is not the file this index
    packed, so it is refused and not replaced.
    """
    stored = stored_size(client, bucket, key)
    if stored is None:
        return False
    size = path.stat().st_size
    if stored != size:
        raise UploadError(
            f"the bucket already holds {key} ({stored} bytes) and it is not the file this index packed"
            f" ({size} bytes). Another index has written this part. Nothing was replaced."
        )
    return True


def push_parts(con: sqlite3.Connection, data: Path, client, bucket: str, keep_local: bool) -> tuple[int, int]:
    """Upload every packed part the bucket lacks, then drop the local copy.

    Returns how many parts went up and how many bytes.
    """
    pushed = sent = 0
    for (shard,) in con.execute("SELECT shard FROM parts WHERE uploaded_at IS NULL ORDER BY shard").fetchall():
        files = part_files(data, shard)
        # All five keys are checked before any file is sent, so a part is refused whole.
        to_send = {key: path for key, path in files.items() if not already_stored(client, bucket, path, key)}
        for key, path in to_send.items():
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
    ensure_current(con, client, env["R2_BUCKET"])
    pushed, sent = push_parts(con, data, client, env["R2_BUCKET"], keep_local)
    if state:
        backup_state(con, data, client, env["R2_BUCKET"])
    print(f"RESULT push ok parts={pushed} mb={sent / 1e6:.1f}")
    return 0
