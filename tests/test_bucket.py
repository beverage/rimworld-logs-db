"""Uploading parts, and refusing to write over another index's work."""

import pytest

from rimworld_logs_db import bucket, cli, removal
from rimworld_logs_db.common import OutOfDate, UploadError
from rimworld_logs_db.layout import STATE_KEY, bucket_key

BUCKET = "stand-in"
THEIRS = b"a part packed from another index"


def put(folder_bucket, key: str, content: bytes) -> None:
    """Place an object in the stand-in bucket, as another machine would have."""
    path = folder_bucket.root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def objects(folder_bucket) -> list[str]:
    return sorted(
        path.relative_to(folder_bucket.root).as_posix() for path in folder_bucket.root.rglob("*") if path.is_file()
    )


def unpushed(con) -> int:
    return con.execute("SELECT COUNT(*) FROM parts WHERE uploaded_at IS NULL").fetchone()[0]


def test_a_push_never_replaces_a_part_another_index_wrote(archive, folder_bucket):
    # The third of the five files, so the check has to run before the first two are sent.
    taken = bucket_key("ActivePatches", archive.shard)
    put(folder_bucket, taken, THEIRS)
    with pytest.raises(UploadError, match="not the file this index packed"):
        bucket.push_parts(archive.con, archive.data, folder_bucket, BUCKET, keep_local=False)
    assert objects(folder_bucket) == [taken]
    assert (folder_bucket.root / taken).read_bytes() == THEIRS
    assert all(path.is_file() for path in archive.paths.values())
    assert unpushed(archive.con) == 1


def test_a_push_that_was_cut_short_picks_up_where_it_stopped(archive, folder_bucket):
    sizes = {table: path.stat().st_size for table, path in archive.paths.items()}
    put(folder_bucket, bucket_key("logs", archive.shard), archive.paths["logs"].read_bytes())
    pushed, sent = bucket.push_parts(archive.con, archive.data, folder_bucket, BUCKET, keep_local=False)
    assert (pushed, sent) == (1, sum(sizes.values()) - sizes["logs"])
    assert len(objects(folder_bucket)) == 5
    assert unpushed(archive.con) == 0


def test_an_index_that_knows_every_part_in_the_bucket_may_write(archive, folder_bucket):
    bucket.push_parts(archive.con, archive.data, folder_bucket, BUCKET, keep_local=False)
    put(folder_bucket, STATE_KEY, b"a backup of the index is not a part")
    bucket.ensure_current(archive.con, folder_bucket, BUCKET)


def test_an_index_behind_the_bucket_is_refused(con, folder_bucket):
    put(folder_bucket, "logs/week=2026-W40/part-000.parquet", THEIRS)
    put(folder_bucket, "logs/week=2026-W41/part-000.parquet", THEIRS)
    with pytest.raises(OutOfDate, match="holds 2 parts this index has no record of, the newest week=2026-W41/part-000"):
        bucket.ensure_current(con, folder_bucket, BUCKET)


def test_an_empty_bucket_refuses_nobody(con, folder_bucket):
    bucket.ensure_current(con, folder_bucket, BUCKET)


@pytest.mark.parametrize("command", [["run", "--no-supabase"], ["push", "--state"]])
def test_a_command_stops_before_writing_when_the_index_is_behind(command, data, fake_supabase, folder_bucket, capsys):
    put(folder_bucket, "logs/week=2026-W40/part-000.parquet", THEIRS)
    assert cli.main(["--data", str(data), *command]) == 1
    assert "this index has no record of" in capsys.readouterr().err
    # In particular the index was not backed up over the other machine's.
    assert objects(folder_bucket) == ["logs/week=2026-W40/part-000.parquet"]


def test_removal_is_refused_from_an_index_that_is_behind(archive, folder_bucket):
    bucket.push_parts(archive.con, archive.data, folder_bucket, BUCKET, keep_local=False)
    before = {key: (folder_bucket.root / key).read_bytes() for key in objects(folder_bucket)}
    put(folder_bucket, "logs/week=2026-W41/part-000.parquet", THEIRS)
    with pytest.raises(OutOfDate):
        removal.remove(archive.data, [archive.ids.A], level=3)
    assert archive.state(archive.ids.A) == "packed"
    assert {key: (folder_bucket.root / key).read_bytes() for key in before} == before
    assert STATE_KEY not in objects(folder_bucket)
    assert archive.supabase.deletes == []
