"""Taking a log out of the archive on request: on disk, in the bucket, and for good."""

import pyarrow.parquet as pq
import pytest

from rimworld_logs_db import bucket, listing, removal, supabase
from rimworld_logs_db.layout import STATE_KEY, bucket_key

GIST = "0123456789abcdef0123456789abcdef"
ENTRY = {"size": 10, "raw_url": "https://raw.test/log"}


def column(path, name: str) -> list:
    return pq.read_table(path, columns=[name]).column(name).to_pylist()


def listed(gist_id: str) -> dict:
    """A gist as GitHub's list endpoint returns it."""
    return {"id": gist_id, "created_at": "2026-09-30T10:00:00Z", "description": None}


@pytest.mark.parametrize(
    "text",
    [
        f"https://gist.github.com/HugsLibRecordKeeper/{GIST}",
        f"https://gist.github.com/{GIST}#file-output_log-txt",
        f"https://gist.githubusercontent.com/HugsLibRecordKeeper/{GIST}/raw/0f0f0f/output_log.txt",
        f"  {GIST}\n",
    ],
    ids=["gist link", "link without the account", "raw log link", "bare id"],
)
def test_the_gist_id_is_read_from_a_link_or_a_bare_id(text):
    assert removal.gist_id_of(text) == GIST


@pytest.mark.parametrize("text", ["https://example.com/some/page", "not an id", ""])
def test_anything_else_names_no_gist(text):
    assert removal.gist_id_of(text) is None


def test_a_request_naming_no_gist_is_refused(data):
    assert removal.remove(data, ["https://example.com/some/page"]) == 1


def test_a_dry_run_changes_nothing(archive):
    before = {table: path.read_bytes() for table, path in archive.paths.items()}
    assert removal.remove(archive.data, [archive.ids.A], dry_run=True, level=3) == 0
    assert {table: path.read_bytes() for table, path in archive.paths.items()} == before
    assert archive.state(archive.ids.A) == "packed"
    assert archive.con.execute("SELECT COUNT(*) FROM removed").fetchone()[0] == 0
    assert (archive.supabase.deletes, archive.supabase.removed) == ([], {})
    assert list(archive.data.rglob("*.new")) == []
    assert not (archive.data / "removal").exists()


@pytest.fixture
def without_a(archive):
    """The archive after log A was removed from the part on disk."""
    archive.log_a = archive.log_no(archive.ids.A)
    archive.dictionary = {
        table: archive.paths[table].read_bytes() for table in ("HarmonyPatches", "HarmonyPatchNamespaces")
    }
    assert removal.remove(archive.data, [archive.ids.A], level=3) == 0
    return archive


def test_a_removed_log_is_gone_from_the_log_file(without_a):
    ids = without_a.ids
    assert column(without_a.paths["logs"], "id") == [ids.B, ids.C, ids.D, ids.E, ids.F]


def test_a_removed_log_is_gone_from_the_patch_tables(without_a):
    assert without_a.log_a not in column(without_a.paths["HugslibLogs"], "HugslibLogId")
    active = column(without_a.paths["ActivePatches"], "HugslibLogId")
    assert without_a.log_a not in active
    assert len(active) == 15


def test_the_logs_left_behind_are_unchanged(without_a):
    rows = pq.read_table(without_a.paths["logs"], columns=["id", "text"]).to_pylist()
    kept = {gist_id: text for gist_id, text in without_a.texts.items() if gist_id != without_a.ids.A}
    assert {row["id"]: row["text"] for row in rows} == kept


def test_the_patch_dictionary_is_not_rewritten(without_a):
    for table, before in without_a.dictionary.items():
        assert without_a.paths[table].read_bytes() == before


def test_the_index_records_the_removal(without_a):
    con, removed = without_a.con, without_a.ids.A
    assert without_a.state(removed) == "removed"
    assert con.execute("SELECT sha256 FROM removed WHERE id = ?", (removed,)).fetchone() == (without_a.hashes[removed],)
    assert con.execute("SELECT COUNT(*) FROM facts WHERE id = ?", (removed,)).fetchone()[0] == 0
    assert con.execute("SELECT logs FROM parts").fetchone()[0] == 5


def test_supabase_loses_the_row_and_gains_a_removed_entry(without_a):
    removed = without_a.ids.A
    assert without_a.supabase.deletes == [(f"/rest/v1/logs?id=eq.{removed}", "test-key")]
    assert without_a.supabase.removed[removed]["sha256"] == without_a.hashes[removed]


def test_a_removed_log_is_not_indexed_again(without_a):
    assert listing.add_gist(without_a.con, listed(without_a.ids.A), ENTRY) == 0
    assert without_a.state(without_a.ids.A) == "removed"


def test_another_upload_of_the_same_log_is_left_by_default(archive):
    assert removal.remove(archive.data, [archive.ids.B], level=3) == 0
    assert (archive.state(archive.ids.B), archive.state(archive.ids.C)) == ("removed", "packed")
    assert archive.ids.C in column(archive.paths["logs"], "id")


def test_same_content_removes_every_upload_of_the_log(archive):
    ids = archive.ids
    assert removal.remove(archive.data, [ids.D], same_content=True, level=3) == 0
    assert (archive.state(ids.D), archive.state(ids.E)) == ("removed", "removed")
    assert column(archive.paths["logs"], "id") == [ids.A, ids.B, ids.C, ids.F]
    assert set(archive.supabase.removed) == {ids.D, ids.E}


def test_a_log_still_in_staging_loses_its_staged_copy(data, con, stage_log, fake_supabase):
    staged = stage_log(GIST, "2026-09-30T10:00:00Z", b"downloaded and not yet packed")
    assert removal.remove(data, [GIST]) == 0
    assert not staged.exists()
    assert con.execute("SELECT state FROM gists WHERE id = ?", (GIST,)).fetchone() == ("removed",)
    assert GIST in fake_supabase.removed


def test_a_log_can_be_removed_before_it_was_ever_listed(data, con, fake_supabase):
    assert removal.remove(data, [GIST], no_supabase=True) == 0
    assert listing.add_gist(con, listed(GIST), ENTRY) == 0
    assert listing.add_gist(con, listed("9" * 32), ENTRY) == 1
    con.commit()
    assert fake_supabase.removed == {}


def test_a_removal_made_without_supabase_is_sent_at_the_next_sync(data, con, fake_supabase):
    removal.remove(data, [GIST], no_supabase=True)
    assert supabase.sync_removed(con, fake_supabase.env, data) == (0, 1)
    assert GIST in fake_supabase.removed


@pytest.fixture
def uploaded(archive, folder_bucket):
    """The archive after its part was pushed to the stand-in bucket."""
    pushed, _ = bucket.push_parts(archive.con, archive.data, folder_bucket, "stand-in", keep_local=False)
    assert pushed == 1
    archive.bucket = folder_bucket.root
    return archive


def test_a_pushed_part_leaves_nothing_on_disk(uploaded):
    folders = [uploaded.data / name for name in ("staging", "shards", "patches")]
    assert [path for folder in folders for path in folder.rglob("*") if path.is_file()] == []
    assert all((uploaded.bucket / bucket_key(table, uploaded.shard)).is_file() for table in uploaded.paths)


def test_removal_rewrites_the_files_in_the_bucket(uploaded):
    ids, shard = uploaded.ids, uploaded.shard
    gone = {uploaded.log_no(ids.D), uploaded.log_no(ids.E)}
    assert removal.remove(uploaded.data, [ids.D], same_content=True, level=3) == 0
    assert column(uploaded.bucket / bucket_key("logs", shard), "id") == [ids.A, ids.B, ids.C, ids.F]
    assert gone.isdisjoint(column(uploaded.bucket / bucket_key("HugslibLogs", shard), "HugslibLogId"))
    active = column(uploaded.bucket / bucket_key("ActivePatches", shard), "HugslibLogId")
    assert gone.isdisjoint(active)
    assert len(active) == 12


def test_removal_from_the_bucket_backs_up_the_index_and_cleans_up(uploaded):
    assert removal.remove(uploaded.data, [uploaded.ids.A], level=3) == 0
    assert (uploaded.bucket / STATE_KEY).is_file()
    assert not (uploaded.data / "removal").exists()
    assert not (uploaded.data / "index.backup.sqlite").exists()
