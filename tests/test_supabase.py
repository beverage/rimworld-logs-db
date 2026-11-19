"""Keeping the two Supabase tables in step with the index."""

import pytest

from rimworld_logs_db import listing, supabase
from rimworld_logs_db.common import ConfigError, UploadError

SHARD = "week=2026-W46/part-000.parquet"
REMOVED_AT = "2026-11-19T00:00:00Z"


def add_packed(con, count: int) -> None:
    """Put `count` packed logs in the index, g0000 upwards, as `pack` leaves them."""
    for number in range(count):
        gist_id = f"g{number:04d}"
        con.execute(
            "INSERT INTO gists (id, created_at, week, size, raw_url, uploader, state, sha256, shard, log_no)"
            " VALUES (?, '2026-11-10T08:00:00Z', '2026-W46', 1000, 'u', 'standalone', 'packed', ?, ?, ?)",
            (gist_id, f"hash-{number}", SHARD, number + 1),
        )
        con.execute("INSERT INTO facts VALUES (?, '1.6.4633', 77, 12, 'Biotech,Royalty', 1, 4321, 1)", (gist_id,))
    con.commit()


def removed_rows(*gist_ids: str) -> dict[str, dict]:
    """Rows of the `removed_logs` table, as another machine's removals would leave them."""
    return {gist_id: {"id": gist_id, "sha256": f"hash-of-{gist_id}", "removed_at": REMOVED_AT} for gist_id in gist_ids}


def test_packed_logs_are_sent_in_batches(con, fake_supabase):
    add_packed(con, 502)
    assert supabase.sync_logs(con, fake_supabase.env) == 502
    assert [len(rows) for _, _, rows in fake_supabase.posts] == [500, 2]


def test_the_upsert_carries_the_key_on_the_apikey_header(con, fake_supabase):
    add_packed(con, 1)
    supabase.sync_logs(con, fake_supabase.env)
    path, headers, _ = fake_supabase.posts[0]
    assert path == "/rest/v1/logs?on_conflict=id"
    assert headers["apikey"] == "test-key"
    assert headers["prefer"] == supabase.UPSERT
    assert "authorization" not in headers


def test_a_row_holds_the_logs_facts_and_not_its_text(con, fake_supabase):
    add_packed(con, 2)
    supabase.sync_logs(con, fake_supabase.env)
    rows = {row["id"]: row for _, _, batch in fake_supabase.posts for row in batch}
    assert rows["g0001"] == {
        "id": "g0001",
        "hugslib_log_id": 2,
        "created_at": "2026-11-10T08:00:00Z",
        "size_bytes": 1000,
        "uploader": "standalone",
        "week": "2026-W46",
        "shard": SHARD,
        "sha256": "hash-1",
        "game_version": "1.6.4633",
        "game_rev": 77,
        "mod_count": 12,
        "dlcs": ["Biotech", "Royalty"],
        "trimmed": True,
        "line_count": 4321,
    }


def test_a_log_is_sent_once(con, fake_supabase):
    add_packed(con, 3)
    assert supabase.sync_logs(con, fake_supabase.env) == 3
    assert supabase.sync_logs(con, fake_supabase.env) == 0


def test_a_missing_table_is_a_configuration_error(con, fake_supabase):
    add_packed(con, 1)
    fake_supabase.reply = (404, b'{"code":"PGRST205","message":"Could not find the table"}')
    with pytest.raises(ConfigError, match="supabase-schema.sql"):
        supabase.sync_logs(con, fake_supabase.env)


def test_a_refused_batch_stays_unsent(con, fake_supabase):
    add_packed(con, 3)
    fake_supabase.reply = (500, b"boom")
    with pytest.raises(UploadError, match="HTTP 500"):
        supabase.sync_logs(con, fake_supabase.env)
    assert con.execute("SELECT COUNT(*) FROM gists WHERE synced = 0").fetchone()[0] == 3


def test_the_removed_list_is_read_in_pages(fake_supabase):
    fake_supabase.removed.update(removed_rows(*(f"{number:032x}" for number in range(1005))))
    assert len(supabase.remote_removed(fake_supabase.env)) == 1005
    assert len(fake_supabase.gets) == 2


def test_a_removal_made_here_is_sent_up_once(con, data, fake_supabase):
    con.execute("INSERT INTO removed (id, removed_at, sha256) VALUES (?, ?, 'the-hash')", ("a" * 32, REMOVED_AT))
    con.commit()
    assert supabase.sync_removed(con, fake_supabase.env, data) == (0, 1)
    assert fake_supabase.removed["a" * 32] == {"id": "a" * 32, "removed_at": REMOVED_AT, "sha256": "the-hash"}
    assert supabase.sync_removed(con, fake_supabase.env, data) == (0, 0)


def test_an_index_built_from_nothing_learns_every_removal(con, data, fake_supabase):
    fake_supabase.removed.update(removed_rows(*(letter * 32 for letter in "abcdef")))
    assert supabase.sync_removed(con, fake_supabase.env, data) == (6, 0)
    assert con.execute("SELECT sha256 FROM removed WHERE id = ?", ("c" * 32,)).fetchone() == (f"hash-of-{'c' * 32}",)


def test_a_learned_removal_is_refused_by_the_listing(con, data, fake_supabase):
    fake_supabase.removed.update(removed_rows("a" * 32))
    supabase.sync_removed(con, fake_supabase.env, data)
    gist = {"id": "a" * 32, "created_at": "2026-09-29T10:00:00Z", "description": None}
    assert listing.add_gist(con, gist, {"size": 10, "raw_url": "https://raw.test/a"}) == 0


def test_a_learned_removal_drops_a_log_already_downloaded(con, data, stage_log, fake_supabase):
    staged = stage_log("c" * 32, "2026-09-29T10:00:00Z", b"a log removed on another machine")
    fake_supabase.removed.update(removed_rows("c" * 32))
    assert supabase.sync_removed(con, fake_supabase.env, data) == (1, 0)
    assert con.execute("SELECT state FROM gists WHERE id = ?", ("c" * 32,)).fetchone() == ("removed",)
    assert not staged.exists()
