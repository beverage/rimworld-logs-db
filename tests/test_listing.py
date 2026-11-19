"""Turning a page of GitHub's gist list into index rows."""

import pytest

from rimworld_logs_db import listing
from rimworld_logs_db.common import week_of

GIST = {
    "id": "a" * 32,
    "created_at": "2026-09-29T10:00:00Z",
    "description": "Rimworld output log published using HugsLib",
    "files": {"output_log.txt": {"size": 10, "raw_url": "https://raw.test/a"}},
}


@pytest.mark.parametrize(
    ("timestamp", "week"),
    [("2025-12-29T00:00:00Z", "2026-W01"), ("2025-07-11T12:00:00Z", "2025-W28")],
)
def test_week_is_the_iso_week_even_across_a_year_boundary(timestamp, week):
    assert week_of(timestamp) == week


@pytest.mark.parametrize(
    ("description", "uploader"),
    [
        ("Rimworld output log published using HugsLib Standalone Log Publisher", "standalone"),
        ("Rimworld output log published using HugsLib", "hugslib"),
        (None, "other"),
    ],
)
def test_uploader_is_read_from_the_gist_description(description, uploader):
    assert listing.uploader_of(description) == uploader


def test_the_log_is_output_log_txt_when_a_gist_has_several_files():
    gist = {"files": {"a.txt": {"size": 5}, "output_log.txt": {"size": 1}}}
    assert listing.log_file(gist)["size"] == 1


def test_a_gist_with_no_files_has_no_log():
    assert listing.log_file({"files": {}}) is None


def test_a_page_is_indexed_once(con):
    assert listing.add_page(con, [GIST], "2026-01-01T00:00:00Z") == (1, 0)
    assert listing.add_page(con, [GIST], "2026-01-01T00:00:00Z") == (0, 1)
    row = con.execute("SELECT week, size, uploader, state FROM gists").fetchone()
    assert row == ("2026-W40", 10, "hugslib", "pending")


def test_uploads_older_than_the_cutoff_are_skipped(con):
    assert listing.add_page(con, [GIST], "2026-10-01T00:00:00Z") == (0, 0)


def test_a_gist_on_the_removed_list_is_not_indexed(con):
    con.execute("INSERT INTO removed (id, removed_at) VALUES (?, '2026-11-19T00:00:00Z')", (GIST["id"],))
    assert listing.add_page(con, [GIST], "2026-01-01T00:00:00Z") == (0, 1)
    assert con.execute("SELECT COUNT(*) FROM gists").fetchone()[0] == 0
