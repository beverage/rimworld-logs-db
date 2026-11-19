"""Choosing which week a run works on next."""

from rimworld_logs_db import cli, pipeline
from rimworld_logs_db.common import now_iso, week_of


def add(con, gist_id: str, created_at: str, state: str = "pending") -> None:
    """Put one listed gist in the index."""
    con.execute(
        "INSERT INTO gists (id, created_at, week, size, raw_url, uploader, state)"
        " VALUES (?, ?, ?, 10, 'u', 'hugslib', ?)",
        (gist_id, created_at, week_of(created_at), state),
    )
    con.commit()


def test_the_newest_finished_week_with_work_comes_first(con):
    add(con, "a", "2026-06-02T10:00:00Z")
    add(con, "b", "2026-08-04T10:00:00Z")
    add(con, "c", "2026-08-05T10:00:00Z", state="fetched")
    assert pipeline.next_week(con) == ("2026-W32", 1)


def test_a_week_with_nothing_left_to_do_is_passed_over(con):
    add(con, "a", "2026-06-02T10:00:00Z")
    add(con, "b", "2026-08-04T10:00:00Z", state="packed")
    assert pipeline.next_week(con) == ("2026-W23", 1)


def test_the_week_in_progress_is_left_alone(con):
    add(con, "a", now_iso())
    assert pipeline.next_week(con) is None


def test_weeks_older_than_the_bound_are_left_alone(con):
    add(con, "a", "2026-05-12T10:00:00Z")
    add(con, "b", "2026-05-19T10:00:00Z")
    assert pipeline.next_week(con, "2026-W21") == ("2026-W21", 1)
    assert pipeline.next_week(con, "2026-W22") is None


def test_since_takes_a_date_on_the_command_line():
    args = cli.build_parser().parse_args(["run", "--since", "2026-05-19"])
    assert args.since == "2026-05-19"
    assert week_of(f"{args.since}T00:00:00Z") == "2026-W21"
