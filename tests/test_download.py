"""Downloading one log: success, refusals, errors and the removed list."""

import gzip
import threading
import time

from rimworld_logs_db import download

BODY = b"hello log\n" * 1000


def test_a_refusal_counts_once_per_pause():
    throttle = download.Throttle(rps=1000)
    assert throttle.refused("1") == 1
    # Requests already in flight come back refused during the pause; they are the same refusal.
    assert throttle.refused("1") == 1
    time.sleep(2.1)
    assert throttle.refused("0") == 2
    throttle.succeeded()
    assert throttle.refusals == 0


def test_a_log_is_staged_gzipped_with_its_size_and_hash(raw_host):
    raw_host.script["/ok"] = [(200, {}, BODY)]
    result = raw_host.fetch("/ok", len(BODY))
    assert (result.state, result.status, result.bytes) == ("fetched", 200, len(BODY))
    assert len(result.sha256) == 64
    with gzip.open(raw_host.staging / "2026-W47" / "ok.txt.gz") as staged:
        assert staged.read() == BODY


def test_a_missing_gist_is_marked_gone(raw_host):
    raw_host.script["/gone"] = [(404, {}, b"")]
    assert raw_host.fetch("/gone", len(BODY)).state == "gone"


def test_a_short_body_is_an_error_and_leaves_no_file(raw_host):
    raw_host.script["/short"] = [(200, {}, BODY[:100])]
    result = raw_host.fetch("/short", len(BODY))
    assert (result.state, result.error) == ("error", f"got 100 of {len(BODY)} bytes")
    assert list((raw_host.staging / "2026-W47").glob("short*")) == []


def test_a_log_over_the_size_limit_is_skipped_without_a_request(raw_host):
    assert raw_host.fetch("/never-requested", (64 << 20) + 1).state == "too_large"


def test_refusals_are_waited_out_then_the_download_succeeds(raw_host):
    refused = (429, {"Retry-After": "1"}, b"")
    raw_host.script["/twice"] = [refused, refused, (200, {}, BODY)]
    throttle = download.Throttle(rps=1000)
    began = time.monotonic()
    result = raw_host.fetch("/twice", len(BODY), throttle=throttle)
    assert result.state == "fetched"
    assert time.monotonic() - began >= 4.0
    assert throttle.refusals == 0


def test_five_refusals_in_a_row_stop_the_run_and_leave_the_log_pending(raw_host):
    raw_host.script["/always"] = [(429, {"Retry-After": "0"}, b"")]
    stop, throttle = threading.Event(), download.Throttle(rps=1000)
    result = raw_host.fetch("/always", len(BODY), stop=stop, throttle=throttle)
    assert stop.is_set()
    assert throttle.refusals == download.MAX_REFUSALS
    assert result.state is None


def test_a_server_error_is_retried_with_doubling_waits_then_recorded(raw_host):
    raw_host.script["/broken"] = [(500, {}, b"")]
    stop, waits = threading.Event(), []
    stop.wait = lambda seconds=None: waits.append(seconds) or False
    result = raw_host.fetch("/broken", len(BODY), stop=stop)
    assert (result.state, result.error) == ("error", "HTTP 500")
    assert [seconds for seconds in waits if seconds >= 1] == [2.0, 4.0, 8.0, 16.0]


def test_a_download_matching_a_removed_logs_content_is_discarded(data, con):
    staged = data / "again.txt.gz"
    staged.write_bytes(b"the same log under a new id")
    fetched = download.Download("7" * 32, "fetched", sha256="a" * 64)
    result = download.drop_if_removed(con, staged, fetched, {"a" * 64})
    assert result.state == "removed"
    assert not staged.exists()
    assert con.execute("SELECT sha256 FROM removed WHERE id = ?", ("7" * 32,)).fetchone() == ("a" * 64,)


def test_any_other_download_is_kept(data, con):
    staged = data / "kept.txt.gz"
    staged.write_bytes(b"an ordinary log")
    fetched = download.Download("6" * 32, "fetched", sha256="f" * 64)
    assert download.drop_if_removed(con, staged, fetched, {"a" * 64}) == fetched
    assert staged.exists()
