"""Shared fixtures. Nothing in the test suite touches GitHub, R2 or Supabase.

A local HTTP server stands in for GitHub's raw file host, another for
Supabase's REST interface, and a folder for the R2 bucket.
"""

from __future__ import annotations

import gzip
import hashlib
import http.client
import http.server
import json
import shutil
import threading
import urllib.parse
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

from rimworld_logs_db import bucket, download, index_db, layout, packing, settings
from rimworld_logs_db.common import week_of


def serve(handler) -> tuple[http.server.ThreadingHTTPServer, int]:
    """Start a local server on a free port, in a background thread."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    # A short poll, so shutting the server down at the end of a test does not wait half a second.
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
    return server, server.server_address[1]


def make_log(seed: int) -> bytes:
    """A small HugsLib-shaped log. Two logs with the same seed are byte-identical."""
    return (
        "Log uploaded on Monday\nLoaded mods:\nCore(Ludeon.RimWorld): (no assemblies)\n"
        f"Mod {seed}(author.mod{seed}): Mod{seed}(1.0.0)\n\n"
        "Active Harmony patches:\n"
        f"Pawn.Tick: PRE: Mod{seed}.Patches.Pre post: Shared.Patches.Post\n"
        f"Thing.Draw: TRANS: [800]Mod{seed}.Patches.Trans\n"
        "Harmony versions present: 2.3.3.0: some.owner\n\n"
        f"Log file contents:\nRimWorld 1.6.4871 rev1\nline from game {seed}\n"
    ).encode()


@pytest.fixture
def data(tmp_path) -> Path:
    """An empty data folder."""
    return tmp_path / "data"


@pytest.fixture
def con(data):
    """The index of the data folder, created empty."""
    connection = index_db.open_db(data)
    yield connection
    connection.close()


@pytest.fixture
def stage_log(data, con):
    """A function that puts one log in staging and in the index, as `fetch` leaves it."""

    def stage(gist_id: str, created_at: str, raw: bytes) -> Path:
        week = week_of(created_at)
        staged = layout.staged_path(data / "staging", week, gist_id)
        staged.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(staged, "wb") as out:
            out.write(raw)
        con.execute(
            "INSERT INTO gists (id, created_at, week, size, raw_url, uploader, state, sha256)"
            " VALUES (?, ?, ?, ?, 'https://raw.test/log', 'hugslib', 'fetched', ?)",
            (gist_id, created_at, week, len(raw), hashlib.sha256(raw).hexdigest()),
        )
        con.commit()
        return staged

    return stage


@pytest.fixture
def raw_host(tmp_path, monkeypatch):
    """A stand-in for the raw file host, serving scripted responses.

    `script[path]` is a list of (status, headers, body) replies, used in order;
    the last one repeats. `fetch(path, size)` downloads that path through
    `download.fetch_one` into `staging`.
    """
    script: dict[str, list] = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            replies = script[self.path]
            status, headers, body = replies.pop(0) if len(replies) > 1 else replies[0]
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server, port = serve(Handler)

    def local_connection():
        conn = getattr(download._thread, "conn", None)
        if conn is None:
            conn = download._thread.conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        return conn

    monkeypatch.setattr(download, "raw_connection", local_connection)
    staging = tmp_path / "staging"

    def fetch(path: str, expect: int, stop=None, throttle=None) -> download.Download:
        row = (path.strip("/"), f"http://raw.test{path}", "2026-W47", expect)
        return download.fetch_one(
            row, staging, throttle or download.Throttle(rps=1000), stop or threading.Event(), 64 << 20
        )

    yield SimpleNamespace(script=script, fetch=fetch, staging=staging)
    download.drop_connection()
    server.shutdown()
    server.server_close()


@pytest.fixture
def fake_supabase(tmp_path, monkeypatch):
    """A stand-in for Supabase's REST interface, and an env file pointing at it.

    Upserts into `logs` are recorded in `posts`, the `removed_logs` table is
    kept in `removed`, and reads and row deletions are recorded in `gets` and
    `deletes`. Set `reply` to a (status, body) pair to make every POST fail.
    """
    state = SimpleNamespace(posts=[], removed={}, gets=[], deletes=[], reply=None)

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def respond(self, status: int, body: bytes = b"") -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            rows = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            if state.reply:
                return self.respond(*state.reply)
            if self.path.startswith("/rest/v1/removed_logs"):
                state.removed.update({row["id"]: row for row in rows})
            else:
                headers = {key.lower(): value for key, value in self.headers.items()}
                state.posts.append((self.path, headers, rows))
            self.respond(201)

        def do_GET(self):
            state.gets.append(self.path)
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            offset, limit = int(query.get("offset", ["0"])[0]), int(query["limit"][0])
            listed = sorted(state.removed.values(), key=lambda row: row["id"])[offset : offset + limit]
            self.respond(200, json.dumps(listed).encode("utf-8"))

        def do_DELETE(self):
            state.deletes.append((self.path, self.headers.get("apikey")))
            self.respond(204)

        def log_message(self, *args):
            pass

    server, port = serve(Handler)
    state.env = {"SUPABASE_URL": f"http://127.0.0.1:{port}", "SUPABASE_SECRET_KEY": "test-key"}
    env_file = tmp_path / "env"
    env_file.write_text(
        "".join(f"{key}={value}\n" for key, value in state.env.items())
        + "R2_ENDPOINT=unused\nR2_BUCKET=stand-in\nR2_ACCESS_KEY_ID=unused\nR2_SECRET_ACCESS_KEY=unused\n"
    )
    monkeypatch.setenv(settings.ENV_VARIABLE, str(env_file))
    yield state
    server.shutdown()
    server.server_close()


class FolderBucket:
    """A folder with the S3 calls the package makes."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def head_bucket(self, Bucket: str) -> dict:
        return {}

    def download_file(self, bucket_name: str, key: str, path: str) -> None:
        shutil.copyfile(self.root / key, path)

    def upload_file(self, path: str, bucket_name: str, key: str) -> None:
        (self.root / key).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, self.root / key)

    def head_object(self, Bucket: str, Key: str) -> dict:
        if not (self.root / Key).is_file():
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ContentLength": (self.root / Key).stat().st_size}

    def get_paginator(self, operation: str) -> FolderBucket:
        assert operation == "list_objects_v2"
        return self

    def paginate(self, Bucket: str, Prefix: str = ""):
        files = sorted(path for path in self.root.rglob("*") if path.is_file()) if self.root.exists() else []
        keys = [path.relative_to(self.root).as_posix() for path in files]
        yield {"Contents": [{"Key": key} for key in keys if key.startswith(Prefix)]}


@pytest.fixture
def folder_bucket(tmp_path, monkeypatch) -> FolderBucket:
    """A folder standing in for the R2 bucket, handed out wherever the package asks for a client."""
    stand_in = FolderBucket(tmp_path / "bucket")
    monkeypatch.setattr(bucket, "r2_client", lambda env: stand_in)
    return stand_in


@pytest.fixture
def archive(data, con, stage_log, fake_supabase):
    """Six made-up logs, A to F, packed into one part that is still on disk.

    B and C are the same log uploaded twice, and so are D and E.
    """
    ids = SimpleNamespace(**{letter.upper(): letter * 32 for letter in "abcdef"})
    seeds = {ids.A: 1, ids.B: 2, ids.C: 2, ids.D: 3, ids.E: 3, ids.F: 4}
    for number, gist_id in enumerate(seeds):
        stage_log(gist_id, f"2026-09-29T10:00:0{number}Z", make_log(seeds[gist_id]))
    assert packing.pack(data, include_open=True, level=3) == 0
    week = "2026-W40"
    shard = f"week={week}/part-000.parquet"

    def log_no(gist_id: str) -> int:
        return con.execute("SELECT log_no FROM gists WHERE id = ?", (gist_id,)).fetchone()[0]

    def state(gist_id: str) -> str | None:
        row = con.execute("SELECT state FROM gists WHERE id = ?", (gist_id,)).fetchone()
        return row and row[0]

    return SimpleNamespace(
        data=data,
        con=con,
        week=week,
        shard=shard,
        ids=ids,
        texts={gist_id: make_log(seed).decode("utf-8") for gist_id, seed in seeds.items()},
        hashes={gist_id: hashlib.sha256(make_log(seed)).hexdigest() for gist_id, seed in seeds.items()},
        paths=layout.part_paths(data, shard),
        log_no=log_no,
        state=state,
        stage=stage_log,
        make_log=make_log,
        supabase=fake_supabase,
    )
