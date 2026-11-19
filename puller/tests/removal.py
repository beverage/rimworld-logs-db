# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=17"]
# ///
"""Checks for `corpus.py remove`, on a small made-up archive. Nothing here touches R2 or Supabase.

Packs six synthetic logs, then removes some of them three ways: a dry run, a
removal while the part is still on disk, and a removal after upload, with a
folder standing in for the bucket and a local server standing in for Supabase.

Run:  uv run puller/tests/removal.py
"""

import argparse
import gzip
import hashlib
import http.server
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import threading
import urllib.parse
from pathlib import Path

import pyarrow.parquet as pq

spec = importlib.util.spec_from_file_location("corpus", Path(__file__).resolve().parents[1] / "corpus.py")
corpus = importlib.util.module_from_spec(spec)
spec.loader.exec_module(corpus)

failed = []


def check(name, got, want):
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": got {got!r}, want {want!r}"))
    if not ok:
        failed.append(name)


# ---- links

A, B, C, D, E, F = (letter * 32 for letter in "abcdef")
check("id from a gist link", corpus.gist_id_of(f"https://gist.github.com/HugsLibRecordKeeper/{A}"), A)
check("id from a link with no account", corpus.gist_id_of(f"https://gist.github.com/{A}#file-output_log-txt"), A)
check(
    "id from a raw log link",
    corpus.gist_id_of(f"https://gist.githubusercontent.com/HugsLibRecordKeeper/{A}/raw/{'9' * 40}/output_log.txt"),
    A,
)
check("a bare id", corpus.gist_id_of(f"  {A}\n"), A)
check("not a gist", corpus.gist_id_of("https://example.com/whatever"), None)

# ---- a small packed archive


def make_log(seed: int) -> bytes:
    return (
        "Log uploaded on Monday\nLoaded mods:\nCore(Ludeon.RimWorld): (no assemblies)\n"
        f"Mod {seed}(author.mod{seed}): Mod{seed}(1.0.0)\n\n"
        "Active Harmony patches:\n"
        f"Pawn.Tick: PRE: Mod{seed}.Patches.Pre post: Shared.Patches.Post\n"
        f"Thing.Draw: TRANS: [800]Mod{seed}.Patches.Trans\n"
        "Harmony versions present: 2.3.3.0: some.owner\n\n"
        f"Log file contents:\nRimWorld 1.6.4871 rev1\nline from game {seed}\n"
    ).encode("utf-8")


data = Path(tempfile.mkdtemp(prefix="corpus-removal-"))
week = "2026-W40"
con = corpus.open_db(data)
(data / "staging" / week).mkdir(parents=True)
# B and C are the same log uploaded twice, and so are D and E.
for number, (gist_id, seed) in enumerate(((A, 1), (B, 2), (C, 2), (D, 3), (E, 3), (F, 4))):
    raw = make_log(seed)
    with gzip.open(data / "staging" / week / f"{gist_id}.txt.gz", "wb") as out:
        out.write(raw)
    con.execute(
        "INSERT INTO gists (id, created_at, week, size, raw_url, uploader, state, sha256)"
        " VALUES (?, ?, ?, ?, 'u', 'hugslib', 'fetched', ?)",
        (gist_id, f"2026-09-29T10:00:0{number}Z", week, len(raw), hashlib.sha256(raw).hexdigest()),
    )
con.commit()
packed = corpus.cmd_pack(
    argparse.Namespace(data=data, week=None, include_open=False, partial=False, keep_staging=False, level=3)
)
check("the six logs pack", packed, 0)
shard = f"week={week}/part-000.parquet"
files = corpus.part_files(data, shard)


def rows(path, column):
    return pq.read_table(path, columns=[column])[column].to_pylist()


def log_no(gist_id):
    return con.execute("SELECT log_no FROM gists WHERE id = ?", (gist_id,)).fetchone()[0]


def state(gist_id):
    found = con.execute("SELECT state FROM gists WHERE id = ?", (gist_id,)).fetchone()
    return found and found[0]


def remove(*gists, **options):
    settings = {"dry_run": False, "same_content": False, "no_supabase": False, "level": 3, **options}
    return corpus.cmd_remove(argparse.Namespace(data=data, gists=list(gists), **settings))


# A local server stands in for Supabase, and a folder for the bucket.
deletes = []
remote_removed = {}  # the removed_logs table, by gist id


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_DELETE(self):
        deletes.append((self.path, self.headers.get("apikey")))
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        for row in json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0")))):
            remote_removed[row["id"]] = row
        self.send_response(201)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        offset, limit = int(query["offset"][0]), int(query["limit"][0])
        listed = sorted(remote_removed.values(), key=lambda row: row["id"])[offset : offset + limit]
        body = json.dumps(listed).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
env_file = data / "env"
env_file.write_text(
    f"SUPABASE_URL=http://127.0.0.1:{server.server_address[1]}\nSUPABASE_SECRET_KEY=test-key\n"
    "R2_ENDPOINT=unused\nR2_BUCKET=stand-in\nR2_ACCESS_KEY_ID=unused\nR2_SECRET_ACCESS_KEY=unused\n"
)
os.environ["HUGSLIB_CORPUS_ENV"] = str(env_file)

# ---- dry run

a_no = log_no(A)
before = {key: path.stat().st_size for key, path in files.items()}
check("a dry run succeeds", remove(f"https://gist.github.com/HugsLibRecordKeeper/{A}", dry_run=True), 0)
check("a dry run leaves the files alone", {key: path.stat().st_size for key, path in files.items()}, before)
check("a dry run leaves the index alone", (state(A), deletes), ("packed", []))

# ---- removal while the part is still on disk

others_before = sum(1 for n in rows(files[f"patches/ActivePatches/{shard}"], "HugslibLogId") if n != a_no)
check("removing A succeeds", remove(A), 0)
check("A is gone from the log file", sorted(rows(files[f"logs/{shard}"], "id")), sorted([B, C, D, E, F]))
check("A is gone from HugslibLogs", a_no in rows(files[f"patches/HugslibLogs/{shard}"], "HugslibLogId"), False)
active = rows(files[f"patches/ActivePatches/{shard}"], "HugslibLogId")
check("A's patch rows are gone and the others are intact", (a_no in active, len(active)), (False, others_before))
check("the patch dictionary is untouched", before[f"patches/HarmonyPatches/{shard}"], files[f"patches/HarmonyPatches/{shard}"].stat().st_size)
check("the index marks A removed", (state(A), con.execute("SELECT COUNT(*) FROM removed WHERE id = ?", (A,)).fetchone()[0]), ("removed", 1))
check("A's facts are deleted", con.execute("SELECT COUNT(*) FROM facts WHERE id = ?", (A,)).fetchone()[0], 0)
check("the part's log count drops", con.execute("SELECT logs FROM parts WHERE shard = ?", (shard,)).fetchone()[0], 5)
check("Supabase is asked to delete A's row", deletes, [(f"/rest/v1/logs?id=eq.{A}", "test-key")])

check("removing B alone succeeds", remove(B), 0)
check("B's twin C stays without --same-content", (state(B), state(C)), ("removed", "packed"))

# ---- the removed list keeps a log out of later pulls

gist = {"id": A, "created_at": "2026-09-29T10:00:00Z", "description": "Rimworld output log published using HugsLib"}


def index(gist_id):
    """What an index run does when it meets this gist, committed as the index commits."""
    added = corpus.add_gist(con, {**gist, "id": gist_id}, {"size": 1, "raw_url": "u"})
    con.commit()
    return added


check("a removed log is not indexed again", index(A), 0)
check("removing a log the index has never seen succeeds", remove("9" * 32, no_supabase=True), 0)
check("that log is refused when it turns up", index("9" * 32), 0)
check("an ordinary new log is still indexed", index("8" * 32), 1)
con.execute("DELETE FROM gists WHERE id = ?", ("8" * 32,))
con.commit()

# ---- removal after upload, through the stand-in bucket

bucket = data / "bucket"


class Bucket:
    def download_file(self, name, key, path):
        shutil.copyfile(bucket / key, path)

    def upload_file(self, path, name, key):
        (bucket / key).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, bucket / key)

    def head_object(self, Bucket, Key):
        return {"ContentLength": (bucket / Key).stat().st_size}


corpus.r2_client = lambda env: Bucket()
check("the part uploads", corpus.push_parts(con, data, Bucket(), "stand-in", keep_local=False)[0], 1)
check("nothing of the part is left on disk", [p for p in files.values() if p.exists()], [])
d_no, e_no = log_no(D), log_no(E)
check("removing D with its twin succeeds", remove(D, same_content=True), 0)
check("D and its twin E are both removed", (state(D), state(E)), ("removed", "removed"))
check("the bucket's log file holds only C and F", sorted(rows(bucket / f"logs/{shard}", "id")), sorted([C, F]))
left = set(rows(bucket / f"patches/ActivePatches/{shard}", "HugslibLogId"))
check("the bucket's patch rows are for C and F only", left, {log_no(C), log_no(F)})
check("no working files are left behind", (data / "removal").exists(), False)
check("a fresh copy of the index went to the bucket", (bucket / "state" / "index.sqlite").exists(), True)

# ---- what remains still rebuilds exactly

patches = {}
table = pq.read_table(bucket / f"patches/HarmonyPatches/{shard}").to_pylist()
for row in table:
    patches[row["HarmonyPatchId"]] = (row["Target"], row["Kind"], row["Priority"], row["Method"])
by_log = {}
pairs = pq.read_table(bucket / f"patches/ActivePatches/{shard}").to_pylist()
for row in pairs:
    by_log.setdefault(row["HugslibLogId"], set()).add(patches[row["HarmonyPatchId"]])
mismatched = 0
for row in pq.read_table(bucket / f"logs/{shard}", columns=["hugslib_log_id", "text"]).to_pylist():
    mismatched += set(corpus.parse_patches(row["text"])) != by_log.get(row["hugslib_log_id"], set())
check("the remaining logs' patches still rebuild from the tables", mismatched, 0)
check("every removal reached Supabase", sorted(path for path, _ in deletes), sorted(f"/rest/v1/logs?id=eq.{g}" for g in (A, B, D, E)))

# ---- the durable list, and an index rebuilt from nothing

env = corpus.load_env()
check(
    "each removal is on the durable list with its content hash",
    {g: bool(remote_removed.get(g, {}).get("sha256")) for g in (A, B, D, E)},
    {A: True, B: True, D: True, E: True},
)
check("a sync sends up a removal made without Supabase", corpus.sync_removed(con, env, data), (0, 1))
check("that removal is now on the durable list", "9" * 32 in remote_removed, True)

fresh = Path(tempfile.mkdtemp(prefix="corpus-rebuilt-"))
rebuilt = corpus.open_db(fresh)
# C was fetched on this second machine, then removed on the first.
(fresh / "staging" / week).mkdir(parents=True)
staged_c = fresh / "staging" / week / f"{C}.txt.gz"
staged_c.write_bytes(b"staged")
rebuilt.execute(
    "INSERT INTO gists (id, created_at, week, size, raw_url, uploader, state) VALUES (?, ?, ?, 1, 'u', 'hugslib', 'fetched')",
    (C, "2026-09-29T10:00:02Z", week),
)
rebuilt.commit()
remote_removed[C] = {"id": C, "sha256": None, "removed_at": "2026-11-19T00:00:00+00:00"}
check("a rebuilt index learns the whole list", corpus.sync_removed(rebuilt, env, fresh), (6, 0))
added = corpus.add_gist(rebuilt, {**gist, "id": A}, {"size": 1, "raw_url": "u"})
rebuilt.commit()
check("a rebuilt index refuses a removed log", added, 0)
check(
    "a log removed elsewhere is dropped here, staged copy included",
    (rebuilt.execute("SELECT state FROM gists WHERE id = ?", (C,)).fetchone()[0], staged_c.exists()),
    ("removed", False),
)

sha_a = remote_removed[A]["sha256"]
again = fresh / "staging" / week / f"{'7' * 32}.txt.gz"
again.write_bytes(b"the same log under a new id")
result = corpus.drop_if_removed(rebuilt, again, {"id": "7" * 32, "state": "fetched", "sha256": sha_a}, {sha_a})
rebuilt.commit()
check(
    "a download with a removed log's content is discarded",
    (result["state"], again.exists(), rebuilt.execute("SELECT COUNT(*) FROM removed WHERE id = ?", ("7" * 32,)).fetchone()[0]),
    ("removed", False, 1),
)
other = {"id": "6" * 32, "state": "fetched", "sha256": "f" * 64}
check("any other download is kept", corpus.drop_if_removed(rebuilt, again, other, {sha_a}), other)

for number in range(1005):
    remote_removed[f"{number:032x}"] = {"id": f"{number:032x}", "sha256": None, "removed_at": "2026-01-01T00:00:00+00:00"}
paged = Path(tempfile.mkdtemp(prefix="corpus-paged-"))
check("a long list is read in pages", corpus.sync_removed(corpus.open_db(paged), env, paged)[0], len(remote_removed))
shutil.rmtree(fresh, ignore_errors=True)
shutil.rmtree(paged, ignore_errors=True)

server.shutdown()
shutil.rmtree(data, ignore_errors=True)
print(f"RESULT removal {'ok' if not failed else 'failed'} failed={len(failed)}")
sys.exit(1 if failed else 0)
