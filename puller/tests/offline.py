"""Offline checks for the puller. Nothing here touches GitHub, R2 or Supabase.

Covers the log and patch parsers, the download step's handling of refusals,
errors and short bodies (against a local test server), the env file reader,
and the Supabase sync's requests (against the same local server).

Run:  python3 puller/tests/offline.py
"""

import gzip
import http.client
import http.server
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from logsdb import download, index_db, layout, listing, parsing, settings, supabase  # noqa: E402
from logsdb.common import ConfigError, UploadError, week_of  # noqa: E402

failed = []


def check(name, got, want):
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": got {got!r}, want {want!r}"))
    if not ok:
        failed.append(name)


# ---- parsing

log = (
    "Log uploaded on Saturday, November 14, 2026, 1:58:50 PM\r\n"
    "Loaded mods:\r\n"
    "Harmony(brrainz.harmony)[mv:3.0.1.0]: 0Harmony(2.4.3), HarmonyMod(2.4.2)\r\n"
    "Core(Ludeon.RimWorld): (no assemblies)\r\n"
    "Odyssey(Ludeon.RimWorld.Odyssey): (no assemblies)\r\n"
    "Royalty(ludeon.rimworld.royalty): (no assemblies)\r\n"
    "Yayo's Animation (Continued)(com.yayo.yayoAni.continued): 0PrepatcherAPI(1.2.0), yayoAni(1.0.0)\r\n"
    "Odd (v2): Name(odd.name)[ov:1.0]: Thing(av:1.0.0,fv:1.2.0)\r\n"
    "\r\n"
    "Active Harmony patches:\r\n"
    "Not.A.Mod(should.not.count): PRE: x\r\n"
    "\r\n"
    "Log file contents:\r\n"
    "[00:00:01] RimWorld 1.7.5011 rev1082\r\n"
    "(log trimmed to 10,000 lines. Use publishing options to upload the full log)"
)
facts = parsing.parse_log(log)
check(
    "mod package ids",
    facts["mods"],
    [
        "brrainz.harmony",
        "Ludeon.RimWorld",
        "Ludeon.RimWorld.Odyssey",
        "ludeon.rimworld.royalty",
        "com.yayo.yayoAni.continued",
        "odd.name",
    ],
)
check("dlcs", facts["dlcs"], ["Odyssey", "royalty"])
check("game version", (facts["game_version"], facts["game_rev"]), ("1.7.5011", 1082))
check("trimmed", facts["trimmed"], True)
check("no header", parsing.parse_log("just text")["mod_count"], 0)
check("week at ISO year edge", week_of("2025-12-29T00:00:00Z"), "2026-W01")
check("week mid-year", week_of("2025-07-11T12:00:00Z"), "2025-W28")
check(
    "uploader standalone",
    listing.uploader_of("Rimworld output log published using HugsLib Standalone Log Publisher"),
    "standalone",
)
check("uploader hugslib", listing.uploader_of("Rimworld output log published using HugsLib"), "hugslib")
check("uploader other", listing.uploader_of(None), "other")
check(
    "log file pick",
    listing.log_file({"files": {"a.txt": {"size": 5}, "output_log.txt": {"size": 1}}})["size"],
    1,
)

# ---- Harmony patch block

patch_log = (
    "Loaded mods:\nCore(Ludeon.RimWorld): (no assemblies)\n\n"
    "Active Harmony patches:\n"
    "Ability.PreActivate: post: A.B.Post\n"
    "Pawn.Tick: PRE: [800]X.Y.Pre, Z.Class+Nested.Pre post: Q.R.Post TRANS: T.U.Trans\n"
    "Pawn.Tick: PRE: [800]X.Y.Pre\n"
    "Dialog_Trade.Close: (no patches)\n"
    "FleckSystemBase`1.ForceDraw: PRE: RimThreadedTTR.FleckPatch`1.ForceDrawPrefix\n"
    "Low.Priority: post: [-5]Neg.Class.Post\n"
    "Harmony versions present: 2.4.3.0: some.owner, other.owner; 2.3.3.0: x.y\n"
    "\n"
    "Log file contents:\nSomething.Else: PRE: Not.A.Patch\n"
)
check(
    "patch entries",
    set(parsing.parse_patches(patch_log)),
    {
        ("Ability.PreActivate", "postfix", None, "A.B.Post"),
        ("Pawn.Tick", "prefix", 800, "X.Y.Pre"),
        ("Pawn.Tick", "prefix", None, "Z.Class+Nested.Pre"),
        ("Pawn.Tick", "postfix", None, "Q.R.Post"),
        ("Pawn.Tick", "transpiler", None, "T.U.Trans"),
        ("FleckSystemBase`1.ForceDraw", "prefix", None, "RimThreadedTTR.FleckPatch`1.ForceDrawPrefix"),
        ("Low.Priority", "postfix", -5, "Neg.Class.Post"),
    },
)
check("no patch block", parsing.parse_patches("nothing here"), [])
check(
    "namespace of a nested class",
    parsing.namespace_of("VFEEmpire.HarmonyPatches.Patch_Conceit+Work.Transpiler"),
    "VFEEmpire.HarmonyPatches",
)
check("namespace of a bare class", parsing.namespace_of("Class.Method"), "")
key = parsing.patch_key(("a", "prefix", None, "b"))
check(
    "patch key is stable and fits 64 signed bits",
    (key == parsing.patch_key(("a", "prefix", None, "b")), -(2**63) <= key < 2**63),
    (True, True),
)
check("priority is part of the key", key != parsing.patch_key(("a", "prefix", 800, "b")), True)

# ---- refusal counting

stop = threading.Event()
throttle = download.Throttle(rps=1000)
check("first refusal counts", throttle.refused("1"), 1)
check("refusal during the pause does not count", throttle.refused("1"), 1)
time.sleep(2.1)
check("refusal after the pause counts", throttle.refused("0"), 2)
throttle.succeeded()
check("a success resets the run", throttle.refusals, 0)

# ---- fetch_one against a local server

BODY = b"hello log\n" * 1000
script = {}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        steps = script[self.path]
        status, headers, body = steps.pop(0) if len(steps) > 1 else steps[0]
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
port = server.server_address[1]


def local_connection():
    conn = getattr(download._thread, "conn", None)
    if conn is None:
        conn = download._thread.conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    return conn


download.raw_connection = local_connection
staging = Path(tempfile.mkdtemp(prefix="corpus-offline-"))


def run(path, expect=len(BODY), stop_event=None, throttle_obj=None):
    return download.fetch_one(
        (path.strip("/"), f"http://x{path}", "2026-W47", expect),
        staging,
        throttle_obj or download.Throttle(rps=1000),
        stop_event or threading.Event(),
        64 << 20,
    )


script["/ok"] = [(200, {}, BODY)]
result = run("/ok")
check("200 is fetched", (result.state, result.bytes), ("fetched", len(BODY)))
check("staged gzip holds the body", gzip.open(staging / "2026-W47" / "ok.txt.gz").read(), BODY)

script["/gone"] = [(404, {}, b"")]
check("404 is gone", run("/gone").state, "gone")

script["/short"] = [(200, {}, BODY[:100])]
result = run("/short")
check("short body is an error", (result.state, result.error), ("error", f"got 100 of {len(BODY)} bytes"))
check("short body leaves no file", list((staging / "2026-W47").glob("short*")), [])

check("too large is skipped", run("/ok", expect=(64 << 20) + 1).state, "too_large")

script["/twice"] = [(429, {"Retry-After": "1"}, b""), (429, {"Retry-After": "1"}, b""), (200, {}, BODY)]
throttle = download.Throttle(rps=1000)
began = time.monotonic()
result = run("/twice", throttle_obj=throttle)
waited = time.monotonic() - began
check("429 twice then 200 is fetched", result.state, "fetched")
check("each refusal was waited out", waited >= 4.0, True)
check("refusal run reset by the success", throttle.refusals, 0)

script["/always"] = [(429, {"Retry-After": "0"}, b"")]
stop = threading.Event()
throttle = download.Throttle(rps=1000)
result = run("/always", stop_event=stop, throttle_obj=throttle)
check("five refusals in a row stop the run", (stop.is_set(), throttle.refusals, result.state), (True, 5, None))

script["/broken"] = [(500, {}, b"")]
stop = threading.Event()
original_wait = stop.wait
slept = []
stop.wait = lambda seconds=None: slept.append(seconds) or False
result = run("/broken", stop_event=stop)
check("500 four times is an error", (result.state, result.error), ("error", "HTTP 500"))
check("backoff doubles", [s for s in slept if s >= 1], [2.0, 4.0, 8.0, 16.0])

# ---- env file and bucket keys

env_dir = Path(tempfile.mkdtemp(prefix="corpus-env-"))
env_file = env_dir / "env"
env_file.write_text("# comment\n\nA=1\nB = two words \nC=has=equals==\nnot a pair\nEMPTY=\n")
os.environ["HUGSLIB_CORPUS_ENV"] = str(env_file)
check("env file parses", settings.load_env(), {"A": "1", "B": "two words", "C": "has=equals==", "EMPTY": ""})
try:
    settings.require(settings.load_env(), ("A", "EMPTY", "MISSING"))
    raised = None
except ConfigError as exc:
    raised = str(exc)
check("missing settings are named, not shown", raised, "not set in the env file: EMPTY, MISSING")
os.environ["HUGSLIB_CORPUS_ENV"] = str(env_dir / "absent")
try:
    settings.load_env()
    raised = False
except ConfigError:
    raised = True
check("an unreadable env file is a config error", raised, True)
check(
    "part files map to bucket keys",
    sorted(layout.part_files(Path("/d"), "week=2026-W46/part-000.parquet")),
    [
        "logs/week=2026-W46/part-000.parquet",
        "patches/ActivePatches/week=2026-W46/part-000.parquet",
        "patches/HarmonyPatchNamespaces/week=2026-W46/part-000.parquet",
        "patches/HarmonyPatches/week=2026-W46/part-000.parquet",
        "patches/HugslibLogs/week=2026-W46/part-000.parquet",
    ],
)

# ---- Supabase sync against the local server

posts = []
post_reply = [(201, b"")]


def do_POST(self):
    body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
    posts.append((self.path, {k.lower(): v for k, v in self.headers.items()}, json.loads(body)))
    status, reply = post_reply[0]
    self.send_response(status)
    self.send_header("Content-Length", str(len(reply)))
    self.end_headers()
    self.wfile.write(reply)


Handler.do_POST = do_POST
sync_dir = Path(tempfile.mkdtemp(prefix="corpus-sync-"))
con = index_db.open_db(sync_dir)
for n in range(1, 503):
    con.execute(
        "INSERT INTO gists (id, created_at, week, size, raw_url, uploader, state, shard, sha256, log_no)"
        " VALUES (?, '2026-11-10T00:00:00Z', '2026-W46', 10, 'u', 'hugslib', 'packed',"
        " 'week=2026-W46/part-000.parquet', 'aa', ?)",
        (f"g{n:04d}", n),
    )
    con.execute(
        "INSERT INTO facts VALUES (?, '1.6.4871', 1, 5, ?, 0, 9, 1)", (f"g{n:04d}", "Royalty,Odyssey" if n == 1 else "")
    )
con.commit()
fake = {"SUPABASE_URL": f"http://127.0.0.1:{port}", "SUPABASE_SECRET_KEY": "sb_secret_test"}
check("sync upserts every packed log", supabase.sync_logs(con, fake), 502)
check("sync sends two batches", [len(post[2]) for post in posts], [500, 2])
path, headers, _ = posts[0]
check(
    "sync request shape",
    (path, headers.get("apikey"), headers.get("prefer"), headers.get("authorization")),
    ("/rest/v1/logs?on_conflict=id", "sb_secret_test", "resolution=merge-duplicates,return=minimal", None),
)
check(
    "sync row",
    next(row for post in posts for row in post[2] if row["id"] == "g0001"),
    {
        "id": "g0001",
        "hugslib_log_id": 1,
        "created_at": "2026-11-10T00:00:00Z",
        "size_bytes": 10,
        "uploader": "hugslib",
        "week": "2026-W46",
        "shard": "week=2026-W46/part-000.parquet",
        "sha256": "aa",
        "game_version": "1.6.4871",
        "game_rev": 1,
        "mod_count": 5,
        "dlcs": ["Royalty", "Odyssey"],
        "trimmed": False,
        "line_count": 9,
    },
)
check("a second sync has nothing to send", supabase.sync_logs(con, fake), 0)
con.execute("UPDATE gists SET synced = 0 WHERE id = 'g0002'")
con.commit()
for reply, want in (((404, b'{"code":"PGRST205","message":"no table"}'), "config"), ((500, b"boom"), "upload")):
    post_reply[0] = reply
    try:
        supabase.sync_logs(con, fake)
        raised = None
    except ConfigError:
        raised = "config"
    except UploadError:
        raised = "upload"
    check(f"HTTP {reply[0]} from Supabase is a {want} error", raised, want)
check("a refused batch stays unsynced", con.execute("SELECT synced FROM gists WHERE id = 'g0002'").fetchone()[0], 0)

server.shutdown()
print(f"RESULT offline {'ok' if not failed else 'failed'} failed={len(failed)}")
sys.exit(1 if failed else 0)
