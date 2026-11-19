# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.1", "pytz"]
# ///
"""Run every SQL block in the docs against the live bucket, following the README's setup.

Works in a fresh temporary folder with its own DuckDB secret and extension
folders, so it proves the setup from nothing and leaves ~/.duckdb alone.
Prints row counts, the first rows of each result and timings. No query in the
docs returns log text or a log id.

Run:  uv run puller/checks/docs.py [env file]
"""

import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

import duckdb

ENV_FILE = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.home() / ".config" / "rimworld-logs-db" / "env"
env = {}
for line in ENV_FILE.read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
repo = Path(__file__).resolve().parents[2]
scratch = Path(tempfile.mkdtemp(prefix="rimworld-logs-db-docs-"))
(scratch / "secrets").mkdir()
(scratch / "extensions").mkdir()
os.chdir(scratch)
failed = []


def connect():
    con = duckdb.connect(str(scratch / "rimworld.duckdb"))
    con.execute(f"SET extension_directory = '{scratch / 'extensions'}'")
    con.execute(f"SET secret_directory = '{scratch / 'secrets'}'")
    return con


def blocks(path):
    return re.findall(r"```sql\n(.*?)```", path.read_text(), flags=re.S)


con = duckdb.connect()
print(
    "default secret directory:",
    con.execute("SELECT current_setting('secret_directory')").fetchone()[0].replace(str(Path.home()), "~"),
)
con.close()

# Setup step 1: the README's own statement, with the placeholders filled in.
con = connect()
secret_sql = next(b for b in blocks(repo / "README.md") if "CREATE PERSISTENT SECRET" in b)
filled = (
    secret_sql.replace("<access key id>", env["R2_ACCESS_KEY_ID"])
    .replace("<secret access key>", env["R2_SECRET_ACCESS_KEY"])
    .replace("<endpoint host>", env["R2_ENDPOINT"].split("//", 1)[1].rstrip("/"))
)
con.execute(filled)
stored = list((scratch / "secrets").iterdir())
print("secret stored as", [p.name for p in stored], "mode", oct(stored[0].stat().st_mode & 0o777) if stored else None)

# Setup step 2: the views file, into a database file.
con.execute((repo / "sql" / "views.sql").read_text())
con.close()

# A new session: nothing but opening the database file.
con = connect()
print(
    "views after reopening:",
    sorted(r[0] for r in con.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()),
)

for doc in ("README.md", "docs/tables.md", "docs/queries.md"):
    for sql in blocks(repo / doc):
        if "CREATE PERSISTENT SECRET" in sql:
            continue
        title = " ".join(sql.split())[:70]
        began = time.monotonic()
        try:
            rows = []
            for statement in [s for s in sql.split(";") if s.strip()]:
                result = con.execute(statement)
                rows = result.fetchall() if result.description else []
            took = time.monotonic() - began
            shown = [tuple(str(v)[:60] for v in row) for row in rows[:3]]
            print(f"OK   {doc}: {title}\n       {len(rows)} rows in {took:.1f} s; first: {shown}")
        except Exception as exc:
            failed.append(title)
            print(f"FAIL {doc}: {title}\n       {type(exc).__name__}: {str(exc)[:300]}")

outputs = {p.name: p.stat().st_size for p in scratch.iterdir() if p.is_file() and p.name != "rimworld.duckdb"}
print("files the examples wrote:", outputs)
con.close()
shutil.rmtree(scratch, ignore_errors=True)
print(f"RESULT docs {'ok' if not failed else 'failed'} failed={len(failed)}")
sys.exit(1 if failed else 0)
