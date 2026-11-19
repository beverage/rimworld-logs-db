"""Run every SQL block in the docs against the live bucket, following the README's setup.

Works in a fresh temporary folder with its own DuckDB secret and extension
folders, so it proves the setup from nothing and leaves ~/.duckdb alone.
Prints row counts, the first rows of each result and timings. No query in the
docs returns log text or a log id.

Run:  uv run python scripts/check_docs.py [env file]
"""

import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

import duckdb

from rimworld_logs_db import settings

REPO = Path(__file__).resolve().parents[1]
DOCS = ("README.md", "docs/tables.md", "docs/queries.md")


def sql_blocks(path: Path) -> list[str]:
    """The contents of every ```sql block in a Markdown file."""
    return re.findall(r"```sql\n(.*?)```", path.read_text(), flags=re.S)


def connect(scratch: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(scratch / "rimworld.duckdb"))
    con.execute(f"SET extension_directory = '{scratch / 'extensions'}'")
    con.execute(f"SET secret_directory = '{scratch / 'secrets'}'")
    return con


def set_up(scratch: Path, env: dict[str, str]) -> None:
    """The README's two setup steps: store the secret, then load the views into a database file."""
    con = connect(scratch)
    secret_sql = next(block for block in sql_blocks(REPO / "README.md") if "CREATE PERSISTENT SECRET" in block)
    con.execute(
        secret_sql.replace("<access key id>", env["R2_ACCESS_KEY_ID"])
        .replace("<secret access key>", env["R2_SECRET_ACCESS_KEY"])
        .replace("<endpoint host>", env["R2_ENDPOINT"].split("//", 1)[1].rstrip("/"))
    )
    stored = list((scratch / "secrets").iterdir())
    mode = oct(stored[0].stat().st_mode & 0o777) if stored else None
    print("secret stored as", [path.name for path in stored], "mode", mode)
    con.execute((REPO / "sql" / "views.sql").read_text())
    con.close()


def run_block(con: duckdb.DuckDBPyConnection, doc: str, sql: str) -> bool:
    title = " ".join(sql.split())[:70]
    began = time.monotonic()
    try:
        rows = []
        for statement in [s for s in sql.split(";") if s.strip()]:
            result = con.execute(statement)
            rows = result.fetchall() if result.description else []
    except Exception as exc:
        print(f"FAIL {doc}: {title}\n       {type(exc).__name__}: {str(exc)[:300]}")
        return False
    took = time.monotonic() - began
    shown = [tuple(str(value)[:60] for value in row) for row in rows[:3]]
    print(f"OK   {doc}: {title}\n       {len(rows)} rows in {took:.1f} s; first: {shown}")
    return True


def main() -> int:
    if len(sys.argv) > 1:
        os.environ[settings.ENV_VARIABLE] = str(Path(sys.argv[1]).resolve())
    env = settings.load_env()
    scratch = Path(tempfile.mkdtemp(prefix="rimworld-logs-db-docs-"))
    (scratch / "secrets").mkdir()
    (scratch / "extensions").mkdir()
    # The examples that write a file write it to the current folder.
    os.chdir(scratch)

    set_up(scratch, env)
    # A new session: nothing but opening the database file.
    con = connect(scratch)
    views = con.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()
    print("views after reopening:", sorted(row[0] for row in views))

    failed = 0
    for doc in DOCS:
        for sql in sql_blocks(REPO / doc):
            if "CREATE PERSISTENT SECRET" not in sql:
                failed += not run_block(con, doc, sql)

    outputs = {p.name: p.stat().st_size for p in scratch.iterdir() if p.is_file() and p.name != "rimworld.duckdb"}
    print("files the examples wrote:", outputs)
    con.close()
    shutil.rmtree(scratch, ignore_errors=True)
    print(f"RESULT docs {'ok' if not failed else 'failed'} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
