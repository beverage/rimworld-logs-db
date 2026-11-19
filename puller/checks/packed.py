# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.1", "pyarrow>=17"]
# ///
"""Check packed patch tables on disk against the log text they came from. Aggregates only.

Rebuilds every log's patch set from the four tables and compares it with a
fresh parse of the log text. It needs the packed files, so pack with
--keep-staging and check before pushing, or run it on files pulled back down.

Run:  uv run puller/checks/packed.py [data folder]
"""

import glob
import importlib.util
import os
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

spec = importlib.util.spec_from_file_location("corpus", Path(__file__).resolve().parents[1] / "corpus.py")
corpus = importlib.util.module_from_spec(spec)
spec.loader.exec_module(corpus)
root = str(Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else corpus.DEFAULT_DATA).rstrip("/")

failed = []


def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (f" ({detail})" if detail else ""))
    if not ok:
        failed.append(name)


def parts(table):
    return sorted(glob.glob(f"{root}/patches/{table}/*/*.parquet"))


def size(table):
    return sum(os.path.getsize(p) for p in parts(table))


# dictionaries
patches, patch_rows, patch_namespace = {}, 0, {}
for path in parts("HarmonyPatches"):
    for row in pq.read_table(path).to_pylist():
        patch_rows += 1
        patches[row["HarmonyPatchId"]] = (row["Target"], row["Kind"], row["Priority"], row["Method"])
        patch_namespace[row["HarmonyPatchId"]] = row["HarmonyPatchNamespaceId"]
namespaces, namespace_rows = {}, 0
for path in parts("HarmonyPatchNamespaces"):
    for row in pq.read_table(path).to_pylist():
        namespace_rows += 1
        namespaces[row["HarmonyPatchNamespaceId"]] = row["Namespace"]

check("patch ids are unique across parts", patch_rows == len(patches), f"{patch_rows} rows")
check("patch ids run 1..N with no gaps", sorted(patches) == list(range(1, len(patches) + 1)))
check("each distinct patch is listed once", len(set(patches.values())) == len(patches))
check("namespace ids are unique across parts", namespace_rows == len(namespaces), f"{namespace_rows} rows")
check(
    "every patch's namespace matches its method",
    all(namespaces.get(patch_namespace[i]) == corpus.namespace_of(patches[i][3]) for i in patches),
)

# the join table
active, active_rows, ordered = {}, 0, True
for path in parts("ActivePatches"):
    table = pq.read_table(path)
    pairs = list(zip(table["HugslibLogId"].to_pylist(), table["HarmonyPatchId"].to_pylist()))
    ordered = ordered and pairs == sorted(pairs)
    active_rows += len(pairs)
    for log_no, patch in pairs:
        active.setdefault(log_no, set()).add(patch)
check("ActivePatches is sorted by log then patch", ordered)
check("ActivePatches has no repeated pair", active_rows == sum(len(v) for v in active.values()), f"{active_rows} rows")

# the logs
log_guid = {}
for path in parts("HugslibLogs"):
    for row in pq.read_table(path).to_pylist():
        log_guid[row["HugslibLogId"]] = row["GUID"]
logs = mismatched = wrong_guid = expected_rows = 0
for path in sorted(glob.glob(f"{root}/shards/*/*.parquet")):
    table = pq.read_table(path, columns=["hugslib_log_id", "id", "text"])
    for log_no, gist_id, text in zip(table["hugslib_log_id"].to_pylist(), table["id"].to_pylist(), table["text"].to_pylist()):
        logs += 1
        expect = set(corpus.parse_patches(text))
        expected_rows += len(expect)
        got = {patches[p] for p in active.get(log_no, ())}
        mismatched += expect != got
        wrong_guid += log_guid.get(log_no) != gist_id
check("every log's patches rebuild exactly from the tables", mismatched == 0, f"{logs} logs, {mismatched} mismatched")
check("HugslibLogs matches the log files", wrong_guid == 0 and len(log_guid) == logs, f"{len(log_guid)} rows")
check("row count equals the parser's count", active_rows == expected_rows, f"{expected_rows}")

print(f"-- files: ActivePatches {len(parts('ActivePatches'))} parts, {size('ActivePatches')} bytes, {size('ActivePatches') / active_rows:.2f} bytes a row")
print(f"-- files: HarmonyPatches {size('HarmonyPatches')} bytes for {len(patches)} patches, {size('HarmonyPatches') / len(patches):.1f} bytes each")
print(f"-- files: HarmonyPatchNamespaces {size('HarmonyPatchNamespaces')} bytes for {len(namespaces)}; HugslibLogs {size('HugslibLogs')} bytes")

# the same data through SQL, under the table names in the design
con = duckdb.connect()
for table in ("HugslibLogs", "ActivePatches", "HarmonyPatches", "HarmonyPatchNamespaces"):
    con.execute(f"create view {table} as select * exclude (week) from read_parquet('{root}/patches/{table}/*/*.parquet', hive_partitioning=true)")
print("-- SQL: rows by kind", con.execute("select Kind, count(*) from ActivePatches join HarmonyPatches using (HarmonyPatchId) group by 1 order by 2 desc").fetchall())
print(
    "-- SQL: patches by how many logs carry them (1 log, 2-9, 10+)",
    con.execute(
        "select count(*) filter (where n = 1), count(*) filter (where n between 2 and 9), count(*) filter (where n >= 10)"
        " from (select HarmonyPatchId, count(*) n from ActivePatches group by 1)"
    ).fetchall(),
)
print(
    "-- SQL: logs, and the widest three-way join row count",
    con.execute(
        "select count(distinct l.HugslibLogId), count(*) from HugslibLogs l join ActivePatches a using (HugslibLogId)"
        " join HarmonyPatches p using (HarmonyPatchId) join HarmonyPatchNamespaces n using (HarmonyPatchNamespaceId)"
    ).fetchall(),
)
print(f"RESULT patches {'ok' if not failed else 'failed'} failed={len(failed)}")
sys.exit(1 if failed else 0)
