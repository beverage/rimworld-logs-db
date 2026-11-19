"""Where things live: in the data folder on disk, and in the bucket.

On disk, inside the data folder:

    index.sqlite                                  the local index
    staging/<week>/<gist id>.txt.gz               a downloaded log, until its week is packed
    shards/week=<week>/part-NNN.parquet           the packed log file, until it is uploaded
    patches/<Table>/week=<week>/part-NNN.parquet  the four patch tables, likewise

In the bucket:

    logs/week=<week>/part-NNN.parquet
    patches/<Table>/week=<week>/part-NNN.parquet
    state/index.sqlite                            a backup of the local index

A "shard" names one part: `week=2026-W46/part-000.parquet`. It is the same
string in every path above.
"""

from __future__ import annotations

from pathlib import Path

LOGS = "logs"
PATCH_TABLES = ("HugslibLogs", "ActivePatches", "HarmonyPatches", "HarmonyPatchNamespaces")
STATE_KEY = "state/index.sqlite"


def staged_path(staging: Path, week: str, gist_id: str) -> Path:
    """Where a downloaded log waits, gzipped, until its week is packed."""
    return staging / week / f"{gist_id}.txt.gz"


def part_paths(data: Path, shard: str) -> dict[str, Path]:
    """The five files of a part on disk, by table name."""
    return {LOGS: data / "shards" / shard, **{name: data / "patches" / name / shard for name in PATCH_TABLES}}


def bucket_key(table: str, shard: str) -> str:
    """The bucket key of one table's file in a part."""
    return f"logs/{shard}" if table == LOGS else f"patches/{table}/{shard}"


def part_files(data: Path, shard: str) -> dict[str, Path]:
    """Bucket key to local file, for the five files of a part."""
    return {bucket_key(table, shard): path for table, path in part_paths(data, shard).items()}
