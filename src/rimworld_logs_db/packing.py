"""pack: write a finished week of staged logs as Parquet.

A week with nothing left to download becomes one part: five Parquet files
written together.

    logs                     one row per log: its text and the facts parsed from it
    HugslibLogs              one row per log, in the shape the patch tables join to
    ActivePatches            one row per log and Harmony patch active in it
    HarmonyPatches           one row per distinct patch, first seen in this part
    HarmonyPatchNamespaces   one row per namespace, first seen in this part

A log carries about 4,000 patches, so ActivePatches is the large table: two
years is roughly a billion rows. Sorted by log and patch and delta-encoded,
it takes about a fifth of a byte a row. That only works because a patch's id
is a small number handed out the first time the patch is seen, and never
changed. The index keeps that dictionary, and a part lists only the patches
it introduced, so the dictionary tables grow by appending.

The five files are written under temporary names and renamed together once
their row counts check out. The part's number comes from the index, not from
what is on disk, so a part left behind by a crashed run is overwritten rather
than duplicated.
"""

from __future__ import annotations

import contextlib
import gzip
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .common import now_iso, parse_time, say, week_of
from .index_db import open_db
from .layout import LOGS, PATCH_TABLES, part_paths, staged_path
from .parsing import Patch, decode, namespace_of, parse_log, parse_patches, patch_key

# Sorted ids delta-encode to a fraction of a byte a row; a dictionary would undo that.
ACTIVE_PATCHES_ENCODING = {
    "use_dictionary": False,
    "column_encoding": {"HugslibLogId": "DELTA_BINARY_PACKED", "HarmonyPatchId": "DELTA_BINARY_PACKED"},
}

# A batch is written as one row group, so a reader can skip to the logs it wants.
BATCH_LOGS = 64
BATCH_BYTES = 64 << 20

TIMESTAMP = pa.timestamp("s", tz="UTC")
SCHEMAS = {
    LOGS: pa.schema(
        [
            ("hugslib_log_id", pa.int32()),
            ("id", pa.string()),
            ("created_at", TIMESTAMP),
            ("size", pa.int64()),
            ("uploader", pa.string()),
            ("sha256", pa.string()),
            ("utf8_ok", pa.bool_()),
            ("game_version", pa.string()),
            ("game_rev", pa.int32()),
            ("mod_count", pa.int32()),
            ("dlcs", pa.list_(pa.string())),
            ("mods", pa.list_(pa.string())),
            ("trimmed", pa.bool_()),
            ("line_count", pa.int32()),
            ("text", pa.large_string()),
        ]
    ),
    "HugslibLogs": pa.schema([("HugslibLogId", pa.int32()), ("GUID", pa.string()), ("UploadTime", TIMESTAMP)]),
    "ActivePatches": pa.schema([("HugslibLogId", pa.int32()), ("HarmonyPatchId", pa.int32())]),
    "HarmonyPatches": pa.schema(
        [
            ("HarmonyPatchId", pa.int32()),
            ("Target", pa.string()),
            ("Kind", pa.string()),
            ("Priority", pa.int32()),
            ("Method", pa.string()),
            ("HarmonyPatchNamespaceId", pa.int32()),
        ]
    ),
    "HarmonyPatchNamespaces": pa.schema([("HarmonyPatchNamespaceId", pa.int32()), ("Namespace", pa.string())]),
}

# One log on its way into a batch: its row for the logs table, and its parsed patches.
PendingLog = tuple[dict, list[Patch]]


class PatchDictionary:
    """Gives every distinct patch, and every namespace, a small permanent number.

    The numbers live in the index, keyed by a hash of the patch, so the index
    holds no patch text. Lookups go through a temporary table, which keeps a
    batch of a quarter of a million patches to one query.
    """

    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        con.execute("CREATE TEMP TABLE IF NOT EXISTS wanted (key INTEGER PRIMARY KEY)")
        self.namespace_ids = dict(con.execute("SELECT name, id FROM namespace_ids"))
        self.next_id = (con.execute("SELECT MAX(id) FROM patch_ids").fetchone()[0] or 0) + 1
        self.new_patches = 0
        self.collisions = 0

    def keyed(self, logs: list[PendingLog]) -> tuple[dict[int, Patch], list[tuple[int, list[int]]]]:
        """Hash a batch's patches. Returns every distinct patch by key, and each log's keys."""
        patches, per_log = {}, []
        for row, entries in logs:
            keys = []
            for entry in entries:
                key = patch_key(entry)
                # Two different patches sharing a 64-bit key would be merged; count it if it ever happens.
                if patches.setdefault(key, entry) != entry:
                    self.collisions += 1
                keys.append(key)
            per_log.append((row["hugslib_log_id"], keys))
        return patches, per_log

    def known(self, keys) -> dict[int, int]:
        """The ids already given to these patch keys."""
        self.con.execute("DELETE FROM wanted")
        self.con.executemany("INSERT INTO wanted (key) VALUES (?)", ((key,) for key in keys))
        return dict(self.con.execute("SELECT p.key, p.id FROM wanted w JOIN patch_ids p ON p.key = w.key"))

    def number(self, patches: dict[int, Patch]) -> tuple[dict[int, int], list[dict], list[dict]]:
        """Ids for these patches, by key, giving new ones where needed.

        Also returns the rows the new patches and new namespaces add to the
        HarmonyPatches and HarmonyPatchNamespaces tables.
        """
        ids = self.known(patches)
        # New patches take the next ids in method order, which keeps one mod's
        # patches on neighbouring ids.
        fresh = sorted(
            (key for key in patches if key not in ids),
            key=lambda key: (patches[key][3], patches[key][0], patches[key][1]),
        )
        patch_rows, namespace_rows = [], []
        for key in fresh:
            target, kind, priority, method = patches[key]
            namespace = namespace_of(method)
            if namespace not in self.namespace_ids:
                self.namespace_ids[namespace] = len(self.namespace_ids) + 1
                namespace_rows.append(
                    {"HarmonyPatchNamespaceId": self.namespace_ids[namespace], "Namespace": namespace}
                )
            ids[key] = self.next_id
            patch_rows.append(
                {
                    "HarmonyPatchId": self.next_id,
                    "Target": target,
                    "Kind": kind,
                    "Priority": priority,
                    "Method": method,
                    "HarmonyPatchNamespaceId": self.namespace_ids[namespace],
                }
            )
            self.next_id += 1
        self.con.executemany("INSERT INTO patch_ids (key, id) VALUES (?, ?)", [(key, ids[key]) for key in fresh])
        self.con.executemany(
            "INSERT INTO namespace_ids (name, id) VALUES (?, ?)",
            [(row["Namespace"], row["HarmonyPatchNamespaceId"]) for row in namespace_rows],
        )
        self.new_patches += len(fresh)
        return ids, patch_rows, namespace_rows


class PartWriter:
    """The five files of one part, written under temporary names and published together."""

    def __init__(self, outputs: dict[str, Path], level: int) -> None:
        self.outputs = outputs
        self.temps = {name: path.with_name(path.name + ".tmp") for name, path in outputs.items()}
        self.patch_rows = 0
        self.stack = contextlib.ExitStack()
        self.writers = {}
        for name, path in outputs.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            options = ACTIVE_PATCHES_ENCODING if name == "ActivePatches" else {}
            self.writers[name] = self.stack.enter_context(
                pq.ParquetWriter(
                    self.temps[name], SCHEMAS[name], compression="zstd", compression_level=level, **options
                )
            )

    def write_rows(self, name: str, rows: list[dict]) -> None:
        self.writers[name].write_table(pa.Table.from_pylist(rows, schema=SCHEMAS[name]))

    def write_batch(self, logs: list[PendingLog], dictionary: PatchDictionary) -> None:
        """Write one batch of logs as a row group in every table."""
        self.write_rows(LOGS, [row for row, _ in logs])
        self.write_rows(
            "HugslibLogs",
            [
                {"HugslibLogId": row["hugslib_log_id"], "GUID": row["id"], "UploadTime": row["created_at"]}
                for row, _ in logs
            ],
        )
        patches, per_log = dictionary.keyed(logs)
        ids, patch_rows, namespace_rows = dictionary.number(patches)
        if patch_rows:
            self.write_rows("HarmonyPatches", patch_rows)
        if namespace_rows:
            self.write_rows("HarmonyPatchNamespaces", namespace_rows)

        log_column, patch_column = [], []
        for log_no, keys in per_log:
            mine = sorted({ids[key] for key in keys})
            log_column += [log_no] * len(mine)
            patch_column += mine
        self.writers["ActivePatches"].write_table(
            pa.table(
                {
                    "HugslibLogId": pa.array(log_column, pa.int32()),
                    "HarmonyPatchId": pa.array(patch_column, pa.int32()),
                },
                schema=SCHEMAS["ActivePatches"],
            )
        )
        self.patch_rows += len(patch_column)

    def close(self) -> None:
        self.stack.close()

    def rows_written(self, name: str) -> int:
        return pq.ParquetFile(self.temps[name]).metadata.num_rows

    def publish(self) -> None:
        for name, temp in self.temps.items():
            os.replace(temp, self.outputs[name])

    def discard(self) -> None:
        for temp in self.temps.values():
            temp.unlink(missing_ok=True)


@dataclass
class Packed:
    """What packing one week produced."""

    logs: int = 0
    missing: int = 0
    text_bytes: int = 0
    patch_rows: int = 0
    logs_file_bytes: int = 0
    patch_file_bytes: int = 0
    failed: bool = False


class Packer:
    """Packs weeks one after another, carrying the log counter and the patch dictionary between them."""

    def __init__(self, con: sqlite3.Connection, data: Path, level: int, keep_staging: bool) -> None:
        self.con, self.data = con, data
        self.level, self.keep_staging = level, keep_staging
        self.dictionary = PatchDictionary(con)
        self.next_log = (con.execute("SELECT MAX(log_no) FROM gists").fetchone()[0] or 0) + 1

    def pack_week(self, week: str) -> Packed:
        con, result = self.con, Packed()
        rows = con.execute(
            "SELECT id, created_at, size, uploader, sha256 FROM gists"
            " WHERE week = ? AND state = 'fetched' ORDER BY created_at",
            (week,),
        ).fetchall()
        part = con.execute(
            "SELECT COUNT(DISTINCT shard) FROM gists WHERE week = ? AND state = 'packed'", (week,)
        ).fetchone()[0]
        shard = f"week={week}/part-{part:03d}.parquet"
        outputs = part_paths(self.data, shard)
        writer = PartWriter(outputs, self.level)

        batch, batch_bytes, done, facts = [], 0, [], []
        with contextlib.closing(writer):
            for gist_id, created_at, size, uploader, sha256 in rows:
                staged = staged_path(self.data / "staging", week, gist_id)
                try:
                    with gzip.open(staged, "rb") as handle:
                        raw = handle.read()
                except FileNotFoundError:
                    # The staged copy is gone, so the log goes back in the download queue.
                    con.execute("UPDATE gists SET state = 'pending' WHERE id = ?", (gist_id,))
                    result.missing += 1
                    continue
                text, utf8_ok = decode(raw)
                parsed = parse_log(text)
                log_row = {
                    "hugslib_log_id": self.next_log,
                    "id": gist_id,
                    "created_at": parse_time(created_at),
                    "size": size,
                    "uploader": uploader,
                    "sha256": sha256,
                    "utf8_ok": utf8_ok,
                    "text": text,
                    **parsed,
                }
                batch.append((log_row, parse_patches(text)))
                facts.append(
                    (
                        gist_id,
                        parsed["game_version"],
                        parsed["game_rev"],
                        parsed["mod_count"],
                        ",".join(parsed["dlcs"]),
                        int(parsed["trimmed"]),
                        parsed["line_count"],
                        int(utf8_ok),
                    )
                )
                done.append((gist_id, staged, self.next_log))
                self.next_log += 1
                batch_bytes += len(raw)
                result.text_bytes += len(raw)
                if len(batch) >= BATCH_LOGS or batch_bytes >= BATCH_BYTES:
                    writer.write_batch(batch, self.dictionary)
                    batch, batch_bytes = [], 0
            if batch:
                writer.write_batch(batch, self.dictionary)

        if not done:
            writer.discard()
            con.commit()
            return result
        written, active = writer.rows_written(LOGS), writer.rows_written("ActivePatches")
        if written != len(done) or active != writer.patch_rows:
            con.rollback()
            writer.discard()
            say(
                f"{week}: wrote {written} log rows for {len(done)} logs and {active} patch rows"
                f" for {writer.patch_rows}, leaving the week staged"
            )
            result.failed = True
            return result

        writer.publish()
        con.executemany("INSERT OR REPLACE INTO facts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", facts)
        con.executemany(
            "UPDATE gists SET state = 'packed', shard = ?, log_no = ? WHERE id = ?",
            [(shard, log_no, gist_id) for gist_id, _, log_no in done],
        )
        con.execute("INSERT OR REPLACE INTO parts (shard, logs, uploaded_at) VALUES (?, ?, NULL)", (shard, len(done)))
        con.commit()
        if not self.keep_staging:
            for _, staged, _ in done:
                staged.unlink(missing_ok=True)
            with contextlib.suppress(OSError):
                (self.data / "staging" / week).rmdir()

        result.logs, result.patch_rows = len(done), writer.patch_rows
        result.logs_file_bytes = outputs[LOGS].stat().st_size
        result.patch_file_bytes = sum(outputs[name].stat().st_size for name in PATCH_TABLES)
        say(
            f"  {week}: {result.logs} logs -> {shard} ({result.logs_file_bytes / 1e6:.1f} MB),"
            f" {result.patch_rows} patch rows ({result.patch_file_bytes / 1e6:.1f} MB)"
        )
        return result


def pack(
    data: Path,
    week: str | None = None,
    include_open: bool = False,
    partial: bool = False,
    keep_staging: bool = False,
    level: int = 9,
) -> int:
    """Pack every week that is ready, or just `week`.

    A week is ready when none of its logs is still waiting to download and it
    is not the week in progress. `partial` and `include_open` lift those two
    conditions. `level` is the zstd level.
    """
    con = open_db(data)
    packer = Packer(con, data, level, keep_staging)
    open_week = week_of(now_iso())
    weeks = [w for (w,) in con.execute("SELECT DISTINCT week FROM gists WHERE state = 'fetched' ORDER BY week")]
    total, packed_weeks, skipped = Packed(), 0, 0
    for candidate in weeks:
        if week and candidate != week:
            continue
        waiting = con.execute(
            "SELECT COUNT(*) FROM gists WHERE week = ? AND state = 'pending'", (candidate,)
        ).fetchone()[0]
        if (candidate == open_week and not include_open) or (waiting and not partial):
            skipped += 1
            continue
        result = packer.pack_week(candidate)
        if result.failed:
            return 1
        total.missing += result.missing
        total.text_bytes += result.text_bytes
        if result.logs:
            packed_weeks += 1
            total.logs += result.logs
            total.patch_rows += result.patch_rows
            total.logs_file_bytes += result.logs_file_bytes
            total.patch_file_bytes += result.patch_file_bytes

    print(
        f"RESULT pack ok weeks={packed_weeks} logs={total.logs} skipped_weeks={skipped} missing={total.missing}"
        f" text_mb={total.text_bytes / 1e6:.1f} parquet_mb={total.logs_file_bytes / 1e6:.1f}"
        f" patch_rows={total.patch_rows} new_patches={packer.dictionary.new_patches}"
        f" patches_mb={total.patch_file_bytes / 1e6:.1f} key_collisions={packer.dictionary.collisions}"
    )
    return 0
