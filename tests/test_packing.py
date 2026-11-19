"""Packing a week of staged logs into the five Parquet files."""

from collections import Counter

import pyarrow.parquet as pq

from rimworld_logs_db import layout, packing, parsing


def column(path, name: str) -> list:
    return pq.read_table(path, columns=[name]).column(name).to_pylist()


def state_of(con, gist_id: str) -> str:
    return con.execute("SELECT state FROM gists WHERE id = ?", (gist_id,)).fetchone()[0]


def test_a_week_becomes_five_files_and_leaves_staging(archive):
    assert all(path.is_file() for path in archive.paths.values())
    assert not (archive.data / "staging" / archive.week).exists()
    assert list(archive.data.rglob("*.tmp")) == []


def test_logs_are_numbered_in_upload_order(archive):
    ids = archive.ids
    assert column(archive.paths["logs"], "id") == [ids.A, ids.B, ids.C, ids.D, ids.E, ids.F]
    assert column(archive.paths["logs"], "hugslib_log_id") == [1, 2, 3, 4, 5, 6]


def test_a_log_row_holds_the_text_and_the_facts_read_off_it(archive):
    row = pq.read_table(archive.paths["logs"]).to_pylist()[0]
    assert row["text"] == archive.texts[archive.ids.A]
    assert row["sha256"] == archive.hashes[archive.ids.A]
    assert row["mods"] == ["Ludeon.RimWorld", "author.mod1"]
    assert (row["game_version"], row["game_rev"], row["mod_count"], row["trimmed"]) == ("1.6.4871", 1, 2, False)


def test_hugslib_logs_pairs_each_number_with_its_gist(archive):
    rows = pq.read_table(archive.paths["HugslibLogs"]).to_pylist()
    assert [(row["HugslibLogId"], row["GUID"]) for row in rows] == [
        (archive.log_no(gist_id), gist_id) for gist_id in vars(archive.ids).values()
    ]


def test_each_distinct_patch_is_numbered_once(archive):
    # Four different logs with two patches of their own each, and one patch they all share.
    patches = pq.read_table(archive.paths["HarmonyPatches"]).to_pylist()
    assert sorted(row["HarmonyPatchId"] for row in patches) == list(range(1, 10))
    assert len({(row["Target"], row["Kind"], row["Priority"], row["Method"]) for row in patches}) == 9


def test_active_patches_is_sorted_by_log_then_patch(archive):
    table = pq.read_table(archive.paths["ActivePatches"])
    pairs = list(zip(table["HugslibLogId"].to_pylist(), table["HarmonyPatchId"].to_pylist(), strict=True))
    assert pairs == sorted(pairs)
    assert Counter(log_no for log_no, _ in pairs) == {log_no: 3 for log_no in range(1, 7)}


def test_every_logs_patches_rebuild_from_the_tables(archive):
    patches = {
        row["HarmonyPatchId"]: (row["Target"], row["Kind"], row["Priority"], row["Method"])
        for row in pq.read_table(archive.paths["HarmonyPatches"]).to_pylist()
    }
    namespaces = {
        row["HarmonyPatchNamespaceId"]: row["Namespace"]
        for row in pq.read_table(archive.paths["HarmonyPatchNamespaces"]).to_pylist()
    }
    active: dict[int, set] = {}
    for row in pq.read_table(archive.paths["ActivePatches"]).to_pylist():
        active.setdefault(row["HugslibLogId"], set()).add(patches[row["HarmonyPatchId"]])
    for row in pq.read_table(archive.paths["logs"], columns=["hugslib_log_id", "text"]).to_pylist():
        assert active[row["hugslib_log_id"]] == set(parsing.parse_patches(row["text"]))
    assert sorted(namespaces.values()) == ["Mod1", "Mod2", "Mod3", "Mod4", "Shared"]


def test_a_later_week_lists_only_the_patches_it_introduces(archive):
    archive.stage("1" * 32, "2026-10-06T10:00:00Z", archive.make_log(1))
    archive.stage("2" * 32, "2026-10-06T10:00:01Z", archive.make_log(9))
    assert packing.pack(archive.data, include_open=True, level=3) == 0
    later = layout.part_paths(archive.data, "week=2026-W41/part-000.parquet")
    introduced = pq.read_table(later["HarmonyPatches"]).to_pylist()
    assert sorted(row["HarmonyPatchId"] for row in introduced) == [10, 11]
    assert {row["Method"] for row in introduced} == {"Mod9.Patches.Pre", "Mod9.Patches.Trans"}
    assert column(later["HarmonyPatchNamespaces"], "Namespace") == ["Mod9"]
    assert column(later["logs"], "hugslib_log_id") == [7, 8]


def test_a_week_with_downloads_outstanding_waits_unless_told_otherwise(data, con, stage_log):
    stage_log("1" * 32, "2026-10-06T10:00:00Z", b"a downloaded log")
    con.execute(
        "INSERT INTO gists (id, created_at, week, size, raw_url, uploader)"
        " VALUES (?, '2026-10-06T11:00:00Z', '2026-W41', 10, 'u', 'hugslib')",
        ("2" * 32,),
    )
    con.commit()
    assert packing.pack(data, include_open=True, level=3) == 0
    assert state_of(con, "1" * 32) == "fetched"
    assert packing.pack(data, include_open=True, partial=True, level=3) == 0
    assert state_of(con, "1" * 32) == "packed"
    assert state_of(con, "2" * 32) == "pending"


def test_a_log_whose_staged_copy_is_missing_goes_back_in_the_queue(data, con, stage_log):
    stage_log("1" * 32, "2026-10-06T10:00:00Z", b"a downloaded log").unlink()
    assert packing.pack(data, include_open=True, level=3) == 0
    assert state_of(con, "1" * 32) == "pending"
    assert con.execute("SELECT COUNT(*) FROM parts").fetchone()[0] == 0
    assert list(data.rglob("*.parquet*")) == []
