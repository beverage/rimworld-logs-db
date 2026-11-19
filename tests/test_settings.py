"""The env file that holds the keys, and where files live."""

import pytest

from rimworld_logs_db import layout, settings
from rimworld_logs_db.common import ConfigError

ENV_FILE = """\
# the keys
A=1
B = two words
C=has=equals==
EMPTY=

not a setting
"""


@pytest.fixture
def env(tmp_path, monkeypatch) -> dict[str, str]:
    path = tmp_path / "env"
    path.write_text(ENV_FILE)
    monkeypatch.setenv(settings.ENV_VARIABLE, str(path))
    return settings.load_env()


def test_the_env_file_is_key_value_lines_with_comments_and_blanks_skipped(env):
    assert env == {"A": "1", "B": "two words", "C": "has=equals==", "EMPTY": ""}


def test_a_missing_or_empty_key_is_named_without_any_value(env):
    with pytest.raises(ConfigError, match="^not set in the env file: EMPTY, MISSING$"):
        settings.require(env, ("A", "EMPTY", "MISSING"))


def test_keys_that_are_all_set_pass(env):
    settings.require(env, ("A", "B", "C"))


def test_an_env_file_that_cannot_be_read_is_a_configuration_error(tmp_path, monkeypatch):
    monkeypatch.setenv(settings.ENV_VARIABLE, str(tmp_path / "absent"))
    with pytest.raises(ConfigError, match="cannot read"):
        settings.load_env()


def test_a_part_is_five_files_under_the_same_shard_name(tmp_path):
    shard = "week=2026-W46/part-000.parquet"
    assert layout.part_files(tmp_path, shard) == {
        f"logs/{shard}": tmp_path / "shards" / shard,
        f"patches/HugslibLogs/{shard}": tmp_path / "patches" / "HugslibLogs" / shard,
        f"patches/ActivePatches/{shard}": tmp_path / "patches" / "ActivePatches" / shard,
        f"patches/HarmonyPatches/{shard}": tmp_path / "patches" / "HarmonyPatches" / shard,
        f"patches/HarmonyPatchNamespaces/{shard}": tmp_path / "patches" / "HarmonyPatchNamespaces" / shard,
    }
