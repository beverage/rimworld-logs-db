"""The command line: arguments in, exit codes and RESULT lines out."""

import pytest

from rimworld_logs_db import cli, settings


def test_status_runs_on_an_empty_data_folder(data, capsys):
    assert cli.main(["--data", str(data), "status"]) == 0
    assert "distinct patches 0, namespaces 0" in capsys.readouterr().out


def test_a_missing_env_file_ends_the_command_with_a_message(data, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(settings.ENV_VARIABLE, str(tmp_path / "absent"))
    assert cli.main(["--data", str(data), "sync"]) == 1
    assert "error: cannot read" in capsys.readouterr().err


def test_a_date_must_be_in_iso_form(capsys):
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["index", "--since", "last year"])
    assert "not a date in YYYY-MM-DD form" in capsys.readouterr().err


def test_remove_takes_a_gist_link(archive, capsys):
    link = f"https://gist.github.com/HugsLibRecordKeeper/{archive.ids.A}"
    assert cli.main(["--data", str(archive.data), "remove", "--dry-run", link]) == 0
    assert "RESULT remove dry run asked=1 removed=0" in capsys.readouterr().out
