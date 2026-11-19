#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=17", "boto3>=1.36"]
# ///
"""Builds the RimWorld Logs DB archive from the public HugsLib log gists.

HugsLib and the standalone Log Publisher upload every shared log as a public
gist under one GitHub account, HugsLibRecordKeeper. This tool lists those
gists, downloads each log once, packs a week at a time into Parquet, uploads
the files to a private R2 bucket, and records one row per log in Supabase.

The code is in the `logsdb` package beside this file, one module per step.
Each module opens with why its step works the way it does.

USAGE

  corpus.py index --since 2024-11-18  list uploads back to a date
  corpus.py index --new               list only what is newer than the index
  uv run corpus.py run                fetch, pack, push and sync every week
  uv run corpus.py run --weeks 4      the same, stopping after four weeks
  uv run corpus.py remove LINK        take one log out, by gist link or id
  corpus.py status                    counts by state and disk use

  The steps `run` chains also stand alone: fetch, pack, push, sync.
  `corpus.py <command> --help` lists each one's options.

  index, fetch, sync and status need only Python. pack, push, run and remove
  need pyarrow and boto3, which `uv run` installs from the header above.

  --data DIR goes before the command and names the data folder.

DATA RULES

  The data folder stays outside every repo. These are anonymous uploads made
  to get help: publish counts and rates, never a log id or an excerpt tied to
  a finding. Log text is untrusted input, written by any mod or player.
"""

import sys
from pathlib import Path

# The logsdb package sits beside this file; find it however Python was started.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from logsdb.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
