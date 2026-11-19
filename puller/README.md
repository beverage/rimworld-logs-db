# The puller

`corpus.py` builds the archive. It lists the public gists under `HugsLibRecordKeeper`, downloads each log once, packs a week of logs into Parquet, uploads the files to an R2 bucket, and records one row per log in a Supabase table.

You only need this folder to maintain the archive. To query it, the [main README](../README.md) is enough.

## What it needs

- Python 3.11 or newer, and [uv](https://docs.astral.sh/uv/). The packing, upload and removal steps use `pyarrow` and `boto3`, which `uv run` installs from the header of `corpus.py`.
- A GitHub token for listing gists. It is read from `GITHUB_TOKEN` or `GH_TOKEN`, or from the `gh` CLI if you are signed in.
- An R2 bucket and an API token with "Object Read & Write" on it.
- A Supabase project with [schema.sql](schema.sql) applied, and its secret key.

## How the code is laid out

`corpus.py` is only the entry point. The work is in the `logsdb` package beside it, one module per step of the pipeline:

| Module | What it does |
|---|---|
| `listing.py` | `index`: lists the gists into the local index |
| `download.py` | `fetch`: downloads each log once |
| `parsing.py` | reads the mod list, game version and Harmony patches off a log |
| `packing.py` | `pack`: writes a finished week as Parquet |
| `bucket.py` | `push`: uploads a week to R2 and drops the local copy |
| `supabase.py` | `sync`: the `logs` and `removed_logs` tables |
| `removal.py` | `remove`: takes one log out |
| `pipeline.py` | `run`, which chains the steps week by week, and `status` |

Five smaller modules support those: `index_db.py` (the local SQLite index), `layout.py` (where files live on disk and in the bucket), `settings.py` (names, defaults and the env file), `common.py` (messages, timestamps and errors) and `cli.py` (the command line).

Each module opens with a note on why its step works the way it does, so that is the place to start reading. `parsing.py` has no dependencies and is the part most likely to be useful on its own.

## The env file

Keys are read from `~/.config/rimworld-logs-db/env`, or from the path in `RIMWORLD_LOGS_DB_ENV`. One `KEY=value` per line, no quotes:

```
R2_ENDPOINT=https://<account id>.eu.r2.cloudflarestorage.com
R2_BUCKET=hugslib-logs
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
SUPABASE_URL=https://<project ref>.supabase.co
SUPABASE_SECRET_KEY=
```

A bucket created in R2's EU jurisdiction is served only from the `.eu.` host. Through the default host, `https://<account id>.r2.cloudflarestorage.com`, every call to it returns "access denied".

`uv run puller/checks/credentials.py` confirms the keys work without printing them.

## Commands

| Command | What it does |
|---|---|
| `corpus.py index --since 2024-11-18` | Lists uploads back to a date into the local index. |
| `corpus.py index --new` | Lists only uploads newer than the index already holds. |
| `uv run corpus.py run` | For each week, newest first: download, pack, upload, sync. |
| `uv run corpus.py run --weeks 4` | The same, stopping after four weeks. |
| `uv run corpus.py remove <gist link>` | Takes one log out of the archive. See below. |
| `corpus.py status` | Counts by state, and what is waiting to upload or sync. |

`run` chains four steps that also exist as commands of their own, for redoing one by hand: `fetch`, `pack`, `push` and `sync`. Every command resumes where an earlier one stopped. The week still in progress is skipped, because uploads are still arriving for it.

## Pace

**Downloads** go to GitHub's raw file host without a token, at 6 requests a second by default (`--rps`). A 403 or 429 pauses every download for the time the response asks, and five refusals in a row end the run. One week of 2,123 logs took 7 minutes from download to upload.

**Listing** is slower than it looks. A list request for this account takes about 10 seconds, and GitHub's secondary rate limit allows about 90 seconds of request time a minute, so pages are read one at a time. Two years is about 2,450 pages. The listing and a `run` can work at the same time: both go newest first, and the listing stays ahead.

## What stays on disk

Everything local lives in one data folder, `~/.local/share/rimworld-logs-db` unless `--data` or `RIMWORLD_LOGS_DB_DATA` says otherwise.

- `index.sqlite` is the working state: every gist, how far it has got, and the dictionary that gives each Harmony patch its number. `run` copies it to `state/index.sqlite` in the bucket every eight weeks and when it finishes.
- `staging/`, `shards/` and `patches/` hold one week while it is being processed. After the upload is checked against the file sizes, they are deleted.

A log is compressed as it arrives and is never written to disk as plain text.

## What it writes to the bucket

```
logs/week=2026-W46/part-000.parquet
patches/HugslibLogs/week=2026-W46/part-000.parquet
patches/ActivePatches/week=2026-W46/part-000.parquet
patches/HarmonyPatches/week=2026-W46/part-000.parquet
patches/HarmonyPatchNamespaces/week=2026-W46/part-000.parquet
state/index.sqlite
```

A week normally has one part. A second part appears when logs for that week are fetched later, for example after retrying failed downloads.

## Removing a log

When someone asks for their log to be taken out, pass its link or its gist id:

```
uv run puller/corpus.py remove https://gist.github.com/HugsLibRecordKeeper/<gist id> --dry-run
uv run puller/corpus.py remove https://gist.github.com/HugsLibRecordKeeper/<gist id>
```

The dry run downloads the week's files, reports how many rows would go, and changes nothing. The real run then does four things:

1. Rewrites three of that week's files without the log: the log file, `HugslibLogs` and `ActivePatches`. Each is uploaded over the old one and its stored size is checked.
2. Deletes the log's row from the Supabase table.
3. Adds the gist id and the log's content hash to the `removed_logs` table in Supabase, and to the local index. This works for a log the index has not reached yet.
4. Copies the updated index to the bucket.

The Supabase table is the list's home, because the local index can be lost or rebuilt. `index` and `run` copy the table down before they list anything, so a fresh index on any machine still refuses a removed log. `fetch` also discards a download whose hash is on the list, which catches the same log uploaded again under a new id. If `index` cannot read the table it stops; `--no-supabase` overrides that.

The patch dictionary is not rewritten. It lists mods' patch methods, which are not about any player, and its numbers must stay fixed.

A dry run against a real week of 2,123 logs took 16 seconds and would have dropped 1 log row and 31 patch rows.

`--same-content` also removes other uploads of the identical log, found by the `sha256` column. Without it, `remove` reports how many there are.

Run `remove` between runs. If a `run` is working on the same week at that moment, its files are still on disk and the two would be writing the same part.

## Tests and checks

| Script | Needs | What it proves |
|---|---|---|
| `python3 puller/tests/offline.py` | nothing | The parsers, the refusal handling and the Supabase requests, against a local test server. |
| `uv run puller/tests/removal.py` | nothing | Removing a log on disk and after upload, with a folder standing in for the bucket. |
| `uv run puller/checks/credentials.py` | the env file | The R2 token can list, write, read and delete; the Supabase table answers. |
| `uv run puller/checks/packed.py` | packed files on disk | Every log's patch set rebuilds exactly from the four patch tables. |
| `uv run puller/checks/bucket.py` | the env file and the index | The bucket holds every uploaded part, and its row counts match the index. |
| `uv run puller/checks/docs.py` | the env file | Every SQL block in the docs runs against the bucket, from a fresh setup. |

`uvx ruff check puller` lints the code and `uvx ruff format puller` formats it, with the settings in `ruff.toml`.
