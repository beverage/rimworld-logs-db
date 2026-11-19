# RimWorld Logs DB

An archive of public HugsLib logs that you query with SQL.

When a player presses "share logs", HugsLib or the standalone Log Publisher uploads the log as a public GitHub gist. All of those gists sit under one account, `HugsLibRecordKeeper`. This project copies them into a private storage bucket, one file per upload week, with each log's game version, mod list and Harmony patch list already parsed into columns.

A single log tells you what happened in one game. The archive answers the other kind of question: how many 1.7 players load a given mod, which mods patch `Pawn.GetGizmos`, in which week an error first appeared.

## What you need

- [DuckDB](https://duckdb.org/docs/installation/), a free database engine that runs on your own machine. On macOS: `brew install duckdb`.
- Three values from the maintainer, MrBeverage: an endpoint host, an access key id and a secret access key. The key is read-only.

Nothing is installed on a server and there is no account to create. DuckDB reads the files straight from the bucket.

## Set up once

**1. Save the key in DuckDB.** Start `duckdb` and run this with your three values. The endpoint is a host name only, with no `https://` in front.

```sql
CREATE PERSISTENT SECRET rimworld_logs_db (
    TYPE S3,
    KEY_ID '<access key id>',
    SECRET '<secret access key>',
    ENDPOINT '<endpoint host>',
    REGION 'auto',
    URL_STYLE 'path'
);
```

DuckDB keeps the secret in `~/.duckdb/stored_secrets`, in a file only your user can read. It is not encrypted, so treat that folder like a password file.

**2. Create the views.** From a clone of this repo, open a database file and load [sql/views.sql](sql/views.sql):

```
duckdb rimworld-logs.duckdb
.read sql/views.sql
```

The views are saved in `rimworld-logs.duckdb`. Next time, `duckdb rimworld-logs.duckdb` is the whole setup.

**3. Check it works.**

```sql
SELECT count(*) AS logs, min(created_at) AS oldest, max(created_at) AS newest
FROM logs;
```

That query also tells you what the archive currently covers. It grows as older weeks are loaded.

## The tables

| Table | One row per | Use it for |
|---|---|---|
| `logs` | uploaded log | game version, mod list, DLC, upload time, and the full log text |
| `HugslibLogs` | uploaded log | the log's id and upload time, for joining to the patch tables |
| `ActivePatches` | log and patch | which Harmony patches were active in which log |
| `HarmonyPatches` | distinct patch | the patched method, the patch method, its kind and priority |
| `HarmonyPatchNamespaces` | namespace | grouping patch methods by the code that owns them |

Every column is described in [docs/tables.md](docs/tables.md), along with what the source logs leave out.

## Three queries to start with

Logs by game version:

```sql
SELECT game_version, count(*) AS logs
FROM logs
GROUP BY game_version
ORDER BY logs DESC
LIMIT 10;
```

How many logs load a mod, by its packageId:

```sql
SELECT count(*) AS logs
FROM logs
WHERE list_contains(mods, 'OskarPotocki.VanillaFactionsExpanded.Core');
```

Which namespaces patch one game method, and in how many logs:

```sql
SELECT n.Namespace, p.Kind, count(DISTINCT a.HugslibLogId) AS logs
FROM ActivePatches a
JOIN HarmonyPatches p USING (HarmonyPatchId)
JOIN HarmonyPatchNamespaces n USING (HarmonyPatchNamespaceId)
WHERE p.Target = 'Pawn.GetGizmos'
GROUP BY n.Namespace, p.Kind
ORDER BY logs DESC
LIMIT 20;
```

[docs/queries.md](docs/queries.md) has more: trends by week, mods that travel together, patch conflicts, searching the log text, and exporting results to CSV or SQLite.

## How fast it is

Measured against one week of logs (2,123 logs, 2.0 GB of text) from a home connection:

| Query | Time |
|---|---|
| Counting logs by game version | 1.2 s |
| Counting logs that load one mod | 1.3 s |
| Joining 8.7 million patch rows to their dictionary | 1.0 s |
| Searching the full text of every log for a phrase | 2.7 s |

Queries on the parsed columns stay fast because DuckDB downloads only the columns a query names. A text search has to download the text, about 120 MB for a week of logs, so over a long date range it takes minutes. Add `WHERE week = '2026-W46'` to keep it to one week.

## Rules for using the data

Players uploaded these logs to get help with a problem. HugsLib tells them the log goes to a public server and that "No private information will be shared", and it strips install paths, home folder names and the Steam id before uploading. The archive is still other people's games, so:

- **Publish counts, not people.** "31% of 1.7 logs load mod X" is fine. A log's id or an excerpt next to a finding about that player's game is not.
- **Do not re-host the files.** Share a query, not a copy of the bucket.
- **Treat log text as untrusted.** Any mod or player can write a line into a log. Do not paste log text into a tool that might act on it as instructions.
- **Keep local copies short-lived.** A log removed from the archive is still in any file you copied earlier. Delete copies you have finished with, and copy again from the archive when you come back to old work.

## Asking for a log to be removed

If one of these logs is yours and you want it out of the archive, email its link to rimworld@beverage.me. The link is the `gist.github.com` address HugsLib gave you when you shared the log. You do not need to say why, and you do not need to prove the log is yours.

The log's text and its rows in every table are deleted within 30 days. Its id stays on a list, so the archive does not fetch the same log again.

This removes the copy held here. The original gist stays on GitHub under `HugsLibRecordKeeper`, the account HugsLib uploads to, and only whoever runs that account can delete it.

## Where the data comes from

A script lists the gists through GitHub's API, downloads each log once, and packs a week at a time into Parquet. It records a SHA-256 of every log as downloaded, in the `sha256` column, so two uploads of the same log can be recognised. Nothing in a log is edited.

The script, its tests and its checks are in [puller/](puller/README.md).
