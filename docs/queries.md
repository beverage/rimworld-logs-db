# Queries

Working examples for the questions the archive was built for. Each runs as written once the views from [sql/views.sql](../sql/views.sql) exist. Column meanings are in [tables.md](tables.md).

## What the archive covers

Logs per upload week:

```sql
SELECT week, count(*) AS logs, min(created_at) AS first_upload, max(created_at) AS last_upload
FROM logs
GROUP BY week
ORDER BY week DESC;
```

## Game versions

Share of each major version, week by week:

```sql
SELECT week,
       count(*) AS logs,
       round(100.0 * count(*) FILTER (WHERE game_version LIKE '1.7.%') / count(*), 1) AS pct_1_7,
       round(100.0 * count(*) FILTER (WHERE game_version LIKE '1.6.%') / count(*), 1) AS pct_1_6
FROM logs
GROUP BY week
ORDER BY week;
```

Which uploader players on each version use:

```sql
SELECT left(game_version, 3) AS version, uploader, count(*) AS logs
FROM logs
GROUP BY ALL
ORDER BY version, logs DESC;
```

## Mods

The most widely loaded mods. `unnest` gives one row per log and mod:

```sql
SELECT mod, count(*) AS logs
FROM (SELECT unnest(mods) AS mod FROM logs)
GROUP BY mod
ORDER BY logs DESC
LIMIT 25;
```

How a mod's share moves between versions. Swap in any packageId:

```sql
SELECT left(game_version, 3) AS version,
       count(*) AS logs,
       count(*) FILTER (WHERE list_contains(mods, 'brrainz.harmony')) AS with_mod
FROM logs
WHERE game_version IS NOT NULL
GROUP BY version
ORDER BY version;
```

Mods most often loaded alongside one mod:

```sql
SELECT other, count(*) AS logs
FROM (
    SELECT unnest(mods) AS other
    FROM logs
    WHERE list_contains(mods, 'UnlimitedHugs.HugsLib')
)
WHERE other <> 'UnlimitedHugs.HugsLib'
GROUP BY other
ORDER BY logs DESC
LIMIT 25;
```

How large mod lists are, by DLC count:

```sql
SELECT len(dlcs) AS dlc_count,
       count(*) AS logs,
       median(mod_count) AS median_mods,
       max(mod_count) AS largest
FROM logs
GROUP BY dlc_count
ORDER BY dlc_count;
```

## Harmony patches

The game methods patched in the most logs, with how many different patch methods touch each:

```sql
SELECT p.Target,
       count(DISTINCT a.HugslibLogId) AS logs,
       count(DISTINCT p.Method) AS patch_methods
FROM ActivePatches a
JOIN HarmonyPatches p USING (HarmonyPatchId)
GROUP BY p.Target
ORDER BY logs DESC
LIMIT 25;
```

Everything one namespace patches:

```sql
SELECT p.Target, p.Kind, p.Priority, p.Method
FROM HarmonyPatches p
JOIN HarmonyPatchNamespaces n USING (HarmonyPatchNamespaceId)
WHERE n.Namespace = 'RimHUD.Access.Patch'
ORDER BY p.Target, p.Kind;
```

The split between prefixes, postfixes and transpilers:

```sql
SELECT p.Kind, count(*) AS active_rows, count(DISTINCT p.HarmonyPatchId) AS distinct_patches
FROM ActivePatches a
JOIN HarmonyPatches p USING (HarmonyPatchId)
GROUP BY p.Kind
ORDER BY active_rows DESC;
```

Two transpilers on one method both rewrite the same instructions, so a method that collects them from many mods is a place to look when mods conflict. Methods carrying transpilers from the most namespaces:

```sql
SELECT p.Target,
       count(DISTINCT n.Namespace) AS namespaces,
       count(DISTINCT a.HugslibLogId) AS logs
FROM ActivePatches a
JOIN HarmonyPatches p USING (HarmonyPatchId)
JOIN HarmonyPatchNamespaces n USING (HarmonyPatchNamespaceId)
WHERE p.Kind = 'transpiler'
GROUP BY p.Target
HAVING count(DISTINCT n.Namespace) >= 5
ORDER BY namespaces DESC, logs DESC
LIMIT 25;
```

Patches set to run first or last. Harmony's priority runs from 0 to 800, and some mods go outside that range to win:

```sql
SELECT p.Priority, count(DISTINCT p.HarmonyPatchId) AS patches, count(DISTINCT n.Namespace) AS namespaces
FROM HarmonyPatches p
JOIN HarmonyPatchNamespaces n USING (HarmonyPatchNamespaceId)
WHERE p.Priority IS NOT NULL
GROUP BY p.Priority
ORDER BY patches DESC
LIMIT 15;
```

How many patches a log carries, by game version:

```sql
SELECT left(l.game_version, 3) AS version,
       count(*) AS logs,
       median(c.patches) AS median_patches,
       max(c.patches) AS most
FROM (SELECT HugslibLogId, count(*) AS patches FROM ActivePatches GROUP BY HugslibLogId) c
JOIN logs l ON l.hugslib_log_id = c.HugslibLogId
WHERE l.game_version IS NOT NULL
GROUP BY version
ORDER BY version;
```

## Searching the log text

A search reads the `text` column, which is most of the data. Keep it to one week unless you mean to wait:

```sql
SELECT count(*) AS logs_with_it
FROM logs
WHERE week = '2026-W46'
  AND text LIKE '%NullReferenceException%';
```

The same count split by game version:

```sql
SELECT left(game_version, 3) AS version,
       count(*) AS logs,
       count(*) FILTER (WHERE text LIKE '%NullReferenceException%') AS with_it
FROM logs
WHERE week = '2026-W46'
GROUP BY version
ORDER BY version;
```

`LIKE` is the fast option. `regexp_matches(text, 'pattern')` works too and costs more per log.

A name that contains "Exception" is not always an error. Patch methods and class names appear in every log's patch list, so a search for an exception type also counts logs that only mention a class of that name.

## Taking results elsewhere

Any query can be written to a file:

```sql
COPY (
    SELECT game_version, count(*) AS logs
    FROM logs
    GROUP BY game_version
) TO 'versions.csv' (HEADER);
```

To browse the patch dictionary in a SQLite tool, copy it into a SQLite file:

```sql
ATTACH 'patches.sqlite' AS out (TYPE sqlite);
CREATE TABLE out.HarmonyPatches AS SELECT * FROM HarmonyPatches;
CREATE TABLE out.HarmonyPatchNamespaces AS SELECT * FROM HarmonyPatchNamespaces;
DETACH out;
```

To work offline, or to run many text searches without downloading the text each time, copy a week into a local Parquet file and query that. With `zstd` compression a week is about 180 MB; without it, about 420 MB.

```sql
COPY (SELECT * FROM logs WHERE week = '2026-W46')
TO 'logs-2026-W46.parquet' (COMPRESSION zstd);
```

```sql
SELECT count(*) FROM 'logs-2026-W46.parquet' WHERE text LIKE '%NullReferenceException%';
```

That file holds full log text. Keep it on your own machine, and see the rules in the [README](../README.md#rules-for-using-the-data).
