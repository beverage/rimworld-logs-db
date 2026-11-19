# Tables

Five views, created by [sql/views.sql](../sql/views.sql). `logs` holds everything about a log. The other four hold the Harmony patch list in a compact, joinable form.

## logs

One row per uploaded log.

| Column | Type | Meaning |
|---|---|---|
| `hugslib_log_id` | integer | This archive's number for the log. Joins to `HugslibLogId` in the patch tables. |
| `id` | text | The gist id on GitHub. The same value as `GUID` in `HugslibLogs`. |
| `created_at` | timestamp | When the log was uploaded. |
| `week` | text | The ISO week of the upload, such as `2026-W46`. Filtering on it limits which files are read. |
| `size` | integer | Size of the log text in bytes. |
| `uploader` | text | `hugslib` for HugsLib itself, `standalone` for the Log Publisher mod, `other` otherwise. |
| `game_version` | text | For example `1.6.4871`. Null when the log has no version line. |
| `game_rev` | integer | The revision number printed after the version. |
| `mod_count` | integer | Entries under "Loaded mods", Core and DLC included. |
| `mods` | list of text | The packageId of every loaded mod, in the order the log lists them. |
| `dlcs` | list of text | The DLC among them: `Royalty`, `Ideology`, `Biotech`, `Anomaly`, `Odyssey`. |
| `trimmed` | boolean | True when the uploader cut the log at 10,000 lines. |
| `line_count` | integer | Lines in the stored text. |
| `sha256` | text | SHA-256 of the log as downloaded. Two rows with the same value are the same log uploaded twice. |
| `utf8_ok` | boolean | False when the log held bytes that were not valid UTF-8 and were replaced. |
| `text` | text | The whole log, unedited. |

`mods` and `dlcs` are lists. `list_contains(mods, 'some.packageId')` tests membership, and `unnest(mods)` turns one log into one row per mod.

## HugslibLogs

One row per uploaded log, for joining.

| Column | Type | Meaning |
|---|---|---|
| `HugslibLogId` | integer | The log's number. Equal to `logs.hugslib_log_id`. |
| `GUID` | text | The gist id. Equal to `logs.id`. |
| `UploadTime` | timestamp | When the log was uploaded. |
| `week` | text | The ISO week of the upload. |

## ActivePatches

One row for each Harmony patch active in each log. This is the large table: a log carries about 4,000 patches on average.

| Column | Type | Meaning |
|---|---|---|
| `HugslibLogId` | integer | The log. |
| `HarmonyPatchId` | integer | The patch. |
| `week` | text | The ISO week of the log's upload. |

## HarmonyPatches

One row per distinct patch. Two patches are the same row when all four of `Target`, `Kind`, `Priority` and `Method` match.

| Column | Type | Meaning |
|---|---|---|
| `HarmonyPatchId` | integer | The patch's number. |
| `Target` | text | The patched method as the log prints it, such as `Pawn.GetGizmos`. |
| `Kind` | text | `prefix`, `postfix` or `transpiler`. |
| `Priority` | integer | The Harmony priority, or null when the patch leaves it at the default. |
| `Method` | text | The patch method's full name, such as `RimHUD.Access.Patch.Verse_ActiveTip_DrawInner.Prefix`. |
| `HarmonyPatchNamespaceId` | integer | The namespace the patch method lives in. |

## HarmonyPatchNamespaces

| Column | Type | Meaning |
|---|---|---|
| `HarmonyPatchNamespaceId` | integer | The namespace's number. |
| `Namespace` | text | The patch method's name with its last two parts, the class and the method, removed. Empty when the class has no namespace. |

A namespace is the closest thing the log gives to "which mod owns this patch". It is not a packageId. A mod can use several namespaces, and two mods can share one.

## What the source logs leave out

These limits come from how HugsLib prints its patch list. No query can work around them.

- **A target has no namespace and no parameter list.** HugsLib prints `Type.Method`. Overloads of one method share a name, and so do same-named types from different mods.
- **Finalizers are not listed.** HugsLib prints prefixes, postfixes and transpilers. A mod that only adds a finalizer to a method does not appear as patching it.
- **Execution order is not in the tables.** HugsLib prints the patches on a method in the order they run. The tables keep which patches are present, and the order is still in `logs.text`.
- **Long logs are cut.** Unless the player allows unlimited size, the uploader keeps the first 10,000 lines. The mod list and patch list come before the log body, so they survive the cut. Errors late in a long session do not.
- **A few logs have no version line.** In one week of 2,123 logs, 9 had a null `game_version`.
- **The mod list is what was active at upload.** It says nothing about mods the player removed before sharing.
