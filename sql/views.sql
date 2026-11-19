-- Views over the RimWorld Logs DB bucket.
--
-- Run this once inside a DuckDB database file. The views are saved in that
-- file, and because each one reads a file pattern, new weeks show up in them
-- without running this again.

-- One row per uploaded log: the parsed facts, the mod list and the full text.
CREATE OR REPLACE VIEW logs AS
SELECT *
FROM read_parquet('s3://hugslib-logs/logs/*/*.parquet', hive_partitioning = true);

-- One row per uploaded log, in the shape the patch tables join to.
CREATE OR REPLACE VIEW HugslibLogs AS
SELECT *
FROM read_parquet('s3://hugslib-logs/patches/HugslibLogs/*/*.parquet', hive_partitioning = true);

-- One row per log and Harmony patch active in it.
CREATE OR REPLACE VIEW ActivePatches AS
SELECT *
FROM read_parquet('s3://hugslib-logs/patches/ActivePatches/*/*.parquet', hive_partitioning = true);

-- One row per distinct Harmony patch.
CREATE OR REPLACE VIEW HarmonyPatches AS
SELECT * EXCLUDE (week)
FROM read_parquet('s3://hugslib-logs/patches/HarmonyPatches/*/*.parquet', hive_partitioning = true);

-- One row per namespace a patch method lives in.
CREATE OR REPLACE VIEW HarmonyPatchNamespaces AS
SELECT * EXCLUDE (week)
FROM read_parquet('s3://hugslib-logs/patches/HarmonyPatchNamespaces/*/*.parquet', hive_partitioning = true);
