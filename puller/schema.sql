-- Index and per-log facts for the HugsLib log corpus, for a Supabase project.
-- One row per public gist. The log text and the full mod list live in the
-- weekly Parquet files; this table is what fits a 500 MB database.

create table if not exists public.logs (
  id            text primary key,       -- gist id (GUID in the patch tables)
  hugslib_log_id integer unique,        -- HugslibLogId in the patch tables
  created_at    timestamptz not null,   -- upload time
  size_bytes    integer not null,       -- log size as text
  uploader      text not null,          -- hugslib | standalone | other
  week          text not null,          -- ISO week of the upload, e.g. 2026-W46
  shard         text,                   -- Parquet file holding the text
  sha256        text,                   -- hash of the log as downloaded
  game_version  text,                   -- e.g. 1.6.4633
  game_rev      integer,
  mod_count     integer,                -- every entry under "Loaded mods", Core and DLC included
  dlcs          text[],
  trimmed       boolean,                -- HugsLib cut the log at 10,000 lines
  line_count    integer
);

create index if not exists logs_created_at on public.logs (created_at);
create index if not exists logs_game_version on public.logs (game_version);

-- Row-level security with no policy: the API keys read nothing. Collaborators
-- query as organization members (SQL editor, connection string, MCP), which
-- bypasses row-level security. A policy "to authenticated" would open the
-- table to anyone who signs up while sign-ups are enabled.
alter table public.logs enable row level security;
