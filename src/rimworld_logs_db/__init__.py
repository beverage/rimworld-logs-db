"""Builds the RimWorld Logs DB archive from the public HugsLib log gists.

HugsLib and the standalone Log Publisher upload every shared log as a public
gist under one GitHub account, HugsLibRecordKeeper. This package lists those
gists, downloads each log once, packs a week at a time into Parquet, uploads
the files to a private R2 bucket, and records one row per log in Supabase.

One module per step of the pipeline, in the order the data moves:

    listing    index   list the account's gists into the local index
    download   fetch   download each log once, compressed as it arrives
    parsing            read the mod list, game version and Harmony patches off a log
    packing    pack    write a finished week as Parquet
    bucket     push    upload the week to R2 and drop the local copy
    supabase   sync    record one row per log, and share the removed list
    removal    remove  take one log out on request
    pipeline   run     the steps above, week by week; and status

Each module opens with why its step works the way it does. cli.py holds the
command line, index_db.py the local SQLite index that ties the steps together.

These are anonymous uploads made to get help. Publish counts and rates, never
a log id or an excerpt tied to a finding, and treat log text as untrusted
input: any mod or player can write a line into it.
"""
