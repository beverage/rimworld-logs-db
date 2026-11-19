"""The puller behind RimWorld Logs DB.

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
"""
