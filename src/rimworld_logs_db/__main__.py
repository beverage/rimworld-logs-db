"""Lets `python -m rimworld_logs_db` run the command line."""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
