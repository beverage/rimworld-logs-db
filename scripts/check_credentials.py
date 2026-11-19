"""Check that the keys in the env file work. Prints outcomes only, never a key or a value.

Tests the R2 bucket by listing it, then writing, reading back and deleting one
small object under _check/, and tests that the Supabase logs table answers.

Run:  uv run python scripts/check_credentials.py [env file]
"""

import json
import os
import sys
import urllib.error
import urllib.request

from botocore.exceptions import ClientError

from rimworld_logs_db import bucket, settings
from rimworld_logs_db.common import ConfigError

TEST_KEY = "_check/hello.txt"


def attempt(name: str, call) -> bool:
    """Run one R2 call and print whether it worked, with the error code if it did not."""
    try:
        result = call()
        print(f"R2 {name}: ok", result if result is not None else "")
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        print(f"R2 {name}: FAILED {code} (HTTP {status})")
        return False
    except Exception as exc:  # connection and endpoint problems
        print(f"R2 {name}: FAILED {type(exc).__name__}")
        return False


def check_supabase(env: dict[str, str]) -> bool:
    request = urllib.request.Request(
        env["SUPABASE_URL"].rstrip("/") + "/rest/v1/logs?select=id&limit=1",
        headers={"apikey": env["SUPABASE_SECRET_KEY"], "User-Agent": settings.USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            rows = len(json.loads(response.read()))
            print(f"Supabase logs table: ok (HTTP {response.status}), rows returned {rows}")
            return True
    except urllib.error.HTTPError as exc:
        try:
            code = json.loads(exc.read()).get("code")
        except Exception:
            code = None
        print(f"Supabase logs table: HTTP {exc.code}, code {code}")
    except Exception as exc:
        print(f"Supabase: FAILED {type(exc).__name__}")
    return False


def main() -> int:
    if len(sys.argv) > 1:
        os.environ[settings.ENV_VARIABLE] = sys.argv[1]
    try:
        env = settings.load_env()
        print("keys set:", {key: bool(env.get(key)) for key in settings.R2_KEYS + settings.SUPABASE_KEYS})
        settings.require(env, settings.R2_KEYS + settings.SUPABASE_KEYS)
    except ConfigError as exc:
        print(exc)
        return 1

    s3, name = bucket.r2_client(env), env["R2_BUCKET"]
    passed = [
        attempt("head bucket", lambda: s3.head_bucket(Bucket=name) and None),
        attempt("list", lambda: {"objects": s3.list_objects_v2(Bucket=name, MaxKeys=5).get("KeyCount")}),
    ]
    if attempt("put test object", lambda: s3.put_object(Bucket=name, Key=TEST_KEY, Body=b"hello") and None):
        passed += [
            attempt("read it back", lambda: {"bytes": s3.head_object(Bucket=name, Key=TEST_KEY)["ContentLength"]}),
            attempt("delete it", lambda: s3.delete_object(Bucket=name, Key=TEST_KEY) and None),
        ]
    else:
        passed.append(False)
    passed.append(check_supabase(env))
    return 0 if all(passed) else 1


if __name__ == "__main__":
    sys.exit(main())
