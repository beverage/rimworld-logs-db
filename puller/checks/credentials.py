# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3>=1.36"]
# ///
"""Check that the keys in the env file work. Prints outcomes only, never a key or a value.

Tests the R2 bucket by listing it, then writing, reading back and deleting one
small object under _check/, and tests that the Supabase logs table answers.

Run:  uv run puller/checks/credentials.py [env file]
"""

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENV_FILE = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.home() / ".config" / "hugslib-corpus" / "env"
env = {}
for line in ENV_FILE.read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()

wanted = ["R2_ENDPOINT", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "SUPABASE_URL", "SUPABASE_SECRET_KEY"]
print("keys set:", {key: bool(env.get(key)) for key in wanted})

s3 = boto3.client(
    "s3",
    endpoint_url=env["R2_ENDPOINT"],
    aws_access_key_id=env["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
    region_name="auto",
    config=Config(
        signature_version="s3v4",
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
        retries={"max_attempts": 3, "mode": "standard"},
    ),
)
bucket = env["R2_BUCKET"]


def attempt(name, call):
    try:
        result = call()
        print(f"R2 {name}: ok", result if result is not None else "")
        return True
    except ClientError as exc:
        error = exc.response.get("Error", {})
        print(f"R2 {name}: FAILED {error.get('Code')} (HTTP {exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode')})")
        return False
    except Exception as exc:  # connection and endpoint problems
        print(f"R2 {name}: FAILED {type(exc).__name__}")
        return False


attempt("head bucket", lambda: s3.head_bucket(Bucket=bucket) and None)
attempt("list", lambda: {"objects": s3.list_objects_v2(Bucket=bucket, MaxKeys=5).get("KeyCount")})
if attempt("put test object", lambda: s3.put_object(Bucket=bucket, Key="_check/hello.txt", Body=b"hello") and None):
    attempt("read it back", lambda: {"bytes": s3.head_object(Bucket=bucket, Key="_check/hello.txt")["ContentLength"]})
    attempt("delete it", lambda: s3.delete_object(Bucket=bucket, Key="_check/hello.txt") and None)

request = urllib.request.Request(
    env["SUPABASE_URL"].rstrip("/") + "/rest/v1/logs?select=id&limit=1",
    headers={"apikey": env["SUPABASE_SECRET_KEY"], "User-Agent": "hugslib-corpus/0.1"},
)
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        print(f"Supabase logs table: ok (HTTP {response.status}), rows returned {len(json.loads(response.read()))}")
except urllib.error.HTTPError as exc:
    try:
        code = json.loads(exc.read()).get("code")
    except Exception:
        code = None
    print(f"Supabase logs table: HTTP {exc.code}, code {code}")
except Exception as exc:
    print(f"Supabase: FAILED {type(exc).__name__}")
