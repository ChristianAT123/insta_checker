#!/usr/bin/env python3
"""
Prepare Google service-account credentials for CI:
- Reads GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_JSON from env
  (either raw JSON or base64 of that JSON)
- Validates the JSON and writes ./credentials.json
- Exports GOOGLE_APPLICATION_CREDENTIALS for downstream steps
"""

import os, sys, json, base64, pathlib

def main() -> int:
    raw = (os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS_JSON") or "").strip()
    if not raw:
        print("ERROR: GOOGLE_CREDENTIALS/GOOGLE_CREDENTIALS_JSON is empty or not set.", file=sys.stderr)
        return 1

    # If it doesn't look like JSON, try base64 decode
    if not raw.lstrip().startswith("{"):
        try:
            raw = base64.b64decode(raw).decode("utf-8")
        except Exception as e:
            print(f"ERROR: Secret is not JSON and base64 decode failed: {e}", file=sys.stderr)
            return 1

    # Validate JSON
    try:
        obj = json.loads(raw)
    except Exception as e:
        print(f"ERROR: credentials are not valid JSON: {e}", file=sys.stderr)
        return 1

    required = (
        "type","project_id","private_key_id","private_key",
        "client_email","client_id","auth_uri","token_uri"
    )
    missing = [k for k in required if k not in obj]
    if missing:
        print("ERROR: credentials JSON missing expected keys: " + ", ".join(missing), file=sys.stderr)
        return 1

    # Write credentials.json
    pathlib.Path("credentials.json").write_text(raw, encoding="utf-8")

    # Export GOOGLE_APPLICATION_CREDENTIALS for this job
    gha_env = os.getenv("GITHUB_ENV")
    if gha_env:
        with open(gha_env, "a", encoding="utf-8") as f:
            f.write(f"GOOGLE_APPLICATION_CREDENTIALS={os.getcwd()}/credentials.json\n")

    print("Wrote credentials.json; keys:", ", ".join(sorted(obj.keys())))
    return 0

if __name__ == "__main__":
    sys.exit(main())
