#!/usr/bin/env python3
"""
Reads GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_JSON from env (raw JSON or base64),
validates it, writes credentials.json, and exports GOOGLE_APPLICATION_CREDENTIALS.
Exits with a clear error message if anything is wrong.
"""
import os, sys, json, base64, pathlib

def main():
    raw = (os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS_JSON") or "").strip()
    if not raw:
        print("ERROR: GOOGLE_CREDENTIALS/GOOGLE_CREDENTIALS_JSON secret is empty or not set.", file=sys.stderr)
        sys.exit(1)

    # If it doesn't look like JSON, try base64 decode
    if not raw.lstrip().startswith("{"):
        try:
            raw = base64.b64decode(raw).decode("utf-8")
        except Exception as e:
            print(f"ERROR: Secret is not raw JSON and base64 decode failed: {e}", file=sys.stderr)
            sys.exit(1)

    # Validate JSON
    try:
        obj = json.loads(raw)
    except Exception as e:
        print(f"ERROR: credentials are not valid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    required = (
        "type","project_id","private_key_id","private_key",
        "client_email","client_id","auth_uri","token_uri"
    )
    missing = [k for k in required if k not in obj]
    if missing:
        print("ERROR: credentials JSON missing expected keys: " + ", ".join(missing), file=sys.stderr)
        sys.exit(1)

    pathlib.Path("credentials.json").write_text(raw, encoding="utf-8")
    # Tell downstream libs where the file lives
    gha_env = os.getenv("GITHUB_ENV")
    if gha_env:
        with open(gha_env, "a", encoding="utf-8") as f:
            f.write(f"GOOGLE_APPLICATION_CREDENTIALS={os.getcwd()}/credentials.json\n")

    print("Wrote credentials.json; keys:", ", ".join(sorted(obj.keys())))
    return 0

if __name__ == "__main__":
    sys.exit(main())
