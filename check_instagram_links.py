#!/usr/bin/env python3
import os
import re
import json
import time
import math
import datetime as dt

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

# ===== Spreadsheet config (PRIMARY sheet) =====
SHEET_ID = "1fFTIEDy-86lDaCgll1GkTq376hvrTd-DXulioXrDAgw"
TAB = "Logs"

# Column mapping (1-based)
COL_URL = 6           # F  Infringing URL
COL_STATUS = 13       # M  Status
COL_REMOVAL_DATE = 14 # N  Removal Date
COL_LAST_CHECKED = 15 # O  Last Checked

# Text markers that indicate FB content is gone (unauthenticated views)
FB_REMOVED_PATTERNS = [
    "this page isn't available right now",
    "this video isn't available anymore",
    "the link may be broken or the video may have been removed",
    "content isn't available",
    "page isn't available",
]
FB_REMOVED_RE = re.compile("|".join(re.escape(s) for s in FB_REMOVED_PATTERNS), re.I)

# ===== Controls via env =====
SHARD_INDEX = int(os.environ.get("SHARD_INDEX", "0"))
TOTAL_SHARDS = max(1, int(os.environ.get("TOTAL_SHARDS", "1")))
SKIP_RECENT_DAYS = int(os.environ.get("SKIP_RECENT_DAYS", "0"))

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

def make_gspread_client():
    """Hardened credentials loader: env var first, then local file."""
    raw = (os.environ.get("GOOGLE_CREDENTIALS_JSON") or "").strip()
    if not raw:
        raw = open("credentials.json", "r", encoding="utf-8").read()
    creds_dict = json.loads(raw)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
    return gspread.authorize(creds)

def is_facebook_removed_text(body_text: str) -> bool:
    if not body_text:
        return False
    return bool(FB_REMOVED_RE.search(body_text))

def decide_status_for_url(pw, url: str) -> tuple[str, int]:
    """
    Returns (status_str, http_code)
    Status is one of: "Active", "Removed", "Unknown"
    """
    # Fast path: non-Facebook — treat 200 presence/404 etc. with a simple fetch
    if "facebook.com" not in url:
        try:
            resp = pw.request.get(url, timeout=20000)
            code = resp.status()
            if code == 404 or code == 410:
                return "Removed", code
            if 200 <= code < 300:
                return "Active", code
            return "Unknown", code
        except Exception:
            return "Unknown", 0

    # Facebook: use a real browser page (to render the login wall + text)
    code = 0
    try:
        page = pw.chromium.launch(headless=True).new_context().new_page()
        resp = page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if resp:
            code = resp.status
        # Grab visible text (even if a login modal is present)
        text = page.text_content("body") or ""
        text = " ".join(text.split())
        # If the page clearly shows a "removed" banner text, treat as Removed
        if is_facebook_removed_text(text):
            page.close()
            return "Removed", code or 200
        # Some reels redirect to generic /reel or /watch and show a removed banner
        # Check again after small wait for any late banner render
        page.wait_for_timeout(1000)
        text2 = page.text_content("body") or ""
        if is_facebook_removed_text(text2):
            page.close()
            return "Removed", code or 200
        # Otherwise if HTTP 200 and no removal banner, call it Active
        page.close()
        if 200 <= (code or 200) < 300:
            return "Active", code or 200
        if code in (404, 410):
            return "Removed", code
        return "Unknown", code or 0
    except Exception:
        return "Unknown", code or 0

def shard_range(total_rows: int, shard_idx: int, shard_count: int) -> range:
    """Return the row indexes (1-based data rows excluding header) for this shard."""
    if shard_count <= 1:
        return range(2, total_rows + 1)
    # we will distribute (rows 2..total_rows) evenly
    data_count = max(0, total_rows - 1)
    chunk = math.ceil(data_count / shard_count)
    start = 2 + shard_idx * chunk
    end = min(total_rows, 1 + (shard_idx + 1) * chunk)
    if start > total_rows:
        return range(0, 0)
    return range(start, end + 1)

def should_skip_recent(last_checked_str: str) -> bool:
    if SKIP_RECENT_DAYS <= 0:
        return False
    if not last_checked_str:
        return False
    try:
        # Accept both MM/DD/YYYY and ISO
        for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                d = dt.datetime.strptime(last_checked_str.strip(), fmt).date()
                break
            except Exception:
                d = None
        if not d:
            return False
        return (dt.date.today() - d).days < SKIP_RECENT_DAYS
    except Exception:
        return False

def main():
    gc = make_gspread_client()
    ws = gc.open_by_key(SHEET_ID).worksheet(TAB)

    # Get dimensions
    values = ws.get_all_values()
    total_rows = len(values)
    if total_rows <= 1:
        print("No data rows.")
        return

    # Determine rows to process for this shard
    rows_range = shard_range(total_rows, SHARD_INDEX, TOTAL_SHARDS)

    # Batch reading of the needed columns
    urls = ws.col_values(COL_URL)
    statuses = ws.col_values(COL_STATUS)
    last_checked_list = ws.col_values(COL_LAST_CHECKED)

    # Prepare batch updates
    updates = []

    with sync_playwright() as p:
        for r in rows_range:
            # Safety for short columns
            url = (urls[r-1] if r-1 < len(urls) else "").strip()
            if not url:
                continue

            last_checked = (last_checked_list[r-1] if r-1 < len(last_checked_list) else "").strip()
            if should_skip_recent(last_checked):
                continue

            status, code = decide_status_for_url(p, url)

            # Write status
            updates.append({
                "range": gspread.utils.rowcol_to_a1(r, COL_STATUS),
                "values": [[status]],
            })
            # Removal Date only when status becomes Removed
            if status.lower() == "removed":
                today = dt.date.today().strftime("%m/%d/%Y")
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(r, COL_REMOVAL_DATE),
                    "values": [[today]],
                })
            # Always set Last Checked
            today = dt.date.today().strftime("%m/%d/%Y")
            updates.append({
                "range": gspread.utils.rowcol_to_a1(r, COL_LAST_CHECKED),
                "values": [[today]],
            })

            # Flush periodically
            if len(updates) >= 200:
                ws.batch_update(updates, value_input_option="USER_ENTERED")
                updates.clear()
                time.sleep(0.4)

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")

if __name__ == "__main__":
    main()
