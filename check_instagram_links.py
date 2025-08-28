#!/usr/bin/env python3
import os
import re
import json
import time
import random
from dataclasses import dataclass
from typing import Tuple, List, Dict
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ====== Sheet config (unchanged) ======
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1ps5Luzxgk0nNGWqTPCw9y9MhgCCnoAuKpZ73UmlanPE")
SHEET_NAME = os.getenv("SHEET_NAME", "Sheet1")

# Column model (A=1). We read A (Link) and write B..E.
COL_LINK = 1
COL_STATUS = 2
COL_REMOVAL_DATE = 3
COL_LAST_CHECKED = 4
COL_DETAILS = 5

# ====== Runtime knobs ======
START_ROW = int(os.getenv("START_ROW", "2"))
DELAY_SEC = (4.0, 7.0)
NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "15000"))
SETTLE_SLEEP_S = float(os.getenv("SETTLE_SLEEP_S", "3.0"))

# Optional sharding from workflow
SHARD_INDEX = int(str(os.getenv("SHARD_INDEX", "0")) or "0")
TOTAL_SHARDS = int(str(os.getenv("TOTAL_SHARDS", "1")) or "1")
SKIP_RECENT_DAYS = int(str(os.getenv("SKIP_RECENT_DAYS", "0")) or "0")

# ====== User agents ======
UA_PRIMARY = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
UA_RETRY = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.5 Safari/605.1.15"
)

# ====== Status values ======
STATUS_ACTIVE = "Active"
STATUS_REMOVED = "Removed"
STATUS_UNKNOWN = "Unknown"
STATUS_LOGIN_REQUIRED = "Login required"

# ====== Heuristics ======
REMOVAL_TEXT: Dict[str, List[str]] = {
    "instagram": ["Sorry, this page isn't available."],
    "youtube": ["This video isn't available anymore", "Video unavailable"],
    "tiktok": ["Video currently unavailable"],
    "facebook": [
        "This content isn't available right now",
        # legacy variants sometimes appear:
        "This content is not available",
    ],
    "threads": ["Post unavailable", "error=invalid_post"],  # we also check URL
}

# Signals that strongly indicate a login wall
LOGIN_WALL: Dict[str, List[str]] = {
    "instagram": ["Log in • Instagram", "login • Instagram"],
    "facebook": [
        "Log in to Facebook", "Facebook – log in", "facebook.com/login", "m.facebook.com/login"
    ],
    # For others we fall back to generic patterns
}

GENERIC_LOGIN_WORDS = [
    "log in", "login", "sign in", "/accounts/login", "require cookies to be enabled"
]

FACEBOOK_WATCH_HOME_PATH = re.compile(r"^/watch/?$", re.IGNORECASE)

@dataclass
class CheckResult:
    status: str
    details: str


# ---------- Google auth ----------
def get_gspread_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    # Prefer env JSON (raw or base64)
    creds_env = os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS_JSON")
    if creds_env:
        s = creds_env.strip()
        if not s.lstrip().startswith("{"):
            import base64
            try:
                s = base64.b64decode(s).decode("utf-8")
            except Exception:
                pass
        try:
            creds_dict = json.loads(s)
            return gspread.authorize(
                ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            )
        except Exception:
            pass  # fall through to file path

    # Then GOOGLE_APPLICATION_CREDENTIALS path, then local credentials.json
    for path in [os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), "credentials.json"]:
        if path and os.path.isfile(path):
            try:
                raw = open(path, "rb").read()
                raw = raw.lstrip(b"\xef\xbb\xbf\r\n\t ")
                creds_dict = json.loads(raw.decode("utf-8"))
                return gspread.authorize(
                    ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
                )
            except Exception:
                continue

    raise RuntimeError("Could not load Google credentials from env or file.")


# ---------- URL helpers ----------
def detect_platform(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return "unknown"
    if "instagram.com" in host:
        return "instagram"
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    if "tiktok.com" in host:
        return "tiktok"
    if "facebook.com" in host or "fb.watch" in host:
        return "facebook"
    if "threads.net" in host or "threads.com" in host:
        return "threads"
    return "unknown"


def looks_like_fb_watch_home(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if "facebook.com" not in parsed.netloc.lower():
            return False
        if FACEBOOK_WATCH_HOME_PATH.fullmatch(parsed.path or ""):
            qs = parse_qs(parsed.query or "")
            return "v" not in qs or len(qs.get("v", [])) == 0
        return False
    except Exception:
        return False


def contains_any(haystack: str, needles: List[str]) -> bool:
    h = (haystack or "").lower()
    return any(n.lower() in h for n in needles)


def is_login_wall(platform: str, title: str, html: str, url: str) -> bool:
    # URL based quick checks
    u = url.lower()
    if "instagram.com/accounts/login" in u or "facebook.com/login" in u or "m.facebook.com/login" in u:
        return True

    # Title checks
    if platform in LOGIN_WALL:
        if contains_any(title, LOGIN_WALL[platform]):
            return True

    # Generic fallback
    if contains_any(title, GENERIC_LOGIN_WORDS) or contains_any(html, GENERIC_LOGIN_WORDS):
        return True

    return False


def looks_removed(platform: str, title: str, html: str, final_url: str) -> bool:
    # Hard URL signal for Threads "invalid_post"
    if platform == "threads" and "error=invalid_post" in final_url.lower():
        return True

    phrases = REMOVAL_TEXT.get(platform, [])
    if phrases and (contains_any(title, phrases) or contains_any(html, phrases)):
        return True

    # Facebook reel/shortlink sometimes redirects to /watch/ home when removed
    if platform == "facebook" and looks_like_fb_watch_home(final_url):
        return True

    return False


# ---------- Page check ----------
def check_one(page, url: str, timeout_ms: int) -> CheckResult:
    platform = detect_platform(url)
    try:
        resp = page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
        time.sleep(SETTLE_SLEEP_S)
        code = resp.status if resp else 0
        html = page.content()
        final_url = page.url
        title = (page.title() or "").strip()

        # 1) Removed?
        if looks_removed(platform, title, html, final_url):
            return CheckResult(STATUS_REMOVED, f"Code: {code}")

        # 2) Login wall?
        if is_login_wall(platform, title, html, final_url):
            return CheckResult(STATUS_LOGIN_REQUIRED, f"Login wall | Code: {code}")

        # 3) Otherwise classify by status code
        if code and 200 <= code < 400:
            return CheckResult(STATUS_ACTIVE, f"Code: {code}")

        # 4) Unknown (include code)
        return CheckResult(STATUS_UNKNOWN, f"Code: {code}")

    except PWTimeout:
        return CheckResult(STATUS_UNKNOWN, "Timeout")
    except Exception as e:
        return CheckResult(STATUS_UNKNOWN, f"Error: {e}")


# ---------- Main ----------
def main():
    client = get_gspread_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
    rows = ws.get_all_values()
    if not rows:
        print("No data in sheet.")
        return

    today = datetime.now().strftime("%m/%d/%Y")
    now_dt = datetime.now()

    # Compute “skip recent” cutoff date
    cutoff = None
    if SKIP_RECENT_DAYS > 0:
        cutoff = (now_dt - timedelta(days=SKIP_RECENT_DAYS)).date()

    # Build worklist with sharding
    work: List[Tuple[int, str, str, str]] = []  # (row_num, link, current_status, last_checked_str)
    for i in range(START_ROW - 1, len(rows)):
        if TOTAL_SHARDS > 1 and (i % TOTAL_SHARDS) != SHARD_INDEX:
            continue
        row_num = i + 1
        row = rows[i]

        link = (row[COL_LINK - 1] if len(row) >= COL_LINK else "").strip()
        status = (row[COL_STATUS - 1] if len(row) >= COL_STATUS else "").strip()
        last_checked = (row[COL_LAST_CHECKED - 1] if len(row) >= COL_LAST_CHECKED else "").strip()

        if not link:
            continue
        if status.lower() == "removed":
            continue
        if cutoff and last_checked:
            try:
                lc = datetime.strptime(last_checked, "%m/%d/%Y").date()
                if lc >= cutoff:
                    continue
            except Exception:
                pass

        work.append((row_num, link, status, last_checked))

    updates_batch = []
    retry_candidates: List[Tuple[int, str]] = []

    def run_pass(user_agent: str, timeout_ms: int, pass_name: str):
        nonlocal updates_batch, retry_candidates
        if not work:
            return
        print(f"--- {pass_name}: {len(work)} rows ---")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=user_agent)
            page = context.new_page()

            for row_num, link, _, _ in work:
                print(f"🔍 Row {row_num}: {link}")
                res = check_one(page, link, timeout_ms=timeout_ms)
                removal_date = today if res.status == STATUS_REMOVED else ""
                updates_batch.append({
                    "range": f"{col_letter(COL_STATUS)}{row_num}:{col_letter(COL_DETAILS)}{row_num}",
                    "values": [[res.status, removal_date, today, res.details]],
                })
                # Queue a retry for Login required / Unknown (not Removed)
                if res.status in (STATUS_LOGIN_REQUIRED, STATUS_UNKNOWN):
                    retry_candidates.append((row_num, link))

                sleep_for = random.uniform(*DELAY_SEC)
                print(f"   → {res.status:>14} | sleep {sleep_for:.1f}s | {res.details}")
                time.sleep(sleep_for)

            browser.close()

    # First pass
    run_pass(UA_PRIMARY, NAV_TIMEOUT_MS, "First pass")

    # Build second-pass worklist from candidates we just collected
    work[:] = [(r, l, "", "") for (r, l) in retry_candidates]
    retry_candidates.clear()

    if work:
        print(f"\nRe-trying {len(work)} rows with alternate UA/timeout…")
        run_pass(UA_RETRY, int(NAV_TIMEOUT_MS * 1.6), "Retry pass")

    # Apply updates
    if updates_batch:
        ws.batch_update(updates_batch)

    print("✅ Done.")


# Helpers
def col_letter(n: int) -> str:
    """1 -> A, 2 -> B, …"""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


if __name__ == "__main__":
    main()
