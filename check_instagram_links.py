import os
import re
import json
import time
import random
from urllib.parse import urlparse
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ==== CONFIG: Primary sheet ====
SHEET_ID = "1fFTIEDy-86lDaCgll1GkTq376hvrTd-DXulioXrDAgw"
TAB = "Logs"

# Columns are 1-based indexes
URL_COL = 6            # F: Infringing URL
STATUS_COL = 13        # M: Status
REMOVAL_DATE_COL = 14  # N: Removal Date
LAST_CHECKED_COL = 15  # O: Last Checked

# Behavior
START_ROW = 2
SKIP_STATUS_VALUES = {"removed"}      # we re-check Unknown and Active
DELAY_RANGE = (4.0, 7.0)

# Browser timing
NAV_TIMEOUT_MS = 15000
NETWORK_IDLE_MS = 7000
SETTLE_SLEEP_S = 2.0

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# --- Text fingerprints (lowercased when compared) ---
INST_REMOVAL_PHRASES = [
    "sorry, this page isn't available",
    "the link you followed may be broken",
    "page not found",
]

YT_REMOVAL = [
    "video unavailable",
    "this video isn't available anymore",
]

TT_REMOVAL = [
    "video currently unavailable",
]

FB_REMOVAL = [
    "this content isn't available right now",
]

# Threads: removed posts redirect to this
THREADS_REMOVED_URL_FRAGMENT = "threads.com/?error=invalid_post"
THREADS_UNAVAILABLE_BADGE = "post unavailable"

# Login-wall fingerprints (kept conservative)
LOGIN_CUES = [
    "log in",           # generic
    "sign up",          # generic
    "/accounts/login",  # instagram
    "login.facebook",   # facebook variants
    "log in to facebook",
]

# ----------------- Sheets auth -----------------
def make_gspread_client():
    """Load service-account creds from env (raw/base64) or credentials.json."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    env_val = os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS_JSON")
    if env_val:
        s = env_val.strip()
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
            pass  # fall through

    for path in [os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), "credentials.json"]:
        if path and os.path.isfile(path):
            raw = open(path, "rb").read().lstrip(b"\xef\xbb\xbf\r\n\t ")
            creds_dict = json.loads(raw.decode("utf-8"))
            return gspread.authorize(
                ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            )

    raise RuntimeError("Could not load Google credentials from env or credentials.json")

# ----------------- Helpers -----------------
def normalize_url(u: str) -> str:
    return (u or "").strip()

def page_text(page) -> str:
    try:
        return (page.content() or "").lower()
    except Exception:
        return ""

def host_platform(u: str) -> str:
    try:
        host = urlparse(u).netloc.lower()
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

def contains_any(haystack: str, needles) -> bool:
    return any(n in haystack for n in needles)

def looks_like_login(body: str, url_now: str) -> bool:
    return ("/accounts/login" in url_now) or contains_any(body, LOGIN_CUES)

# ----------------- Per-platform checkers -----------------
def check_instagram(page, url: str) -> str:
    """
    IG:
      - REMOVED if we see IG's removal text (or very clear failure).
      - ACTIVE if we hit the login wall without removal text.
      - ACTIVE if we can see post containers (article/video/og tags).
      - otherwise UNKNOWN.
    """
    page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
    page.set_default_timeout(NAV_TIMEOUT_MS)

    try:
        resp = page.goto(url, wait_until="domcontentloaded")
    except PWTimeout:
        try:
            resp = page.goto(url, wait_until="networkidle")
        except Exception:
            return "unknown"
    except Exception:
        return "unknown"

    try:
        page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_MS)
    except Exception:
        pass
    time.sleep(SETTLE_SLEEP_S)

    body = page_text(page)
    cur_url = page.url

    # Positive removal text
    if contains_any(body, [p.lower() for p in INST_REMOVAL_PHRASES]):
        return "removed"

    # Login wall without removal content -> Active per request
    if looks_like_login(body, cur_url):
        return "active"

    # Active signals (visible media containers or OG tags)
    try:
        if page.query_selector("article, video, div[role='dialog']"):
            return "active"
        if page.query_selector('meta[property="og:video"], meta[property="og:image"]'):
            return "active"
    except Exception:
        pass

    return "unknown"

def check_youtube(page, url: str) -> str:
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    except Exception:
        return "unknown"
    body = page_text(page)
    if contains_any(body, [x.lower() for x in YT_REMOVAL]):
        return "removed"
    code = resp.status if resp else 0
    if code and 200 <= code < 400:
        return "active"
    return "unknown"

def check_tiktok(page, url: str) -> str:
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    except Exception:
        return "unknown"
    body = page_text(page)
    if contains_any(body, [x.lower() for x in TT_REMOVAL]):
        return "removed"
    code = resp.status if resp else 0
    if code and 200 <= code < 400:
        return "active"
    return "unknown"

def check_facebook(page, url: str) -> str:
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    except Exception:
        return "unknown"

    try:
        page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_MS)
    except Exception:
        pass
    time.sleep(SETTLE_SLEEP_S)

    body = page_text(page)
    cur_url = page.url.lower()

    if contains_any(body, [x.lower() for x in FB_REMOVAL]):
        return "removed"

    # Login wall without clear removal -> Active per request
    if looks_like_login(body, cur_url):
        return "active"

    code = resp.status if resp else 0
    if code and 200 <= code < 400:
        return "active"
    return "unknown"

def check_threads(page, url: str) -> str:
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    except Exception:
        return "unknown"

    final_url = page.url.lower()
    if "error=invalid_post" in final_url:
        return "removed"

    try:
        page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_MS)
    except Exception:
        pass
    time.sleep(SETTLE_SLEEP_S)

    body = page_text(page)
    if THREADS_UNAVAILABLE_BADGE in body:
        return "removed"

    code = resp.status if resp else 0
    if code and 200 <= code < 400:
        return "active"
    return "unknown"

def check_one(page, url: str) -> str:
    p = host_platform(url)
    if p == "instagram":
        return check_instagram(page, url)
    if p == "youtube":
        return check_youtube(page, url)
    if p == "tiktok":
        return check_tiktok(page, url)
    if p == "facebook":
        return check_facebook(page, url)
    if p == "threads":
        return check_threads(page, url)

    # Fallback
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        code = resp.status if resp else 0
        return "active" if code and 200 <= code < 400 else "unknown"
    except Exception:
        return "unknown"

# ----------------- Main -----------------
def main():
    gc = make_gspread_client()
    ws = gc.open_by_key(SHEET_ID).worksheet(TAB)

    values = ws.get_all_values()
    if not values:
        print("No data.")
        return

    today = datetime.now().strftime("%m/%d/%Y")
    updates = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        for i in range(START_ROW - 1, len(values)):
            row_idx = i + 1  # 1-based
            row = values[i]
            url = normalize_url(row[URL_COL - 1] if len(row) >= URL_COL else "")
            status_now = (row[STATUS_COL - 1] if len(row) >= STATUS_COL else "").strip().lower()

            if not url:
                continue
            if status_now in SKIP_STATUS_VALUES:
                print(f"⏭️  Skipping row {row_idx} (status: '{status_now}')")
                continue

            print(f"🔎 Checking row {row_idx}: {url}")
            result = check_one(page, url)

            removal_date = today if result == "removed" else ""
            last_checked = today

            updates.append({
                "range": f"{col_letter(STATUS_COL)}{row_idx}:{col_letter(LAST_CHECKED_COL)}{row_idx}",
                "values": [[result.title(), removal_date, last_checked]],
            })

            sleep_for = random.uniform(*DELAY_RANGE)
            print(f"   → {result} | sleeping {sleep_for:.1f}s")
            time.sleep(sleep_for)

        browser.close()

    if updates:
        ws.batch_update(updates)
    print("✅ Done.")

# Utility: 1-based column index → letters (13 → 'M')
def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

if __name__ == "__main__":
    main()
