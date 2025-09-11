# check_instagram_links.py
import os
import re
import json
import time
import random
from urllib.parse import urlparse
from datetime import datetime, timedelta

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ===== Primary sheet (existing) =====
SHEET_ID = "1sPsWqoEqd1YmD752fuz7j1K3VSGggpzlkc_Tp7Pr4jQ"
TAB = "Logs"
URL_COL = 6
STATUS_COL = 13
REMOVAL_DATE_COL = 14
LAST_CHECKED_COL = 15

# ===== Additional sheet (two tabs) =====
ARCHIVE_SHEET_ID = "1P698PUG-i578PdPm13MfrGo9svzK97sHw012isxisUY"
ARCHIVE_TABS = [
    ("Facebook RM Archives", dict(URL=10, STATUS=15, REMOVAL=16, LAST=17)),
    ("Instagram RM Archives", dict(URL=10, STATUS=15, REMOVAL=16, LAST=17)),
]

# Scheduler/runtime behavior
START_ROW = 2
SKIP_STATUS_VALUES = {"removed"}
DELAY_RANGE = (4.0, 7.0)
FLUSH_EVERY = 250

# Browser timing
NAV_TIMEOUT_MS = 15000
NETWORK_IDLE_MS = 7000
SETTLE_SLEEP_S = 2.0

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

INST_REMOVAL_PHRASES = [
    "sorry, this page isn't available",
    "the link you followed may be broken",
    "page not found",
]
YT_REMOVAL = ["video unavailable", "this video isn't available anymore"]
TT_REMOVAL = ["video currently unavailable"]
FB_REMOVAL = ["this content isn't available right now"]
THREADS_UNAVAILABLE_BADGE = "post unavailable"
LOGIN_CUES = ["log in", "sign up", "/accounts/login", "login.facebook", "log in to facebook"]


def make_gspread_client():
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
            pass
    for path in [os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), "credentials.json"]:
        if path and os.path.isfile(path):
            raw = open(path, "rb").read().lstrip(b"\xef\xbb\xbf\r\n\t ")
            creds_dict = json.loads(raw.decode("utf-8"))
            return gspread.authorize(
                ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            )
    raise RuntimeError("Could not load Google credentials from env or credentials.json")


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
    return ("/accounts/login" in (url_now or "")) or contains_any(body or "", LOGIN_CUES)


def parse_mmddyyyy(s: str):
    try:
        return datetime.strptime(s.strip(), "%m/%d/%Y")
    except Exception:
        return None


def recent_enough(last_str: str, skip_days: int) -> bool:
    if skip_days <= 0:
        return False
    d = parse_mmddyyyy(last_str or "")
    if not d:
        return False
    return (datetime.now() - d) < timedelta(days=skip_days)


def check_instagram(page, url: str) -> str:
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
    if contains_any(body, [p.lower() for p in INST_REMOVAL_PHRASES]):
        return "removed"
    if looks_like_login(body, cur_url):
        return "active"
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
    cur_url = page.url.lower() if page.url else ""
    if contains_any(body, [x.lower() for x in FB_REMOVAL]):
        return "removed"
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
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        code = resp.status if resp else 0
        return "active" if code and 200 <= code < 400 else "unknown"
    except Exception:
        return "unknown"


def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def run_one_sheet(gc, sheet_id: str, tab: str, cols: dict, page):
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    values = ws.get_all_values()
    if not values:
        return

    URLC = cols["URL"]
    STC = cols["STATUS"]
    RDC = cols["REMOVAL"]
    LCC = cols["LAST"]

    SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
    TOTAL_SHARDS = int(os.getenv("TOTAL_SHARDS", "1"))
    SKIP_RECENT_DAYS = int(os.getenv("SKIP_RECENT_DAYS", "0"))

    today = datetime.now().strftime("%m/%d/%Y")
    updates = []

    def flush():
        nonlocal updates
        if updates:
            ws.batch_update(updates)
            updates = []

    for i in range(START_ROW - 1, len(values)):
        row_idx = i + 1
        if (i % TOTAL_SHARDS) != SHARD_INDEX:
            continue

        row = values[i]
        url = normalize_url(row[URLC - 1] if len(row) >= URLC else "")
        status_now = (row[STC - 1] if len(row) >= STC else "").strip().lower()
        last_checked_str = (row[LCC - 1] if len(row) >= LCC else "").strip()

        if not url:
            continue
        if status_now in SKIP_STATUS_VALUES:
            print(f"⏭️  Skipping row {row_idx} [{tab}] (status: '{status_now}')")
            continue
        if SKIP_RECENT_DAYS > 0 and recent_enough(last_checked_str, SKIP_RECENT_DAYS):
            print(f"⏭️  Skipping row {row_idx} [{tab}] (recent: '{last_checked_str}')")
            continue

        print(f"🔎 [{tab}] Checking row {row_idx}: {url}")
        result = check_one(page, url)

        removal_date = today if result == "removed" else ""
        last_checked = today

        updates.append({
            "range": f"{col_letter(STC)}{row_idx}:{col_letter(LCC)}{row_idx}",
            "values": [[result.title(), removal_date, last_checked]],
        })

        if len(updates) >= FLUSH_EVERY:
            flush()

        sleep_for = random.uniform(*DELAY_RANGE)
        print(f"   → {result} | sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)

    flush()


def main():
    gc = make_gspread_client()

    run_list_env = os.getenv("SHEETS_INPUT", "").strip()
    if run_list_env:
        try:
            parsed = json.loads(run_list_env)
            run_list = []
            for item in parsed:
                run_list.append(
                    (
                        item["sheet_id"],
                        item["tab"],
                        dict(
                            URL=int(item["cols"]["URL"]),
                            STATUS=int(item["cols"]["STATUS"]),
                            REMOVAL=int(item["cols"]["REMOVAL"]),
                            LAST=int(item["cols"]["LAST"]),
                        ),
                    )
                )
        except Exception:
            run_list = []
    else:
        run_list = [
            (SHEET_ID, TAB, dict(URL=URL_COL, STATUS=STATUS_COL, REMOVAL=REMOVAL_DATE_COL, LAST=LAST_CHECKED_COL)),
            *[(ARCHIVE_SHEET_ID, t, c) for t, c in ARCHIVE_TABS],
        ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        for sheet_id, tab, cols in run_list:
            print(f"=== Running: {sheet_id} / {tab} ===")
            run_one_sheet(gc, sheet_id, tab, cols, page)

        browser.close()

    print("✅ Done.")


if __name__ == "__main__":
    main()
