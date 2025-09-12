# check_instagram_links.py
import os
import json
import time
import random
from urllib.parse import urlparse
from datetime import datetime, timedelta

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SHEETS = {
    "primary": {
        "sheet_id": "1sPsWqoEqd1YmD752fuz7j1K3VSGggpzlkc_Tp7Pr4jQ",
        "tabs": ["Logs"],
        "url_col": 6,    # F
        "status_col": 13,  # M
        "removal_col": 14, # N
        "checked_col": 15, # O
        "start_row": 2,
    },
    "fb_rm": {
        "sheet_id": "1P698PUG-i578PdPm13MfrGo9svzK97sHw012isxisUY",
        "tabs": ["Facebook RM Archives"],
        "url_col": 10,   # J
        "status_col": 15, # O
        "removal_col": 16,# P
        "checked_col": 17,# Q
        "start_row": 2,
    },
    "ig_rm": {
        "sheet_id": "1P698PUG-i578PdPm13MfrGo9svzK97sHw012isxisUY",
        "tabs": ["Instagram RM Archives"],
        "url_col": 10,   # J
        "status_col": 15, # O
        "removal_col": 16,# P
        "checked_col": 17,# Q
        "start_row": 2,
    },
}

DELAY_RANGE = (4.0, 7.0)

NAV_TIMEOUT_MS = 15000
NETWORK_IDLE_MS = 7000
SETTLE_SLEEP_S = 2.0
ITEM_BUDGET_S = 30.0

FLUSH_EVERY = 250
SKIP_STATUS_VALUES = {"removed"}

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

YT_REMOVAL = [
    "video unavailable",
    "this video isn't available anymore",
]

TT_REMOVAL = [
    "video currently unavailable",
]

FB_REMOVAL = [
    "this content isn't available right now",
    "this page isn't available right now",
    "this video isn't available anymore",
    "this page isn't available right now."
]

THREADS_UNAVAILABLE_BADGE = "post unavailable"

LOGIN_CUES = [
    "log in",
    "sign up",
    "/accounts/login",
    "login.facebook",
    "log in to facebook",
]

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
    raise RuntimeError("Could not load Google credentials")

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return u
    if u.startswith(("http://", "https://")):
        return u
    # Handle bare domains like facebook.com/...
    if u.startswith("www."):
        return "https://" + u
    if any(u.startswith(dom) for dom in ("facebook.com", "instagram.com", "threads.net", "threads.com", "youtu.be", "youtube.com", "tiktok.com", "fb.watch")):
        return "https://" + u
    return u

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

def safe_goto(page, url: str) -> "Response|None":
    try:
        return page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    except PWTimeout:
        try:
            return page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT_MS)
        except Exception:
            return None
    except Exception:
        return None

def check_instagram(page_webkit, url: str) -> str:
    start = time.monotonic()
    page = page_webkit
    page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
    page.set_default_timeout(NAV_TIMEOUT_MS)
    resp = safe_goto(page, url)
    try:
        page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_MS)
    except Exception:
        pass
    time.sleep(SETTLE_SLEEP_S)
    body = page_text(page)
    cur_url = (page.url or "").lower()

    if contains_any(body, [p.lower() for p in INST_REMOVAL_PHRASES]):
        return "removed"
    if "instagram.com/accounts/login" in cur_url:
        # Behind login; look for unavailable markers present in bg
        if "content isn't available" in body or "not available" in body:
            return "removed"
        return "active"
    try:
        if page.query_selector("article, video, div[role='dialog']"):
            return "active"
        if page.query_selector('meta[property="og:video"], meta[property="og:image"]'):
            return "active"
    except Exception:
        pass

    if (time.monotonic() - start) > ITEM_BUDGET_S:
        return "unknown"
    code = resp.status if resp else 0
    return "active" if code and 200 <= code < 400 else "unknown"

def check_youtube(page, url: str) -> str:
    resp = safe_goto(page, url)
    body = page_text(page)
    if contains_any(body, [x.lower() for x in YT_REMOVAL]):
        return "removed"
    code = resp.status if resp else 0
    return "active" if code and 200 <= code < 400 else "unknown"

def check_tiktok(page, url: str) -> str:
    resp = safe_goto(page, url)
    body = page_text(page)
    if contains_any(body, [x.lower() for x in TT_REMOVAL]):
        return "removed"
    code = resp.status if resp else 0
    return "active" if code and 200 <= code < 400 else "unknown"

def dismiss_fb_login_modal(page):
    try:
        btn = page.query_selector('div[role="dialog"] [aria-label="Close"], [aria-label="Close"]')
        if btn:
            btn.click()
            time.sleep(0.5)
    except Exception:
        pass
    try:
        page.evaluate("""
            (() => {
              const dialogs = document.querySelectorAll('div[role="dialog"]');
              dialogs.forEach(d => d.remove());
              const overlays = document.querySelectorAll('[data-visualcompletion="ignore-dynamic"]');
              overlays.forEach(o => { if (getComputedStyle(o).position === 'fixed') o.remove(); });
            })();
        """)
        time.sleep(0.5)
    except Exception:
        pass

def check_facebook(page, url: str) -> str:
    resp = safe_goto(page, url)
    try:
        page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_MS)
    except Exception:
        pass
    time.sleep(SETTLE_SLEEP_S)

    dismiss_fb_login_modal(page)

    body = page_text(page)
    cur_url = (page.url or "").lower()

    if contains_any(body, [x.lower() for x in FB_REMOVAL]):
        return "removed"

    if "facebook.com/watch/" in cur_url and "v=" not in cur_url:
        return "removed"

    code = resp.status if resp else 0
    return "active" if code and 200 <= code < 400 else "unknown"

def check_threads(page_webkit, url: str) -> str:
    page = page_webkit
    resp = safe_goto(page, url)
    try:
        page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_MS)
    except Exception:
        pass
    time.sleep(SETTLE_SLEEP_S)
    body = page_text(page)
    cur_url = (page.url or "").lower()
    if "threads.com/?error=invalid_post" in cur_url:
        return "removed"
    if THREADS_UNAVAILABLE_BADGE in body:
        return "removed"
    code = resp.status if resp else 0
    return "active" if code and 200 <= code < 400 else "unknown"

def check_one(page_chromium, page_webkit, url: str) -> str:
    p = host_platform(url)
    if p == "instagram":
        return check_instagram(page_webkit, url)
    if p == "youtube":
        return check_youtube(page_chromium, url)
    if p == "tiktok":
        return check_tiktok(page_chromium, url)
    if p == "facebook":
        return check_facebook(page_chromium, url)
    if p == "threads":
        return check_threads(page_webkit, url)
    resp = safe_goto(page_chromium, url)
    code = resp.status if resp else 0
    return "active" if code and 200 <= code < 400 else "unknown"

def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def get_worksheet_by_title(gc, sheet_id: str, title: str):
    sh = gc.open_by_key(sheet_id)
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        want = title.strip().lower()
        for ws in sh.worksheets():
            if ws.title.strip().lower() == want:
                return ws
        raise

def run_sheet(gc, cfg, page_chromium, page_webkit):
    ws = get_worksheet_by_title(gc, cfg["sheet_id"], cfg["tabs"][0])
    values = ws.get_all_values()
    if not values:
        return

    URL_COL = cfg["url_col"]
    STATUS_COL = cfg["status_col"]
    REMOVAL_COL = cfg["removal_col"]
    CHECKED_COL = cfg["checked_col"]
    START_ROW = cfg["start_row"]

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
        url = normalize_url(row[URL_COL - 1] if len(row) >= URL_COL else "")
        status_now = (row[STATUS_COL - 1] if len(row) >= STATUS_COL else "").strip().lower()
        last_checked_str = (row[CHECKED_COL - 1] if len(row) >= CHECKED_COL else "").strip()

        if not url:
            continue
        if status_now in SKIP_STATUS_VALUES:
            print(f"⏭️  Skipping row {row_idx} (status: '{status_now}')")
            continue
        if SKIP_RECENT_DAYS > 0 and recent_enough(last_checked_str, SKIP_RECENT_DAYS):
            print(f"⏭️  Skipping row {row_idx} (recent: '{last_checked_str}')")
            continue

        print(f"🔎 Checking row {row_idx}: {url}")
        try:
            result = check_one(page_chromium, page_webkit, url)
        except Exception:
            result = "unknown"

        removal_date = today if result == "removed" else ""
        last_checked = today

        updates.append({
            "range": f"{col_letter(STATUS_COL)}{row_idx}:{col_letter(CHECKED_COL)}{row_idx}",
            "values": [[result.title(), removal_date, last_checked]],
        })

        if len(updates) >= FLUSH_EVERY:
            flush()

        sleep_for = random.uniform(*DELAY_RANGE)
        print(f"   → {result} | sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)

    flush()

def main():
    which = os.getenv("SHEET_NAME", "primary").strip().lower()
    if which not in SHEETS:
        which = "primary"
    cfg = SHEETS[which]
    print(f"=== Running: {cfg['sheet_id']} / {cfg['tabs'][0]} ===")

    gc = make_gspread_client()

    with sync_playwright() as p:
        browser_chromium = p.chromium.launch(headless=True)
        context_c = browser_chromium.new_context(user_agent=USER_AGENT)
        page_c = context_c.new_page()

        browser_webkit = p.webkit.launch(headless=True)
        context_w = browser_webkit.new_context(user_agent=USER_AGENT)
        page_w = context_w.new_page()

        run_sheet(gc, cfg, page_c, page_w)

        browser_c_pages = [page_c]
        browser_w_pages = [page_w]
        for pg in browser_c_pages:
            try:
                pg.context.browser.close()
            except Exception:
                pass
        for pg in browser_w_pages:
            try:
                pg.context.browser.close()
            except Exception:
                pass

    print("✅ Done.")

if __name__ == "__main__":
    main()
