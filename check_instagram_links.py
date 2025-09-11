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

# ------------ Sheet configs (choose via env SHEET_TO_RUN: primary | fb_rm | ig_rm) ------------
SHEETS = {
    "primary": {
        "sheet_id": "1sPsWqoEqd1YmD752fuz7j1K3VSGggpzlkc_Tp7Pr4jQ",
        "tabs": ["Logs"],
        "URL_COL": 6,   # F
        "STATUS_COL": 13,  # M
        "REMOVAL_DATE_COL": 14,  # N
        "LAST_CHECKED_COL": 15,  # O
    },
    "fb_rm": {
        "sheet_id": "1P698PUG-i578PdPm13MfrGo9svzK97sHw012isxisUY",
        "tabs": ["Facebook RM Archives"],
        "URL_COL": 10,  # J
        "STATUS_COL": 15,  # O
        "REMOVAL_DATE_COL": 16,  # P
        "LAST_CHECKED_COL": 17,  # Q
    },
    "ig_rm": {
        "sheet_id": "1P698PUG-i578PdPm13MfrGo9svzK97sHw012isxisUY",
        "tabs": ["Instagram RM Archives"],
        "URL_COL": 10,  # J
        "STATUS_COL": 15,  # O
        "REMOVAL_DATE_COL": 16,  # P
        "LAST_CHECKED_COL": 17,  # Q
    },
}

# ------------ Behavior ------------
START_ROW = 2
SKIP_STATUS_VALUES = {"removed"}
DELAY_RANGE = (4.0, 7.0)
FLUSH_EVERY = 250

# ------------ Browser timing ------------
NAV_TIMEOUT_MS = 15000
NETWORK_IDLE_MS = 7000
SETTLE_SLEEP_S = 2.0

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ------------ Text fingerprints ------------
INST_REMOVAL_PHRASES = [
    "sorry, this page isn't available",
    "the link you followed may be broken",
    "page not found",
]
YT_REMOVAL = ["video unavailable", "this video isn't available anymore"]
TT_REMOVAL = ["video currently unavailable"]
FB_REMOVAL = ["this content isn't available right now", "this video isn't available anymore"]
THREADS_UNAVAILABLE_BADGE = "post unavailable"
LOGIN_CUES = ["log in", "sign up", "/accounts/login", "login.facebook", "log in to facebook"]


# ==================== Sheets auth ====================
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


# ==================== Helpers ====================
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
    return ("/login" in (url_now or "")) or contains_any(body or "", LOGIN_CUES)

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

def _attr(page, sel, attr):
    try:
        el = page.query_selector(sel)
        if el:
            return el.get_attribute(attr) or ""
    except Exception:
        pass
    return ""

def _canonical_url(page) -> str:
    return (_attr(page, 'link[rel="canonical"]', "href") or "").lower()

def _jsonld_video_present(page) -> bool:
    try:
        for el in page.query_selector_all('script[type="application/ld+json"]'):
            txt = (el.inner_text() or "").lower()
            if '"videoobject"' in txt or '"contenturl"' in txt or '"embedurl"' in txt:
                return True
    except Exception:
        pass
    return False

def _canonical_says_no_media(page) -> bool:
    can = _canonical_url(page)
    # If canonical collapses to /watch/ without ?v= or to a non-reel/non-video page, suspect removed
    return ("/watch" in can and "?v=" not in can) or ("/reel/" in can and can.rstrip("/").endswith("/reel"))

def _fb_watch_missing_v(u: str) -> bool:
    return "/watch" in u and "?v=" not in u

def _fb_reel_missing_id(u: str) -> bool:
    if "/reel/" not in u:
        return False
    try:
        tail = u.split("/reel/", 1)[1].split("?", 1)[0].strip("/")
        return not tail or not tail[0].isdigit()
    except Exception:
        return False

def fb_probe(p, engine: str, url: str):
    if engine == "chromium":
        br = p.chromium.launch(headless=True)
    elif engine == "webkit":
        br = p.webkit.launch(headless=True)
    else:
        br = p.chromium.launch(headless=True)
    ctx = br.new_context(user_agent=USER_AGENT)
    pg = ctx.new_page()
    try:
        try:
            pg.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        except PWTimeout:
            pg.goto(url, wait_until="networkidle")
        try:
            pg.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_MS)
        except Exception:
            pass
        time.sleep(SETTLE_SLEEP_S)
        final_url = (pg.url or "").lower()
        body = page_text(pg)
        og = bool(_attr(pg, 'meta[property="og:video"]', "content"))
        jd = _jsonld_video_present(pg)
        can = _canonical_url(pg)
        return final_url, body, og, jd, can
    finally:
        ctx.close()
        br.close()


# ==================== Per-platform checkers ====================
def check_instagram(page, url: str) -> str:
    page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
    page.set_default_timeout(NAV_TIMEOUT_MS)
    try:
        page.goto(url, wait_until="domcontentloaded")
    except PWTimeout:
        try:
            page.goto(url, wait_until="networkidle")
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

def check_facebook(page, url: str, pw) -> str:
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    except Exception:
        return "unknown"
    try:
        page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_MS)
    except Exception:
        pass
    time.sleep(SETTLE_SLEEP_S)

    final_url = (page.url or "").lower()
    body = page_text(page)

    og_video = bool(_attr(page, 'meta[property="og:video"]', "content"))
    jsonld_has_video = _jsonld_video_present(page)

    # Removal banner visible and no media → removed
    if contains_any(body, [x.lower() for x in FB_REMOVAL]) and not og_video and not jsonld_has_video:
        return "removed"

    # URL or canonical heuristics
    if _fb_watch_missing_v(final_url) or _fb_reel_missing_id(final_url):
        return "removed"
    if _canonical_says_no_media(page) and not og_video and not jsonld_has_video:
        return "removed"

    # Login wall probe: pick engine based on path
    if looks_like_login(body, final_url):
        try:
            path = urlparse(url).path.lower()
            if path.startswith("/watch"):
                f2, b2, og2, jd2, can2 = fb_probe(pw, "chromium", url)
            else:
                f2, b2, og2, jd2, can2 = fb_probe(pw, "webkit", url)

            if contains_any(b2, [x.lower() for x in FB_REMOVAL]) and not og2 and not jd2:
                if ("/watch" in can2 and "?v=" not in can2) or _fb_reel_missing_id(can2) or _fb_watch_missing_v(f2):
                    return "removed"
                return "removed"
        except Exception:
            pass

    if og_video or jsonld_has_video or "?v=" in final_url or "/reel/" in final_url:
        return "active"

    try:
        code = resp.status if resp else 0
        if code and 200 <= code < 400:
            return "active"
    except Exception:
        pass
    return "unknown"


def check_one(page, url: str, pw) -> str:
    p = host_platform(url)
    if p == "instagram":
        return check_instagram(page, url)
    if p == "youtube":
        return check_youtube(page, url)
    if p == "tiktok":
        return check_tiktok(page, url)
    if p == "facebook":
        return check_facebook(page, url, pw)
    if p == "threads":
        return check_threads(page, url)
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        code = resp.status if resp else 0
        return "active" if code and 200 <= code < 400 else "unknown"
    except Exception:
        return "unknown"


# ==================== Runner ====================
def run_sheet(gc, cfg, pw, shared_browser=None):
    ws = gc.open_by_key(cfg["sheet_id"]).worksheet(cfg["tabs"][0])
    values = ws.get_all_values()
    if not values:
        print("No data.")
        return

    URL_COL = cfg["URL_COL"]
    STATUS_COL = cfg["STATUS_COL"]
    REMOVAL_DATE_COL = cfg["REMOVAL_DATE_COL"]
    LAST_CHECKED_COL = cfg["LAST_CHECKED_COL"]

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

    browser = shared_browser or pw.chromium.launch(headless=True)
    context = browser.new_context(user_agent=USER_AGENT)
    page = context.new_page()

    print(f"=== Running: {cfg['sheet_id']} / {cfg['tabs'][0]} ===")
    for i in range(START_ROW - 1, len(values)):
        row_idx = i + 1
        if (i % TOTAL_SHARDS) != SHARD_INDEX:
            continue

        row = values[i]
        url = normalize_url(row[URL_COL - 1] if len(row) >= URL_COL else "")
        status_now = (row[STATUS_COL - 1] if len(row) >= STATUS_COL else "").strip().lower()
        last_checked_str = (row[LAST_CHECKED_COL - 1] if len(row) >= LAST_CHECKED_COL else "").strip()

        if not url:
            continue
        if status_now in SKIP_STATUS_VALUES:
            print(f"⏭️  Skipping row {row_idx} (status: '{status_now}')")
            continue
        if SKIP_RECENT_DAYS > 0 and recent_enough(last_checked_str, SKIP_RECENT_DAYS):
            print(f"⏭️  Skipping row {row_idx} (recent: '{last_checked_str}')")
            continue

        print(f"🔎 Checking row {row_idx}: {url}")
        result = check_one(page, url, pw)

        removal_date = today if result == "removed" else ""
        last_checked = today

        updates.append({
            "range": f"{col_letter(STATUS_COL)}{row_idx}:{col_letter(LAST_CHECKED_COL)}{row_idx}",
            "values": [[result.title(), removal_date, last_checked]],
        })

        if len(updates) >= FLUSH_EVERY:
            flush()

        sleep_for = random.uniform(*DELAY_RANGE)
        print(f"   → {result} | sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)

    context.close()
    if not shared_browser:
        browser.close()
    flush()


def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def main():
    sheet_key = (os.getenv("SHEET_TO_RUN") or "primary").strip().lower()
    if sheet_key not in SHEETS:
        sheet_key = "primary"
    cfg = SHEETS[sheet_key]

    gc = make_gspread_client()

    with sync_playwright() as p:
        # Base Chromium installed by Actions; WebKit is installed in workflow
        run_sheet(gc, cfg, p)


if __name__ == "__main__":
    main()
