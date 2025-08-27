import os
import re
import json
import time
import random
from urllib.parse import urlparse, parse_qs
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

SPREADSHEET_ID = "1ps5Luzxgk0nNGWqTPCw9y9MhgCCnoAuKpZ73UmlanPE"
SHEET_NAME = "Sheet1"

START_ROW = 2
DELAY_SEC = (4, 7)
NAV_TIMEOUT_MS = 15000
SETTLE_SLEEP_S = 3

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

REMOVAL_TEXT = {
    "instagram": ["Sorry, this page isn't available."],
    "youtube": ["This video isn't available anymore", "Video unavailable"],
    "tiktok": ["Video currently unavailable"],
    "facebook": ["This content isn't available right now"],
}

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
                raw = raw.lstrip(b"\xef\xbb\xbf\r\n\t ")  # strip BOM/whitespace/newlines
                creds_dict = json.loads(raw.decode("utf-8"))
                return gspread.authorize(
                    ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
                )
            except Exception:
                continue

    raise RuntimeError("Could not load Google credentials from env or file.")

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
    return "unknown"

def looks_like_fb_watch_home(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if "facebook.com" not in parsed.netloc.lower():
            return False
        if re.fullmatch(r"/watch/?", parsed.path):
            qs = parse_qs(parsed.query or "")
            return "v" not in qs or len(qs.get("v", [])) == 0
        return False
    except Exception:
        return False

def contains_any(haystack: str, needles: list[str]) -> bool:
    haystack = haystack or ""
    return any(n in haystack for n in needles)

def check_one(page, url: str) -> tuple[str, str]:
    platform = detect_platform(url)
    try:
        resp = page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        time.sleep(SETTLE_SLEEP_S)
        code = resp.status if resp else 0
        html = page.content()
        final_url = page.url

        if platform == "facebook" and looks_like_fb_watch_home(final_url):
            return "Removed", f"Code: {code} (redirected to /watch/)"

        phrases = REMOVAL_TEXT.get(platform, [])
        if phrases and contains_any(html, phrases):
            return "Removed", f"Code: {code}"

        if code and 200 <= code < 400:
            return "Active", f"Code: {code}"

        return "Unknown", f"Code: {code}"
    except Exception as e:
        return "Unknown", f"Error: {e}"

def main():
    client = get_gspread_client()
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
    rows = sheet.get_all_values()
    if not rows:
        print("No data in sheet.")
        return

    updates = []
    today = datetime.now().strftime("%m/%d/%Y")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        for i in range(START_ROW - 1, len(rows)):
            row_num = i + 1
            row = rows[i]
            link = row[0].strip() if len(row) >= 1 else ""
            current_status = row[1].strip().lower() if len(row) >= 2 else ""

            if not link:
                print(f"⏭️  Skipping row {row_num} (no URL)")
                continue
            if current_status == "removed":
                print(f"⏭️  Skipping row {row_num} (status: 'removed')")
                continue

            print(f"🔍 Checking row {row_num}: {link}")
            status, details = check_one(page, link)

            removal_date = today if status == "Removed" else ""
            last_checked = today

            updates.append({
                "range": f"B{row_num}:E{row_num}",
                "values": [[status, removal_date, last_checked, details]],
            })

            sleep_for = random.uniform(*DELAY_SEC)
            print(f"   → {status} | sleeping {sleep_for:.1f}s")
            time.sleep(sleep_for)

        browser.close()

    if updates:
        sheet.batch_update(updates)

    print("✅ Done checking links without touching pre-Removed rows.")

if __name__ == "__main__":
    main()
