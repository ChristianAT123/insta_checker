# check_instagram_links.py  (multi-platform, hardened Facebook detection)

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

# ========= CONFIG =========
SPREADSHEET_ID = "1ps5Luzxgk0nNGWqTPCw9y9MhgCCnoAuKpZ73UmlanPE"
SHEET_NAME = "Sheet1"

START_ROW = 2
DELAY_SEC = (4, 7)
NAV_TIMEOUT_MS = 20000            # give a bit more time on FB
SETTLE_SLEEP_S = 4                # extra settle time

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Removal phrases (case-sensitive substring checks on HTML).
# We'll also do case-insensitive locator checks for robustness.
REMOVAL_TEXT = {
    "instagram": [
        "Sorry, this page isn't available.",
    ],
    "youtube": [
        "This video isn't available anymore",
        "Video unavailable",
    ],
    "tiktok": [
        "Video currently unavailable",
    ],
    "facebook": [
        # common FB wordings
        "This page isn't available right now",
        "This content isn't available right now",
        "This Video Isn't Available Anymore",
        "The link may be broken or the video may have been removed",
    ],
}

# ========= HELPERS =========
def get_gspread_client():
    """
    Accept credentials from:
      - GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_JSON (raw JSON)
      - GOOGLE_APPLICATION_CREDENTIALS (filepath)
      - fallback to local credentials.json
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_env = os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS_JSON")
    if creds_env:
        creds_dict = json.loads(creds_env)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)

    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and os.path.isfile(cred_path):
        creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scope)
        return gspread.authorize(creds)

    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    return gspread.authorize(creds)


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
    """
    Treat redirect to /watch/ with no ?v= as 'Removed' for Facebook videos.
    """
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


def fb_removed_via_locators(page) -> bool:
    """
    Extra-robust DOM checks for FB 'removed' states even with login modals.
    We use case-insensitive regex text locators.
    """
    patterns = [
        r"This\s+page\s+isn'?t\s+available\s+right\s+now",
        r"This\s+content\s+isn'?t\s+available\s+right\s+now",
        r"This\s+Video\s+Isn'?t\s+Available\s+Anymore",
        r"The\s+link\s+may\s+be\s+broken\s+or\s+the\s+video\s+may\s+have\s+been\s+removed",
    ]
    for pat in patterns:
        if page.locator(f"text=/{pat}/i").count() > 0:
            return True
    return False


def check_one(page, url: str) -> tuple[str, str]:
    """
    Navigate and classify.
    Returns (status, error_details).
    """
    platform = detect_platform(url)
    try:
        # FB can be slow; use 'networkidle' to let the login modal / banners render.
        resp = page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="networkidle")
        time.sleep(SETTLE_SLEEP_S)
        code = resp.status if resp else 0
        html = page.content()
        final_url = page.url

        # Facebook special: redirected to /watch/ without v= param
        if platform == "facebook" and looks_like_fb_watch_home(final_url):
            return "Removed", f"Code: {code} (redirected to /watch/)"

        # Phrase checks (raw HTML)
        phrases = REMOVAL_TEXT.get(platform, [])
        if phrases and contains_any(html, phrases):
            return "Removed", f"Code: {code}"

        # Extra robust FB locator checks for banners under modals
        if platform == "facebook" and fb_removed_via_locators(page):
            return "Removed", f"Code: {code} (banner)"

        # Basic success heuristic
        if code and 200 <= code < 400:
            return "Active", f"Code: {code}"

        return "Unknown", f"Code: {code}"

    except Exception as e:
        return "Unknown", f"Error: {e}"


# ========= MAIN =========
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
        context = browser.new_context(
            user_agent=USER_AGENT,
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = context.new_page()

        for i in range(START_ROW - 1, len(rows)):
            row_num = i + 1  # 1-based
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

            # B=status, C=removal date, D=last checked, E=error details
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
