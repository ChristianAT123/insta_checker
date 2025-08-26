import os
import json
import time
import random
from datetime import datetime
from urllib.parse import urlparse

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

# ==== CONFIG ====
SPREADSHEET_ID = "1ps5Luzxgk0nNGWqTPCw9y9MhgCCnoAuKpZ73UmlanPE"
SHEET_NAME = "Sheet1"

START_ROW = 2                    # first data row
DELAY_RANGE = (4, 7)             # seconds between requests
NAV_TIMEOUT_MS = 15000           # Playwright goto timeout (ms)
SETTLE_SLEEP_S = 3               # wait after navigation (s)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Removal phrases (exact strings provided)
REMOVAL_PHRASES = {
    "instagram.com": ["sorry, this page isn't available."],
    "youtube.com":   ["this video isn't available anymore"],
    "youtu.be":      ["this video isn't available anymore"],
    "tiktok.com":    ["video currently unavailable"],
    "facebook.com":  ["this content isn't available right now"],
    "m.facebook.com":["this content isn't available right now"],
    "fb.watch":      ["this content isn't available right now"],  # fb short links
}

# Heuristic: URLs that indicate a login wall (we mark Unknown)
LOGIN_HINTS = [
    "/login", "login.php", "signin", "facebook.com/?next=", "youtube.com/accounts",
]

# Facebook “watch hub” URL used as a removed redirect target
FB_WATCH_HUB = "https://www.facebook.com/watch/"

# ==== Google Sheets auth ====
def get_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_env = os.getenv("GOOGLE_CREDENTIALS")
    if creds_env:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_env), scope)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    return gspread.authorize(creds)

def host_key(netloc: str) -> str:
    """
    Collapse subdomains to a matching key present in REMOVAL_PHRASES.
    e.g., www.instagram.com -> instagram.com
    """
    netloc = netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    # Special-case youtu.be shortener
    if netloc == "youtu.be":
        return "youtu.be"
    # Reduce deep subdomains to second-level for fb/tiktok/youtube/instagram
    parts = netloc.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])  # example: m.facebook.com -> facebook.com (we also include m.facebook.com above)
    return netloc

def appears_login_url(url: str) -> bool:
    u = url.lower()
    return any(hint in u for hint in LOGIN_HINTS)

def classify_status(final_url: str, html: str, original_host: str) -> str:
    """
    Decide Active / Removed / Unknown using:
    - domain-specific phrases
    - facebook watch redirect
    - login-wall heuristics
    """
    h = html.lower()
    # 1) Domain phrases
    phrases = []
    # Check both exact host and collapsed host, plus a couple of alternates for FB
    keys_to_try = {original_host, host_key(original_host)}
    if original_host.endswith("facebook.com"):
        keys_to_try.update({"facebook.com", "m.facebook.com"})
    for k in keys_to_try:
        phrases.extend(REMOVAL_PHRASES.get(k, []))

    if any(p in h for p in phrases):
        return "Removed"

    # 2) Facebook: removed often redirects to watch hub
    if original_host.endswith("facebook.com"):
        # Normalize trailing slash
        if final_url.split("?")[0].rstrip("/") + "/" == FB_WATCH_HUB:
            return "Removed"

    # 3) Login wall
    if appears_login_url(final_url):
        return "Unknown (Login required)"

    # 4) Default
    return "Active"

def main():
    client = get_client()
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    rows = sheet.get_all_values()
    if not rows:
        print("No data in sheet.")
        return

    updates = []  # collected per-row updates for batch_update
    now_date = datetime.now().strftime("%m/%d/%Y")
    now_ts   = datetime.now().strftime("%m/%d/%Y %H:%M")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        # loop through rows
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

            parsed = urlparse(link)
            original_host = parsed.netloc.lower()
            print(f"🔎 Checking row {row_num}: {link}")

            status = "Unknown"
            removal_date = ""
            error_details = ""

            try:
                resp = page.goto(link, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
                time.sleep(SETTLE_SLEEP_S)

                html = page.content()
                code = resp.status if resp else 0
                final_url = page.url  # after redirects
                error_details = f"Code: {code}"

                status = classify_status(final_url, html, original_host)
                if status == "Removed":
                    removal_date = now_date

            except Exception as e:
                status = "Unknown"
                error_details = f"Error: {e!s}"

            updates.append({
                "range": f"B{row_num}:E{row_num}",
                "values": [[status, removal_date, now_ts, error_details]],
            })

            sleep_for = random.uniform(*DELAY_RANGE)
            print(f"   → {status} | sleeping {sleep_for:.1f}s")
            time.sleep(sleep_for)

        browser.close()

    if updates:
        sheet.batch_update(updates)

    print("✅ Done checking links across Instagram/YouTube/TikTok/Facebook.")

if __name__ == "__main__":
    main()
