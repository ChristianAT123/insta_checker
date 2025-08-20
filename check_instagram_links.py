import time
import random
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

# ==== CONFIG ====
SPREADSHEET_ID = "1ps5Luzxgk0nNGWqTPCw9y9MhgCCnoAuKpZ73UmlanPE"
SHEET_NAME = "Sheet1"

REMOVAL_PHRASE = "Sorry, this page isn't available."
START_ROW = 2                   # first data row
DELAY_RANGE = (4, 7)            # seconds between requests
NAV_TIMEOUT_MS = 15000          # Playwright goto timeout
SETTLE_SLEEP_S = 3              # wait after navigation

# ==== Google Sheets auth ====
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# Fetch all rows
rows = sheet.get_all_values()  # list of lists
# rows[0] is the header row (A1:E1). Data begins at START_ROW-1 index.

updates = []  # collect per-row updates for batch_update

now_date = datetime.now().strftime("%m/%d/%Y")
now_ts   = datetime.now().strftime("%m/%d/%Y %H:%M")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36")
    )
    page = context.new_page()

    # Loop over sheet rows
    for i in range(START_ROW - 1, len(rows)):
        row_num = i + 1  # 1-based row in the sheet
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

        status = "Unknown"
        removal_date = ""
        error_details = ""

        try:
            resp = page.goto(link, timeout=NAV_TIMEOUT_MS)
            time.sleep(SETTLE_SLEEP_S)

            html = page.content()
            code = resp.status if resp else 0
            error_details = f"Code: {code}"

            if REMOVAL_PHRASE in html:
                status = "Removed"
                removal_date = now_date
            else:
                status = "Active"

        except Exception as e:
            status = "Unknown"
            error_details = f"Error: {e!s}"

        # Queue an update ONLY for this row (does not touch other rows)
        updates.append({
            "range": f"B{row_num}:E{row_num}",
            "values": [[status, removal_date, now_date, error_details]],
        })

        # polite delay
        sleep_for = random.uniform(*DELAY_RANGE)
        print(f"   → {status} | sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)

    browser.close()

# Apply all updates at once without clearing skipped rows
if updates:
    sheet.batch_update(updates)

print("✅ Done checking Instagram links without clearing skipped rows.")
