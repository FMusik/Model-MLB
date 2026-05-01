"""
atropa_writer.py
────────────────
Paste Atropa picks (extracted by Claude) and write to Google Sheets.

USAGE:
    1. Paste screenshot into Claude chat
    2. Claude extracts picks → pastes them into the PICKS list below
    3. Run: python atropa_writer.py

REQUIREMENTS:
    pip install gspread google-auth
"""

import os
import datetime
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────
SHEET_URL   = "https://docs.google.com/spreadsheets/d/11mgGrwt8ZTNSXlMXk3mTctUOLoU7_4Y8pfiQ2RHYVGc/edit"
ATROPA_TAB  = "📸 Atropa vs Model"
CREDS_FILE  = os.path.join(os.path.dirname(__file__), "credentials.json")

HEADERS = [
    "Date", "Game", "Time", "Type",
    "Atropa Pick", "Atropa Book", "Atropa Proj",
    "Our Pick", "Our Proj", "Agreement",
    "Atropa W/L", "Our W/L",
]

# ── PASTE PICKS HERE ──────────────────────────────────────────
# Claude will fill this in after you paste a screenshot.
# Format: one dict per pick.
DATE  = "2026-05-01"   # ← update to today's date
PICKS = [
    # Example — Claude will replace these:
    # {"matchup": "ARI@CHC", "time": "1:20 AM", "type": "Total", "pick": "O7.5", "book": "115", "proj": "proj 8.6 (+1.1)"},
    # {"matchup": "CWS@SDP", "time": "8:40 AM", "type": "ML",    "pick": "CWS ML", "book": "129", "proj": "62% win"},
]

# ── SHEETS AUTH ───────────────────────────────────────────────
def get_sheet():
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc    = gspread.authorize(creds)
    return gc.open_by_url(SHEET_URL)


def get_or_create_tab(sheet):
    try:
        ws = sheet.worksheet(ATROPA_TAB)
        print(f"   📋 Found tab: {ATROPA_TAB}")
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=ATROPA_TAB, rows=500, cols=len(HEADERS))
        ws.append_row(HEADERS)
        print(f"   ✅ Created tab: {ATROPA_TAB}")
    return ws


# ── WRITE ─────────────────────────────────────────────────────
def write_picks():
    if not PICKS:
        print("❌ No picks in PICKS list — paste data first.")
        return

    print(f"\n⚾ Writing {len(PICKS)} Atropa picks for {DATE}...")
    sheet = get_sheet()
    ws    = get_or_create_tab(sheet)

    for p in PICKS:
        row = [
            DATE,
            p.get("matchup", ""),
            p.get("time", ""),
            p.get("type", ""),
            p.get("pick", ""),
            p.get("book", ""),
            p.get("proj", ""),
            "",          # Our Pick
            "",          # Our Proj
            "",          # Agreement
            "PENDING",   # Atropa W/L
            "PENDING",   # Our W/L
        ]
        ws.append_row(row)
        print(f"   ✅ {p.get('matchup','?')} | {p.get('pick','?')} | {p.get('proj','?')}")

    print(f"\n✅ Done — {len(PICKS)} rows written to '{ATROPA_TAB}'")


if __name__ == "__main__":
    write_picks()
