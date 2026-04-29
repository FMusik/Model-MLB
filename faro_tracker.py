"""
faro_tracker.py
───────────────
Writes daily Faro picks + our model picks to '🏟️ Faro vs Model' tab.
Derives W/L from actual scores already in the 📊 Tracker tab.

USAGE (manual, from any session):
    python faro_tracker.py

    Then paste Faro picks when prompted, OR call write_faro_day() directly.

HOW TO USE WITH SCREENSHOTS:
    1. Paste screenshot into Claude chat
    2. Claude extracts picks → gives you a list of dicts
    3. Pass that list to write_faro_day()
"""

import json
import os
import datetime
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────
SHEET_URL    = "https://docs.google.com/spreadsheets/d/11mgGrwt8ZTNSXlMXk3mTctUOLoU7_4Y8pfiQ2RHYVGc/edit"
FARO_TAB     = "🏟️ Faro vs Model"
TRACKER_TAB  = "📊 Tracker"
CREDS_FILE   = "credentials.json"

FARO_HEADERS = [
    "Date",
    "Game",           # "Away @ Home"
    "Faro Pick",      # team name Faro predicted
    "Faro Conf%",     # e.g. 68.2
    "Our Pick",       # team with higher Our Proj score
    "Our Proj Score", # "Away X – Home Y"
    "Actual Winner",  # derived from Actual Away/Home scores
    "Faro W/L",       # WIN / LOSS / PUSH / PENDING
    "Our W/L",        # WIN / LOSS / PUSH / PENDING
]

# ── GOOGLE SHEETS AUTH ────────────────────────────────────────
def get_sheet():
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_path = os.path.join(os.path.dirname(__file__), CREDS_FILE)
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    gc    = gspread.authorize(creds)
    return gc.open_by_url(SHEET_URL)


# ── TEAM NAME NORMALISER ──────────────────────────────────────
# Faro uses full city+name; tracker uses city only or short name.
# Extend as needed.
_ALIASES = {
    "kansas city royals":     "Kansas City Royals",
    "atlanta braves":         "Atlanta Braves",
    "houston astros":         "Houston Astros",
    "chicago white sox":      "Chicago White Sox",
    "cincinnati reds":        "Cincinnati Reds",
    "tampa bay rays":         "Tampa Bay Rays",
    "los angeles dodgers":    "Los Angeles Dodgers",
    "milwaukee brewers":      "Milwaukee Brewers",
    "minnesota twins":        "Minnesota Twins",
    "new york mets":          "New York Mets",
    "philadelphia phillies":  "Philadelphia Phillies",
    "pittsburgh pirates":     "Pittsburgh Pirates",
    "san diego padres":       "San Diego Padres",
    "texas rangers":          "Texas Rangers",
    "toronto blue jays":      "Toronto Blue Jays",
    "new york yankees":       "New York Yankees",
    "boston red sox":         "Boston Red Sox",
    "baltimore orioles":      "Baltimore Orioles",
    "cleveland guardians":    "Cleveland Guardians",
    "detroit tigers":         "Detroit Tigers",
    "miami marlins":          "Miami Marlins",
    "arizona diamondbacks":   "Arizona Diamondbacks",
    "colorado rockies":       "Colorado Rockies",
    "washington nationals":   "Washington Nationals",
    "st. louis cardinals":    "St. Louis Cardinals",
    "chicago cubs":           "Chicago Cubs",
    "san francisco giants":   "San Francisco Giants",
    "los angeles angels":     "Los Angeles Angels",
    "seattle mariners":       "Seattle Mariners",
    "oakland athletics":      "Athletics",
    "athletics":              "Athletics",
}

def norm(name: str) -> str:
    return _ALIASES.get(name.strip().lower(), name.strip())


# ── LOAD OUR PICKS FROM TRACKER ──────────────────────────────
def load_our_picks(sheet, date_str: str) -> dict:
    """
    Returns dict keyed by game string "Away @ Home":
        { "Away @ Home": { "our_pick": str, "our_proj": str,
                           "actual_away": float, "actual_home": float } }
    Pulls all rows from 📊 Tracker for the given date.
    """
    print(f"  📋 Loading our picks from Tracker for {date_str}...")
    try:
        ws   = sheet.worksheet(TRACKER_TAB)
        rows = ws.get_all_values()
    except Exception as e:
        print(f"  ⚠️  Could not read Tracker: {e}")
        return {}

    if not rows:
        return {}

    headers = rows[0]
    def col(name):
        try:    return headers.index(name)
        except: return None

    date_col        = col("Date")
    game_col        = col("Game")
    proj_away_col   = col("Our Proj Away")
    proj_home_col   = col("Our Proj Home")
    actual_away_col = col("Actual Away")
    actual_home_col = col("Actual Home")

    if None in (date_col, game_col):
        print("  ⚠️  Tracker missing Date or Game column")
        return {}

    picks = {}
    seen_games = set()  # one entry per game (avoid duplicate bet types)

    for row in rows[1:]:
        if not row or len(row) <= game_col:
            continue
        if row[date_col].strip() != date_str:
            continue

        game = row[game_col].strip()
        if game in seen_games:
            continue
        seen_games.add(game)

        def safe_float(ci):
            try:
                v = row[ci].strip() if ci and ci < len(row) else ""
                return float(v) if v else None
            except:
                return None

        proj_away   = safe_float(proj_away_col)
        proj_home   = safe_float(proj_home_col)
        actual_away = safe_float(actual_away_col)
        actual_home = safe_float(actual_home_col)

        # Determine our pick: whichever proj is higher
        parts = game.split(" @ ")
        away_team = parts[0].strip() if len(parts) == 2 else "Away"
        home_team = parts[1].strip() if len(parts) == 2 else "Home"

        if proj_away is not None and proj_home is not None:
            our_pick = away_team if proj_away > proj_home else home_team
            our_proj = f"{away_team} {proj_away:.1f} – {home_team} {proj_home:.1f}"
        else:
            our_pick = ""
            our_proj = ""

        picks[game] = {
            "our_pick":    our_pick,
            "our_proj":    our_proj,
            "actual_away": actual_away,
            "actual_home": actual_home,
            "away_team":   away_team,
            "home_team":   home_team,
        }

    print(f"  ✅ Found {len(picks)} games in Tracker for {date_str}")
    return picks


# ── FUZZY GAME MATCH ──────────────────────────────────────────
def match_game(faro_away: str, faro_home: str, our_picks: dict) -> tuple:
    """
    Try to match Faro's Home/Away to our tracker game string.
    Returns (game_key, pick_dict) or (None, None).
    """
    fa = norm(faro_away).lower()
    fh = norm(faro_home).lower()

    for game_key, data in our_picks.items():
        ga = data["away_team"].lower()
        gh = data["home_team"].lower()
        # Both teams match (any word overlap)
        if (any(w in ga for w in fa.split()) and
                any(w in gh for w in fh.split())):
            return game_key, data
        # Also try reversed (Faro Home → our Away etc.)
        if (any(w in gh for w in fa.split()) and
                any(w in ga for w in fh.split())):
            return game_key, data

    return None, None


# ── DERIVE W/L ────────────────────────────────────────────────
def derive_wl(pick: str, away_team: str, home_team: str,
              actual_away, actual_home) -> str:
    """
    Given a pick (team name) and actual scores, return WIN/LOSS/PUSH/PENDING.
    """
    if actual_away is None or actual_home is None:
        return "PENDING"

    pick_l = pick.lower().strip()
    away_l = away_team.lower().strip()
    home_l = home_team.lower().strip()

    if actual_away > actual_home:
        actual_winner = away_l
    elif actual_home > actual_away:
        actual_winner = home_l
    else:
        return "PUSH"

    # Fuzzy match pick to winner
    pick_words  = set(pick_l.split())
    winner_words = set(actual_winner.split())
    if pick_words & winner_words:
        return "WIN"
    return "LOSS"


# ── ENSURE FARO TAB EXISTS ────────────────────────────────────
def ensure_faro_tab(sheet):
    try:
        ws = sheet.worksheet(FARO_TAB)
        print(f"  ✅ Tab '{FARO_TAB}' exists")
        return ws
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(FARO_TAB, rows=500, cols=len(FARO_HEADERS) + 2)
        ws.append_row(FARO_HEADERS)
        print(f"  ✅ Created tab '{FARO_TAB}'")
        return ws


# ── WRITE FARO DAY ────────────────────────────────────────────
def write_faro_day(faro_picks: list, date_str: str = None):
    """
    Main entry point.

    faro_picks: list of dicts, each with:
        {
            "home":      "Athletics",
            "away":      "Kansas City Royals",
            "home_pred": 5,
            "away_pred": 6,
            "faro_pick": "Kansas City Royals",
            "conf_pct":  58.7
        }

    date_str: "YYYY-MM-DD" — defaults to today
    """
    if date_str is None:
        date_str = datetime.date.today().strftime("%Y-%m-%d")

    print(f"\n🏟️  FARO TRACKER — {date_str}")
    print(f"  📥 {len(faro_picks)} Faro picks to process\n")

    sheet     = get_sheet()
    ws        = ensure_faro_tab(sheet)
    our_picks = load_our_picks(sheet, date_str)

    # Check for existing rows for this date (avoid dupes)
    existing = ws.get_all_values()
    existing_games = set()
    for row in existing[1:]:
        if len(row) >= 2 and row[0] == date_str:
            existing_games.add(row[1].strip().lower())

    rows_to_add = []
    matched = 0
    unmatched = []

    for pick in faro_picks:
        faro_away = pick.get("away", "")
        faro_home = pick.get("home", "")
        faro_winner = norm(pick.get("faro_pick", ""))
        faro_conf   = pick.get("conf_pct", "")

        # Build game string (Away @ Home) using our naming if matched
        game_key, tracker_data = match_game(faro_away, faro_home, our_picks)

        if tracker_data:
            game_str    = game_key
            away_team   = tracker_data["away_team"]
            home_team   = tracker_data["home_team"]
            our_pick    = tracker_data["our_pick"]
            our_proj    = tracker_data["our_proj"]
            actual_away = tracker_data["actual_away"]
            actual_home = tracker_data["actual_home"]
            matched += 1
        else:
            # No match in our tracker — still log Faro pick
            away_team   = norm(faro_away)
            home_team   = norm(faro_home)
            game_str    = f"{away_team} @ {home_team}"
            our_pick    = ""
            our_proj    = ""
            actual_away = None
            actual_home = None
            unmatched.append(game_str)

        if game_str.lower() in existing_games:
            print(f"  ⏭️  Already logged: {game_str}")
            continue

        # Derive W/L
        faro_wl = derive_wl(faro_winner, away_team, home_team,
                            actual_away, actual_home)
        our_wl  = derive_wl(our_pick, away_team, home_team,
                            actual_away, actual_home) if our_pick else "NO BET"

        # Actual winner label
        if actual_away is not None and actual_home is not None:
            if actual_away > actual_home:
                actual_winner = away_team
            elif actual_home > actual_away:
                actual_winner = home_team
            else:
                actual_winner = "PUSH"
        else:
            actual_winner = "PENDING"

        row = [
            date_str,
            game_str,
            faro_winner,
            faro_conf,
            our_pick,
            our_proj,
            actual_winner,
            faro_wl,
            our_wl,
        ]
        rows_to_add.append(row)

        status = f"Faro:{faro_wl} | Ours:{our_wl}"
        print(f"  {'✅' if faro_wl != 'PENDING' else '⏳'} {game_str} → {faro_winner} vs {our_pick} | {status}")

    if rows_to_add:
        ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        print(f"\n  ✅ Wrote {len(rows_to_add)} rows to '{FARO_TAB}'")
    else:
        print(f"\n  ℹ️  No new rows to write")

    if unmatched:
        print(f"\n  ⚠️  {len(unmatched)} games not found in our Tracker (logged anyway):")
        for g in unmatched:
            print(f"       {g}")

    print(f"\n  📊 Matched to our tracker: {matched}/{len(faro_picks)}")


# ── UPDATE W/L FOR EXISTING PENDING ROWS ─────────────────────
def update_pending_wl(date_str: str = None):
    """
    Re-check PENDING rows for a given date and fill in W/L
    once actual scores are in the Tracker tab.
    Call this after actuals are filled in.
    """
    if date_str is None:
        date_str = datetime.date.today().strftime("%Y-%m-%d")

    print(f"\n🔄 Updating PENDING rows for {date_str}...")
    sheet     = get_sheet()
    ws        = sheet.worksheet(FARO_TAB)
    our_picks = load_our_picks(sheet, date_str)

    all_rows = ws.get_all_values()
    if not all_rows:
        return

    headers = all_rows[0]
    updated = 0

    for i, row in enumerate(all_rows[1:], start=2):
        if len(row) < len(FARO_HEADERS):
            row += [""] * (len(FARO_HEADERS) - len(row))

        if row[0] != date_str:
            continue
        if "PENDING" not in (row[7], row[8]) or row[8] == "NO BET":
            continue

        game_str    = row[1].strip()
        faro_winner = row[2].strip()
        our_pick    = row[4].strip()

        # Find actual scores from our tracker
        tracker_data = our_picks.get(game_str)
        if not tracker_data:
            continue

        actual_away = tracker_data["actual_away"]
        actual_home = tracker_data["actual_home"]
        away_team   = tracker_data["away_team"]
        home_team   = tracker_data["home_team"]

        if actual_away is None or actual_home is None:
            continue

        # Actual winner
        if actual_away > actual_home:
            actual_winner = away_team
        elif actual_home > actual_away:
            actual_winner = home_team
        else:
            actual_winner = "PUSH"

        faro_wl = derive_wl(faro_winner, away_team, home_team, actual_away, actual_home)
        our_wl  = derive_wl(our_pick,    away_team, home_team, actual_away, actual_home) if our_pick else "NO BET"

        # Update sheet: cols G (actual winner), H (faro wl), I (our wl) = indices 7,8,9 (1-based: col 7,8,9)
        faro_col_letter  = chr(ord("A") + FARO_HEADERS.index("Actual Winner"))
        faro_wl_letter   = chr(ord("A") + FARO_HEADERS.index("Faro W/L"))
        our_wl_letter    = chr(ord("A") + FARO_HEADERS.index("Our W/L"))

        ws.update(f"{faro_col_letter}{i}", actual_winner)
        ws.update(f"{faro_wl_letter}{i}",  faro_wl)
        ws.update(f"{our_wl_letter}{i}",   our_wl)
        updated += 1
        print(f"  ✅ {game_str}: Faro {faro_wl} | Ours {our_wl}")

    print(f"\n  ✅ Updated {updated} rows")


# ── PRINT SUMMARY ─────────────────────────────────────────────
def print_comparison_summary(date_str: str = None):
    """Print a W/L comparison summary for a given date."""
    if date_str is None:
        date_str = datetime.date.today().strftime("%Y-%m-%d")

    sheet    = get_sheet()
    ws       = sheet.worksheet(FARO_TAB)
    all_rows = ws.get_all_values()
    if not all_rows:
        return

    rows = [r for r in all_rows[1:] if len(r) >= 9 and r[0] == date_str
            and r[7] not in ("PENDING", "") and r[8] not in ("PENDING", "", "NO BET")]

    if not rows:
        print(f"No settled results for {date_str}")
        return

    faro_wins  = sum(1 for r in rows if r[7] == "WIN")
    faro_loss  = sum(1 for r in rows if r[7] == "LOSS")
    our_wins   = sum(1 for r in rows if r[8] == "WIN")
    our_loss   = sum(1 for r in rows if r[8] == "LOSS")
    total      = len(rows)

    print(f"\n📊 FARO vs MODEL — {date_str}")
    print(f"{'='*40}")
    print(f"  {'Model':<20} {'W':>4} {'L':>4} {'Win%':>7}")
    print(f"  {'-'*35}")
    print(f"  {'Faro':<20} {faro_wins:>4} {faro_loss:>4} {faro_wins/total*100:>6.1f}%")
    print(f"  {'Our Model':<20} {our_wins:>4} {our_loss:>4} {our_wins/total*100:>6.1f}%")
    print(f"\n  Games tracked: {total}")


# ── MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    args = sys.argv[1:]

    if args and args[0] == "update":
        date = args[1] if len(args) > 1 else None
        update_pending_wl(date)
    elif args and args[0] == "summary":
        date = args[1] if len(args) > 1 else None
        print_comparison_summary(date)
    else:
        print("Usage:")
        print("  python faro_tracker.py update [YYYY-MM-DD]   # fill in PENDING W/L")
        print("  python faro_tracker.py summary [YYYY-MM-DD]  # print comparison")
        print()
        print("To log picks, call write_faro_day() from Python or from Claude session.")
