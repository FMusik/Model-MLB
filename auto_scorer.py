"""
auto_scorer.py
==============
Automatically fills in Actual Away, Actual Home, Actual Total,
and Hit/Miss in the 📊 Tracker tab after games finish.

Runs at the end of the 5PM workflow, or manually via:
  python auto_scorer.py              # today
  python auto_scorer.py 2026-05-03  # specific date

Tracker column layout (0-indexed):
  0  Date
  1  Game
  2  Bet Type
  3  Confidence Signal
  4  Conf%
  5  Our Prob%
  6  Fair Odds
  7  Market Odds
  8  Edge%
  9  Our Proj Away
  10 Our Proj Home
  11 Our Proj Total
  12 BP Proj Away
  13 BP Proj Home
  14 BP Proj Total
  15 BP YRFI%
  16 Total Diff
  17 Sharp Signal
  18 Units
  19 Actual Away
  20 Actual Home
  21 Actual Total
  22 Hit/Miss
  23 Notes
"""

import re
import sys
import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────
# CONFIG — mirrors mlb_model.py
# ─────────────────────────────────────────────
SHEET_NAME       = "MLB Daily Model"
CREDENTIALS_FILE = "credentials.json"
TRACKER_TAB      = "📊 Tracker"
MLB_API_BASE     = "https://statsapi.mlb.com/api/v1"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Tracker column indices (0-based)
COL_DATE       = 0
COL_GAME       = 1
COL_BET_TYPE   = 2
COL_SIGNAL     = 3   # Confidence Signal
COL_CONF_PCT   = 4
COL_OUR_PROB   = 5
COL_FAIR_ODDS  = 6
COL_MKT_ODDS  = 7
COL_EDGE       = 8
COL_PROJ_AWAY  = 9
COL_PROJ_HOME  = 10
COL_PROJ_TOT   = 11
COL_ACT_AWAY   = 19
COL_ACT_HOME   = 20
COL_ACT_TOT    = 21
COL_HIT_MISS   = 22
COL_NOTES      = 23


# ─────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────
def get_sheet():
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)


# ─────────────────────────────────────────────
# FETCH FINAL SCORES
# ─────────────────────────────────────────────
def fetch_final_scores(date_str: str) -> dict:
    """
    Returns dict keyed by "Away Team @ Home Team" with:
      away_score, home_score, total, f1_away, f1_home, yrfi
    Only includes games with Final status.
    """
    try:
        r = requests.get(
            f"{MLB_API_BASE}/schedule",
            params={"sportId": 1, "date": date_str, "hydrate": "linescore"},
            timeout=15,
        )
        data   = r.json()
        scores = {}

        for db in data.get("dates", []):
            for game in db.get("games", []):
                status = game.get("status", {})
                if status.get("abstractGameState") != "Final":
                    continue

                teams      = game.get("teams", {})
                away_team  = teams.get("away", {}).get("team", {}).get("name", "")
                home_team  = teams.get("home", {}).get("team", {}).get("name", "")
                away_score = teams.get("away", {}).get("score")
                home_score = teams.get("home", {}).get("score")

                if not away_team or not home_team or away_score is None:
                    continue

                # First inning from linescore
                innings = game.get("linescore", {}).get("innings", [])
                f1_away = f1_home = 0
                if innings:
                    f1 = innings[0]
                    f1_away = int(f1.get("away", {}).get("runs", 0) or 0)
                    f1_home = int(f1.get("home", {}).get("runs", 0) or 0)

                key = f"{away_team} @ {home_team}"
                scores[key] = {
                    "away_score": int(away_score),
                    "home_score": int(home_score),
                    "total":      int(away_score) + int(home_score),
                    "away_team":  away_team,
                    "home_team":  home_team,
                    "f1_away":    f1_away,
                    "f1_home":    f1_home,
                    "yrfi":       (f1_away + f1_home) > 0,
                }

        print(f"  ✅ Final scores fetched: {len(scores)} games")
        return scores

    except Exception as e:
        print(f"  ⚠️  Score fetch error: {e}")
        return {}


# ─────────────────────────────────────────────
# FUZZY GAME MATCH
# ─────────────────────────────────────────────
def fuzzy_match(tracker_game: str, scores: dict) -> str:
    """Match tracker 'Away @ Home' to MLB API game key."""
    clean = tracker_game.strip().lower()
    # Exact match first
    for key in scores:
        if key.lower() == clean:
            return key
    # Word overlap — need at least 2 shared words
    t_parts = set(clean.replace(" @ ", " ").split())
    for key in scores:
        s_parts = set(key.lower().replace(" @ ", " ").split())
        if len(t_parts & s_parts) >= 2:
            return key
    return None


# ─────────────────────────────────────────────
# DETERMINE HIT / MISS
# ─────────────────────────────────────────────
def determine_hit_miss(bet_type: str, signal: str, score_data: dict) -> str:
    """
    Returns WIN, LOSS, PUSH, or PENDING.

    Handles: ML, RL (with direction from label), OVER, UNDER, YRFI, F5 OVER,
             TT OVER, TT UNDER.

    RL direction is now stored in the bet label:
      "LAD RL -1.5"  → LAD must win by 2+ (laying -1.5)
      "LAD RL +1.5"  → LAD must not lose by 2+ (taking +1.5)
    """
    bt  = bet_type.strip().upper()
    sig = signal.strip().upper()
    sd  = score_data or {}

    # Signals that were skipped/faded — no result
    if "SKIP" in sig or "FADE" in sig or not sig or sig in ("—", "-"):
        return "NO BET"

    away_score = sd.get("away_score", 0)
    home_score = sd.get("home_score", 0)
    total      = sd.get("total", 0)
    away_team  = sd.get("away_team", "").upper()
    home_team  = sd.get("home_team", "").upper()
    margin     = away_score - home_score   # positive = away won by X

    try:

        # ── MONEYLINE ─────────────────────────────────────────
        if bt.endswith(" ML") and "TT" not in bt and "F5" not in bt:
            bt_team = bt.replace(" ML", "").strip()
            if away_score == home_score:
                return "PUSH"
            away_won = away_score > home_score
            # Match bet team to away or home
            is_away = (bt_team in away_team or away_team in bt_team)
            return "WIN" if (is_away and away_won) or (not is_away and not away_won) else "LOSS"

        # ── RUN LINE ──────────────────────────────────────────
        elif "RL" in bt:
            # Parse direction from label: "TEAM RL -1.5" or "TEAM RL +1.5"
            # -1.5 means this team is the FAVORITE, must win by 2+
            # +1.5 means this team is the UNDERDOG, must not lose by 2+
            m = re.search(r"RL\s*([+-]?\d+\.?\d*)", bt)
            if m:
                rl_line = float(m.group(1))  # e.g. -1.5 or +1.5
            else:
                rl_line = -1.5  # default: assume bet team is favorite

            # Determine if bet is on away or home team
            bt_team  = re.sub(r"RL.*", "", bt).strip()
            is_away  = (bt_team in away_team or away_team in bt_team)

            if is_away:
                # away_margin = margin (positive = away won)
                # rl_line = -1.5 → away laying 1.5 → need margin > 1.5
                # rl_line = +1.5 → away getting 1.5 → need margin > -1.5 (i.e. lose by 1 or win)
                effective_margin = margin + abs(rl_line) if rl_line > 0 else margin
                threshold = abs(rl_line) if rl_line < 0 else 0
                if rl_line < 0:   # away is favorite, laying 1.5
                    if margin > 1.5:  return "WIN"
                    elif margin < 1.5 and margin > -0.5: return "LOSS"
                    else: return "PUSH" if abs(margin - 1.5) < 0.01 else "LOSS"
                else:             # away is underdog, getting 1.5
                    if margin > -1.5: return "WIN"   # away wins or loses by 1
                    elif margin < -1.5: return "LOSS"
                    else: return "PUSH"
            else:
                # bet is on home team
                home_margin = home_score - away_score
                if rl_line < 0:   # home is favorite, laying 1.5
                    if home_margin > 1.5:  return "WIN"
                    elif home_margin < 1.5: return "LOSS"
                    else: return "PUSH"
                else:             # home is underdog, getting 1.5
                    if home_margin > -1.5: return "WIN"
                    elif home_margin < -1.5: return "LOSS"
                    else: return "PUSH"

        # ── GAME OVER ─────────────────────────────────────────
        elif bt.startswith("OVER") and "TT" not in bt and "F5" not in bt:
            m = re.search(r"(\d+\.?\d*)", bt)
            if not m:
                return "PENDING"
            line = float(m.group(1))
            if total > line:   return "WIN"
            elif total < line: return "LOSS"
            else:              return "PUSH"

        # ── GAME UNDER ────────────────────────────────────────
        elif bt.startswith("UNDER") and "TT" not in bt and "F5" not in bt:
            m = re.search(r"(\d+\.?\d*)", bt)
            if not m:
                return "PENDING"
            line = float(m.group(1))
            if total < line:   return "WIN"
            elif total > line: return "LOSS"
            else:              return "PUSH"

        # ── YRFI ──────────────────────────────────────────────
        elif "YRFI" in bt and "NRFI" not in bt:
            if "yrfi" not in sd:
                return "PENDING"
            return "WIN" if sd["yrfi"] else "LOSS"

        # ── NRFI ──────────────────────────────────────────────
        elif "NRFI" in bt:
            if "yrfi" not in sd:
                return "PENDING"
            return "WIN" if not sd["yrfi"] else "LOSS"

        # ── F5 OVER ───────────────────────────────────────────
        elif "F5 OVER" in bt or "F5 UNDER" in bt:
            # F5 scores not easily available — leave PENDING
            # (would need boxscore per-inning data)
            return "PENDING"

        # ── TEAM TOTAL OVER ───────────────────────────────────
        elif "TT OVER" in bt:
            m = re.search(r"(\d+\.?\d*)", bt)
            if not m:
                return "PENDING"
            line = float(m.group(1))
            bt_team = re.sub(r"TT.*", "", bt).strip()
            is_away = (bt_team in away_team or away_team in bt_team)
            score   = away_score if is_away else home_score
            if score > line:   return "WIN"
            elif score < line: return "LOSS"
            else:              return "PUSH"

        # ── TEAM TOTAL UNDER ──────────────────────────────────
        elif "TT UNDER" in bt:
            m = re.search(r"(\d+\.?\d*)", bt)
            if not m:
                return "PENDING"
            line = float(m.group(1))
            bt_team = re.sub(r"TT.*", "", bt).strip()
            is_away = (bt_team in away_team or away_team in bt_team)
            score   = away_score if is_away else home_score
            if score < line:   return "WIN"
            elif score > line: return "LOSS"
            else:              return "PUSH"

    except Exception as e:
        print(f"    ⚠️  Hit/miss error for '{bet_type}': {e}")

    return "PENDING"


# ─────────────────────────────────────────────
# COLUMN INDEX → LETTER
# ─────────────────────────────────────────────
def col_letter(idx: int) -> str:
    """0-based column index → spreadsheet column letter (A, B, ... Z, AA...)"""
    if idx < 26:
        return chr(ord("A") + idx)
    return chr(ord("A") + idx // 26 - 1) + chr(ord("A") + idx % 26)


# ─────────────────────────────────────────────
# MAIN AUTO-SCORER
# ─────────────────────────────────────────────
def auto_score(sheet, date_str: str = None) -> int:
    """
    Fills in Actual Away, Actual Home, Actual Total, Hit/Miss
    for ALL unfilled Tracker rows — any date, not just today.

    If date_str is given, only scores that date.
    Otherwise scores every unfilled row across all dates.
    Returns count of rows updated.
    """
    today = datetime.date.today().strftime("%Y-%m-%d")
    target_date = date_str  # None = score all unfilled

    if target_date:
        print(f"\n🏁 Auto-scoring Tracker for {target_date}...")
    else:
        print(f"\n🏁 Auto-scoring all unfilled Tracker rows...")

    try:
        ws       = sheet.worksheet(TRACKER_TAB)
        all_vals = ws.get_all_values()
    except Exception as e:
        print(f"  ⚠️  Tracker tab error: {e}")
        return 0

    if len(all_vals) < 2:
        print("  ⚠️  Tracker is empty")
        return 0

    # DEBUG: show what we're working with
    print(f"  🔍 DEBUG: total rows in tracker = {len(all_vals)}")
    print(f"  🔍 DEBUG: header row = {all_vals[0][:5]}")
    # Check last 5 data rows
    for dbg_row in all_vals[-6:-1]:
        if dbg_row and dbg_row[0]:
            hm_val = dbg_row[COL_HIT_MISS] if len(dbg_row) > COL_HIT_MISS else "MISSING_COL"
            print(f"  🔍 DEBUG row: date={dbg_row[0]} | game={dbg_row[1][:30] if len(dbg_row)>1 else '?'} | len={len(dbg_row)} | hit_miss='{hm_val}'")

    # Collect all unique dates that have unfilled rows
    dates_needed = set()
    for row in all_vals[1:]:
        if not row or len(row) <= COL_DATE:
            continue
        row_date = row[COL_DATE].strip() if COL_DATE < len(row) else ""
        if not row_date:
            continue
        if target_date and row_date != target_date:
            continue
        # Check if unfilled — short rows (< COL_HIT_MISS cols) are always unfilled
        if len(row) <= COL_HIT_MISS:
            existing_hm = ""
        else:
            existing_hm = row[COL_HIT_MISS].strip()
        if not existing_hm or existing_hm in ("", "PENDING", "—", "-"):
            # Only fetch past dates (today or earlier)
            if row_date <= today:
                dates_needed.add(row_date)

    if not dates_needed:
        print("  ✅ Nothing to fill — all rows already scored")
        return 0

    print(f"  📅 Dates with unfilled rows: {', '.join(sorted(dates_needed))}")

    # Fetch scores for each needed date
    scores_by_date = {}
    for d in sorted(dates_needed):
        scores = fetch_final_scores(d)
        if scores:
            scores_by_date[d] = scores
            print(f"  ✅ {d}: {len(scores)} final scores")
        else:
            print(f"  ⏳ {d}: no final scores yet")

    if not scores_by_date:
        print("  ⚠️  No final scores available for any date")
        return 0

    updates = []
    updated = 0
    skipped = 0
    tab = TRACKER_TAB

    for i, row in enumerate(all_vals[1:], start=2):
        if not row or len(row) <= COL_DATE:
            continue

        row_date = row[COL_DATE].strip() if COL_DATE < len(row) else ""
        if not row_date:
            continue
        if target_date and row_date != target_date:
            continue

        game_str = row[COL_GAME].strip() if COL_GAME < len(row) else ""
        if not game_str:
            continue

        # Skip if already scored — short rows are always unfilled
        if len(row) <= COL_HIT_MISS:
            existing_hm = ""
        else:
            existing_hm = row[COL_HIT_MISS].strip()
        if existing_hm and existing_hm not in ("", "PENDING", "—", "-"):
            skipped += 1
            continue

        # Get scores for this row's date
        scores = scores_by_date.get(row_date, {})
        if not scores:
            continue

        matched_key = fuzzy_match(game_str, scores)
        if not matched_key:
            print(f"  ⏳ No final score: {game_str} ({row_date})")
            continue

        sd         = scores[matched_key]
        away_score = sd["away_score"]
        home_score = sd["home_score"]
        total      = sd["total"]
        bet_type   = row[COL_BET_TYPE].strip() if COL_BET_TYPE < len(row) else ""
        signal     = row[COL_SIGNAL].strip()   if COL_SIGNAL   < len(row) else ""

        hit_miss = determine_hit_miss(bet_type, signal, sd)

        for col_idx, value in [
            (COL_ACT_AWAY, away_score),
            (COL_ACT_HOME, home_score),
            (COL_ACT_TOT,  total),
            (COL_HIT_MISS, hit_miss),
        ]:
            updates.append({
                "range":  f"\'{tab}\'!{col_letter(col_idx)}{i}",
                "values": [[value]],
            })

        icon = "✅" if hit_miss == "WIN" else ("❌" if hit_miss == "LOSS" else ("➡️" if hit_miss == "PUSH" else "⏳"))
        print(f"  {icon} [{row_date}] {game_str} | {bet_type} | {away_score}-{home_score} | {hit_miss}")
        updated += 1

    # Batch write to Sheets
    if updates:
        try:
            ws.spreadsheet.values_batch_update({
                "valueInputOption": "USER_ENTERED",
                "data": updates,
            })
            print(f"\n  ✅ Auto-scored {updated} signals ({skipped} already done)")
        except Exception as e:
            print(f"\n  ⚠️  Batch write error: {e}")
            print("  🔄 Falling back to individual writes...")
            for update in updates:
                try:
                    ws.spreadsheet.values_update(
                        update["range"],
                        params={"valueInputOption": "USER_ENTERED"},
                        body={"values": update["values"]},
                    )
                except Exception as e2:
                    print(f"    ⚠️  Cell write failed: {e2}")
    else:
        print(f"  ✅ Nothing to update ({skipped} already scored, no new finals)")

    return updated


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    sheet    = get_sheet()
    count    = auto_score(sheet, date_str)
    print(f"\n🏁 Done — {count} rows updated")
