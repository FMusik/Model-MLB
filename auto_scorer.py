"""
auto_scorer.py - debug version
"""
import re
import sys
import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

SHEET_NAME       = "MLB Daily Model"
CREDENTIALS_FILE = "credentials.json"
TRACKER_TAB      = "📊 Tracker"
MLB_API_BASE     = "https://statsapi.mlb.com/api/v1"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_sheet():
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)

def fetch_final_scores(date_str: str) -> dict:
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
        print(f"  ✅ {date_str}: {len(scores)} final scores")
        for k,v in scores.items():
            print(f"     {k}: {v['away_score']}-{v['home_score']}")
        return scores
    except Exception as e:
        print(f"  ⚠️  Score fetch error for {date_str}: {e}")
        return {}

def fuzzy_match(tracker_game: str, scores: dict) -> str:
    clean = tracker_game.strip().lower()
    for key in scores:
        if key.lower() == clean:
            return key
    t_parts = set(clean.replace(" @ ", " ").split())
    for key in scores:
        s_parts = set(key.lower().replace(" @ ", " ").split())
        if len(t_parts & s_parts) >= 2:
            return key
    return None

def determine_hit_miss(bet_type: str, signal: str, score_data: dict) -> str:
    import re
    bt  = bet_type.strip().upper()
    sig = signal.strip().upper()
    sd  = score_data or {}

    if "SKIP" in sig or "FADE" in sig or not sig or sig in ("—", "-"):
        return "NO BET"

    away_score = sd.get("away_score", 0)
    home_score = sd.get("home_score", 0)
    total      = sd.get("total", 0)
    away_team  = sd.get("away_team", "").upper()
    home_team  = sd.get("home_team", "").upper()
    margin     = away_score - home_score

    try:
        if bt.endswith(" ML") and "TT" not in bt and "F5" not in bt:
            bt_team = bt.replace(" ML", "").strip()
            if away_score == home_score:
                return "PUSH"
            away_won = away_score > home_score
            is_away = (bt_team in away_team or away_team in bt_team)
            return "WIN" if (is_away and away_won) or (not is_away and not away_won) else "LOSS"

        elif "RL" in bt:
            m = re.search(r"RL\s*([+-]?\d+\.?\d*)", bt)
            rl_line = float(m.group(1)) if m else -1.5
            bt_team = re.sub(r"RL.*", "", bt).strip()
            is_away = (bt_team in away_team or away_team in bt_team)
            if is_away:
                if rl_line < 0:
                    return "WIN" if margin > 1.5 else ("PUSH" if abs(margin - 1.5) < 0.01 else "LOSS")
                else:
                    return "WIN" if margin > -1.5 else ("PUSH" if abs(margin + 1.5) < 0.01 else "LOSS")
            else:
                home_margin = home_score - away_score
                if rl_line < 0:
                    return "WIN" if home_margin > 1.5 else ("PUSH" if abs(home_margin - 1.5) < 0.01 else "LOSS")
                else:
                    return "WIN" if home_margin > -1.5 else ("PUSH" if abs(home_margin + 1.5) < 0.01 else "LOSS")

        elif bt.startswith("OVER") and "TT" not in bt and "F5" not in bt:
            m = re.search(r"(\d+\.?\d*)", bt)
            if not m: return "PENDING"
            line = float(m.group(1))
            return "WIN" if total > line else ("LOSS" if total < line else "PUSH")

        elif bt.startswith("UNDER") and "TT" not in bt and "F5" not in bt:
            m = re.search(r"(\d+\.?\d*)", bt)
            if not m: return "PENDING"
            line = float(m.group(1))
            return "WIN" if total < line else ("LOSS" if total > line else "PUSH")

        elif "YRFI" in bt and "NRFI" not in bt:
            return "WIN" if sd.get("yrfi") else "LOSS"

        elif "NRFI" in bt:
            return "WIN" if not sd.get("yrfi") else "LOSS"

        elif "F5" in bt:
            return "PENDING"

    except Exception as e:
        print(f"    ⚠️  Hit/miss error for '{bet_type}': {e}")

    return "PENDING"

def col_letter(idx: int) -> str:
    if idx < 26:
        return chr(ord("A") + idx)
    return chr(ord("A") + idx // 26 - 1) + chr(ord("A") + idx % 26)

def auto_score(sheet, date_str: str = None) -> int:
    today = datetime.date.today().strftime("%Y-%m-%d")
    print(f"\n🏁 Auto-scoring all unfilled Tracker rows...")

    try:
        ws       = sheet.worksheet(TRACKER_TAB)
        all_vals = ws.get_all_values()
    except Exception as e:
        print(f"  ⚠️  Tracker tab error: {e}")
        return 0

    print(f"  🔍 Total rows from gspread: {len(all_vals)}")
    if len(all_vals) < 2:
        print("  ⚠️  Tracker is empty")
        return 0

    # Print header to confirm column layout
    headers = all_vals[0] if all_vals else []
    print(f"  🔍 Headers ({len(headers)} cols): {headers}")

    # Find column indices dynamically from header row
    def find_col(name):
        for i, h in enumerate(headers):
            if h.strip() == name:
                return i
        return None

    COL_DATE     = find_col("Date") or 0
    COL_GAME     = find_col("Game") or 1
    COL_BET_TYPE = find_col("Bet Type") or 2
    COL_SIGNAL   = find_col("Confidence Signal") or 3
    COL_ACT_AWAY = find_col("Actual Away")
    COL_ACT_HOME = find_col("Actual Home")
    COL_ACT_TOT  = find_col("Actual Total")
    COL_HIT_MISS = find_col("Hit/Miss")

    print(f"  🔍 Column indices: Date={COL_DATE} Game={COL_GAME} BetType={COL_BET_TYPE}")
    print(f"  🔍 ActualAway={COL_ACT_AWAY} ActualHome={COL_ACT_HOME} ActualTotal={COL_ACT_TOT} HitMiss={COL_HIT_MISS}")

    if None in (COL_ACT_AWAY, COL_ACT_HOME, COL_ACT_TOT, COL_HIT_MISS):
        print("  ❌ CRITICAL: Could not find required columns in header row!")
        print(f"     Available headers: {headers}")
        return 0

    # Scan for unfilled rows
    dates_needed = set()
    unfilled_count = 0
    for row in all_vals[1:]:
        if not row or not row[0].strip():
            continue
        row_date = row[COL_DATE].strip()
        if date_str and row_date != date_str:
            continue
        if row_date > today:
            continue
        existing_hm = row[COL_HIT_MISS].strip() if len(row) > COL_HIT_MISS else ""
        if not existing_hm or existing_hm in ("PENDING", "—", "-"):
            dates_needed.add(row_date)
            unfilled_count += 1

    print(f"  🔍 Unfilled rows: {unfilled_count}")
    print(f"  🔍 Dates needed: {sorted(dates_needed)}")

    if not dates_needed:
        print("  ✅ All rows already scored")
        return 0

    # Fetch scores for each date
    scores_by_date = {}
    for d in sorted(dates_needed):
        scores = fetch_final_scores(d)
        if scores:
            scores_by_date[d] = scores

    if not scores_by_date:
        print("  ⚠️  No final scores found for any needed date")
        return 0

    updates = []
    updated = 0
    tab = TRACKER_TAB

    for i, row in enumerate(all_vals[1:], start=2):
        if not row or not row[0].strip():
            continue
        row_date = row[COL_DATE].strip()
        if date_str and row_date != date_str:
            continue
        if row_date > today:
            continue

        game_str    = row[COL_GAME].strip() if len(row) > COL_GAME else ""
        existing_hm = row[COL_HIT_MISS].strip() if len(row) > COL_HIT_MISS else ""

        if existing_hm and existing_hm not in ("PENDING", "—", "-"):
            continue

        scores = scores_by_date.get(row_date, {})
        if not scores:
            continue

        matched_key = fuzzy_match(game_str, scores)
        if not matched_key:
            print(f"  ⏳ No match: '{game_str}' ({row_date})")
            continue

        sd         = scores[matched_key]
        away_score = sd["away_score"]
        home_score = sd["home_score"]
        total      = sd["total"]
        bet_type   = row[COL_BET_TYPE].strip() if len(row) > COL_BET_TYPE else ""
        signal     = row[COL_SIGNAL].strip()   if len(row) > COL_SIGNAL   else ""

        hit_miss = determine_hit_miss(bet_type, signal, sd)

        for col_idx, value in [
            (COL_ACT_AWAY, away_score),
            (COL_ACT_HOME, home_score),
            (COL_ACT_TOT,  total),
            (COL_HIT_MISS, hit_miss),
        ]:
            cell_range = f"'{tab}'!{col_letter(col_idx)}{i}"
            updates.append({"range": cell_range, "values": [[value]]})

        icon = "✅" if hit_miss == "WIN" else ("❌" if hit_miss == "LOSS" else "➡️")
        print(f"  {icon} row {i} [{row_date}] {game_str} | {bet_type} | {away_score}-{home_score} | {hit_miss}")
        updated += 1

    print(f"\n  🔍 Total updates queued: {len(updates)}")

    if updates:
        try:
            ws.spreadsheet.values_batch_update({
                "valueInputOption": "USER_ENTERED",
                "data": updates,
            })
            print(f"  ✅ Batch update successful — {updated} rows scored")
        except Exception as e:
            print(f"  ❌ Batch update FAILED: {e}")
            print("  🔄 Trying individual cell writes...")
            for upd in updates:
                try:
                    ws.spreadsheet.values_update(
                        upd["range"],
                        params={"valueInputOption": "USER_ENTERED"},
                        body={"values": upd["values"]},
                    )
                except Exception as e2:
                    print(f"    ❌ Cell write failed {upd['range']}: {e2}")
    else:
        print("  ✅ Nothing to update")

    return updated

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    sheet    = get_sheet()
    count    = auto_score(sheet, date_arg)
    print(f"\n🏁 Done — {count} rows updated")
