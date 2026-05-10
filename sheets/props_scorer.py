"""
sheets/props_scorer.py
──────────────────────
Auto-score the 📊 Tracker tab in the Props Sheet.

After games finish, this script:
  1. Reads all rows in 📊 Tracker where Result is blank and Date == today (ET)
  2. Looks up each player's MLBAM ID (BPP PlayerId column → API people search)
  3. Fetches their hit total from the MLB Stats API gameLog for today
  4. Compares hits against Line/Side and writes WIN / LOSS / PUSH back

ENV:
  PROPS_SHEET_ID      Google Sheet ID
  GSHEET_CREDENTIALS  Service-account JSON content OR path
                      (falls back to ../credentials.json or ./credentials.json)
"""

import os
import sys
import json
import datetime
import unicodedata

try:
    from zoneinfo import ZoneInfo
    ET_ZONE = ZoneInfo("America/New_York")
except Exception:
    ET_ZONE = None

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


# ── CONFIG ─────────────────────────────────────────────────────
HERE         = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE) if os.path.basename(HERE) == "sheets" else HERE
BATTERS_FILE = os.path.join(PROJECT_ROOT, "ballparkpal_batters.xlsx")

PROPS_SHEET_ID   = os.environ.get("PROPS_SHEET_ID", "")
GSHEET_CRED_ENV  = os.environ.get("GSHEET_CREDENTIALS", "")

MLB_STATS_BASE     = "https://statsapi.mlb.com/api/v1"
MANUAL_TRACKER_TAB = "📊 Tracker"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ── GOOGLE SHEETS AUTH ─────────────────────────────────────────
def _load_credentials():
    if GSHEET_CRED_ENV:
        try:
            info = json.loads(GSHEET_CRED_ENV)
            if isinstance(info, dict):
                return Credentials.from_service_account_info(info, scopes=SCOPES)
        except json.JSONDecodeError:
            pass
        if os.path.exists(GSHEET_CRED_ENV):
            return Credentials.from_service_account_file(GSHEET_CRED_ENV, scopes=SCOPES)
    for candidate in (
        os.path.join(PROJECT_ROOT, "credentials.json"),
        os.path.join(os.getcwd(), "credentials.json"),
    ):
        if os.path.exists(candidate):
            return Credentials.from_service_account_file(candidate, scopes=SCOPES)
    sys.exit("❌ No credentials available — set GSHEET_CREDENTIALS or write credentials.json")


def get_sheet():
    if not PROPS_SHEET_ID:
        sys.exit("❌ PROPS_SHEET_ID not set")
    client = gspread.authorize(_load_credentials())
    return client.open_by_key(PROPS_SHEET_ID)


# ── HELPERS ────────────────────────────────────────────────────
def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def normalize_name(s: str) -> str:
    s = _strip_accents(str(s)).lower()
    s = "".join(c for c in s if c.isalnum() or c.isspace())
    return " ".join(s.split())


def today_et() -> str:
    """ET date as YYYY-MM-DD — what 'today' should mean for player game logs."""
    if ET_ZONE:
        return datetime.datetime.now(ET_ZONE).date().isoformat()
    return datetime.date.today().isoformat()


# ── BPP PLAYER-ID MAP ──────────────────────────────────────────
def load_bpp_id_map() -> dict:
    """name → MLBAM id from the BPP batters export (best-effort)."""
    if not os.path.exists(BATTERS_FILE):
        print(f"  ⚠️  {BATTERS_FILE} not found — will fall back to API name search")
        return {}
    try:
        df = pd.read_excel(BATTERS_FILE, engine="openpyxl")
    except Exception as e:
        print(f"  ⚠️  Could not read BPP batters: {e}")
        return {}
    cols = {c.lower(): c for c in df.columns}
    name_col = next(
        (cols[k] for k in ("fullname", "player", "name", "playername", "batter") if k in cols),
        None,
    )
    pid_col = cols.get("playerid") or cols.get("mlbid") or cols.get("id")
    if not (name_col and pid_col):
        print(f"  ⚠️  BPP batters missing name/PlayerId columns; have: {list(df.columns)}")
        return {}

    out = {}
    for _, row in df.iterrows():
        name = str(row[name_col]).strip()
        if not name or name.lower() == "nan":
            continue
        pid_raw = row[pid_col]
        if pid_raw is None or (isinstance(pid_raw, float) and pd.isna(pid_raw)):
            continue
        try:
            pid = str(int(float(pid_raw)))
        except (TypeError, ValueError):
            pid = str(pid_raw).strip()
        if pid:
            out[normalize_name(name)] = pid
    print(f"  ✅ BPP MLBAM IDs loaded: {len(out)} players")
    return out


def search_player_id(name: str, session: requests.Session) -> str:
    """Fallback when the player isn't in the BPP id map — MLB Stats API name search."""
    try:
        r = session.get(
            f"{MLB_STATS_BASE}/people/search",
            params={"names": name, "active": "true"},
            timeout=8,
        )
        if r.status_code != 200:
            return ""
        people = (r.json() or {}).get("people", []) or []
        if people:
            return str(people[0].get("id", "") or "")
    except Exception:
        pass
    return ""


# ── HIT LOOKUP ─────────────────────────────────────────────────
def fetch_player_hits_for_date(player_id: str, target_date: str,
                                session: requests.Session):
    """Return total hits for `target_date` (YYYY-MM-DD), or None if no game logged yet."""
    if not player_id:
        return None
    season = int(target_date[:4])
    try:
        r = session.get(
            f"{MLB_STATS_BASE}/people/{player_id}/stats",
            params={"stats": "gameLog", "group": "hitting", "season": season},
            timeout=8,
        )
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception:
        return None

    for block in data.get("stats", []):
        if "gamelog" not in (block.get("type", {}).get("displayName") or "").lower():
            continue
        total = 0
        found = False
        for s in block.get("splits", []):
            game_date = s.get("date") or (s.get("gameDate", "") or "")[:10]
            if game_date != target_date:
                continue
            try:
                total += int(s.get("stat", {}).get("hits", 0))
                found  = True
            except (TypeError, ValueError):
                pass
        if found:
            return total
    return None


# ── SCORING ────────────────────────────────────────────────────
def score_result(line: float, side: str, hits: int) -> str:
    """WIN / LOSS / PUSH from line + side + actual hits."""
    s = (side or "").strip().lower()
    if s == "over":
        if hits > line:  return "WIN"
        if hits < line:  return "LOSS"
        return "PUSH"
    if s == "under":
        if hits < line:  return "WIN"
        if hits > line:  return "LOSS"
        return "PUSH"
    return ""


# ── MAIN ───────────────────────────────────────────────────────
def main():
    print("🔍 Auto-scoring 📊 Tracker tab...")
    today = today_et()
    print(f"  Today (ET): {today}")

    sheet = get_sheet()
    try:
        ws = sheet.worksheet(MANUAL_TRACKER_TAB)
    except gspread.WorksheetNotFound:
        print(f"  ⚠️  {MANUAL_TRACKER_TAB} tab not found — nothing to score")
        return

    all_values = ws.get_all_values()
    if not all_values:
        print(f"  ⚠️  {MANUAL_TRACKER_TAB} is empty")
        return

    header = all_values[0]
    try:
        date_idx   = header.index("Date")
        player_idx = header.index("Player")
        line_idx   = header.index("Line")
        side_idx   = header.index("Side")
        result_idx = header.index("Result")
    except ValueError as e:
        sys.exit(f"❌ Missing required column in {MANUAL_TRACKER_TAB}: {e}")

    # Collect candidate rows: today + blank Result
    candidates = []  # (sheet_row, player, line, side)
    for i, row in enumerate(all_values[1:], start=2):  # rows are 1-indexed; header is row 1
        max_idx = max(date_idx, player_idx, line_idx, side_idx, result_idx)
        if max_idx >= len(row):
            continue
        if row[date_idx] != today:
            continue
        if str(row[result_idx]).strip():
            continue
        try:
            line = float(row[line_idx])
        except (TypeError, ValueError):
            continue
        candidates.append((i, row[player_idx], line, row[side_idx]))

    if not candidates:
        print(f"  ✅ No rows to score (Date = {today}, Result blank)")
        return

    print(f"  📋 {len(candidates)} candidate row(s) to score")

    bpp_id_map = load_bpp_id_map()
    session    = requests.Session()
    id_cache   = {}  # normalized name → mlbam id (avoids duplicate lookups)

    updates = []
    scored, no_data, no_id = 0, 0, 0
    result_col = _col_letter(result_idx + 1)

    for sheet_row, player, line, side in candidates:
        key = normalize_name(player)
        if key not in id_cache:
            pid = bpp_id_map.get(key, "") or search_player_id(player, session)
            id_cache[key] = pid
        pid = id_cache[key]
        if not pid:
            no_id += 1
            print(f"  ⚠️  No MLBAM ID for {player!r} — skipped")
            continue

        hits = fetch_player_hits_for_date(pid, today, session)
        if hits is None:
            no_data += 1
            continue

        result = score_result(line, side, hits)
        if not result:
            continue

        updates.append({
            "range":  f"{result_col}{sheet_row}",
            "values": [[result]],
        })
        scored += 1
        print(f"  ✅ {player}: {side} {line} | hits={hits} → {result}")

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        print(
            f"\n🎯 Auto-scored {scored} row(s) | "
            f"{no_data} player(s) with no game data yet | {no_id} unmapped"
        )
    else:
        print(
            f"\n🎯 0 scored — game data not yet available "
            f"({no_data} not-played, {no_id} unmapped)"
        )


if __name__ == "__main__":
    main()
