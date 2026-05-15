"""
sheets/props_scorer.py
──────────────────────
Auto-score the 📊 Tracker tab in the Props Sheet.

After games finish, this script:
  1. Creates the 📊 Tracker tab with correct header if it doesn't exist
  2. Collapses any duplicate rows (same Date+Player+Line+Side)
  3. Reads rows that still need scoring (blank Result, "PENDING", or a
     "DNP" that contradicts Confirmed=YES so bad rows can self-heal)
  4. Looks up each player's MLBAM ID (BPP PlayerId column → API people search)
  5. Fetches their hits/AB from the MLB Stats API gameLog for that date
  6. Writes WIN / LOSS / PUSH from hits vs Line/Side. When the player has no
     AB it writes DNP — except when Confirmed=YES and no game log was found,
     which is marked PENDING (retried next run) to avoid a YES/DNP
     contradiction caused by a stale or mismatched stats lookup.

ENV:
  PROPS_SHEET_ID      Google Sheet ID
  GSHEET_CREDENTIALS  Service-account JSON content OR path
                      (falls back to ../credentials.json or ./credentials.json)
"""

import argparse
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

# Header used when creating the tab from scratch.
# Must match what Best Bets rows contain so W/L scoring aligns correctly.
MANUAL_TRACKER_HEADERS = [
    "Date", "Game Time", "Player", "Team", "Game", "Line", "Side",
    "BPP Hit%", "Model Prob%", "Edge%",
    "Kelly Units", "MC Win%",
    "Composite Score", "Rating",
    "Confirmed",
    "Result", "Notes",
]

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


def _normalize_line(v) -> str:
    try:
        return f"{float(v):.1f}"
    except (TypeError, ValueError):
        return str(v).strip()


def _dedup_key(date, player, line, side) -> tuple:
    return (
        str(date).strip(),
        normalize_name(player),
        _normalize_line(line),
        str(side).strip().lower(),
    )


def _result_priority(val: str) -> int:
    v = (val or "").strip().upper()
    if v in ("WIN", "LOSS", "PUSH"): return 3
    if v == "DNP":                   return 2
    if v == "PENDING":               return 1
    return 0


def today_et() -> str:
    if ET_ZONE:
        return datetime.datetime.now(ET_ZONE).date().isoformat()
    return datetime.date.today().isoformat()


# ── TRACKER TAB — CREATE IF MISSING ───────────────────────────
def _get_or_create_tracker(sheet):
    """Open 📊 Tracker, creating it with the correct header if missing.
    Never clears or overwrites existing data — safe to call on every run.
    """
    try:
        ws         = sheet.worksheet(MANUAL_TRACKER_TAB)
        all_values = ws.get_all_values()
        if not all_values:
            # Tab exists but is completely empty — write header
            end_col = _col_letter(len(MANUAL_TRACKER_HEADERS))
            ws.update(
                range_name=f"A1:{end_col}1",
                values=[MANUAL_TRACKER_HEADERS],
                value_input_option="USER_ENTERED",
            )
            print(f"  ➕ {MANUAL_TRACKER_TAB}: tab was empty — wrote header")
        return ws
    except gspread.WorksheetNotFound:
        print(f"  ➕ {MANUAL_TRACKER_TAB}: tab not found — creating with header")
        ws = sheet.add_worksheet(
            title=MANUAL_TRACKER_TAB,
            rows=10000,
            cols=max(30, len(MANUAL_TRACKER_HEADERS)),
        )
        end_col = _col_letter(len(MANUAL_TRACKER_HEADERS))
        ws.update(
            range_name=f"A1:{end_col}1",
            values=[MANUAL_TRACKER_HEADERS],
            value_input_option="USER_ENTERED",
        )
        print(f"  ✅ {MANUAL_TRACKER_TAB}: created with header")
        return ws


# ── BPP PLAYER-ID MAP ──────────────────────────────────────────
def load_bpp_player_info() -> dict:
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
    pid_col  = cols.get("playerid") or cols.get("mlbid") or cols.get("id")
    team_col = cols.get("team") or cols.get("teamabbr")
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
            pid = ""
        else:
            try:    pid = str(int(float(pid_raw)))
            except (TypeError, ValueError): pid = str(pid_raw).strip()
        team = str(row[team_col]).strip().upper() if team_col else ""
        out[normalize_name(name)] = {"id": pid, "team": team}
    print(f"  ✅ BPP player info loaded: {len(out)} players")
    return out


def fetch_team_game_statuses(date_str: str, session: requests.Session, cache: dict) -> dict:
    if date_str in cache:
        return cache[date_str]
    try:
        r = session.get(
            f"{MLB_STATS_BASE}/schedule",
            params={"sportId": 1, "date": date_str},
            timeout=8,
        )
        if r.status_code != 200:
            cache[date_str] = {}; return {}
        data = r.json()
    except Exception:
        cache[date_str] = {}; return {}
    statuses: dict = {}
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            status_block = game.get("status") or {}
            status   = status_block.get("abstractGameState", "") or ""
            detailed = (status_block.get("detailedState", "") or "").lower()
            if any(k in detailed for k in ("postpon", "cancel", "suspend")):
                status = "Postponed"
            teams_block = game.get("teams") or {}
            for side in ("home", "away"):
                abbr = (((teams_block.get(side) or {}).get("team") or {}).get("abbreviation","") or "").upper()
                if abbr: statuses[abbr] = status
    cache[date_str] = statuses
    return statuses


def search_player_id(name: str, session: requests.Session) -> str:
    try:
        r = session.get(
            f"{MLB_STATS_BASE}/people/search",
            params={"names": name, "active": "true"},
            timeout=8,
        )
        if r.status_code != 200: return ""
        people = (r.json() or {}).get("people", []) or []
        if people: return str(people[0].get("id", "") or "")
    except Exception:
        pass
    return ""


# ── HIT LOOKUP ─────────────────────────────────────────────────
def fetch_player_gamelog(player_id: str, season: int,
                          session: requests.Session, cache: dict) -> dict:
    key = (player_id, season)
    if key in cache: return cache[key]
    if not player_id:
        cache[key] = {}; return {}
    try:
        r = session.get(
            f"{MLB_STATS_BASE}/people/{player_id}/stats",
            params={"stats": "gameLog", "group": "hitting", "season": season},
            timeout=8,
        )
        if r.status_code != 200:
            cache[key] = {}; return {}
        data = r.json()
    except Exception:
        cache[key] = {}; return {}
    by_date: dict = {}
    for block in data.get("stats", []):
        if "gamelog" not in (block.get("type", {}).get("displayName") or "").lower():
            continue
        for s in block.get("splits", []):
            d = s.get("date") or (s.get("gameDate", "") or "")[:10]
            if not d: continue
            stat = s.get("stat") or {}
            try:
                hits = int(stat.get("hits", 0))
                ab   = int(stat.get("atBats", 0))
            except (TypeError, ValueError): continue
            prev_hits, prev_ab = by_date.get(d, (0, 0))
            by_date[d] = (prev_hits + hits, prev_ab + ab)
    cache[key] = by_date
    return by_date


# ── SCORING ────────────────────────────────────────────────────
def score_result(line: float, side: str, hits: int) -> str:
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


# ── CLI ────────────────────────────────────────────────────────
def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Auto-score 📊 Tracker rows by reading hits from the MLB Stats API.",
    )
    parser.add_argument("date", nargs="?", default=None,
                        help="Specific date YYYY-MM-DD to score (default: today ET)")
    parser.add_argument("--backfill", type=int, default=0, metavar="N",
                        help="Score the last N days instead of a single date")
    return parser.parse_args(argv)


def resolve_target_dates(args) -> list:
    if args.backfill > 0:
        today = datetime.datetime.now(ET_ZONE).date() if ET_ZONE else datetime.date.today()
        return [(today - datetime.timedelta(days=i)).isoformat() for i in range(args.backfill)]
    if args.date:
        try: datetime.date.fromisoformat(args.date)
        except ValueError: sys.exit(f"❌ Invalid date format: {args.date} (expected YYYY-MM-DD)")
        return [args.date]
    return [today_et()]


# ── DEDUP ──────────────────────────────────────────────────────
def _dedupe_tracker_rows(sheet, ws, all_values, header):
    try:
        date_idx   = header.index("Date");   player_idx = header.index("Player")
        line_idx   = header.index("Line");   side_idx   = header.index("Side")
    except ValueError:
        return all_values
    result_idx = header.index("Result") if "Result" in header else None
    notes_idx  = header.index("Notes")  if "Notes"  in header else None
    edge_idx   = header.index("Edge%")  if "Edge%"  in header else None
    key_max_idx = max(date_idx, player_idx, line_idx, side_idx)
    groups: dict = {}
    for i, row in enumerate(all_values[1:], start=2):
        if key_max_idx >= len(row): continue
        key = _dedup_key(row[date_idx], row[player_idx], row[line_idx], row[side_idx])
        groups.setdefault(key, []).append((i, list(row)))

    def _rank(member):
        i, row = member
        res  = row[result_idx] if (result_idx is not None and result_idx < len(row)) else ""
        edge = -float("inf")
        if edge_idx is not None and edge_idx < len(row):
            try: edge = float(row[edge_idx])
            except (TypeError, ValueError): pass
        return (_result_priority(res), edge, i)

    cell_updates = []; rows_to_delete = []
    for members in groups.values():
        if len(members) < 2: continue
        winner = max(members, key=_rank)
        winner_idx, winner_row = winner
        losers = [m for m in members if m[0] != winner_idx]
        for col_idx in (result_idx, notes_idx):
            if col_idx is None: continue
            winner_val = winner_row[col_idx] if col_idx < len(winner_row) else ""
            if str(winner_val).strip(): continue
            for _, lr in losers:
                if col_idx < len(lr) and str(lr[col_idx]).strip():
                    cell_updates.append({"range": f"{_col_letter(col_idx+1)}{winner_idx}", "values": [[lr[col_idx]]]}); break
        rows_to_delete.extend(m[0] for m in losers)

    if not rows_to_delete: return all_values
    if cell_updates:
        ws.batch_update(cell_updates, value_input_option="USER_ENTERED")
        print(f"  🔄 Promoted {len(cell_updates)} Result/Notes cell(s) onto dedup winners")
    rows_to_delete.sort(reverse=True)
    try: sheet_id = ws.id
    except AttributeError: sheet_id = ws._properties.get("sheetId")
    sheet.batch_update({"requests": [{"deleteDimension":{"range":{"sheetId":sheet_id,"dimension":"ROWS","startIndex":ri-1,"endIndex":ri}}} for ri in rows_to_delete]})
    print(f"  🧹 Deleted {len(rows_to_delete)} duplicate row(s) from {MANUAL_TRACKER_TAB}")
    return ws.get_all_values()


# ── MAIN ───────────────────────────────────────────────────────
def main(argv=None):
    args         = parse_args(argv)
    target_dates = resolve_target_dates(args)
    target_set   = set(target_dates)

    print("🔍 Auto-scoring 📊 Tracker tab...")
    if args.backfill > 0:
        sd = sorted(target_set)
        print(f"  📅 Backfill mode — last {args.backfill} day(s): {sd[0]} → {sd[-1]}")
    elif args.date:
        print(f"  📅 Target date: {args.date}")
    else:
        print(f"  📅 Today (ET): {target_dates[0]}")

    sheet = get_sheet()

    # Create tab with header if it was accidentally deleted or never existed
    ws = _get_or_create_tracker(sheet)

    all_values = ws.get_all_values()
    if not all_values or all_values == [MANUAL_TRACKER_HEADERS]:
        print(f"  ⚠️  {MANUAL_TRACKER_TAB} has no data rows to score")
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
    confirmed_idx = header.index("Confirmed") if "Confirmed" in header else None

    all_values = _dedupe_tracker_rows(sheet, ws, all_values, header)

    candidates = []
    for i, row in enumerate(all_values[1:], start=2):
        max_idx = max(date_idx, player_idx, line_idx, side_idx, result_idx)
        if max_idx >= len(row): continue
        if row[date_idx] not in target_set: continue
        confirmed = ""
        if confirmed_idx is not None and confirmed_idx < len(row):
            confirmed = str(row[confirmed_idx]).strip().upper()
        result_val = str(row[result_idx]).strip().upper()
        reprocessable = (
            not result_val
            or result_val == "PENDING"
            or (result_val == "DNP" and confirmed == "YES")
        )
        if not reprocessable: continue
        try: line = float(row[line_idx])
        except (TypeError, ValueError): continue
        candidates.append((i, row[date_idx], row[player_idx], line, row[side_idx], confirmed))

    if not candidates:
        print(f"  ✅ No rows to score (target dates: {len(target_set)}, all rows already scored or empty)")
        return

    print(f"  📋 {len(candidates)} candidate row(s) to score")

    bpp_info      = load_bpp_player_info()
    session       = requests.Session()
    id_cache      = {}
    gamelog_cache = {}
    status_cache  = {}
    updates       = []
    scored = no_data = no_id = dnp_count = pending_count = 0
    result_col = _col_letter(result_idx + 1)

    for sheet_row, target_date, player, line, side, confirmed in candidates:
        key  = normalize_name(player)
        info = bpp_info.get(key) or {}
        if key not in id_cache:
            pid = info.get("id", "") or search_player_id(player, session)
            id_cache[key] = pid
        pid = id_cache[key]
        if not pid:
            no_id += 1
            print(f"  ⚠️  No MLBAM ID for {player!r} — skipped")
            continue

        season  = int(target_date[:4])
        gamelog = fetch_player_gamelog(pid, season, session, gamelog_cache)
        entry   = gamelog.get(target_date)

        if entry is not None and entry[1] > 0:
            hits   = entry[0]
            result = score_result(line, side, hits)
            if not result: continue
            updates.append({"range": f"{result_col}{sheet_row}", "values": [[result]]})
            scored += 1
            print(f"  ✅ {target_date} {player}: {side} {line} | hits={hits} → {result}")
            continue

        player_team = info.get("team", "")
        team_status = ""
        if player_team:
            statuses    = fetch_team_game_statuses(target_date, session, status_cache)
            team_status = statuses.get(player_team, "")
        game_final = team_status == "Final"

        if entry is not None and entry[1] == 0:
            if confirmed == "YES" and not game_final:
                no_data += 1; continue
            updates.append({"range": f"{result_col}{sheet_row}", "values": [["DNP"]]})
            dnp_count += 1
            print(f"  ⏸️  {target_date} {player}: AB=0 in game log → DNP")
            continue

        if team_status in ("Live", "Preview"):
            no_data += 1; continue

        if confirmed == "YES":
            updates.append({"range": f"{result_col}{sheet_row}", "values": [["PENDING"]]})
            pending_count += 1
            print(f"  ⏳ {target_date} {player}: Confirmed=YES, no game log → PENDING")
            continue

        updates.append({"range": f"{result_col}{sheet_row}", "values": [["DNP"]]})
        dnp_count += 1
        if player_team and team_status in ("", "Postponed"):
            reason = "team had no game" if team_status == "" else "game postponed"
        else:
            reason = f"Confirmed={confirmed or 'blank'}, no game log"
        print(f"  ⏸️  {target_date} {player}: {reason} → DNP")

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        print(
            f"\n🎯 Auto-scored {scored} W/L/P + {dnp_count} DNP + "
            f"{pending_count} PENDING ({len(updates)} total) | "
            f"{no_data} player(s) with game still live | {no_id} unmapped"
        )
    else:
        print(
            f"\n🎯 0 scored — game data not yet available "
            f"({no_data} games still live, {no_id} unmapped)"
        )


if __name__ == "__main__":
    main()
