"""
mlb_props.py  —  Daily batter-hits props model.

Changes vs previous version:
  - BEST_HEADERS slimmed to 14 actionable columns (removed research noise)
  - build_best_bets(): Confirmed=YES filter added (PENDING rows excluded)
  - write_best_bets(): dedup indices updated to match new BEST_HEADERS layout
"""

import os, sys, json, math, datetime, unicodedata, difflib
from bisect import bisect_right
from collections import Counter

try:
    from zoneinfo import ZoneInfo
    ET_ZONE = ZoneInfo("America/New_York")
except Exception:
    ET_ZONE = None

import numpy as np
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

try:
    from fetchers.savant import load_savant_pitcher_data, get_pitcher_vs_roster
except Exception as _e:
    print(f"  ⚠️  fetchers.savant unavailable ({_e}) — matchup data disabled")
    def load_savant_pitcher_data(): return {}
    def get_pitcher_vs_roster(pitcher_id, pitcher_name=""): return {}


# ── CONFIG ─────────────────────────────────────────────────────
HERE               = os.path.dirname(os.path.abspath(__file__))
BATTERS_FILE       = os.path.join(HERE, "ballparkpal_batters.xlsx")
PITCHERS_FILE      = os.path.join(HERE, "ballparkpal_pitchers.xlsx")
TEAMS_FILE         = os.path.join(HERE, "ballparkpal_teams.xlsx")
ODDS_API_KEY       = os.environ.get("ODDS_API_KEY", "")
PROPS_SHEET_ID     = os.environ.get("PROPS_SHEET_ID", "")
GSHEET_CRED_ENV    = os.environ.get("GSHEET_CREDENTIALS", "")
ODDS_API_BASE      = "https://api.the-odds-api.com/v4"
SPORT              = "baseball_mlb"
MARKET             = "batter_hits"
EDGE_THRESHOLD     = 0.05
MLB_STATS_BASE     = "https://statsapi.mlb.com/api/v1"
WEIGHT_CAREER      = 0.40
WEIGHT_SEASON      = 0.35
WEIGHT_L20         = 0.25
BLEND_SAVANT       = 0.55
BLEND_BA           = 0.45
PITCHER_ADJUST     = 0.40
PITCHER_MATCHUP_PA = 25
TODAY_TAB          = "Props Today"
BEST_TAB           = "🎯 Best Bets"
MANUAL_TRACKER_TAB = "📊 Tracker"  # populated by props_scorer.py only — model never writes here

# Full scouting card dump — every signal the model uses, visible in one row.
# Groups: Identity | Prop | Batter Statcast | BA Splits | Pitcher Matchup |
#         Model Calcs | Composite | Sizing | Environment | Status
HEADERS = [
    # ── IDENTITY ──────────────────────────────────────────────
    "Date",             # 0
    "Game Time",        # 1
    "Player",           # 2
    "Team",             # 3
    "Pitcher",          # 4  opposing starter name
    "P Hand",           # 5  pitcher hand (L/R)
    "Batter Hand",      # 6  batter stand (L/R/S)
    "Game",             # 7
    # ── PROP ──────────────────────────────────────────────────
    "Line",             # 8
    "Side",             # 9
    "Bookmaker",        # 10
    "Odds",             # 11
    "Implied %",        # 12
    # ── BATTER STATCAST ───────────────────────────────────────
    "xBA",              # 13  Savant expected batting average
    "Hard Hit%",        # 14  % batted balls >= 95 mph EV
    "Barrel%",          # 15  barrel rate
    "Whiff%",           # 16  swing-and-miss rate
    "Exit Velo",        # 17  avg exit velocity
    # ── BA SPLITS ─────────────────────────────────────────────
    "Career BA",        # 18
    "Season BA",        # 19
    "L20 BA",           # 20
    "Weighted BA",      # 21  40% career + 35% season + 25% L20
    "BPP HitProb%",     # 22  BallparkPal raw hit probability
    "BPP AtBats",       # 23
    # ── PITCHER MATCHUP ───────────────────────────────────────
    "P xBA vs Roster",  # 24  pitcher xBA allowed vs this roster (Savant)
    "P Sample PA",      # 25  PA sample size for matchup
    "P K%",             # 26  pitcher K% vs this roster
    "P Hits Allow Rate",# 27  season hits-allowed rate (BPP fallback)
    # ── MODEL CALCS (show your work) ──────────────────────────
    "Batter Blend",     # 28  final batter hit prob before pitcher adj (%)
    "Run Adj Applied",  # 29  team run-total multiplier applied (Y/N + value)
    "Pitcher Adj",      # 30  pitcher adjustment label (matchup/season/none)
    "Final Hit Prob%",  # 31  pitcher-adjusted batter hit prob (%)
    "Model Prob%",      # 32  P(over/under) from binomial model
    "Edge%",            # 33
    "Edge Flag",        # 34  ✅ if edge >= 5%
    # ── COMPOSITE ─────────────────────────────────────────────
    "Composite Score",  # 35
    "Rating",           # 36  ELITE / STRONG / LEAN
    # ── SIZING ────────────────────────────────────────────────
    "Kelly Units",      # 37
    "MC Win%",          # 38  Monte Carlo forward simulation
    # ── ENVIRONMENT ───────────────────────────────────────────
    "Team Proj Runs",   # 39
    "Data Source",      # 40
    # ── STATUS ────────────────────────────────────────────────
    "Confirmed",        # 41  YES / PENDING (extra col, sliced off for Props Today write)
]

# ── BEST BETS: slim 14-column layout ───────────────────────────
# Confirmed=YES + ELITE/STRONG only. Result/Notes for manual W/L fill.
# Index map (used in build_best_bets and write_best_bets):
#   0=Game Time  1=Player  2=Team  3=Game  4=Line  5=Side
#   6=Best Odds  7=Best Book  8=Composite Score  9=Rating
#   10=Edge%  11=Kelly Units  12=Result  13=Notes
BEST_HEADERS = [
    "Game Time", "Player", "Team", "Game", "Line", "Side",
    "Best Odds", "Best Book",
    "Composite Score", "Rating", "Edge%", "Kelly Units",
    "Result", "Notes",
]



def format_game_time(iso_str: str) -> str:
    if not iso_str:
        return ""
    try:
        s  = iso_str.replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        if ET_ZONE:
            dt = dt.astimezone(ET_ZONE)
        return dt.strftime("%H:%M")
    except Exception:
        return iso_str


# ── GOOGLE SHEETS AUTH ─────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

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
    fallback = os.path.join(HERE, "credentials.json")
    return Credentials.from_service_account_file(fallback, scopes=SCOPES)

def get_sheet():
    if not PROPS_SHEET_ID:
        sys.exit("❌ PROPS_SHEET_ID not set")
    creds  = _load_credentials()
    client = gspread.authorize(creds)
    print(f"🔑 PROPS_SHEET_ID = {PROPS_SHEET_ID}")
    print(f"🔑 Service account = {getattr(creds, 'service_account_email', '(unknown)')}")
    return client.open_by_key(PROPS_SHEET_ID)


# ── NAME NORMALIZATION ─────────────────────────────────────────
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


# ── BPP BATTERS ────────────────────────────────────────────────
def load_bpp_batters() -> dict:
    if not os.path.exists(BATTERS_FILE):
        sys.exit(f"❌ Missing {BATTERS_FILE}")
    df   = pd.read_excel(BATTERS_FILE, engine="openpyxl")
    cols = {c.lower(): c for c in df.columns}
    name_col  = next((cols[k] for k in ("fullname","player","name","playername","batter") if k in cols), None)
    hit_col   = cols.get("hitprobability") or cols.get("hitprob")
    ab_col    = cols.get("atbats") or cols.get("ab")
    team_col  = cols.get("team") or cols.get("teamabbr")
    opp_col   = cols.get("opponent") or cols.get("opp")
    side_col  = cols.get("side")
    stand_col = cols.get("batterstand") or cols.get("bats") or cols.get("stand")
    pid_col   = cols.get("playerid") or cols.get("mlbid") or cols.get("id")
    if not (name_col and hit_col and ab_col):
        sys.exit(f"❌ BPP batters missing required columns. Have: {list(df.columns)}")
    out = {}
    for _, row in df.iterrows():
        raw_name = str(row[name_col]).strip()
        if not raw_name or raw_name.lower() == "nan":
            continue
        try:
            p_hit = float(row[hit_col]); ab = float(row[ab_col])
        except (TypeError, ValueError):
            continue
        if pd.isna(p_hit) or pd.isna(ab) or ab <= 0:
            continue
        if p_hit > 1: p_hit /= 100.0
        if not (0 < p_hit < 1): continue
        pid_raw = row[pid_col] if pid_col is not None else None
        if pid_raw is None or (isinstance(pid_raw, float) and pd.isna(pid_raw)):
            pid_str = ""
        else:
            try: pid_str = str(int(float(pid_raw)))
            except (TypeError, ValueError): pid_str = str(pid_raw).strip()
        out[normalize_name(raw_name)] = {
            "name": raw_name, "player_id": pid_str,
            "team": str(row[team_col]).strip().upper() if team_col else "",
            "opp":  str(row[opp_col]).strip().upper()  if opp_col  else "",
            "side": str(row[side_col]).strip()          if side_col else "",
            "stand":str(row[stand_col]).strip().upper() if stand_col else "",
            "p_hit": p_hit, "ab": ab,
        }
    print(f"  ✅ BPP batters loaded: {len(out)}")
    return out


def load_bpp_pitchers() -> dict:
    if not os.path.exists(PITCHERS_FILE):
        print(f"  ⚠️  {PITCHERS_FILE} not found — pitcher matchup/adjust will default to neutral")
        return {}
    df   = pd.read_excel(PITCHERS_FILE, engine="openpyxl")
    cols = {c.lower(): c for c in df.columns}
    team_col = cols.get("team")
    hand_col = cols.get("pitcherhand") or cols.get("throws") or cols.get("hand")
    pid_col  = cols.get("playerid") or cols.get("mlbid") or cols.get("id")
    name_col = next((cols[k] for k in ("fullname","player","name","playername","pitcher") if k in cols), None)
    baa_col  = next((cols[k] for k in ("baa","avgagainst","avg_against","battingaverageagainst","oppavg","oppba","hitprobability","hitprob") if k in cols), None)
    hits_col = next((cols[k] for k in ("hitsallowed","hits","h") if k in cols), None)
    ab_col   = next((cols[k] for k in ("atbatsagainst","atbats","ab","battersfaced","bf","tbf","plateappearances","pa") if k in cols), None)
    if not team_col:
        print(f"  ⚠️  pitchers file missing Team column. Have: {list(df.columns)}")
        return {}
    out = {}; rate_count = 0
    for _, row in df.iterrows():
        team = str(row[team_col]).strip().upper()
        if not team or team in out: continue
        hand_raw = str(row[hand_col]).strip().upper() if hand_col else ""
        hand     = hand_raw[0] if hand_raw else ""
        pid = ""
        if pid_col is not None:
            pid_raw = row[pid_col]
            if pid_raw is not None and not (isinstance(pid_raw, float) and pd.isna(pid_raw)):
                try: pid = str(int(float(pid_raw)))
                except (TypeError, ValueError): pid = str(pid_raw).strip()
        name = str(row[name_col]).strip() if name_col else ""
        hits_allowed_rate = None
        if baa_col is not None:
            try:
                v = float(row[baa_col])
                if v > 1: v /= 100.0
                if 0 < v < 1: hits_allowed_rate = v
            except (TypeError, ValueError): pass
        if hits_allowed_rate is None and hits_col is not None and ab_col is not None:
            try:
                h = float(row[hits_col]); ab = float(row[ab_col])
                if ab > 0:
                    r = h / ab
                    if 0 < r < 1: hits_allowed_rate = r
            except (TypeError, ValueError): pass
        if hits_allowed_rate is not None: rate_count += 1
        out[team] = {"hand": hand, "player_id": pid, "name": name, "hits_allowed_rate": hits_allowed_rate}
    print(f"  ✅ BPP pitchers loaded: {len(out)} teams ({rate_count} with season hits-allowed rate)")
    sample = [(t, v["hand"], v["player_id"], v["name"]) for t, v in list(out.items())[:5]]
    print(f"  🔧 First 5 pitcher entries (team, hand, pid, name): {sample}")
    return out


def load_bpp_teams() -> dict:
    if not os.path.exists(TEAMS_FILE):
        print(f"  ⚠️  {TEAMS_FILE} not found — team run total filter disabled")
        return {}
    df   = pd.read_excel(TEAMS_FILE, engine="openpyxl")
    cols = {c.lower(): c for c in df.columns}
    team_col = cols.get("team")
    runs_col = cols.get("runs") or cols.get("projectedruns") or cols.get("runtotal")
    if not (team_col and runs_col):
        print(f"  ⚠️  teams file missing Team/Runs columns. Have: {list(df.columns)}")
        return {}
    out = {}
    for _, row in df.iterrows():
        team = str(row[team_col]).strip().upper()
        if not team or team in out: continue
        try: runs = float(row[runs_col])
        except (TypeError, ValueError): continue
        if pd.isna(runs): continue
        out[team] = round(runs, 2)
    print(f"  ✅ BPP teams loaded: {len(out)} teams with projected runs")
    return out


# ── BASEBALL SAVANT xBA ────────────────────────────────────────
def fetch_savant_xba(season: int) -> dict:
    try:
        import pybaseball
    except ImportError:
        print("  ⚠️  pybaseball not installed — skipping Savant xBA"); return {}
    try:
        df = pybaseball.statcast_batter_expected_stats(year=season, minPA=25)
    except Exception as e:
        print(f"  ⚠️  Savant xBA fetch failed: {e}"); return {}
    if df is None or getattr(df, "empty", True):
        print("  ⚠️  Savant xBA returned no data"); return {}
    cols    = {c.lower(): c for c in df.columns}
    pid_col = cols.get("player_id") or cols.get("playerid") or cols.get("mlbam_id")
    xba_col = cols.get("est_ba") or cols.get("xba")
    if not (pid_col and xba_col):
        print(f"  ⚠️  Savant xBA missing player_id/est_ba columns; have: {list(df.columns)}"); return {}
    out = {}
    for _, row in df.iterrows():
        try:
            pid = str(int(float(row[pid_col]))); xba = float(row[xba_col])
        except (TypeError, ValueError): continue
        if not (0 < xba < 1): continue
        out[pid] = xba
    print(f"  ✅ Savant xBA loaded: {len(out)} players (year={season}, minPA=25)")
    return out


# ── BASEBALL SAVANT BATTER STATCAST ───────────────────────────
def fetch_savant_batter_statcast(season: int) -> dict:
    """Return {mlbam_id_str: {hard_hit, barrel, whiff, exit_velo}} from
    the Savant statcast leaderboard (batter side).

    Columns pulled:
      hard_hit_percent  — % batted balls >= 95 mph EV
      brl_percent       — barrel rate
      whiff_percent     — swing-and-miss %
      avg_hit_speed     — average exit velocity

    Returns {} on any failure so callers fall back gracefully.
    """
    import csv, io
    print(f"  📡 Fetching Savant batter statcast (year={season}, minPA=25)...")
    try:
        r = requests.get(
            "https://baseballsavant.mlb.com/leaderboard/statcast",
            params={"type": "batter", "year": season, "position": "",
                    "team": "", "min": 25, "csv": "true"},
            timeout=25,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if r.status_code != 200 or not r.content:
            print(f"  ⚠️  Savant batter statcast HTTP {r.status_code}"); return {}
        rows = list(csv.DictReader(io.StringIO(r.content.decode("utf-8-sig"))))
        if not rows:
            print("  ⚠️  Savant batter statcast returned no rows"); return {}
        out = {}
        for row in rows:
            pid_raw = row.get("player_id", "").strip()
            if not pid_raw: continue
            try: pid = str(int(float(pid_raw)))
            except (TypeError, ValueError): continue
            def sf(k):
                v = row.get(k, "").strip()
                try: return float(v) if v else None
                except (TypeError, ValueError): return None
            hh = sf("hard_hit_percent"); brl = sf("brl_percent")
            wh = sf("whiff_percent");    ev  = sf("avg_hit_speed")
            entry = {}
            if hh  is not None: entry["hard_hit"]  = round(hh,  1)
            if brl is not None: entry["barrel"]     = round(brl, 1)
            if wh  is not None: entry["whiff"]      = round(wh,  1)
            if ev  is not None: entry["exit_velo"]  = round(ev,  1)
            if entry: out[pid] = entry
        print(f"  ✅ Savant batter statcast loaded: {len(out)} players")
        return out
    except Exception as e:
        print(f"  ⚠️  Savant batter statcast failed: {e}"); return {}


# ── CONFIRMED LINEUPS ──────────────────────────────────────────
def fetch_confirmed_lineups():
    today = datetime.date.today().isoformat()
    try:
        r = requests.get(f"{MLB_STATS_BASE}/schedule",
                         params={"sportId": 1, "date": today, "hydrate": "lineups"}, timeout=10)
        if r.status_code != 200:
            print(f"  ⚠️  Lineups fetch failed: HTTP {r.status_code}"); return set(), set(), set()
        data = r.json()
    except Exception as e:
        print(f"  ⚠️  Lineups fetch error: {e}"); return set(), set(), set()
    teams_with_lineups, names, ids = set(), set(), set()
    games_with_lineups = 0
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            lineups = game.get("lineups") or {}
            teams_block = game.get("teams") or {}
            had = False
            for side_key, side_team in (("homePlayers","home"), ("awayPlayers","away")):
                players_list = lineups.get(side_key) or []
                if not players_list: continue
                team_abbr = (((teams_block.get(side_team) or {}).get("team") or {}).get("abbreviation","") or "").upper()
                if team_abbr: teams_with_lineups.add(team_abbr)
                for player in players_list:
                    nm  = player.get("fullName") or ""
                    pid = player.get("id")
                    if nm: names.add(normalize_name(nm)); had = True
                    if pid is not None: ids.add(str(pid))
            if had: games_with_lineups += 1
    print(f"  ✅ Confirmed lineups loaded: {games_with_lineups} games, {len(names)} batters, {len(teams_with_lineups)} teams posted")
    return teams_with_lineups, names, ids

def determine_confirmed_status(rec, teams_with_lineups, names, ids) -> str:
    pid  = rec.get("player_id", "")
    key  = normalize_name(rec.get("name", ""))
    team = (rec.get("team") or "").upper()
    if key in names or (pid and pid in ids): return "YES"
    if team in teams_with_lineups: return "NO"
    return "PENDING"


# ── ODDS API ───────────────────────────────────────────────────
def get_batter_hits_props():
    if not ODDS_API_KEY: sys.exit("❌ ODDS_API_KEY not set")
    print("📡 Fetching batter_hits props from The Odds API...")
    try:
        events = requests.get(f"{ODDS_API_BASE}/sports/{SPORT}/events",
                              params={"apiKey": ODDS_API_KEY}, timeout=15).json()
    except Exception as e:
        sys.exit(f"❌ Could not list events: {e}")
    if not isinstance(events, list): sys.exit(f"❌ Unexpected events response: {events}")
    today  = datetime.date.today().isoformat()
    todays = [e for e in events if str(e.get("commence_time","")).startswith(today)]
    print(f"   {len(todays)} events today")
    rows = []; remaining = "?"
    for ev in todays:
        eid = ev.get("id")
        if not eid: continue
        try:
            r = requests.get(f"{ODDS_API_BASE}/sports/{SPORT}/events/{eid}/odds",
                             params={"apiKey": ODDS_API_KEY, "regions": "us",
                                     "markets": MARKET, "oddsFormat": "american"}, timeout=15)
            remaining = r.headers.get("x-requests-remaining", remaining)
            data = r.json()
        except Exception as e:
            print(f"   ⚠️  {eid}: {e}"); continue
        game    = f"{ev.get('away_team','')} @ {ev.get('home_team','')}"
        commence = ev.get("commence_time","")
        for book in data.get("bookmakers",[]) if isinstance(data, dict) else []:
            bk_key = book.get("key","")
            for mkt in book.get("markets",[]):
                if mkt.get("key") != MARKET: continue
                for o in mkt.get("outcomes",[]):
                    player = (o.get("description") or "").strip()
                    side   = (o.get("name") or "").strip()
                    line   = o.get("point"); price = o.get("price")
                    if not player or line is None or price is None: continue
                    rows.append({"player": player, "side": side, "line": float(line),
                                 "price": int(price), "bookmaker": bk_key,
                                 "game": game, "game_time": commence})
    print(f"   ✅ {len(rows)} prop quotes pulled (requests remaining: {remaining})")
    return rows


# ── MLB STATS API ──────────────────────────────────────────────
def fetch_batter_stats(player_id: str, session: requests.Session) -> dict:
    if not player_id: return {}
    season = datetime.date.today().year
    try:
        r = session.get(f"{MLB_STATS_BASE}/people/{player_id}/stats",
                        params={"stats":"career,season,gameLog","group":"hitting","season":season}, timeout=8)
        if r.status_code != 200: return {}
        data = r.json()
    except Exception: return {}
    out = {}
    for block in data.get("stats",[]):
        kind   = (block.get("type",{}).get("displayName") or "").lower()
        splits = block.get("splits",[])
        if not splits: continue
        if "career" in kind:
            avg = splits[0].get("stat",{}).get("avg")
            try: out["career"] = float(avg)
            except (TypeError, ValueError): pass
        elif "season" in kind:
            stat = splits[0].get("stat",{})
            try: out["season"] = float(stat.get("avg"))
            except (TypeError, ValueError): pass
            try: out["season_h"]  = int(stat.get("hits"))
            except (TypeError, ValueError): pass
            try: out["season_ab"] = int(stat.get("atBats"))
            except (TypeError, ValueError): pass
        elif "gamelog" in kind:
            recent = splits[-20:] if len(splits) > 20 else splits
            hits = abs_ = 0
            for s in recent:
                stat = s.get("stat",{})
                try: hits += int(stat.get("hits",0)); abs_ += int(stat.get("atBats",0))
                except (TypeError, ValueError): pass
            if abs_ > 0: out["l20"] = hits / abs_
    return out

def weighted_ba(career, season, l20):
    parts = []
    if career is not None: parts.append((WEIGHT_CAREER, career))
    if season is not None: parts.append((WEIGHT_SEASON, season))
    if l20    is not None: parts.append((WEIGHT_L20,    l20))
    if not parts: return None
    total_w = sum(w for w, _ in parts)
    return sum(w * v for w, v in parts) / total_w

def ba_to_hit_prob(ba: float, ab_raw: float) -> float:
    n = max(1, int(round(ab_raw)))
    if ba is None or ba <= 0: return 0.0
    if ba >= 1: return 1.0
    return 1 - (1 - ba) ** n


# ── PROBABILITY MATH ───────────────────────────────────────────
def implied_prob(american: int) -> float:
    if american == 0: return 0.0
    if american > 0:  return 100.0 / (american + 100)
    return -american / (-american + 100)

def per_ab_hit_prob(p_game_hit: float, ab: int) -> float:
    p_no_game = max(1e-9, 1 - p_game_hit)
    return 1 - p_no_game ** (1 / ab)

def model_over_prob(p_game_hit: float, ab_raw: float, line: float) -> float:
    n = max(1, int(round(ab_raw)))
    p = per_ab_hit_prob(p_game_hit, n)
    threshold = math.floor(line) + 1
    if threshold <= 0: return 1.0
    if threshold > n:  return 0.0
    cdf = 0.0
    for k in range(threshold):
        cdf += math.comb(n, k) * (p ** k) * ((1 - p) ** (n - k))
    return max(0.0, min(1.0, 1 - cdf))


# ── BET SIZING + SIMULATION ────────────────────────────────────
def kelly_units(model_prob, american_odds, fraction=0.25, min_units=0.5, max_units=3.0) -> float:
    if model_prob is None or american_odds is None: return 0.0
    if american_odds > 0:  decimal_odds = 1.0 + american_odds / 100.0
    elif american_odds < 0: decimal_odds = 1.0 + 100.0 / abs(american_odds)
    else: return 0.0
    b = decimal_odds - 1.0
    if b <= 0: return 0.0
    full_kelly_pct = (model_prob * b - (1 - model_prob)) / b * 100.0
    fractional_pct = fraction * full_kelly_pct
    if fractional_pct <= 0: return 0.0
    return round(max(min_units, min(max_units, fractional_pct)) * 2) / 2

def monte_carlo_win_prob(per_ab_rate, bpp_ab, line, side, n_sims=10_000):
    if per_ab_rate is None or bpp_ab is None or bpp_ab <= 0: return None
    if not (0 < per_ab_rate < 1): return None
    if side not in ("Over","Under"): return None
    n_ab = max(1, int(round(bpp_ab)))
    hits = np.random.binomial(n_ab, per_ab_rate, size=n_sims)
    wins = int((hits > line).sum()) if side == "Over" else int((hits < line).sum())
    return wins / n_sims


# ── COMPOSITE SCORING ──────────────────────────────────────────
def composite_score(hit_pct_rank: float, edge_pct_rank: float) -> float:
    return 0.60 * hit_pct_rank + 0.40 * edge_pct_rank

def _percentile(sorted_scores, p):
    if not sorted_scores: return 0.0
    if len(sorted_scores) == 1: return sorted_scores[0]
    k = (len(sorted_scores) - 1) * p / 100
    lo = int(k); hi = min(lo + 1, len(sorted_scores) - 1)
    return sorted_scores[lo] + (k - lo) * (sorted_scores[hi] - sorted_scores[lo])


# ── MATCHING ───────────────────────────────────────────────────
def match_player(prop_name: str, bpp: dict):
    key = normalize_name(prop_name)
    if key in bpp: return bpp[key]
    close = difflib.get_close_matches(key, list(bpp.keys()), n=1, cutoff=0.85)
    return bpp[close[0]] if close else None


# ── BUILD ROWS ─────────────────────────────────────────────────
def build_rows(props, bpp, pitchers, confirmed_map=None, savant_xba=None,
               savant_statcast=None, team_runs=None):
    confirmed_map   = confirmed_map   or {}
    savant_xba      = savant_xba      or {}
    savant_statcast = savant_statcast or {}
    pitchers        = pitchers        or {}
    team_runs       = team_runs       or {}
    today = datetime.date.today().isoformat()
    out = []; misses = 0
    bs_ph_counter = Counter(); source_counter = Counter(); pitcher_source_counter = Counter()
    stats_session = requests.Session(); stats_cache = {}
    stats_hits = stats_total = pitcher_used = 0

    for p in props:
        rec = match_player(p["player"], bpp)
        if not rec: misses += 1; continue

        pid = rec.get("player_id","")
        if pid and pid not in stats_cache:
            stats_cache[pid] = fetch_batter_stats(pid, stats_session)
            stats_total += 1
            if stats_cache[pid]: stats_hits += 1
        stats     = stats_cache.get(pid, {})
        career_ba = stats.get("career"); season_ba = stats.get("season"); l20_ba = stats.get("l20")
        wba       = weighted_ba(career_ba, season_ba, l20_ba)
        xba       = savant_xba.get(pid) if pid else None

        # Batter Savant statcast (Hard Hit%, Barrel%, Whiff%, Exit Velo)
        sv_stat   = savant_statcast.get(pid, {}) if pid else {}
        hard_hit  = sv_stat.get("hard_hit",  "")
        barrel    = sv_stat.get("barrel",    "")
        whiff     = sv_stat.get("whiff",     "")
        exit_velo = sv_stat.get("exit_velo", "")

        if xba is not None and wba is not None:
            batter_phit = BLEND_SAVANT * ba_to_hit_prob(xba, rec["ab"]) + BLEND_BA * ba_to_hit_prob(wba, rec["ab"])
            data_source = "Savant+MLB"
        elif xba is not None:
            batter_phit = ba_to_hit_prob(xba, rec["ab"]); data_source = "Savant only"
        elif wba is not None:
            batter_phit = ba_to_hit_prob(wba, rec["ab"]); data_source = "MLB only"
        else:
            batter_phit = rec["p_hit"]; data_source = "BPP fallback"
        source_counter[data_source] += 1

        batter_blend_pct = round(batter_phit * 100, 2)  # before team-run adj

        team_proj_runs = team_runs.get((rec.get("team") or "").upper())
        run_adj_label  = ""
        if team_proj_runs is not None:
            if team_proj_runs < 2.5:
                batter_phit *= 0.75; run_adj_label = f"x0.75 ({team_proj_runs}R)"
            elif team_proj_runs < 3.5:
                batter_phit *= 0.85; run_adj_label = f"x0.85 ({team_proj_runs}R)"
            else:
                run_adj_label = f"none ({team_proj_runs}R)"
        else:
            run_adj_label = "none"

        opp_team     = (rec.get("opp") or "").upper()
        pitcher_rec  = pitchers.get(opp_team, {})
        pitcher_pid  = pitcher_rec.get("player_id",""); pitcher_name = pitcher_rec.get("name","")
        p_xba_vs_roster = p_k_pct = p_sample_pa = ""
        combined_pitcher_phit = None; pitcher_source = ""

        matchup_data: dict = {}
        if pitcher_pid:
            try: matchup_data = get_pitcher_vs_roster(int(pitcher_pid), pitcher_name) or {}
            except (TypeError, ValueError): matchup_data = {}

        sv_pa  = matchup_data.get("sv_vs_pa") or 0
        sv_xba = matchup_data.get("sv_vs_xba"); sv_k = matchup_data.get("sv_vs_k_pct")

        if sv_pa >= PITCHER_MATCHUP_PA and sv_xba is not None:
            pitcher_phit = ba_to_hit_prob(sv_xba, rec["ab"])
            k_penalty    = 1.0 - (sv_k / 100.0) if sv_k is not None else 1.0
            combined_pitcher_phit = pitcher_phit * k_penalty
            pitcher_source  = "matchup"
            p_xba_vs_roster = round(sv_xba, 3)
            p_k_pct         = round(sv_k, 1) if sv_k is not None else ""
            p_sample_pa     = sv_pa
        else:
            season_rate = pitcher_rec.get("hits_allowed_rate")
            if season_rate is not None:
                combined_pitcher_phit = ba_to_hit_prob(season_rate, rec["ab"])
                pitcher_source = "season"

        if combined_pitcher_phit is not None:
            final_phit = (1 - PITCHER_ADJUST) * batter_phit + PITCHER_ADJUST * combined_pitcher_phit
            pitcher_used += 1; pitcher_source_counter[pitcher_source] += 1
            pitcher_adj_label = pitcher_source  # "matchup" or "season"
        else:
            final_phit = batter_phit
            pitcher_adj_label = "none"

        if p["side"] == "Over":   model_p = model_over_prob(final_phit, rec["ab"], p["line"])
        elif p["side"] == "Under": model_p = 1 - model_over_prob(final_phit, rec["ab"], p["line"])
        else: continue

        imp      = implied_prob(p["price"])
        edge     = model_p - imp
        edge_pct = edge * 100
        bs       = (rec.get("stand","") or "").strip().upper()[:1] or "?"
        ph       = (pitcher_rec.get("hand","") or "").strip().upper()[:1] or "?"
        bs_ph_counter[(bs, ph)] += 1
        kelly        = kelly_units(model_p, p["price"])
        final_per_ab = per_ab_hit_prob(final_phit, rec["ab"])
        mc           = monte_carlo_win_prob(final_per_ab, rec["ab"], p["line"], p["side"])

        out.append([
            today,                                                          # 0  Date
            format_game_time(p.get("game_time","")),                        # 1  Game Time
            p["player"],                                                    # 2  Player
            rec.get("team",""),                                             # 3  Team
            pitcher_name,                                                   # 4  Pitcher
            ph,                                                             # 5  P Hand
            bs,                                                             # 6  Batter Hand
            p["game"],                                                      # 7  Game
            p["line"],                                                      # 8  Line
            p["side"],                                                      # 9  Side
            p["bookmaker"],                                                 # 10 Bookmaker
            p["price"],                                                     # 11 Odds
            round(imp * 100, 2),                                            # 12 Implied %
            round(xba, 3)       if xba       is not None else "",           # 13 xBA
            hard_hit,                                                       # 14 Hard Hit%
            barrel,                                                         # 15 Barrel%
            whiff,                                                          # 16 Whiff%
            exit_velo,                                                      # 17 Exit Velo
            round(career_ba, 3) if career_ba is not None else "",           # 18 Career BA
            round(season_ba, 3) if season_ba is not None else "",           # 19 Season BA
            round(l20_ba,    3) if l20_ba    is not None else "",           # 20 L20 BA
            round(wba,       3) if wba       is not None else "",           # 21 Weighted BA
            round(rec["p_hit"] * 100, 2),                                   # 22 BPP HitProb%
            int(round(rec["ab"])),                                          # 23 BPP AtBats
            p_xba_vs_roster,                                                # 24 P xBA vs Roster
            p_sample_pa,                                                    # 25 P Sample PA
            p_k_pct,                                                        # 26 P K%
            round(pitcher_rec.get("hits_allowed_rate", 0), 3) if pitcher_rec.get("hits_allowed_rate") else "",  # 27 P Hits Allow Rate
            batter_blend_pct,                                               # 28 Batter Blend%
            run_adj_label,                                                  # 29 Run Adj Applied
            pitcher_adj_label,                                              # 30 Pitcher Adj
            round(final_phit * 100, 2),                                     # 31 Final Hit Prob%
            round(model_p    * 100, 2),                                     # 32 Model Prob%
            round(edge_pct, 2),                                             # 33 Edge%
            "✅" if edge >= EDGE_THRESHOLD else "",                          # 34 Edge Flag
            0.0,                                                            # 35 Composite (post-pass)
            "",                                                             # 36 Rating (post-pass)
            kelly,                                                          # 37 Kelly Units
            round(mc * 100, 2) if mc is not None else "",                   # 38 MC Win%
            team_proj_runs if team_proj_runs is not None else "",           # 39 Team Proj Runs
            data_source,                                                    # 40 Data Source
            confirmed_map.get(normalize_name(rec.get("name","")), ""),      # 41 Confirmed (extra col)
        ])

    if misses: print(f"   ⚠️  {misses} prop quotes had no BPP match")
    if stats_total: print(f"  📊 MLB Stats API: {stats_hits}/{stats_total} players returned BA components")
    if source_counter:
        print(f"  📊 Data Source — Savant+MLB: {source_counter['Savant+MLB']}, "
              f"Savant only: {source_counter['Savant only']}, "
              f"MLB only: {source_counter['MLB only']}, "
              f"BPP fallback: {source_counter['BPP fallback']}")
    if pitcher_used:
        print(f"  📊 Pitcher adjustment: {pitcher_used} rows "
              f"(matchup: {pitcher_source_counter.get('matchup',0)}, "
              f"season: {pitcher_source_counter.get('season',0)})")
    if bs_ph_counter:
        rr = bs_ph_counter.get(("R","R"),0); rl = bs_ph_counter.get(("R","L"),0)
        lr = bs_ph_counter.get(("L","R"),0); ll = bs_ph_counter.get(("L","L"),0)
        s_count = sum(v for (b,_),v in bs_ph_counter.items() if b=="S")
        print(f"  📊 Matchup: R vs R:{rr} R vs L:{rl} L vs R:{lr} L vs L:{ll} S:{s_count}")

    # Composite percentile ranks
    # BPP HitProb% = r[22], Edge% = r[33], Composite = r[35], Side = r[9], Line = r[8]
    if out:
        sorted_hit  = sorted(r[22] for r in out)
        sorted_edge = sorted(r[33] for r in out)
        n = len(out)
        for r in out:
            hit_pr  = 100.0 * bisect_right(sorted_hit,  r[22]) / n
            edge_pr = 100.0 * bisect_right(sorted_edge, r[33]) / n
            base    = composite_score(hit_pr, edge_pr)
            adj = 0
            if r[9] == "Under": adj += 10
            if r[8] == 1.5:     adj += 5
            if r[9] == "Over" and r[8] == 0.5: adj -= 5
            r[35] = round(min(100.0, base + adj), 2)

    # Assign ratings by rank — Rating = r[36]
    if out:
        out.sort(key=lambda r: r[35], reverse=True)
        n          = len(out)
        elite_end  = max(1, int(round(n * 0.10)))
        strong_end = elite_end  + max(0, int(round(n * 0.15)))
        lean_end   = strong_end + max(0, int(round(n * 0.35)))
        scores     = [r[35] for r in out]
        print(f"  📊 Composite — n={n} min={scores[-1]:.2f} median={scores[n//2]:.2f} max={scores[0]:.2f}")
        print(f"  🎯 Cutoffs — ELITE: top {elite_end} | STRONG: next {strong_end-elite_end} | "
              f"LEAN: next {lean_end-strong_end} | dropping {n-lean_end}")
        for i, r in enumerate(out):
            if   i < elite_end:  r[36] = "ELITE"
            elif i < strong_end: r[36] = "STRONG"
            elif i < lean_end:   r[36] = "LEAN"

    return out


# ── BEST BETS ──────────────────────────────────────────────────
def build_best_bets(rows):
    """Confirmed=YES + ELITE/STRONG + Kelly>0 + Edge>0.
    One row per player (highest Edge%). Sorted Game Time ASC, Composite DESC.

    BEST_HEADERS indices:
      0=Game Time  1=Player  2=Team  3=Game  4=Line  5=Side
      6=Best Odds  7=Best Book  8=Composite Score  9=Rating
      10=Edge%  11=Kelly Units  12=Result(blank)  13=Notes(blank)
    """
    by_player = {}
    for r in rows:
        if r[36] not in ("ELITE", "STRONG"):        # Rating
            continue
        if (r[41] if len(r) > 41 else "") != "YES": # Confirmed
            continue
        if not isinstance(r[37], (int, float)) or r[37] <= 0:  # Kelly
            continue
        if not isinstance(r[33], (int, float)) or r[33] <= 0:  # Edge%
            continue
        key = r[2]
        if key not in by_player or r[33] > by_player[key][33]:
            by_player[key] = r

    bests = []
    for r in by_player.values():
        bests.append([
            r[1],   # 0  Game Time
            r[2],   # 1  Player
            r[3],   # 2  Team
            r[7],   # 3  Game
            r[8],   # 4  Line
            r[9],   # 5  Side
            r[11],  # 6  Best Odds
            r[10],  # 7  Best Book
            r[35],  # 8  Composite Score
            r[36],  # 9  Rating
            r[33],  # 10 Edge%
            r[37],  # 11 Kelly Units
            "",     # 12 Result (manual fill after game)
            "",     # 13 Notes  (manual fill after game)
        ])

    # Game Time ASC, Composite Score DESC (index 8)
    bests.sort(key=lambda b: (b[0], -b[8]))
    return bests


def write_best_bets(sheet, best_rows):
    end_col = _col_letter(len(BEST_HEADERS))
    print(f"  🔧 BEST_HEADERS ({len(BEST_HEADERS)} cols): {BEST_HEADERS}")
    ws = _get_or_create_ws(sheet, BEST_TAB, cols=max(20, len(BEST_HEADERS)))
    ws.clear()
    ws.update(range_name=f"A1:{end_col}1", values=[BEST_HEADERS], value_input_option="USER_ENTERED")

    # Dedup by (Player, normLine, Side). BEST_HEADERS: Player=1, Line=4, Side=5, Edge%=10
    best_for_key: dict = {}; key_order: list = []; ungroupable: list = []
    for b in best_rows:
        if len(b) <= 10:
            ungroupable.append(b); continue
        key = (b[1], _normalize_line(b[4]), b[5])
        new_edge = b[10] if isinstance(b[10], (int, float)) else -float("inf")
        if key not in best_for_key:
            best_for_key[key] = b; key_order.append(key)
        else:
            existing_edge = best_for_key[key][10] if isinstance(best_for_key[key][10], (int, float)) else -float("inf")
            if new_edge > existing_edge:
                best_for_key[key] = b
    deduped = ungroupable + [best_for_key[k] for k in key_order]
    dupes   = len(best_rows) - len(deduped)
    if dupes:
        print(f"  🧹 {BEST_TAB}: collapsed {dupes} duplicate row(s) — kept highest Edge%")

    if deduped:
        end_row = 1 + len(deduped)
        ws.update(range_name=f"A2:{end_col}{end_row}", values=deduped, value_input_option="USER_ENTERED")
    print(f"  ✅ {BEST_TAB}: {len(deduped)} rows written (Confirmed=YES only)")


# ── PROPS TRACKER REMOVED ──────────────────────────────────────
# Props Tracker tab deleted. 📊 Tracker is populated by props_scorer.py
# only — reading from Best Bets and writing W/L results back to Tracker.


# ── SHEET UTILITIES ────────────────────────────────────────────
def _get_or_create_ws(sheet, title, rows=1000, cols=30):
    try: return sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return sheet.add_worksheet(title=title, rows=rows, cols=max(cols, len(HEADERS)))

def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26); s = chr(65 + r) + s
    return s


def write_today(sheet, rows):
    end_col = _col_letter(len(HEADERS))
    ws = _get_or_create_ws(sheet, TODAY_TAB); ws.clear()
    ws.update(range_name=f"A1:{end_col}1", values=[HEADERS], value_input_option="USER_ENTERED")
    best_for_key: dict = {}; key_order: list = []; ungroupable: list = []
    for r in rows:
        if len(r) <= 33: ungroupable.append(r); continue
        key = (r[0], r[2], _normalize_line(r[8]), r[9])   # Date, Player, Line, Side
        new_edge = r[33] if isinstance(r[33],(int,float)) else -float("inf")
        if key not in best_for_key:
            best_for_key[key] = r; key_order.append(key)
        else:
            existing_edge = best_for_key[key][33] if isinstance(best_for_key[key][33],(int,float)) else -float("inf")
            if new_edge > existing_edge: best_for_key[key] = r
    deduped = ungroupable + [best_for_key[k] for k in key_order]
    dupes   = len(rows) - len(deduped)
    if dupes: print(f"  🧹 {TODAY_TAB}: collapsed {dupes} duplicate row(s)")
    if deduped:
        sliced = [r[:len(HEADERS)] for r in deduped]
        ws.update(range_name=f"A2:{end_col}{1+len(sliced)}", values=sliced, value_input_option="USER_ENTERED")
    print(f"  ✅ {TODAY_TAB}: header + {len(deduped)} rows")


# ── MAIN ───────────────────────────────────────────────────────
def main():
    bpp      = load_bpp_batters()
    pitchers = load_bpp_pitchers()

    teams_with_lineups, confirmed_names, confirmed_ids = fetch_confirmed_lineups()
    confirmed_map = {}; new_bpp = {}; yes_count = pending_count = no_count = 0
    for key, rec in bpp.items():
        status = determine_confirmed_status(rec, teams_with_lineups, confirmed_names, confirmed_ids)
        if status == "NO":      no_count      += 1; continue
        if status == "YES":     yes_count     += 1
        else:                   pending_count += 1
        confirmed_map[key] = status; new_bpp[key] = rec
    bpp = new_bpp
    print(f"  ✅ Confirmed status — YES: {yes_count} | PENDING: {pending_count} | NO (excluded): {no_count}")

    season          = datetime.date.today().year
    savant_xba      = fetch_savant_xba(season)
    savant_statcast = fetch_savant_batter_statcast(season)
    load_savant_pitcher_data()
    team_runs  = load_bpp_teams()

    props = get_batter_hits_props()
    rows  = build_rows(props, bpp, pitchers, confirmed_map=confirmed_map,
                       savant_xba=savant_xba, savant_statcast=savant_statcast,
                       team_runs=team_runs)

    rated  = [r for r in rows if r[36] in ("ELITE","STRONG","LEAN")]
    bests  = build_best_bets(rated)

    sheet = get_sheet()
    write_today(sheet, rated)
    write_best_bets(sheet, bests)

    edges  = sum(1 for r in rows if r[34])             # Edge Flag index
    elite  = sum(1 for r in rated if r[36] == "ELITE")
    strong = sum(1 for r in rated if r[36] == "STRONG")
    lean   = sum(1 for r in rated if r[36] == "LEAN")
    print(
        f"\n🎯 Done — {len(rows)} priced props, "
        f"{edges} flagged edges >= {int(EDGE_THRESHOLD*100)}% | "
        f"ELITE: {elite}, STRONG: {strong}, LEAN: {lean} | "
        f"Best Bets: {len(bests)} (Confirmed=YES only) | "
        f"📊 Tracker populated by props_scorer.py only"
    )


if __name__ == "__main__":
    main()
