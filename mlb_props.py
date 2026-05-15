"""
mlb_props.py
─────────────
Daily batter-hits props model.

Reads BallparkPal batters export, pulls batter_hits prop lines from The Odds
API, runs a binomial model on HitProbability + AtBats, flags edges >= 5%,
and writes results to two tabs in a Google Sheet:
  - "Props Today"    (cleared and rewritten each run)
  - "Props Tracker"  (running log, appended each run)

ENV:
  ODDS_API_KEY        The Odds API key
  PROPS_SHEET_ID      Google Sheet ID
  GSHEET_CREDENTIALS  Service-account JSON content OR path to credentials file
                      (falls back to ./credentials.json)
"""

import os
import sys
import json
import math
import datetime
import unicodedata
import difflib
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

# Baseball Savant probable-pitcher matchup helpers (per-roster xBA + K%).
# Imported here so build_rows can use the matchup-specific signal, falling
# back to the BPP file's season hits-allowed rate. Defensive import: degrades
# to empty data when Savant is blocked or the file isn't present.
try:
    from fetchers.savant import (
        load_savant_pitcher_data,
        get_pitcher_vs_roster,
    )
except Exception as _e:  # pragma: no cover — Savant is an optional signal
    print(f"  ⚠️  fetchers.savant unavailable ({_e}) — matchup data disabled")
    def load_savant_pitcher_data():  # type: ignore
        return {}
    def get_pitcher_vs_roster(pitcher_id, pitcher_name=""):  # type: ignore
        return {}


# ── CONFIG ─────────────────────────────────────────────────────
HERE              = os.path.dirname(os.path.abspath(__file__))
BATTERS_FILE      = os.path.join(HERE, "ballparkpal_batters.xlsx")
PITCHERS_FILE     = os.path.join(HERE, "ballparkpal_pitchers.xlsx")

ODDS_API_KEY      = os.environ.get("ODDS_API_KEY", "")
PROPS_SHEET_ID    = os.environ.get("PROPS_SHEET_ID", "")
GSHEET_CRED_ENV   = os.environ.get("GSHEET_CREDENTIALS", "")

ODDS_API_BASE     = "https://api.the-odds-api.com/v4"
SPORT             = "baseball_mlb"
MARKET            = "batter_hits"
EDGE_THRESHOLD    = 0.05  # flag edges >= 5%

MLB_STATS_BASE    = "https://statsapi.mlb.com/api/v1"
WEIGHT_CAREER     = 0.40
WEIGHT_SEASON     = 0.35
WEIGHT_L20        = 0.25
# Batter hit-prob blend. BPP HitProbability is a FALLBACK ONLY — it never
# enters the weighted blend. The four cases (recorded in the "Data Source"
# column):
#   Savant + MLB → 0.55 * Savant xBA + 0.45 * MLB weighted BA
#   Savant only  → 1.00 * Savant xBA
#   MLB only     → 1.00 * MLB weighted BA
#   neither      → 1.00 * BPP HitProbability   ("BPP fallback")
BLEND_SAVANT      = 0.55
BLEND_BA          = 0.45
# Opposing-pitcher adjustment to the batter blend (per-game hit prob):
#   final = (1 - PITCHER_ADJUST) * batter_blend + PITCHER_ADJUST * combined_pitcher_phit
# combined_pitcher_phit comes from the Savant probable-pitcher matchup when
# the sample is solid (sv_vs_pa >= 25): xBA-allowed converted to a per-game
# hit prob and then knocked down by the pitcher's K% vs this roster. When the
# matchup is thin/missing it falls back to the BPP file's season hits-allowed
# rate. If neither is available the adjustment is skipped.
PITCHER_ADJUST     = 0.40
PITCHER_MATCHUP_PA = 25   # min Savant sample size to trust the matchup signal

TODAY_TAB           = "Props Today"
TRACKER_TAB         = "Props Tracker"
BEST_TAB            = "🎯 Best Bets"
MANUAL_TRACKER_TAB  = "📊 Tracker"

HEADERS = [
    "Date", "Game Time", "Player", "Team", "Game", "Line", "Side",
    "Bookmaker", "Odds", "Implied %",
    "BPP HitProb %", "BPP AtBats",
    "Career BA", "Season BA", "L20 BA", "Weighted BA",
    "Savant xBA", "P xBA vs Roster",
    "Model Prob",
    "Model Prob %", "Edge %", "Edge Flag",
    "Composite Score", "Rating",
    "Kelly Units", "MC Win%",
    "Data Source",
    "P Sample PA", "P K%",
]

BEST_HEADERS = [
    "Game Time", "Player", "Team", "Line", "Side",
    "Best Odds", "Best Book", "BPP Hit%", "Savant xBA", "P xBA vs Roster",
    "P Sample PA", "P K%",
    "Data Source",
    "Model Prob%", "Edge%",
    "Rating", "Composite Score",
    "Kelly Units", "MC Win%",
    "Confirmed",
]

TRACKER_HEADERS = [
    "Date", "Game Time", "Player", "Team", "Game", "Line", "Side",
    "BPP Hit%", "Model Prob%", "Edge%",
    "Composite Score", "Rating",
    "Kelly Units", "MC Win%",
    "Result", "Notes",
]

MANUAL_TRACKER_HEADERS = [
    "Date", "Game Time", "Player", "Team", "Game", "Line", "Side",
    "BPP Hit%", "Model Prob%", "Edge%",
    "Kelly Units", "MC Win%",
    "Composite Score", "Rating",
    "Confirmed",
    "Result", "Notes",
]


def format_game_time(iso_str: str) -> str:
    """Convert ISO UTC commence_time to 24-hour ET display (e.g. '19:35')."""
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
    """Canonical dedup string for a Line value.

    Sheets writes 1.0 back as "1" (trailing zeros stripped) while build_rows
    keeps lines as floats, so str(1.0)="1.0" never matches the sheet's "1".
    Normalizing both sides to "1.0" / "1.5" / "2.5" fixes the false-negative
    on whole-number lines.
    """
    try:
        return f"{float(v):.1f}"
    except (TypeError, ValueError):
        return str(v).strip()


def _dedup_key(date, player, line, side) -> tuple:
    """Canonical (Date, Player, Line, Side) dedup key.

    Player goes through normalize_name and Side is stripped+lowercased so
    subtle whitespace/case drift between runs (or between the sheet's stored
    value and a fresh Odds-API value) can't split true duplicates into
    separate groups — that was why duplicate rows kept surviving cleanup.
    """
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
    df = pd.read_excel(BATTERS_FILE, engine="openpyxl")
    cols = {c.lower(): c for c in df.columns}

    name_col = next(
        (cols[k] for k in ("fullname", "player", "name", "playername", "batter") if k in cols),
        None,
    )
    hit_col  = cols.get("hitprobability") or cols.get("hitprob")
    ab_col   = cols.get("atbats") or cols.get("ab")
    team_col = cols.get("team") or cols.get("teamabbr")
    opp_col  = cols.get("opponent") or cols.get("opp")
    side_col = cols.get("side")
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
            p_hit = float(row[hit_col])
            ab    = float(row[ab_col])
        except (TypeError, ValueError):
            continue
        if pd.isna(p_hit) or pd.isna(ab) or ab <= 0:
            continue
        if p_hit > 1:
            p_hit = p_hit / 100.0
        if not (0 < p_hit < 1):
            continue
        pid_raw = row[pid_col] if pid_col is not None else None
        if pid_raw is None or (isinstance(pid_raw, float) and pd.isna(pid_raw)):
            pid_str = ""
        else:
            try:
                pid_str = str(int(float(pid_raw)))
            except (TypeError, ValueError):
                pid_str = str(pid_raw).strip()
        out[normalize_name(raw_name)] = {
            "name":      raw_name,
            "player_id": pid_str,
            "team":      str(row[team_col]).strip().upper() if team_col else "",
            "opp":       str(row[opp_col]).strip().upper() if opp_col else "",
            "side":      str(row[side_col]).strip() if side_col else "",
            "stand":     str(row[stand_col]).strip().upper() if stand_col else "",
            "p_hit":     p_hit,
            "ab":        ab,
        }
    print(f"  ✅ BPP batters loaded: {len(out)}")
    return out


def load_bpp_pitchers() -> dict:
    """team_abbr → {'hand', 'player_id', 'name', 'hits_allowed_rate'}.

    Joined on the batter's Opponent to attach the opposing starter — used
    for the B/P matchup distribution print, the Savant matchup lookup, and
    the pitcher-adjust season fallback. `hits_allowed_rate` is a per-AB hit
    rate (≈ batting average against): a direct BAA-style column if present,
    else HitsAllowed / AtBats. None when neither can be derived. Returns {}
    silently if the file or required columns are missing.
    """
    if not os.path.exists(PITCHERS_FILE):
        print(f"  ⚠️  {PITCHERS_FILE} not found — pitcher matchup/adjust will default to neutral")
        return {}
    df = pd.read_excel(PITCHERS_FILE, engine="openpyxl")
    cols = {c.lower(): c for c in df.columns}
    team_col = cols.get("team")
    hand_col = cols.get("pitcherhand") or cols.get("throws") or cols.get("hand")
    pid_col  = cols.get("playerid") or cols.get("mlbid") or cols.get("id")
    name_col = next(
        (cols[k] for k in ("fullname", "player", "name", "playername", "pitcher") if k in cols),
        None,
    )
    # Season hits-allowed fallback: a direct rate column wins; otherwise
    # derive HitsAllowed / AtBats. Column names vary across BPP exports, so
    # probe a handful of likely spellings.
    baa_col = next(
        (cols[k] for k in (
            "baa", "avgagainst", "avg_against", "battingaverageagainst",
            "oppavg", "oppba", "hitprobability", "hitprob",
        ) if k in cols),
        None,
    )
    hits_col = next(
        (cols[k] for k in ("hitsallowed", "hits", "h") if k in cols),
        None,
    )
    ab_col = next(
        (cols[k] for k in (
            "atbatsagainst", "atbats", "ab", "battersfaced", "bf", "tbf",
            "plateappearances", "pa",
        ) if k in cols),
        None,
    )
    if not team_col:
        print(f"  ⚠️  pitchers file missing Team column. Have: {list(df.columns)}")
        return {}

    out = {}
    rate_count = 0
    for _, row in df.iterrows():
        team = str(row[team_col]).strip().upper()
        if not team or team in out:
            continue  # first row per team = projected starter
        hand_raw = str(row[hand_col]).strip().upper() if hand_col else ""
        hand     = hand_raw[0] if hand_raw else ""
        pid = ""
        if pid_col is not None:
            pid_raw = row[pid_col]
            if pid_raw is not None and not (isinstance(pid_raw, float) and pd.isna(pid_raw)):
                try:
                    pid = str(int(float(pid_raw)))
                except (TypeError, ValueError):
                    pid = str(pid_raw).strip()
        name = str(row[name_col]).strip() if name_col else ""

        hits_allowed_rate = None
        if baa_col is not None:
            try:
                v = float(row[baa_col])
                if v > 1:          # stored as a percentage
                    v /= 100.0
                if 0 < v < 1:
                    hits_allowed_rate = v
            except (TypeError, ValueError):
                pass
        if hits_allowed_rate is None and hits_col is not None and ab_col is not None:
            try:
                h  = float(row[hits_col])
                ab = float(row[ab_col])
                if ab > 0:
                    r = h / ab
                    if 0 < r < 1:
                        hits_allowed_rate = r
            except (TypeError, ValueError):
                pass
        if hits_allowed_rate is not None:
            rate_count += 1

        out[team] = {
            "hand":              hand,
            "player_id":         pid,
            "name":              name,
            "hits_allowed_rate": hits_allowed_rate,
        }
    print(f"  ✅ BPP pitchers loaded: {len(out)} teams ({rate_count} with season hits-allowed rate)")
    sample = [(t, v["hand"], v["player_id"], v["name"]) for t, v in list(out.items())[:5]]
    print(f"  🔧 First 5 pitcher entries (team, hand, pid, name): {sample}")
    return out


# ── BASEBALL SAVANT xBA ────────────────────────────────────────
def fetch_savant_xba(season: int) -> dict:
    """Return {mlbam_id_str: xba_float} from Baseball Savant via pybaseball.

    Uses `statcast_batter_expected_stats(year=season, minPA=25)`. xBA is
    expected batting average from quality of contact (Statcast column
    `est_ba`). Returns {} on any failure so the caller can fall back to
    the no-Savant blend.
    """
    try:
        import pybaseball  # type: ignore
    except ImportError:
        print("  ⚠️  pybaseball not installed — skipping Savant xBA")
        return {}
    try:
        df = pybaseball.statcast_batter_expected_stats(year=season, minPA=25)
    except Exception as e:
        print(f"  ⚠️  Savant xBA fetch failed: {e}")
        return {}

    if df is None or getattr(df, "empty", True):
        print("  ⚠️  Savant xBA returned no data")
        return {}

    cols    = {c.lower(): c for c in df.columns}
    pid_col = cols.get("player_id") or cols.get("playerid") or cols.get("mlbam_id")
    xba_col = cols.get("est_ba") or cols.get("xba")
    if not (pid_col and xba_col):
        print(f"  ⚠️  Savant xBA missing player_id/est_ba columns; have: {list(df.columns)}")
        return {}

    out = {}
    for _, row in df.iterrows():
        try:
            pid = str(int(float(row[pid_col])))
            xba = float(row[xba_col])
        except (TypeError, ValueError):
            continue
        if not (0 < xba < 1):
            continue
        out[pid] = xba
    print(f"  ✅ Savant xBA loaded: {len(out)} players (year={season}, minPA=25)")
    return out


# ── CONFIRMED LINEUPS ──────────────────────────────────────────
def fetch_confirmed_lineups():
    """Pull today's confirmed lineups from the MLB Stats API.

    Returns (teams_with_lineups, names, ids):
      - teams_with_lineups: set of team abbreviations whose lineup is posted
      - names: normalized names of all confirmed starters across all games
      - ids:   player_id strings of all confirmed starters
    All empty means no lineups posted yet (early-morning run).
    """
    today = datetime.date.today().isoformat()
    try:
        r = requests.get(
            f"{MLB_STATS_BASE}/schedule",
            params={"sportId": 1, "date": today, "hydrate": "lineups"},
            timeout=10,
        )
        if r.status_code != 200:
            print(f"  ⚠️  Lineups fetch failed: HTTP {r.status_code}")
            return set(), set(), set()
        data = r.json()
    except Exception as e:
        print(f"  ⚠️  Lineups fetch error: {e}")
        return set(), set(), set()

    teams_with_lineups, names, ids = set(), set(), set()
    games_with_lineups = 0
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            lineups = game.get("lineups") or {}
            teams_block = game.get("teams") or {}
            had = False
            for side_key, side_team in (
                ("homePlayers", "home"),
                ("awayPlayers", "away"),
            ):
                players_list = lineups.get(side_key) or []
                if not players_list:
                    continue
                team_abbr = (
                    ((teams_block.get(side_team) or {}).get("team") or {})
                    .get("abbreviation", "") or ""
                ).upper()
                if team_abbr:
                    teams_with_lineups.add(team_abbr)
                for player in players_list:
                    nm  = player.get("fullName") or ""
                    pid = player.get("id")
                    if nm:
                        names.add(normalize_name(nm))
                        had = True
                    if pid is not None:
                        ids.add(str(pid))
            if had:
                games_with_lineups += 1
    print(
        f"  ✅ Confirmed lineups loaded: {games_with_lineups} games, "
        f"{len(names)} batters, {len(teams_with_lineups)} teams posted"
    )
    return teams_with_lineups, names, ids


def determine_confirmed_status(rec, teams_with_lineups, names, ids) -> str:
    """Per-player 'YES' / 'NO' / 'PENDING' from confirmed-lineup state.

    YES     player appears in a posted lineup
    NO      their team's lineup is posted but they're not in it (benched)
    PENDING their team's lineup has not been posted yet (morning run)
    """
    pid  = rec.get("player_id", "")
    key  = normalize_name(rec.get("name", ""))
    team = (rec.get("team") or "").upper()

    if key in names or (pid and pid in ids):
        return "YES"
    if team in teams_with_lineups:
        return "NO"
    return "PENDING"


# ── ODDS API ───────────────────────────────────────────────────
def get_batter_hits_props():
    if not ODDS_API_KEY:
        sys.exit("❌ ODDS_API_KEY not set")
    print("📡 Fetching batter_hits props from The Odds API...")

    try:
        events = requests.get(
            f"{ODDS_API_BASE}/sports/{SPORT}/events",
            params={"apiKey": ODDS_API_KEY},
            timeout=15,
        ).json()
    except Exception as e:
        sys.exit(f"❌ Could not list events: {e}")
    if not isinstance(events, list):
        sys.exit(f"❌ Unexpected events response: {events}")

    today = datetime.date.today().isoformat()
    todays = [e for e in events if str(e.get("commence_time", "")).startswith(today)]
    print(f"   {len(todays)} events today")

    rows = []
    remaining = "?"
    for ev in todays:
        eid = ev.get("id")
        if not eid:
            continue
        try:
            r = requests.get(
                f"{ODDS_API_BASE}/sports/{SPORT}/events/{eid}/odds",
                params={
                    "apiKey":     ODDS_API_KEY,
                    "regions":    "us",
                    "markets":    MARKET,
                    "oddsFormat": "american",
                },
                timeout=15,
            )
            remaining = r.headers.get("x-requests-remaining", remaining)
            data = r.json()
        except Exception as e:
            print(f"   ⚠️  {eid}: {e}")
            continue

        game = f"{ev.get('away_team','')} @ {ev.get('home_team','')}"
        commence = ev.get("commence_time", "")
        for book in data.get("bookmakers", []) if isinstance(data, dict) else []:
            bk_key = book.get("key", "")
            for mkt in book.get("markets", []):
                if mkt.get("key") != MARKET:
                    continue
                for o in mkt.get("outcomes", []):
                    player = (o.get("description") or "").strip()
                    side   = (o.get("name") or "").strip()
                    line   = o.get("point")
                    price  = o.get("price")
                    if not player or line is None or price is None:
                        continue
                    rows.append({
                        "player":    player,
                        "side":      side,
                        "line":      float(line),
                        "price":     int(price),
                        "bookmaker": bk_key,
                        "game":      game,
                        "game_time": commence,
                    })
    print(f"   ✅ {len(rows)} prop quotes pulled (requests remaining: {remaining})")
    return rows


# ── MLB STATS API ──────────────────────────────────────────────
def fetch_batter_stats(player_id: str, session: requests.Session) -> dict:
    """Fetch career, current-season, and last-20-game BA for a batter.

    Returns {career: float, season: float, l20: float} with whichever keys
    are available. Empty dict on any failure — the caller falls back to
    100% BPP HitProbability when no BA components are present.
    """
    if not player_id:
        return {}
    season = datetime.date.today().year
    params = {
        "stats":  "career,season,gameLog",
        "group":  "hitting",
        "season": season,
    }
    try:
        r = session.get(
            f"{MLB_STATS_BASE}/people/{player_id}/stats",
            params=params,
            timeout=8,
        )
        if r.status_code != 200:
            return {}
        data = r.json()
    except Exception:
        return {}

    out = {}
    for block in data.get("stats", []):
        kind   = (block.get("type", {}).get("displayName") or "").lower()
        splits = block.get("splits", [])
        if not splits:
            continue
        if "career" in kind:
            avg = splits[0].get("stat", {}).get("avg")
            try: out["career"] = float(avg)
            except (TypeError, ValueError): pass
        elif "season" in kind:
            stat = splits[0].get("stat", {})
            avg  = stat.get("avg")
            h    = stat.get("hits")
            ab   = stat.get("atBats")
            try: out["season"] = float(avg)
            except (TypeError, ValueError): pass
            try: out["season_h"]  = int(h)
            except (TypeError, ValueError): pass
            try: out["season_ab"] = int(ab)
            except (TypeError, ValueError): pass
        elif "gamelog" in kind:
            recent = splits[-20:] if len(splits) > 20 else splits
            hits, abs_ = 0, 0
            for s in recent:
                stat = s.get("stat", {})
                try:
                    hits += int(stat.get("hits", 0))
                    abs_ += int(stat.get("atBats", 0))
                except (TypeError, ValueError):
                    pass
            if abs_ > 0:
                out["l20"] = hits / abs_
    return out


def weighted_ba(career, season, l20):
    """40% career + 35% season + 25% L20, redistributing missing weights."""
    parts = []
    if career is not None: parts.append((WEIGHT_CAREER, career))
    if season is not None: parts.append((WEIGHT_SEASON, season))
    if l20    is not None: parts.append((WEIGHT_L20,    l20))
    if not parts:
        return None
    total_w = sum(w for w, _ in parts)
    return sum(w * v for w, v in parts) / total_w


def ba_to_hit_prob(ba: float, ab_raw: float) -> float:
    """Per-AB BA → P(>=1 hit in game) over `ab_raw` plate appearances."""
    n = max(1, int(round(ab_raw)))
    if ba is None or ba <= 0:
        return 0.0
    if ba >= 1:
        return 1.0
    return 1 - (1 - ba) ** n


# ── PROBABILITY MATH ───────────────────────────────────────────
def implied_prob(american: int) -> float:
    if american == 0:
        return 0.0
    if american > 0:
        return 100.0 / (american + 100)
    return -american / (-american + 100)


def per_ab_hit_prob(p_game_hit: float, ab: int) -> float:
    """Convert P(>=1 hit in game) to per-AB hit probability."""
    p_no_game = max(1e-9, 1 - p_game_hit)
    return 1 - p_no_game ** (1 / ab)


def model_over_prob(p_game_hit: float, ab_raw: float, line: float) -> float:
    """P(hits > line) under a binomial model with derived per-AB hit prob."""
    n = max(1, int(round(ab_raw)))
    p = per_ab_hit_prob(p_game_hit, n)
    threshold = math.floor(line) + 1  # need strictly more than `line` hits
    if threshold <= 0:
        return 1.0
    if threshold > n:
        return 0.0
    cdf = 0.0
    for k in range(threshold):
        cdf += math.comb(n, k) * (p ** k) * ((1 - p) ** (n - k))
    return max(0.0, min(1.0, 1 - cdf))


# ── BET SIZING + SIMULATION ────────────────────────────────────
def kelly_units(model_prob, american_odds, fraction: float = 0.25,
                min_units: float = 0.5, max_units: float = 3.0) -> float:
    """Quarter-Kelly recommendation in units (1 unit = 1% bankroll).

    Kelly% = (p·b − q) / b   with   b = decimal_odds − 1
    Returns 0 when Kelly is non-positive (no-bet). Otherwise the quarter-Kelly
    percentage is clamped to [min_units, max_units] and rounded to 0.5.
    """
    if model_prob is None or american_odds is None:
        return 0.0
    if american_odds > 0:
        decimal_odds = 1.0 + american_odds / 100.0
    elif american_odds < 0:
        decimal_odds = 1.0 + 100.0 / abs(american_odds)
    else:
        return 0.0
    b = decimal_odds - 1.0
    if b <= 0:
        return 0.0
    p = model_prob
    q = 1.0 - p
    full_kelly_pct = (p * b - q) / b * 100.0
    fractional_pct = fraction * full_kelly_pct
    if fractional_pct <= 0:
        return 0.0
    units = max(min_units, min(max_units, fractional_pct))
    return round(units * 2) / 2  # nearest 0.5


def monte_carlo_win_prob(per_ab_rate, bpp_ab, line, side,
                          n_sims: int = 10_000):
    """Forward Monte Carlo using the model's pitcher-adjusted per-AB hit rate.

    Simulates `bpp_ab` Bernoulli trials at `per_ab_rate` for each of n_sims
    runs and counts wins by the bet's side. Side semantics (half-lines):

        Over 0.5  → P(hits >= 1)
        Over 1.5  → P(hits >= 2)
        Under 0.5 → P(hits == 0)
        Under 1.5 → P(hits <= 1)
        Under 2.5 → P(hits <= 2)

    Equivalent to (hits > line) for Over and (hits < line) for Under;
    whole-number lines push on equality and aren't counted as wins.
    """
    if per_ab_rate is None or bpp_ab is None or bpp_ab <= 0:
        return None
    if not (0 < per_ab_rate < 1):
        return None
    if side not in ("Over", "Under"):
        return None
    n_ab = max(1, int(round(bpp_ab)))
    hits = np.random.binomial(n_ab, per_ab_rate, size=n_sims)
    if side == "Over":
        wins = int((hits > line).sum())
    else:
        wins = int((hits < line).sum())
    return wins / n_sims


# ── COMPOSITE SCORING ──────────────────────────────────────────
def composite_score(hit_pct_rank: float, edge_pct_rank: float) -> float:
    """60% BPP HitProb percentile rank + 40% edge percentile rank.

    Both inputs are percentile ranks (0–100) computed across today's pool
    of priced players, so composite naturally spans ~0–100 regardless of
    how tightly the underlying BPP/edge values cluster.
    """
    return 0.60 * hit_pct_rank + 0.40 * edge_pct_rank


def _percentile(sorted_scores, p):
    """Linear-interpolated percentile (0-100). `sorted_scores` must be sorted ascending."""
    if not sorted_scores:
        return 0.0
    if len(sorted_scores) == 1:
        return sorted_scores[0]
    k    = (len(sorted_scores) - 1) * p / 100
    lo   = int(k)
    hi   = min(lo + 1, len(sorted_scores) - 1)
    frac = k - lo
    return sorted_scores[lo] + frac * (sorted_scores[hi] - sorted_scores[lo])


# ── MATCHING ───────────────────────────────────────────────────
def match_player(prop_name: str, bpp: dict):
    key = normalize_name(prop_name)
    if key in bpp:
        return bpp[key]
    close = difflib.get_close_matches(key, list(bpp.keys()), n=1, cutoff=0.85)
    return bpp[close[0]] if close else None


# ── BUILD ROWS ─────────────────────────────────────────────────
def build_rows(props, bpp, pitchers, confirmed_map=None, savant_xba=None):
    """Build per-prop rows.

    confirmed_map (normalized BPP name → 'YES'/'PENDING') is stashed as an
    extra column at index 29 — Best Bets and 📊 Tracker pick it up;
    HEADERS-based writers (Props Today, Props Tracker) slice it off.

    savant_xba (mlbam_id → est_ba) feeds the batter blend. Per-player
    fallbacks: no xBA → 50/50 BA+BPP; neither xBA nor BA → 100% BPP.

    pitchers (team_abbr → {hand, player_id, name, hits_allowed_rate}) drives
    the opposing-pitcher adjustment:
      - Savant probable-pitcher matchup (xBA + K% vs THIS roster) when the
        sample is solid (sv_vs_pa >= PITCHER_MATCHUP_PA):
            pitcher_phit = ba_to_hit_prob(sv_vs_xba, AB)
            k_penalty    = 1 - sv_vs_k_pct/100
            combined     = pitcher_phit * k_penalty
      - else the BPP file's season hits-allowed rate.
        final_phit = (1 - PITCHER_ADJUST) * batter_blend + PITCHER_ADJUST * combined
      Skipped (per-row) only when neither source is available.
    """
    confirmed_map      = confirmed_map or {}
    savant_xba         = savant_xba or {}
    pitchers           = pitchers or {}
    today = datetime.date.today().isoformat()
    out = []
    misses = 0
    bs_ph_counter           = Counter()
    source_counter          = Counter()  # rows per Data Source category
    pitcher_source_counter  = Counter()  # rows per pitcher xBA source (matchup/season)
    stats_session = requests.Session()
    stats_cache   = {}
    stats_hits    = 0  # players with at least one BA component
    stats_total   = 0  # unique players we tried to fetch
    pitcher_used  = 0  # rows where the pitcher adjustment was applied
    for p in props:
        rec = match_player(p["player"], bpp)
        if not rec:
            misses += 1
            continue

        # Fetch MLB Stats API blend (career / season / L20). Cache per player_id.
        pid = rec.get("player_id", "")
        if pid and pid not in stats_cache:
            stats_cache[pid] = fetch_batter_stats(pid, stats_session)
            stats_total += 1
            if stats_cache[pid]:
                stats_hits += 1
        stats = stats_cache.get(pid, {})
        career_ba = stats.get("career")
        season_ba = stats.get("season")
        l20_ba    = stats.get("l20")
        wba       = weighted_ba(career_ba, season_ba, l20_ba)
        xba       = savant_xba.get(pid) if pid else None

        # Batter blend — see BLEND_* constants. BPP is a FALLBACK only; it
        # never enters the weighted blend. The 4-case selector also produces
        # the "Data Source" string written into the output row.
        if xba is not None and wba is not None:
            xba_phit    = ba_to_hit_prob(xba, rec["ab"])
            ba_phit     = ba_to_hit_prob(wba, rec["ab"])
            batter_phit = BLEND_SAVANT * xba_phit + BLEND_BA * ba_phit
            data_source = "Savant+MLB"
        elif xba is not None:
            batter_phit = ba_to_hit_prob(xba, rec["ab"])
            data_source = "Savant only"
        elif wba is not None:
            batter_phit = ba_to_hit_prob(wba, rec["ab"])
            data_source = "MLB only"
        else:
            batter_phit = rec["p_hit"]
            data_source = "BPP fallback"
        source_counter[data_source] += 1

        # Opposing-pitcher adjustment. Primary source is the Savant
        # probable-pitcher matchup (xBA-allowed + K% vs THIS roster); when
        # that sample is thin (< PITCHER_MATCHUP_PA) or missing we fall back
        # to the BPP file's season hits-allowed rate. The selected
        # combined_pitcher_phit feeds the PITCHER_ADJUST blend.
        opp_team     = (rec.get("opp") or "").upper()
        pitcher_rec  = pitchers.get(opp_team, {})
        pitcher_pid  = pitcher_rec.get("player_id", "")
        pitcher_name = pitcher_rec.get("name", "")

        p_xba_vs_roster = ""   # → col "P xBA vs Roster" (blank unless matchup used)
        p_k_pct         = ""   # → col "P K%"
        p_sample_pa     = ""   # → col "P Sample PA"
        combined_pitcher_phit = None
        pitcher_source        = ""   # "matchup" | "season" | ""

        matchup_data: dict = {}
        if pitcher_pid:
            try:
                matchup_data = get_pitcher_vs_roster(int(pitcher_pid), pitcher_name) or {}
            except (TypeError, ValueError):
                matchup_data = {}

        sv_pa  = matchup_data.get("sv_vs_pa") or 0
        sv_xba = matchup_data.get("sv_vs_xba")
        sv_k   = matchup_data.get("sv_vs_k_pct")

        if sv_pa >= PITCHER_MATCHUP_PA and sv_xba is not None:
            pitcher_phit = ba_to_hit_prob(sv_xba, rec["ab"])
            k_penalty    = 1.0 - (sv_k / 100.0) if sv_k is not None else 1.0
            combined_pitcher_phit = pitcher_phit * k_penalty
            pitcher_source  = "matchup"
            p_xba_vs_roster = round(sv_xba, 3)
            p_k_pct         = round(sv_k, 1) if sv_k is not None else ""
            p_sample_pa     = sv_pa
        else:
            # Thin/no matchup — fall back to BPP season hits-allowed rate.
            season_rate = pitcher_rec.get("hits_allowed_rate")
            if season_rate is not None:
                combined_pitcher_phit = ba_to_hit_prob(season_rate, rec["ab"])
                pitcher_source = "season"

        if combined_pitcher_phit is not None:
            final_phit = (
                (1 - PITCHER_ADJUST) * batter_phit
                + PITCHER_ADJUST * combined_pitcher_phit
            )
            pitcher_used += 1
            pitcher_source_counter[pitcher_source] += 1
        else:
            final_phit = batter_phit

        if p["side"] == "Over":
            model_p = model_over_prob(final_phit, rec["ab"], p["line"])
        elif p["side"] == "Under":
            model_p = 1 - model_over_prob(final_phit, rec["ab"], p["line"])
        else:
            continue
        imp  = implied_prob(p["price"])
        edge = model_p - imp
        edge_pct = edge * 100

        bs_raw = rec.get("stand", "")
        ph_raw = pitcher_rec.get("hand", "")
        bs = (bs_raw or "").strip().upper()[:1] or "?"
        ph = (ph_raw or "").strip().upper()[:1] or "?"
        bs_ph_counter[(bs, ph)] += 1

        # Bet sizing + forward MC over the final pitcher-adjusted per-AB rate.
        kelly        = kelly_units(model_p, p["price"])
        final_per_ab = per_ab_hit_prob(final_phit, rec["ab"])
        mc           = monte_carlo_win_prob(
            final_per_ab, rec["ab"], p["line"], p["side"],
        )

        out.append([
            today,                                        # 0  Date
            format_game_time(p.get("game_time", "")),     # 1  Game Time
            p["player"],                                  # 2  Player
            rec.get("team", ""),                          # 3  Team
            p["game"],                                    # 4  Game
            p["line"],                                    # 5  Line
            p["side"],                                    # 6  Side
            p["bookmaker"],                               # 7  Bookmaker
            p["price"],                                   # 8  Odds
            round(imp * 100, 2),                          # 9  Implied %
            round(rec["p_hit"] * 100, 2),                 # 10 BPP HitProb %
            int(round(rec["ab"])),                        # 11 BPP AtBats
            round(career_ba, 3) if career_ba is not None else "",      # 12 Career BA
            round(season_ba, 3) if season_ba is not None else "",      # 13 Season BA
            round(l20_ba,    3) if l20_ba    is not None else "",      # 14 L20 BA
            round(wba,       3) if wba       is not None else "",      # 15 Weighted BA
            round(xba, 3) if xba is not None else "",     # 16 Savant xBA
            p_xba_vs_roster,                              # 17 P xBA vs Roster
            round(final_phit * 100, 2),                   # 18 Model Prob (pitcher-adjusted)
            round(model_p   * 100, 2),                    # 19 Model Prob %
            round(edge_pct, 2),                           # 20 Edge %
            "✅" if edge >= EDGE_THRESHOLD else "",        # 21 Edge Flag
            0.0,    # 22 Composite (filled by percentile post-pass)
            "",     # 23 Rating (assigned below by rank)
            kelly,                                        # 24 Kelly Units
            round(mc * 100, 2) if mc is not None else "", # 25 MC Win%
            data_source,                                  # 26 Data Source
            p_sample_pa,                                  # 27 P Sample PA
            p_k_pct,                                      # 28 P K%
            confirmed_map.get(normalize_name(rec.get("name", "")), ""),  # 29 Confirmed (extra)
        ])
    if misses:
        print(f"   ⚠️  {misses} prop quotes had no BPP match")

    if stats_total:
        print(f"  📊 MLB Stats API: {stats_hits}/{stats_total} players returned BA components")
    if source_counter:
        print(
            f"  📊 Data Source distribution — "
            f"Savant+MLB: {source_counter['Savant+MLB']}, "
            f"Savant only: {source_counter['Savant only']}, "
            f"MLB only: {source_counter['MLB only']}, "
            f"BPP fallback: {source_counter['BPP fallback']}"
        )
    if pitcher_used:
        m = pitcher_source_counter.get("matchup", 0)
        s = pitcher_source_counter.get("season", 0)
        print(
            f"  📊 Pitcher adjustment applied to {pitcher_used} rows "
            f"(Savant matchup: {m}, BPP season hits-allowed: {s})"
        )
    if bs_ph_counter:
        rr = bs_ph_counter.get(("R", "R"), 0)
        rl = bs_ph_counter.get(("R", "L"), 0)
        lr = bs_ph_counter.get(("L", "R"), 0)
        ll = bs_ph_counter.get(("L", "L"), 0)
        s_count = sum(v for (b, _), v in bs_ph_counter.items() if b == "S")
        print(
            f"  📊 Matchup distribution: "
            f"R vs R: {rr}, R vs L: {rl}, L vs R: {lr}, L vs L: {ll}, S: {s_count}"
        )

    # Composite from percentile RANKS of today's pool: 60% hit prob, 40% edge.
    # Both BPP HitProbability (0.24–0.76 band) and Edge % cluster too tightly
    # for raw-scaled scores to produce real spread; ranking against the day's
    # distribution forces the full 0–100 range.
    #   r[10] = BPP HitProb %   r[20] = Edge %   r[22] = Composite Score (target)
    if out:
        sorted_hit  = sorted(r[10] for r in out)
        sorted_edge = sorted(r[20] for r in out)
        n           = len(out)
        for r in out:
            hit_pr  = 100.0 * bisect_right(sorted_hit,  r[10]) / n
            edge_pr = 100.0 * bisect_right(sorted_edge, r[20]) / n
            base    = composite_score(hit_pr, edge_pr)
            # Week-1 calibration boosts (r[5] = Line, r[6] = Side).
            adj = 0
            if r[6] == "Under":
                adj += 10
            if r[5] == 1.5:
                adj += 5
            if r[6] == "Over" and r[5] == 0.5:
                adj -= 5
            r[22]   = round(min(100.0, base + adj), 2)

    # Assign ratings by RANK in today's composite-score distribution.
    # ELITE = top 10%, STRONG = next 15% (10–25%), LEAN = next 35% (25–60%),
    # bottom 40% dropped. Composite is at column index 22, rating at 23.
    if out:
        out.sort(key=lambda r: r[22], reverse=True)
        n = len(out)
        elite_end  = max(1, int(round(n * 0.10)))
        strong_end = elite_end  + max(0, int(round(n * 0.15)))
        lean_end   = strong_end + max(0, int(round(n * 0.35)))

        scores = [r[22] for r in out]
        print(
            f"  📊 Composite distribution — n={n} "
            f"min={scores[-1]:.2f} median={scores[n // 2]:.2f} max={scores[0]:.2f}"
        )
        print(
            f"  🎯 Rank cutoffs — ELITE: top {elite_end} | "
            f"STRONG: next {strong_end - elite_end} | "
            f"LEAN: next {lean_end - strong_end} | "
            f"dropping {n - lean_end}"
        )

        for i, r in enumerate(out):
            if   i < elite_end:  r[23] = "ELITE"
            elif i < strong_end: r[23] = "STRONG"
            elif i < lean_end:   r[23] = "LEAN"
            # else: r[23] stays "" — caller filters per tab.

    return out


# ── BEST BETS ──────────────────────────────────────────────────
def build_best_bets(rows):
    """Distill the full prop list into one row per ELITE/STRONG player.

    For each player we keep the prop with the highest Edge % across all
    bookmakers and lines, then sort the result by Game Time ASC and
    Composite Score DESC.

    Column indices used (against current HEADERS):
      1=Game Time, 2=Player, 3=Team, 5=Line, 6=Side,
      7=Bookmaker, 8=Odds, 10=BPP HitProb %, 16=Savant xBA, 17=P xBA vs Roster,
      19=Model Prob %, 20=Edge %, 22=Composite, 23=Rating,
      24=Kelly Units, 25=MC Win%, 26=Data Source,
      27=P Sample PA, 28=P K%, 29=Confirmed (extra)
    """
    by_player = {}
    for r in rows:
        if r[23] not in ("ELITE", "STRONG"):
            continue
        # Drop zero-Kelly and negative-edge rows — they aren't real plays.
        if not isinstance(r[24], (int, float)) or r[24] <= 0:
            continue
        if not isinstance(r[20], (int, float)) or r[20] <= 0:
            continue
        key = r[2]
        if key not in by_player or r[20] > by_player[key][20]:
            by_player[key] = r

    bests = []
    for r in by_player.values():
        bests.append([
            r[1],   # Game Time
            r[2],   # Player
            r[3],   # Team
            r[5],   # Line
            r[6],   # Side
            r[8],   # Best Odds
            r[7],   # Best Book
            r[10],  # BPP Hit%
            r[16],  # Savant xBA
            r[17],  # P xBA vs Roster
            r[27],  # P Sample PA
            r[28],  # P K%
            r[26],  # Data Source
            r[19],  # Model Prob%
            r[20],  # Edge%
            r[23],  # Rating
            r[22],  # Composite Score
            r[24],  # Kelly Units
            r[25],  # MC Win%
            r[29] if len(r) > 29 else "",  # Confirmed
        ])
    # Sort: Game Time ASC, then Composite Score DESC. Game Time is "HH:MM"
    # (24-hour ET) so lexicographic sort is chronological. Composite is at
    # column 16 of the Best Bets row (BEST_HEADERS, after P Sample PA / P K% /
    # Data Source).
    bests.sort(key=lambda b: (b[0], -b[16]))
    return bests


def write_best_bets(sheet, best_rows):
    end_col = _col_letter(len(BEST_HEADERS))
    print(f"  🔧 BEST_HEADERS ({len(BEST_HEADERS)} cols): {BEST_HEADERS}")
    ws = _get_or_create_ws(sheet, BEST_TAB, cols=max(30, len(BEST_HEADERS)))
    ws.clear()
    print(f"  🔧 {BEST_TAB}: writing header → A1:{end_col}1")
    ws.update(
        range_name=f"A1:{end_col}1",
        values=[BEST_HEADERS],
        value_input_option="USER_ENTERED",
    )

    # Dedup by (Player, normLine, Side) keeping the highest-Edge% row.
    # Date is implied as "today" across the tab, so it's omitted from the
    # key. BEST_HEADERS layout: Player=1, Line=3, Side=4, Edge%=14.
    best_for_key: dict = {}
    key_order: list = []
    ungroupable: list = []
    for b in best_rows:
        if len(b) <= 14:
            ungroupable.append(b)
            continue
        key = (b[1], _normalize_line(b[3]), b[4])
        new_edge = b[14] if isinstance(b[14], (int, float)) else -float("inf")
        if key not in best_for_key:
            best_for_key[key] = b
            key_order.append(key)
        else:
            existing = best_for_key[key]
            existing_edge = existing[14] if isinstance(existing[14], (int, float)) else -float("inf")
            if new_edge > existing_edge:
                best_for_key[key] = b
    deduped = ungroupable + [best_for_key[k] for k in key_order]
    dupes   = len(best_rows) - len(deduped)
    if dupes:
        print(
            f"  🧹 {BEST_TAB}: collapsed {dupes} duplicate (player, line, side) "
            f"row(s) — kept highest Edge%"
        )

    if deduped:
        end_row = 1 + len(deduped)
        print(f"  🔧 {BEST_TAB}: writing {len(deduped)} rows → A2:{end_col}{end_row}")
        ws.update(
            range_name=f"A2:{end_col}{end_row}",
            values=deduped,
            value_input_option="USER_ENTERED",
        )
    print(f"  ✅ {BEST_TAB}: header + {len(deduped)} rows")


# ── PROPS TRACKER ──────────────────────────────────────────────
def build_tracker_rows(rows):
    """ELITE/STRONG rows mapped to TRACKER_HEADERS shape.

    Deduplicated to one row per (date, player, line) — across all bookmakers
    and sides we keep the row with the highest Edge %. Result and Notes
    columns are blank for manual fill-in after the game.

    Source HEADERS index → tracker column:
      0  Date          → Date
      1  Game Time     → Game Time
      2  Player        → Player
      3  Team          → Team
      4  Game          → Game
      5  Line          → Line
      6  Side          → Side
      10 BPP HitProb % → BPP Hit%
      19 Model Prob %  → Model Prob%
      20 Edge %        → Edge%
      22 Composite     → Composite Score
      23 Rating        → Rating
      24 Kelly Units   → Kelly Units
      25 MC Win%       → MC Win%
      ""               → Result   (manual fill-in)
      ""               → Notes    (manual fill-in)
    """
    by_key = {}
    for r in rows:
        if r[23] not in ("ELITE", "STRONG"):
            continue
        key = (r[0], r[2], r[5])  # (date, player, line)
        if key not in by_key or r[20] > by_key[key][20]:
            by_key[key] = r

    out = []
    for r in by_key.values():
        out.append([
            r[0], r[1], r[2], r[3], r[4], r[5], r[6],
            r[10], r[19], r[20],
            r[22], r[23],
            r[24], r[25],
            "", "",
        ])
    return out


def build_manual_tracker_rows(rows):
    """Same selection as Best Bets (ELITE/STRONG, one row per player, highest
    Edge %), mapped to MANUAL_TRACKER_HEADERS shape with Result/Notes blank.
    Same filters as Best Bets: Kelly > 0 and Edge% > 0.
    """
    by_player = {}
    for r in rows:
        if r[23] not in ("ELITE", "STRONG"):
            continue
        if not isinstance(r[24], (int, float)) or r[24] <= 0:
            continue
        if not isinstance(r[20], (int, float)) or r[20] <= 0:
            continue
        key = r[2]
        if key not in by_player or r[20] > by_player[key][20]:
            by_player[key] = r

    out = []
    for r in by_player.values():
        out.append([
            r[0],   # Date
            r[1],   # Game Time
            r[2],   # Player
            r[3],   # Team
            r[4],   # Game
            r[5],   # Line
            r[6],   # Side
            r[10],  # BPP Hit%
            r[19],  # Model Prob%
            r[20],  # Edge%
            r[24],  # Kelly Units
            r[25],  # MC Win%
            r[22],  # Composite Score
            r[23],  # Rating
            r[29] if len(r) > 29 else "",  # Confirmed
            "",     # Result
            "",     # Notes
        ])
    return out


# ── SHEET WRITES ───────────────────────────────────────────────
def _get_or_create_ws(sheet, title, rows=1000, cols=30):
    try:
        return sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return sheet.add_worksheet(title=title, rows=rows, cols=max(cols, len(HEADERS)))


def _col_letter(n: int) -> str:
    """1-indexed column number → letter (1='A', 26='Z', 27='AA')."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def write_today(sheet, rows):
    end_col = _col_letter(len(HEADERS))
    print(f"  🔧 HEADERS ({len(HEADERS)} cols): {HEADERS}")
    ws = _get_or_create_ws(sheet, TODAY_TAB)
    ws.clear()
    print(f"  🔧 {TODAY_TAB}: writing header → A1:{end_col}1")
    ws.update(
        range_name=f"A1:{end_col}1",
        values=[HEADERS],
        value_input_option="USER_ENTERED",
    )

    # Dedup by (Date, Player, normLine, Side) keeping the highest-Edge% row
    # per group. This collapses multi-bookmaker rows to the single best line
    # per prop, per the all-tabs rule "one row per (Date, Player, Line, Side),
    # highest Edge% wins". HEADERS row layout: Date=0, Player=2, Line=5,
    # Side=6, Edge%=20.
    best_for_key: dict = {}
    key_order: list = []
    ungroupable: list = []
    for r in rows:
        if len(r) <= 20:
            ungroupable.append(r)
            continue
        key = (r[0], r[2], _normalize_line(r[5]), r[6])
        new_edge = r[20] if isinstance(r[20], (int, float)) else -float("inf")
        if key not in best_for_key:
            best_for_key[key] = r
            key_order.append(key)
        else:
            existing = best_for_key[key]
            existing_edge = existing[20] if isinstance(existing[20], (int, float)) else -float("inf")
            if new_edge > existing_edge:
                best_for_key[key] = r
    deduped = ungroupable + [best_for_key[k] for k in key_order]
    dupes   = len(rows) - len(deduped)
    if dupes:
        print(
            f"  🧹 {TODAY_TAB}: collapsed {dupes} duplicate (date, player, line, side) "
            f"row(s) — kept highest Edge%"
        )

    if deduped:
        end_row = 1 + len(deduped)
        # Rows may carry an extra trailing Confirmed column (r[29]) that
        # Best Bets / 📊 Tracker read but Props Today does not. Slice to HEADERS.
        sliced = [r[:len(HEADERS)] for r in deduped]
        print(f"  🔧 {TODAY_TAB}: writing {len(sliced)} rows → A2:{end_col}{end_row}")
        ws.update(
            range_name=f"A2:{end_col}{end_row}",
            values=sliced,
            value_input_option="USER_ENTERED",
        )
    print(f"  ✅ {TODAY_TAB}: header + {len(deduped)} rows")


def append_tracker(sheet, tracker_rows):
    """Append ELITE/STRONG rows to Props Tracker. NEVER WIPES — under any
    circumstances. Existing rows and column positions are preserved.

    Schema policy: any TRACKER_HEADERS columns that aren't already present
    in the sheet's row 1 are appended to the RIGHT of existing columns.
    Data rows are then aligned to the SHEET'S actual column order — values
    are placed under matching column names regardless of where the column
    sits in the sheet.
    """
    n_cols  = len(TRACKER_HEADERS)
    end_col = _col_letter(n_cols)
    print(f"  🔧 TRACKER_HEADERS ({n_cols} cols): {TRACKER_HEADERS}")

    try:
        ws = sheet.worksheet(TRACKER_TAB)
        first_row = ws.row_values(1)
    except gspread.WorksheetNotFound:
        print(f"  ➕ {TRACKER_TAB}: creating new tab with header")
        ws = sheet.add_worksheet(title=TRACKER_TAB, rows=10000, cols=max(30, n_cols))
        ws.update(
            range_name=f"A1:{end_col}1",
            values=[TRACKER_HEADERS],
            value_input_option="USER_ENTERED",
        )
        first_row = list(TRACKER_HEADERS)

    if not first_row:
        ws.update(
            range_name=f"A1:{end_col}1",
            values=[TRACKER_HEADERS],
            value_input_option="USER_ENTERED",
        )
        first_row = list(TRACKER_HEADERS)
        print(f"  ➕ {TRACKER_TAB}: tab was empty — wrote header")
    else:
        existing_set = set(first_row)
        missing      = [c for c in TRACKER_HEADERS if c not in existing_set]
        if missing:
            start_col   = _col_letter(len(first_row) + 1)
            new_end_col = _col_letter(len(first_row) + len(missing))
            ws.update(
                range_name=f"{start_col}1:{new_end_col}1",
                values=[missing],
                value_input_option="USER_ENTERED",
            )
            first_row = first_row + missing
            print(f"  ➕ {TRACKER_TAB}: added {len(missing)} new column(s) to right → {missing}")
        else:
            print(f"  🔧 {TRACKER_TAB}: all TRACKER_HEADERS columns present in row 1")

    if not tracker_rows:
        print(f"  ⚠️  {TRACKER_TAB}: no ELITE/STRONG rows to append")
        return

    # Align each row's values (positionally indexed against TRACKER_HEADERS)
    # to whatever column order the sheet currently has. Unknown sheet columns
    # get blank values so we never overwrite cells we don't own.
    aligned = []
    for row in tracker_rows:
        d = dict(zip(TRACKER_HEADERS, row))
        aligned.append([d.get(col, "") for col in first_row])

    ws.append_rows(aligned, value_input_option="USER_ENTERED")
    print(f"  ✅ {TRACKER_TAB}: appended {len(aligned)} ELITE/STRONG rows")


def _row_edge(row, edge_idx) -> float:
    """Read Edge% as float from a sheet row; -inf if missing/unparseable so
    such rows never win a dedup tie."""
    if edge_idx is None or edge_idx >= len(row):
        return -float("inf")
    try:
        return float(row[edge_idx])
    except (TypeError, ValueError):
        return -float("inf")


def _cleanup_manual_tracker_duplicates(sheet, ws, all_values, first_row):
    """Remove pre-existing duplicate rows in 📊 Tracker.

    Groups by (Date, Player, normLine, Side); within each group the row
    with the highest Edge% wins (later sheet row wins ties). Non-empty
    Result/Notes from any loser are promoted onto the winner's row first so
    manual W/L/P fills aren't lost. Losers are deleted via batch
    deleteDimension in descending row order.

    Returns the refreshed all_values after deletions (or the original if no
    cleanup was needed). This only fires once per run — the in-memory dedup
    below then guards future appends.
    """
    try:
        date_idx   = first_row.index("Date")
        player_idx = first_row.index("Player")
        line_idx   = first_row.index("Line")
        side_idx   = first_row.index("Side")
    except ValueError:
        return all_values

    edge_idx   = first_row.index("Edge%")  if "Edge%"  in first_row else None
    result_idx = first_row.index("Result") if "Result" in first_row else None
    notes_idx  = first_row.index("Notes")  if "Notes"  in first_row else None
    if edge_idx is None:
        # No way to choose a winner — skip cleanup.
        return all_values

    key_max_idx = max(date_idx, player_idx, line_idx, side_idx, edge_idx)
    groups: dict = {}  # key → list of (sheet_row_1based, row_values_copy, edge_float)
    for i, row in enumerate(all_values[1:], start=2):
        if key_max_idx >= len(row):
            continue
        groups.setdefault(_dedup_key(
            row[date_idx], row[player_idx], row[line_idx], row[side_idx],
        ), []).append((i, list(row), _row_edge(row, edge_idx)))

    cell_updates   = []
    rows_to_delete = []
    for key, members in groups.items():
        if len(members) < 2:
            continue
        # Winner: highest edge, tie-break by later row index (latest run).
        winner = max(members, key=lambda m: (m[2], m[0]))
        winner_idx, winner_row, _ = winner
        losers = [m for m in members if m[0] != winner_idx]

        # Promote a non-empty Result/Notes from any loser onto the winner row.
        for col_idx in (result_idx, notes_idx):
            if col_idx is None:
                continue
            winner_val = winner_row[col_idx] if col_idx < len(winner_row) else ""
            if winner_val.strip():
                continue
            for _, lr, _ in losers:
                if col_idx < len(lr) and lr[col_idx].strip():
                    col_letter = _col_letter(col_idx + 1)
                    cell_updates.append({
                        "range":  f"{col_letter}{winner_idx}",
                        "values": [[lr[col_idx]]],
                    })
                    break

        rows_to_delete.extend(m[0] for m in losers)

    if not rows_to_delete:
        return all_values

    if cell_updates:
        ws.batch_update(cell_updates, value_input_option="USER_ENTERED")
        print(
            f"  🔄 {MANUAL_TRACKER_TAB}: promoted {len(cell_updates)} Result/Notes "
            f"cell(s) onto dedup winners"
        )

    rows_to_delete.sort(reverse=True)
    try:
        sheet_id = ws.id
    except AttributeError:
        sheet_id = ws._properties.get("sheetId")
    delete_requests = [
        {
            "deleteDimension": {
                "range": {
                    "sheetId":    sheet_id,
                    "dimension":  "ROWS",
                    "startIndex": ri - 1,
                    "endIndex":   ri,
                }
            }
        }
        for ri in rows_to_delete
    ]
    sheet.batch_update({"requests": delete_requests})
    print(f"  🧹 {MANUAL_TRACKER_TAB}: deleted {len(rows_to_delete)} duplicate row(s)")
    return ws.get_all_values()


def write_manual_tracker(sheet, manual_rows):
    """Append Best-Bets-equivalent rows to '📊 Tracker', deduped against
    existing rows by (Date, Player, Line, Side). NEVER clears the tab.
    Pre-existing duplicate rows from older runs are actively removed first
    (highest Edge% wins, Result/Notes preserved); the in-memory dedup then
    keeps the tab clean going forward.
    """
    n_cols  = len(MANUAL_TRACKER_HEADERS)
    end_col = _col_letter(n_cols)
    print(f"  🔧 MANUAL_TRACKER_HEADERS ({n_cols} cols): {MANUAL_TRACKER_HEADERS}")

    try:
        ws = sheet.worksheet(MANUAL_TRACKER_TAB)
        all_values = ws.get_all_values()
        first_row  = all_values[0] if all_values else []
    except gspread.WorksheetNotFound:
        print(f"  ➕ {MANUAL_TRACKER_TAB}: creating new tab with header")
        ws = sheet.add_worksheet(
            title=MANUAL_TRACKER_TAB,
            rows=10000,
            cols=max(30, n_cols),
        )
        ws.update(
            range_name=f"A1:{end_col}1",
            values=[MANUAL_TRACKER_HEADERS],
            value_input_option="USER_ENTERED",
        )
        first_row  = list(MANUAL_TRACKER_HEADERS)
        all_values = [first_row]

    if not first_row:
        ws.update(
            range_name=f"A1:{end_col}1",
            values=[MANUAL_TRACKER_HEADERS],
            value_input_option="USER_ENTERED",
        )
        first_row  = list(MANUAL_TRACKER_HEADERS)
        all_values = [first_row]
        print(f"  ➕ {MANUAL_TRACKER_TAB}: tab was empty — wrote header")
    else:
        existing_set = set(first_row)
        missing = [c for c in MANUAL_TRACKER_HEADERS if c not in existing_set]
        if missing:
            start_col   = _col_letter(len(first_row) + 1)
            new_end_col = _col_letter(len(first_row) + len(missing))
            ws.update(
                range_name=f"{start_col}1:{new_end_col}1",
                values=[missing],
                value_input_option="USER_ENTERED",
            )
            first_row = first_row + missing
            print(f"  ➕ {MANUAL_TRACKER_TAB}: added {len(missing)} new column(s) → {missing}")

    # Actively delete pre-existing duplicates before doing anything else.
    # This is what cleans up duplicate rows that accumulated from older runs
    # — without it, the dedup below only prevents *future* duplicates.
    all_values = _cleanup_manual_tracker_duplicates(sheet, ws, all_values, first_row)

    if not manual_rows:
        print(f"  ⚠️  {MANUAL_TRACKER_TAB}: 0 candidate rows from Best Bets")
        return

    # Build (Date, Player, normLine, Side) dedup key set from existing rows.
    # _normalize_line bridges the Sheets-vs-Python representation gap (e.g.
    # whole-number lines come back as "1" from the sheet but the build_rows
    # value is float 1.0 → str() = "1.0"); without normalization the dedup
    # key never matches and duplicates accumulate.
    try:
        date_idx   = first_row.index("Date")
        player_idx = first_row.index("Player")
        line_idx   = first_row.index("Line")
        side_idx   = first_row.index("Side")
    except ValueError as exc:
        print(f"  ⚠️  {MANUAL_TRACKER_TAB}: missing key column ({exc}); skipping dedup")
        date_idx = player_idx = line_idx = side_idx = None

    existing_keys = set()
    if None not in (date_idx, player_idx, line_idx, side_idx):
        for row in all_values[1:]:
            # Bounds-safe per-cell read: gspread trims trailing empty cells,
            # so a short existing row must still contribute its key — otherwise
            # a matching new row slips past the check and gets appended as a
            # duplicate.
            existing_keys.add(_dedup_key(
                row[date_idx]   if date_idx   < len(row) else "",
                row[player_idx] if player_idx < len(row) else "",
                row[line_idx]   if line_idx   < len(row) else "",
                row[side_idx]   if side_idx   < len(row) else "",
            ))

    # Filter to rows whose (Date, Player, Line, Side) isn't already logged.
    new_rows, skipped = [], 0
    for row in manual_rows:
        d   = dict(zip(MANUAL_TRACKER_HEADERS, row))
        key = _dedup_key(
            d.get("Date", ""), d.get("Player", ""),
            d.get("Line", ""), d.get("Side", ""),
        )
        if key in existing_keys:
            skipped += 1
            continue
        existing_keys.add(key)
        new_rows.append(row)

    if not new_rows:
        print(f"  ⚠️  {MANUAL_TRACKER_TAB}: 0 new rows ({skipped} candidate(s) already logged)")
        return

    # Align each row to the sheet's column order.
    aligned = []
    for row in new_rows:
        d = dict(zip(MANUAL_TRACKER_HEADERS, row))
        aligned.append([d.get(col, "") for col in first_row])

    ws.append_rows(aligned, value_input_option="USER_ENTERED")
    print(
        f"  ✅ {MANUAL_TRACKER_TAB}: appended {len(aligned)} new row(s) "
        f"({skipped} skipped as dupes)"
    )


# ── MAIN ───────────────────────────────────────────────────────
def main():
    bpp      = load_bpp_batters()
    pitchers = load_bpp_pitchers()

    # Confirmed lineup status — YES/PENDING players are kept (PENDING means
    # their team's lineup isn't posted yet, e.g. early-morning run). NO players
    # are dropped entirely: their team's lineup IS posted but they're benched.
    teams_with_lineups, confirmed_names, confirmed_ids = fetch_confirmed_lineups()
    confirmed_map = {}  # normalized BPP name → 'YES' | 'PENDING'
    new_bpp = {}
    yes_count = pending_count = no_count = 0
    for key, rec in bpp.items():
        status = determine_confirmed_status(
            rec, teams_with_lineups, confirmed_names, confirmed_ids,
        )
        if status == "NO":
            no_count += 1
            continue
        if status == "YES":
            yes_count += 1
        else:
            pending_count += 1
        confirmed_map[key] = status
        new_bpp[key]       = rec
    bpp = new_bpp
    print(
        f"  ✅ Confirmed status — YES: {yes_count} | "
        f"PENDING: {pending_count} | NO (excluded): {no_count}"
    )

    # Baseball Savant xBA (batters) — feeds the batter blend. The opposing-
    # pitcher adjustment (PITCHER_ADJUST=0.40) uses the per-roster matchup
    # signal from the Savant probable-pitchers page (fetchers.savant) when the
    # sample is solid, else the BPP file's season hits-allowed rate.
    season     = datetime.date.today().year
    savant_xba = fetch_savant_xba(season)
    load_savant_pitcher_data()  # pre-warm the probable-pitchers cache once

    props = get_batter_hits_props()
    rows  = build_rows(
        props, bpp, pitchers,
        confirmed_map=confirmed_map,
        savant_xba=savant_xba,
    )

    # Per-tab views drawn from the same row pool. Row indices:
    # Edge Flag = 21, Composite = 22, Rating = 23.
    #  - Today   = ELITE/STRONG/LEAN (cleared and rewritten daily, full HEADERS)
    #  - Bests   = ELITE/STRONG      (cleared and rewritten daily, BEST_HEADERS)
    #  - Tracker = ELITE/STRONG      (append-only history, TRACKER_HEADERS,
    #                                 deduped to one row per player+line)
    #  - 📊 Tracker = manual-entry tab with header only (created if missing)
    rated         = [r for r in rows if r[23] in ("ELITE", "STRONG", "LEAN")]
    bests         = build_best_bets(rated)
    tracker_rows  = build_tracker_rows(rows)
    manual_rows   = build_manual_tracker_rows(rows)

    sheet = get_sheet()
    write_today(sheet, rated)
    append_tracker(sheet, tracker_rows)
    write_best_bets(sheet, bests)
    write_manual_tracker(sheet, manual_rows)

    edges  = sum(1 for r in rows if r[21])
    elite  = sum(1 for r in rated if r[23] == "ELITE")
    strong = sum(1 for r in rated if r[23] == "STRONG")
    lean   = sum(1 for r in rated if r[23] == "LEAN")
    print(
        f"\n🎯 Done — {len(rows)} priced props, "
        f"{edges} flagged edges >= {int(EDGE_THRESHOLD*100)}% | "
        f"ELITE: {elite}, STRONG: {strong}, LEAN: {lean} | "
        f"Best Bets: {len(bests)} | Props Tracker append: {len(tracker_rows)}"
    )


if __name__ == "__main__":
    main()
