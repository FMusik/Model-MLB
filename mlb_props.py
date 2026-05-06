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

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


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
BLEND_BPP         = 0.70   # weight on BPP HitProbability in final blend
BLEND_BA          = 0.30   # weight on BA-derived hit prob in final blend

TODAY_TAB         = "Props Today"
TRACKER_TAB       = "Props Tracker"
BEST_TAB          = "🎯 Best Bets"

HEADERS = [
    "Date", "Game Time", "Player", "Team", "Game", "Line", "Side",
    "Bookmaker", "Odds", "Implied %",
    "BPP HitProb %", "BPP AtBats",
    "Career BA", "Season BA", "L20 BA", "Weighted BA", "Model Prob",
    "Matchup",
    "Model Prob %", "Edge %", "Edge Flag",
    "Composite Score", "Rating",
]

BEST_HEADERS = [
    "Game Time", "Player", "Team", "Matchup", "Line", "Side",
    "Best Odds", "Best Book", "BPP Hit%", "Model Prob%", "Edge%",
    "Rating", "Composite Score",
]

TRACKER_HEADERS = [
    "Date", "Game Time", "Player", "Team", "Game", "Line", "Side",
    "Best Odds", "Best Book",
    "BPP Hit%", "Model Prob%", "Edge%",
    "Composite Score", "Rating", "Result", "Notes",
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


def load_bpp_pitcher_hands() -> dict:
    """Map team-abbreviation -> starting pitcher's throwing hand (R/L).

    Joined later on the batter's Opponent so we know which arm each batter
    is facing. Returns {} silently if the file or columns are missing — the
    matchup component will then default to neutral.
    """
    if not os.path.exists(PITCHERS_FILE):
        print(f"  ⚠️  {PITCHERS_FILE} not found — matchup score will default to neutral")
        return {}
    df = pd.read_excel(PITCHERS_FILE, engine="openpyxl")
    cols = {c.lower(): c for c in df.columns}
    team_col = cols.get("team")
    hand_col = cols.get("pitcherhand") or cols.get("throws") or cols.get("hand")
    if not (team_col and hand_col):
        print(f"  ⚠️  pitchers file missing Team/PitcherHand. Have: {list(df.columns)}")
        return {}
    out = {}
    for _, row in df.iterrows():
        team = str(row[team_col]).strip().upper()
        hand = str(row[hand_col]).strip().upper()
        if team and hand and team not in out:
            out[team] = hand[0]  # first char only ('R'/'L')
    print(f"  ✅ BPP pitcher hands loaded: {len(out)} teams")
    sample = list(out.items())[:5]
    print(f"  🔧 First 5 pitcher_hands entries: {sample}")
    return out


# ── CONFIRMED LINEUPS ──────────────────────────────────────────
def fetch_confirmed_lineups():
    """Pull today's confirmed lineups from the MLB Stats API.

    Returns (names: set[str], ids: set[str]). Both empty means no lineups
    have been posted yet (early-morning run) — callers should fall back to
    using every BPP batter with a warning.
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
            return set(), set()
        data = r.json()
    except Exception as e:
        print(f"  ⚠️  Lineups fetch error: {e}")
        return set(), set()

    names, ids = set(), set()
    games_with_lineups = 0
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            lineups = game.get("lineups") or {}
            had = False
            for side_key in ("homePlayers", "awayPlayers"):
                for player in lineups.get(side_key) or []:
                    nm  = player.get("fullName") or ""
                    pid = player.get("id")
                    if nm:
                        names.add(normalize_name(nm))
                        had = True
                    if pid is not None:
                        ids.add(str(pid))
            if had:
                games_with_lineups += 1
    print(f"  ✅ Confirmed lineups loaded: {games_with_lineups} games, {len(names)} batters")
    return names, ids


def filter_to_confirmed(bpp: dict, names: set, ids: set) -> dict:
    """Return only the BPP batters whose name or PlayerId appears in lineups."""
    if not names and not ids:
        return bpp
    return {
        key: rec
        for key, rec in bpp.items()
        if key in names or (rec.get("player_id") and rec["player_id"] in ids)
    }


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
            avg = splits[0].get("stat", {}).get("avg")
            try: out["season"] = float(avg)
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
def build_rows(props, bpp, pitcher_hands):
    today = datetime.date.today().isoformat()
    out = []
    misses = 0
    bs_ph_counter = Counter()
    stats_session = requests.Session()
    stats_cache   = {}
    stats_hits    = 0  # players with at least one BA component
    stats_total   = 0  # unique players we tried to fetch
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

        # Blend: 70% BPP + 30% BA-derived. Falls back to 100% BPP if no BA.
        if wba is not None:
            ba_phit = ba_to_hit_prob(wba, rec["ab"])
            blended = BLEND_BPP * rec["p_hit"] + BLEND_BA * ba_phit
        else:
            blended = rec["p_hit"]

        if p["side"] == "Over":
            model_p = model_over_prob(blended, rec["ab"], p["line"])
        elif p["side"] == "Under":
            model_p = 1 - model_over_prob(blended, rec["ab"], p["line"])
        else:
            continue
        imp  = implied_prob(p["price"])
        edge = model_p - imp
        edge_pct = edge * 100

        bs_raw = rec.get("stand", "")
        ph_raw = pitcher_hands.get(rec.get("opp", ""), "")
        bs = (bs_raw or "").strip().upper()[:1] or "?"
        ph = (ph_raw or "").strip().upper()[:1] or "?"
        bs_ph_counter[(bs, ph)] += 1
        matchup_label = f"{bs} vs {ph}"

        out.append([
            today,
            format_game_time(p.get("game_time", "")),
            p["player"],
            rec.get("team", ""),
            p["game"],
            p["line"],
            p["side"],
            p["bookmaker"],
            p["price"],
            round(imp * 100, 2),
            round(rec["p_hit"] * 100, 2),
            int(round(rec["ab"])),
            round(career_ba, 3) if career_ba is not None else "",
            round(season_ba, 3) if season_ba is not None else "",
            round(l20_ba,    3) if l20_ba    is not None else "",
            round(wba,       3) if wba       is not None else "",
            round(blended * 100, 2),
            matchup_label,
            round(model_p * 100, 2),
            round(edge_pct, 2),
            "✅" if edge >= EDGE_THRESHOLD else "",
            0.0,   # composite placeholder, filled by the percentile post-pass
            "",    # rating assigned below by rank
        ])
    if misses:
        print(f"   ⚠️  {misses} prop quotes had no BPP match")

    if stats_total:
        print(f"  📊 MLB Stats API: {stats_hits}/{stats_total} players returned BA components")
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
    #   r[10] = BPP HitProb %   r[19] = Edge %   r[21] = Composite Score (target)
    if out:
        sorted_hit  = sorted(r[10] for r in out)
        sorted_edge = sorted(r[19] for r in out)
        n           = len(out)
        for r in out:
            hit_pr  = 100.0 * bisect_right(sorted_hit,  r[10]) / n
            edge_pr = 100.0 * bisect_right(sorted_edge, r[19]) / n
            r[21]   = round(composite_score(hit_pr, edge_pr), 2)

    # Assign ratings by RANK in today's composite-score distribution.
    # ELITE = top 10%, STRONG = next 15% (10–25%), LEAN = next 35% (25–60%),
    # bottom 40% dropped. Composite is at column index 21, rating at 22.
    if out:
        out.sort(key=lambda r: r[21], reverse=True)
        n = len(out)
        elite_end  = max(1, int(round(n * 0.10)))
        strong_end = elite_end  + max(0, int(round(n * 0.15)))
        lean_end   = strong_end + max(0, int(round(n * 0.35)))

        scores = [r[21] for r in out]
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

        # Tag each row with its rating; rows below the 40th percentile keep
        # rating="" so the tracker can still log them when edge-flagged.
        for i, r in enumerate(out):
            if   i < elite_end:  r[22] = "ELITE"
            elif i < strong_end: r[22] = "STRONG"
            elif i < lean_end:   r[22] = "LEAN"
            # else: r[22] stays "" — caller filters per tab.

    return out


# ── BEST BETS ──────────────────────────────────────────────────
def build_best_bets(rows):
    """Distill the full prop list into one row per ELITE/STRONG player.

    For each player we keep the prop with the highest Edge % across all
    bookmakers and lines, then sort the result by Game Time ASC and
    Composite Score DESC.

    Column indices used (against current HEADERS):
      1=Game Time, 2=Player, 3=Team, 5=Line, 6=Side,
      7=Bookmaker, 8=Odds, 10=BPP HitProb %, 17=Matchup,
      18=Model Prob %, 19=Edge %, 21=Composite, 22=Rating
    """
    by_player = {}
    for r in rows:
        if r[22] not in ("ELITE", "STRONG"):
            continue
        key = r[2]
        if key not in by_player or r[19] > by_player[key][19]:
            by_player[key] = r

    bests = []
    for r in by_player.values():
        bests.append([
            r[1],   # Game Time
            r[2],   # Player
            r[3],   # Team
            r[17],  # Matchup
            r[5],   # Line
            r[6],   # Side
            r[8],   # Best Odds
            r[7],   # Best Book
            r[10],  # BPP Hit%
            r[18],  # Model Prob%
            r[19],  # Edge%
            r[22],  # Rating
            r[21],  # Composite Score
        ])
    # Sort: Game Time ASC, then Composite Score DESC. Game Time is "HH:MM"
    # (24-hour ET) so lexicographic sort is chronological.
    bests.sort(key=lambda b: (b[0], -b[12]))
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
    if best_rows:
        end_row = 1 + len(best_rows)
        print(f"  🔧 {BEST_TAB}: writing {len(best_rows)} rows → A2:{end_col}{end_row}")
        ws.update(
            range_name=f"A2:{end_col}{end_row}",
            values=best_rows,
            value_input_option="USER_ENTERED",
        )
    print(f"  ✅ {BEST_TAB}: header + {len(best_rows)} rows")


# ── PROPS TRACKER ──────────────────────────────────────────────
def build_tracker_rows(rows):
    """ELITE/STRONG rows mapped to TRACKER_HEADERS shape.

    Result and Notes columns are blank — filled in manually after the game.

    Source HEADERS index → tracker column:
      0  Date          → Date
      1  Game Time     → Game Time
      2  Player        → Player
      3  Team          → Team
      4  Game          → Game
      5  Line          → Line
      6  Side          → Side
      8  Odds          → Best Odds
      7  Bookmaker     → Best Book
      10 BPP HitProb % → BPP Hit%
      18 Model Prob %  → Model Prob%
      19 Edge %        → Edge%
      21 Composite     → Composite Score
      22 Rating        → Rating
      ""               → Result
      ""               → Notes
    """
    out = []
    for r in rows:
        if r[22] not in ("ELITE", "STRONG"):
            continue
        out.append([
            r[0], r[1], r[2], r[3], r[4], r[5], r[6],
            r[8], r[7],
            r[10], r[18], r[19],
            r[21], r[22],
            "", "",
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
    if rows:
        end_row = 1 + len(rows)
        print(f"  🔧 {TODAY_TAB}: writing {len(rows)} rows → A2:{end_col}{end_row}")
        ws.update(
            range_name=f"A2:{end_col}{end_row}",
            values=rows,
            value_input_option="USER_ENTERED",
        )
    print(f"  ✅ {TODAY_TAB}: header + {len(rows)} rows")


def append_tracker(sheet, tracker_rows):
    """Append ELITE/STRONG rows to the Props Tracker. NEVER clears the tab.

    The tracker is the historical log — every play we'd care to track
    (ELITE/STRONG only) accumulates here. The "Result" and "Notes" columns
    are left blank for manual fill-in after the game.

    Schema migration policy: never wipe historical data. If the existing
    header is a strict prefix of TRACKER_HEADERS, the new column names get
    appended to the right of row 1 and existing rows keep their layout.
    Other mismatches log a warning and append new rows in the current
    schema — old rows are preserved as-is, possibly misaligned.
    """
    n_cols = len(TRACKER_HEADERS)
    end_col = _col_letter(n_cols)
    print(f"  🔧 TRACKER_HEADERS ({n_cols} cols): {TRACKER_HEADERS}")

    try:
        ws = sheet.worksheet(TRACKER_TAB)
        first_row = ws.row_values(1)

        if not first_row:
            ws.update(
                range_name=f"A1:{end_col}1",
                values=[TRACKER_HEADERS],
                value_input_option="USER_ENTERED",
            )
            print(f"  ➕ {TRACKER_TAB}: tab was empty — wrote header")
        elif first_row == TRACKER_HEADERS:
            print(f"  🔧 {TRACKER_TAB}: header matches, append-only")
        elif (
            len(first_row) < n_cols
            and TRACKER_HEADERS[: len(first_row)] == first_row
        ):
            new_cols  = TRACKER_HEADERS[len(first_row):]
            start_col = _col_letter(len(first_row) + 1)
            ws.update(
                range_name=f"{start_col}1:{end_col}1",
                values=[new_cols],
                value_input_option="USER_ENTERED",
            )
            print(f"  ➕ {TRACKER_TAB}: extended header with {len(new_cols)} new column(s) → {new_cols}")
        else:
            print(
                f"  ⚠️  {TRACKER_TAB}: existing header doesn't match TRACKER_HEADERS and isn't a "
                f"prefix. History preserved as-is; new rows append in current schema and may be "
                f"misaligned against the old header. Existing first row: {first_row}"
            )
    except gspread.WorksheetNotFound:
        print(f"  ➕ {TRACKER_TAB}: creating new tab with header")
        ws = sheet.add_worksheet(title=TRACKER_TAB, rows=10000, cols=max(30, n_cols))
        ws.update(
            range_name=f"A1:{end_col}1",
            values=[TRACKER_HEADERS],
            value_input_option="USER_ENTERED",
        )

    if not tracker_rows:
        print(f"  ⚠️  {TRACKER_TAB}: no ELITE/STRONG rows to append")
        return

    ws.append_rows(tracker_rows, value_input_option="USER_ENTERED")
    print(f"  ✅ {TRACKER_TAB}: appended {len(tracker_rows)} ELITE/STRONG rows")


# ── MAIN ───────────────────────────────────────────────────────
def main():
    bpp           = load_bpp_batters()
    pitcher_hands = load_bpp_pitcher_hands()

    # Confirmed lineup filter — early runs (no lineups yet) keep all BPP
    # batters; later runs trim to only players in posted lineups.
    confirmed_names, confirmed_ids = fetch_confirmed_lineups()
    if confirmed_names or confirmed_ids:
        before = len(bpp)
        bpp = filter_to_confirmed(bpp, confirmed_names, confirmed_ids)
        print(f"  ✅ Confirmed lineup filter: {len(bpp)}/{before} BPP batters in posted lineups")
    else:
        print(f"  ⚠️  No confirmed lineups yet — using all {len(bpp)} BPP batters")

    props = get_batter_hits_props()
    rows  = build_rows(props, bpp, pitcher_hands)

    # Per-tab views drawn from the same row pool:
    #  - Today   = ELITE/STRONG/LEAN (cleared and rewritten daily, full HEADERS)
    #  - Bests   = ELITE/STRONG      (cleared and rewritten daily, BEST_HEADERS)
    #  - Tracker = ELITE/STRONG      (append-only history, TRACKER_HEADERS)
    rated         = [r for r in rows if r[22] in ("ELITE", "STRONG", "LEAN")]
    bests         = build_best_bets(rated)
    tracker_rows  = build_tracker_rows(rows)

    sheet = get_sheet()
    write_today(sheet, rated)
    append_tracker(sheet, tracker_rows)
    write_best_bets(sheet, bests)

    edges  = sum(1 for r in rows if r[20])
    elite  = sum(1 for r in rated if r[-1] == "ELITE")
    strong = sum(1 for r in rated if r[-1] == "STRONG")
    lean   = sum(1 for r in rated if r[-1] == "LEAN")
    print(
        f"\n🎯 Done — {len(rows)} priced props, "
        f"{edges} flagged edges >= {int(EDGE_THRESHOLD*100)}% | "
        f"ELITE: {elite}, STRONG: {strong}, LEAN: {lean} | "
        f"Best Bets: {len(bests)} | Tracker append: {len(tracker_rows)}"
    )


if __name__ == "__main__":
    main()
