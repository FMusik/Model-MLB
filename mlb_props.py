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

TODAY_TAB         = "Props Today"
TRACKER_TAB       = "Props Tracker"

HEADERS = [
    "Date", "Player", "Team", "Game", "Line", "Side",
    "Bookmaker", "Odds", "Implied %",
    "BPP HitProb %", "BPP AtBats", "Matchup",
    "Model Prob %", "Edge %", "Edge Flag",
    "Matchup Score", "Composite Score", "Rating",
]


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
        out[normalize_name(raw_name)] = {
            "name":  raw_name,
            "team":  str(row[team_col]).strip().upper() if team_col else "",
            "opp":   str(row[opp_col]).strip().upper() if opp_col else "",
            "side":  str(row[side_col]).strip() if side_col else "",
            "stand": str(row[stand_col]).strip().upper() if stand_col else "",
            "p_hit": p_hit,
            "ab":    ab,
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
    return out


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
                    })
    print(f"   ✅ {len(rows)} prop quotes pulled (requests remaining: {remaining})")
    return rows


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
def matchup_score(batter_stand: str, pitcher_hand: str) -> float:
    """0-100 batter-vs-pitcher handedness score.

    Same-hand matchup (R vs R, L vs L) favors the pitcher → low score.
    Cross-hand (L vs R, R vs L) favors the batter → high score.
    Switch hitters (S) always get the platoon side → cross-hand value.
    Unknown / missing hands → neutral 50.
    """
    bs = (batter_stand or "").strip().upper()[:1]
    ph = (pitcher_hand or "").strip().upper()[:1]
    if ph not in ("L", "R") or bs not in ("L", "R", "S"):
        return 50.0
    if bs == "S":
        return 65.0
    return 35.0 if bs == ph else 65.0


def edge_to_score(edge_pct: float) -> float:
    """Map edge %  to a 0-100 score. 0% edge = 50, ±20% saturates to 100/0."""
    return max(0.0, min(100.0, 50.0 + edge_pct * 2.5))


def composite_score(hit_prob: float, edge_pct: float, matchup: float) -> float:
    """50% BPP HitProb, 30% edge vs market, 20% pitcher matchup."""
    return 0.5 * (hit_prob * 100.0) + 0.3 * edge_to_score(edge_pct) + 0.2 * matchup


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
    for p in props:
        rec = match_player(p["player"], bpp)
        if not rec:
            misses += 1
            continue
        if p["side"] == "Over":
            model_p = model_over_prob(rec["p_hit"], rec["ab"], p["line"])
        elif p["side"] == "Under":
            model_p = 1 - model_over_prob(rec["p_hit"], rec["ab"], p["line"])
        else:
            continue
        imp  = implied_prob(p["price"])
        edge = model_p - imp
        edge_pct = edge * 100

        bs = rec.get("stand", "")
        ph = pitcher_hands.get(rec.get("opp", ""), "")
        matchup_label = f"{bs or '?'} vs {ph or '?'}"
        matchup       = matchup_score(bs, ph)
        composite     = composite_score(rec["p_hit"], edge_pct, matchup)

        out.append([
            today,
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
            matchup_label,
            round(model_p * 100, 2),
            round(edge_pct, 2),
            "✅" if edge >= EDGE_THRESHOLD else "",
            round(matchup, 2),
            round(composite, 2),
            "",  # rating assigned below by percentile
        ])
    if misses:
        print(f"   ⚠️  {misses} prop quotes had no BPP match")

    # Assign ratings by percentile of today's composite-score distribution.
    # ELITE = top 10%, STRONG = next 20%, LEAN = next 30%, rest dropped.
    if out:
        scores = sorted(r[16] for r in out)
        cut_elite  = _percentile(scores, 90)
        cut_strong = _percentile(scores, 70)
        cut_lean   = _percentile(scores, 40)
        print(
            f"  🎯 Rating cutoffs — ELITE >= {cut_elite:.2f}, "
            f"STRONG >= {cut_strong:.2f}, LEAN >= {cut_lean:.2f}"
        )
        kept = []
        for r in out:
            s = r[16]
            if s >= cut_elite:
                r[17] = "ELITE"
            elif s >= cut_strong:
                r[17] = "STRONG"
            elif s >= cut_lean:
                r[17] = "LEAN"
            else:
                continue
            kept.append(r)
        out = kept

    out.sort(key=lambda r: r[16], reverse=True)  # sort by Composite Score
    return out


# ── SHEET WRITES ───────────────────────────────────────────────
def _get_or_create_ws(sheet, title, rows=1000, cols=20):
    try:
        return sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return sheet.add_worksheet(title=title, rows=rows, cols=cols)


def write_today(sheet, rows):
    ws = _get_or_create_ws(sheet, TODAY_TAB)
    ws.clear()
    ws.update(range_name="A1", values=[HEADERS] + rows, value_input_option="USER_ENTERED")
    print(f"  ✅ {TODAY_TAB}: {len(rows)} rows")


def append_tracker(sheet, rows):
    if not rows:
        return
    ws = _get_or_create_ws(sheet, TRACKER_TAB, rows=10000)
    if not ws.get_all_values():
        ws.append_row(HEADERS, value_input_option="USER_ENTERED")
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"  ✅ {TRACKER_TAB}: appended {len(rows)} rows")


# ── MAIN ───────────────────────────────────────────────────────
def main():
    bpp           = load_bpp_batters()
    pitcher_hands = load_bpp_pitcher_hands()
    props         = get_batter_hits_props()
    rows          = build_rows(props, bpp, pitcher_hands)

    sheet = get_sheet()
    write_today(sheet, rows)
    append_tracker(sheet, rows)

    edges  = sum(1 for r in rows if r[14])
    elite  = sum(1 for r in rows if r[-1] == "ELITE")
    strong = sum(1 for r in rows if r[-1] == "STRONG")
    lean   = sum(1 for r in rows if r[-1] == "LEAN")
    print(
        f"\n🎯 Done — {len(rows)} priced props, "
        f"{edges} flagged edges >= {int(EDGE_THRESHOLD*100)}% | "
        f"ELITE: {elite}, STRONG: {strong}, LEAN: {lean}"
    )


if __name__ == "__main__":
    main()
