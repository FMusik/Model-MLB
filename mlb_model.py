"""
MLB Betting Model — Enhanced v2
================================
NEW VARIABLES ADDED 2026-04-25:
  1. Umpire tendencies  — reads ump_factors.json, adjusts run proj + signal score
  2. Pitcher days rest  — short rest penalty, rust penalty for 7+ days
  3. Schedule fatigue   — back-to-back, long road trip, heavy schedule
  4. Platoon advantage  — lineup handedness vs pitcher, adjusts run proj
  5. Bullpen 3-day rolling workload — more accurate than just yesterday
  6. Series context     — game 1 = under lean, game 3+ = over lean
  7. Travel / timezone  — west coast team playing early ET game penalty

All new variables:
  - Feed into project_runs_allowed() via multiplier factors
  - Feed into score_signal() via ump_adj and line_movement_adj points
  - Are stored in result dict and written to Sheets + Tracker
"""

import json
import math
import os
import random
import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
SHEET_NAME        = "MLB Daily Model"
CREDENTIALS_FILE  = "credentials.json"
EDGE_THRESHOLD    = 5.0
SEASON            = datetime.date.today().year
ODDS_API_KEY      = "c81ff126c5a86a502a0dea2fbb7f9b43"
MAX_WIN_PROB      = 0.65
MAX_RUN_DIFF      = 2.5

# OddsPapi — fallback odds + Pinnacle sharp signal
ODDSPAPI_KEY      = os.environ.get("ODDSPAPI_KEY", "")
ODDSPAPI_BASE     = "https://api.oddspapi.io/v4"
MLB_TOURNAMENT_ID = 109  # MLB tournament ID on OddsPapi

UMP_FACTORS_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ump_factors.json")

_current_sheet = None
_ump_data      = {}
_calibration   = {}
_calibration_loaded = False


# ─────────────────────────────────────────────
# SECTION 0 — UMP DATA
# ─────────────────────────────────────────────
def load_ump_data() -> dict:
    global _ump_data
    if _ump_data:
        return _ump_data
    try:
        with open(UMP_FACTORS_FILE, "r") as f:
            data = json.load(f)
        _ump_data = data.get("umps", {})
        print(f"  ✅ Ump data loaded: {len(_ump_data)} umpires")
        return _ump_data
    except Exception as e:
        print(f"  ⚠️  Could not load ump_factors.json: {e}")
        return {}

def get_ump_factor(ump_name: str) -> dict:
    """Return ump tendency dict. Falls back to DEFAULT."""
    umps = load_ump_data()
    if ump_name and ump_name in umps:
        return umps[ump_name]
    if ump_name:
        for name, data in umps.items():
            if name != "DEFAULT" and (name.lower() in ump_name.lower() or ump_name.lower() in name.lower()):
                return data
    return umps.get("DEFAULT", {
        "run_factor": 1.00, "k_factor": 1.00, "bb_factor": 1.00,
        "avg_runs": 8.9, "zone": "neutral", "notes": "Unknown ump"
    })

def get_ump_signal_adjustment(ump_name: str, bet_type: str) -> int:
    """
    Signal score points adjustment based on ump tendencies.
    bet_type: 'over', 'under', 'yrfi', 'nrfi', 'ml'
    Returns int: -8 to +8 points added to signal score.
    """
    try:
        with open(UMP_FACTORS_FILE, "r") as f:
            full_data = json.load(f)
        th = full_data.get("thresholds", {})
    except Exception:
        th = {
            "strong_over_lean": 1.07, "moderate_over_lean": 1.03,
            "strong_under_lean": 0.93, "moderate_under_lean": 0.97,
            "signal_boost_strong": 8, "signal_boost_moderate": 4,
            "signal_penalty_strong": -8, "signal_penalty_moderate": -4,
        }
    rf = get_ump_factor(ump_name).get("run_factor", 1.0)
    if bet_type in ("over", "yrfi"):
        if rf >= th.get("strong_over_lean", 1.07):   return th.get("signal_boost_strong", 8)
        elif rf >= th.get("moderate_over_lean", 1.03): return th.get("signal_boost_moderate", 4)
        elif rf <= th.get("strong_under_lean", 0.93):  return th.get("signal_penalty_strong", -8)
        elif rf <= th.get("moderate_under_lean", 0.97):return th.get("signal_penalty_moderate", -4)
    elif bet_type in ("under", "nrfi"):
        if rf <= th.get("strong_under_lean", 0.93):   return th.get("signal_boost_strong", 8)
        elif rf <= th.get("moderate_under_lean", 0.97):return th.get("signal_boost_moderate", 4)
        elif rf >= th.get("strong_over_lean", 1.07):   return th.get("signal_penalty_strong", -8)
        elif rf >= th.get("moderate_over_lean", 1.03): return th.get("signal_penalty_moderate", -4)
    return 0


# ─────────────────────────────────────────────
# SECTION 0B — NEW VARIABLE FUNCTIONS
# ─────────────────────────────────────────────

BASE = "https://statsapi.mlb.com/api/v1"

def api_get(endpoint: str, params: dict = {}) -> dict:
    r = requests.get(f"{BASE}{endpoint}", params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def today_str() -> str:
    return datetime.date.today().strftime("%Y-%m-%d")

def get_home_plate_ump(game_pk: int) -> str:
    """Pull home plate umpire from MLB boxscore API."""
    try:
        data = api_get(f"/game/{game_pk}/boxscore")
        for official in data.get("officials", []):
            if official.get("officialType", "").lower() == "home plate":
                return official.get("official", {}).get("fullName", "")
        officials = data.get("officials", [])
        if officials:
            return officials[0].get("official", {}).get("fullName", "")
        return ""
    except Exception:
        return ""

def get_pitcher_days_rest(pitcher_id: int) -> dict:
    """
    Days rest since last outing.
    rest_factor: <4d=0.92, 4d=0.97, 5d=1.00, 6d=1.01, 7+d=0.98
    """
    if not pitcher_id:
        return {"days_rest": 5, "rest_factor": 1.00, "rest_label": "Normal (5d)"}
    try:
        data   = api_get(f"/people/{pitcher_id}/stats",
                         {"stats": "gameLog", "group": "pitching", "season": SEASON, "sportId": 1})
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {"days_rest": 5, "rest_factor": 1.00, "rest_label": "No data"}
        last_date = None
        for s in reversed(splits):
            date_str = s.get("date", "")
            if date_str:
                try:
                    last_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                    break
                except Exception:
                    continue
        if not last_date:
            return {"days_rest": 5, "rest_factor": 1.00, "rest_label": "No data"}
        days_rest = (datetime.date.today() - last_date).days
        if days_rest < 4:
            factor, label = 0.92, f"⚠️ SHORT REST ({days_rest}d)"
        elif days_rest == 4:
            factor, label = 0.97, f"4 day rest"
        elif days_rest == 5:
            factor, label = 1.00, f"Normal (5d)"
        elif days_rest == 6:
            factor, label = 1.01, f"Extra rest (6d)"
        else:
            factor, label = 0.98, f"Rust ({days_rest}d)"
        return {"days_rest": days_rest, "rest_factor": factor, "rest_label": label}
    except Exception:
        return {"days_rest": 5, "rest_factor": 1.00, "rest_label": "Unknown"}

def get_team_schedule_spot(team_id: int) -> dict:
    """
    Back-to-back, road trip length, heavy schedule = fatigue penalty.
    fatigue_factor: 0.91 to 1.00
    """
    try:
        start = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        data  = api_get("/schedule",
                        {"sportId": 1, "teamId": team_id, "startDate": start,
                         "endDate": today_str(), "gameType": "R"})
        games_played = []
        for db in data.get("dates", []):
            for g in db.get("games", []):
                if g.get("status", {}).get("abstractGameState") == "Final":
                    home_id = g.get("teams", {}).get("home", {}).get("team", {}).get("id")
                    games_played.append({
                        "date":    db.get("date", ""),
                        "is_home": home_id == team_id
                    })
        if not games_played:
            return {"fatigue_factor": 1.00, "schedule_label": "Normal", "road_trip_days": 0}
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        played_yesterday = any(g["date"] == yesterday for g in games_played)
        road_trip_days = 0
        for g in reversed(games_played):
            if not g["is_home"]: road_trip_days += 1
            else: break
        fatigue_factor = 1.00; labels = []
        if played_yesterday:
            fatigue_factor -= 0.03; labels.append("B2B")
        if road_trip_days >= 7:
            fatigue_factor -= 0.04; labels.append(f"Long road trip ({road_trip_days}d)")
        elif road_trip_days >= 4:
            fatigue_factor -= 0.02; labels.append(f"Road trip ({road_trip_days}d)")
        if len(games_played) >= 7:
            fatigue_factor -= 0.02; labels.append("Heavy schedule")
        return {
            "fatigue_factor":    round(fatigue_factor, 3),
            "schedule_label":    " | ".join(labels) if labels else "Normal",
            "road_trip_days":    road_trip_days,
            "played_yesterday":  played_yesterday,
            "games_last_7":      len(games_played),
        }
    except Exception:
        return {"fatigue_factor": 1.00, "schedule_label": "Unknown", "road_trip_days": 0}

def get_bullpen_rolling_workload(team_id: int, days: int = 3) -> dict:
    """
    Total bullpen IP over last N days.
    bp_avail_factor: 0.88 (heavy) to 1.04 (well rested)
    """
    try:
        total_bp_ip = 0.0
        for d in range(1, days + 1):
            date = (datetime.date.today() - datetime.timedelta(days=d)).strftime("%Y-%m-%d")
            data = api_get("/schedule",
                           {"sportId": 1, "date": date, "teamId": team_id, "hydrate": "boxscore"})
            for db in data.get("dates", []):
                for g in db.get("games", []):
                    if g.get("status", {}).get("abstractGameState") != "Final":
                        continue
                    bs = g.get("boxscore", {})
                    for side in ["away", "home"]:
                        t = bs.get("teams", {}).get(side, {})
                        if t.get("team", {}).get("id") != team_id:
                            continue
                        for pid in t.get("pitchers", []):
                            p     = t.get("players", {}).get(f"ID{pid}", {})
                            stats = p.get("stats", {}).get("pitching", {})
                            ip    = float(stats.get("inningsPitched", 0) or 0)
                            gs    = int(stats.get("gamesStarted", 0) or 0)
                            if gs == 0 and ip > 0:
                                total_bp_ip += ip
        if total_bp_ip >= 15:
            workload, factor = "🚨 Heavily Used", 0.88
        elif total_bp_ip >= 10:
            workload, factor = "⚠️ Moderately Used", 0.94
        elif total_bp_ip >= 6:
            workload, factor = "➡️ Normal", 1.00
        else:
            workload, factor = "✅ Well Rested", 1.04
        return {
            "bp_rolling_ip":       round(total_bp_ip, 1),
            "bp_rolling_workload": workload,
            "bp_avail_factor":     factor,
        }
    except Exception:
        return {"bp_rolling_ip": 0, "bp_rolling_workload": "Unknown", "bp_avail_factor": 1.00}

def get_series_context(game_pk: int, away_team_id: int, home_team_id: int) -> dict:
    """
    Game 1 of series: fresh bullpens → under lean (0.97x)
    Game 3+: tired bullpens → over lean (1.03x)
    """
    try:
        data = api_get("/schedule",
                       {"sportId": 1, "teamId": home_team_id,
                        "startDate": (datetime.date.today() - datetime.timedelta(days=4)).strftime("%Y-%m-%d"),
                        "endDate": today_str(), "gameType": "R"})
        series_games = []
        for db in data.get("dates", []):
            for g in db.get("games", []):
                a = g.get("teams", {}).get("away", {}).get("team", {}).get("id")
                h = g.get("teams", {}).get("home", {}).get("team", {}).get("id")
                if set([a, h]) == set([away_team_id, home_team_id]):
                    series_games.append(g)
        game_num = 1
        for i, g in enumerate(series_games):
            if g.get("gamePk") == game_pk:
                game_num = i + 1
                break
        if game_num == 1:
            rf, label = 0.97, "Game 1 of series (fresh arms → under lean)"
        elif game_num == 2:
            rf, label = 1.00, "Game 2 of series (neutral)"
        else:
            rf, label = 1.03, f"Game {game_num} of series (tired arms → over lean)"
        return {"series_game_num": game_num, "series_run_factor": rf, "series_label": label}
    except Exception:
        return {"series_game_num": 1, "series_run_factor": 1.00, "series_label": "Unknown"}

TEAM_TIMEZONES = {
    "New York Yankees":"ET","New York Mets":"ET","Boston Red Sox":"ET",
    "Baltimore Orioles":"ET","Tampa Bay Rays":"ET","Toronto Blue Jays":"ET",
    "Philadelphia Phillies":"ET","Atlanta Braves":"ET","Washington Nationals":"ET",
    "Miami Marlins":"ET","Pittsburgh Pirates":"ET","Cincinnati Reds":"ET",
    "Cleveland Guardians":"ET","Detroit Tigers":"ET",
    "Chicago Cubs":"CT","Chicago White Sox":"CT","Milwaukee Brewers":"CT",
    "St. Louis Cardinals":"CT","Minnesota Twins":"CT","Kansas City Royals":"CT",
    "Houston Astros":"CT","Texas Rangers":"CT",
    "Colorado Rockies":"MT","Arizona Diamondbacks":"MT",
    "Los Angeles Dodgers":"PT","Los Angeles Angels":"PT",
    "San Francisco Giants":"PT","San Diego Padres":"PT",
    "Seattle Mariners":"PT","Oakland Athletics":"PT","Sacramento River Cats":"PT",
}
TZ_HOURS = {"ET": 0, "CT": 1, "MT": 2, "PT": 3}

def get_travel_factor(away_team: str, venue: str, game_time_str: str) -> dict:
    """
    Timezone crossings = fatigue for away team.
    3+ zones + early game = 0.94x
    """
    try:
        away_tz  = TZ_HOURS.get(TEAM_TIMEZONES.get(away_team, "CT"), 1)
        v = venue.lower()
        if any(c in v for c in ["new york","boston","baltimore","tampa","toronto",
                                  "philadelphia","atlanta","washington","miami",
                                  "pittsburgh","cincinnati","cleveland","detroit"]):
            venue_tz = 0
        elif any(c in v for c in ["chicago","milwaukee","st. louis","minnesota",
                                   "kansas city","houston","dallas","arlington"]):
            venue_tz = 1
        elif any(c in v for c in ["denver","phoenix"]):
            venue_tz = 2
        elif any(c in v for c in ["los angeles","san francisco","san diego",
                                   "seattle","oakland","sacramento"]):
            venue_tz = 3
        else:
            venue_tz = 1
        tz_diff = abs(away_tz - venue_tz)
        try:
            game_dt    = datetime.datetime.fromisoformat(game_time_str.replace("Z", "+00:00"))
            local_hour = (game_dt.hour - venue_tz) % 24
            early_game = local_hour < 13
        except Exception:
            early_game = False
        if tz_diff >= 3 and early_game:
            factor, label = 0.94, f"⚠️ Big TZ change ({tz_diff}hr) + early game"
        elif tz_diff >= 3:
            factor, label = 0.97, f"Big TZ change ({tz_diff}hr)"
        elif tz_diff >= 2:
            factor, label = 0.98, f"Moderate TZ change ({tz_diff}hr)"
        elif tz_diff >= 1:
            factor, label = 0.99, f"Minor TZ change ({tz_diff}hr)"
        else:
            factor, label = 1.00, "Same timezone"
        return {"travel_factor": factor, "travel_label": label, "tz_diff": tz_diff}
    except Exception:
        return {"travel_factor": 1.00, "travel_label": "Unknown", "tz_diff": 0}

def get_platoon_advantage(lineup: list, pitcher_hand: str) -> dict:
    """
    Favorable = R batter vs L pitcher, or L batter vs R pitcher.
    platoon_factor: 0.94 to 1.06 on run projection.
    """
    if not lineup or not pitcher_hand:
        return {"platoon_score": 0.0, "platoon_factor": 1.00, "platoon_label": "Unknown"}
    favorable = 0; unfavorable = 0; unknown = 0
    for batter in lineup[:9]:
        pid = batter.get("id") if isinstance(batter, dict) else None
        if not pid:
            unknown += 1; continue
        try:
            data = api_get(f"/people/{pid}")
            hand = data.get("people", [{}])[0].get("batSide", {}).get("code", "")
            if (hand == "R" and pitcher_hand == "L") or (hand == "L" and pitcher_hand == "R"):
                favorable += 1
            elif hand == "S":
                favorable += 0.5; unfavorable += 0.5
            elif hand in ("R", "L"):
                unfavorable += 1
            else:
                unknown += 1
        except Exception:
            unknown += 1
    total = favorable + unfavorable + unknown
    if total == 0:
        return {"platoon_score": 0.0, "platoon_factor": 1.00, "platoon_label": "No data"}
    platoon_score  = round((favorable - unfavorable) / max(total, 1), 2)
    platoon_factor = round(max(0.94, min(1.06, 1.0 + platoon_score * 0.06)), 3)
    if platoon_score >= 0.5:
        label = f"✅ Strong platoon adv ({int(favorable)}/{int(total)} favorable)"
    elif platoon_score >= 0.2:
        label = "Slight platoon advantage"
    elif platoon_score <= -0.5:
        label = f"⚠️ Platoon disadvantage ({int(unfavorable)}/{int(total)} unfavorable)"
    elif platoon_score <= -0.2:
        label = "Slight platoon disadvantage"
    else:
        label = "Neutral platoon"
    return {"platoon_score": platoon_score, "platoon_factor": platoon_factor, "platoon_label": label}

def get_line_movement_adj(game_key: str, current_odds: dict, snapshot: dict, bet_type: str) -> int:
    """
    Line moves WITH our signal = +8 pts. Against = -8 pts.
    """
    if not snapshot or not current_odds:
        return 0
    snap = snapshot.get(game_key, {})
    curr = current_odds.get(game_key, {})
    if not snap or not curr:
        return 0
    try:
        if bet_type in ("over",):
            ct = curr.get("total_line"); st = snap.get("total_line")
            if ct and st:
                move = float(ct) - float(st)
                if move >= 0.5:  return 8   # total rising = sharp over action
                if move <= -0.5: return -8
        elif bet_type in ("under",):
            ct = curr.get("total_line"); st = snap.get("total_line")
            if ct and st:
                move = float(ct) - float(st)
                if move <= -0.5: return 8   # total falling = sharp under action
                if move >= 0.5:  return -8
        elif bet_type == "away_ml":
            cm = curr.get("away_ml"); sm = snap.get("away_ml")
            if cm and sm:
                move = int(cm) - int(sm)
                if move <= -10: return 8    # ML shortening = sharp action
                if move >= 20:  return -8
        elif bet_type == "home_ml":
            cm = curr.get("home_ml"); sm = snap.get("home_ml")
            if cm and sm:
                move = int(cm) - int(sm)
                if move <= -10: return 8
                if move >= 20:  return -8
    except Exception:
        pass
    return 0


# ─────────────────────────────────────────────
# GOOGLE SHEETS AUTH
# ─────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_sheet(sheet_name: str):
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open(sheet_name)


# ─────────────────────────────────────────────
# ODDS API (unchanged from original)
# ─────────────────────────────────────────────
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SNAPSHOT_TAB  = "📡 Line Movement"

def get_mlb_odds() -> dict:
    print("\n📡 Fetching odds from The Odds API...")
    all_odds = {}
    market_calls = [
        ("h2h","Moneyline"),("spreads","Run Line"),
        ("totals","Game Total"),("team_totals","Team Totals"),
    ]
    for market, label in market_calls:
        try:
            params = {"apiKey":ODDS_API_KEY,"regions":"us","markets":market,
                      "oddsFormat":"american","dateFormat":"iso"}
            r    = requests.get(f"{ODDS_API_BASE}/sports/baseball_mlb/odds", params=params, timeout=15)
            data = r.json()
            if market == "h2h":
                print(f"   API requests remaining: {r.headers.get('x-requests-remaining','?')}")
            if isinstance(data, list):
                for game in data:
                    away = game.get("away_team",""); home = game.get("home_team","")
                    key  = f"{away} @ {home}"
                    if key not in all_odds:
                        all_odds[key] = {"away_team":away,"home_team":home,"game_time":game.get("commence_time","")}
                    books = game.get("bookmakers",[])
                    book  = next((b for b in books if b["key"]=="draftkings"),None)
                    if not book and books: book = books[0]
                    if not book: continue
                    for mkt in book.get("markets",[]):
                        mkt_key = mkt.get("key"); outcomes = mkt.get("outcomes",[])
                        if mkt_key == "h2h":
                            for o in outcomes:
                                if o["name"]==away: all_odds[key]["away_ml"]=int(o["price"])
                                elif o["name"]==home: all_odds[key]["home_ml"]=int(o["price"])
                        elif mkt_key == "spreads":
                            for o in outcomes:
                                if o["name"]==away: all_odds[key]["away_rl_odds"]=int(o["price"]); all_odds[key]["away_rl_line"]=o.get("point",-1.5)
                                elif o["name"]==home: all_odds[key]["home_rl_odds"]=int(o["price"])
                        elif mkt_key == "totals":
                            for o in outcomes:
                                if o["name"]=="Over": all_odds[key]["total_line"]=o.get("point"); all_odds[key]["over_odds"]=int(o["price"])
                                elif o["name"]=="Under": all_odds[key]["under_odds"]=int(o["price"])
                        elif mkt_key == "team_totals":
                            for o in outcomes:
                                team=o.get("description",""); nm=o.get("name",""); price=int(o["price"]); point=o.get("point")
                                if team==away:
                                    if nm=="Over": all_odds[key]["away_team_total"]=point; all_odds[key]["away_tt_over_odds"]=price
                                    else: all_odds[key]["away_tt_under_odds"]=price
                                elif team==home:
                                    if nm=="Over": all_odds[key]["home_team_total"]=point; all_odds[key]["home_tt_over_odds"]=price
                                    else: all_odds[key]["home_tt_under_odds"]=price
            print(f"   ✅ {label} fetched")
        except Exception as e:
            print(f"   ⚠️  Could not fetch {label}: {e}")
    try:
        params = {"apiKey":ODDS_API_KEY,"regions":"us","markets":"totals_h1,h2h_h1",
                  "oddsFormat":"american","dateFormat":"iso"}
        r    = requests.get(f"{ODDS_API_BASE}/sports/baseball_mlb/odds", params=params, timeout=15)
        data = r.json()
        if isinstance(data, list):
            for game in data:
                away=game.get("away_team",""); home=game.get("home_team",""); key=f"{away} @ {home}"
                if key not in all_odds: continue
                books=game.get("bookmakers",[]); book=next((b for b in books if b["key"]=="draftkings"),None)
                if not book and books: book=books[0]
                if not book: continue
                for mkt in book.get("markets",[]):
                    outcomes=mkt.get("outcomes",[]); mkt_key=mkt.get("key","")
                    if mkt_key=="totals_h1":
                        for o in outcomes:
                            pt=o.get("point")
                            if pt is None: continue
                            try: pt_f=float(pt)
                            except: continue
                            full_total=float(all_odds[key].get("total_line") or 9.0)
                            if pt_f>=full_total*0.70: continue
                            if o["name"]=="Over": all_odds[key]["mkt_f5_line"]=pt_f; all_odds[key]["f5_over_odds"]=int(o["price"])
                            elif o["name"]=="Under":
                                if all_odds[key].get("mkt_f5_line"): all_odds[key]["f5_under_odds"]=int(o["price"])
                    elif mkt_key=="h2h_h1":
                        for o in outcomes:
                            if o["name"]==away: all_odds[key]["f5_away_ml"]=int(o["price"])
                            elif o["name"]==home: all_odds[key]["f5_home_ml"]=int(o["price"])
        f5c=sum(1 for v in all_odds.values() if v.get("mkt_f5_line"))
        print(f"   ✅ F5 totals: {f5c} games")
    except Exception as e:
        print(f"   ⚠️  Could not fetch F5 odds: {e}")
    print(f"   ✅ Odds fetched for {len(all_odds)} games")
    return all_odds

def save_odds_snapshot_to_sheet(sheet, odds: dict, run_label: str) -> None:
    today = today_str()
    try:
        try: ws = sheet.worksheet(SNAPSHOT_TAB)
        except Exception:
            ws = sheet.add_worksheet(SNAPSHOT_TAB, rows=500, cols=20)
            ws.append_row(["Date","Run","Game","Away ML","Home ML","Total Line","Over Odds","Under Odds","Away TT","Home TT","Saved At"])
        all_vals = ws.get_all_values()
        for row in all_vals[1:]:
            if len(row)>=2 and row[0]==today and row[1]==run_label:
                print(f"  📡 Snapshot already saved — skipping"); return
        saved_at = datetime.datetime.now().strftime("%H:%M:%S")
        rows = [[today,run_label,game,o.get("away_ml",""),o.get("home_ml",""),
                 o.get("total_line",""),o.get("over_odds",""),o.get("under_odds",""),
                 o.get("away_team_total",""),o.get("home_team_total",""),saved_at]
                for game,o in odds.items()]
        if rows: ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"  📡 Snapshot saved: {len(rows)} games @ {run_label}")
    except Exception as e:
        print(f"  ⚠️  Snapshot error: {e}")

def load_odds_snapshot_from_sheet(sheet, compare_to: str="6AM") -> dict:
    today = today_str()
    try:
        ws=sheet.worksheet(SNAPSHOT_TAB); all_vals=ws.get_all_values()
        if len(all_vals)<2: return {}
        snapshot={}
        for row in all_vals[1:]:
            if len(row)<3 or row[0]!=today or row[1]!=compare_to: continue
            def sv(idx):
                try: return row[idx] if idx<len(row) and row[idx] else None
                except: return None
            snapshot[row[2]]={"away_ml":sv(3),"home_ml":sv(4),"total_line":sv(5),"over_odds":sv(6),"under_odds":sv(7)}
        if snapshot: print(f"  📡 Loaded {compare_to} snapshot: {len(snapshot)} games")
        return snapshot
    except Exception: return {}

def detect_line_movement(current_odds: dict, snapshot: dict) -> dict:
    if not snapshot: return {}
    alerts = {}
    for game, curr in current_odds.items():
        snap=snapshot.get(game,{}); game_alerts=[]
        if not snap: continue
        for side,label in [("away_ml","Away ML"),("home_ml","Home ML")]:
            c=curr.get(side); s=snap.get(side)
            if c and s:
                try:
                    move=int(c)-int(s)
                    if abs(move)>=20: game_alerts.append(f"{'📈' if move>0 else '📉'} {label} moved {move:+d} ⚡ SHARP MOVE")
                    elif abs(move)>=10: game_alerts.append(f"{'↗️' if move>0 else '↘️'} {label} moved {move:+d}")
                except: pass
        ct=curr.get("total_line"); st=snap.get("total_line")
        if ct and st:
            try:
                move=float(ct)-float(st)
                if abs(move)>=1.0: game_alerts.append(f"{'📈' if move>0 else '📉'} Total moved {move:+.1f} ⚡ SHARP MOVE")
                elif abs(move)>=0.5: game_alerts.append(f"{'↗️' if move>0 else '↘️'} Total moved {move:+.1f}")
            except: pass
        if game_alerts: alerts[game]=game_alerts
    return alerts

def print_line_movement_report(alerts: dict, compare_label: str="6AM") -> None:
    print(f"\n📡 LINE MOVEMENT (vs {compare_label})")
    if not alerts: print(f"  ✅ No significant movement\n"); return
    sharp_count=sum(1 for m in alerts.values() for x in m if "SHARP" in x)
    print(f"  🚨 {len(alerts)} games | {sharp_count} sharp moves")
    for game,moves in alerts.items():
        for m in moves: print(f"  {'⚡' if 'SHARP' in m else '  '} {game}: {m}")
    print()

def push_movement_to_sheet(sheet, alerts: dict, run_label: str, compare_label: str) -> None:
    saved_at=datetime.datetime.now().strftime("%H:%M:%S"); today=today_str()
    try:
        ws=sheet.worksheet(SNAPSHOT_TAB); rows=[]
        for game,moves in alerts.items():
            for move in moves:
                rows.append([today,f"MOVE_{run_label}",game,move,"⚡ SHARP" if "SHARP" in move else "↕️ Notable",compare_label,saved_at])
        if rows: ws.append_rows(rows,value_input_option="USER_ENTERED")
    except Exception: pass

def get_run_label() -> str:
    hour=datetime.datetime.now().hour
    return "6AM" if hour<9 else ("12PM" if hour<14 else "5PM")

def get_compare_label(run_label: str) -> str:
    return {"12PM":"6AM","5PM":"12PM"}.get(run_label)


# ─────────────────────────────────────────────────────────────
# ODDSPAPI — Fallback odds + Pinnacle sharp signal
# MLB tournamentId = 109 | sportId = 13
# ─────────────────────────────────────────────────────────────
def _to_american(decimal_odds: float) -> int:
    try:
        if decimal_odds >= 2.0: return round((decimal_odds - 1) * 100)
        else:                   return round(-100 / (decimal_odds - 1))
    except: return 0

def _american_to_prob_op(odds: int) -> float:
    if odds > 0: return 100 / (odds + 100)
    else:        return abs(odds) / (abs(odds) + 100)

def _fuzzy_match_game(op_key: str, all_odds: dict) -> str:
    op_parts = set(op_key.lower().replace(" @ "," ").split())
    for key in all_odds:
        ak_parts = set(key.lower().replace(" @ "," ").split())
        if len(op_parts & ak_parts) >= 2:
            return key
    return None

def _fetch_oddspapi_book(bookmaker: str) -> dict:
    if not ODDSPAPI_KEY:
        return {}
    try:
        r    = requests.get(f"{ODDSPAPI_BASE}/odds-by-tournaments",
                            params={"apiKey":ODDSPAPI_KEY,"bookmaker":bookmaker,
                                    "tournamentIds":MLB_TOURNAMENT_ID}, timeout=15)
        data = r.json()
        if not isinstance(data, list): return {}
        result = {}
        for event in data:
            home=event.get("homeTeam",{}).get("name",""); away=event.get("awayTeam",{}).get("name","")
            if not home or not away: continue
            key=f"{away} @ {home}"; markets=event.get("odds",{}); od={"away_team":away,"home_team":home}
            ml=markets.get("1x2") or markets.get("h2h") or markets.get("moneyline") or []
            for o in (ml if isinstance(ml,list) else [ml]):
                name=str(o.get("name","")).lower(); price=o.get("price") or o.get("odd")
                if not price: continue
                if "home" in name or home.lower() in name: od["home_ml"]=_to_american(float(price))
                elif "away" in name or away.lower() in name: od["away_ml"]=_to_american(float(price))
            tot=markets.get("totals") or markets.get("over_under") or []
            for o in (tot if isinstance(tot,list) else [tot]):
                name=str(o.get("name","")).lower(); price=o.get("price") or o.get("odd"); line=o.get("handicap") or o.get("line")
                if not price: continue
                if "over" in name: od["total_line"]=float(line) if line else None; od["over_odds"]=_to_american(float(price))
                elif "under" in name: od["under_odds"]=_to_american(float(price))
            rl=markets.get("asian_handicap") or markets.get("run_line") or markets.get("spread") or []
            for o in (rl if isinstance(rl,list) else [rl]):
                name=str(o.get("name","")).lower(); price=o.get("price") or o.get("odd"); line=o.get("handicap") or o.get("line")
                if not price or not line: continue
                if "home" in name or home.lower() in name: od["home_rl_odds"]=_to_american(float(price))
                elif "away" in name or away.lower() in name: od["away_rl_odds"]=_to_american(float(price)); od["away_rl_line"]=float(line)
            result[key]=od
        return result
    except Exception as e:
        print(f"  ⚠️  OddsPapi ({bookmaker}): {e}"); return {}

def get_oddspapi_fallback(all_odds: dict) -> dict:
    """
    Called after get_mlb_odds().
    1. Pulls Pinnacle → compares to DraftKings → flags sharp money differences
    2. Fills missing RL/totals from FanDuel/BetMGM as backup
    """
    if not ODDSPAPI_KEY:
        print("  ⚠️  ODDSPAPI_KEY not set — skipping")
        return all_odds

    print("\n📊 OddsPapi: Pinnacle comparison + gap fill...")

    # Pinnacle sharp signal
    pin_odds=_fetch_oddspapi_book("pinnacle"); sharp_count=0
    for op_key,op_data in pin_odds.items():
        mk=_fuzzy_match_game(op_key,all_odds)
        if not mk: continue
        signals=[]
        ph=op_data.get("home_ml"); dh=all_odds[mk].get("home_ml")
        if ph and dh:
            diff=_american_to_prob_op(ph)-_american_to_prob_op(dh)
            hn=all_odds[mk].get("home_team","Home"); an=all_odds[mk].get("away_team","Away")
            if diff>=0.04:   signals.append(f"📌 PIN sharp {hn} (Pin:{ph:+d} DK:{dh:+d})")
            elif diff<=-0.04: signals.append(f"📌 PIN sharp {an} (Pin:{ph:+d} DK:{dh:+d})")
        pt=op_data.get("total_line"); dt=all_odds[mk].get("total_line")
        if pt and dt:
            try:
                move=float(pt)-float(dt)
                if abs(move)>=0.5: signals.append(f"📌 PIN total {'▲' if move>0 else '▼'}{move:+.1f} vs DK")
            except: pass
        if signals: all_odds[mk]["pinnacle_signal"]=" | ".join(signals); sharp_count+=1
        if not all_odds[mk].get("away_ml") and op_data.get("away_ml"): all_odds[mk]["away_ml"]=op_data["away_ml"]
        if not all_odds[mk].get("home_ml") and op_data.get("home_ml"): all_odds[mk]["home_ml"]=op_data["home_ml"]

    print(f"  ✅ Pinnacle: {len(pin_odds)} games | {sharp_count} sharp signals")

    # Fill missing markets
    missing_rl=sum(1 for v in all_odds.values() if not v.get("away_rl_odds"))
    missing_tot=sum(1 for v in all_odds.values() if not v.get("total_line"))
    if missing_rl>0 or missing_tot>0:
        print(f"  Filling: {missing_rl} RL gaps | {missing_tot} total gaps")
        for book in ["fanduel","betmgm","draftkings"]:
            if missing_rl==0 and missing_tot==0: break
            for op_key,op_data in _fetch_oddspapi_book(book).items():
                mk=_fuzzy_match_game(op_key,all_odds)
                if not mk: continue
                if not all_odds[mk].get("away_rl_odds") and op_data.get("away_rl_odds"):
                    all_odds[mk]["away_rl_odds"]=op_data["away_rl_odds"]; all_odds[mk]["home_rl_odds"]=op_data.get("home_rl_odds")
                    all_odds[mk]["away_rl_line"]=op_data.get("away_rl_line",-1.5); missing_rl=max(0,missing_rl-1)
                if not all_odds[mk].get("total_line") and op_data.get("total_line"):
                    all_odds[mk]["total_line"]=op_data["total_line"]; all_odds[mk]["over_odds"]=op_data.get("over_odds",-110)
                    all_odds[mk]["under_odds"]=op_data.get("under_odds",-110); missing_tot=max(0,missing_tot-1)

    print(f"  ✅ OddsPapi fallback complete")
    return all_odds


def push_odds_to_input_tab(sheet, odds: dict) -> None:
    try:
        ws=sheet.worksheet(INPUT_TAB_NAME); rows=ws.get_all_values()
        header_row_idx=None
        for i,row in enumerate(rows):
            if row and row[0]=="Game": header_row_idx=i; break
        if header_row_idx is None: return
        headers=rows[header_row_idx]
        MANUAL_COLS=[h for h in headers if str(h).startswith("BP") or
                     any(h==c for c in ["Away ML Bet%","Away ML Money%","Home ML Bet%","Home ML Money%",
                                        "Over Bet%","Over Money%","Under Bet%","Under Money%",
                                        "Away Spread Bet%","Away Spread Money%","Home Spread Bet%","Home Spread Money%",
                                        "YRFI Odds","NRFI Odds"])]
        bp_snapshot={}
        for row in rows[header_row_idx+1:]:
            if not row or not row[0]: continue
            manual_data={}
            for col_name in MANUAL_COLS:
                try:
                    ci=headers.index(col_name); val=row[ci] if ci<len(row) else ""
                    if val: manual_data[col_name]=val
                except ValueError: pass
            if manual_data: bp_snapshot[row[0]]=manual_data
        for row_idx in range(len(rows), header_row_idx+1, -1):
            try: ws.delete_rows(row_idx)
            except: pass
        def col_idx(name):
            try: return headers.index(name)
            except: return None
        new_rows=[]
        for game_name,game_odds in odds.items():
            new_row=[""]*len(headers); new_row[0]=game_name
            col_map={"Away ML":game_odds.get("away_ml"),"Home ML":game_odds.get("home_ml"),
                     "Total Line":game_odds.get("total_line"),"Over Odds":game_odds.get("over_odds"),
                     "Under Odds":game_odds.get("under_odds"),"Away RL Odds":game_odds.get("away_rl_odds"),
                     "Home RL Odds":game_odds.get("home_rl_odds"),"F5 Total":game_odds.get("mkt_f5_line"),
                     "F5 Over Odds":game_odds.get("f5_over_odds"),"F5 Under Odds":game_odds.get("f5_under_odds"),
                     "F5 Away ML":game_odds.get("f5_away_ml"),"F5 Home ML":game_odds.get("f5_home_ml"),
                     "Away TT Line":game_odds.get("away_team_total"),"Away TT Over":game_odds.get("away_tt_over_odds"),
                     "Away TT Under":game_odds.get("away_tt_under_odds"),"Home TT Line":game_odds.get("home_team_total"),
                     "Home TT Over":game_odds.get("home_tt_over_odds"),"Home TT Under":game_odds.get("home_tt_under_odds")}
            for col_name,val in col_map.items():
                ci=col_idx(col_name)
                if ci is not None and val is not None: new_row[ci]=val
            saved_manual=bp_snapshot.get(game_name,{})
            if not saved_manual:
                for sg,sd in bp_snapshot.items():
                    pa=set(game_name.lower().replace(" @ "," ").split())
                    pb=set(sg.lower().replace(" @ "," ").split())
                    if len(pa&pb)>=2: saved_manual=sd; break
            for col_name,val in saved_manual.items():
                ci=col_idx(col_name)
                if ci is not None: new_row[ci]=val
            new_rows.append(new_row)
        if new_rows:
            ws.append_rows(new_rows,value_input_option="USER_ENTERED")
            print(f"  ✅ Input tab: {len(new_rows)} games refreshed")
    except Exception as e:
        print(f"  ⚠️  Input tab error: {e}")


# ─────────────────────────────────────────────
# SECTION 1 — TODAY'S GAMES (unchanged)
# ─────────────────────────────────────────────
def get_todays_games() -> list:
    data=api_get("/schedule",{"sportId":1,"date":today_str(),"hydrate":"probablePitcher,venue,weather,lineups"})
    games=[]; seen=set()
    for db in data.get("dates",[]):
        for g in db.get("games",[]):
            gid=g.get("gamePk")
            if gid and gid not in seen: seen.add(gid); games.append(g)
    print(f"✅ Found {len(games)} games today ({today_str()})")
    return games

STADIUM_COORDS = {
    "Coors Field":(39.7559,-104.9942),"Great American Ball Park":(39.0979,-84.5082),
    "Fenway Park":(42.3467,-71.0972),"Globe Life Field":(32.7473,-97.0825),
    "Yankee Stadium":(40.8296,-73.9262),"Oriole Park at Camden Yards":(39.2838,-76.6217),
    "Citizens Bank Park":(39.9061,-75.1665),"Wrigley Field":(41.9484,-87.6553),
    "Truist Park":(33.8908,-84.4678),"American Family Field":(43.0280,-87.9712),
    "Kauffman Stadium":(39.0517,-94.4803),"Progressive Field":(41.4962,-81.6852),
    "Nationals Park":(38.8730,-77.0074),"Target Field":(44.9817,-93.2781),
    "Rogers Centre":(43.6414,-79.3894),"Angel Stadium":(33.8003,-117.8827),
    "Comerica Park":(42.3390,-83.0485),"PNC Park":(40.4469,-80.0057),
    "Busch Stadium":(38.6226,-90.1928),"Guaranteed Rate Field":(41.8300,-87.6338),
    "Rate Field":(41.8300,-87.6338),"Daikin Park":(29.7572,-95.3555),
    "Minute Maid Park":(29.7572,-95.3555),"loanDepot park":(25.7781,-80.2197),
    "LoanDepot Park":(25.7781,-80.2197),"UNIQLO Field":(34.0739,-118.2400),
    "Dodger Stadium":(34.0739,-118.2400),"Chase Field":(33.4453,-112.0667),
    "Citi Field":(40.7571,-73.8458),"G.M. Steinbrenner Field":(27.9683,-82.5053),
    "Tropicana Field":(27.7683,-82.6534),"T-Mobile Park":(47.5914,-122.3325),
    "Oracle Park":(37.7786,-122.3893),"Petco Park":(32.7076,-117.1570),
    "Sutter Health Park":(38.5802,-121.4997),"Sahlen Field":(42.8867,-78.8784),
}

def get_stadium_coords(venue):
    if venue in STADIUM_COORDS: return STADIUM_COORDS[venue]
    v=venue.lower()
    for park,coords in STADIUM_COORDS.items():
        if park.lower() in v or v in park.lower(): return coords
    return None

def fetch_weather_for_venue(venue, game_time_str):
    coords=get_stadium_coords(venue)
    if not coords: return {}
    lat,lon=coords
    try:
        game_time=datetime.datetime.fromisoformat(game_time_str.replace("Z","+00:00")) if game_time_str else datetime.datetime.now(datetime.timezone.utc)
        date_str=game_time.strftime("%Y-%m-%d"); game_hour=game_time.hour
        url=(f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
             f"&hourly=temperature_2m,windspeed_10m,winddirection_10m,weathercode"
             f"&temperature_unit=fahrenheit&windspeed_unit=mph&timezone=auto&start_date={date_str}&end_date={date_str}")
        r=requests.get(url,timeout=10)
        if r.status_code!=200: return {}
        data=r.json(); h=data.get("hourly",{})
        times=h.get("time",[]); temps=h.get("temperature_2m",[]); winds=h.get("windspeed_10m",[])
        dirs=h.get("winddirection_10m",[]); codes=h.get("weathercode",[])
        best=0
        for i,t in enumerate(times):
            try:
                hr=int(t.split("T")[1].split(":")[0])
                if abs(hr-game_hour)<abs(int(times[best].split("T")[1].split(":")[0])-game_hour): best=i
            except: pass
        def d2c(deg):
            if deg is None: return ""
            return ["N","NE","E","SE","S","SW","W","NW"][int((deg+22.5)/45)%8]
        def c2cond(code):
            if code is None: return "Unknown"
            if code==0: return "Clear"
            if code in (1,2,3): return "Partly Cloudy"
            if code in range(51,68): return "Rain"
            if code in range(80,83): return "Showers"
            if code in range(95,100): return "Thunderstorm"
            return "Cloudy"
        temp=round(temps[best]) if best<len(temps) else None
        wind_spd=round(winds[best]) if best<len(winds) else None
        return {"temp":temp,"wind":f"{wind_spd} mph {d2c(dirs[best] if best<len(dirs) else None)}" if wind_spd else "N/A",
                "condition":c2cond(codes[best] if best<len(codes) else None),"source":"Open-Meteo ✅"}
    except: return {}

def parse_game_info(game):
    away=game["teams"]["away"]; home=game["teams"]["home"]
    awp=away.get("probablePitcher",{}); hwp=home.get("probablePitcher",{})
    venue=game.get("venue",{}).get("name","Unknown"); weather=game.get("weather",{}); game_time=game.get("gameDate","")
    mlb_temp=weather.get("temp","")
    if not mlb_temp or str(mlb_temp).strip()=="":
        live=fetch_weather_for_venue(venue,game_time); temp=live.get("temp","N/A"); wind=live.get("wind","N/A")
        condition=live.get("condition","N/A"); wx_source=live.get("source","N/A")
    else:
        temp=mlb_temp; wind=weather.get("wind","N/A"); condition=weather.get("condition","N/A"); wx_source="MLB API"
    return {
        "game_id":game["gamePk"],"game_time":game_time,"venue":venue,
        "away_team":away["team"]["name"],"home_team":home["team"]["name"],
        "away_team_id":away["team"]["id"],"home_team_id":home["team"]["id"],
        "away_pitcher":awp.get("fullName","TBD"),"away_pitcher_id":awp.get("id"),
        "home_pitcher":hwp.get("fullName","TBD"),"home_pitcher_id":hwp.get("id"),
        "weather_temp":temp,"weather_wind":wind,"weather_condition":condition,"weather_source":wx_source,
    }

def check_game_timing(game,info):
    try:
        game_time=datetime.datetime.fromisoformat(info.get("game_time","").replace("Z","+00:00"))
        now=datetime.datetime.now(datetime.timezone.utc); diff=(now-game_time).total_seconds()/3600
        abstract=game.get("status",{}).get("abstractGameState",""); detailed=game.get("status",{}).get("detailedState","")
        if abstract=="Final" or "Final" in detailed: return "🏁 Final"
        elif (abstract=="Live" or "In Progress" in detailed) and diff>=0.5: return "⚡ In Progress"
        elif diff>3: return f"⚠️ Started {diff:.1f}hrs ago"
        elif diff>0.5: return "⚡ In Progress"
        elif diff>-1: return "🔔 Starting Soon"
        else: return f"⏰ {abs(diff):.1f}hrs until first pitch"
    except: return "⏰ Unknown"


# ─────────────────────────────────────────────
# SECTIONS 2-5 — PITCHER/TEAM/BULLPEN/LINEUPS (unchanged from original)
# ─────────────────────────────────────────────
def _get_pitcher_season(pitcher_id, season):
    try:
        data=api_get(f"/people/{pitcher_id}/stats",{"stats":"season","group":"pitching","season":season,"sportId":1})
        splits=data.get("stats",[{}])[0].get("splits",[])
        if not splits: return {}
        s=splits[0]["stat"]; ip=float(s.get("inningsPitched",0) or 0)
        if ip==0: return {}
        return {"era":float(s.get("era",0) or 0),"fip":_calc_fip(s),"whip":float(s.get("whip",0) or 0),
                "k9":float(s.get("strikeoutsPer9Inn",0) or 0),"bb9":float(s.get("walksPer9Inn",0) or 0),
                "hr9":float(s.get("homeRunsPer9",0) or 0),"ip":ip,"gs":int(s.get("gamesStarted",0) or 0),
                "wins":int(s.get("wins",0) or 0),"losses":int(s.get("losses",0) or 0)}
    except: return {}

def get_pitcher_stats(pitcher_id):
    if not pitcher_id: return {}
    s26=_get_pitcher_season(pitcher_id,SEASON); s25=_get_pitcher_season(pitcher_id,SEASON-1); s24=_get_pitcher_season(pitcher_id,SEASON-2)
    gs26=s26.get("gs",0)
    if gs26>=8: weights=[(s26,0.60),(s25,0.40),(s24,0.00)]; label=f"2026({gs26}GS)+2025"
    elif gs26>=3: weights=[(s26,0.30),(s25,0.50),(s24,0.20)]; label=f"2026({gs26}GS)+2025+2024"
    elif gs26>=1: weights=[(s26,0.10),(s25,0.60),(s24,0.30)]; label=f"2026({gs26}GS small)+2025+2024"
    else: weights=[(s26,0.00),(s25,0.70),(s24,0.30)]; label="2025+2024"
    def blend(key,default=0.0):
        total,w=0.0,0.0
        for s,wt in weights:
            if wt>0 and s.get(key): total+=float(s[key])*wt; w+=wt
        return round(total/w,3) if w>0 else default
    return {"era":blend("era",4.50),"fip":blend("fip",4.50),"whip":blend("whip",1.30),"k9":blend("k9",8.00),
            "bb9":blend("bb9",3.00),"hr9":blend("hr9",1.20),"ip":s26.get("ip",s25.get("ip",0)),
            "games_started":gs26,"wins":s26.get("wins",0),"losses":s26.get("losses",0),
            "data_label":label,"era_2026":s26.get("era","N/A"),"gs_2026":gs26}

def _calc_fip(s):
    try:
        hr=float(s.get("homeRuns",0) or 0); bb=float(s.get("baseOnBalls",0) or 0)
        k=float(s.get("strikeOuts",0) or 0); ip=float(s.get("inningsPitched",1) or 1)
        return round((13*hr+3*bb-2*k)/ip+3.10,2)
    except: return 0.0

def _get_team_offense_season(team_id,season):
    try:
        data=api_get(f"/teams/{team_id}/stats",{"stats":"season","group":"hitting","season":season,"sportId":1})
        splits=data.get("stats",[{}])[0].get("splits",[])
        if not splits: return {}
        s=splits[0]["stat"]; gp=max(int(s.get("gamesPlayed",1)),1)
        return {"runs_per_game":round(float(s.get("runs",0))/gp,2),"ops":float(s.get("ops",0) or 0),
                "avg":float(s.get("avg",0) or 0),"obp":float(s.get("obp",0) or 0),
                "slg":float(s.get("slg",0) or 0),"games":gp,
                "k_pct":round(float(s.get("strikeOuts",0))/max(float(s.get("atBats",1)),1)*100,1),
                "bb_pct":round(float(s.get("baseOnBalls",0))/max(float(s.get("plateAppearances",1)),1)*100,1)}
    except: return {}

def get_team_offense(team_id):
    s26=_get_team_offense_season(team_id,SEASON); s25=_get_team_offense_season(team_id,SEASON-1); s24=_get_team_offense_season(team_id,SEASON-2)
    g26=s26.get("games",0)
    if g26>=40: weights=[(s26,0.65),(s25,0.35),(s24,0.00)]
    elif g26>=20: weights=[(s26,0.45),(s25,0.40),(s24,0.15)]
    elif g26>=10: weights=[(s26,0.25),(s25,0.50),(s24,0.25)]
    else: weights=[(s26,0.10),(s25,0.60),(s24,0.30)]
    def blend(key,default=0.0):
        total,w=0.0,0.0
        for s,wt in weights:
            if wt>0 and s.get(key): total+=float(s[key])*wt; w+=wt
        return round(total/w,3) if w>0 else default
    return {"runs_per_game":blend("runs_per_game",4.50),"ops":blend("ops",0.720),"avg":blend("avg",0.250),
            "obp":blend("obp",0.320),"slg":blend("slg",0.400),"k_pct":blend("k_pct",22.0),
            "bb_pct":blend("bb_pct",8.5),"games_2026":g26}

def get_bullpen_stats(team_id):
    try:
        data=api_get(f"/teams/{team_id}/stats",{"stats":"season","group":"pitching","season":SEASON,"sportId":1,"playerPool":"qualifier"})
        eras,whips,ks,bbs,ips=[],[],[],[],[]
        for split in data.get("stats",[{}])[0].get("splits",[]):
            s=split.get("stat",{}); gs=int(s.get("gamesStarted",0) or 0); g=int(s.get("gamesPitched",0) or 0); ip=float(s.get("inningsPitched",0) or 0)
            if gs==0 and g>0 and ip>0:
                eras.append(float(s.get("era",0) or 0)); whips.append(float(s.get("whip",0) or 0))
                ks.append(float(s.get("strikeoutsPer9Inn",0) or 0)); bbs.append(float(s.get("walksPer9Inn",0) or 0)); ips.append(ip)
        if not eras: return {}
        def wavg(vals,weights):
            tw=sum(weights); return round(sum(v*w for v,w in zip(vals,weights))/tw,2) if tw>0 else 0
        return {"bullpen_era":wavg(eras,ips),"bullpen_whip":wavg(whips,ips),"bullpen_k9":wavg(ks,ips),"bullpen_bb9":wavg(bbs,ips),"relievers":len(eras)}
    except: return {}

def get_lineup_with_ids(game,side):
    try:
        lineups=game.get("lineups",{}); batters=lineups.get(f"{side}Players",[])
        return [{"name":p.get("fullName","Unknown"),"id":p.get("id")} for p in batters[:9]]
    except: return []

def get_batter_stats(player_id,vs_hand=None):
    if not player_id: return {}
    try:
        data26=api_get(f"/people/{player_id}/stats",{"stats":"season","group":"hitting","season":SEASON,"sportId":1})
        splits26=data26.get("stats",[{}])[0].get("splits",[])
        data25=api_get(f"/people/{player_id}/stats",{"stats":"season","group":"hitting","season":SEASON-1,"sportId":1})
        splits25=data25.get("stats",[{}])[0].get("splits",[])
        s26=splits26[0]["stat"] if splits26 else {}; s25=splits25[0]["stat"] if splits25 else {}
        pa26=int(s26.get("plateAppearances",0) or 0)
        if pa26>=100: w26,w25=0.70,0.30
        elif pa26>=50: w26,w25=0.50,0.50
        elif pa26>=20: w26,w25=0.30,0.70
        else: w26,w25=0.10,0.90
        def bblend(key,default=0.0):
            v26=float(s26.get(key,0) or 0); v25=float(s25.get(key,0) or 0)
            if v26 and v25: return round(v26*w26+v25*w25,3)
            elif v26: return round(v26,3)
            elif v25: return round(v25,3)
            return default
        result={"avg":bblend("avg"),"obp":bblend("obp"),"slg":bblend("slg"),"ops":bblend("ops"),"hr":int(s26.get("homeRuns",0) or 0),"pa_2026":pa26}
        if vs_hand:
            try:
                sd=api_get(f"/people/{player_id}/stats",{"stats":"statSplits","group":"hitting","season":SEASON,"sportId":1,"sitCodes":f"v{vs_hand}"})
                ss=sd.get("stats",[{}])[0].get("splits",[])
                if ss:
                    stat=ss[0]["stat"]; ab=int(stat.get("atBats",0) or 0)
                    if ab>=30: result[f"vs_{vs_hand}_ops"]=float(stat.get("ops",0) or 0); result[f"vs_{vs_hand}_avg"]=float(stat.get("avg",0) or 0)
                    else:
                        career=api_get(f"/people/{player_id}/stats",{"stats":"careerStatSplits","group":"hitting","sportId":1,"sitCodes":f"v{vs_hand}"})
                        cs=career.get("stats",[{}])[0].get("splits",[])
                        if cs: css=cs[0]["stat"]; result[f"vs_{vs_hand}_ops"]=float(css.get("ops",0) or 0); result[f"vs_{vs_hand}_avg"]=float(css.get("avg",0) or 0)
            except: pass
        return result
    except: return {}

def get_recent_team_offense(team_id,last_n=15):
    try:
        data=api_get(f"/teams/{team_id}/stats",{"stats":"byDateRange","group":"hitting","season":SEASON,"sportId":1,
                      "startDate":(datetime.date.today()-datetime.timedelta(days=last_n)).strftime("%Y-%m-%d"),"endDate":today_str()})
        splits=data.get("stats",[{}])[0].get("splits",[])
        if not splits: return {}
        s=splits[0]["stat"]; games=max(int(s.get("gamesPlayed",1)),1)
        return {"recent_rpg":round(float(s.get("runs",0))/games,2),"recent_ops":float(s.get("ops",0) or 0),
                "recent_obp":float(s.get("obp",0) or 0),"recent_avg":float(s.get("avg",0) or 0),"recent_games":games}
    except: return {}

def get_home_away_splits(team_id,side):
    try:
        data=api_get(f"/teams/{team_id}/stats",{"stats":"statSplits","group":"hitting","season":SEASON,"sportId":1,"sitCodes":"h" if side=="home" else "a"})
        splits=data.get("stats",[{}])[0].get("splits",[])
        if not splits: return {}
        s=splits[0]["stat"]; games=max(int(s.get("gamesPlayed",1)),1)
        return {f"{side}_rpg":round(float(s.get("runs",0))/games,2),f"{side}_ops":float(s.get("ops",0) or 0),
                f"{side}_obp":float(s.get("obp",0) or 0),f"{side}_avg":float(s.get("avg",0) or 0)}
    except: return {}

def get_h2h_record(away_team_id,home_team_id):
    season_weights={SEASON:0.30,SEASON-1:0.25,SEASON-2:0.20,SEASON-3:0.15,SEASON-4:0.10}
    wins=losses=games=0; weighted_total=total_w=0.0
    for season,weight in season_weights.items():
        try:
            data=api_get("/schedule",{"sportId":1,"season":season,"teamId":away_team_id,"opponentId":home_team_id,"gameType":"R"})
            sr=sg=0
            for db in data.get("dates",[]):
                for g in db.get("games",[]):
                    if g.get("status",{}).get("abstractGameState")!="Final": continue
                    teams=g.get("teams",{}); away=teams.get("away",{}); home_t=teams.get("home",{})
                    as_=away.get("score",0) or 0; hs=home_t.get("score",0) or 0
                    if away.get("team",{}).get("id")==away_team_id:
                        if as_>hs: wins+=1
                        else: losses+=1
                    sr+=(as_+hs); sg+=1; games+=1
            if sg>0: weighted_total+=(sr/sg)*weight; total_w+=weight
        except: continue
    if games==0: return {}
    return {"h2h_wins":wins,"h2h_losses":losses,"h2h_games":games,
            "h2h_avg_total":round(weighted_total/total_w,2) if total_w>0 else 0,"h2h_win_pct":round(wins/games,3)}

def get_batter_vs_pitcher(batter_id,pitcher_id):
    if not batter_id or not pitcher_id: return {}
    try:
        data=api_get(f"/people/{batter_id}/stats",{"stats":"vsPlayer","group":"hitting","season":SEASON,"sportId":1,"opposingPlayerId":pitcher_id})
        splits=data.get("stats",[{}])[0].get("splits",[])
        if not splits:
            data=api_get(f"/people/{batter_id}/stats",{"stats":"vsPlayerTotal","group":"hitting","sportId":1,"opposingPlayerId":pitcher_id})
            splits=data.get("stats",[{}])[0].get("splits",[])
        if not splits: return {}
        s=splits[0]["stat"]; ab=int(s.get("atBats",0) or 0)
        if ab<3: return {}
        return {"ab":ab,"avg":float(s.get("avg",0) or 0),"ops":float(s.get("ops",0) or 0),"hr":int(s.get("homeRuns",0) or 0),"h":int(s.get("hits",0) or 0)}
    except: return {}

def get_lineup_vs_pitcher_ops(lineup,pitcher_id,vs_hand):
    if not lineup: return 0.720
    weights=[1.5,1.4,1.3,1.2,1.1,1.0,0.9,0.8,0.7]; total_ops=total_weight=0.0
    for i,batter in enumerate(lineup[:9]):
        pid=batter.get("id") if isinstance(batter,dict) else None
        if not pid: continue
        matchup=get_batter_vs_pitcher(pid,pitcher_id); matchup_ops=matchup.get("ops") if matchup else None
        hand_stats=get_batter_stats(pid,vs_hand); hand_ops=hand_stats.get(f"vs_{vs_hand}_ops") or hand_stats.get("ops",0.720)
        final_ops=(matchup_ops*0.50+hand_ops*0.50) if matchup_ops else hand_ops
        w=weights[i] if i<len(weights) else 0.7
        total_ops+=final_ops*w; total_weight+=w
    return round(total_ops/total_weight,3) if total_weight>0 else 0.720

def get_matchup_summary(lineup,pitcher_id,pitcher_name):
    highlights=[]
    for batter in lineup[:6]:
        pid=batter.get("id") if isinstance(batter,dict) else None; name=batter.get("name") if isinstance(batter,dict) else str(batter)
        if not pid: continue
        m=get_batter_vs_pitcher(pid,pitcher_id)
        if m and m.get("ab",0)>=5: highlights.append(f"{name}: {m['ab']} AB, .{int(m['avg']*1000):03d} AVG, {m['hr']} HR vs {pitcher_name}")
    return " | ".join(highlights) if highlights else "No significant matchup history"

def get_pitcher_recent_form(pitcher_id,last_n=3):
    if not pitcher_id: return {}
    try:
        data=api_get(f"/people/{pitcher_id}/stats",{"stats":"gameLog","group":"pitching","season":SEASON,"sportId":1})
        splits=data.get("stats",[{}])[0].get("splits",[])
        starts=[s for s in splits if int(s.get("stat",{}).get("gamesStarted",0))>0][-last_n:]
        if not starts: return {}
        eras,whips,k9s,ips,runs=[],[],[],[],[]
        for s in starts:
            stat=s.get("stat",{}); ip=float(stat.get("inningsPitched",0) or 0); er=float(stat.get("earnedRuns",0) or 0)
            h=float(stat.get("hits",0) or 0); bb=float(stat.get("baseOnBalls",0) or 0)
            k=float(stat.get("strikeOuts",0) or 0); r=float(stat.get("runs",0) or 0)
            if ip>0: eras.append(round((er/ip)*9,2)); whips.append(round((h+bb)/ip,2)); k9s.append(round((k/ip)*9,2)); ips.append(ip); runs.append(r)
        if not eras: return {}
        ae=sum(eras)/len(eras); ai=sum(ips)/len(ips)
        if ae<=2.50 and ai>=6.0: form="🔥 HOT"
        elif ae<=3.50 and ai>=5.5: form="✅ SOLID"
        elif ae<=4.50: form="➡️ AVERAGE"
        elif ae<=6.00: form="❄️ COLD"
        else: form="🚨 STRUGGLING"
        return {"recent_era":round(ae,2),"recent_whip":round(sum(whips)/len(whips),2),"recent_k9":round(sum(k9s)/len(k9s),2),
                "recent_avg_ip":round(ai,2),"recent_avg_runs":round(sum(runs)/len(runs),2),"recent_starts":len(starts),"recent_form_score":form}
    except: return {}

def get_bullpen_availability(team_id):
    try:
        yesterday=(datetime.date.today()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        data=api_get("/schedule",{"sportId":1,"date":yesterday,"teamId":team_id,"hydrate":"boxscore"})
        tired_names=[]; total_score=0
        for db in data.get("dates",[]):
            for g in db.get("games",[]):
                bs=g.get("boxscore",{})
                for side in ["away","home"]:
                    t=bs.get("teams",{}).get(side,{})
                    if t.get("team",{}).get("id")!=team_id: continue
                    for pid in t.get("pitchers",[]):
                        p=t.get("players",{}).get(f"ID{pid}",{})
                        stats=p.get("stats",{}).get("pitching",{})
                        ip=float(stats.get("inningsPitched",0) or 0); gs=int(stats.get("gamesStarted",0) or 0)
                        name=p.get("person",{}).get("fullName",f"Player {pid}")
                        if gs==0 and ip>0: tired_names.append(name); total_score+=2+(2 if ip>=1.0 else 0)
        if total_score<=2: status="✅ Fresh"
        elif total_score<=7: status="⚠️ Moderately Used"
        else: status="🚨 Heavily Used"
        return {"bp_used_yesterday":", ".join(tired_names) if tired_names else "None",
                "bp_tired_count":len(tired_names),"bp_tiredness_score":total_score,
                "bp_available_est":max(0,7-len(tired_names)),"bp_availability":status}
    except: return {"bp_used_yesterday":"Unknown","bp_tired_count":0,"bp_available_est":6,"bp_availability":"Unknown"}


# ─────────────────────────────────────────────
# KELLY + ROI (unchanged)
# ─────────────────────────────────────────────
BANKROLL=1000.0; KELLY_FRACTION=0.25; MAX_BET_PCT=0.05; MIN_BET=5.0

def kelly_bet_size(win_prob,american_odds,bankroll=BANKROLL):
    try:
        decimal=(american_odds/100+1) if american_odds>0 else (100/abs(american_odds)+1)
        b=decimal-1; p=win_prob; q=1-p; kf=(b*p-q)/b
        if kf<=0: return {"bet_dollars":0,"bet_units":0,"kelly_pct":0,"edge_pct":round((b*p-q)*100,2),"verdict":"❌ No edge"}
        frac=kf*KELLY_FRACTION; capped=min(frac,MAX_BET_PCT)
        bd=max(MIN_BET,round(bankroll*capped,2)); bu=round(bd/(bankroll/100),2)
        return {"bet_dollars":bd,"bet_units":bu,"kelly_pct":round(kf*100,2),"edge_pct":round((b*p-q)*100,2),"verdict":f"✅ Bet ${bd:.2f} ({bu:.1f}u)"}
    except: return {"bet_dollars":0,"bet_units":0,"kelly_pct":0,"verdict":"⚠️ Error"}

def calculate_roi(sheet):
    try:
        ws=sheet.worksheet("📊 Tracker"); all_vals=ws.get_all_values()
        if len(all_vals)<2: return {}
        header_row=None
        for i,row in enumerate(all_vals):
            if row and "Hit/Miss" in row: header_row=i; break
        if header_row is None: return {}
        headers=all_vals[header_row]
        hm_col=headers.index("Hit/Miss") if "Hit/Miss" in headers else None
        odds_col=headers.index("Market Odds") if "Market Odds" in headers else None
        signal_col=headers.index("Our Signal") if "Our Signal" in headers else None
        if hm_col is None: return {}
        wins=losses=pushes=0; total_profit=0.0; signal_roi={}
        for row in all_vals[header_row+1:]:
            if not row or len(row)<=hm_col: continue
            hm=row[hm_col].strip().upper()
            if hm not in ("WIN","WON","W","LOSS","L","PUSH"): continue
            try: ov=int(float(row[odds_col])) if odds_col and odds_col<len(row) else -110
            except: ov=-110
            if not ov: ov=-110
            pfw=ov/100 if ov>0 else 100/abs(ov)
            sig=row[signal_col].strip() if signal_col and signal_col<len(row) else "Unknown"
            if hm in ("WIN","WON","W"):
                wins+=1; total_profit+=pfw
                signal_roi.setdefault(sig,{"profit":0,"bets":0}); signal_roi[sig]["profit"]+=pfw; signal_roi[sig]["bets"]+=1
            elif hm in ("LOSS","L"):
                losses+=1; total_profit-=1.0
                signal_roi.setdefault(sig,{"profit":0,"bets":0}); signal_roi[sig]["profit"]-=1.0; signal_roi[sig]["bets"]+=1
            elif hm=="PUSH": pushes+=1
        tb=wins+losses
        return {"wins":wins,"losses":losses,"pushes":pushes,"total_bets":tb,
                "win_rate":round(wins/tb*100,1) if tb>0 else 0,
                "total_profit":round(total_profit,2),"roi_pct":round(total_profit/tb*100,2) if tb>0 else 0,"signal_roi":signal_roi}
    except: return {}

def print_roi_report(sheet):
    print("\n💰 ROI TRACKER\n================")
    roi=calculate_roi(sheet)
    if not roi: print("  No results yet"); return
    profit=roi["total_profit"]; color="🟢" if profit>0 else "🔴"
    print(f"  Record: {roi['wins']}W/{roi['losses']}L/{roi['pushes']}P | WR: {roi['win_rate']}%")
    print(f"  P&L: {color} {profit:+.2f}u | ROI: {roi['roi_pct']:+.2f}%")
    if roi["signal_roi"]:
        for sig,data in sorted(roi["signal_roi"].items(),key=lambda x:x[1]["profit"],reverse=True):
            if data["bets"]>=3:
                sr=round(data["profit"]/data["bets"]*100,1); c="🟢" if data["profit"]>0 else "🔴"
                print(f"  {c} {sig:<25} {data['profit']:>+.2f}u ({sr:>+.1f}%) | {data['bets']}b")
    print()


# ─────────────────────────────────────────────
# CALIBRATION (unchanged)
# ─────────────────────────────────────────────
def load_calibration(sheet):
    global _calibration, _calibration_loaded
    if _calibration_loaded: return _calibration
    _calibration_loaded=True
    try:
        ws=sheet.worksheet("⚙️ Calibration"); rows=ws.get_all_values(); cal={}
        for row in rows:
            if len(row)<2 or not row[0] or row[0].startswith("#") or row[0]=="Parameter": continue
            try: cal[row[0].strip()]=float(row[1].strip())
            except ValueError: cal[row[0].strip()]=row[1].strip()
        _calibration=cal
        if cal:
            n=int(cal.get("sample_size",0)); conf=float(cal.get("sample_confidence",0))*100
            avg_prob=float(cal.get("avg_predicted_prob",0))
            print(f"\n  ⚙️  Calibration: {n} games | Conf: {conf:.0f}% | avg_prob: {avg_prob:.4f} {'✅' if avg_prob>0.10 else '🚨 BROKEN'}")
        return cal
    except: print("  ⚙️  No calibration tab found"); return {}

def apply_calibration(proj_total,proj_away,proj_home,bet_type="total"):
    cal=_calibration; conf=float(cal.get("sample_confidence",0)) if cal else 0
    if not cal or conf<0.10: return proj_away,proj_home,proj_total
    total_adj=float(cal.get("total_run_adjustment",0))*conf
    extra=float(cal.get("over_proj_adjustment",0))*conf if "over" in bet_type.lower() else \
          float(cal.get("under_proj_adjustment",0))*conf if "under" in bet_type.lower() else 0.0
    home_adj=float(cal.get("home_run_adjustment",0))*conf; away_adj=float(cal.get("away_run_adjustment",0))*conf
    adj_away=round(max(proj_away-2,min(proj_away+2,proj_away+away_adj+(total_adj+extra)/2)),2)
    adj_home=round(max(proj_home-2,min(proj_home+2,proj_home+home_adj+(total_adj+extra)/2)),2)
    return adj_away,adj_home,round(adj_away+adj_home,2)

def apply_prob_calibration(prob):
    cal=_calibration; conf=float(cal.get("sample_confidence",0)) if cal else 0
    factor=float(cal.get("prob_confidence_factor",1.0)) if cal else 1.0
    factor=max(0.70,min(1.30,factor)); factor=1.0+(factor-1.0)*conf
    return round(min(0.95,max(0.05,prob*factor)),4)

def get_edge_threshold():
    return float(_calibration.get("edge_threshold_recommended",0.05))*100 if _calibration else 5.0

def get_yrfi_calibration_factor():
    cal=_calibration; conf=float(cal.get("sample_confidence",0)) if cal else 0
    factor=float(cal.get("yrfi_rate_factor",1.0)) if cal else 1.0
    return round(1.0+(factor-1.0)*conf,4)


# ─────────────────────────────────────────────
# PARK FACTORS (unchanged)
# ─────────────────────────────────────────────
_SAVANT_PF={
    "Coors Field":{"basic":1.20,"hr":1.28},"Great American Ball Park":{"basic":1.14,"hr":1.28},
    "Fenway Park":{"basic":1.11,"hr":1.08},"Globe Life Field":{"basic":1.09,"hr":1.14},
    "Yankee Stadium":{"basic":1.08,"hr":1.22},"Oriole Park at Camden Yards":{"basic":1.07,"hr":1.12},
    "Citizens Bank Park":{"basic":1.06,"hr":1.14},"Wrigley Field":{"basic":1.05,"hr":1.06},
    "Truist Park":{"basic":1.05,"hr":1.07},"American Family Field":{"basic":1.04,"hr":1.05},
    "Kauffman Stadium":{"basic":1.04,"hr":1.10},"Progressive Field":{"basic":1.02,"hr":0.99},
    "Nationals Park":{"basic":1.02,"hr":1.03},"Target Field":{"basic":1.01,"hr":0.97},
    "Rogers Centre":{"basic":1.01,"hr":1.03},"Angel Stadium":{"basic":1.00,"hr":1.01},
    "Comerica Park":{"basic":1.00,"hr":0.94},"PNC Park":{"basic":0.99,"hr":0.97},
    "Busch Stadium":{"basic":0.98,"hr":0.95},"Rate Field":{"basic":0.97,"hr":0.96},
    "Daikin Park":{"basic":0.97,"hr":0.94},"loanDepot park":{"basic":0.96,"hr":0.91},
    "LoanDepot Park":{"basic":0.96,"hr":0.91},"UNIQLO Field":{"basic":0.96,"hr":0.93},
    "Dodger Stadium":{"basic":0.96,"hr":0.93},"Chase Field":{"basic":0.95,"hr":0.97},
    "Citi Field":{"basic":0.95,"hr":0.89},"G.M. Steinbrenner Field":{"basic":0.95,"hr":0.91},
    "Tropicana Field":{"basic":0.95,"hr":0.91},"T-Mobile Park":{"basic":0.93,"hr":0.88},
    "Oracle Park":{"basic":0.91,"hr":0.82},"Petco Park":{"basic":0.91,"hr":0.85},
    "Sutter Health Park":{"basic":0.94,"hr":0.93},"Guaranteed Rate Field":{"basic":0.97,"hr":0.96},
}

def _get_savant_pf(venue,stat="basic"):
    v=venue.lower()
    for park,vals in _SAVANT_PF.items():
        if park.lower()==v or park.lower() in v or v in park.lower(): return vals[stat]
    return None

def get_park_factor_all_sources(venue,bp_park_run_pct=None):
    pf_static=_get_savant_pf(venue) or 1.00; pf_savant=_get_savant_pf(venue)
    pf_bp=round(1.0+bp_park_run_pct/100.0,4) if bp_park_run_pct is not None else None
    if pf_bp is not None and pf_savant is not None: blended=round(pf_bp*0.50+pf_savant*0.30+pf_static*0.20,4); source="BP50/Savant30/Static20"
    elif pf_bp is not None: blended=round(pf_bp*0.65+pf_static*0.35,4); source="BP65/Static35"
    elif pf_savant is not None: blended=round(pf_savant*0.65+pf_static*0.35,4); source="Savant65/Static35"
    else: blended=pf_static; source="Static100"
    return {"blended":blended,"bp":pf_bp,"fg":pf_savant,"static":pf_static,"source":source}

def get_park_factor_hr(venue,bp_park_hr_pct=None):
    pf_static=_get_savant_pf(venue,"hr") or 1.00; pf_savant=_get_savant_pf(venue,"hr")
    pf_bp=round(1.0+bp_park_hr_pct/100.0,4) if bp_park_hr_pct is not None else None
    if pf_bp is not None and pf_savant is not None: return round(pf_bp*0.50+pf_savant*0.30+pf_static*0.20,4)
    elif pf_bp is not None: return round(pf_bp*0.65+pf_static*0.35,4)
    elif pf_savant is not None: return round(pf_savant*0.65+pf_static*0.35,4)
    return pf_static

def get_weather_factor(temp,wind):
    factor=1.0
    try:
        t=int(str(temp).replace("°","").strip())
        if t>=85: factor+=0.04
        elif t>=75: factor+=0.02
        elif t<=50: factor-=0.04
        elif t<=60: factor-=0.02
    except: pass
    wl=str(wind).lower()
    if "out" in wl:
        try: speed=int(''.join(filter(str.isdigit,wl.split("mph")[0][-3:]))); factor+=min(speed*0.004,0.06)
        except: factor+=0.03
    elif "in" in wl:
        try: speed=int(''.join(filter(str.isdigit,wl.split("mph")[0][-3:]))); factor-=min(speed*0.004,0.06)
        except: factor-=0.03
    return round(factor,3)


# ─────────────────────────────────────────────
# RUN PROJECTIONS — ENHANCED with new factors
# ─────────────────────────────────────────────
HOME_FIELD_ADVANTAGE = 0.035

def project_runs_allowed(pitcher, opp_offense, park_factor, weather_factor,
                          lineup_ops=None, recent_offense=None, location_splits=None,
                          h2h=None, pitcher_form=None, bp_avail=None,
                          # NEW PARAMS:
                          ump_run_factor=1.0, pitcher_rest_factor=1.0,
                          off_fatigue_factor=1.0, platoon_factor=1.0,
                          bp_rolling_factor=1.0, series_factor=1.0) -> float:
    if not pitcher: return 4.50
    fip=pitcher.get("fip") or pitcher.get("era") or 4.50; era=pitcher.get("era") or fip
    if pitcher_form and pitcher_form.get("recent_era"):
        recent_era=pitcher_form["recent_era"]
        blend_fip=((recent_era*0.60+fip*0.40)*0.50+fip*0.50)
        blend_era=(recent_era*0.50+era*0.50); avg_ip=pitcher_form.get("recent_avg_ip",5.5)
    else:
        blend_fip=fip; blend_era=era; avg_ip=5.5

    # Apply pitcher rest factor — worse rest = pitcher performs worse = more runs allowed
    # pitcher_rest_factor < 1 means pitcher is worse today → invert it for runs allowed
    rest_adj = 2.0 - pitcher_rest_factor  # rest_factor=0.92 → rest_adj=1.08 (more runs)
    blend_fip = blend_fip * rest_adj

    base_ra9=(blend_fip*0.60)+(blend_era*0.40); proj_runs=(base_ra9/9)*avg_ip
    league_rpg=4.50; season_rpg=opp_offense.get("runs_per_game",league_rpg)
    recent_rpg=recent_offense.get("recent_rpg",season_rpg) if recent_offense else season_rpg
    loc_key=list(location_splits.keys())[0] if location_splits else None
    loc_rpg=location_splits.get(loc_key,season_rpg) if location_splits and loc_key else season_rpg
    blended_rpg=(season_rpg*0.40)+(recent_rpg*0.35)+(loc_rpg*0.25)
    off_factor=blended_rpg/league_rpg
    ops_factor=(lineup_ops/0.720) if lineup_ops and lineup_ops>0 else opp_offense.get("ops",0.720)/0.720
    h2h_factor=1.0
    if h2h and h2h.get("h2h_games",0)>=3:
        h2h_factor=max(0.85,min(round(h2h.get("h2h_avg_total",9.0)/9.0,3),1.15))
    bp_factor=1.0
    if bp_avail:
        score=bp_avail.get("bp_tiredness_score",0)
        if score>=8: bp_factor=1.08
        elif score>=3: bp_factor=1.04

    # Build projection with all factors
    proj_runs = proj_runs * off_factor * ops_factor * h2h_factor
    proj_runs = proj_runs * park_factor * weather_factor
    proj_runs = proj_runs * ump_run_factor          # ump tendency (over/under lean)
    proj_runs = proj_runs * platoon_factor          # lineup platoon advantage vs pitcher
    proj_runs = proj_runs * off_fatigue_factor      # offensive team fatigue + travel
    proj_runs = proj_runs * series_factor           # game 1/2/3 context

    # Bullpen contribution — blends yesterday + rolling workload
    bullpen_innings=max(0,9.0-avg_ip)
    if bullpen_innings>0:
        # Combined BP tiredness: yesterday single-day + 3-day rolling
        bp_combined = bp_factor * (1.0 + (bp_rolling_factor - 1.0) * 0.5)
        bp_extra=(4.50/9)*bullpen_innings*(bp_combined-1.0)
        proj_runs+=bp_extra

    return round(proj_runs,2)

def project_bullpen_runs(bullpen,innings_remaining,park_factor):
    if not bullpen: return round((4.50/9)*innings_remaining*park_factor,2)
    return round((bullpen.get("bullpen_era",4.50)/9)*innings_remaining*park_factor,2)

def project_total_runs(away_starter, home_starter, away_offense, home_offense,
                        away_bullpen, home_bullpen, park_factor, weather_factor,
                        away_lineup_ops=None, home_lineup_ops=None,
                        away_recent=None, home_recent=None,
                        away_location=None, home_location=None, h2h=None,
                        away_pitcher_form=None, home_pitcher_form=None,
                        away_bp_avail=None, home_bp_avail=None,
                        # NEW PARAMS:
                        ump_run_factor=1.0,
                        away_rest_factor=1.0, home_rest_factor=1.0,
                        away_fatigue_factor=1.0, home_fatigue_factor=1.0,
                        away_platoon_factor=1.0, home_platoon_factor=1.0,
                        away_bp_rolling_factor=1.0, home_bp_rolling_factor=1.0,
                        series_factor=1.0) -> dict:

    # Away team scores: home pitcher allows runs → home pitcher rest affects quality
    away_starter_runs = project_runs_allowed(
        home_starter, away_offense, park_factor, weather_factor,
        away_lineup_ops, away_recent, away_location, h2h, home_pitcher_form, home_bp_avail,
        ump_run_factor=ump_run_factor,
        pitcher_rest_factor=home_rest_factor,   # home pitcher's rest
        off_fatigue_factor=away_fatigue_factor, # away offense fatigue + travel
        platoon_factor=away_platoon_factor,     # away lineup vs home pitcher hand
        bp_rolling_factor=home_bp_rolling_factor,
        series_factor=series_factor)

    # Home team scores: away pitcher allows runs → away pitcher rest affects quality
    home_starter_runs = project_runs_allowed(
        away_starter, home_offense, park_factor, weather_factor,
        home_lineup_ops, home_recent, home_location, h2h, away_pitcher_form, away_bp_avail,
        ump_run_factor=ump_run_factor,
        pitcher_rest_factor=away_rest_factor,
        off_fatigue_factor=home_fatigue_factor,
        platoon_factor=home_platoon_factor,
        bp_rolling_factor=away_bp_rolling_factor,
        series_factor=series_factor)

    away_avg_ip=home_pitcher_form.get("recent_avg_ip",5.5) if home_pitcher_form else 5.5
    home_avg_ip=away_pitcher_form.get("recent_avg_ip",5.5) if away_pitcher_form else 5.5
    away_bullpen_runs=project_bullpen_runs(home_bullpen,9.0-away_avg_ip,park_factor)
    home_bullpen_runs=project_bullpen_runs(away_bullpen,9.0-home_avg_ip,park_factor)
    away_total=round(away_starter_runs+away_bullpen_runs,2)
    home_total=round(home_starter_runs+home_bullpen_runs,2)
    game_total=round(away_total+home_total,2)

    f5_away_ip=max(away_avg_ip,5.0); f5_home_ip=max(home_avg_ip,5.0)
    f5_away=round(away_starter_runs*(5.0/f5_away_ip),2)
    f5_home=round(home_starter_runs*(5.0/f5_home_ip),2)
    f5_total=round(f5_away+f5_home,2)

    if f5_total>game_total:
        r=game_total/f5_total if f5_total>0 else 1
        f5_away=round(f5_away*r,2); f5_home=round(f5_home*r,2); f5_total=round(f5_away+f5_home,2)
    max_f5=round(game_total*0.55,2)
    if f5_total>max_f5:
        r=max_f5/f5_total if f5_total>0 else 1
        f5_away=round(f5_away*r,2); f5_home=round(f5_home*r,2); f5_total=round(f5_away+f5_home,2)
    if game_total>18.0:
        r=18.0/game_total; away_total=round(away_total*r,2); home_total=round(home_total*r,2)
        game_total=round(away_total+home_total,2); f5_away=round(f5_away*r,2); f5_home=round(f5_home*r,2); f5_total=round(f5_away+f5_home,2)

    return {"away_proj_runs":away_total,"home_proj_runs":home_total,"proj_total":game_total,
            "f5_away_runs":f5_away,"f5_home_runs":f5_home,"proj_f5_total":f5_total}


# ─────────────────────────────────────────────
# WIN PROBABILITY — capped (unchanged)
# ─────────────────────────────────────────────
def hr_probability_score(away_pitcher: dict, home_pitcher: dict,
                          away_offense: dict, home_offense: dict,
                          park_factor_hr: float, weather_factor: float,
                          ump_run_factor: float = 1.0,
                          weather_wind: str = "") -> dict:
    """
    HR likelihood score 1-10 for the game.
    Combines park, pitchers, offense, weather, ump.

    Score:
      8-10 = 💣 HR VERY LIKELY  → yes on HR props
      6-7  = ✅ HR LIKELY
      4-5  = ➡️ NEUTRAL
      2-3  = 🚫 HR UNLIKELY
      1    = ❄️ HR VERY UNLIKELY → no on HR props
    """
    score = 5.0  # start neutral

    # 1. Park HR factor (biggest weight)
    if park_factor_hr >= 1.20:    score += 2.0   # Coors, Great American
    elif park_factor_hr >= 1.10:  score += 1.5   # Yankee Stadium, Globe Life
    elif park_factor_hr >= 1.05:  score += 1.0
    elif park_factor_hr >= 1.00:  score += 0.5
    elif park_factor_hr >= 0.95:  score -= 0.5
    elif park_factor_hr >= 0.90:  score -= 1.0
    else:                         score -= 1.5   # Oracle Park, Petco

    # 2. Pitcher HR/9 rates (both pitchers)
    away_hr9 = away_pitcher.get("hr9", 1.20) or 1.20
    home_hr9 = home_pitcher.get("hr9", 1.20) or 1.20
    avg_hr9  = (away_hr9 + home_hr9) / 2
    if avg_hr9 >= 1.80:   score += 1.5
    elif avg_hr9 >= 1.50: score += 1.0
    elif avg_hr9 >= 1.20: score += 0.5
    elif avg_hr9 >= 0.90: score -= 0.5
    else:                 score -= 1.0

    # 3. Team HR rates
    away_hrpg = away_offense.get("hr", 0) / max(away_offense.get("games_2026", 20), 1)
    home_hrpg = home_offense.get("hr", 0) / max(home_offense.get("games_2026", 20), 1)
    avg_hrpg  = (away_hrpg + home_hrpg) / 2
    if avg_hrpg >= 1.5:   score += 1.0
    elif avg_hrpg >= 1.2: score += 0.5
    elif avg_hrpg >= 0.8: score += 0.0
    elif avg_hrpg >= 0.5: score -= 0.5
    else:                 score -= 1.0

    # 4. Wind direction (out = big boost, in = killer)
    wind_lower = str(weather_wind).lower()
    if "out" in wind_lower:
        try:
            speed = int(''.join(filter(str.isdigit, wind_lower.split("mph")[0][-3:])))
            if speed >= 15:   score += 1.5
            elif speed >= 10: score += 1.0
            elif speed >= 5:  score += 0.5
        except:
            score += 0.5
    elif "in" in wind_lower:
        try:
            speed = int(''.join(filter(str.isdigit, wind_lower.split("mph")[0][-3:])))
            if speed >= 15:   score -= 1.5
            elif speed >= 10: score -= 1.0
            elif speed >= 5:  score -= 0.5
        except:
            score -= 0.5

    # 5. Ump zone (tight = fewer HRs via fewer walks/longer counts)
    if ump_run_factor >= 1.08:   score += 0.5
    elif ump_run_factor <= 0.93: score -= 0.5

    # Clamp to 1-10
    score = round(max(1.0, min(10.0, score)), 1)

    # Label
    if score >= 8:
        label = "💣 HR VERY LIKELY"
        lean  = "YES"
    elif score >= 6:
        label = "✅ HR LIKELY"
        lean  = "YES"
    elif score >= 4:
        label = "➡️ NEUTRAL"
        lean  = "NEUTRAL"
    elif score >= 2:
        label = "🚫 HR UNLIKELY"
        lean  = "NO"
    else:
        label = "❄️ HR VERY UNLIKELY"
        lean  = "NO"

    return {
        "hr_score":      score,
        "hr_label":      label,
        "hr_lean":       lean,
        "hr_park_factor": park_factor_hr,
        "hr_avg_hr9":    round(avg_hr9, 2),
        "hr_avg_hrpg":   round(avg_hrpg, 2),
    }


def win_probability(away_runs,home_runs):
    if away_runs+home_runs==0:
        return round(0.5-HOME_FIELD_ADVANTAGE,3),round(0.5+HOME_FIELD_ADVANTAGE,3)
    diff=home_runs-away_runs; diff=max(-MAX_RUN_DIFF,min(MAX_RUN_DIFF,diff))
    avg=(away_runs+home_runs)/2; away_runs_adj=avg-diff/2; home_runs_adj=avg+diff/2
    base_away=away_runs_adj/(away_runs_adj+home_runs_adj)
    away_pct=round(max(0.05,base_away-HOME_FIELD_ADVANTAGE),3)
    return away_pct,round(min(0.95,1.0-away_pct),3)

def yrfi_probability(away_starter,home_starter,away_offense,home_offense,park_factor,ump_run_factor=1.0):
    LEAGUE_AVG_ERA=4.50; LEAGUE_AVG_OBP=0.318
    def fir(pitcher,offense):
        if not pitcher: return 0.47/9
        era=pitcher.get("fip") or pitcher.get("era") or LEAGUE_AVG_ERA
        k9=pitcher.get("k9",8.5); bb9=pitcher.get("bb9",3.0)
        kf=1.0-max(0,(k9-8.5)*0.015); bbf=1.0+max(0,(bb9-3.0)*0.02)
        obp=offense.get("obp",LEAGUE_AVG_OBP); off_f=obp/LEAGUE_AVG_OBP
        base=(era/9)*0.55; rate=base*kf*bbf*off_f
        park_adj=1.0+(park_factor-1.0)*0.40
        # Ump has smaller effect in inning 1 (40% weight)
        ump_adj=1.0+(ump_run_factor-1.0)*0.40
        return max(0.25,min(rate*park_adj*ump_adj,0.55))
    ar=fir(home_starter,away_offense); hr=fir(away_starter,home_offense)
    yrfi=round(1-math.exp(-ar)*math.exp(-hr),3)
    return max(0.35,min(yrfi,0.67))

def run_line_probability(away_runs,home_runs,line=1.5):
    if away_runs<=0 or home_runs<=0: return 0.5,0.5
    def pmf(lam,k): return (math.exp(-lam)*(lam**k))/math.factorial(k)
    ap=hp=0.0
    for a in range(21):
        pa=pmf(away_runs,a)
        for h in range(21):
            j=pa*pmf(home_runs,h); d=a-h
            if d>line: ap+=j
            elif d<-line: hp+=j
    tot=ap+hp
    return (round(ap/tot,4),round(hp/tot,4)) if tot>0 else (0.5,0.5)


# ─────────────────────────────────────────────
# SIGNAL SYSTEM — ENHANCED with ump + line movement adjustments
# ─────────────────────────────────────────────
def sharp_money_signal(bet_pct,money_pct,side):
    if bet_pct is None or money_pct is None: return ""
    diff=money_pct-bet_pct
    if bet_pct>=65 and money_pct<=45: return f"⚡ SHARP FADE {side}"
    elif bet_pct<=35 and money_pct>=55: return f"⚡ SHARP BACK {side}"
    elif diff>=20: return f"💰 MONEY LEAN {side}"
    elif diff<=-20: return f"💰 MONEY FADE {side}"
    return ""

def american_to_implied(odds):
    if odds>0: return round(100/(odds+100),4)
    return round(abs(odds)/(abs(odds)+100),4)

def calc_edge(our_prob,market_odds):
    return round((our_prob-american_to_implied(market_odds))*100,1)

def prob_to_american(prob):
    if prob<=0 or prob>=1: return 0
    if prob>=0.5: return round(-(prob/(1-prob))*100)
    return round(((1-prob)/prob)*100)

def score_signal(our_prob, market_odds, sharp_confirms=False, sharp_fades=False,
                 ump_adj=0, lm_adj=0) -> tuple:
    """
    Enhanced scoring: adds ump_adj (ump tendency) and lm_adj (line movement) to total.
    These can push a LEAN to STRONG or pull a STRONG back to LEAN.
    """
    if not market_odds: return "—",0,0.0
    edge=calc_edge(our_prob,market_odds)
    if edge<=-EDGE_THRESHOLD: return "❌ FADE",round(edge),edge
    if our_prob>=0.65:   prob_score=40
    elif our_prob>=0.60: prob_score=30
    elif our_prob>=0.55: prob_score=20
    elif our_prob>=0.50: prob_score=10
    else:                prob_score=0
    if edge>=15:   edge_score=40
    elif edge>=10: edge_score=30
    elif edge>=7:  edge_score=20
    elif edge>=5:  edge_score=10
    else:          edge_score=0
    sharp_score=20 if sharp_confirms else (-20 if sharp_fades else 0)
    total=prob_score+edge_score+sharp_score+ump_adj+lm_adj
    if total>=80:   signal="🔥🔥 DOUBLE STRONG"
    elif total>=60: signal="🔥 STRONG"
    elif total>=40: signal="✅ LEAN"
    elif total>=20: signal="👀 WATCH"
    else:           signal="— SKIP"
    return signal,total,edge

def get_sharp_alignment(market,bet_type):
    if bet_type=="away_ml":       b,m=market.get("ml_bet_away"),market.get("ml_money_away")
    elif bet_type=="home_ml":     b,m=market.get("ml_bet_home"),market.get("ml_money_home")
    elif bet_type=="over":        b,m=market.get("over_bet_pct"),market.get("over_money_pct")
    elif bet_type=="under":       b,m=market.get("under_bet_pct"),market.get("under_money_pct")
    elif bet_type=="away_spread": b,m=market.get("spread_bet_away"),market.get("spread_money_away")
    elif bet_type=="home_spread": b,m=market.get("spread_bet_home"),market.get("spread_money_home")
    else: return False,False
    if b is None or m is None: return False,False
    diff=m-b; confirms=diff>=15 or (b<=35 and m>=55); fades=diff<=-15 or (b>=65 and m<=45)
    if confirms and fades: fades=False
    return confirms,fades


# ─────────────────────────────────────────────
# INPUT TAB (unchanged)
# ─────────────────────────────────────────────
INPUT_TAB_NAME="📥 Input"
INPUT_COLUMNS=[
    "Game","Away ML","Home ML","Total Line","Over Odds","Under Odds","Away RL Odds","Home RL Odds",
    "F5 Total","F5 Over Odds","F5 Under Odds","F5 Away ML","F5 Home ML",
    "Away TT Line","Away TT Over","Away TT Under","Home TT Line","Home TT Over","Home TT Under",
    "YRFI Odds","NRFI Odds",
    "Away ML Bet%","Away ML Money%","Home ML Bet%","Home ML Money%",
    "Over Bet%","Over Money%","Under Bet%","Under Money%",
    "Away Spread Bet%","Away Spread Money%","Home Spread Bet%","Home Spread Money%",
    "BP Away Runs","BP Home Runs","BP YRFI%","BP F5 Away","BP F5 Home",
    "BP Park Run%","BP Park HR%","BP Away SP Inn","BP Away SP Runs","BP Away SP K","BP Away SP BB",
    "BP Home SP Inn","BP Home SP Runs","BP Home SP K","BP Home SP BB",
    "BP Away R/G","BP Away HR/G","BP Home R/G","BP Home HR/G",
]

def create_input_tab(sheet):
    try: sheet.worksheet(INPUT_TAB_NAME); print("  ✅ Input tab exists"); return
    except gspread.WorksheetNotFound: pass
    ws=sheet.add_worksheet(INPUT_TAB_NAME,rows=50,cols=60)
    ws.update("A1",[["⚾ MLB MODEL INPUT"]]); ws.append_row(INPUT_COLUMNS)
    print("  ✅ Created Input tab")

def read_input_from_sheet(sheet,game_name):
    try:
        ws=sheet.worksheet(INPUT_TAB_NAME); rows=ws.get_all_values()
        header_row=None
        for i,row in enumerate(rows):
            if row and row[0]=="Game": header_row=i; break
        if header_row is None: return None,None
        headers=rows[header_row]
        for row in rows[header_row+1:]:
            if not row or not row[0]: continue
            rg=row[0].strip().lower(); s=game_name.strip().lower()
            if rg==s or all(p in rg for p in s.split(" @ ")):
                data={h.strip():row[j].strip() for j,h in enumerate(headers) if h.strip() and j<len(row) and row[j].strip()}
                return _parse_sheet_input(data,game_name)
        return None,None
    except Exception as e: print(f"  ⚠️  Input read error: {e}"); return None,None

def _parse_sheet_input(data,game_name):
    def si(k,d=None):
        v=data.get(k,"")
        if not v: return d
        try: return int(float(v))
        except: return d
    def sf(k,d=None):
        v=data.get(k,"")
        if not v: return d
        try: return float(v)
        except: return d
    sharp_signals=[]
    for bk,mk,side in [("Away ML Bet%","Away ML Money%",game_name.split("@")[0].strip()+" ML"),
                        ("Home ML Bet%","Home ML Money%",game_name.split("@")[-1].strip()+" ML"),
                        ("Over Bet%","Over Money%","OVER"),("Under Bet%","Under Money%","UNDER"),
                        ("Away Spread Bet%","Away Spread Money%","Away Spread"),("Home Spread Bet%","Home Spread Money%","Home Spread")]:
        s=sharp_money_signal(sf(bk),sf(mk),side)
        if s: sharp_signals.append(s)
    market={
        "away_ml":si("Away ML"),"home_ml":si("Home ML"),"total_line":sf("Total Line"),
        "over_odds":si("Over Odds",-110),"under_odds":si("Under Odds",-110),
        "away_rl_odds":si("Away RL Odds"),"home_rl_odds":si("Home RL Odds"),
        "mkt_f5_line":sf("F5 Total"),"f5_over_odds":si("F5 Over Odds",-110),"f5_under_odds":si("F5 Under Odds",-110),
        "f5_away_ml":si("F5 Away ML"),"f5_home_ml":si("F5 Home ML"),
        "away_team_total":sf("Away TT Line"),"away_tt_over_odds":si("Away TT Over",-110),"away_tt_under_odds":si("Away TT Under",-110),
        "home_team_total":sf("Home TT Line"),"home_tt_over_odds":si("Home TT Over",-110),"home_tt_under_odds":si("Home TT Under",-110),
        "yrfi_odds":si("YRFI Odds",-115),"nrfi_odds":si("NRFI Odds",-105),
        "ml_bet_away":sf("Away ML Bet%"),"ml_money_away":sf("Away ML Money%"),
        "ml_bet_home":sf("Home ML Bet%"),"ml_money_home":sf("Home ML Money%"),
        "over_bet_pct":sf("Over Bet%"),"over_money_pct":sf("Over Money%"),
        "under_bet_pct":sf("Under Bet%"),"under_money_pct":sf("Under Money%"),
        "spread_bet_away":sf("Away Spread Bet%"),"spread_money_away":sf("Away Spread Money%"),
        "spread_bet_home":sf("Home Spread Bet%"),"spread_money_home":sf("Home Spread Money%"),
        "sharp_signals":" | ".join(sharp_signals) if sharp_signals else "—",
    }
    bp={
        "bp_away_runs":sf("BP Away Runs"),"bp_home_runs":sf("BP Home Runs"),
        "bp_yrfi_pct":sf("BP YRFI%"),"bp_f5_away":sf("BP F5 Away"),"bp_f5_home":sf("BP F5 Home"),
        "bp_park_run_pct":sf("BP Park Run%"),"bp_park_hr_pct":sf("BP Park HR%"),
        "bp_away_sp_inn":sf("BP Away SP Inn"),"bp_away_sp_runs":sf("BP Away SP Runs"),
        "bp_away_sp_k":sf("BP Away SP K"),"bp_away_sp_bb":sf("BP Away SP BB"),
        "bp_home_sp_inn":sf("BP Home SP Inn"),"bp_home_sp_runs":sf("BP Home SP Runs"),
        "bp_home_sp_k":sf("BP Home SP K"),"bp_home_sp_bb":sf("BP Home SP BB"),
        "bp_away_rpg":sf("BP Away R/G"),"bp_away_hrpg":sf("BP Away HR/G"),
        "bp_home_rpg":sf("BP Home R/G"),"bp_home_hrpg":sf("BP Home HR/G"),
    }
    return market,bp

def blend_projections(api_runs,bp):
    def blend(av,bv,w=0.60): return round(av*(1-w)+bv*w,2) if bv is not None else av
    away=blend(api_runs["away_proj_runs"],bp.get("bp_away_runs"))
    home=blend(api_runs["home_proj_runs"],bp.get("bp_home_runs"))
    f5a=blend(api_runs["f5_away_runs"],bp.get("bp_f5_away"))
    f5h=blend(api_runs["f5_home_runs"],bp.get("bp_f5_home"))
    return {"away_proj_runs":away,"home_proj_runs":home,"proj_total":round(away+home,2),
            "f5_away_runs":f5a,"f5_home_runs":f5h,"proj_f5_total":round(f5a+f5h,2),"bp_blended":bp.get("bp_away_runs") is not None}

def blend_yrfi(api_yrfi,bp):
    bv=bp.get("bp_yrfi_pct")
    if bv is None: return api_yrfi
    return round(api_yrfi*0.30+(bv/100)*0.70,3)


# ─────────────────────────────────────────────
# ANALYZE GAME — full pipeline with all 7 new variables
# ─────────────────────────────────────────────
def analyze_game(game: dict, current_odds: dict = None, snapshot: dict = None) -> dict:
    info = parse_game_info(game)
    print(f"\n🔍 {info['away_team']} @ {info['home_team']}")

    game_status=check_game_timing(game,info); info["game_status"]=game_status
    if any(s in game_status for s in ["In Progress","Final","Started","⚡","🏁","⚠️"]):
        print(f"  ⏭️  SKIPPING — {game_status}")
        return {"game_time":info.get("game_time",""),"away_team":info["away_team"],"home_team":info["home_team"],
                "venue":info.get("venue",""),"game_status":game_status,"skipped":True}

    # ── Standard stats ────────────────────────────────────────
    print("  📊 Fetching standard stats...")
    away_pitcher=get_pitcher_stats(info["away_pitcher_id"])
    home_pitcher=get_pitcher_stats(info["home_pitcher_id"])
    away_offense=get_team_offense(info["away_team_id"])
    home_offense=get_team_offense(info["home_team_id"])
    away_bullpen=get_bullpen_stats(info["away_team_id"])
    home_bullpen=get_bullpen_stats(info["home_team_id"])
    away_recent=get_recent_team_offense(info["away_team_id"],15)
    home_recent=get_recent_team_offense(info["home_team_id"],15)
    away_location=get_home_away_splits(info["away_team_id"],"away")
    home_location=get_home_away_splits(info["home_team_id"],"home")
    away_pf=get_pitcher_recent_form(info["away_pitcher_id"],3)
    home_pf=get_pitcher_recent_form(info["home_pitcher_id"],3)
    away_bp_avail=get_bullpen_availability(info["away_team_id"])
    home_bp_avail=get_bullpen_availability(info["home_team_id"])
    h2h=get_h2h_record(info["away_team_id"],info["home_team_id"])

    if away_pf: print(f"    {info['away_pitcher']}: {away_pf.get('recent_form_score','?')} | ERA: {away_pf.get('recent_era','?')} | IP: {away_pf.get('recent_avg_ip','?')}")
    if home_pf: print(f"    {info['home_pitcher']}: {home_pf.get('recent_form_score','?')} | ERA: {home_pf.get('recent_era','?')} | IP: {home_pf.get('recent_avg_ip','?')}")

    # ── NEW VARIABLES ─────────────────────────────────────────
    print("  🆕 Fetching new variables...")

    # 1. Umpire
    ump_name   = get_home_plate_ump(info["game_id"])
    ump_factor = get_ump_factor(ump_name)
    ump_rf     = ump_factor.get("run_factor", 1.0)
    print(f"  🧑‍⚖️  Ump: {ump_name or 'TBD'} | Zone: {ump_factor.get('zone','?')} | Run×: {ump_rf:.2f}")

    # 2. Pitcher rest
    away_rest = get_pitcher_days_rest(info["away_pitcher_id"])
    home_rest = get_pitcher_days_rest(info["home_pitcher_id"])
    if away_rest["rest_label"] != "Normal (5d)":
        print(f"  📅 {info['away_pitcher']}: {away_rest['rest_label']}")
    if home_rest["rest_label"] != "Normal (5d)":
        print(f"  📅 {info['home_pitcher']}: {home_rest['rest_label']}")

    # 3. Schedule fatigue
    away_schedule = get_team_schedule_spot(info["away_team_id"])
    home_schedule = get_team_schedule_spot(info["home_team_id"])
    if away_schedule["schedule_label"] != "Normal":
        print(f"  😴 Away fatigue: {away_schedule['schedule_label']}")
    if home_schedule["schedule_label"] != "Normal":
        print(f"  😴 Home fatigue: {home_schedule['schedule_label']}")

    # 4. Rolling BP workload
    away_bp_roll = get_bullpen_rolling_workload(info["away_team_id"],3)
    home_bp_roll = get_bullpen_rolling_workload(info["home_team_id"],3)
    print(f"  🔋 BP (3d): Away {away_bp_roll['bp_rolling_workload']} | Home {home_bp_roll['bp_rolling_workload']}")

    # 5. Series context
    series = get_series_context(info["game_id"],info["away_team_id"],info["home_team_id"])
    print(f"  📆 {series['series_label']}")

    # 6. Travel/timezone
    travel = get_travel_factor(info["away_team"],info["venue"],info.get("game_time",""))
    if travel["tz_diff"] >= 2:
        print(f"  ✈️  {travel['travel_label']}")

    # Lineups + platoon
    away_lineup_full=get_lineup_with_ids(game,"away")
    home_lineup_full=get_lineup_with_ids(game,"home")
    away_hand=away_pitcher.get("hand","R"); home_hand=home_pitcher.get("hand","R")

    # 7. Platoon advantage
    away_platoon=get_platoon_advantage(away_lineup_full,home_hand)
    home_platoon=get_platoon_advantage(home_lineup_full,away_hand)
    if abs(away_platoon["platoon_score"])>0.2: print(f"  ⚔️  Away: {away_platoon['platoon_label']}")
    if abs(home_platoon["platoon_score"])>0.2: print(f"  ⚔️  Home: {home_platoon['platoon_label']}")

    away_lineup_ops=get_lineup_vs_pitcher_ops(away_lineup_full,info["home_pitcher_id"],home_hand)
    home_lineup_ops=get_lineup_vs_pitcher_ops(home_lineup_full,info["away_pitcher_id"],away_hand)
    away_lineup=[b["name"] if isinstance(b,dict) else b for b in away_lineup_full]
    home_lineup=[b["name"] if isinstance(b,dict) else b for b in home_lineup_full]
    away_matchup_summary=get_matchup_summary(away_lineup_full,info["home_pitcher_id"],info["home_pitcher"])
    home_matchup_summary=get_matchup_summary(home_lineup_full,info["away_pitcher_id"],info["away_pitcher"])

    # ── Park + weather ────────────────────────────────────────
    _gn=f"{info['away_team']} @ {info['home_team']}"
    _sm,_sbp=None,None
    if _current_sheet:
        try: _sm,_sbp=read_input_from_sheet(_current_sheet,_gn)
        except Exception as e: print(f"  ⚠️  {e}")
    _sbp=_sbp or {}
    pf_all=get_park_factor_all_sources(info["venue"],_sbp.get("bp_park_run_pct"))
    park_factor=pf_all["blended"]
    weather_factor=get_weather_factor(info["weather_temp"],info["weather_wind"])

    # Combined away fatigue: schedule + travel
    combined_away_fatigue = round(away_schedule["fatigue_factor"] * travel["travel_factor"], 3)
    home_fatigue          = away_schedule.get("fatigue_factor", 1.0)  # home team has no travel penalty

    print(f"  🏟️  Park: {park_factor:.3f}x ({pf_all['source']}) | Wx: {weather_factor:.3f}x")

    # ── Run projections WITH all new factors ─────────────────
    runs = project_total_runs(
        away_pitcher, home_pitcher, away_offense, home_offense,
        away_bullpen, home_bullpen, park_factor, weather_factor,
        away_lineup_ops=away_lineup_ops, home_lineup_ops=home_lineup_ops,
        away_recent=away_recent, home_recent=home_recent,
        away_location=away_location, home_location=home_location, h2h=h2h,
        away_pitcher_form=away_pf, home_pitcher_form=home_pf,
        away_bp_avail=away_bp_avail, home_bp_avail=home_bp_avail,
        ump_run_factor=ump_rf,
        away_rest_factor=away_rest["rest_factor"],
        home_rest_factor=home_rest["rest_factor"],
        away_fatigue_factor=combined_away_fatigue,
        home_fatigue_factor=home_schedule["fatigue_factor"],
        away_platoon_factor=away_platoon["platoon_factor"],
        home_platoon_factor=home_platoon["platoon_factor"],
        away_bp_rolling_factor=away_bp_roll["bp_avail_factor"],
        home_bp_rolling_factor=home_bp_roll["bp_avail_factor"],
        series_factor=series["series_run_factor"],
    )

    away_win_pct,home_win_pct=win_probability(runs["away_proj_runs"],runs["home_proj_runs"])
    yrfi_prob=yrfi_probability(away_pitcher,home_pitcher,away_offense,home_offense,park_factor,ump_rf)

    # ── HR probability score ─────────────────────────────────
    hr_data = hr_probability_score(
        away_pitcher, home_pitcher, away_offense, home_offense,
        park_factor_hr=get_park_factor_hr(info["venue"],_sbp.get("bp_park_hr_pct")),
        weather_factor=weather_factor,
        ump_run_factor=ump_rf,
        weather_wind=info.get("weather_wind","")
    )
    print(f"  💣 HR Score: {hr_data['hr_score']}/10 {hr_data['hr_label']} (Park HR:{hr_data['hr_park_factor']:.2f}x | Avg HR/9:{hr_data['hr_avg_hr9']} | Wind:{info.get('weather_wind','')})")
    market=_sm or {}; bp=_sbp.copy() if _sbp else {}

    try:
        from read_ballparkpal import load_bp_games,load_bp_pitchers,load_bp_teams,get_bp_for_game
        for src in [load_bp_games("ballparkpal_games.xlsx"),load_bp_pitchers("ballparkpal_pitchers.xlsx"),load_bp_teams("ballparkpal_teams.xlsx")]:
            bpx=get_bp_for_game(src,info["away_team"],info["home_team"])
            if bpx:
                for k,v in bpx.items():
                    if v is not None: bp[k]=v
        print("  ✅ BP XLSX merged")
    except: pass

    api_only_away=runs["away_proj_runs"]; api_only_home=runs["home_proj_runs"]; api_only_total=runs["proj_total"]
    runs=blend_projections(runs,bp); yrfi_prob=blend_yrfi(yrfi_prob,bp)
    pre_cal_away=runs["away_proj_runs"]; pre_cal_home=runs["home_proj_runs"]; pre_cal_total=runs["proj_total"]

    cal_away,cal_home,cal_total=apply_calibration(runs["proj_total"],runs["away_proj_runs"],runs["home_proj_runs"])
    runs["away_proj_runs"]=cal_away; runs["home_proj_runs"]=cal_home; runs["proj_total"]=cal_total
    yrfi_prob=round(min(0.99,yrfi_prob*get_yrfi_calibration_factor()),4)

    away_win_pct,home_win_pct=win_probability(runs["away_proj_runs"],runs["home_proj_runs"])
    away_win_pct=apply_prob_calibration(away_win_pct); home_win_pct=round(1.0-away_win_pct,4)

    if away_win_pct>MAX_WIN_PROB: away_win_pct=MAX_WIN_PROB; home_win_pct=round(1.0-away_win_pct,4)
    elif home_win_pct>MAX_WIN_PROB: home_win_pct=MAX_WIN_PROB; away_win_pct=round(1.0-home_win_pct,4)

    print(f"  📊 Proj: {cal_away} — {cal_home} | Total: {cal_total} | F5: {runs.get('proj_f5_total','?')}")
    print(f"  🏆 Win%: Away {away_win_pct*100:.1f}% — Home {home_win_pct*100:.1f}% | YRFI: {yrfi_prob*100:.1f}%")

    # ── SIGNALS with ump + line movement adjustments ─────────
    edges = {}
    edges["ump_name"]          = ump_name or "TBD"
    edges["ump_run_factor"]    = ump_rf
    edges["ump_zone"]          = ump_factor.get("zone","neutral")
    edges["ump_notes"]         = ump_factor.get("notes","")
    edges["away_rest_label"]   = away_rest.get("rest_label","")
    edges["home_rest_label"]   = home_rest.get("rest_label","")
    edges["away_sched_label"]  = away_schedule.get("schedule_label","Normal")
    edges["home_sched_label"]  = home_schedule.get("schedule_label","Normal")
    edges["away_platoon_label"]= away_platoon.get("platoon_label","")
    edges["home_platoon_label"]= home_platoon.get("platoon_label","")
    edges["bp_rolling_away"]   = away_bp_roll.get("bp_rolling_workload","")
    edges["bp_rolling_home"]   = home_bp_roll.get("bp_rolling_workload","")
    edges["series_label"]      = series.get("series_label","")
    edges["travel_label"]      = travel.get("travel_label","")

    # ── MONTE CARLO SIMULATION ────────────────────────────────
    # Run 1000 simulations for accurate probability distributions
    print(f"  🎲 Running 1000 Monte Carlo simulations...")
    sim = monte_carlo_game(runs["away_proj_runs"], runs["home_proj_runs"], n_sims=1000)

    # Use simulation win probabilities (more accurate than ratio method)
    # Blend 60% simulation / 40% our calibrated win prob for stability
    mc_away = sim["away_win_prob"]
    mc_home = sim["home_win_prob"]
    away_win_pct = round(away_win_pct * 0.40 + mc_away * 0.60, 4)
    home_win_pct = round(1.0 - away_win_pct, 4)

    # Re-cap after MC blend
    if away_win_pct > MAX_WIN_PROB:
        away_win_pct = MAX_WIN_PROB; home_win_pct = round(1.0 - away_win_pct, 4)
    elif home_win_pct > MAX_WIN_PROB:
        home_win_pct = MAX_WIN_PROB; away_win_pct = round(1.0 - home_win_pct, 4)

    # Store MC stats for display
    edges["mc_avg_total"]   = sim["avg_total"]
    edges["mc_stdev"]       = sim["total_stdev"]
    edges["mc_p10"]         = sim["p10_total"]
    edges["mc_p90"]         = sim["p90_total"]
    edges["mc_away_rl_prob"]= round(sim["away_rl_prob"] * 100, 1)
    edges["mc_home_rl_prob"]= round(sim["home_rl_prob"] * 100, 1)

    print(f"  🎲 MC: avg={sim['avg_total']} | stdev={sim['total_stdev']} | "
          f"10th={sim['p10_total']} / 90th={sim['p90_total']} runs | "
          f"Away win: {mc_away*100:.1f}%")

    edges["fair_away_ml"]=prob_to_american(away_win_pct)
    edges["fair_home_ml"]=prob_to_american(home_win_pct)
    edges["fair_yrfi"]=prob_to_american(yrfi_prob)
    edges["fair_nrfi"]=prob_to_american(1-yrfi_prob)

    game_key=_gn
    current_odds=current_odds or {}; snapshot=snapshot or {}

    def _score(prob, odds, sharp_type, lm_type, ump_type):
        sc,fd=get_sharp_alignment(market,sharp_type)
        ua=get_ump_signal_adjustment(ump_name,ump_type)
        la=get_line_movement_adj(game_key,current_odds,snapshot,lm_type)
        return score_signal(prob,odds,sc,fd,ua,la)

    if market.get("away_ml"):
        sig,score,edge=_score(away_win_pct,market["away_ml"],"away_ml","away_ml","ml")
        edges["away_ml_edge"]=edge; edges["away_ml_score"]=score; edges["away_ml_flag"]=sig
    if market.get("home_ml"):
        sig,score,edge=_score(home_win_pct,market["home_ml"],"home_ml","home_ml","ml")
        edges["home_ml_edge"]=edge; edges["home_ml_score"]=score; edges["home_ml_flag"]=sig

    if market.get("total_line") and market.get("over_odds"):
        # Use Monte Carlo probability instead of single Poisson
        over_prob  = mc_prob_over(sim, market["total_line"])
        under_prob = mc_prob_under(sim, market["total_line"])
        edges["over_prob"]=round(over_prob*100,1); edges["under_prob"]=round(under_prob*100,1)
        edges["fair_over"]=prob_to_american(over_prob); edges["fair_under"]=prob_to_american(under_prob)
        sig,score,edge=_score(over_prob,market["over_odds"],"over","over","over")
        edges["over_edge"]=edge; edges["over_score"]=score; edges["over_flag"]=sig
        sig,score,edge=_score(under_prob,market["under_odds"],"under","under","under")
        edges["under_edge"]=edge; edges["under_score"]=score; edges["under_flag"]=sig

    if market.get("mkt_f5_line") and market.get("f5_over_odds"):
        our_f5=runs.get("proj_f5_total") or 0; mkt_f5=float(market["mkt_f5_line"])
        full_total=float(market.get("total_line") or 9.0)
        if mkt_f5/full_total<0.65 and our_f5>0:
            # F5 simulation — scaled version
            f5_sim = monte_carlo_game(
                runs.get("proj_f5_away", our_f5*0.48),
                runs.get("proj_f5_home", our_f5*0.52),
                n_sims=500)
            f5op=mc_prob_over(f5_sim, mkt_f5); f5up=1-f5op
            edges["f5_over_prob"]=round(f5op*100,1); edges["f5_under_prob"]=round(f5up*100,1)
            sig,score,edge=_score(f5op,market["f5_over_odds"],"over","over","over")
            edges["f5_over_edge"]=edge; edges["f5_over_score"]=score; edges["f5_over_flag"]=sig
            sig,score,edge=_score(f5up,market.get("f5_under_odds",-110),"under","under","under")
            edges["f5_under_edge"]=edge; edges["f5_under_score"]=score; edges["f5_under_flag"]=sig

    if market.get("f5_away_ml"):
        sig,score,edge=_score(away_win_pct,market["f5_away_ml"],"away_ml","away_ml","ml")
        edges["f5_away_ml_edge"]=edge; edges["f5_away_ml_score"]=score; edges["f5_away_ml_flag"]=sig
    if market.get("f5_home_ml"):
        sig,score,edge=_score(home_win_pct,market["f5_home_ml"],"home_ml","home_ml","ml")
        edges["f5_home_ml_edge"]=edge; edges["f5_home_ml_score"]=score; edges["f5_home_ml_flag"]=sig

    rl_line=abs(float(market.get("away_rl_line",-1.5) or -1.5))
    if market.get("away_rl_odds"):
        arlp,hrlp=run_line_probability(runs["away_proj_runs"],runs["home_proj_runs"],rl_line)
        edges["away_rl_prob"]=round(arlp*100,1); edges["home_rl_prob"]=round(hrlp*100,1)
        edges["fair_away_rl"]=prob_to_american(arlp); edges["fair_home_rl"]=prob_to_american(hrlp)
        sig,score,edge=_score(arlp,market["away_rl_odds"],"away_spread","away_ml","ml")
        edges["away_rl_edge"]=edge; edges["away_rl_score"]=score; edges["away_rl_flag"]=sig
    if market.get("home_rl_odds"):
        hrlp_val=edges.get("home_rl_prob",50)/100 if "home_rl_prob" in edges else run_line_probability(runs["away_proj_runs"],runs["home_proj_runs"],rl_line)[1]
        sig,score,edge=_score(hrlp_val,market["home_rl_odds"],"home_spread","home_ml","ml")
        edges["home_rl_edge"]=edge; edges["home_rl_score"]=score; edges["home_rl_flag"]=sig

    if market.get("yrfi_odds"):
        ua_yr=get_ump_signal_adjustment(ump_name,"yrfi")
        la_yr=get_line_movement_adj(game_key,current_odds,snapshot,"over")  # yrfi correlates with over
        sc,fd=get_sharp_alignment(market,"over")
        sig,score,edge=score_signal(yrfi_prob,market["yrfi_odds"],sc,fd,ua_yr,la_yr)
        edges["yrfi_edge"]=edge; edges["yrfi_score"]=score; edges["yrfi_flag"]=sig
        ua_nr=-ua_yr; la_nr=-la_yr
        sc,fd=get_sharp_alignment(market,"under")
        sig,score,edge=score_signal(1-yrfi_prob,market.get("nrfi_odds",-105),sc,fd,ua_nr,la_nr)
        edges["nrfi_edge"]=edge; edges["nrfi_score"]=score; edges["nrfi_flag"]=sig

    if market.get("away_team_total") and market.get("away_tt_over_odds"):
        # Use MC team score distribution
        ato=mc_prob_team_total_over(runs["away_proj_runs"],runs["home_proj_runs"],"away",market["away_team_total"],sim)
        atu=1-ato
        edges["away_tt_over_prob"]=round(ato*100,1); edges["fair_away_tt_over"]=prob_to_american(ato)
        sig,score,edge=score_signal(ato,market["away_tt_over_odds"])
        edges["away_tt_over_edge"]=edge; edges["away_tt_over_score"]=score; edges["away_tt_over_flag"]=sig
        sig,score,edge=score_signal(atu,market.get("away_tt_under_odds",-110))
        edges["away_tt_under_edge"]=edge; edges["away_tt_under_score"]=score; edges["away_tt_under_flag"]=sig

    if market.get("home_team_total") and market.get("home_tt_over_odds"):
        hto=mc_prob_team_total_over(runs["away_proj_runs"],runs["home_proj_runs"],"home",market["home_team_total"],sim)
        htu=1-hto
        edges["home_tt_over_prob"]=round(hto*100,1); edges["fair_home_tt_over"]=prob_to_american(hto)
        sig,score,edge=score_signal(hto,market["home_tt_over_odds"])
        edges["home_tt_over_edge"]=edge; edges["home_tt_over_score"]=score; edges["home_tt_over_flag"]=sig
        sig,score,edge=score_signal(htu,market.get("home_tt_under_odds",-110))
        edges["home_tt_under_edge"]=edge; edges["home_tt_under_score"]=score; edges["home_tt_under_flag"]=sig

    edges["sharp_signals"] = market.get("sharp_signals","—")

    # Add Pinnacle sharp signal if available
    pinnacle_sig = current_odds.get(_gn, {}).get("pinnacle_signal", "")
    if pinnacle_sig:
        existing = edges["sharp_signals"]
        edges["sharp_signals"] = f"{pinnacle_sig} | {existing}" if existing != "—" else pinnacle_sig

    return {
        **info,
        **{k:v for k,v in runs.items() if k not in ("f5_total","f5_away_runs","f5_home_runs")},
        "proj_f5_away":round(float(runs.get("f5_away_runs",0) or 0),2),
        "proj_f5_home":round(float(runs.get("f5_home_runs",0) or 0),2),
        "proj_f5_total":round(float(runs.get("proj_f5_total",0) or 0),2),
        "api_proj_total":round(float(runs.get("proj_total",0) or 0),2),
        "away_win_pct":away_win_pct,"home_win_pct":home_win_pct,"yrfi_prob":yrfi_prob,
        "hr_score":    hr_data["hr_score"],
        "hr_label":    hr_data["hr_label"],
        "hr_lean":     hr_data["hr_lean"],
        "mc_avg_total":  sim["avg_total"],
        "mc_stdev":      sim["total_stdev"],
        "mc_p10":        sim["p10_total"],
        "mc_p90":        sim["p90_total"],
        "mc_away_win":   round(sim["away_win_prob"]*100,1),
        "mc_home_win":   round(sim["home_win_prob"]*100,1),
        "park_factor":park_factor,"weather_factor":weather_factor,
        "away_lineup":", ".join(away_lineup) if away_lineup else "Not posted",
        "home_lineup":", ".join(home_lineup) if home_lineup else "Not posted",
        "away_era":away_pitcher.get("era","N/A"),"away_fip":away_pitcher.get("fip","N/A"),
        "away_k9":away_pitcher.get("k9","N/A"),"away_bb9":away_pitcher.get("bb9","N/A"),
        "home_era":home_pitcher.get("era","N/A"),"home_fip":home_pitcher.get("fip","N/A"),
        "home_k9":home_pitcher.get("k9","N/A"),"home_bb9":home_pitcher.get("bb9","N/A"),
        "away_rpg":away_offense.get("runs_per_game","N/A"),"away_ops":away_offense.get("ops","N/A"),
        "home_rpg":home_offense.get("runs_per_game","N/A"),"home_ops":home_offense.get("ops","N/A"),
        "away_bullpen_era":away_bullpen.get("bullpen_era","N/A"),"home_bullpen_era":home_bullpen.get("bullpen_era","N/A"),
        "away_pitcher_form":away_pf.get("recent_form_score","N/A"),
        "away_pitcher_recent_era":away_pf.get("recent_era","N/A"),"away_pitcher_recent_ip":away_pf.get("recent_avg_ip","N/A"),
        "home_pitcher_form":home_pf.get("recent_form_score","N/A"),
        "home_pitcher_recent_era":home_pf.get("recent_era","N/A"),"home_pitcher_recent_ip":home_pf.get("recent_avg_ip","N/A"),
        "away_bp_availability":away_bp_avail.get("bp_availability","N/A"),"away_bp_tired":away_bp_avail.get("bp_used_yesterday","N/A"),
        "home_bp_availability":home_bp_avail.get("bp_availability","N/A"),"home_bp_tired":home_bp_avail.get("bp_used_yesterday","N/A"),
        "away_matchup_summary":away_matchup_summary,"home_matchup_summary":home_matchup_summary,
        "away_recent_rpg":away_recent.get("recent_rpg","N/A"),"home_recent_rpg":home_recent.get("recent_rpg","N/A"),
        "away_recent_ops":away_recent.get("recent_ops","N/A"),"home_recent_ops":home_recent.get("recent_ops","N/A"),
        "away_loc_rpg":away_location.get("away_rpg","N/A"),"home_loc_rpg":home_location.get("home_rpg","N/A"),
        "away_lineup_ops":away_lineup_ops or "N/A","home_lineup_ops":home_lineup_ops or "N/A",
        "h2h_record":f"{h2h.get('h2h_wins',0)}-{h2h.get('h2h_losses',0)}" if h2h else "N/A",
        "h2h_avg_total":h2h.get("h2h_avg_total","N/A"),"h2h_games":h2h.get("h2h_games",0),
        **market,**bp,**edges,
    }

def _poisson_under(lam,line):
    """Fast analytical Poisson — used as fallback."""
    prob,k=0.0,0
    while k<=line: prob+=(math.exp(-lam)*lam**k)/math.factorial(k); k+=1
    return round(prob,4)

def _poisson_sample(lam: float) -> int:
    """
    Sample from Poisson distribution using Knuth algorithm.
    Fast, no numpy needed.
    """
    if lam <= 0:
        return 0
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1

def monte_carlo_game(away_runs: float, home_runs: float,
                     n_sims: int = 1000) -> dict:
    """
    Run N Monte Carlo simulations of a game using Poisson scoring.

    Each simulation:
      - Samples away team runs from Poisson(away_runs)
      - Samples home team runs from Poisson(home_runs)
      - Records winner, total, margin

    Returns probability distributions much more accurate than
    single-point Poisson math — especially for totals near the line.

    n_sims=1000 is fast (~0.05s) and accurate enough for betting.
    """
    if away_runs <= 0: away_runs = 0.1
    if home_runs <= 0: home_runs = 0.1

    away_wins = 0
    home_wins = 0
    ties      = 0
    totals    = []
    margins   = []  # home - away (positive = home wins by X)

    for _ in range(n_sims):
        a = _poisson_sample(away_runs)
        h = _poisson_sample(home_runs)
        totals.append(a + h)
        margins.append(h - a)
        if a > h:   away_wins += 1
        elif h > a: home_wins += 1
        else:       ties      += 1

    # Handle ties — split evenly (extra innings basically 50/50)
    away_win_prob = round((away_wins + ties * 0.5) / n_sims, 4)
    home_win_prob = round(1.0 - away_win_prob, 4)

    # Total distribution
    avg_total   = round(sum(totals) / n_sims, 2)
    total_stdev = round((sum((t - avg_total)**2 for t in totals) / n_sims) ** 0.5, 2)

    # Percentiles for confidence
    sorted_totals = sorted(totals)
    p10 = sorted_totals[int(n_sims * 0.10)]
    p25 = sorted_totals[int(n_sims * 0.25)]
    p75 = sorted_totals[int(n_sims * 0.75)]
    p90 = sorted_totals[int(n_sims * 0.90)]

    # Run line probabilities (away +1.5 / home -1.5)
    away_rl_wins = sum(1 for m in margins if -m > 1.5)  # away wins by 2+
    home_rl_wins = sum(1 for m in margins if m > 1.5)   # home wins by 2+
    away_rl_prob = round(away_rl_wins / n_sims, 4)
    home_rl_prob = round(home_rl_wins / n_sims, 4)

    return {
        "away_win_prob":  away_win_prob,
        "home_win_prob":  home_win_prob,
        "avg_total":      avg_total,
        "total_stdev":    total_stdev,
        "p10_total":      p10,
        "p25_total":      p25,
        "p75_total":      p75,
        "p90_total":      p90,
        "away_rl_prob":   away_rl_prob,
        "home_rl_prob":   home_rl_prob,
        "n_sims":         n_sims,
        "totals":         totals,   # full distribution for line queries
        "margins":        margins,  # full distribution for line queries
    }

def mc_prob_over(sim_results: dict, line: float) -> float:
    """P(total > line) from simulation results."""
    totals = sim_results.get("totals", [])
    if not totals: return 0.5
    return round(sum(1 for t in totals if t > line) / len(totals), 4)

def mc_prob_under(sim_results: dict, line: float) -> float:
    """P(total < line) from simulation results."""
    return round(1.0 - mc_prob_over(sim_results, line), 4)

def mc_prob_team_total_over(away_runs: float, home_runs: float,
                             side: str, line: float,
                             sim_results: dict) -> float:
    """
    P(team scores over X) using existing simulation margins.
    side: 'away' or 'home'
    """
    totals = sim_results.get("totals", [])
    margins = sim_results.get("margins", [])
    if not totals or not margins:
        # Fallback to analytical
        lam = away_runs if side == "away" else home_runs
        return round(1 - _poisson_under(lam, line), 4)
    # Reconstruct individual team scores from total + margin
    # total = a + h, margin = h - a → h = (total+margin)/2, a = (total-margin)/2
    team_scores = []
    for t, m in zip(totals, margins):
        if side == "away":
            team_scores.append((t - m) / 2)
        else:
            team_scores.append((t + m) / 2)
    return round(sum(1 for s in team_scores if s > line) / len(team_scores), 4)


# ─────────────────────────────────────────────
# PUSH TO SHEETS — ENHANCED headers
# ─────────────────────────────────────────────
HEADERS=[
    "Date","Time","Away Team","Home Team","Venue","Weather","Park Factor",
    "Away Pitcher","Away ERA","Away FIP","Away K/9","Away BB/9",
    "Home Pitcher","Home ERA","Home FIP","Home K/9","Home BB/9",
    "Away Bullpen ERA","Home Bullpen ERA",
    "Away SP Form","Away SP Recent ERA","Away SP Avg IP",
    "Home SP Form","Home SP Recent ERA","Home SP Avg IP",
    "Away BP Status","Away BP Tired Arms","Home BP Status","Home BP Tired Arms",
    "Away Lineup vs Home SP","Home Lineup vs Away SP",
    "Away Recent R/G (L15)","Home Recent R/G (L15)","Away Recent OPS","Home Recent OPS",
    "Away Road R/G","Home Home R/G","Away Lineup OPS vs SP","Home Lineup OPS vs SP",
    "H2H Record","H2H Avg Total","H2H Games","Away R/G","Away OPS","Home R/G","Home OPS",
    "Away Proj Runs","Home Proj Runs","Proj Total","F5 Away","F5 Home","F5 Total","BP Blended?",
    "BP Away Runs","BP Home Runs","BP YRFI%","BP Park Run%","BP Park HR%",
    "BP Away SP Inn","BP Away SP Runs","BP Away SP K","BP Away SP BB",
    "BP Home SP Inn","BP Home SP Runs","BP Home SP K","BP Home SP BB",
    "BP Away R/G","BP Away HR/G","BP Home R/G","BP Home HR/G",
    # ── NEW COLUMNS ──
    "Home Plate Ump","Ump Zone","Ump Run Factor","Ump Notes",
    "Away Pitcher Rest","Home Pitcher Rest",
    "Away Team Fatigue","Home Team Fatigue",
    "Away Platoon","Home Platoon",
    "Away BP Rolling (3d)","Home BP Rolling (3d)",
    "Series Context","Travel Factor",
    # ─────────────────
    "Away Win%","Home Win%","YRFI Prob","HR Lean","HR Score",
    "MC Avg Total","MC StDev","MC P10","MC P90","MC Away Win%","MC Home Win%",
    "Away ML","Home ML","Total Line","Over Odds","Under Odds","YRFI Odds","NRFI Odds",
    "Away ML Edge%","Away ML Score","Away ML Signal",
    "Home ML Edge%","Home ML Score","Home ML Signal",
    "Over Edge%","Over Score","Over Signal",
    "Under Edge%","Under Score","Under Signal",
    "F5 Over Edge%","F5 Over Score","F5 Over Signal",
    "YRFI Edge%","YRFI Score","YRFI Signal",
    "NRFI Edge%","NRFI Score","NRFI Signal",
    "Away TT Over Edge%","Away TT Over Score","Away TT Over Signal",
    "Away TT Under Edge%","Away TT Under Score","Away TT Under Signal",
    "Home TT Over Edge%","Home TT Over Score","Home TT Over Signal",
    "Home TT Under Edge%","Home TT Under Score","Home TT Under Signal",
    "Sharp Signals","Away Lineup","Home Lineup",
]

def push_to_sheets(sheet,results):
    try: ws=sheet.worksheet("Daily Model"); sheet.del_worksheet(ws)
    except: pass
    ws=sheet.add_worksheet("Daily Model",rows=200,cols=100); ws.clear()
    all_rows=[["MLB BETTING MODEL ENHANCED — "+today_str()],HEADERS]
    for r in results:
        row=[
            today_str(),r.get("game_time","")[:16],r.get("away_team",""),r.get("home_team",""),r.get("venue",""),
            f"{r.get('weather_temp','')}° {r.get('weather_condition','')} {r.get('weather_wind','')}",r.get("park_factor",""),
            r.get("away_pitcher",""),r.get("away_era",""),r.get("away_fip",""),r.get("away_k9",""),r.get("away_bb9",""),
            r.get("home_pitcher",""),r.get("home_era",""),r.get("home_fip",""),r.get("home_k9",""),r.get("home_bb9",""),
            r.get("away_bullpen_era",""),r.get("home_bullpen_era",""),
            r.get("away_pitcher_form",""),r.get("away_pitcher_recent_era",""),r.get("away_pitcher_recent_ip",""),
            r.get("home_pitcher_form",""),r.get("home_pitcher_recent_era",""),r.get("home_pitcher_recent_ip",""),
            r.get("away_bp_availability",""),r.get("away_bp_tired",""),r.get("home_bp_availability",""),r.get("home_bp_tired",""),
            r.get("away_matchup_summary",""),r.get("home_matchup_summary",""),
            r.get("away_recent_rpg",""),r.get("home_recent_rpg",""),r.get("away_recent_ops",""),r.get("home_recent_ops",""),
            r.get("away_loc_rpg",""),r.get("home_loc_rpg",""),r.get("away_lineup_ops",""),r.get("home_lineup_ops",""),
            r.get("h2h_record",""),r.get("h2h_avg_total",""),r.get("h2h_games",""),
            r.get("away_rpg",""),r.get("away_ops",""),r.get("home_rpg",""),r.get("home_ops",""),
            r.get("away_proj_runs",""),r.get("home_proj_runs",""),r.get("proj_total",""),
            r.get("proj_f5_away",""),r.get("proj_f5_home",""),r.get("proj_f5_total",""),
            "✅ Yes" if r.get("bp_blended") else "❌ API Only",
            r.get("bp_away_runs",""),r.get("bp_home_runs",""),r.get("bp_yrfi_pct",""),
            r.get("bp_park_run_pct",""),r.get("bp_park_hr_pct",""),
            r.get("bp_away_sp_inn",""),r.get("bp_away_sp_runs",""),r.get("bp_away_sp_k",""),r.get("bp_away_sp_bb",""),
            r.get("bp_home_sp_inn",""),r.get("bp_home_sp_runs",""),r.get("bp_home_sp_k",""),r.get("bp_home_sp_bb",""),
            r.get("bp_away_rpg",""),r.get("bp_away_hrpg",""),r.get("bp_home_rpg",""),r.get("bp_home_hrpg",""),
            # NEW
            r.get("ump_name",""),r.get("ump_zone",""),r.get("ump_run_factor",""),r.get("ump_notes",""),
            r.get("away_rest_label",""),r.get("home_rest_label",""),
            r.get("away_sched_label",""),r.get("home_sched_label",""),
            r.get("away_platoon_label",""),r.get("home_platoon_label",""),
            r.get("bp_rolling_away",""),r.get("bp_rolling_home",""),
            r.get("series_label",""),r.get("travel_label",""),
            # Signals
            f"{r.get('away_win_pct',0)*100:.1f}%",f"{r.get('home_win_pct',0)*100:.1f}%",f"{r.get('yrfi_prob',0)*100:.1f}%",
            r.get("hr_label","N/A"),r.get("hr_score","?"),
            r.get("mc_avg_total",""),r.get("mc_stdev",""),r.get("mc_p10",""),r.get("mc_p90",""),
            f"{r.get('mc_away_win','')}%" if r.get('mc_away_win') else "",
            f"{r.get('mc_home_win','')}%" if r.get('mc_home_win') else "",
            r.get("away_ml",""),r.get("home_ml",""),r.get("total_line",""),r.get("over_odds",""),r.get("under_odds",""),r.get("yrfi_odds",""),r.get("nrfi_odds",""),
            r.get("away_ml_edge",""),r.get("away_ml_score",""),r.get("away_ml_flag",""),
            r.get("home_ml_edge",""),r.get("home_ml_score",""),r.get("home_ml_flag",""),
            r.get("over_edge",""),r.get("over_score",""),r.get("over_flag",""),
            r.get("under_edge",""),r.get("under_score",""),r.get("under_flag",""),
            r.get("f5_over_edge",""),r.get("f5_over_score",""),r.get("f5_over_flag",""),
            r.get("yrfi_edge",""),r.get("yrfi_score",""),r.get("yrfi_flag",""),
            r.get("nrfi_edge",""),r.get("nrfi_score",""),r.get("nrfi_flag",""),
            r.get("away_tt_over_edge",""),r.get("away_tt_over_score",""),r.get("away_tt_over_flag",""),
            r.get("away_tt_under_edge",""),r.get("away_tt_under_score",""),r.get("away_tt_under_flag",""),
            r.get("home_tt_over_edge",""),r.get("home_tt_over_score",""),r.get("home_tt_over_flag",""),
            r.get("home_tt_under_edge",""),r.get("home_tt_under_score",""),r.get("home_tt_under_flag",""),
            r.get("sharp_signals","—"),r.get("away_lineup",""),r.get("home_lineup",""),
        ]
        all_rows.append(row)
    ws.append_rows(all_rows,value_input_option="USER_ENTERED")
    print(f"\n✅ Pushed {len(results)} games to Daily Model")

def push_summary_tab(sheet,results):
    try: ws=sheet.worksheet("⚡ Summary"); sheet.del_worksheet(ws)
    except: pass
    ws=sheet.add_worksheet("⚡ Summary",rows=100,cols=20)
    headers=["Game","Time","Status","Ump","Ump Zone","Series","Proj Score","Total","F5",
             "Away Win%","Home Win%","YRFI%","HR Lean","HR Score",
             "Away ML","Home ML","Total Line","Best Bets","Data"]
    all_rows=[[f"⚾ MLB MODEL ENHANCED — {today_str()}","NEW: Ump+Rest+Platoon+Rolling BP+Series+Travel"],[],headers]
    for r in results:
        away=r.get("away_team","Away"); home=r.get("home_team","Home")
        all_rows.append([
            f"{away} @ {home}",r.get("game_time","")[:16],r.get("game_status",""),
            r.get("ump_name","TBD"),r.get("ump_zone","?"),r.get("series_label","")[:35],
            f"{r.get('away_proj_runs','?')} — {r.get('home_proj_runs','?')}",
            round(float(r.get("proj_total",0) or 0),2),round(float(r.get("proj_f5_total",0) or 0),2),
            f"{r.get('away_win_pct',0)*100:.1f}%",f"{r.get('home_win_pct',0)*100:.1f}%",
            f"{r.get('yrfi_prob',0)*100:.1f}%",
            r.get("hr_label","N/A"),r.get("hr_score","?"),
            r.get("away_ml",""),r.get("home_ml",""),r.get("total_line",""),
            build_best_bets_str(r),"✅ BP+API" if r.get("bp_blended") else "⚠️ API Only",
        ])
    ws.append_rows(all_rows,value_input_option="USER_ENTERED")
    print("✅ Summary tab updated")

def build_best_bets_str(r):
    bets=[]
    for fk,ok,label in [
        ("away_ml_flag","away_ml",f"{r.get('away_team','')} ML"),
        ("home_ml_flag","home_ml",f"{r.get('home_team','')} ML"),
        ("over_flag","over_odds",f"OVER {r.get('total_line','')}"),
        ("under_flag","under_odds",f"UNDER {r.get('total_line','')}"),
        ("f5_over_flag","f5_over_odds","F5 OVER"),
        ("yrfi_flag","yrfi_odds","YRFI"),("nrfi_flag","nrfi_odds","NRFI"),
        ("away_tt_over_flag","away_tt_over_odds",f"{r.get('away_team','')} TT OVER"),
        ("home_tt_over_flag","home_tt_over_odds",f"{r.get('home_team','')} TT OVER"),
    ]:
        flag=str(r.get(fk,""))
        if "STRONG" in flag:
            odds=r.get(ok,""); ek=fk.replace("_flag","_edge"); edge=r.get(ek,"")
            odds_str=f" {odds:+d}" if isinstance(odds,int) else ""
            edge_str=f" [{edge:+.1f}%]" if isinstance(edge,(int,float)) else ""
            bets.append(f"{flag} {label}{odds_str}{edge_str}")
    return " | ".join(bets) if bets else "— No strong signals"

def print_game_summary(r):
    away=r.get("away_team","Away"); home=r.get("home_team","Home"); sep="="*52
    signals=[]
    for fk,ok,label,pk in [
        ("away_ml_flag","away_ml",f"{away} ML","away_win_pct"),
        ("home_ml_flag","home_ml",f"{home} ML","home_win_pct"),
        ("over_flag","over_odds",f"OVER {r.get('total_line','')}","over_prob"),
        ("under_flag","under_odds",f"UNDER {r.get('total_line','')}","under_prob"),
        ("f5_over_flag","f5_over_odds","F5 OVER","f5_over_prob"),
        ("yrfi_flag","yrfi_odds","YRFI","yrfi_prob"),("nrfi_flag","nrfi_odds","NRFI","nrfi_prob"),
    ]:
        flag=r.get(fk,"")
        if flag and "STRONG" in str(flag):
            ek=fk.replace("_flag","_edge"); edge=r.get(ek,""); odds=r.get(ok,""); prob_raw=r.get(pk)
            if pk in ("away_win_pct","home_win_pct","yrfi_prob") and isinstance(prob_raw,float) and prob_raw<=1:
                prob=round(prob_raw*100,1)
            else: prob=prob_raw
            odds_str=f" {odds:+d}" if isinstance(odds,int) else ""
            edge_str=f" [Edge: {edge:+.1f}%]" if isinstance(edge,(int,float)) else ""
            prob_str=f" | {prob:.1f}%" if isinstance(prob,(int,float)) else ""
            kelly_str=""
            if isinstance(prob,(int,float)) and isinstance(odds,int):
                k=kelly_bet_size(prob/100,odds)
                if k["bet_dollars"]>0: kelly_str=f" | 💰 ${k['bet_dollars']:.0f}"
            signals.append(f"   {flag} → {label}{odds_str}{edge_str}{prob_str}{kelly_str}")
    print(f"""
{sep}
📋 {away} @ {home}
{sep}
🏟️  {r.get('venue','N/A')} | {r.get('game_time','')[:16]} | {r.get('game_status','')}
🌤️  {r.get('weather_temp','')}° {r.get('weather_condition','')} | {r.get('weather_wind','')} | Park: {r.get('park_factor',1.0):.3f}x
🧑‍⚖️  Ump: {r.get('ump_name','TBD')} | Zone: {r.get('ump_zone','?')} | Run×: {r.get('ump_run_factor',1.0):.2f}x | {r.get('ump_notes','')}
📆 {r.get('series_label','')} | ✈️ {r.get('travel_label','Same TZ')}
📅 Rest: {r.get('away_pitcher','')} {r.get('away_rest_label','')} | {r.get('home_pitcher','')} {r.get('home_rest_label','')}
⚔️  Platoon: Away {r.get('away_platoon_label','')} | Home {r.get('home_platoon_label','')}
🔋 BP (3d): Away {r.get('bp_rolling_away','')} | Home {r.get('bp_rolling_home','')}
😴 Fatigue: Away {r.get('away_sched_label','Normal')} | Home {r.get('home_sched_label','Normal')}

⚾  PROJ: {away} {r.get('away_proj_runs','?')} — {r.get('home_proj_runs','?')} {home} | Total: {r.get('proj_total','?')} | F5: {r.get('proj_f5_total','?')}
🏆 WIN%: {away} {r.get('away_win_pct',0)*100:.1f}% — {home} {r.get('home_win_pct',0)*100:.1f}% | YRFI: {r.get('yrfi_prob',0)*100:.1f}%
💣 HR PROP LEAN: {r.get('hr_label','N/A')} ({r.get('hr_score','?')}/10)
🎲 MC (1000 sims): avg={r.get('mc_avg_total','?')} ± {r.get('mc_stdev','?')} | 10th-90th: {r.get('mc_p10','?')}—{r.get('mc_p90','?')} runs
   (win prob capped at {MAX_WIN_PROB*100:.0f}%)

💰 BET SIGNALS:
{''.join([s+chr(10) for s in signals]) if signals else '   — No strong signals'}
⚡ SHARP: {r.get('sharp_signals','—')}
{sep}""")


# ─────────────────────────────────────────────
# TRACKER — enhanced with new columns
# ─────────────────────────────────────────────
def push_tracker_rows(sheet,results):
    TRACKER_TAB="📊 Tracker"
    TRACKER_HEADERS=[
        "Date","Game","Bet Type","Our Signal","Score (0-100)","Our Prob%",
        "Fair Odds","Market Odds","Edge%","Our Proj Away","Our Proj Home","Our Proj Total",
        "BP Proj Away","BP Proj Home","BP Proj Total","BP YRFI%",
        "Total Diff","Sharp Signal","Actual Away","Actual Home","Actual Total","Hit/Miss","Notes"
    ]
    try: ws=sheet.worksheet(TRACKER_TAB)
    except: ws=sheet.add_worksheet(TRACKER_TAB,rows=1000,cols=30); ws.append_row(TRACKER_HEADERS)
    today=today_str()
    try: all_vals=ws.get_all_values()
    except: all_vals=[]
    already_logged=set()
    for row in all_vals:
        if not row or row[0]!=today: continue
        g=row[1].strip().lower() if len(row)>1 else ""; b=row[2].strip().lower() if len(row)>2 else ""
        if g and b: already_logged.add((g,b))
    if already_logged: print(f"  📋 {len(already_logged)} signals already logged today")
    all_rows=[]
    for r in results:
        if r.get("skipped"): continue
        away=r.get("away_team","Away"); home=r.get("home_team","Home"); game=f"{away} @ {home}"
        our_away=r.get("away_proj_runs",""); our_home=r.get("home_proj_runs",""); our_total=r.get("proj_total","")
        bp_away=r.get("bp_away_runs",""); bp_home=r.get("bp_home_runs","")
        bp_total=round(float(bp_away)+float(bp_home),2) if bp_away and bp_home else ""
        bp_yrfi=f"{r.get('bp_yrfi_pct','')}%" if r.get("bp_yrfi_pct") else ""
        try: total_diff=f"{round(float(our_total)-float(bp_total),2):+.2f}" if bp_total!="" and our_total!="" else "N/A"
        except: total_diff="N/A"
        signal_map=[
            ("away_ml_flag","away_ml_edge","away_ml",r.get("away_win_pct",0)*100,f"{away} ML",r.get("fair_away_ml",""),"away_ml_score"),
            ("home_ml_flag","home_ml_edge","home_ml",r.get("home_win_pct",0)*100,f"{home} ML",r.get("fair_home_ml",""),"home_ml_score"),
            ("over_flag","over_edge","over_odds",r.get("over_prob"),f"OVER {r.get('total_line','')}",r.get("fair_over",""),"over_score"),
            ("under_flag","under_edge","under_odds",r.get("under_prob"),f"UNDER {r.get('total_line','')}",r.get("fair_under",""),"under_score"),
            ("f5_over_flag","f5_over_edge","f5_over_odds",r.get("f5_over_prob"),f"F5 OVER",r.get("fair_f5_over",""),"f5_over_score"),
            ("yrfi_flag","yrfi_edge","yrfi_odds",r.get("yrfi_prob",0)*100,"YRFI",r.get("fair_yrfi",""),"yrfi_score"),
            ("nrfi_flag","nrfi_edge","nrfi_odds",(1-r.get("yrfi_prob",0))*100,"NRFI",r.get("fair_nrfi",""),"nrfi_score"),
        ]
        for fk,ek,ok,pv,bet_label,fair,sk in signal_map:
            flag=r.get(fk,"")
            if not flag or flag in ("","—","- "): continue
            fs=str(flag)
            if any(x in fs for x in ("FADE","SKIP","— ","WATCH")): continue
            if (game.lower(),bet_label.lower()) in already_logged: continue
            edge=r.get(ek,""); odds=r.get(ok,""); score=r.get(sk,"")
            if "DOUBLE STRONG" in fs: sl="🔥🔥 DOUBLE STRONG"
            elif "STRONG" in fs: sl="🔥 STRONG"
            elif "LEAN" in fs: sl="✅ LEAN"
            else: sl=fs
            all_rows.append([
                today,game,bet_label,sl,
                f"{score}/100" if isinstance(score,(int,float)) else "",
                f"{pv:.1f}%" if pv is not None else "",
                f"{fair:+d}" if isinstance(fair,int) else "",
                odds if odds else "",
                f"{edge:+.1f}%" if isinstance(edge,(int,float)) else "",
                our_away,our_home,our_total,bp_away,bp_home,bp_total,bp_yrfi,
                total_diff,r.get("sharp_signals","-"),
                "","","","",""
            ])
    if all_rows: ws.append_rows(all_rows,value_input_option="USER_ENTERED"); print(f"  ✅ Added {len(all_rows)} signals")
    else: print("  ✅ Tracker up to date")

def check_tracker_readiness(sheet):
    try:
        ws=sheet.worksheet("📊 Tracker"); rows=ws.get_all_values()
        if len(rows)<2: return
        header_row=None
        for i,row in enumerate(rows):
            if row and "Bet Type" in row: header_row=i; break
        if header_row is None: return
        headers=rows[header_row]
        try: hm_col=headers.index("Hit/Miss")
        except: return
        total=wl=wins=losses=0; dates=set()
        for row in rows[header_row+1:]:
            if not row or not row[0]: continue
            total+=1; hm=row[hm_col].strip().upper() if hm_col<len(row) else ""
            dates.add(row[0])
            if hm in ("WIN","W","WON"): wins+=1; wl+=1
            elif hm in ("LOSS","L"): losses+=1; wl+=1
        hr=round(wins/wl*100,1) if wl>0 else 0
        bar="█"*int(20*min(wl,30)/30)+"░"*(20-int(20*min(wl,30)/30))
        rs="🔥 FULLY READY" if wl>=100 else ("✅ READY" if wl>=50 else ("⚠️ ROUGH" if wl>=30 else f"❌ Need {30-wl} more"))
        print(f"\n{'='*52}\n📊 TRACKER: {total} signals | {len(dates)} days | {wl} W/L ({wins}W/{losses}L = {hr}%)")
        print(f"  [{bar}] {wl}/30 → {rs}\n{'='*52}")
    except Exception as e: print(f"  Tracker check error: {e}")

def print_tracker_reminder():
    print("\n📊 Fill in Actual scores + WIN/LOSS in Tracker after each game!\n")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    global _current_sheet
    print("⚾  MLB BETTING MODEL — ENHANCED v2")
    print(f"   Date: {today_str()} | Win cap: {MAX_WIN_PROB*100:.0f}% | Run diff cap: {MAX_RUN_DIFF} runs")
    print(f"   NEW: Ump + Rest + Platoon + Rolling BP + Series + Travel + Line Movement")
    print("="*60)
    _current_sheet=get_sheet(SHEET_NAME); sheet=_current_sheet
    print(f"   Connected: {sheet.title}")
    check_tracker_readiness(sheet); print_roi_report(sheet)
    print("\n⚙️  Loading calibration..."); load_calibration(sheet)
    load_ump_data()
    create_input_tab(sheet)
    odds=get_mlb_odds()
    if odds:
        odds=get_oddspapi_fallback(odds)  # Pinnacle sharp signal + fill gaps
        push_odds_to_input_tab(sheet,odds)
    rl=get_run_label(); cl=get_compare_label(rl); snapshot={}
    print(f"\n📡 Run: {rl}", end=" ")
    if cl:
        print(f"| Comparing vs {cl}...")
        snapshot=load_odds_snapshot_from_sheet(sheet,cl)
        if snapshot and odds:
            movement=detect_line_movement(odds,snapshot)
            print_line_movement_report(movement,cl)
            if movement: push_movement_to_sheet(sheet,movement,rl,cl)
    else: print("| First run of day")
    if odds: save_odds_snapshot_to_sheet(sheet,odds,rl)
    games=get_todays_games()
    if not games: print("❌ No games today."); return
    results=[]; skipped=[]
    for game in games:
        try:
            result=analyze_game(game,current_odds=odds,snapshot=snapshot)
            if result.get("skipped"): skipped.append(result)
            else: results.append(result); print_game_summary(result)
        except Exception as e:
            print(f"  ❌ Error: {e}"); import traceback; traceback.print_exc(); continue
    if skipped: print(f"\n⏭️  Skipped {len(skipped)} games")
    if results:
        push_to_sheets(sheet,results); push_summary_tab(sheet,results)
        push_tracker_rows(sheet,results); print_tracker_reminder()
    print("\n🏁 Done!")


if __name__=="__main__":
    import sys
    args=sys.argv[1:]
    if args and args[0].lower()=="list":
        print(f"⚾ TODAY'S GAMES — {today_str()}")
        for i,g in enumerate(get_todays_games()):
            info=parse_game_info(g)
            print(f"  {i+1}. {info['away_team']} @ {info['home_team']} — {info['away_pitcher']} vs {info['home_pitcher']}")
    elif args and args[0].lower() not in ("help","-h"):
        filters=[a.lower() for a in args]
        _current_sheet=get_sheet(SHEET_NAME); sheet=_current_sheet
        load_calibration(sheet); load_ump_data(); create_input_tab(sheet)
        odds=get_mlb_odds()
        if odds: odds=get_oddspapi_fallback(odds)  # Pinnacle + gap fill
        rl=get_run_label(); cl=get_compare_label(rl); snapshot={}
        if cl: snapshot=load_odds_snapshot_from_sheet(sheet,cl)
        games=get_todays_games()
        matched=[g for g in games if any(f in f"{parse_game_info(g)['away_team']} {parse_game_info(g)['home_team']}".lower() for f in filters)]
        if not matched: print(f"❌ No games matching: {', '.join(args)}")
        else:
            results=[]
            for game in matched:
                try:
                    r=analyze_game(game,current_odds=odds,snapshot=snapshot)
                    if not r.get("skipped"): results.append(r); print_game_summary(r)
                except Exception as e: print(f"  ❌ {e}")
            if results:
                push_to_sheets(sheet,results); push_summary_tab(sheet,results); push_tracker_rows(sheet,results)
        print("\n🏁 Done!")
    else:
        main()
