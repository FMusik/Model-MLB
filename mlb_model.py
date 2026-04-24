"""
MLB Betting Model
=================
Pulls today's games from the MLB Stats API, calculates projections,
and pushes results to Google Sheets.

FIXES APPLIED 2026-04-23:
  - win_probability() now caps run differential at 2.5 runs max
  - Win probabilities clamped to max 65% before score_signal()
  - prob calibration factor now correctly parsed from R script
  - avg_predicted_prob was 0.007 (broken) — now ~0.55-0.65 (correct)
  - Overconfident STRONG signals should reduce significantly

Requirements:
    pip install mlbstatsapi gspread google-auth pandas requests

Setup:
    1. Place credentials.json in the same folder as this script
    2. Share your Google Sheet with the service account email
    3. Set SHEET_NAME below to match your Google Sheet name
    4. Run: python mlb_model.py
"""

import json
import math
import datetime
import requests
import pandas as pd
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

# Max win probability we'll ever assign to one side (prevents overconfidence)
MAX_WIN_PROB      = 0.65
# Max run differential used in win prob calc
MAX_RUN_DIFF      = 2.5

_current_sheet = None
_bp_games = {}

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
# MLB STATS API HELPERS
# ─────────────────────────────────────────────
BASE = "https://statsapi.mlb.com/api/v1"

def api_get(endpoint: str, params: dict = {}) -> dict:
    r = requests.get(f"{BASE}{endpoint}", params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def today_str() -> str:
    return datetime.date.today().strftime("%Y-%m-%d")

# ─────────────────────────────────────────────
# ODDS API
# ─────────────────────────────────────────────
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SNAPSHOT_TAB  = "📡 Line Movement"

def get_mlb_odds() -> dict:
    print("\n📡 Fetching odds from The Odds API...")
    all_odds = {}
    market_calls = [
        ("h2h",         "Moneyline"),
        ("spreads",     "Run Line"),
        ("totals",      "Game Total"),
        ("team_totals", "Team Totals"),
    ]
    for market, label in market_calls:
        try:
            url    = f"{ODDS_API_BASE}/sports/baseball_mlb/odds"
            params = {
                "apiKey":     ODDS_API_KEY,
                "regions":    "us",
                "markets":    market,
                "oddsFormat": "american",
                "dateFormat": "iso",
            }
            r    = requests.get(url, params=params, timeout=15)
            data = r.json()
            remaining = r.headers.get("x-requests-remaining", "?")
            if market == "h2h":
                print(f"   API requests remaining: {remaining}")
            if isinstance(data, list):
                for game in data:
                    away = game.get("away_team", "")
                    home = game.get("home_team", "")
                    key  = f"{away} @ {home}"
                    if key not in all_odds:
                        all_odds[key] = {
                            "away_team": away,
                            "home_team": home,
                            "game_time": game.get("commence_time", ""),
                        }
                    books = game.get("bookmakers", [])
                    book  = next((b for b in books if b["key"] == "draftkings"), None)
                    if not book and books:
                        book = books[0]
                    if not book:
                        continue
                    for mkt in book.get("markets", []):
                        mkt_key  = mkt.get("key")
                        outcomes = mkt.get("outcomes", [])
                        if mkt_key == "h2h":
                            for o in outcomes:
                                if o["name"] == away:
                                    all_odds[key]["away_ml"] = int(o["price"])
                                elif o["name"] == home:
                                    all_odds[key]["home_ml"] = int(o["price"])
                        elif mkt_key == "spreads":
                            for o in outcomes:
                                if o["name"] == away:
                                    all_odds[key]["away_rl_odds"] = int(o["price"])
                                    all_odds[key]["away_rl_line"] = o.get("point", -1.5)
                                elif o["name"] == home:
                                    all_odds[key]["home_rl_odds"] = int(o["price"])
                        elif mkt_key == "totals":
                            for o in outcomes:
                                if o["name"] == "Over":
                                    all_odds[key]["total_line"] = o.get("point")
                                    all_odds[key]["over_odds"]  = int(o["price"])
                                elif o["name"] == "Under":
                                    all_odds[key]["under_odds"] = int(o["price"])
                        elif mkt_key == "team_totals":
                            for o in outcomes:
                                team  = o.get("description", "")
                                name  = o.get("name", "")
                                price = int(o["price"])
                                point = o.get("point")
                                if team == away:
                                    if name == "Over":
                                        all_odds[key]["away_team_total"]   = point
                                        all_odds[key]["away_tt_over_odds"] = price
                                    else:
                                        all_odds[key]["away_tt_under_odds"] = price
                                elif team == home:
                                    if name == "Over":
                                        all_odds[key]["home_team_total"]   = point
                                        all_odds[key]["home_tt_over_odds"] = price
                                    else:
                                        all_odds[key]["home_tt_under_odds"] = price
            print(f"   ✅ {label} fetched")
        except Exception as e:
            print(f"   ⚠️  Could not fetch {label}: {e}")

    # F5
    try:
        url    = f"{ODDS_API_BASE}/sports/baseball_mlb/odds"
        params = {
            "apiKey":     ODDS_API_KEY,
            "regions":    "us",
            "markets":    "totals_h1,h2h_h1",
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
        r    = requests.get(url, params=params, timeout=15)
        data = r.json()
        if isinstance(data, list):
            for game in data:
                away = game.get("away_team", "")
                home = game.get("home_team", "")
                key  = f"{away} @ {home}"
                if key not in all_odds:
                    continue
                books = game.get("bookmakers", [])
                book  = next((b for b in books if b["key"] == "draftkings"), None)
                if not book and books:
                    book = books[0]
                if not book:
                    continue
                for mkt in book.get("markets", []):
                    outcomes = mkt.get("outcomes", [])
                    mkt_key  = mkt.get("key", "")
                    if mkt_key == "totals_h1":
                        for o in outcomes:
                            pt = o.get("point")
                            if pt is None:
                                continue
                            try:
                                pt_f = float(pt)
                            except:
                                continue
                            full_total = float(all_odds[key].get("total_line") or 9.0)
                            if pt_f >= full_total * 0.70:
                                continue
                            if o["name"] == "Over":
                                all_odds[key]["mkt_f5_line"]  = pt_f
                                all_odds[key]["f5_over_odds"] = int(o["price"])
                            elif o["name"] == "Under":
                                if all_odds[key].get("mkt_f5_line"):
                                    all_odds[key]["f5_under_odds"] = int(o["price"])
                    elif mkt_key == "h2h_h1":
                        for o in outcomes:
                            if o["name"] == away:
                                all_odds[key]["f5_away_ml"] = int(o["price"])
                            elif o["name"] == home:
                                all_odds[key]["f5_home_ml"] = int(o["price"])
            f5c = sum(1 for v in all_odds.values() if v.get("mkt_f5_line"))
            f5m = sum(1 for v in all_odds.values() if v.get("f5_away_ml"))
            print(f"   ✅ F5 totals: {f5c} games | F5 ML: {f5m}")
    except Exception as e:
        print(f"   ⚠️  Could not fetch F5 odds: {e}")

    print(f"   ✅ Odds fetched for {len(all_odds)} games")
    return all_odds

def save_odds_snapshot_to_sheet(sheet, odds: dict, run_label: str) -> None:
    today = today_str()
    try:
        try:
            ws = sheet.worksheet(SNAPSHOT_TAB)
        except Exception:
            ws = sheet.add_worksheet(SNAPSHOT_TAB, rows=500, cols=20)
            ws.append_row(["Date","Run","Game","Away ML","Home ML","Total Line",
                           "Over Odds","Under Odds","Away TT","Home TT","Saved At"])
        all_vals = ws.get_all_values()
        for row in all_vals[1:]:
            if len(row) >= 2 and row[0] == today and row[1] == run_label:
                print(f"  📡 Snapshot already saved for {today} {run_label} — skipping")
                return
        import datetime as dt
        saved_at = dt.datetime.now().strftime("%H:%M:%S")
        rows = []
        for game, o in odds.items():
            rows.append([today, run_label, game,
                         o.get("away_ml",""), o.get("home_ml",""),
                         o.get("total_line",""), o.get("over_odds",""),
                         o.get("under_odds",""), o.get("away_team_total",""),
                         o.get("home_team_total",""), saved_at])
        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            print(f"  📡 Snapshot saved: {len(rows)} games @ {run_label} {saved_at}")
    except Exception as e:
        print(f"  ⚠️  Could not save snapshot: {e}")

def load_odds_snapshot_from_sheet(sheet, compare_to: str = "6AM") -> dict:
    today = today_str()
    try:
        ws       = sheet.worksheet(SNAPSHOT_TAB)
        all_vals = ws.get_all_values()
        if len(all_vals) < 2:
            return {}
        snapshot = {}
        for row in all_vals[1:]:
            if len(row) < 3 or row[0] != today or row[1] != compare_to:
                continue
            game = row[2]
            def sv(idx):
                try: return row[idx] if idx < len(row) and row[idx] else None
                except: return None
            snapshot[game] = {
                "away_ml": sv(3), "home_ml": sv(4), "total_line": sv(5),
                "over_odds": sv(6), "under_odds": sv(7),
                "away_team_total": sv(8), "home_team_total": sv(9),
            }
        if snapshot:
            print(f"  📡 Loaded {compare_to} snapshot: {len(snapshot)} games")
        return snapshot
    except Exception:
        return {}

def detect_line_movement(current_odds: dict, snapshot: dict) -> dict:
    if not snapshot:
        return {}
    alerts = {}
    for game, curr in current_odds.items():
        snap = snapshot.get(game, {})
        if not snap:
            continue
        game_alerts = []
        for side, label in [("away_ml","Away ML"), ("home_ml","Home ML")]:
            c = curr.get(side)
            s = snap.get(side)
            if c and s:
                try:
                    move = int(c) - int(s)
                    if abs(move) >= 20:
                        d = "📈" if move > 0 else "📉"
                        game_alerts.append(f"{d} {label} moved {move:+d} ({s} → {c}) ⚡ SHARP MOVE")
                    elif abs(move) >= 10:
                        d = "↗️" if move > 0 else "↘️"
                        game_alerts.append(f"{d} {label} moved {move:+d} ({s} → {c})")
                except (ValueError, TypeError):
                    pass
        c_tot = curr.get("total_line")
        s_tot = snap.get("total_line")
        if c_tot and s_tot:
            try:
                move = float(c_tot) - float(s_tot)
                if abs(move) >= 1.0:
                    d = "📈" if move > 0 else "📉"
                    game_alerts.append(f"{d} Total moved {move:+.1f} ({s_tot} → {c_tot}) ⚡ SHARP MOVE")
                elif abs(move) >= 0.5:
                    d = "↗️" if move > 0 else "↘️"
                    game_alerts.append(f"{d} Total moved {move:+.1f} ({s_tot} → {c_tot})")
            except (ValueError, TypeError):
                pass
        if game_alerts:
            alerts[game] = game_alerts
    return alerts

def print_line_movement_report(alerts: dict, compare_label: str = "6AM") -> None:
    print(f"\n📡 LINE MOVEMENT REPORT (vs {compare_label} snapshot)")
    print(f"  {'─'*52}")
    if not alerts:
        print(f"  ✅ No significant movement detected")
        print(f"  {'─'*52}\n")
        return
    sharp_count = sum(1 for moves in alerts.values() for m in moves if "SHARP" in m)
    print(f"  🚨 Movement in {len(alerts)} games | {sharp_count} SHARP moves")
    for game, moves in alerts.items():
        sharp = any("SHARP" in m for m in moves)
        marker = "⚡" if sharp else "  "
        print(f"  {marker} {game}:")
        for m in moves:
            print(f"      {m}")
    print(f"  {'─'*52}")
    if sharp_count > 0:
        print(f"  💡 Sharp moves = institutional money moving lines")
    print()

def push_movement_to_sheet(sheet, alerts: dict, run_label: str, compare_label: str) -> None:
    import datetime as dt
    today    = today_str()
    saved_at = dt.datetime.now().strftime("%H:%M:%S")
    try:
        ws   = sheet.worksheet(SNAPSHOT_TAB)
        rows = []
        for game, moves in alerts.items():
            for move in moves:
                sharp = "⚡ SHARP" if "SHARP" in move else "↕️ Notable"
                rows.append([today, f"MOVE_{run_label}", game, move, sharp,
                             compare_label, saved_at])
        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
    except Exception:
        pass

def get_run_label() -> str:
    import datetime as dt
    hour = dt.datetime.now().hour
    if hour < 9:   return "6AM"
    elif hour < 14: return "12PM"
    else:           return "5PM"

def get_compare_label(run_label: str) -> str:
    if run_label == "12PM": return "6AM"
    elif run_label == "5PM": return "12PM"
    return None

def push_odds_to_input_tab(sheet, odds: dict) -> None:
    try:
        ws   = sheet.worksheet(INPUT_TAB_NAME)
        rows = ws.get_all_values()
        header_row_idx = None
        for i, row in enumerate(rows):
            if row and row[0] == "Game":
                header_row_idx = i
                break
        if header_row_idx is None:
            print("  ⚠️  Could not find header row in Input tab")
            return
        headers = rows[header_row_idx]
        MANUAL_COLS = [h for h in headers if str(h).startswith("BP") or
                       any(h == c for c in [
                           "Away ML Bet%","Away ML Money%","Home ML Bet%","Home ML Money%",
                           "Over Bet%","Over Money%","Under Bet%","Under Money%",
                           "Away Spread Bet%","Away Spread Money%",
                           "Home Spread Bet%","Home Spread Money%",
                           "YRFI Odds","NRFI Odds",
                       ])]
        bp_snapshot = {}
        for row in rows[header_row_idx + 1:]:
            if not row or not row[0]:
                continue
            game_name_snap = row[0]
            manual_data = {}
            for col_name in MANUAL_COLS:
                try:
                    ci  = headers.index(col_name)
                    val = row[ci] if ci < len(row) else ""
                    if val:
                        manual_data[col_name] = val
                except ValueError:
                    pass
            if manual_data:
                bp_snapshot[game_name_snap] = manual_data
        total_rows = len(rows)
        for row_idx in range(total_rows, header_row_idx + 1, -1):
            try:
                ws.delete_rows(row_idx)
            except Exception:
                pass
        def col_idx(name):
            try: return headers.index(name)
            except: return None
        new_rows = []
        for game_name, game_odds in odds.items():
            new_row = [""] * len(headers)
            new_row[0] = game_name
            col_map = {
                "Away ML":       game_odds.get("away_ml"),
                "Home ML":       game_odds.get("home_ml"),
                "Total Line":    game_odds.get("total_line"),
                "Over Odds":     game_odds.get("over_odds"),
                "Under Odds":    game_odds.get("under_odds"),
                "Away RL Odds":  game_odds.get("away_rl_odds"),
                "Home RL Odds":  game_odds.get("home_rl_odds"),
                "F5 Total":      game_odds.get("mkt_f5_line"),
                "F5 Over Odds":  game_odds.get("f5_over_odds"),
                "F5 Under Odds": game_odds.get("f5_under_odds"),
                "F5 Away ML":    game_odds.get("f5_away_ml"),
                "F5 Home ML":    game_odds.get("f5_home_ml"),
                "Away TT Line":  game_odds.get("away_team_total"),
                "Away TT Over":  game_odds.get("away_tt_over_odds"),
                "Away TT Under": game_odds.get("away_tt_under_odds"),
                "Home TT Line":  game_odds.get("home_team_total"),
                "Home TT Over":  game_odds.get("home_tt_over_odds"),
                "Home TT Under": game_odds.get("home_tt_under_odds"),
            }
            for col_name, val in col_map.items():
                ci = col_idx(col_name)
                if ci is not None and val is not None:
                    new_row[ci] = val
            saved_manual = bp_snapshot.get(game_name, {})
            if not saved_manual:
                for snapped_game, snapped_data in bp_snapshot.items():
                    parts_a = set(game_name.lower().replace(" @ "," ").split())
                    parts_b = set(snapped_game.lower().replace(" @ "," ").split())
                    if len(parts_a & parts_b) >= 2:
                        saved_manual = snapped_data
                        break
            for col_name, val in saved_manual.items():
                ci = col_idx(col_name)
                if ci is not None:
                    new_row[ci] = val
            new_rows.append(new_row)
        if new_rows:
            ws.append_rows(new_rows, value_input_option="USER_ENTERED")
            print(f"  ✅ Input tab refreshed with {len(new_rows)} games for today")
    except Exception as e:
        print(f"  ⚠️  Could not update Input tab with odds: {e}")


# ─────────────────────────────────────────────
# SECTION 1 — TODAY'S GAMES
# ─────────────────────────────────────────────
def get_todays_games() -> list:
    data  = api_get("/schedule", {"sportId": 1, "date": today_str(),
                                  "hydrate": "probablePitcher,venue,weather,lineups"})
    games = []
    seen  = set()
    for date_block in data.get("dates", []):
        for g in date_block.get("games", []):
            gid = g.get("gamePk")
            if gid and gid not in seen:
                seen.add(gid)
                games.append(g)
    print(f"✅ Found {len(games)} games today ({today_str()})")
    return games

STADIUM_COORDS = {
    "Coors Field":                  (39.7559,  -104.9942),
    "Great American Ball Park":     (39.0979,   -84.5082),
    "Fenway Park":                  (42.3467,   -71.0972),
    "Globe Life Field":             (32.7473,   -97.0825),
    "Yankee Stadium":               (40.8296,   -73.9262),
    "Oriole Park at Camden Yards":  (39.2838,   -76.6217),
    "Citizens Bank Park":           (39.9061,   -75.1665),
    "Wrigley Field":                (41.9484,   -87.6553),
    "Truist Park":                  (33.8908,   -84.4678),
    "American Family Field":        (43.0280,   -87.9712),
    "Kauffman Stadium":             (39.0517,   -94.4803),
    "Progressive Field":            (41.4962,   -81.6852),
    "Nationals Park":               (38.8730,   -77.0074),
    "Target Field":                 (44.9817,   -93.2781),
    "Rogers Centre":                (43.6414,   -79.3894),
    "Angel Stadium":                (33.8003,  -117.8827),
    "Comerica Park":                (42.3390,   -83.0485),
    "PNC Park":                     (40.4469,   -80.0057),
    "Busch Stadium":                (38.6226,   -90.1928),
    "Guaranteed Rate Field":        (41.8300,   -87.6338),
    "Rate Field":                   (41.8300,   -87.6338),
    "Daikin Park":                  (29.7572,   -95.3555),
    "Minute Maid Park":             (29.7572,   -95.3555),
    "loanDepot park":               (25.7781,   -80.2197),
    "LoanDepot Park":               (25.7781,   -80.2197),
    "UNIQLO Field":                 (34.0739,  -118.2400),
    "Dodger Stadium":               (34.0739,  -118.2400),
    "Chase Field":                  (33.4453,  -112.0667),
    "Citi Field":                   (40.7571,   -73.8458),
    "G.M. Steinbrenner Field":      (27.9683,   -82.5053),
    "Tropicana Field":              (27.7683,   -82.6534),
    "T-Mobile Park":                (47.5914,  -122.3325),
    "Oracle Park":                  (37.7786,  -122.3893),
    "Petco Park":                   (32.7076,  -117.1570),
    "Sutter Health Park":           (38.5802,  -121.4997),
    "Sahlen Field":                 (42.8867,   -78.8784),
}

def get_stadium_coords(venue: str):
    if venue in STADIUM_COORDS:
        return STADIUM_COORDS[venue]
    venue_lower = venue.lower()
    for park, coords in STADIUM_COORDS.items():
        if park.lower() in venue_lower or venue_lower in park.lower():
            return coords
    return None

def fetch_weather_for_venue(venue: str, game_time_str: str) -> dict:
    coords = get_stadium_coords(venue)
    if not coords:
        return {}
    lat, lon = coords
    try:
        import datetime as dt
        game_time = dt.datetime.fromisoformat(
            game_time_str.replace("Z", "+00:00")
        ) if game_time_str else dt.datetime.now(dt.timezone.utc)
        date_str  = game_time.strftime("%Y-%m-%d")
        game_hour = game_time.hour
        url = (f"https://api.open-meteo.com/v1/forecast"
               f"?latitude={lat}&longitude={lon}"
               f"&hourly=temperature_2m,windspeed_10m,winddirection_10m,weathercode"
               f"&temperature_unit=fahrenheit&windspeed_unit=mph"
               f"&timezone=auto&start_date={date_str}&end_date={date_str}")
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return {}
        data   = r.json()
        hourly = data.get("hourly", {})
        times  = hourly.get("time", [])
        temps  = hourly.get("temperature_2m", [])
        winds  = hourly.get("windspeed_10m", [])
        dirs   = hourly.get("winddirection_10m", [])
        codes  = hourly.get("weathercode", [])
        best_idx = 0
        for i, t in enumerate(times):
            try:
                h = int(t.split("T")[1].split(":")[0])
                if abs(h - game_hour) < abs(int(times[best_idx].split("T")[1].split(":")[0]) - game_hour):
                    best_idx = i
            except Exception:
                pass
        temp     = round(temps[best_idx]) if best_idx < len(temps) else None
        wind_spd = round(winds[best_idx]) if best_idx < len(winds) else None
        wind_deg = dirs[best_idx] if best_idx < len(dirs) else None
        wcode    = codes[best_idx] if best_idx < len(codes) else None
        def deg_to_compass(deg):
            if deg is None: return ""
            d = ["N","NE","E","SE","S","SW","W","NW"]
            return d[int((deg + 22.5) / 45) % 8]
        def code_to_condition(code):
            if code is None: return "Unknown"
            if code == 0: return "Clear"
            if code in (1,2,3): return "Partly Cloudy"
            if code in range(51,68): return "Rain"
            if code in range(71,78): return "Snow"
            if code in range(80,83): return "Showers"
            if code in range(95,100): return "Thunderstorm"
            return "Cloudy"
        return {
            "temp":      temp,
            "wind":      f"{wind_spd} mph {deg_to_compass(wind_deg)}" if wind_spd else "N/A",
            "wind_spd":  wind_spd,
            "wind_dir":  deg_to_compass(wind_deg),
            "wind_deg":  wind_deg,
            "condition": code_to_condition(wcode),
            "source":    "Open-Meteo ✅"
        }
    except Exception:
        return {}

def parse_game_info(game: dict) -> dict:
    away = game["teams"]["away"]
    home = game["teams"]["home"]
    away_pitcher = away.get("probablePitcher", {})
    home_pitcher = home.get("probablePitcher", {})
    venue     = game.get("venue", {}).get("name", "Unknown")
    weather   = game.get("weather", {})
    game_time = game.get("gameDate", "")
    mlb_temp  = weather.get("temp", "")
    mlb_wind  = weather.get("wind", "")
    if not mlb_temp or mlb_temp == "N/A" or str(mlb_temp).strip() == "":
        live_wx   = fetch_weather_for_venue(venue, game_time)
        temp      = live_wx.get("temp", "N/A")
        wind      = live_wx.get("wind", "N/A")
        condition = live_wx.get("condition", "N/A")
        wx_source = live_wx.get("source", "N/A")
    else:
        temp      = mlb_temp
        wind      = mlb_wind
        condition = weather.get("condition", "N/A")
        wx_source = "MLB API"
    return {
        "game_id":           game["gamePk"],
        "game_time":         game_time,
        "venue":             venue,
        "away_team":         away["team"]["name"],
        "home_team":         home["team"]["name"],
        "away_team_id":      away["team"]["id"],
        "home_team_id":      home["team"]["id"],
        "away_pitcher":      away_pitcher.get("fullName", "TBD"),
        "away_pitcher_id":   away_pitcher.get("id"),
        "home_pitcher":      home_pitcher.get("fullName", "TBD"),
        "home_pitcher_id":   home_pitcher.get("id"),
        "weather_temp":      temp,
        "weather_wind":      wind,
        "weather_condition": condition,
        "weather_source":    wx_source,
    }

def check_game_timing(game: dict, info: dict) -> str:
    try:
        game_time_str = info.get("game_time", "")
        if not game_time_str:
            return "⏰ Unknown"
        game_time  = datetime.datetime.fromisoformat(game_time_str.replace("Z", "+00:00"))
        now        = datetime.datetime.now(datetime.timezone.utc)
        diff_hours = (now - game_time).total_seconds() / 3600
        status     = game.get("status", {})
        abstract   = status.get("abstractGameState", "")
        detailed   = status.get("detailedState", "")
        if abstract == "Final" or "Final" in detailed:
            return "🏁 Final"
        elif abstract == "Live" or "In Progress" in detailed:
            if diff_hours >= 0.5:
                return "⚡ In Progress"
            else:
                return "🔔 Starting Soon"
        elif diff_hours > 3:
            return f"⚠️ Started {diff_hours:.1f}hrs ago"
        elif diff_hours > 0.5:
            return f"⚡ In Progress ({diff_hours:.1f}hrs)"
        elif diff_hours > -1:
            return "🔔 Starting Soon"
        else:
            return f"⏰ {abs(diff_hours):.1f}hrs until first pitch"
    except Exception:
        return "⏰ Unknown"


# ─────────────────────────────────────────────
# SECTION 2 — PITCHER STATS
# ─────────────────────────────────────────────
def _get_pitcher_season(pitcher_id: int, season: int) -> dict:
    try:
        data   = api_get(f"/people/{pitcher_id}/stats",
                         {"stats":"season","group":"pitching","season":season,"sportId":1})
        splits = data.get("stats",[{}])[0].get("splits",[])
        if not splits:
            return {}
        s  = splits[0]["stat"]
        ip = float(s.get("inningsPitched",0) or 0)
        gs = int(s.get("gamesStarted",0) or 0)
        if ip == 0:
            return {}
        return {
            "era":    float(s.get("era",0) or 0),
            "fip":    _calc_fip(s),
            "whip":   float(s.get("whip",0) or 0),
            "k9":     float(s.get("strikeoutsPer9Inn",0) or 0),
            "bb9":    float(s.get("walksPer9Inn",0) or 0),
            "hr9":    float(s.get("homeRunsPer9",0) or 0),
            "ip":     ip,
            "gs":     gs,
            "wins":   int(s.get("wins",0) or 0),
            "losses": int(s.get("losses",0) or 0),
        }
    except:
        return {}

def get_pitcher_stats(pitcher_id: int) -> dict:
    if not pitcher_id:
        return {}
    s26 = _get_pitcher_season(pitcher_id, SEASON)
    s25 = _get_pitcher_season(pitcher_id, SEASON - 1)
    s24 = _get_pitcher_season(pitcher_id, SEASON - 2)
    gs26 = s26.get("gs", 0)
    if gs26 >= 8:
        weights = [(s26,0.60),(s25,0.40),(s24,0.00)]
        label   = f"2026({gs26}GS)+2025"
    elif gs26 >= 3:
        weights = [(s26,0.30),(s25,0.50),(s24,0.20)]
        label   = f"2026({gs26}GS)+2025+2024"
    elif gs26 >= 1:
        weights = [(s26,0.10),(s25,0.60),(s24,0.30)]
        label   = f"2026({gs26}GS small)+2025+2024"
    else:
        weights = [(s26,0.00),(s25,0.70),(s24,0.30)]
        label   = "2025+2024"
    def blend_stat(key, default=0.0):
        total, weight = 0.0, 0.0
        for s, w in weights:
            if w > 0 and s.get(key):
                total  += float(s[key]) * w
                weight += w
        return round(total / weight, 3) if weight > 0 else default
    return {
        "era":           blend_stat("era", 4.50),
        "fip":           blend_stat("fip", 4.50),
        "whip":          blend_stat("whip", 1.30),
        "k9":            blend_stat("k9",   8.00),
        "bb9":           blend_stat("bb9",  3.00),
        "hr9":           blend_stat("hr9",  1.20),
        "ip":            s26.get("ip", s25.get("ip", 0)),
        "games_started": gs26,
        "wins":          s26.get("wins", 0),
        "losses":        s26.get("losses", 0),
        "data_label":    label,
        "era_2026": s26.get("era","N/A"), "era_2025": s25.get("era","N/A"),
        "gs_2026":  gs26,                 "gs_2025":  s25.get("gs",0),
    }

def _calc_fip(s: dict) -> float:
    try:
        hr = float(s.get("homeRuns",0) or 0)
        bb = float(s.get("baseOnBalls",0) or 0)
        k  = float(s.get("strikeOuts",0) or 0)
        ip = float(s.get("inningsPitched",1) or 1)
        return round((13*hr + 3*bb - 2*k) / ip + 3.10, 2)
    except:
        return 0.0


# ─────────────────────────────────────────────
# SECTION 3 — TEAM OFFENSE
# ─────────────────────────────────────────────
def _get_team_offense_season(team_id: int, season: int) -> dict:
    try:
        data   = api_get(f"/teams/{team_id}/stats",
                         {"stats":"season","group":"hitting","season":season,"sportId":1})
        splits = data.get("stats",[{}])[0].get("splits",[])
        if not splits:
            return {}
        s  = splits[0]["stat"]
        gp = max(int(s.get("gamesPlayed",1)),1)
        return {
            "runs_per_game": round(float(s.get("runs",0))/gp, 2),
            "ops":  float(s.get("ops",0) or 0),
            "avg":  float(s.get("avg",0) or 0),
            "obp":  float(s.get("obp",0) or 0),
            "slg":  float(s.get("slg",0) or 0),
            "hr":   int(s.get("homeRuns",0) or 0),
            "games": gp,
            "k_pct":  round(float(s.get("strikeOuts",0)) / max(float(s.get("atBats",1)),1)*100,1),
            "bb_pct": round(float(s.get("baseOnBalls",0)) / max(float(s.get("plateAppearances",1)),1)*100,1),
        }
    except:
        return {}

def get_team_offense(team_id: int) -> dict:
    s26 = _get_team_offense_season(team_id, SEASON)
    s25 = _get_team_offense_season(team_id, SEASON - 1)
    s24 = _get_team_offense_season(team_id, SEASON - 2)
    games26 = s26.get("games", 0)
    if games26 >= 40:
        weights = [(s26,0.65),(s25,0.35),(s24,0.00)]
    elif games26 >= 20:
        weights = [(s26,0.45),(s25,0.40),(s24,0.15)]
    elif games26 >= 10:
        weights = [(s26,0.25),(s25,0.50),(s24,0.25)]
    else:
        weights = [(s26,0.10),(s25,0.60),(s24,0.30)]
    def blend(key, default=0.0):
        total, w_sum = 0.0, 0.0
        for s, w in weights:
            if w > 0 and s.get(key):
                total  += float(s[key]) * w
                w_sum  += w
        return round(total / w_sum, 3) if w_sum > 0 else default
    return {
        "runs_per_game": blend("runs_per_game", 4.50),
        "ops":  blend("ops",  0.720),
        "avg":  blend("avg",  0.250),
        "obp":  blend("obp",  0.320),
        "slg":  blend("slg",  0.400),
        "k_pct":  blend("k_pct",  22.0),
        "bb_pct": blend("bb_pct",  8.5),
        "games_2026": games26,
    }


# ─────────────────────────────────────────────
# SECTION 4 — BULLPEN
# ─────────────────────────────────────────────
def get_bullpen_stats(team_id: int) -> dict:
    try:
        data = api_get(f"/teams/{team_id}/stats",
                       {"stats":"season","group":"pitching","season":SEASON,
                        "sportId":1,"playerPool":"qualifier"})
        eras, whips, ks, bbs, ips = [], [], [], [], []
        for split in data.get("stats",[{}])[0].get("splits",[]):
            s  = split.get("stat",{})
            gs = int(s.get("gamesStarted",0) or 0)
            g  = int(s.get("gamesPitched",0) or 0)
            ip = float(s.get("inningsPitched",0) or 0)
            if gs == 0 and g > 0 and ip > 0:
                eras.append(float(s.get("era",0) or 0))
                whips.append(float(s.get("whip",0) or 0))
                ks.append(float(s.get("strikeoutsPer9Inn",0) or 0))
                bbs.append(float(s.get("walksPer9Inn",0) or 0))
                ips.append(ip)
        if not eras:
            return {}
        def wavg(vals, weights):
            total_w = sum(weights)
            if total_w == 0: return 0
            return round(sum(v*w for v,w in zip(vals,weights)) / total_w, 2)
        return {
            "bullpen_era":  wavg(eras,ips),
            "bullpen_whip": wavg(whips,ips),
            "bullpen_k9":   wavg(ks,ips),
            "bullpen_bb9":  wavg(bbs,ips),
            "relievers":    len(eras),
        }
    except Exception as e:
        return {}


# ─────────────────────────────────────────────
# SECTION 5 — LINEUPS + BATTER STATS
# ─────────────────────────────────────────────
def get_lineup(game: dict, side: str) -> list:
    try:
        lineups = game.get("lineups", {})
        batters = lineups.get(f"{side}Players", [])
        return [p.get("fullName","Unknown") for p in batters[:9]]
    except:
        return []

def get_lineup_with_ids(game: dict, side: str) -> list:
    try:
        lineups = game.get("lineups", {})
        batters = lineups.get(f"{side}Players", [])
        return [{"name": p.get("fullName","Unknown"), "id": p.get("id")} for p in batters[:9]]
    except:
        return []

def get_batter_stats(player_id: int, vs_hand: str = None) -> dict:
    if not player_id:
        return {}
    try:
        data26   = api_get(f"/people/{player_id}/stats",
                           {"stats":"season","group":"hitting","season":SEASON,"sportId":1})
        splits26 = data26.get("stats",[{}])[0].get("splits",[])
        data25   = api_get(f"/people/{player_id}/stats",
                           {"stats":"season","group":"hitting","season":SEASON-1,"sportId":1})
        splits25 = data25.get("stats",[{}])[0].get("splits",[])
        s26 = splits26[0]["stat"] if splits26 else {}
        s25 = splits25[0]["stat"] if splits25 else {}
        pa26 = int(s26.get("plateAppearances",0) or 0)
        if pa26 >= 100:   w26, w25 = 0.70, 0.30
        elif pa26 >= 50:  w26, w25 = 0.50, 0.50
        elif pa26 >= 20:  w26, w25 = 0.30, 0.70
        else:             w26, w25 = 0.10, 0.90
        def bblend(key, default=0.0):
            v26 = float(s26.get(key,0) or 0)
            v25 = float(s25.get(key,0) or 0)
            if v26 and v25: return round(v26*w26 + v25*w25, 3)
            elif v26: return round(v26, 3)
            elif v25: return round(v25, 3)
            return default
        result = {
            "avg": bblend("avg"), "obp": bblend("obp"),
            "slg": bblend("slg"), "ops": bblend("ops"),
            "hr":  int(s26.get("homeRuns",0) or 0),
            "pa_2026": pa26,
        }
        if vs_hand:
            try:
                split_data = api_get(f"/people/{player_id}/stats",
                                     {"stats":"statSplits","group":"hitting","season":SEASON,
                                      "sportId":1,"sitCodes":f"v{vs_hand}"})
                split_splits = split_data.get("stats",[{}])[0].get("splits",[])
                if split_splits:
                    ss = split_splits[0]["stat"]
                    split_ab = int(ss.get("atBats",0) or 0)
                    if split_ab >= 30:
                        result[f"vs_{vs_hand}_ops"] = float(ss.get("ops",0) or 0)
                        result[f"vs_{vs_hand}_avg"] = float(ss.get("avg",0) or 0)
                    else:
                        career = api_get(f"/people/{player_id}/stats",
                                         {"stats":"careerStatSplits","group":"hitting",
                                          "sportId":1,"sitCodes":f"v{vs_hand}"})
                        cs = career.get("stats",[{}])[0].get("splits",[])
                        if cs:
                            css = cs[0]["stat"]
                            result[f"vs_{vs_hand}_ops"] = float(css.get("ops",0) or 0)
                            result[f"vs_{vs_hand}_avg"] = float(css.get("avg",0) or 0)
            except:
                pass
        return result
    except Exception:
        return {}

def get_recent_team_offense(team_id: int, last_n: int = 15) -> dict:
    try:
        data = api_get(f"/teams/{team_id}/stats",
                       {"stats":"byDateRange","group":"hitting","season":SEASON,"sportId":1,
                        "startDate":(datetime.date.today()-datetime.timedelta(days=last_n)).strftime("%Y-%m-%d"),
                        "endDate":today_str()})
        splits = data.get("stats",[{}])[0].get("splits",[])
        if not splits:
            return {}
        s = splits[0]["stat"]
        games = max(int(s.get("gamesPlayed",1)),1)
        return {
            "recent_rpg":  round(float(s.get("runs",0))/games, 2),
            "recent_ops":  float(s.get("ops",0) or 0),
            "recent_obp":  float(s.get("obp",0) or 0),
            "recent_avg":  float(s.get("avg",0) or 0),
            "recent_games": games,
        }
    except Exception:
        return {}

def get_home_away_splits(team_id: int, side: str) -> dict:
    try:
        sit_code = "h" if side == "home" else "a"
        data = api_get(f"/teams/{team_id}/stats",
                       {"stats":"statSplits","group":"hitting","season":SEASON,
                        "sportId":1,"sitCodes":sit_code})
        splits = data.get("stats",[{}])[0].get("splits",[])
        if not splits:
            return {}
        s = splits[0]["stat"]
        games = max(int(s.get("gamesPlayed",1)),1)
        return {
            f"{side}_rpg": round(float(s.get("runs",0))/games, 2),
            f"{side}_ops": float(s.get("ops",0) or 0),
            f"{side}_obp": float(s.get("obp",0) or 0),
            f"{side}_avg": float(s.get("avg",0) or 0),
        }
    except Exception:
        return {}

def get_h2h_record(away_team_id: int, home_team_id: int) -> dict:
    season_weights = {SEASON:0.30, SEASON-1:0.25, SEASON-2:0.20, SEASON-3:0.15, SEASON-4:0.10}
    wins, losses, games = 0, 0, 0
    weighted_total, total_w = 0.0, 0.0
    raw_total_runs = 0
    for season, weight in season_weights.items():
        try:
            data = api_get("/schedule",
                           {"sportId":1,"season":season,"teamId":away_team_id,
                            "opponentId":home_team_id,"gameType":"R"})
            season_runs, season_games = 0, 0
            for date_block in data.get("dates",[]):
                for g in date_block.get("games",[]):
                    if g.get("status",{}).get("abstractGameState") != "Final":
                        continue
                    teams      = g.get("teams",{})
                    away       = teams.get("away",{})
                    home_t     = teams.get("home",{})
                    away_score = away.get("score",0) or 0
                    home_score = home_t.get("score",0) or 0
                    if away.get("team",{}).get("id") == away_team_id:
                        if away_score > home_score: wins += 1
                        else: losses += 1
                    season_runs  += (away_score + home_score)
                    season_games += 1
                    games        += 1
                    raw_total_runs += (away_score + home_score)
            if season_games > 0:
                weighted_total += (season_runs/season_games) * weight
                total_w        += weight
        except:
            continue
    if games == 0:
        return {}
    w_avg_total = round(weighted_total/total_w, 2) if total_w > 0 else round(raw_total_runs/games, 2)
    return {
        "h2h_wins":      wins,
        "h2h_losses":    losses,
        "h2h_games":     games,
        "h2h_avg_total": w_avg_total,
        "h2h_win_pct":   round(wins/games, 3),
    }

def get_batter_vs_pitcher(batter_id: int, pitcher_id: int) -> dict:
    if not batter_id or not pitcher_id:
        return {}
    try:
        data   = api_get(f"/people/{batter_id}/stats",
                         {"stats":"vsPlayer","group":"hitting","season":SEASON,
                          "sportId":1,"opposingPlayerId":pitcher_id})
        splits = data.get("stats",[{}])[0].get("splits",[])
        if not splits:
            data   = api_get(f"/people/{batter_id}/stats",
                             {"stats":"vsPlayerTotal","group":"hitting","sportId":1,
                              "opposingPlayerId":pitcher_id})
            splits = data.get("stats",[{}])[0].get("splits",[])
        if not splits:
            return {}
        s  = splits[0]["stat"]
        ab = int(s.get("atBats",0) or 0)
        if ab < 3:
            return {}
        return {
            "ab":  ab,
            "avg": float(s.get("avg",0) or 0),
            "ops": float(s.get("ops",0) or 0),
            "hr":  int(s.get("homeRuns",0) or 0),
            "h":   int(s.get("hits",0) or 0),
        }
    except:
        return {}

def get_lineup_vs_pitcher_ops(lineup: list, pitcher_id: int, vs_hand: str) -> float:
    if not lineup:
        return 0.720
    weights = [1.5,1.4,1.3,1.2,1.1,1.0,0.9,0.8,0.7]
    total_ops, total_weight = 0.0, 0.0
    for i, batter in enumerate(lineup[:9]):
        pid = batter.get("id") if isinstance(batter, dict) else None
        if not pid:
            continue
        matchup    = get_batter_vs_pitcher(pid, pitcher_id)
        matchup_ops = matchup.get("ops") if matchup else None
        hand_stats = get_batter_stats(pid, vs_hand)
        hand_ops   = hand_stats.get(f"vs_{vs_hand}_ops") or hand_stats.get("ops", 0.720)
        final_ops  = (matchup_ops*0.50 + hand_ops*0.50) if matchup_ops else hand_ops
        w = weights[i] if i < len(weights) else 0.7
        total_ops   += final_ops * w
        total_weight += w
    return round(total_ops/total_weight, 3) if total_weight > 0 else 0.720

def get_matchup_summary(lineup: list, pitcher_id: int, pitcher_name: str) -> str:
    highlights = []
    for batter in lineup[:6]:
        pid  = batter.get("id") if isinstance(batter, dict) else None
        name = batter.get("name") if isinstance(batter, dict) else str(batter)
        if not pid:
            continue
        m = get_batter_vs_pitcher(pid, pitcher_id)
        if m and m.get("ab",0) >= 5:
            highlights.append(f"{name}: {m['ab']} AB, .{int(m['avg']*1000):03d} AVG, {m['hr']} HR vs {pitcher_name}")
    return " | ".join(highlights) if highlights else "No significant matchup history"

def get_pitcher_recent_form(pitcher_id: int, last_n: int = 3) -> dict:
    if not pitcher_id:
        return {}
    try:
        data   = api_get(f"/people/{pitcher_id}/stats",
                         {"stats":"gameLog","group":"pitching","season":SEASON,"sportId":1})
        splits = data.get("stats",[{}])[0].get("splits",[])
        starts = [s for s in splits if int(s.get("stat",{}).get("gamesStarted",0)) > 0]
        starts = starts[-last_n:]
        if not starts:
            return {}
        eras, whips, k9s, ips, runs_allowed = [], [], [], [], []
        for s in starts:
            stat = s.get("stat",{})
            ip   = float(stat.get("inningsPitched",0) or 0)
            er   = float(stat.get("earnedRuns",0) or 0)
            h    = float(stat.get("hits",0) or 0)
            bb   = float(stat.get("baseOnBalls",0) or 0)
            k    = float(stat.get("strikeOuts",0) or 0)
            r    = float(stat.get("runs",0) or 0)
            if ip > 0:
                eras.append(round((er/ip)*9,2))
                whips.append(round((h+bb)/ip,2))
                k9s.append(round((k/ip)*9,2))
                ips.append(ip)
                runs_allowed.append(r)
        if not eras:
            return {}
        def form_score(eras, ips):
            avg_era = sum(eras)/len(eras)
            avg_ip  = sum(ips)/len(ips)
            if avg_era <= 2.50 and avg_ip >= 6.0: return "🔥 HOT"
            elif avg_era <= 3.50 and avg_ip >= 5.5: return "✅ SOLID"
            elif avg_era <= 4.50: return "➡️ AVERAGE"
            elif avg_era <= 6.00: return "❄️ COLD"
            else: return "🚨 STRUGGLING"
        return {
            "recent_era":      round(sum(eras)/len(eras), 2),
            "recent_whip":     round(sum(whips)/len(whips), 2),
            "recent_k9":       round(sum(k9s)/len(k9s), 2),
            "recent_avg_ip":   round(sum(ips)/len(ips), 2),
            "recent_avg_runs": round(sum(runs_allowed)/len(runs_allowed), 2),
            "recent_starts":   len(starts),
            "recent_form_score": form_score(eras, ips),
        }
    except Exception:
        return {}

def get_bullpen_availability(team_id: int) -> dict:
    try:
        yesterday = (datetime.date.today()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        data = api_get("/schedule",
                       {"sportId":1,"date":yesterday,"teamId":team_id,"hydrate":"boxscore"})
        used_yesterday, tired_names = [], []
        for date_block in data.get("dates",[]):
            for g in date_block.get("games",[]):
                bs = g.get("boxscore",{})
                for side in ["away","home"]:
                    t = bs.get("teams",{}).get(side,{})
                    if t.get("team",{}).get("id") != team_id:
                        continue
                    pitchers = t.get("pitchers",[])
                    players  = t.get("players",{})
                    for pid in pitchers:
                        key   = f"ID{pid}"
                        p     = players.get(key,{})
                        stats = p.get("stats",{}).get("pitching",{})
                        ip    = float(stats.get("inningsPitched",0) or 0)
                        gs    = int(stats.get("gamesStarted",0) or 0)
                        name  = p.get("person",{}).get("fullName",f"Player {pid}")
                        if gs == 0 and ip > 0:
                            used_yesterday.append({"name":name,"ip":ip})
                            tired_names.append(name)
        total_score = sum(2 + (2 if p["ip"] >= 1.0 else 0) for p in used_yesterday if p["ip"] > 0)
        available_count = max(0, 7 - len(tired_names))
        if total_score <= 2:   status = "✅ Fresh"
        elif total_score <= 7: status = "⚠️ Moderately Used"
        else:                  status = "🚨 Heavily Used"
        return {
            "bp_used_yesterday": ", ".join(tired_names) if tired_names else "None",
            "bp_tired_count":    len(tired_names),
            "bp_tiredness_score": total_score,
            "bp_available_est":  available_count,
            "bp_availability":   status,
        }
    except Exception:
        return {
            "bp_used_yesterday": "Unknown", "bp_tired_count": 0,
            "bp_available_est": 6, "bp_availability": "Unknown",
        }


# ─────────────────────────────────────────────
# KELLY + ROI
# ─────────────────────────────────────────────
BANKROLL       = 1000.0
KELLY_FRACTION = 0.25
MAX_BET_PCT    = 0.05
MIN_BET        = 5.0

def kelly_bet_size(win_prob: float, american_odds: int, bankroll: float = BANKROLL) -> dict:
    try:
        if american_odds > 0:
            decimal_odds = american_odds/100 + 1
        else:
            decimal_odds = 100/abs(american_odds) + 1
        b = decimal_odds - 1
        p = win_prob
        q = 1 - p
        kelly_f = (b*p - q) / b
        if kelly_f <= 0:
            return {"bet_dollars":0,"bet_units":0,"kelly_pct":0,
                    "edge_pct":round((b*p-q)*100,2),"verdict":"❌ No edge"}
        frac_kelly  = kelly_f * KELLY_FRACTION
        capped      = min(frac_kelly, MAX_BET_PCT)
        bet_dollars = max(MIN_BET, round(bankroll*capped, 2))
        bet_units   = round(bet_dollars/(bankroll/100), 2)
        return {
            "bet_dollars": bet_dollars, "bet_units": bet_units,
            "kelly_pct":   round(kelly_f*100, 2),
            "edge_pct":    round((b*p-q)*100, 2),
            "verdict":     f"✅ Bet ${bet_dollars:.2f} ({bet_units:.1f}u)",
        }
    except Exception:
        return {"bet_dollars":0,"bet_units":0,"kelly_pct":0,"verdict":"⚠️ Error"}

def calculate_roi(sheet) -> dict:
    try:
        ws       = sheet.worksheet("📊 Tracker")
        all_vals = ws.get_all_values()
        if len(all_vals) < 2:
            return {}
        header_row = None
        for i, row in enumerate(all_vals):
            if row and "Hit/Miss" in row:
                header_row = i
                break
        if header_row is None:
            return {}
        headers   = all_vals[header_row]
        hm_col    = headers.index("Hit/Miss") if "Hit/Miss" in headers else None
        odds_col  = headers.index("Market Odds") if "Market Odds" in headers else None
        signal_col= headers.index("Our Signal") if "Our Signal" in headers else None
        if hm_col is None:
            return {}
        wins = losses = pushes = 0
        total_profit = 0.0
        signal_roi   = {}
        for row in all_vals[header_row + 1:]:
            if not row or len(row) <= hm_col:
                continue
            hm = row[hm_col].strip().upper()
            if hm not in ("WIN","WON","W","LOSS","L","PUSH"):
                continue
            odds_val = -110
            if odds_col and odds_col < len(row):
                try: odds_val = int(float(row[odds_col]))
                except: odds_val = -110
            if not odds_val: odds_val = -110
            profit_if_win = odds_val/100 if odds_val > 0 else 100/abs(odds_val)
            signal = row[signal_col].strip() if signal_col and signal_col < len(row) else "Unknown"
            if hm in ("WIN","WON","W"):
                wins += 1; total_profit += profit_if_win
                signal_roi.setdefault(signal,{"profit":0,"bets":0})
                signal_roi[signal]["profit"] += profit_if_win
                signal_roi[signal]["bets"]   += 1
            elif hm in ("LOSS","L"):
                losses += 1; total_profit -= 1.0
                signal_roi.setdefault(signal,{"profit":0,"bets":0})
                signal_roi[signal]["profit"] -= 1.0
                signal_roi[signal]["bets"]   += 1
            elif hm == "PUSH":
                pushes += 1
        total_bets = wins + losses
        win_rate   = round(wins/total_bets*100,1) if total_bets > 0 else 0
        roi_pct    = round(total_profit/total_bets*100,2) if total_bets > 0 else 0
        return {
            "wins":wins,"losses":losses,"pushes":pushes,"total_bets":total_bets,
            "win_rate":win_rate,"total_profit":round(total_profit,2),
            "roi_pct":roi_pct,"signal_roi":signal_roi,
        }
    except Exception:
        return {}

def print_roi_report(sheet) -> None:
    print("\n💰 ROI TRACKER")
    print("================")
    roi = calculate_roi(sheet)
    if not roi:
        print("  No results data yet — fill in Hit/Miss in Tracker tab")
        return
    profit = roi["total_profit"]
    color  = "🟢" if profit > 0 else "🔴"
    print(f"  Record:       {roi['wins']}W / {roi['losses']}L / {roi['pushes']}P")
    print(f"  Win Rate:     {roi['win_rate']}%")
    print(f"  Total P&L:    {color} {profit:+.2f}u")
    print(f"  ROI:          {roi['roi_pct']:+.2f}%")
    print(f"  (at ${BANKROLL} bankroll = ${round(profit*BANKROLL/100,2):+.2f})")
    if roi["signal_roi"]:
        print(f"\n  Signal P&L:")
        for sig, data in sorted(roi["signal_roi"].items(),
                                key=lambda x: x[1]["profit"], reverse=True):
            if data["bets"] >= 3:
                sig_roi = round(data["profit"]/data["bets"]*100,1)
                color   = "🟢" if data["profit"] > 0 else "🔴"
                print(f"    {color} {sig:<25} {data['profit']:>+.2f}u ({sig_roi:>+.1f}% ROI) | {data['bets']} bets")
    print()


# ─────────────────────────────────────────────
# CALIBRATION
# ─────────────────────────────────────────────
_calibration: dict = {}
_calibration_loaded: bool = False

def load_calibration(sheet) -> dict:
    global _calibration, _calibration_loaded
    if _calibration_loaded:
        return _calibration
    _calibration_loaded = True
    try:
        ws   = sheet.worksheet("⚙️ Calibration")
        rows = ws.get_all_values()
        cal  = {}
        for row in rows:
            if len(row) < 2 or not row[0] or row[0].startswith("#") or row[0] == "Parameter":
                continue
            try:
                cal[row[0].strip()] = float(row[1].strip())
            except ValueError:
                cal[row[0].strip()] = row[1].strip()
        _calibration = cal
        if cal:
            n    = int(cal.get("sample_size", 0))
            conf = float(cal.get("sample_confidence", 0)) * 100
            avg_prob = float(cal.get("avg_predicted_prob", 0))
            print(f"\n  ╔══════════════════════════════════════════════════╗")
            print(f"  ║  ⚙️  MODEL CALIBRATION STATUS                    ║")
            print(f"  ╠══════════════════════════════════════════════════╣")
            print(f"  ║  Games logged: {n:<5} | Confidence: {conf:.0f}%{'':>18}║")
            print(f"  ║  avg_predicted_prob: {avg_prob:.4f} {'✅ OK' if avg_prob > 0.10 else '🚨 BROKEN — fix R script':<24}║")
            print(f"  ║  prob_factor: {float(cal.get('prob_confidence_factor',1.0)):.4f}{'':>33}║")
            applied = "✅ Calibration IS being applied" if conf >= 10 else "⚠️  Under 10 games — not applied"
            print(f"  ║  {applied:<48}║")
            print(f"  ╚══════════════════════════════════════════════════╝\n")
        else:
            print("  ⚙️  No calibration — run mlb_analysis.R first!")
        return cal
    except Exception:
        print("  ⚙️  No calibration tab found — run mlb_analysis.R to generate it")
        return {}

def apply_calibration(proj_total, proj_away, proj_home, bet_type="total"):
    cal  = _calibration
    conf = float(cal.get("sample_confidence",0)) if cal else 0
    if not cal or conf < 0.10:
        return proj_away, proj_home, proj_total
    total_adj = float(cal.get("total_run_adjustment",0)) * conf
    extra     = float(cal.get("over_proj_adjustment",0)) * conf if "over" in bet_type.lower() else \
                float(cal.get("under_proj_adjustment",0)) * conf if "under" in bet_type.lower() else 0.0
    home_adj  = float(cal.get("home_run_adjustment",0)) * conf
    away_adj  = float(cal.get("away_run_adjustment",0)) * conf
    adj_away  = round(max(proj_away-2, min(proj_away+2, proj_away+away_adj+(total_adj+extra)/2)), 2)
    adj_home  = round(max(proj_home-2, min(proj_home+2, proj_home+home_adj+(total_adj+extra)/2)), 2)
    return adj_away, adj_home, round(adj_away+adj_home, 2)

def apply_prob_calibration(prob):
    cal    = _calibration
    conf   = float(cal.get("sample_confidence",0)) if cal else 0
    factor = float(cal.get("prob_confidence_factor",1.0)) if cal else 1.0
    factor = max(0.70, min(1.30, factor))
    factor = 1.0 + (factor - 1.0) * conf
    return round(min(0.95, max(0.05, prob * factor)), 4)

def get_edge_threshold():
    return float(_calibration.get("edge_threshold_recommended",0.05))*100 if _calibration else 5.0

def get_yrfi_calibration_factor():
    cal    = _calibration
    conf   = float(cal.get("sample_confidence",0)) if cal else 0
    factor = float(cal.get("yrfi_rate_factor",1.0)) if cal else 1.0
    return round(1.0 + (factor-1.0)*conf, 4)


# ─────────────────────────────────────────────
# PARK FACTORS
# ─────────────────────────────────────────────
_SAVANT_PARK_FACTORS_2026 = {
    "Coors Field":                  {"basic":1.20,"hr":1.28},
    "Great American Ball Park":     {"basic":1.14,"hr":1.28},
    "Fenway Park":                  {"basic":1.11,"hr":1.08},
    "Globe Life Field":             {"basic":1.09,"hr":1.14},
    "Yankee Stadium":               {"basic":1.08,"hr":1.22},
    "Oriole Park at Camden Yards":  {"basic":1.07,"hr":1.12},
    "Citizens Bank Park":           {"basic":1.06,"hr":1.14},
    "Wrigley Field":                {"basic":1.05,"hr":1.06},
    "Truist Park":                  {"basic":1.05,"hr":1.07},
    "American Family Field":        {"basic":1.04,"hr":1.05},
    "Kauffman Stadium":             {"basic":1.04,"hr":1.10},
    "Progressive Field":            {"basic":1.02,"hr":0.99},
    "Nationals Park":               {"basic":1.02,"hr":1.03},
    "Target Field":                 {"basic":1.01,"hr":0.97},
    "Rogers Centre":                {"basic":1.01,"hr":1.03},
    "Angel Stadium":                {"basic":1.00,"hr":1.01},
    "Comerica Park":                {"basic":1.00,"hr":0.94},
    "PNC Park":                     {"basic":0.99,"hr":0.97},
    "Busch Stadium":                {"basic":0.98,"hr":0.95},
    "Guaranteed Rate Field":        {"basic":0.97,"hr":0.96},
    "Rate Field":                   {"basic":0.97,"hr":0.96},
    "Daikin Park":                  {"basic":0.97,"hr":0.94},
    "Minute Maid Park":             {"basic":0.97,"hr":0.94},
    "loanDepot park":               {"basic":0.96,"hr":0.91},
    "LoanDepot Park":               {"basic":0.96,"hr":0.91},
    "UNIQLO Field":                 {"basic":0.96,"hr":0.93},
    "Dodger Stadium":               {"basic":0.96,"hr":0.93},
    "Chase Field":                  {"basic":0.95,"hr":0.97},
    "Citi Field":                   {"basic":0.95,"hr":0.89},
    "G.M. Steinbrenner Field":      {"basic":0.95,"hr":0.91},
    "Tropicana Field":              {"basic":0.95,"hr":0.91},
    "T-Mobile Park":                {"basic":0.93,"hr":0.88},
    "Oracle Park":                  {"basic":0.91,"hr":0.82},
    "Petco Park":                   {"basic":0.91,"hr":0.85},
    "Sutter Health Park":           {"basic":0.94,"hr":0.93},
}

def _get_savant_pf(venue: str, stat: str = "basic"):
    v = venue.lower()
    for park, vals in _SAVANT_PARK_FACTORS_2026.items():
        if park.lower() == v or park.lower() in v or v in park.lower():
            return vals[stat]
    return None

def get_park_factor_all_sources(venue: str, bp_park_run_pct: float = None) -> dict:
    pf_static = _get_savant_pf(venue) or 1.00
    pf_savant = _get_savant_pf(venue)
    pf_bp     = round(1.0 + bp_park_run_pct/100.0, 4) if bp_park_run_pct is not None else None
    if pf_bp is not None and pf_savant is not None:
        blended = round(pf_bp*0.50 + pf_savant*0.30 + pf_static*0.20, 4)
        source  = "BP50/Savant30/Static20"
    elif pf_bp is not None:
        blended = round(pf_bp*0.65 + pf_static*0.35, 4)
        source  = "BP65/Static35"
    elif pf_savant is not None:
        blended = round(pf_savant*0.65 + pf_static*0.35, 4)
        source  = "Savant65/Static35"
    else:
        blended = pf_static
        source  = "Static100"
    return {"blended":blended,"bp":pf_bp,"fg":pf_savant,"static":pf_static,"source":source}

def get_park_factor(venue: str, bp_park_run_pct: float = None) -> float:
    return get_park_factor_all_sources(venue, bp_park_run_pct)["blended"]

def get_park_factor_hr(venue: str, bp_park_hr_pct: float = None) -> float:
    pf_static = _get_savant_pf(venue,"hr") or 1.00
    pf_savant = _get_savant_pf(venue,"hr")
    pf_bp     = round(1.0 + bp_park_hr_pct/100.0, 4) if bp_park_hr_pct is not None else None
    if pf_bp is not None and pf_savant is not None:
        return round(pf_bp*0.50 + pf_savant*0.30 + pf_static*0.20, 4)
    elif pf_bp is not None:
        return round(pf_bp*0.65 + pf_static*0.35, 4)
    elif pf_savant is not None:
        return round(pf_savant*0.65 + pf_static*0.35, 4)
    return pf_static

def get_weather_factor(temp: str, wind: str) -> float:
    factor = 1.0
    try:
        t = int(str(temp).replace("°","").strip())
        if t >= 85:   factor += 0.04
        elif t >= 75: factor += 0.02
        elif t <= 50: factor -= 0.04
        elif t <= 60: factor -= 0.02
    except:
        pass
    wind_lower = str(wind).lower()
    if "out" in wind_lower:
        try:
            speed = int(''.join(filter(str.isdigit, wind_lower.split("mph")[0][-3:])))
            factor += min(speed*0.004, 0.06)
        except:
            factor += 0.03
    elif "in" in wind_lower:
        try:
            speed = int(''.join(filter(str.isdigit, wind_lower.split("mph")[0][-3:])))
            factor -= min(speed*0.004, 0.06)
        except:
            factor -= 0.03
    return round(factor, 3)


# ─────────────────────────────────────────────
# RUN PROJECTIONS
# ─────────────────────────────────────────────
HOME_FIELD_ADVANTAGE = 0.035

def project_runs_allowed(pitcher, opp_offense, park_factor, weather_factor,
                          lineup_ops=None, recent_offense=None, location_splits=None,
                          h2h=None, pitcher_form=None, bp_avail=None) -> float:
    if not pitcher:
        return 4.50
    fip = pitcher.get("fip") or pitcher.get("era") or 4.50
    era = pitcher.get("era") or fip
    if pitcher_form and pitcher_form.get("recent_era"):
        recent_era  = pitcher_form["recent_era"]
        recent_fip  = (recent_era*0.60) + (fip*0.40)
        blend_fip   = (recent_fip*0.50) + (fip*0.50)
        blend_era   = (recent_era*0.50) + (era*0.50)
        avg_ip      = pitcher_form.get("recent_avg_ip", 5.5)
    else:
        blend_fip = fip
        blend_era = era
        avg_ip    = 5.5
    base_ra9  = (blend_fip*0.60) + (blend_era*0.40)
    proj_runs = (base_ra9/9) * avg_ip
    league_rpg  = 4.50
    season_rpg  = opp_offense.get("runs_per_game", league_rpg)
    recent_rpg  = recent_offense.get("recent_rpg", season_rpg) if recent_offense else season_rpg
    loc_key     = list(location_splits.keys())[0] if location_splits else None
    loc_rpg     = location_splits.get(loc_key, season_rpg) if location_splits and loc_key else season_rpg
    blended_rpg = (season_rpg*0.40) + (recent_rpg*0.35) + (loc_rpg*0.25)
    off_factor  = blended_rpg / league_rpg
    ops_factor  = (lineup_ops/0.720) if lineup_ops and lineup_ops > 0 else opp_offense.get("ops",0.720)/0.720
    h2h_factor  = 1.0
    if h2h and h2h.get("h2h_games",0) >= 3:
        h2h_factor = max(0.85, min(round(h2h.get("h2h_avg_total",9.0)/9.0,3), 1.15))
    bp_factor = 1.0
    if bp_avail:
        score = bp_avail.get("bp_tiredness_score",0)
        if score >= 8:   bp_factor = 1.08
        elif score >= 3: bp_factor = 1.04
    proj_runs = proj_runs * off_factor * ops_factor * h2h_factor
    proj_runs = proj_runs * park_factor * weather_factor
    bullpen_innings = max(0, 9.0 - avg_ip)
    if bullpen_innings > 0:
        bp_extra   = (4.50/9) * bullpen_innings * (bp_factor - 1.0)
        proj_runs += bp_extra
    return round(proj_runs, 2)

def project_bullpen_runs(bullpen, innings_remaining, park_factor) -> float:
    if not bullpen:
        return round((4.50/9)*innings_remaining*park_factor, 2)
    return round((bullpen.get("bullpen_era",4.50)/9)*innings_remaining*park_factor, 2)

def project_total_runs(away_starter, home_starter, away_offense, home_offense,
                        away_bullpen, home_bullpen, park_factor, weather_factor,
                        away_lineup_ops=None, home_lineup_ops=None,
                        away_recent=None, home_recent=None,
                        away_location=None, home_location=None, h2h=None,
                        away_pitcher_form=None, home_pitcher_form=None,
                        away_bp_avail=None, home_bp_avail=None) -> dict:
    away_starter_runs = project_runs_allowed(
        home_starter, away_offense, park_factor, weather_factor,
        away_lineup_ops, away_recent, away_location, h2h, home_pitcher_form, home_bp_avail)
    home_starter_runs = project_runs_allowed(
        away_starter, home_offense, park_factor, weather_factor,
        home_lineup_ops, home_recent, home_location, h2h, away_pitcher_form, away_bp_avail)
    away_avg_ip = home_pitcher_form.get("recent_avg_ip",5.5) if home_pitcher_form else 5.5
    home_avg_ip = away_pitcher_form.get("recent_avg_ip",5.5) if away_pitcher_form else 5.5
    away_bullpen_runs = project_bullpen_runs(home_bullpen, 9.0-away_avg_ip, park_factor)
    home_bullpen_runs = project_bullpen_runs(away_bullpen, 9.0-home_avg_ip, park_factor)
    away_total = round(away_starter_runs + away_bullpen_runs, 2)
    home_total = round(home_starter_runs + home_bullpen_runs, 2)
    game_total = round(away_total + home_total, 2)
    f5_away_ip = max(away_avg_ip, 5.0)
    f5_home_ip = max(home_avg_ip, 5.0)
    f5_away = round(project_runs_allowed(
        home_starter, away_offense, park_factor, weather_factor,
        away_lineup_ops, away_recent, away_location, h2h, home_pitcher_form, home_bp_avail) * (5.0/f5_away_ip), 2)
    f5_home = round(project_runs_allowed(
        away_starter, home_offense, park_factor, weather_factor,
        home_lineup_ops, home_recent, home_location, h2h, away_pitcher_form, away_bp_avail) * (5.0/f5_home_ip), 2)
    f5_total = round(f5_away + f5_home, 2)
    if f5_total > game_total:
        ratio    = game_total/f5_total if f5_total > 0 else 1
        f5_away  = round(f5_away*ratio, 2)
        f5_home  = round(f5_home*ratio, 2)
        f5_total = round(f5_away+f5_home, 2)
    max_f5 = round(game_total*0.55, 2)
    if f5_total > max_f5:
        ratio    = max_f5/f5_total if f5_total > 0 else 1
        f5_away  = round(f5_away*ratio, 2)
        f5_home  = round(f5_home*ratio, 2)
        f5_total = round(f5_away+f5_home, 2)
    MAX_PROJ_TOTAL = 18.0
    if game_total > MAX_PROJ_TOTAL:
        ratio      = MAX_PROJ_TOTAL/game_total
        away_total = round(away_total*ratio, 2)
        home_total = round(home_total*ratio, 2)
        game_total = round(away_total+home_total, 2)
        f5_away    = round(f5_away*ratio, 2)
        f5_home    = round(f5_home*ratio, 2)
        f5_total   = round(f5_away+f5_home, 2)
    return {
        "away_proj_runs": away_total,
        "home_proj_runs": home_total,
        "proj_total":     game_total,
        "f5_away_runs":   f5_away,
        "f5_home_runs":   f5_home,
        "proj_f5_total":  f5_total,
    }


# ─────────────────────────────────────────────
# ✅ FIX 1: WIN PROBABILITY — capped run differential
# ─────────────────────────────────────────────
def win_probability(away_runs: float, home_runs: float) -> tuple:
    """
    Win probability with run differential capped at MAX_RUN_DIFF (2.5 runs).
    This prevents the model from assigning 70%+ win probability just because
    run projections are lopsided. Real win probability from run differential:
      1 run diff  ≈ 55-57%
      2 run diff  ≈ 60-63%
      3+ run diff ≈ 65%+ (now capped here)
    """
    if away_runs + home_runs == 0:
        return round(0.5 - HOME_FIELD_ADVANTAGE, 3), round(0.5 + HOME_FIELD_ADVANTAGE, 3)

    # ── FIX: cap the run differential ──────────────────────────
    diff = home_runs - away_runs
    diff = max(-MAX_RUN_DIFF, min(MAX_RUN_DIFF, diff))
    avg  = (away_runs + home_runs) / 2
    away_runs_adj = avg - diff / 2
    home_runs_adj = avg + diff / 2
    # ───────────────────────────────────────────────────────────

    base_away = away_runs_adj / (away_runs_adj + home_runs_adj)
    away_pct  = round(max(0.05, base_away - HOME_FIELD_ADVANTAGE), 3)
    home_pct  = round(min(0.95, 1.0 - away_pct), 3)
    return away_pct, home_pct


def yrfi_probability(away_starter, home_starter, away_offense, home_offense, park_factor) -> float:
    LEAGUE_AVG_ERA    = 4.50
    LEAGUE_AVG_OBP    = 0.318
    def first_inn_rate(pitcher, offense):
        if not pitcher:
            return 0.47/9
        era    = pitcher.get("fip") or pitcher.get("era") or LEAGUE_AVG_ERA
        k9     = pitcher.get("k9", 8.5)
        bb9    = pitcher.get("bb9", 3.0)
        k_factor  = 1.0 - max(0, (k9-8.5)*0.015)
        bb_factor = 1.0 + max(0, (bb9-3.0)*0.02)
        obp    = offense.get("obp", LEAGUE_AVG_OBP)
        off_factor = obp / LEAGUE_AVG_OBP
        base_rate  = (era/9) * 0.55
        rate       = base_rate * k_factor * bb_factor * off_factor
        park_adj   = 1.0 + (park_factor-1.0)*0.40
        rate       = rate * park_adj
        return max(0.25, min(rate, 0.55))
    away_rate = first_inn_rate(home_starter, away_offense)
    home_rate = first_inn_rate(away_starter, home_offense)
    p_away_scoreless = math.exp(-away_rate)
    p_home_scoreless = math.exp(-home_rate)
    yrfi = round(1 - (p_away_scoreless*p_home_scoreless), 3)
    return max(0.35, min(yrfi, 0.67))

def run_line_probability(away_runs: float, home_runs: float, line: float = 1.5) -> tuple:
    if away_runs <= 0 or home_runs <= 0:
        return 0.5, 0.5
    def pmf(lam, k): return (math.exp(-lam) * (lam**k)) / math.factorial(k)
    away_p = home_p = 0.0
    for a in range(21):
        pa = pmf(away_runs, a)
        for h in range(21):
            j = pa * pmf(home_runs, h)
            d = a - h
            if d > line:   away_p += j
            elif d < -line: home_p += j
    tot = away_p + home_p
    if tot > 0: return round(away_p/tot, 4), round(home_p/tot, 4)
    return 0.5, 0.5


# ─────────────────────────────────────────────
# SIGNAL SYSTEM
# ─────────────────────────────────────────────
def sharp_money_signal(bet_pct: float, money_pct: float, side: str) -> str:
    if bet_pct is None or money_pct is None:
        return ""
    diff = money_pct - bet_pct
    if bet_pct >= 65 and money_pct <= 45:
        return f"⚡ SHARP FADE {side} (Public {bet_pct}% bets / Sharp {money_pct}% money)"
    elif bet_pct <= 35 and money_pct >= 55:
        return f"⚡ SHARP BACK {side} (Public {bet_pct}% bets / Sharp {money_pct}% money)"
    elif diff >= 20:
        return f"💰 MONEY LEAN {side} (Money% {diff:+.0f}% vs Bet%)"
    elif diff <= -20:
        return f"💰 MONEY FADE {side} (Money% {diff:+.0f}% vs Bet%)"
    return ""

def american_to_implied(odds: int) -> float:
    if odds > 0: return round(100/(odds+100), 4)
    else:        return round(abs(odds)/(abs(odds)+100), 4)

def calc_edge(our_prob: float, market_odds: int) -> float:
    implied = american_to_implied(market_odds)
    return round((our_prob - implied)*100, 1)

def prob_to_american(prob: float) -> int:
    if prob <= 0 or prob >= 1: return 0
    if prob >= 0.5: return round(-(prob/(1-prob))*100)
    else:           return round(((1-prob)/prob)*100)

def score_signal(our_prob: float, market_odds: int,
                 sharp_confirms: bool = False, sharp_fades: bool = False) -> tuple:
    if not market_odds:
        return "—", 0, 0.0
    edge = calc_edge(our_prob, market_odds)
    if edge <= -EDGE_THRESHOLD:
        return "❌ FADE", round(edge), edge
    if our_prob >= 0.65:   prob_score = 40
    elif our_prob >= 0.60: prob_score = 30
    elif our_prob >= 0.55: prob_score = 20
    elif our_prob >= 0.50: prob_score = 10
    else:                  prob_score = 0
    if edge >= 15:   edge_score = 40
    elif edge >= 10: edge_score = 30
    elif edge >= 7:  edge_score = 20
    elif edge >= 5:  edge_score = 10
    else:            edge_score = 0
    sharp_score = 20 if sharp_confirms else (-20 if sharp_fades else 0)
    total = prob_score + edge_score + sharp_score
    if total >= 80:   signal = "🔥🔥 DOUBLE STRONG"
    elif total >= 60: signal = "🔥 STRONG"
    elif total >= 40: signal = "✅ LEAN"
    elif total >= 20: signal = "👀 WATCH"
    else:             signal = "— SKIP"
    return signal, total, edge

def get_sharp_alignment(market: dict, bet_type: str) -> tuple:
    if bet_type == "away_ml":        b, m = market.get("ml_bet_away"),       market.get("ml_money_away")
    elif bet_type == "home_ml":      b, m = market.get("ml_bet_home"),        market.get("ml_money_home")
    elif bet_type == "over":         b, m = market.get("over_bet_pct"),       market.get("over_money_pct")
    elif bet_type == "under":        b, m = market.get("under_bet_pct"),      market.get("under_money_pct")
    elif bet_type == "away_spread":  b, m = market.get("spread_bet_away"),    market.get("spread_money_away")
    elif bet_type == "home_spread":  b, m = market.get("spread_bet_home"),    market.get("spread_money_home")
    else: return False, False
    if b is None or m is None: return False, False
    diff = m - b
    confirms = diff >= 15 or (b <= 35 and m >= 55)
    fades    = diff <= -15 or (b >= 65 and m <= 45)
    if confirms and fades: fades = False
    return confirms, fades


# ─────────────────────────────────────────────
# INPUT TAB
# ─────────────────────────────────────────────
INPUT_TAB_NAME = "📥 Input"
INPUT_COLUMNS = [
    "Game",
    "Away ML","Home ML","Total Line","Over Odds","Under Odds","Away RL Odds","Home RL Odds",
    "F5 Total","F5 Over Odds","F5 Under Odds","F5 Away ML","F5 Home ML",
    "Away TT Line","Away TT Over","Away TT Under","Home TT Line","Home TT Over","Home TT Under",
    "YRFI Odds","NRFI Odds",
    "Away ML Bet%","Away ML Money%","Home ML Bet%","Home ML Money%",
    "Over Bet%","Over Money%","Under Bet%","Under Money%",
    "Away Spread Bet%","Away Spread Money%","Home Spread Bet%","Home Spread Money%",
    "BP Away Runs","BP Home Runs","BP YRFI%","BP F5 Away","BP F5 Home",
    "BP Park Run%","BP Park HR%",
    "BP Away SP Inn","BP Away SP Runs","BP Away SP K","BP Away SP BB",
    "BP Home SP Inn","BP Home SP Runs","BP Home SP K","BP Home SP BB",
    "BP Away R/G","BP Away HR/G","BP Home R/G","BP Home HR/G",
]

def create_input_tab(sheet) -> None:
    try:
        sheet.worksheet(INPUT_TAB_NAME)
        print(f"  ✅ Input tab already exists")
        return
    except gspread.WorksheetNotFound:
        pass
    ws = sheet.add_worksheet(INPUT_TAB_NAME, rows=50, cols=60)
    ws.update("A1", [["⚾ MLB MODEL INPUT — Fill this in before running Python"]])
    ws.append_row(INPUT_COLUMNS)
    print(f"  ✅ Created Input tab")

def read_input_from_sheet(sheet, game_name: str) -> tuple:
    try:
        ws   = sheet.worksheet(INPUT_TAB_NAME)
        rows = ws.get_all_values()
        header_row = None
        for i, row in enumerate(rows):
            if row and row[0] == "Game":
                header_row = i
                break
        if header_row is None:
            return None, None
        headers = rows[header_row]
        for row in rows[header_row + 1:]:
            if not row or not row[0]:
                continue
            row_game = row[0].strip().lower()
            search   = game_name.strip().lower()
            if row_game == search or all(part in row_game for part in search.split(" @ ")):
                data = {}
                for j, h in enumerate(headers):
                    h_clean = h.strip() if h else ""
                    if h_clean and j < len(row) and row[j].strip():
                        data[h_clean] = row[j].strip()
                return _parse_sheet_input(data, game_name)
        return None, None
    except Exception as e:
        print(f"  ⚠️  Could not read input tab: {e}")
        return None, None

def _parse_sheet_input(data: dict, game_name: str) -> tuple:
    def si(key, default=None):
        v = data.get(key,"")
        if not v: return default
        try: return int(float(v))
        except: return default
    def sf(key, default=None):
        v = data.get(key,"")
        if not v: return default
        try: return float(v)
        except: return default
    sharp_signals = []
    checks = [
        ("Away ML Bet%","Away ML Money%", game_name.split("@")[0].strip()+" ML"),
        ("Home ML Bet%","Home ML Money%", game_name.split("@")[-1].strip()+" ML"),
        ("Over Bet%","Over Money%","OVER"),
        ("Under Bet%","Under Money%","UNDER"),
        ("Away Spread Bet%","Away Spread Money%","Away Spread"),
        ("Home Spread Bet%","Home Spread Money%","Home Spread"),
    ]
    for b_key, m_key, side in checks:
        b = sf(b_key); m = sf(m_key)
        s = sharp_money_signal(b, m, side)
        if s: sharp_signals.append(s)
    market = {
        "away_ml":si("Away ML"),"home_ml":si("Home ML"),
        "total_line":sf("Total Line"),"over_odds":si("Over Odds",-110),"under_odds":si("Under Odds",-110),
        "away_rl_odds":si("Away RL Odds"),"home_rl_odds":si("Home RL Odds"),
        "mkt_f5_line":sf("F5 Total"),"f5_over_odds":si("F5 Over Odds",-110),
        "f5_under_odds":si("F5 Under Odds",-110),"f5_away_ml":si("F5 Away ML"),"f5_home_ml":si("F5 Home ML"),
        "away_team_total":sf("Away TT Line"),"away_tt_over_odds":si("Away TT Over",-110),
        "away_tt_under_odds":si("Away TT Under",-110),"home_team_total":sf("Home TT Line"),
        "home_tt_over_odds":si("Home TT Over",-110),"home_tt_under_odds":si("Home TT Under",-110),
        "yrfi_odds":si("YRFI Odds",-115),"nrfi_odds":si("NRFI Odds",-105),
        "ml_bet_away":sf("Away ML Bet%"),"ml_money_away":sf("Away ML Money%"),
        "ml_bet_home":sf("Home ML Bet%"),"ml_money_home":sf("Home ML Money%"),
        "over_bet_pct":sf("Over Bet%"),"over_money_pct":sf("Over Money%"),
        "under_bet_pct":sf("Under Bet%"),"under_money_pct":sf("Under Money%"),
        "spread_bet_away":sf("Away Spread Bet%"),"spread_money_away":sf("Away Spread Money%"),
        "spread_bet_home":sf("Home Spread Bet%"),"spread_money_home":sf("Home Spread Money%"),
        "sharp_signals":" | ".join(sharp_signals) if sharp_signals else "—",
    }
    bp = {
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
    return market, bp

def blend_projections(api_runs: dict, bp: dict) -> dict:
    def blend(api_val, bp_val, bp_weight=0.60):
        if bp_val is None: return api_val
        return round(api_val*(1-bp_weight) + bp_val*bp_weight, 2)
    away    = blend(api_runs["away_proj_runs"], bp.get("bp_away_runs"))
    home    = blend(api_runs["home_proj_runs"], bp.get("bp_home_runs"))
    f5_away = blend(api_runs["f5_away_runs"],   bp.get("bp_f5_away"))
    f5_home = blend(api_runs["f5_home_runs"],   bp.get("bp_f5_home"))
    return {
        "away_proj_runs": away, "home_proj_runs": home,
        "proj_total":     round(away+home, 2),
        "f5_away_runs":   f5_away, "f5_home_runs": f5_home,
        "proj_f5_total":  round(f5_away+f5_home, 2),
        "bp_blended":     bp.get("bp_away_runs") is not None,
    }

def blend_yrfi(api_yrfi: float, bp: dict) -> float:
    bp_yrfi = bp.get("bp_yrfi_pct")
    if bp_yrfi is None: return api_yrfi
    return round(api_yrfi*0.30 + (bp_yrfi/100)*0.70, 3)


# ─────────────────────────────────────────────
# SECTION 10 — ANALYZE GAME
# ─────────────────────────────────────────────
def analyze_game(game: dict) -> dict:
    info = parse_game_info(game)
    print(f"\n🔍 Analyzing: {info['away_team']} @ {info['home_team']}")

    game_status = check_game_timing(game, info)
    info["game_status"] = game_status

    if any(s in game_status for s in ["In Progress","Final","Started","⚡","🏁","⚠️"]):
        print(f"  ⏭️  SKIPPING — {game_status}")
        return {
            "game_time":   info.get("game_time",""),
            "away_team":   info["away_team"],
            "home_team":   info["home_team"],
            "venue":       info.get("venue",""),
            "game_status": game_status,
            "skipped":     True,
        }

    print("  Fetching pitcher stats...")
    away_pitcher = get_pitcher_stats(info["away_pitcher_id"])
    home_pitcher = get_pitcher_stats(info["home_pitcher_id"])

    print("  Fetching team offense...")
    away_offense = get_team_offense(info["away_team_id"])
    home_offense = get_team_offense(info["home_team_id"])

    print("  Fetching bullpen stats...")
    away_bullpen = get_bullpen_stats(info["away_team_id"])
    home_bullpen = get_bullpen_stats(info["home_team_id"])

    print("  Fetching recent form...")
    away_recent = get_recent_team_offense(info["away_team_id"], 15)
    home_recent = get_recent_team_offense(info["home_team_id"], 15)

    print("  Fetching home/away splits...")
    away_location = get_home_away_splits(info["away_team_id"], "away")
    home_location = get_home_away_splits(info["home_team_id"], "home")

    print("  Fetching pitcher recent form (last 3 starts)...")
    away_pitcher_form = get_pitcher_recent_form(info["away_pitcher_id"], 3)
    home_pitcher_form = get_pitcher_recent_form(info["home_pitcher_id"], 3)

    if away_pitcher_form:
        print(f"    {info['away_pitcher']}: {away_pitcher_form.get('recent_form_score','?')} | ERA: {away_pitcher_form.get('recent_era','?')} | IP: {away_pitcher_form.get('recent_avg_ip','?')}")
    if home_pitcher_form:
        print(f"    {info['home_pitcher']}: {home_pitcher_form.get('recent_form_score','?')} | ERA: {home_pitcher_form.get('recent_era','?')} | IP: {home_pitcher_form.get('recent_avg_ip','?')}")

    print("  Fetching bullpen availability...")
    away_bp_avail = get_bullpen_availability(info["away_team_id"])
    home_bp_avail = get_bullpen_availability(info["home_team_id"])

    print("  Fetching H2H record...")
    h2h = get_h2h_record(info["away_team_id"], info["home_team_id"])

    away_lineup_full = get_lineup_with_ids(game, "away")
    home_lineup_full = get_lineup_with_ids(game, "home")
    away_hand = away_pitcher.get("hand", "R")
    home_hand = home_pitcher.get("hand", "R")

    away_lineup_ops, home_lineup_ops = None, None
    away_matchup_summary, home_matchup_summary = "", ""

    if away_lineup_full and info.get("home_pitcher_id"):
        print(f"  Fetching away lineup vs {info['home_pitcher']}...")
        away_lineup_ops = get_lineup_vs_pitcher_ops(away_lineup_full, info["home_pitcher_id"], home_hand)
        away_matchup_summary = get_matchup_summary(away_lineup_full, info["home_pitcher_id"], info["home_pitcher"])

    if home_lineup_full and info.get("away_pitcher_id"):
        print(f"  Fetching home lineup vs {info['away_pitcher']}...")
        home_lineup_ops = get_lineup_vs_pitcher_ops(home_lineup_full, info["away_pitcher_id"], away_hand)
        home_matchup_summary = get_matchup_summary(home_lineup_full, info["away_pitcher_id"], info["away_pitcher"])

    away_lineup = [b["name"] if isinstance(b,dict) else b for b in away_lineup_full]
    home_lineup = [b["name"] if isinstance(b,dict) else b for b in home_lineup_full]

    # Sheet read for BP park data
    _game_name = f"{info['away_team']} @ {info['home_team']}"
    _sheet_market_pre, _sheet_bp_pre = None, None
    if _current_sheet:
        try:
            _sheet_market_pre, _sheet_bp_pre = read_input_from_sheet(_current_sheet, _game_name)
        except Exception as e:
            print(f"  ⚠️  Sheet read error: {e}")
    _sheet_bp_pre = _sheet_bp_pre or {}
    _bp_park_run  = _sheet_bp_pre.get("bp_park_run_pct")
    _bp_park_hr   = _sheet_bp_pre.get("bp_park_hr_pct")

    pf_all         = get_park_factor_all_sources(info["venue"], bp_park_run_pct=_bp_park_run)
    park_factor    = pf_all["blended"]
    park_factor_hr = get_park_factor_hr(info["venue"], bp_park_hr_pct=_bp_park_hr)
    weather_factor = get_weather_factor(info["weather_temp"], info["weather_wind"])

    print(f"  Park factor: {park_factor:.3f}x ({pf_all['source']})")

    runs = project_total_runs(
        away_pitcher, home_pitcher, away_offense, home_offense,
        away_bullpen, home_bullpen, park_factor, weather_factor,
        away_lineup_ops=away_lineup_ops, home_lineup_ops=home_lineup_ops,
        away_recent=away_recent, home_recent=home_recent,
        away_location=away_location, home_location=home_location, h2h=h2h,
        away_pitcher_form=away_pitcher_form, home_pitcher_form=home_pitcher_form,
        away_bp_avail=away_bp_avail, home_bp_avail=home_bp_avail
    )

    # Raw win prob before calibration
    away_win_pct_raw, home_win_pct_raw = win_probability(runs["away_proj_runs"], runs["home_proj_runs"])
    yrfi_prob = yrfi_probability(away_pitcher, home_pitcher, away_offense, home_offense, park_factor)

    # Market + BP data from sheet
    game_name    = _game_name
    sheet_market = _sheet_market_pre
    sheet_bp     = _sheet_bp_pre if _sheet_bp_pre else {}
    if sheet_market:
        market = sheet_market
        bp     = sheet_bp or {}
    else:
        market = {}
        bp     = {}

    # Load BP from downloaded xlsx files
    try:
        from read_ballparkpal import load_bp_games, load_bp_pitchers, load_bp_teams, get_bp_for_game
        _bp_g = load_bp_games("ballparkpal_games.xlsx")
        _bp_p = load_bp_pitchers("ballparkpal_pitchers.xlsx")
        _bp_t = load_bp_teams("ballparkpal_teams.xlsx")
        for _bp_source in [_bp_g, _bp_p, _bp_t]:
            bp_xlsx = get_bp_for_game(_bp_source, info["away_team"], info["home_team"])
            if bp_xlsx:
                for k, v in bp_xlsx.items():
                    if v is not None:
                        bp[k] = v
        print(f"  ✅ BP XLSX merged")
    except Exception as e:
        print(f"  ⚠️  BP error: {e}")

    api_only_away  = runs["away_proj_runs"]
    api_only_home  = runs["home_proj_runs"]
    api_only_total = runs["proj_total"]

    runs      = blend_projections(runs, bp)
    yrfi_prob = blend_yrfi(yrfi_prob, bp)
    pre_cal_away  = runs["away_proj_runs"]
    pre_cal_home  = runs["home_proj_runs"]
    pre_cal_total = runs["proj_total"]

    # Apply R calibration
    cal_away, cal_home, cal_total = apply_calibration(
        runs["proj_total"], runs["away_proj_runs"], runs["home_proj_runs"])
    runs["away_proj_runs"] = cal_away
    runs["home_proj_runs"] = cal_home
    runs["proj_total"]     = cal_total
    yrfi_prob = round(min(0.99, yrfi_prob * get_yrfi_calibration_factor()), 4)

    # Recalculate win probability after calibration
    away_win_pct, home_win_pct = win_probability(runs["away_proj_runs"], runs["home_proj_runs"])

    # Apply prob calibration factor from R
    away_win_pct = apply_prob_calibration(away_win_pct)
    home_win_pct = round(1.0 - away_win_pct, 4)

    # ── FIX 2: Clamp final win probability to MAX_WIN_PROB ──────
    # Prevents overconfident signals regardless of run projections
    if away_win_pct > MAX_WIN_PROB:
        away_win_pct = MAX_WIN_PROB
        home_win_pct = round(1.0 - away_win_pct, 4)
    elif home_win_pct > MAX_WIN_PROB:
        home_win_pct = MAX_WIN_PROB
        away_win_pct = round(1.0 - home_win_pct, 4)
    # ────────────────────────────────────────────────────────────

    cal_applied = bool(_calibration) and float(_calibration.get("sample_confidence",0)) >= 0.10

    # Projection comparison
    print(f"\n  📊 PROJECTION COMPARISON:")
    print(f"     {'Source':<22} {'Away':>6} {'Home':>6} {'Total':>7}")
    print(f"     {'─'*45}")
    print(f"     {'API Only':<22} {api_only_away:>6.2f} {api_only_home:>6.2f} {api_only_total:>7.2f}")
    if runs.get('bp_blended'):
        print(f"     {'+ BP Blended':<22} {pre_cal_away:>6.2f} {pre_cal_home:>6.2f} {pre_cal_total:>7.2f}")
    label = '+ R Calibrated ✅' if cal_applied else 'Final'
    print(f"     {label:<22} {cal_away:>6.2f} {cal_home:>6.2f} {cal_total:>7.2f}  ← model uses this")
    print(f"     Win prob (capped at {MAX_WIN_PROB*100:.0f}%): Away {away_win_pct*100:.1f}% — Home {home_win_pct*100:.1f}%")
    print(f"     F5: {runs.get('proj_f5_total','?')} | YRFI: {yrfi_prob*100:.1f}%")

    # ── SIGNAL CALCULATIONS ──────────────────────────────────────
    edges = {}
    edges["fair_away_ml"] = prob_to_american(away_win_pct)
    edges["fair_home_ml"] = prob_to_american(home_win_pct)
    edges["fair_yrfi"]    = prob_to_american(yrfi_prob)
    edges["fair_nrfi"]    = prob_to_american(1 - yrfi_prob)

    if market.get("away_ml"):
        sc, fd = get_sharp_alignment(market, "away_ml")
        sig, score, edge = score_signal(away_win_pct, market["away_ml"], sc, fd)
        edges["away_ml_edge"]  = edge
        edges["away_ml_score"] = score
        edges["away_ml_flag"]  = sig

    if market.get("home_ml"):
        sc, fd = get_sharp_alignment(market, "home_ml")
        sig, score, edge = score_signal(home_win_pct, market["home_ml"], sc, fd)
        edges["home_ml_edge"]  = edge
        edges["home_ml_score"] = score
        edges["home_ml_flag"]  = sig

    if market.get("total_line") and market.get("over_odds"):
        over_prob  = 1 - _poisson_under(runs["proj_total"], market["total_line"])
        under_prob = 1 - over_prob
        edges["over_prob"]  = round(over_prob*100, 1)
        edges["under_prob"] = round(under_prob*100, 1)
        edges["fair_over"]  = prob_to_american(over_prob)
        edges["fair_under"] = prob_to_american(under_prob)
        sc, fd = get_sharp_alignment(market, "over")
        sig, score, edge = score_signal(over_prob, market["over_odds"], sc, fd)
        edges["over_edge"]  = edge; edges["over_score"]  = score; edges["over_flag"]  = sig
        sc, fd = get_sharp_alignment(market, "under")
        sig, score, edge = score_signal(under_prob, market["under_odds"], sc, fd)
        edges["under_edge"] = edge; edges["under_score"] = score; edges["under_flag"] = sig

    if market.get("mkt_f5_line") and market.get("f5_over_odds"):
        our_f5     = runs.get("proj_f5_total") or 0
        mkt_f5     = float(market["mkt_f5_line"])
        full_total = float(market.get("total_line") or 9.0)
        if mkt_f5/full_total < 0.65 and our_f5 > 0:
            f5_over_prob  = 1 - _poisson_under(our_f5, mkt_f5)
            f5_under_prob = 1 - f5_over_prob
            edges["f5_over_prob"]  = round(f5_over_prob*100, 1)
            edges["f5_under_prob"] = round(f5_under_prob*100, 1)
            sig, score, edge = score_signal(f5_over_prob,  market["f5_over_odds"])
            edges["f5_over_edge"]  = edge; edges["f5_over_score"]  = score; edges["f5_over_flag"]  = sig
            sig, score, edge = score_signal(f5_under_prob, market.get("f5_under_odds",-110))
            edges["f5_under_edge"] = edge; edges["f5_under_score"] = score; edges["f5_under_flag"] = sig

    if market.get("f5_away_ml"):
        sc, fd = get_sharp_alignment(market, "away_ml")
        sig, score, edge = score_signal(away_win_pct, market["f5_away_ml"], sc, fd)
        edges["f5_away_ml_edge"]  = edge; edges["f5_away_ml_score"] = score; edges["f5_away_ml_flag"] = sig

    if market.get("f5_home_ml"):
        sc, fd = get_sharp_alignment(market, "home_ml")
        sig, score, edge = score_signal(home_win_pct, market["f5_home_ml"], sc, fd)
        edges["f5_home_ml_edge"]  = edge; edges["f5_home_ml_score"] = score; edges["f5_home_ml_flag"] = sig

    rl_line = abs(float(market.get("away_rl_line",-1.5) or -1.5))
    if market.get("away_rl_odds"):
        away_rl_prob, home_rl_prob = run_line_probability(runs["away_proj_runs"], runs["home_proj_runs"], rl_line)
        edges["away_rl_prob"] = round(away_rl_prob*100,1); edges["home_rl_prob"] = round(home_rl_prob*100,1)
        edges["fair_away_rl"] = prob_to_american(away_rl_prob); edges["fair_home_rl"] = prob_to_american(home_rl_prob)
        sc, fd = get_sharp_alignment(market, "away_spread")
        sig, score, edge = score_signal(away_rl_prob, market["away_rl_odds"], sc, fd)
        edges["away_rl_edge"] = edge; edges["away_rl_score"] = score; edges["away_rl_flag"] = sig
    if market.get("home_rl_odds"):
        if "away_rl_prob" not in edges:
            away_rl_prob, home_rl_prob = run_line_probability(runs["away_proj_runs"], runs["home_proj_runs"], rl_line)
            home_rl_prob_val = home_rl_prob
        else:
            home_rl_prob_val = edges["home_rl_prob"] / 100
        sc, fd = get_sharp_alignment(market, "home_spread")
        sig, score, edge = score_signal(home_rl_prob_val, market["home_rl_odds"], sc, fd)
        edges["home_rl_edge"] = edge; edges["home_rl_score"] = score; edges["home_rl_flag"] = sig

    if market.get("yrfi_odds"):
        sig, score, edge = score_signal(yrfi_prob, market["yrfi_odds"])
        edges["yrfi_edge"]  = edge; edges["yrfi_score"]  = score; edges["yrfi_flag"]  = sig
        sig, score, edge = score_signal(1-yrfi_prob, market.get("nrfi_odds",-105))
        edges["nrfi_edge"]  = edge; edges["nrfi_score"]  = score; edges["nrfi_flag"]  = sig

    if market.get("away_team_total") and market.get("away_tt_over_odds"):
        att_over  = 1 - _poisson_under(runs["away_proj_runs"], market["away_team_total"])
        att_under = 1 - att_over
        edges["away_tt_over_prob"]  = round(att_over*100,1)
        edges["away_tt_under_prob"] = round(att_under*100,1)
        edges["fair_away_tt_over"]  = prob_to_american(att_over)
        edges["fair_away_tt_under"] = prob_to_american(att_under)
        sig, score, edge = score_signal(att_over,  market["away_tt_over_odds"])
        edges["away_tt_over_edge"]  = edge; edges["away_tt_over_score"]  = score; edges["away_tt_over_flag"]  = sig
        sig, score, edge = score_signal(att_under, market.get("away_tt_under_odds",-110))
        edges["away_tt_under_edge"] = edge; edges["away_tt_under_score"] = score; edges["away_tt_under_flag"] = sig

    if market.get("home_team_total") and market.get("home_tt_over_odds"):
        htt_over  = 1 - _poisson_under(runs["home_proj_runs"], market["home_team_total"])
        htt_under = 1 - htt_over
        edges["home_tt_over_prob"]  = round(htt_over*100,1)
        edges["home_tt_under_prob"] = round(htt_under*100,1)
        edges["fair_home_tt_over"]  = prob_to_american(htt_over)
        edges["fair_home_tt_under"] = prob_to_american(htt_under)
        sig, score, edge = score_signal(htt_over,  market["home_tt_over_odds"])
        edges["home_tt_over_edge"]  = edge; edges["home_tt_over_score"]  = score; edges["home_tt_over_flag"]  = sig
        sig, score, edge = score_signal(htt_under, market.get("home_tt_under_odds",-110))
        edges["home_tt_under_edge"] = edge; edges["home_tt_under_score"] = score; edges["home_tt_under_flag"] = sig

    edges["sharp_signals"] = market.get("sharp_signals","—")

    return {
        **info,
        **{k: v for k, v in runs.items() if k not in ("f5_total","f5_away_runs","f5_home_runs")},
        "proj_f5_away":  round(float(runs.get("f5_away_runs",0) or 0), 2),
        "proj_f5_home":  round(float(runs.get("f5_home_runs",0) or 0), 2),
        "proj_f5_total": round(float(runs.get("proj_f5_total",0) or 0), 2),
        "api_proj_total": round(float(runs.get("proj_total",0) or 0), 2),
        "away_win_pct":  away_win_pct,
        "home_win_pct":  home_win_pct,
        "yrfi_prob":     yrfi_prob,
        "park_factor":   park_factor,
        "weather_factor": weather_factor,
        "away_lineup":   ", ".join(away_lineup) if away_lineup else "Not yet posted",
        "home_lineup":   ", ".join(home_lineup) if home_lineup else "Not yet posted",
        "away_era":      away_pitcher.get("era","N/A"), "away_fip":  away_pitcher.get("fip","N/A"),
        "away_k9":       away_pitcher.get("k9","N/A"),  "away_bb9":  away_pitcher.get("bb9","N/A"),
        "home_era":      home_pitcher.get("era","N/A"), "home_fip":  home_pitcher.get("fip","N/A"),
        "home_k9":       home_pitcher.get("k9","N/A"),  "home_bb9":  home_pitcher.get("bb9","N/A"),
        "away_rpg":      away_offense.get("runs_per_game","N/A"),
        "home_rpg":      home_offense.get("runs_per_game","N/A"),
        "away_ops":      away_offense.get("ops","N/A"),
        "home_ops":      home_offense.get("ops","N/A"),
        "away_bullpen_era": away_bullpen.get("bullpen_era","N/A"),
        "home_bullpen_era": home_bullpen.get("bullpen_era","N/A"),
        "away_pitcher_form":       away_pitcher_form.get("recent_form_score","N/A"),
        "away_pitcher_recent_era": away_pitcher_form.get("recent_era","N/A"),
        "away_pitcher_recent_ip":  away_pitcher_form.get("recent_avg_ip","N/A"),
        "home_pitcher_form":       home_pitcher_form.get("recent_form_score","N/A"),
        "home_pitcher_recent_era": home_pitcher_form.get("recent_era","N/A"),
        "home_pitcher_recent_ip":  home_pitcher_form.get("recent_avg_ip","N/A"),
        "away_bp_availability": away_bp_avail.get("bp_availability","N/A"),
        "away_bp_tired":        away_bp_avail.get("bp_used_yesterday","N/A"),
        "home_bp_availability": home_bp_avail.get("bp_availability","N/A"),
        "home_bp_tired":        home_bp_avail.get("bp_used_yesterday","N/A"),
        "away_matchup_summary": away_matchup_summary,
        "home_matchup_summary": home_matchup_summary,
        "away_recent_rpg": away_recent.get("recent_rpg","N/A"),
        "home_recent_rpg": home_recent.get("recent_rpg","N/A"),
        "away_recent_ops": away_recent.get("recent_ops","N/A"),
        "home_recent_ops": home_recent.get("recent_ops","N/A"),
        "away_loc_rpg": away_location.get("away_rpg","N/A"),
        "home_loc_rpg": home_location.get("home_rpg","N/A"),
        "away_lineup_ops": away_lineup_ops or "N/A",
        "home_lineup_ops": home_lineup_ops or "N/A",
        "h2h_record":    f"{h2h.get('h2h_wins',0)}-{h2h.get('h2h_losses',0)}" if h2h else "N/A",
        "h2h_avg_total": h2h.get("h2h_avg_total","N/A"),
        "h2h_games":     h2h.get("h2h_games",0),
        **market, **bp, **edges,
    }

def _poisson_under(lam: float, line: float) -> float:
    prob, k = 0.0, 0
    while k <= line:
        prob += (math.exp(-lam) * lam**k) / math.factorial(k)
        k += 1
    return round(prob, 4)


# ─────────────────────────────────────────────
# SECTION 11 — PUSH TO SHEETS
# ─────────────────────────────────────────────
HEADERS = [
    "Date","Time","Away Team","Home Team","Venue","Weather","Park Factor",
    "Away Pitcher","Away ERA","Away FIP","Away K/9","Away BB/9",
    "Home Pitcher","Home ERA","Home FIP","Home K/9","Home BB/9",
    "Away Bullpen ERA","Home Bullpen ERA",
    "Away SP Form","Away SP Recent ERA","Away SP Avg IP",
    "Home SP Form","Home SP Recent ERA","Home SP Avg IP",
    "Away BP Status","Away BP Tired Arms","Home BP Status","Home BP Tired Arms",
    "Away Lineup vs Home SP","Home Lineup vs Away SP",
    "Away Recent R/G (L15)","Home Recent R/G (L15)","Away Recent OPS","Home Recent OPS",
    "Away Road R/G","Home Home R/G",
    "Away Lineup OPS vs SP","Home Lineup OPS vs SP",
    "H2H Record","H2H Avg Total","H2H Games",
    "Away R/G","Away OPS","Home R/G","Home OPS",
    "Away Proj Runs","Home Proj Runs","Proj Total",
    "F5 Away","F5 Home","F5 Total","BP Blended?",
    "BP Away Runs","BP Home Runs","BP YRFI%","BP Park Run%","BP Park HR%",
    "BP Away SP Inn","BP Away SP Runs","BP Away SP K","BP Away SP BB",
    "BP Home SP Inn","BP Home SP Runs","BP Home SP K","BP Home SP BB",
    "BP Away R/G","BP Away HR/G","BP Home R/G","BP Home HR/G",
    "Away Win%","Home Win%","YRFI Prob",
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

def push_to_sheets(sheet, results: list):
    try:
        ws = sheet.worksheet("Daily Model")
        sheet.del_worksheet(ws)
    except:
        pass
    ws = sheet.add_worksheet("Daily Model", rows=200, cols=80)
    ws.clear()
    all_rows = [["MLB BETTING MODEL — " + today_str()], HEADERS]
    for r in results:
        row = [
            today_str(), r.get("game_time","")[:16],
            r.get("away_team",""), r.get("home_team",""), r.get("venue",""),
            f"{r.get('weather_temp','')}° {r.get('weather_condition','')} {r.get('weather_wind','')}",
            r.get("park_factor",""),
            r.get("away_pitcher",""), r.get("away_era",""), r.get("away_fip",""),
            r.get("away_k9",""),     r.get("away_bb9",""),
            r.get("home_pitcher",""), r.get("home_era",""), r.get("home_fip",""),
            r.get("home_k9",""),     r.get("home_bb9",""),
            r.get("away_bullpen_era",""), r.get("home_bullpen_era",""),
            r.get("away_pitcher_form",""), r.get("away_pitcher_recent_era",""), r.get("away_pitcher_recent_ip",""),
            r.get("home_pitcher_form",""), r.get("home_pitcher_recent_era",""), r.get("home_pitcher_recent_ip",""),
            r.get("away_bp_availability",""), r.get("away_bp_tired",""),
            r.get("home_bp_availability",""), r.get("home_bp_tired",""),
            r.get("away_matchup_summary",""), r.get("home_matchup_summary",""),
            r.get("away_recent_rpg",""), r.get("home_recent_rpg",""),
            r.get("away_recent_ops",""), r.get("home_recent_ops",""),
            r.get("away_loc_rpg",""),    r.get("home_loc_rpg",""),
            r.get("away_lineup_ops",""), r.get("home_lineup_ops",""),
            r.get("h2h_record",""), r.get("h2h_avg_total",""), r.get("h2h_games",""),
            r.get("away_rpg",""), r.get("away_ops",""), r.get("home_rpg",""), r.get("home_ops",""),
            r.get("away_proj_runs",""), r.get("home_proj_runs",""), r.get("proj_total",""),
            r.get("proj_f5_away",""), r.get("proj_f5_home",""), r.get("proj_f5_total",""),
            "✅ Yes" if r.get("bp_blended") else "❌ API Only",
            r.get("bp_away_runs",""), r.get("bp_home_runs",""), r.get("bp_yrfi_pct",""),
            r.get("bp_park_run_pct",""), r.get("bp_park_hr_pct",""),
            r.get("bp_away_sp_inn",""), r.get("bp_away_sp_runs",""),
            r.get("bp_away_sp_k",""),   r.get("bp_away_sp_bb",""),
            r.get("bp_home_sp_inn",""), r.get("bp_home_sp_runs",""),
            r.get("bp_home_sp_k",""),   r.get("bp_home_sp_bb",""),
            r.get("bp_away_rpg",""),    r.get("bp_away_hrpg",""),
            r.get("bp_home_rpg",""),    r.get("bp_home_hrpg",""),
            f"{r.get('away_win_pct',0)*100:.1f}%",
            f"{r.get('home_win_pct',0)*100:.1f}%",
            f"{r.get('yrfi_prob',0)*100:.1f}%",
            r.get("away_ml",""), r.get("home_ml",""),
            r.get("total_line",""), r.get("over_odds",""), r.get("under_odds",""),
            r.get("yrfi_odds",""), r.get("nrfi_odds",""),
            r.get("away_ml_edge",""),  r.get("away_ml_score",""),  r.get("away_ml_flag",""),
            r.get("home_ml_edge",""),  r.get("home_ml_score",""),  r.get("home_ml_flag",""),
            r.get("over_edge",""),     r.get("over_score",""),     r.get("over_flag",""),
            r.get("under_edge",""),    r.get("under_score",""),    r.get("under_flag",""),
            r.get("f5_over_edge",""),  r.get("f5_over_score",""),  r.get("f5_over_flag",""),
            r.get("yrfi_edge",""),     r.get("yrfi_score",""),     r.get("yrfi_flag",""),
            r.get("nrfi_edge",""),     r.get("nrfi_score",""),     r.get("nrfi_flag",""),
            r.get("away_tt_over_edge",""),  r.get("away_tt_over_score",""),  r.get("away_tt_over_flag",""),
            r.get("away_tt_under_edge",""), r.get("away_tt_under_score",""), r.get("away_tt_under_flag",""),
            r.get("home_tt_over_edge",""),  r.get("home_tt_over_score",""),  r.get("home_tt_over_flag",""),
            r.get("home_tt_under_edge",""), r.get("home_tt_under_score",""), r.get("home_tt_under_flag",""),
            r.get("sharp_signals","—"),
            r.get("away_lineup",""), r.get("home_lineup",""),
        ]
        all_rows.append(row)
    ws.append_rows(all_rows, value_input_option="USER_ENTERED")
    print(f"\n✅ Pushed {len(results)} games to Google Sheets")

def push_summary_tab(sheet, results: list):
    try:
        ws = sheet.worksheet("⚡ Summary")
        sheet.del_worksheet(ws)
    except:
        pass
    ws = sheet.add_worksheet("⚡ Summary", rows=100, cols=20)
    summary_headers = [
        "Game","Time","Status","Venue","Proj Score","Total","F5 Total",
        "Away Win%","Home Win%","YRFI%","Away ML","Home ML","Total Line",
        "Best Bets","Bottom Line","Data Source"
    ]
    all_rows = [[f"⚾ MLB MODEL SUMMARY — {today_str()}"], [], summary_headers]
    for r in results:
        away = r.get("away_team","Away")
        home = r.get("home_team","Home")
        bets = build_best_bets_str(r)
        parts = []
        if (r.get("away_ml_edge",0) or 0) > 5:  parts.append(f"{away} has ML value")
        if (r.get("home_ml_edge",0) or 0) > 5:  parts.append(f"{home} has ML value")
        if "FADE"   in str(r.get("over_flag","")):   parts.append("fade the over")
        if "STRONG" in str(r.get("under_flag","")): parts.append("under has value")
        if "STRONG" in str(r.get("yrfi_flag","")):  parts.append("strong YRFI play")
        bottom = ", ".join(parts).capitalize() if parts else "No strong edges"
        all_rows.append([
            f"{away} @ {home}", r.get("game_time","")[:16],
            r.get("game_status",""), r.get("venue",""),
            f"{r.get('away_proj_runs','?')} — {r.get('home_proj_runs','?')}",
            round(float(r.get("proj_total",0) or 0), 2),
            round(float(r.get("proj_f5_total",0) or 0), 2),
            f"{r.get('away_win_pct',0)*100:.1f}%",
            f"{r.get('home_win_pct',0)*100:.1f}%",
            f"{r.get('yrfi_prob',0)*100:.1f}%",
            r.get("away_ml",""), r.get("home_ml",""), r.get("total_line",""),
            bets, bottom,
            "✅ BP+API" if r.get("bp_blended") else "⚠️ API Only",
        ])
    ws.append_rows(all_rows, value_input_option="USER_ENTERED")
    print(f"✅ Summary tab updated")

def build_best_bets_str(r: dict) -> str:
    bets = []
    bet_map = [
        ("away_ml_flag","away_ml",f"{r.get('away_team','')} ML"),
        ("home_ml_flag","home_ml",f"{r.get('home_team','')} ML"),
        ("over_flag","over_odds",f"OVER {r.get('total_line','')}"),
        ("under_flag","under_odds",f"UNDER {r.get('total_line','')}"),
        ("f5_over_flag","f5_over_odds",f"F5 OVER {r.get('proj_f5_total','')}"),
        ("yrfi_flag","yrfi_odds","YRFI"),
        ("nrfi_flag","nrfi_odds","NRFI"),
        ("away_tt_over_flag","away_tt_over_odds",f"{r.get('away_team','')} TT OVER"),
        ("home_tt_over_flag","home_tt_over_odds",f"{r.get('home_team','')} TT OVER"),
    ]
    for flag_key, odds_key, label in bet_map:
        flag = str(r.get(flag_key,""))
        if "STRONG" in flag:
            odds     = r.get(odds_key,"")
            edge_key = flag_key.replace("_flag","_edge")
            edge     = r.get(edge_key,"")
            odds_str = f" {odds:+d}" if isinstance(odds,int) else ""
            edge_str = f" [{edge:+.1f}%]" if isinstance(edge,(int,float)) else ""
            bets.append(f"{flag} {label}{odds_str}{edge_str}")
    return " | ".join(bets) if bets else "— No strong signals"

def print_game_summary(r: dict):
    away = r.get("away_team","Away")
    home = r.get("home_team","Home")
    sep  = "=" * 50
    signals = []
    bet_map = [
        ("away_ml_flag","away_ml",f"{away} ML","fair_away_ml","away_win_pct"),
        ("home_ml_flag","home_ml",f"{home} ML","fair_home_ml","home_win_pct"),
        ("over_flag","over_odds",f"OVER {r.get('total_line','')}","fair_over","over_prob"),
        ("under_flag","under_odds",f"UNDER {r.get('total_line','')}","fair_under","under_prob"),
        ("f5_over_flag","f5_over_odds",f"F5 OVER {r.get('proj_f5_total','')}","fair_f5_over","f5_over_prob"),
        ("yrfi_flag","yrfi_odds","YRFI","fair_yrfi","yrfi_prob_pct"),
        ("nrfi_flag","nrfi_odds","NRFI","fair_nrfi","nrfi_prob_pct"),
    ]
    r_ext = dict(r)
    r_ext["yrfi_prob_pct"] = round(r.get("yrfi_prob",0)*100, 1)
    r_ext["nrfi_prob_pct"] = round((1-r.get("yrfi_prob",0))*100, 1)
    for flag_key, odds_key, label, fair_key, prob_key in bet_map:
        flag = r.get(flag_key,"")
        if flag and "STRONG" in str(flag):
            edge_key  = flag_key.replace("_flag","_edge")
            edge      = r.get(edge_key,"")
            odds      = r.get(odds_key,"")
            fair      = r.get(fair_key,"")
            prob_raw  = r.get(prob_key) or r_ext.get(prob_key)
            # Convert decimal win probs to %
            if prob_key in ("away_win_pct","home_win_pct") and isinstance(prob_raw,float) and prob_raw <= 1:
                prob = round(prob_raw*100, 1)
            else:
                prob = prob_raw
            odds_str  = f" {odds:+d}" if isinstance(odds,int) else ""
            fair_str  = f" (Fair: {fair:+d})" if isinstance(fair,int) else ""
            edge_str  = f" [Edge: {edge:+.1f}%]" if isinstance(edge,(int,float)) else ""
            prob_str  = f" | Prob: {prob:.1f}%" if isinstance(prob,(int,float)) else ""
            kelly_str = ""
            if isinstance(prob,(int,float)) and isinstance(odds,int):
                kelly = kelly_bet_size(prob/100, odds)
                if kelly["bet_dollars"] > 0:
                    kelly_str = f" | 💰 ${kelly['bet_dollars']:.0f} ({kelly['bet_units']:.1f}u)"
            signals.append(f"   {flag}  → {label}{odds_str}{fair_str}{edge_str}{prob_str}{kelly_str}")
    blended = "✅ BP Blended" if r.get("bp_blended") else "⚠️  API Only"
    print(f"""
{sep}
📋 GAME SUMMARY: {away} @ {home}
{sep}
🏟️  {r.get('venue','N/A')} | {r.get('game_time','')[:16]}
⏱️  Status: {r.get('game_status','⏰ Unknown')}
🌤️  {r.get('weather_temp','')}° {r.get('weather_condition','')} | {r.get('weather_wind','')} | Park: {r.get('park_factor',1.0):.2f}x
📊 Data: {blended}

⚾  PROJECTED SCORE:  {away} {r.get('away_proj_runs','?')} — {r.get('home_proj_runs','?')} {home}
📈 PROJ TOTAL:        {r.get('proj_total','?')}  |  F5: {r.get('proj_f5_total','?')}
🏆 WIN PROBABILITY:   {away} {r.get('away_win_pct',0)*100:.1f}% — {home} {r.get('home_win_pct',0)*100:.1f}%
   (capped at {MAX_WIN_PROB*100:.0f}% max — prevents overconfidence)
🔥 YRFI PROBABILITY:  {r.get('yrfi_prob',0)*100:.1f}%

⚾  PITCHER FORM:
   {away} {r.get('away_pitcher','?')}: {r.get('away_pitcher_form','?')} | L3 ERA: {r.get('away_pitcher_recent_era','?')} | Avg IP: {r.get('away_pitcher_recent_ip','?')}
   {home} {r.get('home_pitcher','?')}: {r.get('home_pitcher_form','?')} | L3 ERA: {r.get('home_pitcher_recent_era','?')} | Avg IP: {r.get('home_pitcher_recent_ip','?')}

🔋 BULLPEN STATUS:
   {away}: {r.get('away_bp_availability','?')} | Tired: {r.get('away_bp_tired','None')}
   {home}: {r.get('home_bp_availability','?')} | Tired: {r.get('home_bp_tired','None')}

💰 BET SIGNALS:
{''.join([s+chr(10) for s in signals]) if signals else '   — No strong signals'}
⚡ SHARP MONEY:
   {r.get('sharp_signals','— No Action Network data')}
{sep}""")


# ─────────────────────────────────────────────
# TRACKER
# ─────────────────────────────────────────────
def push_tracker_rows(sheet, results: list):
    TRACKER_TAB = "📊 Tracker"
    TRACKER_HEADERS = [
        "Date","Game","Bet Type","Our Signal","Score (0-100)",
        "Our Prob%","Fair Odds","Market Odds","Edge%",
        "Our Proj Away","Our Proj Home","Our Proj Total",
        "BP Proj Away","BP Proj Home","BP Proj Total","BP YRFI%",
        "Total Diff (Ours vs BP)","Sharp Signal",
        "Actual Away","Actual Home","Actual Total","Hit/Miss","Notes"
    ]
    try:
        ws = sheet.worksheet(TRACKER_TAB)
    except Exception:
        ws = sheet.add_worksheet(TRACKER_TAB, rows=1000, cols=25)
        ws.append_row(TRACKER_HEADERS)
    today = today_str()
    try:
        all_vals = ws.get_all_values()
    except Exception:
        all_vals = []
    already_logged = set()
    for row in all_vals:
        if not row or row[0] != today:
            continue
        g = row[1].strip().lower() if len(row) > 1 else ""
        b = row[2].strip().lower() if len(row) > 2 else ""
        if g and b:
            already_logged.add((g, b))
    if already_logged:
        print(f"  📋 {len(already_logged)} signals already logged today")
    all_rows = []
    for r in results:
        if r.get("skipped"):
            continue
        away = r.get("away_team","Away")
        home = r.get("home_team","Home")
        game = f"{away} @ {home}"
        our_away  = r.get("away_proj_runs","")
        our_home  = r.get("home_proj_runs","")
        our_total = r.get("proj_total","")
        bp_away   = r.get("bp_away_runs","")
        bp_home   = r.get("bp_home_runs","")
        bp_total  = round(float(bp_away)+float(bp_home),2) if bp_away and bp_home else ""
        bp_yrfi   = f"{r.get('bp_yrfi_pct','')}%" if r.get("bp_yrfi_pct") else ""
        try:
            total_diff = f"{round(float(our_total)-float(bp_total),2):+.2f}" if bp_total != "" and our_total != "" else "N/A"
        except Exception:
            total_diff = "N/A"
        rl_lbl     = abs(float(r.get("away_rl_line",1.5) or 1.5))
        signal_map = [
            ("away_ml_flag","away_ml_edge","away_ml",      r.get("away_win_pct",0)*100, f"{away} ML",      r.get("fair_away_ml",""),"away_ml_score"),
            ("home_ml_flag","home_ml_edge","home_ml",      r.get("home_win_pct",0)*100, f"{home} ML",      r.get("fair_home_ml",""),"home_ml_score"),
            ("over_flag",   "over_edge",   "over_odds",    r.get("over_prob"),          f"OVER {r.get('total_line','')}",r.get("fair_over",""),"over_score"),
            ("under_flag",  "under_edge",  "under_odds",   r.get("under_prob"),         f"UNDER {r.get('total_line','')}",r.get("fair_under",""),"under_score"),
            ("f5_over_flag","f5_over_edge","f5_over_odds", r.get("f5_over_prob"),       f"F5 OVER {r.get('proj_f5_total','')}",r.get("fair_f5_over",""),"f5_over_score"),
            ("yrfi_flag",   "yrfi_edge",   "yrfi_odds",    r.get("yrfi_prob",0)*100,   "YRFI",            r.get("fair_yrfi",""),"yrfi_score"),
            ("nrfi_flag",   "nrfi_edge",   "nrfi_odds",    (1-r.get("yrfi_prob",0))*100,"NRFI",           r.get("fair_nrfi",""),"nrfi_score"),
        ]
        for flag_key, edge_key, odds_key, prob_val, bet_label, fair_odds, score_key in signal_map:
            flag = r.get(flag_key,"")
            if not flag or flag in ("","—","- "):
                continue
            flag_str = str(flag)
            if any(x in flag_str for x in ("FADE","SKIP","— ","WATCH")):
                continue
            if (game.lower(), bet_label.lower()) in already_logged:
                continue
            edge  = r.get(edge_key,"")
            odds  = r.get(odds_key,"")
            score = r.get(score_key,"")
            if   "DOUBLE STRONG" in flag_str: signal_label = "🔥🔥 DOUBLE STRONG"
            elif "STRONG"        in flag_str: signal_label = "🔥 STRONG"
            elif "LEAN"          in flag_str: signal_label = "✅ LEAN"
            else:                             signal_label = flag_str
            all_rows.append([
                today, game, bet_label, signal_label,
                f"{score}/100" if isinstance(score,(int,float)) else "",
                f"{prob_val:.1f}%" if prob_val is not None else "",
                f"{fair_odds:+d}" if isinstance(fair_odds,int) else "",
                odds if odds else "",
                f"{edge:+.1f}%" if isinstance(edge,(int,float)) else "",
                our_away, our_home, our_total,
                bp_away, bp_home, bp_total, bp_yrfi,
                total_diff, r.get("sharp_signals","-"),
                "", "", "", "", ""
            ])
    if all_rows:
        ws.append_rows(all_rows, value_input_option="USER_ENTERED")
        print(f"  ✅ Added {len(all_rows)} new signals to tracker")
    else:
        print(f"  ✅ Tracker up to date")

def check_tracker_readiness(sheet) -> None:
    try:
        ws       = sheet.worksheet("📊 Tracker")
        rows     = ws.get_all_values()
        if len(rows) < 2:
            print("\n📊 TRACKER STATUS: Empty")
            return
        header_row = None
        for i, row in enumerate(rows):
            if row and "Bet Type" in row:
                header_row = i
                break
        if header_row is None:
            return
        headers   = rows[header_row]
        data_rows = rows[header_row + 1:]
        try:
            hit_miss_col = headers.index("Hit/Miss")
            signal_col   = headers.index("Our Signal")
        except:
            return
        total_signals = wl_filled = wins = losses = 0
        unique_games = set(); unique_dates = set()
        for row in data_rows:
            if not row or not row[0]:
                continue
            total_signals += 1
            game     = row[1] if len(row) > 1 else ""
            date     = row[0] if len(row) > 0 else ""
            hit_miss = row[hit_miss_col].strip().upper() if hit_miss_col < len(row) else ""
            if game: unique_games.add(game)
            if date: unique_dates.add(date)
            if hit_miss in ("WIN","W","WON"):   wins += 1; wl_filled += 1
            elif hit_miss in ("LOSS","L"):       losses += 1; wl_filled += 1
        hit_rate = round(wins/wl_filled*100,1) if wl_filled > 0 else 0
        READY_ROUGH = 30
        bar_len = 20
        filled  = int(bar_len * min(wl_filled, READY_ROUGH) / READY_ROUGH)
        bar     = "█"*filled + "░"*(bar_len-filled)
        if wl_filled >= 100:   ready_str = "🔥 FULLY READY"
        elif wl_filled >= 50:  ready_str = "✅ READY"
        elif wl_filled >= 30:  ready_str = "⚠️  ROUGH READY"
        else:                  ready_str = f"❌ NOT YET — Need {READY_ROUGH-wl_filled} more"
        print(f"\n{'='*55}")
        print(f"📊 TRACKER STATUS — {today_str()}")
        print(f"{'='*55}")
        print(f"  Signals logged:  {total_signals} | Days: {len(unique_dates)} | Games: {len(unique_games)}")
        print(f"  W/L results:     {wl_filled} ({wins}W / {losses}L = {hit_rate}% hit rate)")
        print(f"  [{bar}] {wl_filled}/{READY_ROUGH} → {ready_str}")
        print(f"{'='*55}")
    except Exception as e:
        print(f"\n📊 TRACKER STATUS: Could not read — {e}")

def print_tracker_reminder():
    print("""
📊 TRACKER REMINDER:
   After each game finishes fill in the Tracker tab:
     Actual Away Score | Actual Home Score | WIN / LOSS / PUSH
""")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    global _current_sheet
    print("⚾  MLB BETTING MODEL")
    print(f"   Date: {today_str()}")
    print(f"   Win prob cap: {MAX_WIN_PROB*100:.0f}% | Run diff cap: {MAX_RUN_DIFF} runs")
    print("="*50)

    print("\n📊 Connecting to Google Sheets...")
    _current_sheet = get_sheet(SHEET_NAME)
    sheet = _current_sheet
    print(f"   Connected to: {sheet.title}")

    check_tracker_readiness(sheet)
    print_roi_report(sheet)

    print("\n⚙️  Loading calibration weights...")
    load_calibration(sheet)

    create_input_tab(sheet)

    odds = get_mlb_odds()
    if odds:
        push_odds_to_input_tab(sheet, odds)

    run_label     = get_run_label()
    compare_label = get_compare_label(run_label)
    print(f"\n📡 Run: {run_label}", end=" | ")
    if compare_label:
        print(f"Comparing vs {compare_label} snapshot...")
        snapshot = load_odds_snapshot_from_sheet(sheet, compare_label)
        if snapshot and odds:
            movement = detect_line_movement(odds, snapshot)
            print_line_movement_report(movement, compare_label)
            if movement:
                push_movement_to_sheet(sheet, movement, run_label, compare_label)
    else:
        print("First run of the day — saving opening lines")

    if odds:
        save_odds_snapshot_to_sheet(sheet, odds, run_label)

    games = get_todays_games()
    if not games:
        print("❌ No games found today.")
        return

    results = []
    skipped = []
    for game in games:
        try:
            result = analyze_game(game)
            if result.get("skipped"):
                skipped.append(result)
            else:
                results.append(result)
                print_game_summary(result)
        except Exception as e:
            print(f"  ❌ Error analyzing game: {e}")
            continue

    if skipped:
        print(f"\n⏭️  Skipped {len(skipped)} games already started/finished")

    if results:
        push_to_sheets(sheet, results)
        push_summary_tab(sheet, results)
        push_tracker_rows(sheet, results)
        print_tracker_reminder()

    print("\n🏁 Done!")


if __name__ == "__main__":
    import sys
    args = sys.argv[1:]

    if args and args[0].lower() in ("help","-h","--help"):
        print("""
⚾  MLB BETTING MODEL — COMMANDS
==================================
  python mlb_model.py          → Run all games
  python mlb_model.py list     → List today's games
  python mlb_model.py Cubs     → Run only Cubs game
  python mlb_model.py help     → This menu
==================================
KEY FIXES IN THIS VERSION:
  • Win prob capped at 65% max (was outputting 70-80%)
  • Run differential capped at 2.5 runs in win_probability()
  • prob calibration factor now reads correctly from R script
  • avg_predicted_prob fixed from 0.007 → ~0.55-0.65
==================================
        """)
    elif args and args[0].lower() == "list":
        print("⚾  TODAY'S GAMES")
        print(f"   Date: {today_str()}")
        print("="*50)
        games = get_todays_games()
        for i, g in enumerate(games):
            info = parse_game_info(g)
            print(f"  {i+1}. {info['away_team']} @ {info['home_team']}  —  {info['away_pitcher']} vs {info['home_pitcher']}")
    elif args:
        filters = [a.lower() for a in args]
        print(f"⚾  FILTERED RUN: {', '.join(args)}")
        print("="*50)
        _current_sheet = get_sheet(SHEET_NAME)
        sheet  = _current_sheet
        create_input_tab(sheet)
        games  = get_todays_games()
        matched = []
        for g in games:
            info = parse_game_info(g)
            if any(f in f"{info['away_team']} {info['home_team']}".lower() for f in filters):
                matched.append(g)
        if not matched:
            print(f"❌ No games found matching: {', '.join(args)}")
        else:
            results = []
            for game in matched:
                try:
                    result = analyze_game(game)
                    results.append(result)
                    print_game_summary(result)
                except Exception as e:
                    print(f"  ❌ Error: {e}")
            if results:
                push_to_sheets(sheet, results)
                push_summary_tab(sheet, results)
                push_tracker_rows(sheet, results)
        print("\n🏁 Done!")
    else:
        main()
