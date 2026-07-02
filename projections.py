"""
fetchers/mlb_api.py
===================
All MLB Stats API calls. One function per data type.
Nothing here touches odds or Google Sheets.
"""

import csv
import datetime
import io
import requests

from config import (
    MLB_API_BASE, SEASON,
    LEAGUE_RPG, LEAGUE_ERA, FIP_CONSTANT,
)

# ─────────────────────────────────────────────
# CORE REQUEST
# ─────────────────────────────────────────────
def api_get(endpoint: str, params: dict = None) -> dict:
    url = f"{MLB_API_BASE}{endpoint}"
    try:
        r = requests.get(url, params=params or {}, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  ⚠️  MLB API error {endpoint}: {e}")
        return {}


# ─────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────
def today_str() -> str:
    return datetime.date.today().strftime("%Y-%m-%d")


def american_to_prob(odds: int) -> float:
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def prob_to_american(prob: float) -> int:
    prob = max(0.01, min(0.99, prob))
    if prob >= 0.5:
        return round(-prob / (1 - prob) * 100)
    return round((1 - prob) / prob * 100)


STADIUM_COORDS = {
    "Coors Field":                  (39.7559, -104.9942),
    "Great American Ball Park":     (39.0979, -84.5082),
    "Fenway Park":                  (42.3467, -71.0972),
    "Globe Life Field":             (32.7473, -97.0825),
    "Yankee Stadium":               (40.8296, -73.9262),
    "Oriole Park at Camden Yards":  (39.2838, -76.6217),
    "Citizens Bank Park":           (39.9061, -75.1665),
    "Wrigley Field":                (41.9484, -87.6553),
    "Truist Park":                  (33.8908, -84.4678),
    "American Family Field":        (43.0280, -87.9712),
    "Kauffman Stadium":             (39.0517, -94.4803),
    "Progressive Field":            (41.4962, -81.6852),
    "Nationals Park":               (38.8730, -77.0074),
    "Target Field":                 (44.9817, -93.2781),
    "Rogers Centre":                (43.6414, -79.3894),
    "Angel Stadium":                (33.8003, -117.8827),
    "Comerica Park":                (42.3390, -83.0485),
    "PNC Park":                     (40.4469, -80.0057),
    "Busch Stadium":                (38.6226, -90.1928),
    "Guaranteed Rate Field":        (41.8300, -87.6338),
    "Rate Field":                   (41.8300, -87.6338),
    "Daikin Park":                  (29.7572, -95.3555),
    "Minute Maid Park":             (29.7572, -95.3555),
    "loanDepot park":               (25.7781, -80.2197),
    "LoanDepot Park":               (25.7781, -80.2197),
    "Dodger Stadium":               (34.0739, -118.2400),
    "UNIQLO Field":                 (34.0739, -118.2400),
    "Chase Field":                  (33.4453, -112.0667),
    "Citi Field":                   (40.7571, -73.8458),
    "T-Mobile Park":                (47.5914, -122.3324),
    "Oracle Park":                  (37.7786, -122.3893),
    "Petco Park":                   (32.7076, -117.1570),
    "Tropicana Field":              (27.7682, -82.6534),
    "Camden Yards":                 (39.2838, -76.6217),
}

TEAM_ABBREV_MAP = {
    "New York Yankees": "NYY", "Boston Red Sox": "BOS",
    "Toronto Blue Jays": "TOR", "Tampa Bay Rays": "TB",
    "Baltimore Orioles": "BAL", "Chicago White Sox": "CWS",
    "Cleveland Guardians": "CLE", "Detroit Tigers": "DET",
    "Kansas City Royals": "KC", "Minnesota Twins": "MIN",
    "Houston Astros": "HOU", "Los Angeles Angels": "LAA",
    "Athletics": "OAK", "Seattle Mariners": "SEA",
    "Texas Rangers": "TEX", "Atlanta Braves": "ATL",
    "Miami Marlins": "MIA", "New York Mets": "NYM",
    "Philadelphia Phillies": "PHI", "Washington Nationals": "WSH",
    "Chicago Cubs": "CHC", "Cincinnati Reds": "CIN",
    "Milwaukee Brewers": "MIL", "Pittsburgh Pirates": "PIT",
    "St. Louis Cardinals": "STL", "Arizona Diamondbacks": "ARI",
    "Colorado Rockies": "COL", "Los Angeles Dodgers": "LAD",
    "San Diego Padres": "SD", "San Francisco Giants": "SF",
}


# ─────────────────────────────────────────────
# SCHEDULE — TODAY'S GAMES
# ─────────────────────────────────────────────
def get_todays_games() -> list:
    data = api_get("/schedule", {
        "sportId": 1,
        "date": today_str(),
        "hydrate": "probablePitcher,venue,weather,lineups",
    })
    games = []
    seen = set()
    for db in data.get("dates", []):
        for g in db.get("games", []):
            gid = g.get("gamePk")
            if gid and gid not in seen:
                seen.add(gid)
                games.append(g)
    print(f"✅ Found {len(games)} games today ({today_str()})")
    return games


def parse_game_info(game: dict) -> dict:
    teams = game.get("teams", {})
    away  = teams.get("away", {})
    home  = teams.get("home", {})

    away_pp = away.get("probablePitcher", {})
    home_pp = home.get("probablePitcher", {})

    venue   = game.get("venue", {}).get("name", "")
    coords  = STADIUM_COORDS.get(venue, (None, None))
    weather = game.get("weather", {})

    return {
        "game_pk":         game.get("gamePk"),
        "game_time":       game.get("gameDate", ""),
        "venue":           venue,
        "lat":             coords[0],
        "lon":             coords[1],
        "weather_temp":    weather.get("temp", ""),
        "weather_wind":    weather.get("wind", ""),
        "weather_cond":    weather.get("condition", ""),
        "away_team":       away.get("team", {}).get("name", "Unknown"),
        "away_team_id":    away.get("team", {}).get("id"),
        "away_pitcher":    away_pp.get("fullName", "TBD"),
        "away_pitcher_id": away_pp.get("id"),
        "home_team":       home.get("team", {}).get("name", "Unknown"),
        "home_team_id":    home.get("team", {}).get("id"),
        "home_pitcher":    home_pp.get("fullName", "TBD"),
        "home_pitcher_id": home_pp.get("id"),
    }


def check_game_timing(game: dict, info: dict) -> str:
    try:
        game_time_str = info.get("game_time", "")
        if not game_time_str:
            return "⏰ Unknown"
        if game_time_str.endswith("Z"):
            game_time_str = game_time_str.replace("Z", "+00:00")
        elif "+" not in game_time_str and len(game_time_str) >= 16:
            game_time_str += "+00:00"

        game_time = datetime.datetime.fromisoformat(game_time_str)
        now       = datetime.datetime.now(datetime.timezone.utc)
        diff      = (now - game_time).total_seconds() / 3600

        abstract = game.get("status", {}).get("abstractGameState", "")
        detailed = game.get("status", {}).get("detailedState", "")

        if abstract == "Final" or "Final" in detailed:
            return "🏁 Final"
        if abstract == "Live" or "In Progress" in detailed:
            return "⚡ In Progress"
        if diff > 3.5:
            return f"🏁 Likely Final ({diff:.1f}hrs ago)"
        elif diff > 0.5:
            return f"⚡ In Progress ({diff:.1f}hrs)"
        elif diff > -0.5:
            return "🔔 Starting Soon"
        else:
            et_offset = datetime.timedelta(hours=-4)
            game_et   = game_time + et_offset
            return f"⏰ {game_et.strftime('%-I:%M%p')} ET"
    except Exception:
        return "⏰ Unknown"


# ─────────────────────────────────────────────
# HOME PLATE UMPIRE
# ─────────────────────────────────────────────
def get_home_plate_ump(game_pk: int) -> str:
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


# ─────────────────────────────────────────────
# PITCHER STATS
# ─────────────────────────────────────────────
def get_pitcher_stats(pitcher_id: int) -> dict:
    if not pitcher_id:
        return {}
    try:
        data = api_get(f"/people/{pitcher_id}/stats", {
            "stats": "season", "group": "pitching",
            "season": SEASON, "sportId": 1,
        })
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s  = splits[0]["stat"]
        ip = float(s.get("inningsPitched", 0) or 0)
        er = float(s.get("earnedRuns", 0) or 0)
        h  = float(s.get("hits", 0) or 0)
        bb = float(s.get("baseOnBalls", 0) or 0)
        k  = float(s.get("strikeOuts", 0) or 0)
        hr  = float(s.get("homeRuns", 0) or 0)
        hbp = float(s.get("hitByPitch", 0) or 0)
        era  = round((er / ip) * 9, 2) if ip > 0 else LEAGUE_ERA
        fip  = round(((13*hr) + (3*(bb + hbp)) - (2*k)) / ip + FIP_CONSTANT, 2) if ip > 0 else era
        whip = round((h + bb) / ip, 2) if ip > 0 else 1.30
        k9   = round((k / ip) * 9, 2) if ip > 0 else 8.0
        return {
            "era": era, "fip": fip, "whip": whip, "k9": k9,
            "ip": ip,
            "gs": int(s.get("gamesStarted", 0) or 0),
            "hand": s.get("pitchHand", {}).get("code", "R") if isinstance(s.get("pitchHand"), dict) else "R",
        }
    except Exception:
        return {}


def get_pitcher_recent_form(pitcher_id: int, last_n: int = 3) -> dict:
    if not pitcher_id:
        return {}
    try:
        data = api_get(f"/people/{pitcher_id}/stats", {
            "stats": "gameLog", "group": "pitching",
            "season": SEASON, "sportId": 1,
        })
        splits = data.get("stats", [{}])[0].get("splits", [])
        starts = [s for s in splits if int(s.get("stat", {}).get("gamesStarted", 0)) > 0][-last_n:]
        if not starts:
            return {}
        eras, whips, k9s, ips, runs = [], [], [], [], []
        for s in starts:
            stat = s.get("stat", {})
            ip   = float(stat.get("inningsPitched", 0) or 0)
            er   = float(stat.get("earnedRuns", 0) or 0)
            h    = float(stat.get("hits", 0) or 0)
            bb   = float(stat.get("baseOnBalls", 0) or 0)
            k    = float(stat.get("strikeOuts", 0) or 0)
            r    = float(stat.get("runs", 0) or 0)
            if ip > 0:
                eras.append(min(round((er / ip) * 9, 2), 15.00))  # cap at 15 ERA
                whips.append(round((h + bb) / ip, 2))
                k9s.append(round((k / ip) * 9, 2))
                ips.append(ip)
                runs.append(r)
        if not eras:
            return {}
        ae = sum(eras) / len(eras)
        ai = sum(ips)  / len(ips)
        if   ae <= 2.50 and ai >= 6.0: form = "🔥 HOT"
        elif ae <= 3.50 and ai >= 5.5: form = "✅ SOLID"
        elif ae <= 4.50:               form = "➡️ AVERAGE"
        elif ae <= 6.00:               form = "❄️ COLD"
        else:                          form = "🚨 STRUGGLING"
        return {
            "recent_era":      round(ae, 2),
            "recent_whip":     round(sum(whips) / len(whips), 2),
            "recent_k9":       round(sum(k9s) / len(k9s), 2),
            "recent_avg_ip":   round(ai, 2),
            "recent_avg_runs": round(sum(runs) / len(runs), 2),
            "recent_starts":   len(starts),
            "recent_form":     form,
        }
    except Exception:
        return {}


def get_pitcher_days_rest(pitcher_id: int) -> dict:
    if not pitcher_id:
        return {"days_rest": 5, "rest_factor": 1.00, "rest_label": "Normal (5d)"}
    try:
        data = api_get(f"/people/{pitcher_id}/stats", {
            "stats": "gameLog", "group": "pitching",
            "season": SEASON, "sportId": 1,
        })
        splits    = data.get("stats", [{}])[0].get("splits", [])
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
        days = (datetime.date.today() - last_date).days
        if   days < 4: factor, label = 0.92, f"⚠️ SHORT REST ({days}d)"
        elif days == 4: factor, label = 0.97, "4 day rest"
        elif days == 5: factor, label = 1.00, "Normal (5d)"
        elif days == 6: factor, label = 1.01, "Extra rest (6d)"
        else:           factor, label = 0.98, f"Rust ({days}d)"
        return {"days_rest": days, "rest_factor": factor, "rest_label": label}
    except Exception:
        return {"days_rest": 5, "rest_factor": 1.00, "rest_label": "Unknown"}


# ─────────────────────────────────────────────
# TEAM OFFENSE / BULLPEN
# ─────────────────────────────────────────────
def get_team_offense(team_id: int) -> dict:
    season_weights = {
        SEASON: 0.30, SEASON-1: 0.25,
        SEASON-2: 0.20, SEASON-3: 0.15, SEASON-4: 0.10,
    }
    all_data = {}
    for season, weight in season_weights.items():
        try:
            data = api_get(f"/teams/{team_id}/stats", {
                "stats": "season", "group": "hitting",
                "season": season, "sportId": 1,
            })
            splits = data.get("stats", [{}])[0].get("splits", [])
            if not splits:
                continue
            s     = splits[0]["stat"]
            games = max(int(s.get("gamesPlayed", 1)), 1)
            all_data[season] = {
                "weight":       weight,
                "runs_per_game": round(float(s.get("runs", 0)) / games, 2),
                "ops":          float(s.get("ops", 0) or 0),
                "avg":          float(s.get("avg", 0) or 0),
                "obp":          float(s.get("obp", 0) or 0),
                "slg":          float(s.get("slg", 0) or 0),
                "k_pct":        float(s.get("strikeoutPercentage", 22.0) or 22.0),
                "bb_pct":       float(s.get("walkPercentage", 8.5) or 8.5),
                "games":        games,
            }
        except Exception:
            continue
    if not all_data:
        return {"runs_per_game": LEAGUE_RPG, "ops": 0.720, "avg": 0.250,
                "obp": 0.320, "slg": 0.400, "k_pct": 22.0, "bb_pct": 8.5}

    def blend(key, default):
        total = w_total = 0.0
        for d in all_data.values():
            v = d.get(key, default)
            if v:
                total   += v * d["weight"]
                w_total += d["weight"]
        return round(total / w_total, 3) if w_total > 0 else default

    return {
        "runs_per_game": blend("runs_per_game", LEAGUE_RPG),
        "ops":           blend("ops", 0.720),
        "avg":           blend("avg", 0.250),
        "obp":           blend("obp", 0.320),
        "slg":           blend("slg", 0.400),
        "k_pct":         blend("k_pct", 22.0),
        "bb_pct":        blend("bb_pct", 8.5),
        "games_current": all_data.get(SEASON, {}).get("games", 0),
    }


def get_recent_team_offense(team_id: int, days: int = 15) -> dict:
    try:
        start = (datetime.date.today() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        data  = api_get(f"/teams/{team_id}/stats", {
            "stats": "byDateRange", "group": "hitting",
            "season": SEASON, "sportId": 1,
            "startDate": start, "endDate": today_str(),
        })
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s     = splits[0]["stat"]
        games = max(int(s.get("gamesPlayed", 1)), 1)
        return {
            "recent_rpg":  round(float(s.get("runs", 0)) / games, 2),
            "recent_ops":  float(s.get("ops", 0) or 0),
            "recent_obp":  float(s.get("obp", 0) or 0),
            "recent_avg":  float(s.get("avg", 0) or 0),
            "recent_games": games,
        }
    except Exception:
        return {}


def get_home_away_splits(team_id: int, side: str) -> dict:
    sit = "h" if side == "home" else "a"
    try:
        data = api_get(f"/teams/{team_id}/stats", {
            "stats": "statSplits", "group": "hitting",
            "season": SEASON, "sportId": 1, "sitCodes": sit,
        })
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s     = splits[0]["stat"]
        games = max(int(s.get("gamesPlayed", 1)), 1)
        return {
            f"{side}_rpg": round(float(s.get("runs", 0)) / games, 2),
            f"{side}_ops": float(s.get("ops", 0) or 0),
            f"{side}_obp": float(s.get("obp", 0) or 0),
            f"{side}_avg": float(s.get("avg", 0) or 0),
        }
    except Exception:
        return {}


def get_bullpen_stats(team_id: int) -> dict:
    try:
        data = api_get(f"/teams/{team_id}/stats", {
            "stats": "season", "group": "pitching",
            "season": SEASON, "sportId": 1, "playerPool": "qualifier",
        })
        eras, whips, ks, bbs, ips = [], [], [], [], []
        for split in data.get("stats", [{}])[0].get("splits", []):
            s  = split.get("stat", {})
            gs = int(s.get("gamesStarted", 0) or 0)
            g  = int(s.get("gamesPitched", 0) or 0)
            ip = float(s.get("inningsPitched", 0) or 0)
            if gs == 0 and g > 0 and ip > 0:
                eras.append(float(s.get("era", 0) or 0))
                whips.append(float(s.get("whip", 0) or 0))
                ks.append(float(s.get("strikeoutsPer9Inn", 0) or 0))
                bbs.append(float(s.get("walksPer9Inn", 0) or 0))
                ips.append(ip)
        if not eras:
            return {}
        def wavg(vals, weights):
            tw = sum(weights)
            return round(sum(v * w for v, w in zip(vals, weights)) / tw, 2) if tw > 0 else 0
        return {
            "bullpen_era":  wavg(eras, ips),
            "bullpen_whip": wavg(whips, ips),
            "bullpen_k9":   wavg(ks, ips),
            "bullpen_bb9":  wavg(bbs, ips),
            "relievers":    len(eras),
        }
    except Exception:
        return {}


def get_bullpen_availability(team_id: int) -> dict:
    """Yesterday's bullpen usage — tired arms."""
    try:
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        data = api_get("/schedule", {
            "sportId": 1, "date": yesterday,
            "teamId": team_id, "hydrate": "boxscore",
        })
        tired_names = []
        total_score = 0
        for db in data.get("dates", []):
            for g in db.get("games", []):
                bs = g.get("boxscore", {})
                for side in ["away", "home"]:
                    t = bs.get("teams", {}).get(side, {})
                    if t.get("team", {}).get("id") != team_id:
                        continue
                    for pid, pd in t.get("players", {}).items():
                        s    = pd.get("stats", {}).get("pitching", {})
                        gs   = int(s.get("gamesStarted", 0) or 0)
                        outs = int(s.get("outs", 0) or 0)
                        if gs == 0 and outs >= 3:
                            tired_names.append(pd.get("person", {}).get("fullName", "?"))
                            total_score += min(outs, 9)
        fatigue = min(total_score / 27.0, 1.0)
        return {
            "tired_count":   len(tired_names),
            "tired_pitchers": tired_names,
            "fatigue_score": round(fatigue, 3),
            "bp_available":  fatigue < 0.40,
        }
    except Exception:
        return {"tired_count": 0, "tired_pitchers": [], "fatigue_score": 0.0, "bp_available": True}


def get_bullpen_rolling_workload(team_id: int, days: int = 3) -> dict:
    """Total bullpen IP over last N days — more accurate fatigue read."""
    try:
        end_date   = datetime.date.today() - datetime.timedelta(days=1)
        start_date = end_date - datetime.timedelta(days=days - 1)
        data = api_get("/schedule", {
            "sportId": 1,
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate":   end_date.strftime("%Y-%m-%d"),
            "teamId":    team_id,
            "hydrate":   "boxscore",
        })
        total_bp_ip = 0.0
        for db in data.get("dates", []):
            for g in db.get("games", []):
                if g.get("status", {}).get("abstractGameState") != "Final":
                    continue
                bs = g.get("boxscore", {})
                for side in ["away", "home"]:
                    t = bs.get("teams", {}).get(side, {})
                    if t.get("team", {}).get("id") != team_id:
                        continue
                    for pid, pd in t.get("players", {}).items():
                        s  = pd.get("stats", {}).get("pitching", {})
                        gs = int(s.get("gamesStarted", 0) or 0)
                        ip = float(s.get("inningsPitched", 0) or 0)
                        if gs == 0 and ip > 0:
                            total_bp_ip += ip
        # ~4.5 BP IP per game * N games = expected workload
        expected = 4.5 * days
        overuse  = total_bp_ip > expected * 1.20
        return {
            "bp_ip_last3":   round(total_bp_ip, 1),
            "bp_overused":   overuse,
            "bp_roll_factor": 0.96 if overuse else 1.00,
        }
    except Exception:
        return {"bp_ip_last3": 0.0, "bp_overused": False, "bp_roll_factor": 1.00}


# ─────────────────────────────────────────────
# LINEUP / BATTER VS PITCHER
# ─────────────────────────────────────────────
def get_lineup_with_ids(game: dict, side: str) -> list:
    try:
        lineups = game.get("lineups", {})
        batters = lineups.get(f"{side}Players", [])
        return [{"name": p.get("fullName", "Unknown"), "id": p.get("id")} for p in batters[:9]]
    except Exception:
        return []


def get_batter_stats(player_id: int, vs_hand: str = None) -> dict:
    if not player_id:
        return {}
    try:
        data26 = api_get(f"/people/{player_id}/stats", {
            "stats": "season", "group": "hitting",
            "season": SEASON, "sportId": 1,
        })
        data25 = api_get(f"/people/{player_id}/stats", {
            "stats": "season", "group": "hitting",
            "season": SEASON - 1, "sportId": 1,
        })
        s26 = data26.get("stats", [{}])[0].get("splits", [{}])[0].get("stat", {})
        s25 = data25.get("stats", [{}])[0].get("splits", [{}])[0].get("stat", {})
        pa26 = int(s26.get("plateAppearances", 0) or 0)
        pa25 = int(s25.get("plateAppearances", 0) or 0)

        def blend(key, default, s_curr, pa_curr, s_prev, pa_prev):
            v26 = float(s_curr.get(key, default) or default)
            v25 = float(s_prev.get(key, default) or default)
            if pa_curr >= 100 and pa_prev >= 100:
                return round(v26 * 0.65 + v25 * 0.35, 3)
            return v26 if pa_curr >= 50 else v25 if pa_prev >= 50 else default

        return {
            "ops": blend("ops", 0.720, s26, pa26, s25, pa25),
            "avg": blend("avg", 0.250, s26, pa26, s25, pa25),
            "obp": blend("obp", 0.320, s26, pa26, s25, pa25),
            "slg": blend("slg", 0.400, s26, pa26, s25, pa25),
        }
    except Exception:
        return {}


def get_batter_vs_pitcher(batter_id: int, pitcher_id: int) -> dict:
    if not batter_id or not pitcher_id:
        return {}
    try:
        data = api_get(f"/people/{batter_id}/stats", {
            "stats": "vsPlayer", "group": "hitting",
            "season": SEASON, "sportId": 1,
            "opposingPlayerId": pitcher_id,
        })
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            data = api_get(f"/people/{batter_id}/stats", {
                "stats": "vsPlayerTotal", "group": "hitting",
                "sportId": 1, "opposingPlayerId": pitcher_id,
            })
            splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s  = splits[0]["stat"]
        ab = int(s.get("atBats", 0) or 0)
        if ab < 3:
            return {}
        return {
            "ab":  ab,
            "avg": float(s.get("avg", 0) or 0),
            "ops": float(s.get("ops", 0) or 0),
            "hr":  int(s.get("homeRuns", 0) or 0),
            "h":   int(s.get("hits", 0) or 0),
        }
    except Exception:
        return {}


def get_lineup_vs_pitcher_ops(lineup: list, pitcher_id: int, vs_hand: str) -> float:
    if not lineup:
        return 0.720
    weighted_ops = total_weight = 0.0
    for i, batter in enumerate(lineup):
        bvp  = get_batter_vs_pitcher(batter.get("id"), pitcher_id)
        stat = get_batter_stats(batter.get("id"), vs_hand)
        ops  = bvp.get("ops") or stat.get("ops") or 0.720
        w    = 1.2 if i < 3 else (1.0 if i < 6 else 0.8)
        weighted_ops += ops * w
        total_weight += w
    return round(weighted_ops / total_weight, 3) if total_weight > 0 else 0.720


def get_lineup_bvp_highlights(lineup: list, pitcher_id: int, pitcher_name: str) -> str:
    highlights = []
    for batter in lineup:
        m = get_batter_vs_pitcher(batter.get("id"), pitcher_id)
        if m and m.get("ab", 0) >= 5 and m.get("ops", 0) >= 0.900:
            highlights.append(
                f"{batter['name']}: {int(m['avg']*1000):03d} AVG, {m['hr']} HR vs {pitcher_name}"
            )
    return " | ".join(highlights) if highlights else "No significant matchup history"


# ─────────────────────────────────────────────
# PLATOON SPLITS
# ─────────────────────────────────────────────
def get_team_elo(team_id: int) -> float:
    try:
        standings = api_get("/standings", {
            "leagueId": "103,104", "season": SEASON, "sportId": 1,
        })
        wins = losses = 0
        for record in standings.get("records", []):
            for team in record.get("teamRecords", []):
                if team.get("team", {}).get("id") == team_id:
                    wins   = int(team.get("wins", 0))
                    losses = int(team.get("losses", 0))
                    break
        games = wins + losses
        if games == 0:
            return 1500.0
        win_pct = wins / games
        data    = api_get(f"/teams/{team_id}/stats", {
            "stats": "season", "group": "hitting",
            "season": SEASON, "sportId": 1,
        })
        splits = data.get("stats", [{}])[0].get("splits", [])
        rpg    = 0.0
        if splits:
            s   = splits[0]["stat"]
            rpg = float(s.get("runs", 0)) / max(games, 1)
        win_component = (win_pct - 0.500) * 400
        run_component = (rpg - LEAGUE_RPG) * 20
        return round(max(1200.0, min(1800.0, 1500.0 + win_component + run_component)), 1)
    except Exception:
        return 1500.0


def get_team_l10(team_id: int) -> dict:
    try:
        data = api_get("/schedule", {
            "sportId": 1, "teamId": team_id,
            "season": SEASON, "gameType": "R",
        })
        finished = []
        for db in data.get("dates", []):
            for g in db.get("games", []):
                if g.get("status", {}).get("abstractGameState") == "Final":
                    teams  = g.get("teams", {})
                    away   = teams.get("away", {})
                    home   = teams.get("home", {})
                    a_id   = away.get("team", {}).get("id")
                    a_sc   = away.get("score", 0) or 0
                    h_sc   = home.get("score", 0) or 0
                    is_win = (a_id == team_id and a_sc > h_sc) or (a_id != team_id and h_sc > a_sc)
                    finished.append({"win": is_win, "scored": a_sc if a_id == team_id else h_sc,
                                     "allowed": h_sc if a_id == team_id else a_sc})
        last10 = finished[-10:]
        if not last10:
            return {}
        wins   = sum(1 for g in last10 if g["win"])
        losses = len(last10) - wins
        return {
            "l10_wins":    wins,
            "l10_losses":  losses,
            "l10_win_pct": round(wins / len(last10), 3),
        }
    except Exception:
        return {}


def get_team_streak(team_id: int) -> dict:
    try:
        standings = api_get("/standings", {
            "leagueId": "103,104", "season": SEASON, "sportId": 1,
        })
        for record in standings.get("records", []):
            for team in record.get("teamRecords", []):
                if team.get("team", {}).get("id") == team_id:
                    streak_info = team.get("streak", {})
                    streak_type = streak_info.get("streakType", "")
                    streak_num  = int(streak_info.get("streakNumber", 0))
                    streak_val  = streak_num if streak_type == "W" else -streak_num
                    return {"streak": streak_val, "streak_label": f"{'W' if streak_val > 0 else 'L'}{abs(streak_val)}"}
        return {"streak": 0, "streak_label": "—"}
    except Exception:
        return {"streak": 0, "streak_label": "—"}


def get_team_run_differential(team_id: int) -> dict:
    try:
        h_data = api_get(f"/teams/{team_id}/stats", {
            "stats": "season", "group": "hitting",
            "season": SEASON, "sportId": 1,
        })
        p_data = api_get(f"/teams/{team_id}/stats", {
            "stats": "season", "group": "pitching",
            "season": SEASON, "sportId": 1,
        })
        h_splits = h_data.get("stats", [{}])[0].get("splits", [])
        p_splits = p_data.get("stats", [{}])[0].get("splits", [])
        if not h_splits or not p_splits:
            return {}
        hs    = h_splits[0]["stat"]
        ps    = p_splits[0]["stat"]
        games = max(int(hs.get("gamesPlayed", 1)), 1)
        scored  = float(hs.get("runs", 0)) / games
        allowed = float(ps.get("runs", 0)) / games
        return {
            "rdiff_scored":   round(scored, 2),
            "rdiff_allowed":  round(allowed, 2),
            "rdiff_per_game": round(scored - allowed, 3),
        }
    except Exception:
        return {}


def get_h2h_record(away_team_id: int, home_team_id: int) -> dict:
    season_weights = {
        SEASON: 0.30, SEASON-1: 0.25, SEASON-2: 0.20,
        SEASON-3: 0.15, SEASON-4: 0.10,
    }
    wins = losses = games = 0
    weighted_total = total_w = 0.0
    for season, weight in season_weights.items():
        try:
            data = api_get("/schedule", {
                "sportId": 1, "season": season,
                "teamId": away_team_id, "opponentId": home_team_id,
                "gameType": "R",
            })
            sr = sg = 0
            for db in data.get("dates", []):
                for g in db.get("games", []):
                    if g.get("status", {}).get("abstractGameState") != "Final":
                        continue
                    teams  = g.get("teams", {})
                    away   = teams.get("away", {})
                    home_t = teams.get("home", {})
                    a_sc   = away.get("score", 0) or 0
                    h_sc   = home_t.get("score", 0) or 0
                    if away.get("team", {}).get("id") == away_team_id:
                        if a_sc > h_sc: wins += 1
                        else:           losses += 1
                    sr   += a_sc + h_sc
                    sg   += 1
                    games += 1
            if sg > 0:
                weighted_total += (sr / sg) * weight
                total_w        += weight
        except Exception:
            continue
    if games == 0:
        return {}
    return {
        "h2h_wins":      wins,
        "h2h_losses":    losses,
        "h2h_games":     games,
        "h2h_avg_total": round(weighted_total / total_w, 2) if total_w > 0 else 0,
        "h2h_win_pct":   round(wins / games, 3),
    }


def get_team_schedule_fatigue(team_id: int) -> dict:
    """Back-to-back, road trip, heavy schedule — fatigue factors."""
    try:
        start = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        data  = api_get("/schedule", {
            "sportId": 1, "teamId": team_id,
            "startDate": start, "endDate": today_str(), "gameType": "R",
        })
        games_played = []
        for db in data.get("dates", []):
            for g in db.get("games", []):
                if g.get("status", {}).get("abstractGameState") == "Final":
                    home_id = g.get("teams", {}).get("home", {}).get("team", {}).get("id")
                    games_played.append({
                        "date":    db.get("date", ""),
                        "is_home": home_id == team_id,
                    })
        if not games_played:
            return {"fatigue_factor": 1.00, "schedule_label": "Normal", "road_trip_days": 0}
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        played_yesterday = any(g["date"] == yesterday for g in games_played)
        road_trip_days   = 0
        for g in reversed(games_played):
            if not g["is_home"]: road_trip_days += 1
            else:                break
        fatigue_factor = 1.00
        labels         = []
        if played_yesterday:
            fatigue_factor -= 0.03
            labels.append("B2B")
        if road_trip_days >= 7:
            fatigue_factor -= 0.04
            labels.append(f"Long road trip ({road_trip_days}d)")
        elif road_trip_days >= 4:
            fatigue_factor -= 0.02
            labels.append(f"Road trip ({road_trip_days}d)")
        if len(games_played) >= 7:
            fatigue_factor -= 0.02
            labels.append("Heavy schedule")
        return {
            "fatigue_factor":   round(fatigue_factor, 3),
            "schedule_label":   " | ".join(labels) if labels else "Normal",
            "road_trip_days":   road_trip_days,
            "played_yesterday": played_yesterday,
            "games_last_7":     len(games_played),
        }
    except Exception:
        return {"fatigue_factor": 1.00, "schedule_label": "Unknown", "road_trip_days": 0}


def get_series_context(game: dict, away_team_id: int, home_team_id: int) -> dict:
    """
    Which game in the series is this?
    Game 1 = fresh arms → slight under lean.
    Game 3+ = tired arms → slight over lean.
    """
    try:
        data = api_get("/schedule", {
            "sportId": 1, "date": today_str(),
            "teamId": home_team_id, "opponentId": away_team_id,
        })
        series_games = []
        for db in data.get("dates", []):
            for g in db.get("games", []):
                series_games.append(g.get("gamePk"))
        game_pk    = game.get("gamePk")
        series_num = series_games.index(game_pk) + 1 if game_pk in series_games else 1
        return {"series_game_num": series_num}
    except Exception:
        return {"series_game_num": 1}


def get_travel_factor(team_id: int, venue_lon: float) -> dict:
    """
    West coast team playing early ET game = fatigue penalty.
    Simple timezone proxy using venue longitude.
    """
    try:
        # Rough timezone buckets by longitude
        # < -115 = Pacific, -115 to -100 = Mountain, -100 to -85 = Central, > -85 = Eastern
        # We'd need team home city to do this properly — approximation for now
        return {"travel_factor": 1.00, "travel_label": "Normal"}
    except Exception:
        return {"travel_factor": 1.00, "travel_label": "Unknown"}


# Module-level Statcast leaderboard caches (populated on first use)
_statcast_pitcher_cache: list = []
_statcast_team_cache: dict = {}


def _load_statcast_pitcher_leaderboard(season: int) -> list:
    global _statcast_pitcher_cache
    if _statcast_pitcher_cache:
        return _statcast_pitcher_cache
    try:
        r = requests.get(
            "https://baseballsavant.mlb.com/leaderboard/statcast",
            params={"type": "pitcher", "year": season, "position": "", "team": "", "min": 10, "csv": "true"},
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        print(f"  ⚡ Statcast pitcher leaderboard: HTTP {r.status_code} | {len(r.content):,} bytes")
        if r.status_code == 200 and r.content:
            content = r.content.decode("utf-8-sig")
            # Fix header — Savant CSV has "last_name, first_name" as first col which breaks parsing
            lines = content.split("\n")
            if lines and "last_name" in lines[0]:
                lines[0] = lines[0].replace('"last_name, first_name"', 'last_name_first_name', 1)
                lines[0] = lines[0].replace('last_name, first_name', 'last_name_first_name', 1)
                content = "\n".join(lines)
            _statcast_pitcher_cache = list(csv.DictReader(io.StringIO(content)))
            if _statcast_pitcher_cache:
                print(f"  ⚡ Statcast: {len(_statcast_pitcher_cache)} pitchers | cols: {list(_statcast_pitcher_cache[0].keys())[:8]}")
        else:
            print(f"  ⚠️  Statcast pitcher leaderboard failed: {r.text[:100]}")
    except Exception as e:
        print(f"  ⚠️  Statcast pitcher load error: {e}")
    return _statcast_pitcher_cache


def _load_statcast_team_leaderboard(season: int, team_abbrev: str) -> list:
    if team_abbrev in _statcast_team_cache:
        return _statcast_team_cache[team_abbrev]
    try:
        r = requests.get(
            "https://baseballsavant.mlb.com/leaderboard/statcast",
            params={"type": "batter", "year": season, "position": "", "team": team_abbrev, "min": 10, "csv": "true"},
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        if r.status_code == 200 and r.content:
            rows = list(csv.DictReader(io.StringIO(r.content.decode("utf-8-sig"))))
            _statcast_team_cache[team_abbrev] = rows
            return rows
        else:
            print(f"  ⚠️  Statcast team {team_abbrev}: HTTP {r.status_code}")
    except Exception as e:
        print(f"  ⚠️  Statcast team {team_abbrev} error: {e}")
    _statcast_team_cache[team_abbrev] = []
    return []


def get_pitcher_statcast(pitcher_id: int, season: int = None) -> dict:
    season = season or SEASON
    try:
        rows = _load_statcast_pitcher_leaderboard(season)
        if not rows:
            return {}
        # Debug on first call — show sample IDs
        if not hasattr(get_pitcher_statcast, '_debug_done'):
            get_pitcher_statcast._debug_done = True
            sample_ids = [row.get("player_id", "?") for row in rows[:5]]
            print(f"  🔍 Statcast sample IDs: {sample_ids} | looking for: {pitcher_id}")
        for row in rows:
            pid = str(row.get("player_id", ""))
            if pid == str(pitcher_id):
                ev_avg    = float(row.get("avg_hit_speed", 0) or 0)
                ev95      = float(row.get("ev95percent", 0) or 0)
                sweetspot = float(row.get("anglesweetspotpercent", 0) or 0)
                # Quality score: lower EV and EV95 = better pitcher
                quality = round(max(0, min(10,
                    10 - ((ev_avg - 85) / 3) - (ev95 / 10) + (sweetspot - 30) / 10
                )), 1)
                result = {
                    "sv_ev_avg":        round(ev_avg, 1),
                    "sv_ev95":          round(ev95, 1),
                    "sv_sweetspot":     round(sweetspot, 1),
                    "sv_quality_score": quality,
                }
                # Blend in pitch arsenal data (whiff%, xwOBA, K%)
                arsenal = _load_pitch_arsenal_leaderboard(season)
                pitcher_rows = [r for r in arsenal
                                if str(r.get("pitcher_id", "") or r.get("player_id", "") or r.get("pitcher", "")) == str(pitcher_id)]
                if pitcher_rows:
                    whiffs = [float(r.get("whiff_percent", 0) or 0) for r in pitcher_rows if r.get("whiff_percent", "").strip()]
                    xwobas = [float(r.get("xwoba", 0) or 0) for r in pitcher_rows if r.get("xwoba", "").strip()]
                    k_pcts = [float(r.get("k_percent", 0) or 0) for r in pitcher_rows if r.get("k_percent", "").strip()]
                    if whiffs:
                        result["sv_whiff"] = round(sum(whiffs) / len(whiffs), 1)
                        quality = round(max(0, min(10,
                            quality + (result["sv_whiff"] - 25) / 5
                        )), 1)
                        result["sv_quality_score"] = quality
                    if xwobas: result["sv_xwoba"] = round(sum(xwobas) / len(xwobas), 3)
                    if k_pcts: result["sv_k_pct"]  = round(sum(k_pcts) / len(k_pcts), 1)
                return result
        return {}
    except Exception as e:
        print(f"  ⚠️  Statcast pitcher lookup error: {e}")
        return {}


# ─────────────────────────────────────────────
# PITCH ARSENAL LEADERBOARD
# ─────────────────────────────────────────────
_pitch_arsenal_cache: list = []


def _load_pitch_arsenal_leaderboard(season: int) -> list:
    global _pitch_arsenal_cache
    if _pitch_arsenal_cache:
        return _pitch_arsenal_cache
    try:
        r = requests.get(
            "https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats",
            params={
                "type":      "pitcher",
                "pitchType": "",
                "year":      season,
                "team":      "",
                "min":       10,
                "csv":       "true",
            },
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        print(f"  ⚡ Pitch Arsenal: HTTP {r.status_code} | {len(r.content):,} bytes")
        if r.status_code == 200 and r.content:
            _pitch_arsenal_cache = list(csv.DictReader(io.StringIO(r.content.decode("utf-8-sig"))))
            if _pitch_arsenal_cache:
                print(f"  ⚡ Arsenal: {len(_pitch_arsenal_cache)} rows | cols: {list(_pitch_arsenal_cache[0].keys())[:12]}")
        else:
            print(f"  ⚠️  Pitch Arsenal failed: {r.text[:80]}")
    except Exception as e:
        print(f"  ⚠️  Pitch Arsenal load error: {e}")
    return _pitch_arsenal_cache


def get_team_statcast(team_abbrev: str, season: int = None) -> dict:
    season = season or SEASON
    try:
        rows = _load_statcast_team_leaderboard(season, team_abbrev)
        if not rows:
            return {}
        def avg_col(*keys):
            for key in keys:
                vals = [float(row[key]) for row in rows if row.get(key, "").strip()]
                if vals:
                    return round(sum(vals) / len(vals), 3)
            return None
        ev   = avg_col("avg_hit_speed")
        brl  = avg_col("brl_percent")
        ev95 = avg_col("ev95percent")
        result = {}
        if ev:   result["sv_exit_velo"]  = round(ev, 1)
        if brl:  result["sv_barrel_pct"] = round(brl, 1)
        if ev95: result["sv_ev95"]       = round(ev95, 1)
        if ev and brl:
            ev_s   = max(0, min(10, (ev - 85.0) / 1.0))
            brl_s  = max(0, min(10, brl / 1.0))
            ev95_s = max(0, min(10, (ev95 - 30.0) / 3.0)) if ev95 else 5.0
            result["sv_lineup_score"] = round(ev_s * 0.40 + brl_s * 0.40 + ev95_s * 0.20, 1)
        return result
    except Exception as e:
        print(f"  ⚠️  Statcast team {team_abbrev} parse error: {e}")
        return {}
