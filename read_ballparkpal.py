"""
read_ballparkpal.py
Reads the downloaded BallparkPal games XLSX and returns
BP data keyed by team abbreviation for use in mlb_model.py.

BP Export columns we care about:
  AwayTeam, HomeTeam
  RunsAway, RunsHome           → bp_away_runs, bp_home_runs
  RunsFirst5Away, RunsFirst5Home → bp_f5_away, bp_f5_home
  RunsFirstInningPct           → bp_yrfi_pct (as %)
"""

import pandas as pd
import os

# MLB team name → abbreviation mapping
TEAM_NAME_TO_ABBR = {
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",         "Chicago White Sox": "CHW",
    "Cincinnati Reds": "CIN",      "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",     "Detroit Tigers": "DET",
    "Houston Astros": "HOU",       "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",   "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",        "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",      "New York Mets": "NYM",
    "New York Yankees": "NYY",     "Oakland Athletics": "OAK",
    "Athletics": "OAK",            "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",   "San Diego Padres": "SD",
    "San Francisco Giants": "SF",  "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",  "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",        "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WAS",
}

# Reverse: abbr → full name
ABBR_TO_NAME = {v: k for k, v in TEAM_NAME_TO_ABBR.items()}


def load_bp_games(filepath="ballparkpal_games.xlsx") -> dict:
    """
    Load BallparkPal game export and return a dict keyed by
    both team abbreviations so mlb_model.py can look up any game.

    Returns:
        {
          "PHI@CHC": { bp_away_runs, bp_home_runs, bp_f5_away,
                       bp_f5_home, bp_yrfi_pct, ... },
          ...
        }
    """
    if not os.path.exists(filepath):
        print(f"  ⚠️  BallparkPal file not found: {filepath}")
        return {}

    try:
        df = pd.read_excel(filepath, engine="openpyxl")
        print(f"  📊 Loaded BP data: {len(df)} games")
    except Exception as e:
        print(f"  ⚠️  Could not read BP file: {e}")
        return {}

    games = {}
    for _, row in df.iterrows():
        try:
            away_abbr = str(row.get("AwayTeam", "")).strip()
            home_abbr = str(row.get("HomeTeam", "")).strip()

            if not away_abbr or not home_abbr:
                continue

            def safe(col, scale=1.0):
                val = row.get(col)
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                try:
                    return round(float(val) * scale, 3)
                except:
                    return None

            bp = {
                # Core run projections
                "bp_away_runs":    safe("RunsAway"),
                "bp_home_runs":    safe("RunsHome"),
                # F5 projections
                "bp_f5_away":      safe("RunsFirst5Away"),
                "bp_f5_home":      safe("RunsFirst5Home"),
                # YRFI — stored as 0-1 probability, convert to %
                "bp_yrfi_pct":     safe("RunsFirstInningPct", scale=100.0),
                # Win probabilities
                "bp_away_win_pct": safe("AwayWinPct"),
                "bp_home_win_pct": safe("HomeWinPct"),
                # Team abbreviations for reference
                "bp_away_abbr":    away_abbr,
                "bp_home_abbr":    home_abbr,
            }

            # Store under multiple keys so lookups work
            key = f"{away_abbr}@{home_abbr}"
            games[key] = bp

            # Also store by full team names if we know them
            away_name = ABBR_TO_NAME.get(away_abbr, away_abbr)
            home_name = ABBR_TO_NAME.get(home_abbr, home_abbr)
            games[f"{away_name}@{home_name}"] = bp

        except Exception as e:
            continue

    print(f"  ✅ BP data ready for {len(games)//2} games")
    return games


def get_bp_for_game(bp_games: dict, away_team: str, home_team: str) -> dict:
    """
    Look up BP data for a specific game.
    Tries multiple key formats to find a match.
    Returns empty dict if not found.
    """
    if not bp_games:
        return {}

    # Normalize team names
    away = away_team.strip()
    home = home_team.strip()

    # Try direct name match
    key = f"{away}@{home}"
    if key in bp_games:
        return bp_games[key]

    # Try abbreviation match
    away_abbr = TEAM_NAME_TO_ABBR.get(away, away[:3].upper())
    home_abbr = TEAM_NAME_TO_ABBR.get(home, home[:3].upper())
    key_abbr  = f"{away_abbr}@{home_abbr}"
    if key_abbr in bp_games:
        return bp_games[key_abbr]

    # Try partial name match
    for k, v in bp_games.items():
        parts = k.split("@")
        if len(parts) == 2:
            if (away.lower() in parts[0].lower() or parts[0].lower() in away.lower()) and \
               (home.lower() in parts[1].lower() or parts[1].lower() in home.lower()):
                return v

    return {}


if __name__ == "__main__":
    # Test it
    games = load_bp_games()
    for key, data in list(games.items())[:3]:
        print(f"\n  {key}:")
        for k, v in data.items():
            print(f"    {k}: {v}")
