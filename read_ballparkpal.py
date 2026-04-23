import pandas as pd
import os

# BP abbreviation → MLB Stats API full team name
ABBR_TO_FULLNAME = {
    "ARI": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CHW": "Chicago White Sox",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC":  "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
    "OAK": "Oakland Athletics",
    "ATH": "Oakland Athletics",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD":  "San Diego Padres",
    "SF":  "San Francisco Giants",
    "SEA": "Seattle Mariners",
    "STL": "St. Louis Cardinals",
    "TB":  "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WAS": "Washington Nationals",
}

def load_bp_games(filepath="ballparkpal_games.xlsx") -> dict:
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
                "bp_away_runs": safe("RunsAway"),
                "bp_home_runs": safe("RunsHome"),
                "bp_f5_away":   safe("RunsFirst5Away"),
                "bp_f5_home":   safe("RunsFirst5Home"),
                "bp_yrfi_pct":  safe("RunsFirstInningPct", scale=100.0),
            }

            # Store by full team names (what mlb_model.py uses)
            away_full = ABBR_TO_FULLNAME.get(away_abbr, away_abbr)
            home_full = ABBR_TO_FULLNAME.get(home_abbr, home_abbr)

            key = f"{away_full}@{home_full}"
            games[key] = bp
            print(f"  📌 Mapped: {away_abbr}→{away_full} vs {home_abbr}→{home_full} | Runs: {bp['bp_away_runs']}/{bp['bp_home_runs']}")

        except Exception as e:
            print(f"  ⚠️  Row error: {e}")
            continue

    print(f"  ✅ BP data ready for {len(games)} games")
    return games


def get_bp_for_game(bp_games: dict, away_team: str, home_team: str) -> dict:
    if not bp_games:
        return {}

    key = f"{away_team}@{home_team}"
    if key in bp_games:
        print(f"  ✅ BP match found: {key}")
        return bp_games[key]

    # Fuzzy match
    for k, v in bp_games.items():
        parts = k.split("@")
        if len(parts) == 2:
            if away_team.lower() in parts[0].lower() or parts[0].lower() in away_team.lower():
                if home_team.lower() in parts[1].lower() or parts[1].lower() in home_team.lower():
                    print(f"  ✅ BP fuzzy match: {k}")
                    return v

    print(f"  ⚠️  No BP match for: {away_team} @ {home_team}")
    return {}
