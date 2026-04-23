import pandas as pd
import os

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

def safe_float(val, scale=1.0):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return round(float(val) * scale, 3)
    except:
        return None

def load_bp_games(filepath="ballparkpal_games.xlsx") -> dict:
    if not os.path.exists(filepath):
        print(f"  ⚠️  BP games file not found")
        return {}
    try:
        df = pd.read_excel(filepath, engine="openpyxl")
    except Exception as e:
        print(f"  ⚠️  Could not read BP games: {e}")
        return {}

    games = {}
    for _, row in df.iterrows():
        try:
            away_abbr = str(row.get("AwayTeam", "")).strip()
            home_abbr = str(row.get("HomeTeam", "")).strip()
            if not away_abbr or not home_abbr:
                continue

            away_full = ABBR_TO_FULLNAME.get(away_abbr, away_abbr)
            home_full = ABBR_TO_FULLNAME.get(home_abbr, home_abbr)

            bp = {
                "bp_away_runs": safe_float(row.get("RunsAway")),
                "bp_home_runs": safe_float(row.get("RunsHome")),
                "bp_f5_away":   safe_float(row.get("RunsFirst5Away")),
                "bp_f5_home":   safe_float(row.get("RunsFirst5Home")),
                "bp_yrfi_pct":  safe_float(row.get("RunsFirstInningPct"), scale=100.0),
            }

            key = f"{away_full}@{home_full}"
            games[key] = bp
            print(f"  📌 Mapped: {away_abbr}→{away_full} vs {home_abbr}→{home_full} | Runs: {bp['bp_away_runs']}/{bp['bp_home_runs']}")

        except Exception as e:
            continue

    print(f"  ✅ BP data ready for {len(games)} games")
    return games


def load_bp_pitchers(filepath="ballparkpal_pitchers.xlsx") -> dict:
    """
    Returns dict keyed by game (away@home) with pitcher stats.
    Side = A (away pitcher) or H (home pitcher)
    """
    if not os.path.exists(filepath):
        print(f"  ⚠️  BP pitchers file not found")
        return {}
    try:
        df = pd.read_excel(filepath, engine="openpyxl")
    except Exception as e:
        print(f"  ⚠️  Could not read BP pitchers: {e}")
        return {}

    pitchers = {}
    for _, row in df.iterrows():
        try:
            team     = str(row.get("Team", "")).strip()
            opponent = str(row.get("Opponent", "")).strip()
            side     = str(row.get("Side", "")).strip()

            if side == "A":
                away_abbr = team
                home_abbr = opponent
            else:
                away_abbr = opponent
                home_abbr = team

            away_full = ABBR_TO_FULLNAME.get(away_abbr, away_abbr)
            home_full = ABBR_TO_FULLNAME.get(home_abbr, home_abbr)
            key = f"{away_full}@{home_full}"

            if key not in pitchers:
                pitchers[key] = {}

            if side == "A":
                pitchers[key]["bp_away_sp_inn"]  = safe_float(row.get("Innings"))
                pitchers[key]["bp_away_sp_runs"]  = safe_float(row.get("RunsAllowed"))
                pitchers[key]["bp_away_sp_k"]     = safe_float(row.get("Strikeouts"))
                pitchers[key]["bp_away_sp_inn"]  = safe_float(row.get("Innings"))
