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


def safe_val(val, scale=1.0):
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        return round(float(val) * scale, 3)
    except Exception:
        return None


def make_key(away_abbr, home_abbr):
    away_full = ABBR_TO_FULLNAME.get(away_abbr, away_abbr)
    home_full = ABBR_TO_FULLNAME.get(home_abbr, home_abbr)
    return f"{away_full}@{home_full}", away_full, home_full


def load_bp_games(filepath="ballparkpal_games.xlsx"):
    if not os.path.exists(filepath):
        print("  WARNING: BP games file not found")
        return {}
    df = pd.read_excel(filepath, engine="openpyxl")
    games = {}
    for _, row in df.iterrows():
        away_abbr = str(row.get("AwayTeam", "")).strip()
        home_abbr = str(row.get("HomeTeam", "")).strip()
        if not away_abbr or not home_abbr:
            continue
        key, away_full, home_full = make_key(away_abbr, home_abbr)
        games[key] = {
            "bp_away_runs": safe_val(row.get("RunsAway")),
            "bp_home_runs": safe_val(row.get("RunsHome")),
            "bp_f5_away":   safe_val(row.get("RunsFirst5Away")),
            "bp_f5_home":   safe_val(row.get("RunsFirst5Home")),
            "bp_yrfi_pct":  safe_val(row.get("RunsFirstInningPct"), scale=100.0),
        }
        print(f"  Mapped: {away_abbr} vs {home_abbr} | Runs: {games[key]['bp_away_runs']}/{games[key]['bp_home_runs']}")
    print(f"  BP games ready: {len(games)}")
    return games


def load_bp_pitchers(filepath="ballparkpal_pitchers.xlsx"):
    if not os.path.exists(filepath):
        print("  WARNING: BP pitchers file not found")
        return {}
    df = pd.read_excel(filepath, engine="openpyxl")
    pitchers = {}
    for _, row in df.iterrows():
        team     = str(row.get("Team", "")).strip()
        opponent = str(row.get("Opponent", "")).strip()
        side     = str(row.get("Side", "")).strip()
        if side == "A":
            away_abbr = team
            home_abbr = opponent
        else:
            away_abbr = opponent
            home_abbr = team
        key, away_full, home_full = make_key(away_abbr, home_abbr)
        if key not in pitchers:
            pitchers[key] = {}
        if side == "A":
            pitchers[key]["bp_away_sp_inn"]  = safe_val(row.get("Innings"))
            pitchers[key]["bp_away_sp_runs"] = safe_val(row.get("RunsAllowed"))
            pitchers[key]["bp_away_sp_k"]    = safe_val(row.get("Strikeouts"))
            pitchers[key]["bp_away_sp_bb"]   = safe_val(row.get("Walks"))
        else:
            pitchers[key]["bp_home_sp_inn"]  = safe_val(row.get("Innings"))
            pitchers[key]["bp_home_sp_runs"] = safe_val(row.get("RunsAllowed"))
            pitchers[key]["bp_home_sp_k"]    = safe_val(row.get("Strikeouts"))
            pitchers[key]["bp_home_sp_bb"]   = safe_val(row.get("Walks"))
    print(f"  BP pitchers ready: {len(pitchers)}")
    return pitchers


def load_bp_teams(filepath="ballparkpal_teams.xlsx"):
    if not os.path.exists(filepath):
        print("  WARNING: BP teams file not found")
        return {}
    df = pd.read_excel(filepath, engine="openpyxl")
    teams = {}
    for _, row in df.iterrows():
        team     = str(row.get("Team", "")).strip()
        opponent = str(row.get("Opponent", "")).strip()
        side     = str(row.get("Side", "")).strip()
        if side == "A":
            away_abbr = team
            home_abbr = opponent
        else:
            away_abbr = opponent
            home_abbr = team
        key, away_full, home_full = make_key(away_abbr, home_abbr)
        if key not in teams:
            teams[key] = {}
        if side == "A":
            teams[key]["bp_away_rpg"]  = safe_val(row.get("Runs"))
            teams[key]["bp_away_hrpg"] = safe_val(row.get("HomeRuns"))
        else:
            teams[key]["bp_home_rpg"]  = safe_val(row.get("Runs"))
            teams[key]["bp_home_hrpg"] = safe_val(row.get("HomeRuns"))
    print(f"  BP teams ready: {len(teams)}")
    return teams


def get_bp_for_game(bp_dict, away_team, home_team):
    if not bp_dict:
        return {}
    key = f"{away_team}@{home_team}"
    if key in bp_dict:
        return bp_dict[key]
    for k, v in bp_dict.items():
        parts = k.split("@")
        if len(parts) == 2:
            a_match = away_team.lower() in parts[0].lower() or parts[0].lower() in away_team.lower()
            h_match = home_team.lower() in parts[1].lower() or parts[1].lower() in home_team.lower()
            if a_match and h_match:
                return v
    return {}
