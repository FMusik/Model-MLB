"""
adjustments/fatigue.py
======================
Context adjustments that modify run projections:
  - Schedule fatigue (B2B, road trips, heavy schedule)
  - Series context (game 1 fresh arms vs game 3+ tired arms)
  - Travel / timezone crossings
  - Platoon advantage (lineup handedness vs pitcher)

All data is pre-fetched by mlb_api.py and passed in as dicts.
No API calls made here — pure calculation only.
"""

import datetime


# ─────────────────────────────────────────────
# SCHEDULE FATIGUE
# ─────────────────────────────────────────────
def calc_schedule_fatigue(schedule_data: dict) -> dict:
    """
    Takes the output of mlb_api.get_team_schedule_fatigue()
    and returns a clean fatigue factor + label.

    Factors:
      B2B:           -0.03
      Road trip 4d+: -0.02
      Road trip 7d+: -0.04
      Heavy schedule: -0.02 (7+ games in last 7 days)

    Returns factor between 0.91 and 1.00.
    """
    if not schedule_data:
        return {"fatigue_factor": 1.00, "schedule_label": "Normal"}

    # Already calculated in mlb_api — pass through cleanly
    return {
        "fatigue_factor":   schedule_data.get("fatigue_factor", 1.00),
        "schedule_label":   schedule_data.get("schedule_label", "Normal"),
        "road_trip_days":   schedule_data.get("road_trip_days", 0),
        "played_yesterday": schedule_data.get("played_yesterday", False),
        "games_last_7":     schedule_data.get("games_last_7", 0),
    }


def combine_fatigue_factors(
    schedule_factor: float,
    travel_factor:   float,
) -> float:
    """
    Combine schedule fatigue and travel fatigue into one multiplier.
    Multiplicative — each penalty stacks but we floor at 0.88.
    """
    combined = schedule_factor * travel_factor
    return round(max(0.88, combined), 3)


# ─────────────────────────────────────────────
# TRAVEL / TIMEZONE
# ─────────────────────────────────────────────
TEAM_TIMEZONES = {
    "New York Yankees":      "ET", "New York Mets":        "ET",
    "Boston Red Sox":        "ET", "Baltimore Orioles":    "ET",
    "Tampa Bay Rays":        "ET", "Toronto Blue Jays":    "ET",
    "Philadelphia Phillies": "ET", "Atlanta Braves":       "ET",
    "Washington Nationals":  "ET", "Miami Marlins":        "ET",
    "Pittsburgh Pirates":    "ET", "Cincinnati Reds":      "ET",
    "Cleveland Guardians":   "ET", "Detroit Tigers":       "ET",
    "Chicago Cubs":          "CT", "Chicago White Sox":    "CT",
    "Milwaukee Brewers":     "CT", "St. Louis Cardinals":  "CT",
    "Minnesota Twins":       "CT", "Kansas City Royals":   "CT",
    "Houston Astros":        "CT", "Texas Rangers":        "CT",
    "Colorado Rockies":      "MT", "Arizona Diamondbacks": "MT",
    "Los Angeles Dodgers":   "PT", "Los Angeles Angels":   "PT",
    "San Francisco Giants":  "PT", "San Diego Padres":     "PT",
    "Seattle Mariners":      "PT", "Athletics":            "PT",
}

TZ_HOURS = {"ET": 0, "CT": 1, "MT": 2, "PT": 3}


def get_travel_factor(
    away_team:     str,
    venue:         str,
    game_time_str: str = "",
) -> dict:
    """
    Timezone crossings = fatigue for away team.
    3+ zones + early game = 0.94x
    3+ zones             = 0.97x
    2 zones              = 0.98x
    1 zone               = 0.99x
    Same                 = 1.00x
    """
    try:
        away_tz_label = TEAM_TIMEZONES.get(away_team, "CT")
        away_tz       = TZ_HOURS.get(away_tz_label, 1)

        v = venue.lower()
        if any(c in v for c in [
            "new york", "boston", "boston", "baltimore", "tampa", "toronto",
            "philadelphia", "atlanta", "washington", "miami",
            "pittsburgh", "cincinnati", "cleveland", "detroit",
            "progressive", "pnc", "great american", "nationals",
            "camden", "citizens", "truist", "tropicana", "rogers",
        ]):
            venue_tz = 0  # ET
        elif any(c in v for c in [
            "chicago", "milwaukee", "st. louis", "busch", "minnesota",
            "kansas city", "kauffman", "houston", "daikin", "minute maid",
            "dallas", "arlington", "globe life", "wrigley", "guaranteed",
            "american family", "target",
        ]):
            venue_tz = 1  # CT
        elif any(c in v for c in ["denver", "coors", "phoenix", "chase"]):
            venue_tz = 2  # MT
        elif any(c in v for c in [
            "los angeles", "dodger", "angel", "san francisco", "oracle",
            "san diego", "petco", "seattle", "t-mobile", "oakland",
            "sacramento", "uniqlo", "loandepot",
        ]):
            venue_tz = 3  # PT
        else:
            venue_tz = 1  # default CT

        tz_diff = abs(away_tz - venue_tz)

        # Determine if it's an early local game
        early_game = False
        if game_time_str:
            try:
                game_dt    = datetime.datetime.fromisoformat(
                    game_time_str.replace("Z", "+00:00")
                )
                # Convert UTC to local venue time
                local_hour = (game_dt.hour - venue_tz) % 24
                early_game = local_hour < 13  # before 1pm local
            except Exception:
                pass

        if   tz_diff >= 3 and early_game: factor, label = 0.94, f"⚠️ Big TZ ({tz_diff}hr) + early"
        elif tz_diff >= 3:                factor, label = 0.97, f"Big TZ change ({tz_diff}hr)"
        elif tz_diff >= 2:                factor, label = 0.98, f"Moderate TZ ({tz_diff}hr)"
        elif tz_diff >= 1:                factor, label = 0.99, f"Minor TZ ({tz_diff}hr)"
        else:                             factor, label = 1.00, "Same timezone"

        return {
            "travel_factor": factor,
            "travel_label":  label,
            "tz_diff":       tz_diff,
            "early_game":    early_game,
        }

    except Exception:
        return {"travel_factor": 1.00, "travel_label": "Unknown", "tz_diff": 0, "early_game": False}


# ─────────────────────────────────────────────
# SERIES CONTEXT
# ─────────────────────────────────────────────
def calc_series_factor(series_data: dict) -> dict:
    """
    Takes output of mlb_api.get_series_context() and returns
    a run projection multiplier.

    Game 1 = fresh bullpens → slight under lean (0.97x)
    Game 2 = neutral        → 1.00x
    Game 3+ = tired bullpens → slight over lean (1.03x)
    """
    if not series_data:
        return {"series_run_factor": 1.00, "series_label": "Unknown", "series_game_num": 1}

    game_num = series_data.get("series_game_num", 1)

    if   game_num == 1: factor, label = 0.97, "Game 1 (fresh arms → under lean)"
    elif game_num == 2: factor, label = 1.00, "Game 2 (neutral)"
    else:               factor, label = 1.03, f"Game {game_num} (tired arms → over lean)"

    return {
        "series_game_num":  game_num,
        "series_run_factor": factor,
        "series_label":     label,
    }


# ─────────────────────────────────────────────
# PLATOON ADVANTAGE
# ─────────────────────────────────────────────
def get_platoon_factor(pitcher_hand: str, lineup: list = None) -> dict:
    """
    Platoon advantage of lineup vs pitcher handedness.
    R batters vs L pitcher = +advantage (more runs expected)
    L batters vs R pitcher = slight advantage
    Same hand = disadvantage

    Returns factor applied to run projection for the offense.
    platoon_factor > 1.0 = offense has platoon edge.
    """
    if not pitcher_hand:
        return {"platoon_factor": 1.00, "platoon_label": "Unknown"}

    # League average platoon OPS splits
    # R batter vs L pitcher: +.025 OPS advantage
    # L batter vs R pitcher: +.015 OPS advantage
    # Same hand: slight disadvantage
    adv_map = {
        "RL": +0.025,  # R batter vs L pitcher — biggest platoon split
        "LR": +0.015,  # L batter vs R pitcher
        "RR": -0.010,  # R batter vs R pitcher — slight disadvantage
        "LL": -0.015,  # L batter vs L pitcher — biggest same-hand penalty
    }

    # If we have a lineup, estimate dominant handedness
    # Most MLB lineups are predominantly right-handed (~70%)
    lineup_hand = "R"  # default assumption
    if lineup and len(lineup) >= 5:
        # Could be extended to check individual batter hands via API
        # For now, use R as default — R-heavy lineups are the norm
        lineup_hand = "R"

    key     = f"{lineup_hand}{pitcher_hand}"
    ops_adj = adv_map.get(key, 0.0)

    # Convert OPS adjustment to a run factor
    # +0.025 OPS ≈ +3.5% more runs (rough empirical)
    factor = round(1.0 + (ops_adj / 0.720) * 0.10, 3)
    label  = f"Platoon {'▲' if factor > 1.0 else '▼'} ({ops_adj:+.3f} OPS, {pitcher_hand}HP)"

    return {"platoon_factor": factor, "platoon_label": label}
