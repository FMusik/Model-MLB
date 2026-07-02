"""
models/projections.py
=====================
Run projections, park factors, weather factors, win probability,
win probability, and Monte Carlo simulation.

Your model owns all of this.
"""

import math
import random

from config import (
    LEAGUE_RPG, LEAGUE_ERA, BULLPEN_ERA,
    MAX_WIN_PROB,
    MC_SIMULATIONS,
)

# ─────────────────────────────────────────────
# PARK FACTORS
# ─────────────────────────────────────────────
_PARK_FACTORS = {
    "Coors Field":                  {"basic": 1.20, "hr": 1.28},
    "Great American Ball Park":     {"basic": 1.14, "hr": 1.28},
    "Fenway Park":                  {"basic": 1.11, "hr": 1.08},
    "Globe Life Field":             {"basic": 1.09, "hr": 1.14},
    "Yankee Stadium":               {"basic": 1.08, "hr": 1.22},
    "Oriole Park at Camden Yards":  {"basic": 1.07, "hr": 1.12},
    "Camden Yards":                 {"basic": 1.07, "hr": 1.12},
    "Citizens Bank Park":           {"basic": 1.06, "hr": 1.14},
    "Wrigley Field":                {"basic": 1.05, "hr": 1.06},
    "Truist Park":                  {"basic": 1.05, "hr": 1.07},
    "American Family Field":        {"basic": 1.04, "hr": 1.05},
    "Kauffman Stadium":             {"basic": 1.04, "hr": 1.10},
    "Progressive Field":            {"basic": 1.02, "hr": 0.99},
    "Nationals Park":               {"basic": 1.02, "hr": 1.03},
    "Target Field":                 {"basic": 1.01, "hr": 0.97},
    "Rogers Centre":                {"basic": 1.01, "hr": 1.03},
    "Angel Stadium":                {"basic": 1.00, "hr": 1.01},
    "Comerica Park":                {"basic": 1.00, "hr": 0.94},
    "PNC Park":                     {"basic": 0.99, "hr": 0.97},
    "Busch Stadium":                {"basic": 0.98, "hr": 0.95},
    "Guaranteed Rate Field":        {"basic": 0.97, "hr": 0.96},
    "Rate Field":                   {"basic": 0.97, "hr": 0.96},
    "Daikin Park":                  {"basic": 0.97, "hr": 0.94},
    "Minute Maid Park":             {"basic": 0.97, "hr": 0.94},
    "loanDepot park":               {"basic": 0.96, "hr": 0.91},
    "LoanDepot Park":               {"basic": 0.96, "hr": 0.91},
    "Dodger Stadium":               {"basic": 0.96, "hr": 0.93},
    "UNIQLO Field":                 {"basic": 0.96, "hr": 0.93},
    "Chase Field":                  {"basic": 0.95, "hr": 0.97},
    "Citi Field":                   {"basic": 0.95, "hr": 0.89},
    "T-Mobile Park":                {"basic": 0.95, "hr": 0.88},
    "Oracle Park":                  {"basic": 0.93, "hr": 0.82},
    "Petco Park":                   {"basic": 0.94, "hr": 0.86},
    "Tropicana Field":              {"basic": 0.97, "hr": 0.95},
}

HOME_FIELD_ADVANTAGE = 0.020


def get_park_factor(venue: str) -> float:
    return _PARK_FACTORS.get(venue, {}).get("basic", 1.00)


def get_weather_factor(temp_str: str, wind_str: str, condition: str = "") -> float:
    """
    Weather run factor — combines temperature, wind, and conditions.

    Temperature physics: warmer air = lower density = ball carries further.
    Each ~10°F above 70° adds ~1-2 feet of carry on fly balls.

    Wind physics: out = ball carries, in = suppresses, crosswind = neutral.
    Speed matters — 15+ mph blowing out is a major over signal.

    Conditions: rain/dome suppress scoring.
    """
    factor = 1.00

    # ── TEMPERATURE ──────────────────────────────────────────
    # Research: ~0.4% per run change per 10°F above/below 70°
    # Hot day (90°+) at Coors adds ~1.5 runs vs cold day (40°)
    try:
        temp = int(str(temp_str).replace("°", "").strip())
        if   temp >= 95: factor += 0.07   # extreme heat — ball really flies
        elif temp >= 88: factor += 0.05   # hot
        elif temp >= 80: factor += 0.03   # warm
        elif temp >= 72: factor += 0.01   # mild-warm, slight boost
        elif temp >= 62: factor += 0.00   # neutral zone
        elif temp >= 52: factor -= 0.02   # cool
        elif temp >= 42: factor -= 0.04   # cold
        else:            factor -= 0.06   # very cold (<42°) — ball dies
    except Exception:
        pass

    # ── WIND ─────────────────────────────────────────────────
    # Blowing out: strong boost (15+ mph = HR magnet)
    # Blowing in: suppresses fly balls significantly
    # Crosswind (L/R): slight suppression (unpredictable for hitters)
    try:
        wind = str(wind_str).lower()
        if "out" in wind:
            try:
                speed = int("".join(filter(str.isdigit, wind.split("mph")[0][-3:])))
                if   speed >= 20: factor += 0.08   # gale out — Over lean
                elif speed >= 15: factor += 0.06
                elif speed >= 10: factor += 0.04
                elif speed >= 5:  factor += 0.02
                else:             factor += 0.01
            except Exception:
                factor += 0.03
        elif "in" in wind:
            try:
                speed = int("".join(filter(str.isdigit, wind.split("mph")[0][-3:])))
                if   speed >= 20: factor -= 0.08   # gale in — Under lean
                elif speed >= 15: factor -= 0.06
                elif speed >= 10: factor -= 0.04
                elif speed >= 5:  factor -= 0.02
                else:             factor -= 0.01
            except Exception:
                factor -= 0.03
        elif any(d in wind for d in ["left", "right", "lf", "rf", "cross"]):
            # Crosswind — slight neutral-to-suppress effect
            try:
                speed = int("".join(filter(str.isdigit, wind.split("mph")[0][-3:])))
                factor -= min(speed * 0.001, 0.02)
            except Exception:
                pass
    except Exception:
        pass

    # ── CONDITIONS ───────────────────────────────────────────
    # Rain suppresses offense (pitchers control better, ball heavier)
    # Dome = fully neutral (override everything else)
    try:
        cond = str(condition).lower()
        if any(c in cond for c in ["dome", "retractable", "indoor", "roof closed"]):
            factor = 1.00  # dome neutralizes all weather effects
        elif any(c in cond for c in ["rain", "shower", "drizzle", "storm", "thunder"]):
            factor -= 0.04  # rain suppresses scoring
        elif "humid" in cond or "fog" in cond:
            factor -= 0.01
    except Exception:
        pass

    # Cap to avoid extreme swings
    return round(max(0.88, min(factor, 1.15)), 3)


# ─────────────────────────────────────────────
# RUN PROJECTION — SINGLE SIDE
# ─────────────────────────────────────────────
def effective_avg_ip(pitcher: dict, pitcher_form: dict) -> float:
    """
    Starter's expected innings, best source first:
      1) recent per-start average   2) season IP/GS   3) 5.5 default
    Clamped 3.5-7.0 so openers/horses aren't mis-scaled.
    """
    ip = None
    if pitcher_form and pitcher_form.get("recent_avg_ip"):
        ip = pitcher_form["recent_avg_ip"]
    elif pitcher and pitcher.get("gs") and pitcher.get("ip") and pitcher["gs"] > 0:
        ip = pitcher["ip"] / pitcher["gs"]
    if not ip or ip <= 0:
        ip = 5.5
    return round(max(3.5, min(ip, 7.0)), 2)


def bullpen_regression_factor(bp: dict) -> float:
    """
    Peripheral (FIP-style) bullpen regression from K/9 & BB/9 we already fetch.
    Good ERA + weak K-BB -> >1 (more relief runs); bad ERA + strong K-BB -> <1.
    Returns multiplier in [0.90, 1.12]; 1.0 if data missing.
    """
    if not bp:
        return 1.0
    era = bp.get("bullpen_era"); k9 = bp.get("bullpen_k9"); bb9 = bp.get("bullpen_bb9")
    if not era or era <= 0 or k9 is None or bb9 is None:
        return 1.0
    kbb9 = k9 - bb9
    xera = 6.20 - 0.37 * kbb9
    xera = max(2.50, min(xera, 6.50))
    factor = (era * 0.5 + xera * 0.5) / era
    return round(max(0.90, min(factor, 1.12)), 3)


def project_runs_allowed(
    pitcher:          dict,
    opp_offense:      dict,
    park_factor:      float,
    weather_factor:   float,
    lineup_ops:       float = None,
    recent_offense:   dict  = None,
    location_splits:  dict  = None,
    h2h:              dict  = None,
    pitcher_form:     dict  = None,
    bp_avail:         dict  = None,
    # Adjustment factors (all default to 1.0 = neutral)
    ump_run_factor:   float = 1.00,
    pitcher_rest_factor: float = 1.00,
    off_fatigue_factor:  float = 1.00,
    platoon_factor:      float = 1.00,
    bp_rolling_factor:   float = 1.00,
    series_factor:       float = 1.00,
    savant_matchup:      dict  = None,  # pitcher vs this roster: k_pct, woba, xwoba
) -> float:
    if not pitcher:
        return LEAGUE_RPG

    # ── Pitcher quality ───────────────────────────────────────
    fip = pitcher.get("fip") or pitcher.get("era") or LEAGUE_ERA
    era = pitcher.get("era") or fip
    ip  = pitcher.get("ip", 0) or 0

    # Regress toward league mean based on sample size
    # < 30 IP = 60% regression (rookie/small sample)
    # 30-60 IP = 40% regression
    # 60-100 IP = 25% regression
    # 100+ IP  = 15% regression (reliable sample)
    if   ip < 30:  regression = 0.60
    elif ip < 60:  regression = 0.40
    elif ip < 100: regression = 0.25
    else:          regression = 0.15

    fip = fip * (1 - regression) + LEAGUE_ERA * regression
    era = era * (1 - regression) + LEAGUE_ERA * regression

    if pitcher_form and pitcher_form.get("recent_era"):
        recent_era = pitcher_form["recent_era"]
        recent_era = min(recent_era, 7.00)
        recent_era = recent_era * (1 - regression) + LEAGUE_ERA * regression
        fip    = min(fip, 6.00)
        era    = min(era, 6.00)
        fip    = recent_era * 0.50 + fip * 0.50
        era    = recent_era * 0.50 + era * 0.50
        avg_ip = effective_avg_ip(pitcher, pitcher_form)
    else:
        fip    = min(fip, 6.00)
        era    = min(era, 6.00)
        avg_ip = effective_avg_ip(pitcher, pitcher_form)

    # Short rest / rust → pitcher performs worse → more runs allowed
    rest_adj = 2.0 - pitcher_rest_factor  # 0.92 rest → 1.08 adj
    fip      = fip * rest_adj

    base_ra9   = (fip * 0.60) + (era * 0.40)
    proj_runs  = (base_ra9 / 9) * avg_ip

    # ── Opponent offense ──────────────────────────────────────
    season_rpg = opp_offense.get("runs_per_game", LEAGUE_RPG)
    recent_rpg = recent_offense.get("recent_rpg", season_rpg) if recent_offense else season_rpg

    loc_key = list(location_splits.keys())[0] if location_splits else None
    loc_rpg = location_splits.get(loc_key, season_rpg) if location_splits and loc_key else season_rpg

    blended_rpg = (season_rpg * 0.55) + (recent_rpg * 0.25) + (loc_rpg * 0.20)
    off_factor  = blended_rpg / LEAGUE_RPG

    ops_factor  = (lineup_ops / 0.720) if lineup_ops and lineup_ops > 0 \
                  else opp_offense.get("ops", 0.720) / 0.720

    # H2H adjustment
    h2h_factor = 1.00
    if h2h and h2h.get("h2h_games", 0) >= 3:
        h2h_factor = max(0.85, min(round(h2h.get("h2h_avg_total", 9.0) / 9.0, 3), 1.15))

    # Bullpen yesterday availability
    bp_factor = 1.00
    if bp_avail:
        score = bp_avail.get("fatigue_score", 0)
        if score >= 0.30:  bp_factor = 1.08
        elif score >= 0.15: bp_factor = 1.04

    # ── Apply all factors ─────────────────────────────────────
    proj_runs = proj_runs * off_factor * ops_factor * h2h_factor
    proj_runs = proj_runs * park_factor * weather_factor
    proj_runs = proj_runs * ump_run_factor
    proj_runs = proj_runs * platoon_factor
    proj_runs = proj_runs * off_fatigue_factor
    proj_runs = proj_runs * series_factor

    # Bullpen innings contribution
    bullpen_innings = max(0.0, 9.0 - avg_ip)
    if bullpen_innings > 0:
        bp_combined = bp_factor * (1.0 + (bp_rolling_factor - 1.0) * 0.5)
        bp_extra    = (BULLPEN_ERA / 9) * bullpen_innings * (bp_combined - 1.0)
        proj_runs  += bp_extra

    # ── Savant matchup factor ─────────────────────────────────
    # Use pitcher's K% and wOBA vs this specific roster to adjust
    if savant_matchup and (savant_matchup.get("sv_vs_pa") or 0) >= 20:
        sv_woba  = savant_matchup.get("sv_vs_woba")
        sv_xwoba = savant_matchup.get("sv_vs_xwoba")
        sv_k_pct = savant_matchup.get("sv_vs_k_pct")
        lg_woba  = 0.315  # MLB average wOBA

        matchup_factor = 1.00
        # wOBA vs this roster — most reliable signal
        if sv_woba and sv_woba > 0:
            # Each 0.030 wOBA above/below league avg = ~3% run adjustment
            matchup_factor *= 1.0 + ((sv_woba - lg_woba) / lg_woba) * 0.15
        elif sv_xwoba and sv_xwoba > 0:
            # Fall back to xwOBA if wOBA not available
            matchup_factor *= 1.0 + ((sv_xwoba - lg_woba) / lg_woba) * 0.12
        # K% vs this roster — high K% means fewer runs
        if sv_k_pct:
            lg_k_pct = 22.5
            matchup_factor *= 1.0 - ((sv_k_pct - lg_k_pct) / lg_k_pct) * 0.10
        # Cap matchup factor to avoid extreme swings
        matchup_factor = max(0.80, min(1.20, matchup_factor))
        proj_runs *= matchup_factor

    # Hard cap per side — no team should project more than 8 runs
    proj_runs = round(min(proj_runs, 8.0), 2)

    return round(proj_runs, 2)


def project_bullpen_runs(bullpen: dict, innings_remaining: float, park_factor: float) -> float:
    era = bullpen.get("bullpen_era", BULLPEN_ERA) if bullpen else BULLPEN_ERA
    reg = bullpen_regression_factor(bullpen)
    return round((era / 9) * innings_remaining * park_factor * reg, 2)


# ─────────────────────────────────────────────
# RUN PROJECTION — FULL GAME
# ─────────────────────────────────────────────
def project_total_runs(
    away_pitcher: dict, home_pitcher: dict,
    away_offense: dict, home_offense: dict,
    away_bullpen: dict, home_bullpen: dict,
    park_factor:  float, weather_factor: float,
    away_lineup_ops: float = None, home_lineup_ops: float = None,
    away_recent: dict = None, home_recent: dict = None,
    away_location: dict = None, home_location: dict = None,
    h2h: dict = None,
    away_pitcher_form: dict = None, home_pitcher_form: dict = None,
    away_bp_avail: dict = None, home_bp_avail: dict = None,
    ump_run_factor: float = 1.00,
    away_rest_factor: float = 1.00, home_rest_factor: float = 1.00,
    away_fatigue_factor: float = 1.00, home_fatigue_factor: float = 1.00,
    away_platoon_factor: float = 1.00, home_platoon_factor: float = 1.00,
    away_bp_rolling_factor: float = 1.00, home_bp_rolling_factor: float = 1.00,
    series_factor: float = 1.00,
    away_pitcher_regression: float = 1.00,
    home_pitcher_regression: float = 1.00,
    away_savant_matchup: dict = None,  # home pitcher vs away roster
    home_savant_matchup: dict = None,  # away pitcher vs home roster
) -> dict:
    """
    Full game run projection for both sides.
    Returns away/home/total run projections.
    """
    away_starter_runs = project_runs_allowed(
        home_pitcher, away_offense, park_factor, weather_factor,
        lineup_ops=away_lineup_ops,
        recent_offense=away_recent,
        location_splits=away_location,
        h2h=h2h,
        pitcher_form=home_pitcher_form,
        bp_avail=away_bp_avail,
        ump_run_factor=ump_run_factor,
        pitcher_rest_factor=home_rest_factor,
        off_fatigue_factor=away_fatigue_factor,
        platoon_factor=away_platoon_factor,
        bp_rolling_factor=away_bp_rolling_factor,
        series_factor=series_factor,
        savant_matchup=home_savant_matchup,  # home pitcher vs away roster
    )
    away_starter_runs = round(away_starter_runs * home_pitcher_regression, 2)

    home_starter_runs = project_runs_allowed(
        away_pitcher, home_offense, park_factor * (1 + HOME_FIELD_ADVANTAGE), weather_factor,
        lineup_ops=home_lineup_ops,
        recent_offense=home_recent,
        location_splits=home_location,
        h2h=h2h,
        pitcher_form=away_pitcher_form,
        bp_avail=home_bp_avail,
        ump_run_factor=ump_run_factor,
        pitcher_rest_factor=away_rest_factor,
        off_fatigue_factor=home_fatigue_factor,
        platoon_factor=home_platoon_factor,
        bp_rolling_factor=home_bp_rolling_factor,
        series_factor=series_factor,
        savant_matchup=away_savant_matchup,  # away pitcher vs home roster
    )
    home_starter_runs = round(home_starter_runs * away_pitcher_regression, 2)

    away_avg_ip = effective_avg_ip(away_pitcher, away_pitcher_form)
    home_avg_ip = effective_avg_ip(home_pitcher, home_pitcher_form)

    away_bp_runs = project_bullpen_runs(home_bullpen, 9.0 - home_avg_ip, park_factor)
    home_bp_runs = project_bullpen_runs(away_bullpen, 9.0 - away_avg_ip, park_factor)

    away_total = round(away_starter_runs + away_bp_runs, 2)
    home_total = round(home_starter_runs + home_bp_runs, 2)
    game_total = round(away_total + home_total, 2)

    # Hard cap at 18 runs total
    if game_total > 18.0:
        r          = 18.0 / game_total
        away_total = round(away_total * r, 2)
        home_total = round(home_total * r, 2)
        game_total = round(away_total + home_total, 2)

    return {
        "away_proj_runs": away_total,
        "home_proj_runs": home_total,
        "proj_total":     game_total,
    }


# ─────────────────────────────────────────────
# WIN PROBABILITY
# ─────────────────────────────────────────────
def win_probability(away_runs: float, home_runs: float) -> tuple:
    """
    Simple ratio win probability from projected runs.
    Capped at MAX_WIN_PROB to prevent overconfidence.
    Blended with MC simulation in main pipeline.
    """
    total = away_runs + home_runs
    if total <= 0:
        return 0.50, 0.50
    away_prob = away_runs / total
    home_prob = home_runs / total
    # Apply home field edge
    home_prob = min(home_prob + HOME_FIELD_ADVANTAGE, MAX_WIN_PROB)
    away_prob = round(1.0 - home_prob, 4)
    home_prob = round(home_prob, 4)
    # Cap both
    if away_prob > MAX_WIN_PROB:
        away_prob = MAX_WIN_PROB
        home_prob = round(1.0 - away_prob, 4)
    return away_prob, home_prob


# ─────────────────────────────────────────────
# MONTE CARLO SIMULATION
# ─────────────────────────────────────────────
def _poisson_sample(lam: float) -> int:
    """Sample from Poisson using Knuth algorithm. No numpy needed."""
    if lam <= 0:
        return 0
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1


def monte_carlo_game(
    away_runs: float,
    home_runs: float,
    n_sims:    int = None,
) -> dict:
    """
    Run N Monte Carlo simulations using Poisson scoring.
    Returns full probability distributions for totals, margins, win probs.
    """
    n_sims    = n_sims or MC_SIMULATIONS
    away_runs = max(away_runs, 0.1)
    home_runs = max(home_runs, 0.1)

    away_wins = home_wins = ties = 0
    totals  = []
    margins = []  # home - away

    for _ in range(n_sims):
        a = _poisson_sample(away_runs)
        h = _poisson_sample(home_runs)
        totals.append(a + h)
        margins.append(h - a)
        if   a > h: away_wins += 1
        elif h > a: home_wins += 1
        else:       ties      += 1

    away_win_prob = round((away_wins + ties * 0.5) / n_sims, 4)
    home_win_prob = round(1.0 - away_win_prob, 4)

    avg_total   = round(sum(totals) / n_sims, 2)
    total_stdev = round((sum((t - avg_total) ** 2 for t in totals) / n_sims) ** 0.5, 2)

    sorted_totals = sorted(totals)
    p10 = sorted_totals[int(n_sims * 0.10)]
    p25 = sorted_totals[int(n_sims * 0.25)]
    p75 = sorted_totals[int(n_sims * 0.75)]
    p90 = sorted_totals[int(n_sims * 0.90)]

    away_rl_prob = round(sum(1 for m in margins if -m > 1.5) / n_sims, 4)
    home_rl_prob = round(sum(1 for m in margins if  m > 1.5) / n_sims, 4)

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
        "totals":         totals,   # full dist for line queries
        "margins":        margins,
    }


def mc_prob_over(sim_results: dict, line: float) -> float:
    totals = sim_results.get("totals", [])
    if not totals:
        return 0.5
    return round(sum(1 for t in totals if t > line) / len(totals), 4)


def mc_prob_under(sim_results: dict, line: float) -> float:
    return round(1.0 - mc_prob_over(sim_results, line), 4)


# ─────────────────────────────────────────────
# CALIBRATION HELPERS
# ─────────────────────────────────────────────
def apply_prob_calibration(prob: float, calibration: dict) -> float:
    """Scale win probability using R-derived calibration factor."""
    if not calibration:
        return prob
    conf   = float(calibration.get("sample_confidence", 0))
    factor = float(calibration.get("prob_confidence_factor", 1.0))
    factor = max(0.70, min(1.30, factor))
    factor = 1.0 + (factor - 1.0) * conf
    return round(min(0.95, max(0.05, prob * factor)), 4)


def blend_mc_win_prob(
    ratio_prob: float,
    mc_prob:    float,
    max_prob:   float = None,
) -> tuple:
    """
    Blend ratio win prob 40% + MC win prob 60%.
    Returns (away_prob, home_prob) capped at MAX_WIN_PROB.
    """
    max_prob  = max_prob or MAX_WIN_PROB
    away_prob = round(ratio_prob * 0.40 + mc_prob * 0.60, 4)
    home_prob = round(1.0 - away_prob, 4)
    if away_prob > max_prob:
        away_prob = max_prob
        home_prob = round(1.0 - away_prob, 4)
    elif home_prob > max_prob:
        home_prob = max_prob
        away_prob = round(1.0 - home_prob, 4)
    return away_prob, home_prob
