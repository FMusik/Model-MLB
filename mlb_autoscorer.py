"""
models/signals.py
=================
Edge calculation, confidence scoring, Kelly Criterion sizing,
and signal generation.

Your model owns all signals.
"""

from config import (
    EDGE_THRESHOLDS, MAX_EDGE_THRESHOLDS,
    CONF_MIN, CONF_MAX, CONF_SPRINKLE,
    KELLY_BANKROLL, KELLY_FRACTION, KELLY_MAX_BET, KELLY_MIN_BET,
    SKIP_HEAVY_FAV, SKIP_OVERCONFIDENT, SKIP_GAP_LIMIT,
)


# ─────────────────────────────────────────────
# CORE MATH
# ─────────────────────────────────────────────
def american_to_prob(odds: int) -> float:
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def prob_to_american(prob: float) -> int:
    if prob <= 0 or prob >= 1:
        return 0
    if prob >= 0.5:
        return round(-(prob / (1 - prob)) * 100)
    return round(((1 - prob) / prob) * 100)


def calc_edge(our_prob: float, market_odds: int) -> float:
    """Edge % = our implied prob - market implied prob."""
    return round((our_prob - american_to_prob(market_odds)) * 100, 1)


# ─────────────────────────────────────────────
# KELLY CRITERION
# ─────────────────────────────────────────────
def kelly_size(
    our_prob:    float,
    market_odds: int,
    bet_type:    str = "",
) -> float:
    """
    Half-Kelly bet sizing in units.

    Formula: f = (bp - q) / b
      b = decimal odds - 1
      p = our win probability
      q = 1 - p

    Returns unit size rounded to nearest 0.25, capped at KELLY_MAX_BET.
    Returns 0.0 if Kelly is negative (no edge).
    """
    if not market_odds or our_prob <= 0:
        return 0.0

    # Convert American to decimal
    if market_odds > 0:
        decimal_odds = (market_odds / 100) + 1
    else:
        decimal_odds = (100 / abs(market_odds)) + 1

    b = decimal_odds - 1
    p = our_prob
    q = 1 - p

    if b <= 0:
        return 0.0

    full_kelly = (b * p - q) / b
    if full_kelly <= 0:
        return 0.0  # negative Kelly = no bet

    # Half-Kelly for safety
    half_kelly = full_kelly * KELLY_FRACTION

    # Convert to units (fraction of bankroll * bankroll)
    units = half_kelly * KELLY_BANKROLL

    # Round to nearest 0.25U
    units = round(round(units / 0.25) * 0.25, 2)

    # Apply floor and ceiling
    if units < KELLY_MIN_BET:
        return 0.0
    return min(units, KELLY_MAX_BET)


# ─────────────────────────────────────────────
# HARD SKIP FILTERS
# ─────────────────────────────────────────────
def _hard_skip(
    our_prob:    float,
    market_odds: int,
    bet_type:    str,
    edge:        float,
) -> str | None:
    """
    Returns a skip reason string if the bet should be skipped,
    None if it passes all filters.
    """
    if not market_odds:
        return "no odds"

    # Minimum edge by bet type
    min_edge = EDGE_THRESHOLDS.get(bet_type, EDGE_THRESHOLDS["default"])
    if edge < min_edge:
        return f"edge {edge:.1f}%<{min_edge:.0f}%"

    # Maximum edge cap — implausibly large edges are model error, not value.
    max_edge = MAX_EDGE_THRESHOLDS.get(bet_type, MAX_EDGE_THRESHOLDS["default"])
    if edge > max_edge:
        return f"edge {edge:.1f}%>{max_edge:.0f}% cap"

    # Our model is overconfident
    fair = prob_to_american(our_prob)
    if fair < SKIP_OVERCONFIDENT:
        return f"overconfident fair={fair}"

    # Market is a heavy favorite — low value
    if market_odds < SKIP_HEAVY_FAV:
        return f"heavy fav mkt={market_odds}"

    # Gap between fair and market is too large — something's wrong
    if abs(fair - market_odds) > SKIP_GAP_LIMIT:
        return f"gap={abs(fair - market_odds)}"

    return None


# ─────────────────────────────────────────────
# CONFIDENCE SCORING
# ─────────────────────────────────────────────
def score_confidence(
    our_prob:          float,
    edge:              float,
    bet_type:          str   = "",
    # Monte Carlo
    mc_win_prob:       float = None,
    mc_stdev:          float = None,
    # Contextual adjustments
    ump_adj:           int   = 0,
    pitcher_form:      str   = "",
    rest_factor:       float = 1.00,
    fatigue_factor:    float = 1.00,
    bp_rolling_factor: float = 1.00,
    series_game_num:   int   = 1,
    lineup_confirmed:  bool  = True,
    # Support indicators
    elo_gap:           float = 0.0,
    l10_edge:          float = 0.0,
    rdiff_edge:        float = 0.0,
    streak_edge:       float = 0.0,
    support_score:     float = 0.0,
    savant_edge:       float = 0.0,
) -> float:
    """
    Build confidence % score (35–85%) for a given bet.
    Higher = more confident in the signal.
    """
    conf = our_prob * 100  # start from raw win probability

    # 1. EDGE BONUS — each 1% of edge = +0.8% conf
    conf += edge * 0.8

    # 2. MONTE CARLO AGREEMENT
    if mc_win_prob is not None:
        mc_diff = (mc_win_prob - our_prob) * 100
        if   mc_diff >= 3:  conf += 2.0
        elif mc_diff <= -3: conf -= 2.0
    if mc_stdev is not None:
        if   mc_stdev > 4.0: conf -= 3.0
        elif mc_stdev > 3.5: conf -= 1.5
        elif mc_stdev < 2.5: conf += 1.5

    # 5. UMP TENDENCY
    if   ump_adj > 0: conf += 2.0
    elif ump_adj < 0: conf -= 2.0

    # 6. PITCHER FORM
    if   "🔥 HOT"       in str(pitcher_form): conf += 2.0
    elif "✅ SOLID"      in str(pitcher_form): conf += 1.0
    elif "❄️ COLD"       in str(pitcher_form): conf -= 2.0
    elif "🚨 STRUGGLING" in str(pitcher_form): conf -= 3.0

    # 7. PITCHER REST
    if   rest_factor < 0.95: conf -= 2.0  # short rest or rust
    elif rest_factor > 1.00: conf += 1.0  # extra rest

    # 8. FATIGUE
    if   fatigue_factor < 0.95: conf -= 2.0
    elif fatigue_factor < 0.97: conf -= 1.0

    # 9. BULLPEN ROLLING WORKLOAD
    if bp_rolling_factor < 1.00: conf -= 1.5

    # 10. LINEUP CONFIRMED
    if not lineup_confirmed: conf -= 2.0

    # 11. SERIES CONTEXT
    if series_game_num == 1:
        if bet_type == "under": conf += 1.0
        elif bet_type == "over": conf -= 1.0
    elif series_game_num >= 3:
        if bet_type == "over": conf += 1.0

    # 12. ELO GAP
    conf += (elo_gap / 300.0) * 3.0

    # 13. L10 EDGE
    conf += l10_edge * 10.0

    # 14. RUN DIFFERENTIAL EDGE
    conf += rdiff_edge * 3.0

    # 15. STREAK EDGE
    conf += streak_edge * 2.0

    # 16. COMPOSITE SUPPORT
    if   support_score > 0.15:  conf += 3.0
    elif support_score > 0.05:  conf += 1.5
    elif support_score < -0.15: conf -= 3.0
    elif support_score < -0.05: conf -= 1.5

    # 17. SAVANT EDGE
    if   savant_edge > 0.30:  conf += 3.0
    elif savant_edge > 0.15:  conf += 1.5
    elif savant_edge < -0.30: conf -= 3.0
    elif savant_edge < -0.15: conf -= 1.5

    return round(max(CONF_MIN, min(CONF_MAX, conf)), 1)


# ─────────────────────────────────────────────
# MAIN SIGNAL GENERATOR
# ─────────────────────────────────────────────
def score_signal(
    our_prob:    float,
    market_odds: int,
    bet_type:    str   = "",
    line_value:  float = None,
    # Confidence inputs
    mc_win_prob:       float = None,
    mc_stdev:          float = None,
    ump_adj:           int   = 0,
    pitcher_form:      str   = "",
    rest_factor:       float = 1.00,
    fatigue_factor:    float = 1.00,
    bp_rolling_factor: float = 1.00,
    series_game_num:   int   = 1,
    lineup_confirmed:  bool  = True,
    elo_gap:           float = 0.0,
    l10_edge:          float = 0.0,
    rdiff_edge:        float = 0.0,
    streak_edge:       float = 0.0,
    support_score:     float = 0.0,
    savant_edge:       float = 0.0,
) -> tuple:
    """
    Full signal pipeline. Returns (signal_str, confidence, edge, kelly_units).

    signal_str examples:
      "— SKIP (edge 3.1%<5%)"
      "⭐⭐⭐⭐⭐⭐⭐ 71% → 0.75U"
      "❌ FADE"

    Returns 4-tuple: (signal, conf, edge, units)
    """
    if not market_odds:
        return "—", 0.0, 0.0, 0.0

    edge      = calc_edge(our_prob, market_odds)

    # ── Hard skip filters ─────────────────────────────────────
    skip_reason = _hard_skip(our_prob, market_odds, bet_type, edge)
    if skip_reason:
        return f"— SKIP ({skip_reason})", 0.0, edge, 0.0

    # Fade signal — negative edge beyond threshold
    if edge <= -EDGE_THRESHOLDS.get(bet_type, EDGE_THRESHOLDS["default"]):
        return "❌ FADE", 0.0, edge, 0.0

    # ── Confidence score ──────────────────────────────────────
    conf = score_confidence(
        our_prob=our_prob,
        edge=edge,
        bet_type=bet_type,
        mc_win_prob=mc_win_prob,
        mc_stdev=mc_stdev,
        ump_adj=ump_adj,
        pitcher_form=pitcher_form,
        rest_factor=rest_factor,
        fatigue_factor=fatigue_factor,
        bp_rolling_factor=bp_rolling_factor,
        series_game_num=series_game_num,
        lineup_confirmed=lineup_confirmed,
        elo_gap=elo_gap,
        l10_edge=l10_edge,
        rdiff_edge=rdiff_edge,
        streak_edge=streak_edge,
        support_score=support_score,
        savant_edge=savant_edge,
    )

    # ── Kelly sizing ──────────────────────────────────────────
    units = kelly_size(our_prob, market_odds, bet_type)

    # Skip if confidence too low or Kelly says no bet
    if conf < CONF_SPRINKLE or units == 0.0:
        return f"— SKIP ({conf:.0f}%)", conf, edge, 0.0

    # ── Signal label ──────────────────────────────────────────
    stars  = "⭐" * min(int(conf / 10), 8)
    signal = f"{stars} {conf:.0f}% → {units}U"

    return signal, conf, edge, units


# ─────────────────────────────────────────────
# UMP SIGNAL ADJUSTMENT
# ─────────────────────────────────────────────
def calc_savant_edge(
    pitcher_sv:  dict,
    opp_team_sv: dict,
) -> float:
    """
    Savant edge = pitcher quality vs opposing lineup quality.
    Positive = pitcher dominates lineup (fewer runs expected).
    Negative = lineup dominates pitcher (more runs expected).
    Range roughly -1.0 to +1.0.
    """
    if not pitcher_sv or not opp_team_sv:
        return 0.0

    pitcher_score = pitcher_sv.get("sv_quality_score", 5.0)
    lineup_score  = opp_team_sv.get("sv_lineup_score", 5.0)

    # Normalize both to 0-1 scale (they're 0-10)
    edge = (pitcher_score - lineup_score) / 10.0
    return round(edge, 3)
