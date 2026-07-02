"""
models/elo.py
=============
Elo-style team ratings, composite support score, and team
strength indicators derived from season record + run differential.

All data comes from mlb_api.py — no API calls made here.
"""


# ─────────────────────────────────────────────
# ELO RATING
# ─────────────────────────────────────────────
def get_support_score(
    away_elo:    float,
    home_elo:    float,
    away_l10:    dict,
    home_l10:    dict,
    away_rdiff:  dict,
    home_rdiff:  dict,
    away_streak: dict,
    home_streak: dict,
) -> dict:
    """
    Weighted composite of Elo gap, L10 edge, run diff edge, streak edge.
    Positive = away team has edge. Negative = home team has edge.

    Weights:
      Elo gap    35%
      L10 edge   30%
      RDiff edge 25%
      Streak     10%
    """
    # Elo gap — normalize ±300 pts to ±1.0
    elo_gap_raw = away_elo - home_elo
    elo_gap     = elo_gap_raw / 300.0

    # L10 win% edge
    away_l10_wp = away_l10.get("l10_win_pct", 0.5) if away_l10 else 0.5
    home_l10_wp = home_l10.get("l10_win_pct", 0.5) if home_l10 else 0.5
    l10_edge    = away_l10_wp - home_l10_wp

    # Run differential edge — normalize by 5 runs
    away_rd    = away_rdiff.get("rdiff_per_game", 0) if away_rdiff else 0
    home_rd    = home_rdiff.get("rdiff_per_game", 0) if home_rdiff else 0
    rdiff_edge = (away_rd - home_rd) / 5.0

    # Streak edge — normalize by 10 games
    away_str    = away_streak.get("streak", 0) if away_streak else 0
    home_str    = home_streak.get("streak", 0) if home_streak else 0
    streak_edge = (away_str - home_str) / 10.0

    # Weighted composite
    support = round(
        elo_gap     * 0.35 +
        l10_edge    * 0.30 +
        rdiff_edge  * 0.25 +
        streak_edge * 0.10,
        3,
    )

    if   support > 0.05:  label = f"Away +{support:.2f}"
    elif support < -0.05: label = f"Home +{abs(support):.2f}"
    else:                 label = "Neutral"

    return {
        "support_score":  support,
        "elo_gap":        round(elo_gap_raw, 1),
        "l10_edge":       round(l10_edge, 3),
        "rdiff_edge":     round(rdiff_edge, 3),
        "streak_edge":    round(streak_edge, 3),
        "support_label":  label,
    }
