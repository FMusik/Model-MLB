"""
adjustments/ump.py
==================
Umpire tendency loading and factor calculation.
Reads ump_factors.json — no API calls made here.
"""

import json

from config import UMP_FACTORS_FILE


# ─────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────
def load_ump_data() -> dict:
    """
    Load ump_factors.json. Returns full data dict including
    'umps' and 'thresholds' sections.
    Returns {} on failure — model continues with DEFAULT factors.
    """
    try:
        with open(UMP_FACTORS_FILE, "r") as f:
            data = json.load(f)
        ump_count = len(data.get("umps", {}))
        print(f"  ✅ Ump data loaded: {ump_count} umpires")
        return data
    except FileNotFoundError:
        print(f"  ⚠️  ump_factors.json not found at {UMP_FACTORS_FILE}")
        return {}
    except Exception as e:
        print(f"  ⚠️  Could not load ump_factors.json: {e}")
        return {}


# ─────────────────────────────────────────────
# FACTOR LOOKUP
# ─────────────────────────────────────────────
def get_ump_factor(ump_name: str, ump_data: dict) -> dict:
    """
    Return ump tendency dict for a given ump name.
    Falls back to DEFAULT if ump not found.
    """
    if not ump_data or not ump_name:
        return _default_factor()
    umps = ump_data.get("umps", {})
    return umps.get(ump_name) or umps.get("DEFAULT") or _default_factor()


def get_ump_run_factor(ump_name: str, ump_data: dict) -> float:
    """
    Shortcut — returns just the run_factor float.
    1.0 = neutral, >1.0 = over lean, <1.0 = under lean.
    """
    return get_ump_factor(ump_name, ump_data).get("run_factor", 1.00)


def _default_factor() -> dict:
    return {
        "run_factor": 1.00,
        "k_factor":   1.00,
        "bb_factor":  1.00,
        "avg_runs":   8.9,
        "zone":       "neutral",
        "notes":      "Unknown ump — using league average",
    }


# ─────────────────────────────────────────────
# SIGNAL ADJUSTMENT
# ─────────────────────────────────────────────
def get_ump_signal_adj(ump_name: str, bet_type: str, ump_data: dict) -> int:
    """
    Confidence point adjustment based on ump tendency vs bet type.
    Positive = ump favors this bet. Negative = ump hurts it.

    Uses thresholds from ump_factors.json so they stay configurable.
    """
    if not ump_name or not ump_data:
        return 0

    factor     = get_ump_run_factor(ump_name, ump_data)
    thresholds = ump_data.get("thresholds", {})

    strong_over  = thresholds.get("strong_over_lean",    1.07)
    mod_over     = thresholds.get("moderate_over_lean",  1.03)
    strong_under = thresholds.get("strong_under_lean",   0.93)
    mod_under    = thresholds.get("moderate_under_lean", 0.97)
    boost_s      = thresholds.get("signal_boost_strong",     8)
    boost_m      = thresholds.get("signal_boost_moderate",   4)
    pen_s        = thresholds.get("signal_penalty_strong",  -8)
    pen_m        = thresholds.get("signal_penalty_moderate", -4)

    if bet_type == "over":
        if   factor >= strong_over:  return boost_s
        elif factor >= mod_over:     return boost_m
        elif factor <= strong_under: return pen_s
        elif factor <= mod_under:    return pen_m

    elif bet_type == "under":
        if   factor <= strong_under: return boost_s
        elif factor <= mod_under:    return boost_m
        elif factor >= strong_over:  return pen_s
        elif factor >= mod_over:     return pen_m

    return 0


# ─────────────────────────────────────────────
# DISPLAY HELPERS
# ─────────────────────────────────────────────
def format_ump_summary(ump_name: str, ump_data: dict) -> str:
    """One-line ump summary for console output."""
    if not ump_name:
        return "  🧑‍⚖️  Ump: TBD"
    factor = get_ump_factor(ump_name, ump_data)
    rf     = factor.get("run_factor", 1.00)
    zone   = factor.get("zone", "neutral")
    notes  = factor.get("notes", "")
    trend  = "📈 OVER LEAN" if rf >= 1.05 else ("📉 UNDER LEAN" if rf <= 0.96 else "➡️ NEUTRAL")
    return f"  🧑‍⚖️  Ump: {ump_name} | {trend} (factor={rf:.2f}, zone={zone}) — {notes}"
