"""
fetchers/savant.py
==================
Fetches pitcher vs current roster Statcast data from Baseball Savant.
Provides xwOBA, xBA, xSLG, Exit Velo, K%, BB% per pitcher vs today's opponent.

Used for:
1. Regression analysis (xBA vs BA divergence)
2. Better pitcher quality signal in projections
3. Times-through-order penalty calibration

Falls back gracefully if Savant is blocked.
"""

import os
import sys
import json
import datetime
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import MLB_API_BASE, SEASON
except ImportError:
    # config.py isn't present in this repo — neither constant is actually
    # referenced in this file, so provide harmless stubs so the import succeeds.
    MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
    SEASON       = datetime.date.today().year

# ─────────────────────────────────────────────
# CACHE
# ─────────────────────────────────────────────
_probable_pitcher_cache = None  # None = not loaded yet, {} = loaded but empty

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}


# ─────────────────────────────────────────────
# FETCH PROBABLE PITCHERS PAGE
# ─────────────────────────────────────────────
def fetch_probable_pitchers_page() -> str:
    """Fetch the raw HTML from Baseball Savant probable pitchers page."""
    try:
        r = requests.get(
            "https://baseballsavant.mlb.com/probable-pitchers",
            headers=HEADERS,
            timeout=20,
        )
        print(f"  ⚡ Savant probable pitchers: HTTP {r.status_code} | {len(r.content):,} bytes")
        if r.status_code == 200:
            return r.text
        else:
            print(f"  ⚠️  Savant blocked: {r.status_code}")
            return ""
    except Exception as e:
        print(f"  ⚠️  Savant fetch error: {e}")
        return ""


# ─────────────────────────────────────────────
# PARSE PITCHER CARDS
# ─────────────────────────────────────────────
def parse_pitcher_cards(html: str) -> dict:
    """
    Parse pitcher vs roster Statcast data from probable pitchers page.
    Data is embedded server-side in the HTML — look for JS variables and data attributes.
    Returns dict keyed by pitcher_id.
    """
    if not html:
        return {}

    results = {}
    try:
        import re

        # Savant probable pitchers page embeds data in JS variables
        # Common patterns: var pitchers = [...], window.pitcherData = {...}
        js_patterns = [
            r'var\s+pitchers\s*=\s*(\[.+?\]);',
            r'var\s+probable_pitchers\s*=\s*(\[.+?\]);',
            r'window\.pitcherData\s*=\s*(\[.+?\]);',
            r'"pitchers"\s*:\s*(\[.+?\])',
            r'pitcherMatchups\s*=\s*(\[.+?\])',
        ]

        script_pattern = re.compile(r'<script[^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)
        scripts = script_pattern.findall(html)

        for script in scripts:
            # Only look in scripts that contain pitcher-related data
            if not any(kw in script.lower() for kw in ['xwoba', 'exit_velocity', 'k_percent', 'pitcher_id']):
                continue

            for pattern in js_patterns:
                matches = re.findall(pattern, script, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        if isinstance(data, list):
                            for item in data:
                                pid = str(item.get('pitcher_id') or item.get('id') or '')
                                if pid:
                                    results[pid] = _extract_stats(item)
                    except Exception:
                        pass

            # Also scan for any JSON object containing xwoba + pitcher_id
            if not results:
                obj_pattern = re.findall(r'\{[^{}]{50,4000}\}', script)
                for match in obj_pattern:
                    if 'xwoba' not in match.lower() and 'pitcher_id' not in match.lower():
                        continue
                    try:
                        data = json.loads(match)
                        pid = str(data.get('pitcher_id') or data.get('id') or '')
                        if pid and data.get('xwoba'):
                            results[pid] = _extract_stats(data)
                    except Exception:
                        pass

        # Fall back to HTML attribute parsing
        if not results:
            results = _parse_html_cards(html)

        print(f"  ⚡ Savant: parsed {len(results)} pitcher cards")
        return results

    except Exception as e:
        print(f"  ⚠️  Savant parse error: {e}")

    return results


def _extract_stats(data: dict) -> dict:
    """Extract relevant stats from a parsed data dict."""
    return {
        "sv_vs_xwoba":    _safe_float(data.get("xwoba") or data.get("xwOBA")),
        "sv_vs_xba":      _safe_float(data.get("xba")   or data.get("xBA")),
        "sv_vs_xslg":     _safe_float(data.get("xslg")  or data.get("xSLG")),
        "sv_vs_exit_velo":_safe_float(data.get("exit_velocity") or data.get("exitVelo")),
        "sv_vs_k_pct":    _safe_float(data.get("k_percent") or data.get("kPct")),
        "sv_vs_bb_pct":   _safe_float(data.get("bb_percent") or data.get("bbPct")),
        "sv_vs_launch":   _safe_float(data.get("launch_angle") or data.get("launchAngle")),
        "sv_vs_pa":       _safe_int(data.get("pa") or data.get("plateAppearances")),
        "sv_vs_avg":      _safe_float(data.get("avg") or data.get("batting_avg")),
        "sv_vs_woba":     _safe_float(data.get("woba") or data.get("wOBA")),
    }


def _parse_html_cards(html: str) -> dict:
    """Parse pitcher stat tables from HTML using BeautifulSoup."""
    results = {}
    try:
        import re
        soup = BeautifulSoup(html, "html.parser")

        all_tables = soup.find_all("table")

        # Index all tables by their headers
        trad_tables  = []  # PA, K%, BB%, AVG, wOBA
        xwoba_tables = []  # Exit Velo, Launch Angle, xBA, xSLG, xwOBA

        for table in all_tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if "xwoba" in headers:
                xwoba_tables.append((table, headers))
            elif "pa" in headers and ("k%" in headers or "woba" in headers):
                trad_tables.append((table, headers))

        # Match trad + xwoba tables by position (they come in pairs per pitcher)
        for i, (xw_table, xw_headers) in enumerate(xwoba_tables):
            # Find pitcher name from surrounding HTML
            pitcher_name = None
            parent = xw_table.parent
            for _ in range(15):
                if parent is None:
                    break
                for tag in parent.find_all(["h1", "h2", "h3", "h4", "strong", "b"]):
                    text = tag.get_text(strip=True)
                    if len(text) > 3 and len(text) < 50 and not any(c.isdigit() for c in text[:3]):
                        pitcher_name = text
                        break
                for attr in ['data-pitcher', 'data-pitcher-name', 'data-name']:
                    val = parent.get(attr)
                    if val:
                        pitcher_name = val
                        break
                if pitcher_name:
                    break
                parent = parent.parent

            # Extract xwOBA table stats
            stats = {}
            for row in xw_table.find_all("tr"):
                cells = row.find_all("td", class_="data")
                if not cells:
                    continue
                values = [td.get_text(strip=True).split()[0] for td in cells]
                row_dict = dict(zip(xw_headers, values + [''] * max(0, len(xw_headers) - len(values))))
                stats.update({
                    "sv_vs_xwoba":     _safe_float(row_dict.get("xwoba")),
                    "sv_vs_xba":       _safe_float(row_dict.get("xba")),
                    "sv_vs_xslg":      _safe_float(row_dict.get("xslg")),
                    "sv_vs_exit_velo": _safe_float(row_dict.get("exit velo")),
                    "sv_vs_launch":    _safe_float(row_dict.get("launch angle")),
                })
                stats = {k: v for k, v in stats.items() if v is not None}
                break

            # Also extract traditional stats from matching trad table (same index)
            if i < len(trad_tables):
                tr_table, tr_headers = trad_tables[i]
                for row in tr_table.find_all("tr"):
                    cells = row.find_all("td", class_="data")
                    if not cells:
                        continue
                    values = [td.get_text(strip=True).split()[0] for td in cells]
                    row_dict = dict(zip(tr_headers, values + [''] * max(0, len(tr_headers) - len(values))))
                    extra = {
                        "sv_vs_pa":    _safe_int(row_dict.get("pa")),
                        "sv_vs_k_pct": _safe_float(row_dict.get("k%")),
                        "sv_vs_bb_pct":_safe_float(row_dict.get("bb%")),
                        "sv_vs_avg":   _safe_float(row_dict.get("avg")),
                        "sv_vs_woba":  _safe_float(row_dict.get("woba")),
                    }
                    stats.update({k: v for k, v in extra.items() if v is not None})
                    break

            if stats.get("sv_vs_xwoba"):
                key = pitcher_name.lower().replace(" ", "_") if pitcher_name else f"table_{i}"
                results[key] = stats

    except Exception as e:
        print(f"  ⚠️  HTML parse error: {e}")

    return results


# ─────────────────────────────────────────────
# REGRESSION FACTOR
# ─────────────────────────────────────────────
def get_regression_factor(pitcher_data: dict) -> dict:
    """
    Calculate regression factor from xBA vs BA divergence.

    xBA > BA = pitcher has been lucky (overperforming) → project ERA higher
    xBA < BA = pitcher has been unlucky (underperforming) → project ERA lower

    Returns:
      regression_factor > 1.0 = pitcher due for regression (ERA will rise)
      regression_factor < 1.0 = pitcher performing better than results show
      regression_factor = 1.0 = neutral / no data
    """
    if not pitcher_data:
        return {"regression_factor": 1.00, "regression_label": "No data"}

    xba  = pitcher_data.get("sv_vs_xba")
    avg  = pitcher_data.get("sv_vs_avg")
    xwoba = pitcher_data.get("sv_vs_xwoba")
    woba  = pitcher_data.get("sv_vs_woba")
    pa    = pitcher_data.get("sv_vs_pa", 0) or 0

    if pa < 30:
        return {"regression_factor": 1.00, "regression_label": f"Small sample ({pa} PA)"}

    factor = 1.00
    label  = "Neutral"

    if xba and avg and avg > 0:
        xba_ratio = xba / avg
        if   xba_ratio >= 1.20:
            factor = 1.08
            label  = f"⚠️ LUCKY (xBA {xba:.3f} >> BA {avg:.3f})"
        elif xba_ratio >= 1.10:
            factor = 1.04
            label  = f"↑ Slight luck (xBA {xba:.3f} > BA {avg:.3f})"
        elif xba_ratio <= 0.80:
            factor = 0.93
            label  = f"💎 UNLUCKY (xBA {xba:.3f} << BA {avg:.3f})"
        elif xba_ratio <= 0.90:
            factor = 0.97
            label  = f"↓ Slight unlucky (xBA {xba:.3f} < BA {avg:.3f})"

    # Blend with xwOBA vs wOBA if available
    if xwoba and woba and woba > 0:
        xwoba_ratio = xwoba / woba
        if xwoba_ratio >= 1.15:
            factor = min(factor * 1.03, 1.12)
        elif xwoba_ratio <= 0.85:
            factor = max(factor * 0.97, 0.90)

    return {
        "regression_factor": round(factor, 3),
        "regression_label":  label,
        "sv_vs_xba":         xba,
        "sv_vs_avg":         avg,
        "sv_vs_xwoba":       xwoba,
        "sv_vs_woba":        woba,
        "sv_vs_pa":          pa,
    }


# ─────────────────────────────────────────────
# MAIN LOADER
# ─────────────────────────────────────────────
def load_savant_pitcher_data() -> dict:
    """
    Load all probable pitcher vs roster data for today.
    Returns dict keyed by pitcher_id (str).
    Cached per run — only fetches once.
    """
    global _probable_pitcher_cache
    if _probable_pitcher_cache is not None:
        return _probable_pitcher_cache

    print("  ⚡ Fetching Savant probable pitcher data...")
    html = fetch_probable_pitchers_page()
    if html:
        _probable_pitcher_cache = parse_pitcher_cards(html)
    else:
        _probable_pitcher_cache = {}

    return _probable_pitcher_cache


def get_pitcher_vs_roster(pitcher_id: int, pitcher_name: str = "") -> dict:
    """
    Get Savant stats for a specific pitcher vs today's opponent roster.
    Matches by pitcher name since page doesn't expose pitcher IDs easily.
    Returns {} if not available.
    """
    data = load_savant_pitcher_data()
    if not data:
        return {}

    # Try direct ID match first
    if str(pitcher_id) in data:
        return data[str(pitcher_id)]

    # Try name match — normalize name for comparison
    if pitcher_name:
        search = pitcher_name.lower().replace(" ", "_").replace(".", "")
        for key, val in data.items():
            key_clean = key.replace(".", "").replace("-", "_")
            if search in key_clean or key_clean in search:
                return val
        # Fuzzy — first/last name partial match
        parts = pitcher_name.lower().split()
        for key, val in data.items():
            if any(p in key for p in parts if len(p) > 3):
                return val

    return {}


# ─────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────
def _safe_float(val) -> float:
    if val is None:
        return None
    try:
        v = str(val).replace("%", "").strip()
        return round(float(v), 3) if v else None
    except Exception:
        return None


def _safe_int(val) -> int:
    if val is None:
        return None
    try:
        return int(str(val).strip())
    except Exception:
        return None


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"⚡ Savant Pitcher Fetcher — {datetime.date.today()}")
    data = load_savant_pitcher_data()
    if data:
        print(f"✅ {len(data)} pitchers loaded")
        for pid, stats in list(data.items())[:3]:
            reg = get_regression_factor(stats)
            print(f"  Pitcher {pid}: xwOBA={stats.get('sv_vs_xwoba')} | {reg['regression_label']}")
    else:
        print("⚠️  No data — Savant may be blocking GitHub Actions IPs")
        print("   Model will continue without pitcher vs roster data")
