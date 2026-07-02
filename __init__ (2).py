"""
fetchers/odds.py
================
All odds fetching — The Odds API (DraftKings) + OddsPapi (gap fill).
Returns a unified odds dict keyed by "Away Team @ Home Team".
No model logic, no sheets — fetching only.
"""

import datetime
import requests

from config import (
    ODDS_API_KEY, ODDS_API_BASE,
    ODDSPAPI_KEY, ODDSPAPI_BASE, MLB_TOURNAMENT_ID,
)


# ─────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────
def today_str() -> str:
    return datetime.date.today().strftime("%Y-%m-%d")


def american_to_prob(odds: int) -> float:
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def _fuzzy_match_game(op_key: str, all_odds: dict) -> str:
    """Match OddsPapi game key to Odds API game key by shared team name words."""
    op_parts = set(op_key.lower().replace(" @ ", " ").split())
    for key in all_odds:
        ak_parts = set(key.lower().replace(" @ ", " ").split())
        if len(op_parts & ak_parts) >= 2:
            return key
    return None


# ─────────────────────────────────────────────
# THE ODDS API — PRIMARY SOURCE
# ─────────────────────────────────────────────
def get_mlb_odds() -> dict:
    """
    Fetch MLB odds from The Odds API (DraftKings primary).
    Markets: h2h (ML), spreads (Run Line), totals (Game Total).
    Returns unified dict keyed by "Away @ Home".
    """
    print("\n📡 Fetching odds from The Odds API...")
    all_odds = {}

    # ── Main markets ─────────────────────────────────────────
    market_calls = [
        ("h2h",    "Moneyline"),
        ("spreads", "Run Line"),
        ("totals",  "Game Total"),
    ]
    for market, label in market_calls:
        try:
            r = requests.get(
                f"{ODDS_API_BASE}/sports/baseball_mlb/odds",
                params={
                    "apiKey":      ODDS_API_KEY,
                    "regions":     "us",
                    "markets":     market,
                    "oddsFormat":  "american",
                    "dateFormat":  "iso",
                },
                timeout=15,
            )
            if market == "h2h":
                print(f"   API requests remaining: {r.headers.get('x-requests-remaining', '?')}")
            data = r.json()
            if not isinstance(data, list):
                print(f"   ⚠️  {label}: unexpected response — {str(data)[:120]}")
                continue

            for game in data:
                away = game.get("away_team", "")
                home = game.get("home_team", "")
                key  = f"{away} @ {home}"
                if key not in all_odds:
                    all_odds[key] = {
                        "away_team":  away,
                        "home_team":  home,
                        "game_time":  game.get("commence_time", ""),
                    }
                # Prefer DraftKings, fall back to first available
                books = game.get("bookmakers", [])
                book  = next((b for b in books if b["key"] == "draftkings"), None)
                if not book and books:
                    book = books[0]
                if not book:
                    continue

                for mkt in book.get("markets", []):
                    mkt_key  = mkt.get("key")
                    outcomes = mkt.get("outcomes", [])

                    if mkt_key == "h2h":
                        for o in outcomes:
                            if o["name"] == away:
                                all_odds[key]["away_ml"] = int(o["price"])
                            elif o["name"] == home:
                                all_odds[key]["home_ml"] = int(o["price"])

                    elif mkt_key == "spreads":
                        for o in outcomes:
                            if o["name"] == away:
                                all_odds[key]["away_rl_odds"] = int(o["price"])
                                all_odds[key]["away_rl_line"] = o.get("point", -1.5)
                            elif o["name"] == home:
                                all_odds[key]["home_rl_odds"] = int(o["price"])

                    elif mkt_key == "totals":
                        for o in outcomes:
                            if o["name"] == "Over":
                                all_odds[key]["total_line"] = o.get("point")
                                all_odds[key]["over_odds"]  = int(o["price"])
                            elif o["name"] == "Under":
                                all_odds[key]["under_odds"] = int(o["price"])

            print(f"   ✅ {label} fetched")
        except Exception as e:
            print(f"   ⚠️  Could not fetch {label}: {e}")

    # Will populate when plan is upgraded or alternative source found

    print(f"   ✅ Odds fetched for {len(all_odds)} games")
    return all_odds


# ─────────────────────────────────────────────
# ODDSPAPI — PINNACLE GAP FILL
# ─────────────────────────────────────────────
def _fetch_oddspapi_book(bookmaker: str, odds_api_games: dict = None) -> dict:
    """
    Fetch a single bookmaker from OddsPapi.
    Response format: list of events, each with bookmakerOdds[bookmaker][markets].
    Matches to odds_api_games by startTime with ±10min fuzzy matching.
    Returns dict keyed by "Away @ Home".
    """
    if not ODDSPAPI_KEY:
        return {}
    try:
        r = requests.get(
            f"{ODDSPAPI_BASE}/odds-by-tournaments",
            params={
                "apiKey":        ODDSPAPI_KEY,
                "bookmaker":     bookmaker,
                "tournamentIds": MLB_TOURNAMENT_ID,
            },
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()

        if not isinstance(data, list):
            print(f"  ⚠️  OddsPapi unexpected format: {type(data)}")
            return {}

        # Build startTime → game key map with fuzzy ±10min matching
        time_to_key = {}
        time_to_dt  = {}
        if odds_api_games:
            for key, gdata in odds_api_games.items():
                gt = gdata.get("game_time", "")
                if not gt:
                    continue
                try:
                    gt_clean = gt.replace("Z", "+00:00")
                    if "+" not in gt_clean and len(gt_clean) >= 16:
                        gt_clean += "+00:00"
                    utc_dt = datetime.datetime.fromisoformat(gt_clean)
                    # Keep as UTC for matching — OddsPapi also uses UTC
                    et_dt = utc_dt.replace(tzinfo=None)
                    time_to_key[et_dt.strftime("%Y-%m-%dT%H:%M")] = key
                    time_to_dt[key] = et_dt
                except Exception:
                    time_to_key[gt[:16]] = key

        def _fuzzy_time_match(papi_time_str: str) -> str:
            ts = papi_time_str[:16]
            if ts in time_to_key:
                return time_to_key[ts]
            try:
                papi_dt = datetime.datetime.fromisoformat(ts)
            except Exception:
                return ""
            best_key, best_diff = "", 999
            for key, dt in time_to_dt.items():
                diff = abs((papi_dt - dt).total_seconds() / 60)
                if diff < best_diff and diff <= 10:
                    best_diff = diff
                    best_key  = key
            return best_key

        matched = sum(1 for e in data if _fuzzy_time_match(str(e.get("startTime", ""))[:16]))
        print(f"  🔍 OddsPapi: {len(data)} events | {matched} time-matched")

        result = {}
        for event in data:
            if not isinstance(event, dict):
                continue

            bm_data = event.get("bookmakerOdds", {}).get(bookmaker, {})
            if not bm_data or not bm_data.get("bookmakerIsActive", True):
                continue

            start    = str(event.get("startTime", "") or "")[:16]
            game_key = _fuzzy_time_match(start)
            if not game_key or not odds_api_games:
                continue

            gd   = odds_api_games[game_key]
            away = gd.get("away_team", "")
            home = gd.get("home_team", "")
            if not away or not home:
                continue

            key = f"{away} @ {home}"
            od  = {"away_team": away, "home_team": home}

            markets = bm_data.get("markets", {})
            if isinstance(markets, list):
                markets = {str(i): m for i, m in enumerate(markets)}

            for mkey, market in markets.items():
                if not isinstance(market, dict):
                    continue
                market_id = str(market.get("bookmakerMarketId", "") or "").lower()
                if "altline" in market_id:
                    continue

                is_ml       = "moneyline" in market_id
                is_spread   = "spread" in market_id and "team" not in market_id
                is_total    = "total" in market_id and "team" not in market_id and "h1" not in market_id

                # Collect active outcomes
                outcomes_raw = market.get("outcomes", {})
                if isinstance(outcomes_raw, list):
                    outcomes_raw = {str(i): o for i, o in enumerate(outcomes_raw)}

                active_ocs = []
                for oc_id, oc in outcomes_raw.items():
                    if not isinstance(oc, dict):
                        continue
                    players = oc.get("players", {})
                    player  = players.get("0") or players.get(0) or {}
                    if not isinstance(player, dict):
                        continue
                    if not player.get("mainLine", True):
                        continue
                    if not player.get("active", True):
                        continue
                    price_am = player.get("priceAmerican")
                    if price_am is None:
                        continue
                    active_ocs.append({
                        "american": int(price_am),
                        "name":     str(player.get("playerName", "") or "").strip().lower(),
                        "line":     player.get("line"),
                        "desc":     str(player.get("description", "") or "").strip().lower(),
                    })

                if not active_ocs:
                    continue

                if is_ml and len(active_ocs) >= 2:
                    od["away_ml"] = active_ocs[0]["american"]
                    od["home_ml"] = active_ocs[1]["american"]

                elif is_spread and len(active_ocs) >= 2:
                    try:
                        line_f = float(active_ocs[0].get("line") or 0)
                        if line_f < 0:
                            od["away_rl_odds"] = active_ocs[0]["american"]
                            od["home_rl_odds"] = active_ocs[1]["american"]
                            od["away_rl_line"] = abs(line_f)
                        else:
                            od["away_rl_odds"] = active_ocs[1]["american"]
                            od["home_rl_odds"] = active_ocs[0]["american"]
                            od["away_rl_line"] = line_f
                    except Exception:
                        pass

                elif is_total and len(active_ocs) >= 2:
                    over_ocs  = [o for o in active_ocs if "over"  in o["name"] or "over"  in o["desc"]]
                    under_ocs = [o for o in active_ocs if "under" in o["name"] or "under" in o["desc"]]
                    if not over_ocs:
                        over_ocs  = [active_ocs[0]]
                        under_ocs = [active_ocs[1]] if len(active_ocs) > 1 else []
                    try:
                        line_f = float(over_ocs[0].get("line") or 0)
                        if line_f > 0:
                            od["total_line"] = line_f
                        od["over_odds"]  = over_ocs[0]["american"]
                        if under_ocs:
                            od["under_odds"] = under_ocs[0]["american"]
                    except Exception:
                        pass

            if od.get("home_ml") or od.get("away_ml") or od.get("over_odds"):
                result[key] = od

        print(f"  ✅ OddsPapi {bookmaker}: {len(result)} games")
        return result

    except Exception as e:
        print(f"  ⚠️  OddsPapi ({bookmaker}): {e}")
        import traceback
        traceback.print_exc()
        return {}


def get_oddspapi_fallback(all_odds: dict) -> dict:
    """
    Pull Pinnacle → gap-fill missing ML when DraftKings has none.
    Called after get_mlb_odds().
    """

    if not ODDSPAPI_KEY:
        print("  ⚠️  ODDSPAPI_KEY not set — skipping Pinnacle")
        return all_odds

    print("\n📊 OddsPapi: Pinnacle gap fill...")

    # ── Gap-fill missing ML from Pinnacle ────────────────────
    pin_odds = _fetch_oddspapi_book("pinnacle", odds_api_games=all_odds)

    for op_key, op_data in pin_odds.items():
        mk = _fuzzy_match_game(op_key, all_odds)
        if not mk:
            continue
        if not all_odds[mk].get("away_ml") and op_data.get("away_ml"):
            all_odds[mk]["away_ml"] = op_data["away_ml"]
        if not all_odds[mk].get("home_ml") and op_data.get("home_ml"):
            all_odds[mk]["home_ml"] = op_data["home_ml"]

    print(f"  ✅ Pinnacle gap-fill: {len(pin_odds)} games")
    return all_odds