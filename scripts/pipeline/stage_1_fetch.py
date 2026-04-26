"""
Stage 1 — Fetch.

Loads everything needed to score a sport's candidate picks:
  - power ratings (with auto-seed fallback for unrated teams)
  - Elo ratings (surface-split for tennis)
  - SPORT_CONFIG (with auto-inferred tennis surface)
  - games from market_consensus (latest snapshot per event, in time window)
  - threshold values (min_pv, min_pv_totals, min_pv_ml)

Pure data layer — no scoring, no gating, no side effects beyond stdout logs
(which preserve existing behavior).

Extracted from model_engine.generate_predictions() lines 1379-1582 in v26.0
Phase 1. Replay-verified byte-equivalent vs baseline_pre_phase_1.
"""
from datetime import datetime, timedelta

# Tennis surface inference — preserved from model_engine.py
_CLAY_KEYWORDS = [
    'french_open', 'roland_garros', 'monte_carlo', 'madrid', 'italian_open',
    'rome', 'barcelona', 'hamburg', 'rio', 'buenos_aires', 'lyon', 'bastad',
    'kitzbuhel', 'umag', 'gstaad', 'geneva', 'marrakech', 'bucharest',
    'parma', 'palermo', 'prague', 'rabat', 'strasbourg', 'lausanne',
    'portoroz', 'bogota', 'istanbul', 'budapest',
]
_GRASS_KEYWORDS = [
    'wimbledon', 'queens', 'halle', 'stuttgart_grass', 'eastbourne', 'berlin',
    'bad_homburg', 'nottingham', 'mallorca', 's_hertogenbosch', 'birmingham',
    'libema',
]


def _derive_rating_from_results(team, sport_key, conn, ratings, hca_val):
    """Mini-bootstrap: derive a team's rating from their last 10 game margins.

    For each historical game, compute the rating that would explain the actual
    margin given the opponent's known rating. Average across games. Used for
    teams missing from the power_ratings bootstrap (common for small NCAAB
    schools).
    """
    rows = conn.execute(
        """
        SELECT home, away, actual_margin
        FROM results
        WHERE (home = ? OR away = ?) AND sport = ? AND completed = 1
        AND actual_margin IS NOT NULL
        ORDER BY commence_time DESC LIMIT 10
        """, (team, team, sport_key)).fetchall()
    if len(rows) < 2:
        return None
    implied = []
    for home_r, away_r, margin in rows:
        is_home = (home_r == team)
        opponent = away_r if is_home else home_r
        opp_rating = ratings.get(opponent, {}).get('final')
        if opp_rating is None:
            continue
        if is_home:
            implied.append(margin + opp_rating - hca_val)
        else:
            implied.append(-margin + opp_rating + hca_val)
    if not implied:
        return None
    return round(sum(implied) / len(implied), 2)


def _autoseed_unrated(games, ratings, sp, conn, hca_seed):
    """Step through games, seed any unrated team via results → market → zero."""
    seeded = 0
    for g in games:
        home_t, away_t = g[2], g[3]
        mkt_spread = g[4]
        h_rated = home_t in ratings
        a_rated = away_t in ratings
        if h_rated and a_rated:
            continue
        # Step 1: derive from results
        if not h_rated:
            d = _derive_rating_from_results(home_t, sp, conn, ratings, hca_seed)
            if d is not None:
                ratings[home_t] = {'base': d, 'home_court': hca_seed, 'final': d}
                h_rated = True
                seeded += 1
        if not a_rated:
            d = _derive_rating_from_results(away_t, sp, conn, ratings, hca_seed)
            if d is not None:
                ratings[away_t] = {'base': d, 'home_court': hca_seed, 'final': d}
                a_rated = True
                seeded += 1
        if h_rated and a_rated:
            continue
        # Step 2: derive from market spread + rated opponent
        if mkt_spread is not None:
            if h_rated and not a_rated:
                d = ratings[home_t]['final'] + mkt_spread + hca_seed
                ratings[away_t] = {'base': d, 'home_court': hca_seed, 'final': round(d, 2)}
                seeded += 1
            elif a_rated and not h_rated:
                d = ratings[away_t]['final'] - mkt_spread - hca_seed
                ratings[home_t] = {'base': d, 'home_court': hca_seed, 'final': round(d, 2)}
                seeded += 1
            else:
                # Neither rated — seed home at 0 (model = market)
                ratings[home_t] = {'base': 0.0, 'home_court': hca_seed, 'final': 0.0}
                d = 0.0 + mkt_spread + hca_seed
                ratings[away_t] = {'base': d, 'home_court': hca_seed, 'final': round(d, 2)}
                seeded += 2
        else:
            if not h_rated:
                ratings[home_t] = {'base': 0.0, 'home_court': hca_seed, 'final': 0.0}
                seeded += 1
            if not a_rated:
                ratings[away_t] = {'base': 0.0, 'home_court': hca_seed, 'final': 0.0}
                seeded += 1
    return seeded


def _infer_tennis_surface(sp):
    sp_lower = sp.lower()
    if any(kw in sp_lower for kw in _CLAY_KEYWORDS):
        return 'clay'
    if any(kw in sp_lower for kw in _GRASS_KEYWORDS):
        return 'grass'
    return 'hard'


def load_sport_setup(conn, sp, window_start, window_end):
    """
    Load every input the scoring stage needs for one sport.

    Returns:
        dict with keys ratings, elo_data, has_elo, games, is_thin,
                       min_pv, min_pv_totals, min_pv_ml, cfg, seeded
        OR None if the sport should be skipped (insufficient data).

    Side effects:
        Same stdout logging as the original generate_predictions() block.
    """
    # Late imports preserve the existing module-load behavior of
    # generate_predictions() (which imports from elo_engine inside the loop).
    from model_engine import (
        get_latest_ratings, get_elo_ratings, minimum_play_threshold,
        SPORT_CONFIG, _TENNIS_PARAMS, HAS_ELO,
    )

    ratings = get_latest_ratings(conn, sp)

    # Tennis: seed power ratings from Elo when bootstrap is empty.
    if sp.startswith('tennis_') and len(ratings) < 5:
        try:
            from elo_engine import get_tennis_elo
            t_elo, t_key = get_tennis_elo(conn, sp)
            if t_elo:
                for player, data in t_elo.items():
                    val = round((data['elo'] - 1500) / 120, 2)
                    ratings[player] = {'base': val, 'home_court': 0.0, 'final': val}
                print(f"  {sp}: {len(ratings)} players seeded from Elo ({t_key})")
        except Exception as e:
            print(f"  {sp}: Elo seed failed: {e}")

    if len(ratings) < 5:
        print(f"  {sp}: only {len(ratings)} teams — SKIP")
        return None
    print(f"  {sp}: {len(ratings)} teams rated")

    elo_data = {}
    if HAS_ELO:
        if sp.startswith('tennis_'):
            try:
                from elo_engine import get_tennis_elo
                elo_data, elo_key = get_tennis_elo(conn, sp)
                if elo_data:
                    n = sum(1 for v in elo_data.values() if v['confidence'] != 'LOW')
                    print(f"    + Tennis Elo ({elo_key}): {n} players with confidence")
                else:
                    print(f"    ⚠ No tennis Elo — run historical_scores.py + elo_engine.py")
            except ImportError:
                pass
        else:
            elo_data = get_elo_ratings(conn, sp)
            if elo_data:
                n = sum(1 for v in elo_data.values() if v['confidence'] != 'LOW')
                print(f"    + Elo ratings: {n} teams with confidence")
            else:
                print(f"    ⚠ No Elo data — using market ratings only (run historical_scores.py + elo_engine.py)")

    game_count = conn.execute(
        "SELECT COUNT(DISTINCT event_id) FROM market_consensus WHERE sport=?",
        (sp,)).fetchone()[0]
    is_thin = game_count < 30
    if is_thin:
        print(f"    {game_count} games — conservative mode")

    min_pv = minimum_play_threshold(sp, is_thin)
    # v12 FIX: Totals 24% +CLV rate. Require 5% more edge than spreads.
    min_pv_totals = min_pv + 5.0
    if 'soccer' in sp:
        min_pv_totals = 5.0  # Goals-vs-points scale; 9W-2L at 5%+ in backtest
    min_pv_ml = max(5.0, min_pv * 0.50)

    games = conn.execute(
        """
        SELECT event_id, commence_time, home, away,
               best_home_spread, best_home_spread_odds, best_home_spread_book,
               best_away_spread, best_away_spread_odds, best_away_spread_book,
               best_over_total, best_over_odds, best_over_book,
               best_under_total, best_under_odds, best_under_book,
               best_home_ml, best_home_ml_book, best_away_ml, best_away_ml_book
        FROM market_consensus
        WHERE sport=? AND commence_time>=? AND commence_time<=?
        AND snapshot_date = (
            SELECT MAX(mc2.snapshot_date) FROM market_consensus mc2
            WHERE mc2.event_id = market_consensus.event_id AND mc2.sport = market_consensus.sport
        )
        ORDER BY commence_time
        """, (sp, window_start, window_end)).fetchall()
    print(f"    {len(games)} games today")

    hca_seed = SPORT_CONFIG.get(sp, {}).get('home_court', 2.5)
    seeded = _autoseed_unrated(games, ratings, sp, conn, hca_seed)
    if seeded:
        print(f"    + Auto-seeded {seeded} unrated teams (from results + market data)")

    cfg = SPORT_CONFIG.get(sp)
    if cfg is None:
        if 'tennis' in sp:
            surface = _infer_tennis_surface(sp)
            cfg = dict(_TENNIS_PARAMS[surface])
            SPORT_CONFIG[sp] = cfg
            print(f"    Auto-config: {sp} → {surface} court")
        else:
            print(f"    ⚠ Unknown sport: {sp} — skipping")
            return None

    return {
        'ratings': ratings,
        'elo_data': elo_data,
        'has_elo': HAS_ELO,
        'games': games,
        'is_thin': is_thin,
        'min_pv': min_pv,
        'min_pv_totals': min_pv_totals,
        'min_pv_ml': min_pv_ml,
        'cfg': cfg,
        'seeded': seeded,
    }
