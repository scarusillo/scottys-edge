"""
backtest_baseball.py — College baseball backtest with ML, run line, and totals evaluation

Tests what the model would have picked using:
  1. Elo ML: Direct win probability vs de-vigged ML odds
  2. Run Lines: -1.5/+1.5 run line evaluation
  3. Spreads (ATS): Model spread vs closing spread
  4. Totals: Model total vs market total (over/under)
  5. Context-adjusted: Re-evaluates with context engine factors

Rebuilds Elo chronologically (no look-ahead bias).

Usage:
    python backtest_baseball.py                    # Last 30 days
    python backtest_baseball.py --days 60          # Last 60 days
    python backtest_baseball.py --min-edge 5       # Only 5%+ edges
    python backtest_baseball.py --min-edge 8       # Match live threshold
    python backtest_baseball.py --context          # Include context adjustments
    python backtest_baseball.py --verbose          # Show every pick detail
"""
import sqlite3, math, os, sys
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

from elo_engine import ELO_CONFIG, _expected_score, _mov_multiplier, elo_win_probability
from model_engine import (
    SPORT_CONFIG, TOTAL_STD, LEAGUE_AVG_TOTAL,
    spread_to_win_prob, spread_to_cover_prob,
    devig_ml_odds, american_to_implied_prob, _ncdf,
)
from scottys_edge import kelly_units, get_star_rating

SPORT = 'baseball_ncaa'

# Baseball-specific: what % of wins are by 2+ runs (for run line eval)
# v14: Actual data: 78.5% of wins by 2+, 21.5% decided by 1 run
WIN_BY_2_PCT = 0.785
LOSE_BY_1_PCT = 0.215


def _rebuild_elo(games, sport):
    """Rebuild Elo game-by-game chronologically. Returns snapshots BEFORE each game."""
    cfg = ELO_CONFIG.get(sport)
    if not cfg:
        return {}, defaultdict(int), []

    elos = defaultdict(lambda: cfg['initial_elo'])
    game_counts = defaultdict(int)
    history = []

    for home, away, h_score, a_score, commence in games:
        # Snapshot BEFORE update
        history.append({
            'elos': dict(elos),
            'counts': dict(game_counts),
        })

        margin = (h_score or 0) - (a_score or 0)
        home_elo = elos[home] + cfg['home_advantage']
        away_elo = elos[away]

        actual_home = 1.0 if margin > 0 else (0.0 if margin < 0 else 0.5)
        elo_diff = home_elo - away_elo
        mov_mult = _mov_multiplier(margin, elo_diff, cfg)
        games_played = min(game_counts[home], game_counts[away])
        k = cfg['k_factor'] * (1.5 if games_played < 5 else 1.0)
        delta = k * mov_mult * (actual_home - _expected_score(home_elo, away_elo))
        elos[home] += delta
        elos[away] -= delta
        game_counts[home] += 1
        game_counts[away] += 1

    return elos, game_counts, history


def _get_recent_schedule(conn, team, before_date, sport, days=7):
    """Count games a team played in the last N days before a given date."""
    cutoff = (datetime.fromisoformat(before_date.replace('Z', '+00:00').replace('+00:00', ''))
              - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')
    row = conn.execute("""
        SELECT COUNT(*) FROM results
        WHERE (home=? OR away=?) AND sport=? AND completed=1
        AND commence_time >= ? AND commence_time < ?
    """, (team, team, sport, cutoff, before_date)).fetchone()
    return row[0] if row else 0


def _get_series_context(conn, home, away, before_date, sport):
    """Check if teams played recently (series context).
    Returns: (games_in_series, last_margin, home_series_wins, away_series_wins)
    """
    cutoff = (datetime.fromisoformat(before_date.replace('Z', '+00:00').replace('+00:00', ''))
              - timedelta(days=4)).strftime('%Y-%m-%dT%H:%M:%SZ')
    recent = conn.execute("""
        SELECT home, away, home_score, away_score FROM results
        WHERE sport=? AND completed=1
        AND ((home=? AND away=?) OR (home=? AND away=?))
        AND commence_time >= ? AND commence_time < ?
        ORDER BY commence_time ASC
    """, (sport, home, away, away, home, cutoff, before_date)).fetchall()

    if not recent:
        return 0, 0, 0, 0

    series_h_wins = 0
    series_a_wins = 0
    last_margin = 0
    for rh, ra, hs, as_, in recent:
        margin = hs - as_
        if rh == home:
            last_margin = margin
            if margin > 0:
                series_h_wins += 1
            elif margin < 0:
                series_a_wins += 1
        else:
            last_margin = -margin
            if margin < 0:
                series_h_wins += 1
            elif margin > 0:
                series_a_wins += 1

    return len(recent), last_margin, series_h_wins, series_a_wins


def _compute_context_adjustment(conn, home, away, commence, sport):
    """
    Compute context-based edge adjustment for baseball.
    Returns (home_adj, away_adj, context_notes).
    Positive adj = favors that side.
    """
    home_adj = 0.0
    away_adj = 0.0
    notes = []

    # 1. Schedule fatigue: 4+ games in 5 days
    h_sched = _get_recent_schedule(conn, home, commence, sport, days=5)
    a_sched = _get_recent_schedule(conn, away, commence, sport, days=5)

    if a_sched >= 4 and h_sched < 3:
        away_adj -= 0.03  # 3% penalty for heavy away schedule
        home_adj += 0.02
        notes.append(f"Away schedule fatigue ({a_sched} in 5d)")
    elif h_sched >= 4 and a_sched < 3:
        home_adj -= 0.03
        away_adj += 0.02
        notes.append(f"Home schedule fatigue ({h_sched} in 5d)")

    if a_sched >= 5:
        away_adj -= 0.02  # Extra penalty for extreme schedule
        notes.append(f"Away extreme schedule ({a_sched} in 5d)")
    if h_sched >= 5:
        home_adj -= 0.02
        notes.append(f"Home extreme schedule ({h_sched} in 5d)")

    # 2. Series context
    series_games, last_margin, h_wins, a_wins = _get_series_context(
        conn, home, away, commence, sport)

    if series_games > 0:
        # Game 3 rubber match — tighter game, less edge
        if series_games == 2 and h_wins == 1 and a_wins == 1:
            home_adj -= 0.01  # Rubber matches are tighter
            away_adj -= 0.01
            notes.append("Rubber match (game 3)")

        # Blowout bounce-back: if last game was 7+ run blowout, loser bounces back
        if abs(last_margin) >= 7:
            if last_margin > 0:  # Home won big last game
                away_adj += 0.03
                notes.append(f"Away blowout bounce-back (lost by {last_margin})")
            else:  # Away won big
                home_adj += 0.03
                notes.append(f"Home blowout bounce-back (lost by {abs(last_margin)})")

        # Sweep attempt: team up 2-0 may rest players
        if h_wins == 2 and a_wins == 0:
            home_adj -= 0.02  # Sweep complacency
            away_adj += 0.02  # Desperation
            notes.append("Home sweep attempt — away desperate")
        elif a_wins == 2 and h_wins == 0:
            away_adj -= 0.02
            home_adj += 0.02
            notes.append("Away sweep attempt — home desperate")

    # 3. Rest advantage (3+ days rest vs 1 day)
    h_recent_1d = _get_recent_schedule(conn, home, commence, sport, days=1)
    a_recent_1d = _get_recent_schedule(conn, away, commence, sport, days=1)
    h_recent_3d = _get_recent_schedule(conn, home, commence, sport, days=3)
    a_recent_3d = _get_recent_schedule(conn, away, commence, sport, days=3)

    if h_recent_3d == 0 and a_recent_1d >= 1:
        home_adj += 0.02  # Fresh arms vs tired
        notes.append("Home fresh arms advantage")
    elif a_recent_3d == 0 and h_recent_1d >= 1:
        away_adj += 0.02
        notes.append("Away fresh arms advantage")

    return home_adj, away_adj, notes


def run_baseball_backtest(conn, days=30, min_edge=3.0, min_games=8,
                          use_context=False, verbose=True):
    """Full college baseball backtest: ML + run lines + spreads + totals."""
    cfg = ELO_CONFIG.get(SPORT)
    if not cfg:
        print("  No ELO_CONFIG for baseball_ncaa")
        return None

    cutoff = (datetime.now(datetime.now().astimezone().tzinfo) - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Get ALL completed games for Elo rebuild
    all_games = conn.execute("""
        SELECT home, away, home_score, away_score, commence_time
        FROM results
        WHERE sport=? AND completed=1 AND home_score IS NOT NULL
        ORDER BY commence_time ASC
    """, (SPORT,)).fetchall()

    if not all_games:
        print("  No results for baseball_ncaa")
        return None

    # Rebuild Elo chronologically
    elos, game_counts, history = _rebuild_elo(all_games, SPORT)

    # Baseball event_ids don't match between ESPN results and Odds API.
    # Join on team names + date instead.
    backtest_games = conn.execute("""
        SELECT r.event_id, r.home, r.away, r.home_score, r.away_score,
               r.commence_time, r.closing_spread, r.closing_total,
               mc.best_home_spread, mc.best_home_spread_odds,
               mc.best_away_spread, mc.best_away_spread_odds,
               mc.best_over_total, mc.best_over_odds, mc.best_under_odds,
               mc.best_home_ml, mc.best_away_ml, mc.event_id AS mc_event_id
        FROM results r
        JOIN market_consensus mc ON r.home = mc.home AND r.away = mc.away
                                AND SUBSTR(r.commence_time, 1, 10) = SUBSTR(mc.commence_time, 1, 10)
        WHERE r.sport=? AND mc.sport=? AND r.completed=1 AND r.home_score IS NOT NULL
        AND r.commence_time >= ?
        GROUP BY r.event_id
        ORDER BY r.commence_time ASC
    """, (SPORT, SPORT, cutoff)).fetchall()

    if not backtest_games:
        print(f"  No games with market data in last {days} days")
        return None

    # Index for Elo snapshot lookup
    game_index = {}
    for i, (h, a, hs, as_, ct) in enumerate(all_games):
        game_index[(h, a, ct)] = i

    # Get ML odds and run line odds using Odds API event_ids (mc_event_id)
    ml_odds_map = {}
    rl_odds_map = {}
    for g in backtest_games:
        mc_eid = g[17]  # mc.event_id (Odds API format)
        r_eid = g[0]    # results event_id (ESPN format)

        # ML odds
        rows = conn.execute("""
            SELECT selection, odds FROM odds
            WHERE event_id=? AND market='h2h'
            AND snapshot_date = (SELECT MAX(snapshot_date) FROM odds WHERE event_id=? AND market='h2h')
        """, (mc_eid, mc_eid)).fetchall()
        ml_odds_map[r_eid] = {}
        for sel, o in rows:
            ml_odds_map[r_eid][sel] = o

        # Run line odds
        rows = conn.execute("""
            SELECT selection, odds, line FROM odds
            WHERE event_id=? AND market='spreads'
            AND snapshot_date = (SELECT MAX(snapshot_date) FROM odds WHERE event_id=? AND market='spreads')
        """, (mc_eid, mc_eid)).fetchall()
        rl_odds_map[r_eid] = {}
        for sel, o, pt in rows:
            rl_odds_map[r_eid][sel] = {'odds': o, 'point': pt}

    # -- Evaluate each game --
    ml_picks = []
    rl_picks = []  # Run line picks
    spread_picks = []
    total_picks = []
    context_impact = {'boosted': 0, 'blocked': 0, 'neutral': 0}

    spe = cfg['spread_per_elo']
    ha = cfg['home_advantage']
    scfg = SPORT_CONFIG.get(SPORT, {})

    for g in backtest_games:
        eid, home, away = g[0], g[1], g[2]
        h_score, a_score = g[3], g[4]
        commence = g[5]
        closing_spread, closing_total = g[6], g[7]
        mkt_hs, mkt_hs_odds = g[8], g[9]
        mkt_as, mkt_as_odds = g[10], g[11]
        mkt_total, mkt_over_odds, mkt_under_odds = g[12], g[13], g[14]
        mc_home_ml, mc_away_ml = g[15], g[16]
        margin = h_score - a_score
        actual_total = h_score + a_score

        # Get Elo state BEFORE this game
        idx = game_index.get((home, away, commence))
        if idx is None or idx >= len(history):
            continue

        snap = history[idx]
        h_elo = snap['elos'].get(home, cfg['initial_elo'])
        a_elo = snap['elos'].get(away, cfg['initial_elo'])
        h_games = snap['counts'].get(home, 0)
        a_games = snap['counts'].get(away, 0)

        # Skip cold-start teams
        if min(h_games, a_games) < min_games:
            continue

        # Elo spread (negative = home favored)
        elo_spread = (a_elo - (h_elo + ha)) / spe
        conf_w = min(1.0, min(h_games, a_games) / 15.0)

        # Context adjustments
        ctx_h_adj, ctx_a_adj, ctx_notes = (0.0, 0.0, [])
        if use_context:
            ctx_h_adj, ctx_a_adj, ctx_notes = _compute_context_adjustment(
                conn, home, away, commence, SPORT)

        # === ML EVALUATION ===
        ml_data = ml_odds_map.get(eid, {})
        home_ml = ml_data.get(home) or mc_home_ml
        away_ml = ml_data.get(away) or mc_away_ml

        if home_ml and away_ml:
            # Elo win probability
            elo_diff = (h_elo + ha) - a_elo
            home_prob = 1.0 / (1.0 + 10 ** (-elo_diff / 400.0))
            away_prob = 1.0 - home_prob

            # Apply context
            home_prob_adj = min(0.95, max(0.05, home_prob + ctx_h_adj - ctx_a_adj))
            away_prob_adj = 1.0 - home_prob_adj

            # De-vig market odds (2-way for baseball)
            h_fair, a_fair, _ = devig_ml_odds(home_ml, away_ml)

            if h_fair and a_fair:
                # Home ML
                h_edge_raw = (home_prob - h_fair) * 100 * conf_w
                h_edge = (home_prob_adj - h_fair) * 100 * conf_w

                # Block heavy favorites and long dogs (match live filters)
                home_ml_ok = -300 <= home_ml <= 250
                if h_edge >= min_edge and home_ml_ok:
                    won = margin > 0
                    payout = (home_ml / 100.0) if home_ml > 0 else (100.0 / abs(home_ml))
                    units = kelly_units(h_edge, home_ml)
                    pnl = units * payout if won else -units
                    pick_data = {
                        'game': f"{home} vs {away}", 'date': commence[:10],
                        'pick': f"{home} ML ({home_ml:+.0f})",
                        'model_prob': home_prob_adj, 'fair_prob': h_fair,
                        'edge': round(h_edge, 1), 'edge_raw': round(h_edge_raw, 1),
                        'won': won, 'units': units, 'pnl': round(pnl, 2),
                        'score': f"{h_score}-{a_score}",
                        'elo_spread': round(elo_spread, 2),
                        'context': ctx_notes,
                    }
                    ml_picks.append(pick_data)
                    if ctx_notes:
                        if h_edge >= min_edge and h_edge_raw < min_edge:
                            context_impact['boosted'] += 1
                        elif h_edge < min_edge and h_edge_raw >= min_edge:
                            context_impact['blocked'] += 1
                        else:
                            context_impact['neutral'] += 1

                # Away ML
                a_edge_raw = (away_prob - a_fair) * 100 * conf_w
                a_edge = (away_prob_adj - a_fair) * 100 * conf_w

                away_ml_ok = -300 <= away_ml <= 250
                if a_edge >= min_edge and away_ml_ok:
                    won = margin < 0
                    payout = (away_ml / 100.0) if away_ml > 0 else (100.0 / abs(away_ml))
                    units = kelly_units(a_edge, away_ml)
                    pnl = units * payout if won else -units
                    pick_data = {
                        'game': f"{home} vs {away}", 'date': commence[:10],
                        'pick': f"{away} ML ({away_ml:+.0f})",
                        'model_prob': away_prob_adj, 'fair_prob': a_fair,
                        'edge': round(a_edge, 1), 'edge_raw': round(a_edge_raw, 1),
                        'won': won, 'units': units, 'pnl': round(pnl, 2),
                        'score': f"{h_score}-{a_score}",
                        'elo_spread': round(elo_spread, 2),
                        'context': ctx_notes,
                    }
                    ml_picks.append(pick_data)

            # === RUN LINE EVALUATION (-1.5 / +1.5) ===
            rl_data = rl_odds_map.get(eid, {})
            h_rl = rl_data.get(home, {})
            a_rl = rl_data.get(away, {})

            if h_rl and a_rl and h_fair and a_fair:
                h_rl_odds = h_rl.get('odds')
                h_rl_point = h_rl.get('point')  # -1.5 for fav
                a_rl_odds = a_rl.get('odds')
                a_rl_point = a_rl.get('point')  # +1.5 for dog

                if h_rl_odds and a_rl_odds and h_rl_point is not None:
                    # Home -1.5: needs to win by 2+
                    if h_rl_point == -1.5:
                        h_rl_cover_prob = home_prob_adj * WIN_BY_2_PCT
                        h_rl_fair = american_to_implied_prob(h_rl_odds) if h_rl_odds else None
                        if h_rl_fair and h_rl_fair > 0:
                            h_rl_edge = (h_rl_cover_prob - h_rl_fair) * 100 * conf_w
                            if h_rl_edge >= min_edge:
                                won = margin >= 2
                                payout = (h_rl_odds / 100.0) if h_rl_odds > 0 else (100.0 / abs(h_rl_odds))
                                units = kelly_units(h_rl_edge, h_rl_odds)
                                pnl = units * payout if won else -units
                                rl_picks.append({
                                    'game': f"{home} vs {away}", 'date': commence[:10],
                                    'pick': f"{home} -1.5 ({h_rl_odds:+.0f})",
                                    'model_prob': h_rl_cover_prob, 'fair_prob': h_rl_fair,
                                    'edge': round(h_rl_edge, 1),
                                    'won': won, 'units': units, 'pnl': round(pnl, 2),
                                    'score': f"{h_score}-{a_score}",
                                    'context': ctx_notes,
                                })

                    # Away +1.5: wins or loses by 1
                    if a_rl_point == 1.5:
                        a_rl_cover_prob = away_prob_adj + (home_prob_adj * LOSE_BY_1_PCT)
                        a_rl_fair = american_to_implied_prob(a_rl_odds) if a_rl_odds else None
                        if a_rl_fair and a_rl_fair > 0:
                            a_rl_edge = (a_rl_cover_prob - a_rl_fair) * 100 * conf_w
                            if a_rl_edge >= min_edge:
                                won = margin <= 1  # Away covers +1.5 if margin <= 1
                                payout = (a_rl_odds / 100.0) if a_rl_odds > 0 else (100.0 / abs(a_rl_odds))
                                units = kelly_units(a_rl_edge, a_rl_odds)
                                pnl = units * payout if won else -units
                                rl_picks.append({
                                    'game': f"{home} vs {away}", 'date': commence[:10],
                                    'pick': f"{away} +1.5 ({a_rl_odds:+.0f})",
                                    'model_prob': a_rl_cover_prob, 'fair_prob': a_rl_fair,
                                    'edge': round(a_rl_edge, 1),
                                    'won': won, 'units': units, 'pnl': round(pnl, 2),
                                    'score': f"{h_score}-{a_score}",
                                    'context': ctx_notes,
                                })

        # === SPREAD EVALUATION (ATS) ===
        use_spread = closing_spread if closing_spread is not None else mkt_hs
        if use_spread is not None:
            std = scfg.get('spread_std', 10.0)
            # Context adjusts model spread slightly
            adj_elo_spread = elo_spread
            if use_context:
                adj_elo_spread -= (ctx_h_adj - ctx_a_adj) * spe  # Convert prob adj to spread

            home_cover_prob = _ncdf((use_spread - adj_elo_spread) / std)
            implied_cover = 0.52  # -110 vig
            home_spread_edge = (home_cover_prob - implied_cover) * 100
            away_spread_edge = ((1.0 - home_cover_prob) - implied_cover) * 100

            home_covered = (margin + use_spread) > 0
            away_covered = (margin + use_spread) < 0
            push = (margin + use_spread) == 0

            if home_spread_edge >= min_edge:
                units = kelly_units(home_spread_edge, -110)
                pnl = units * 0.909 if home_covered else (-units if not push else 0)
                spread_picks.append({
                    'game': f"{home} vs {away}", 'date': commence[:10],
                    'pick': f"{home} {use_spread:+.1f}",
                    'edge': round(home_spread_edge, 1),
                    'model_spread': round(adj_elo_spread, 2),
                    'market_spread': use_spread,
                    'covered': home_covered, 'push': push,
                    'units': units, 'pnl': round(pnl, 2),
                    'score': f"{h_score}-{a_score}",
                    'context': ctx_notes,
                })

            if away_spread_edge >= min_edge:
                units = kelly_units(away_spread_edge, -110)
                pnl = units * 0.909 if away_covered else (-units if not push else 0)
                spread_picks.append({
                    'game': f"{home} vs {away}", 'date': commence[:10],
                    'pick': f"{away} {-use_spread:+.1f}",
                    'edge': round(away_spread_edge, 1),
                    'model_spread': round(adj_elo_spread, 2),
                    'market_spread': use_spread,
                    'covered': away_covered, 'push': push,
                    'units': units, 'pnl': round(pnl, 2),
                    'score': f"{h_score}-{a_score}",
                    'context': ctx_notes,
                })

        # === TOTALS EVALUATION ===
        use_total = closing_total if closing_total is not None else mkt_total
        if use_total is not None:
            league_row = conn.execute("""
                SELECT AVG(actual_total), COUNT(*) FROM results
                WHERE sport=? AND completed=1 AND actual_total IS NOT NULL
                AND commence_time < ?
            """, (SPORT, commence)).fetchone()
            league_avg = league_row[0] if league_row and league_row[0] and league_row[1] >= 20 else None

            if league_avg:
                h_atk_row = conn.execute("""
                    SELECT AVG(CASE WHEN home=? THEN home_score ELSE away_score END), COUNT(*)
                    FROM results WHERE (home=? OR away=?) AND sport=? AND completed=1
                    AND commence_time < ?
                """, (home, home, home, SPORT, commence)).fetchone()
                a_atk_row = conn.execute("""
                    SELECT AVG(CASE WHEN home=? THEN home_score ELSE away_score END), COUNT(*)
                    FROM results WHERE (home=? OR away=?) AND sport=? AND completed=1
                    AND commence_time < ?
                """, (away, away, away, SPORT, commence)).fetchone()
                h_def_row = conn.execute("""
                    SELECT AVG(CASE WHEN home=? THEN away_score ELSE home_score END)
                    FROM results WHERE (home=? OR away=?) AND sport=? AND completed=1
                    AND commence_time < ?
                """, (home, home, home, SPORT, commence)).fetchone()
                a_def_row = conn.execute("""
                    SELECT AVG(CASE WHEN home=? THEN away_score ELSE home_score END)
                    FROM results WHERE (home=? OR away=?) AND sport=? AND completed=1
                    AND commence_time < ?
                """, (away, away, away, SPORT, commence)).fetchone()

                h_atk = h_atk_row[0] if h_atk_row and h_atk_row[0] and h_atk_row[1] >= 6 else None
                a_atk = a_atk_row[0] if a_atk_row and a_atk_row[0] and a_atk_row[1] >= 6 else None
                h_def = h_def_row[0] if h_def_row and h_def_row[0] else None
                a_def = a_def_row[0] if a_def_row and a_def_row[0] else None

                if h_atk and a_atk and h_def and a_def:
                    league_atk = league_avg / 2
                    total_dev = ((h_atk - league_atk) + (a_atk - league_atk) +
                                 (h_def - league_atk) + (a_def - league_atk)) / 2
                    model_total = use_total + (total_dev * 0.6)

                    total_diff = model_total - use_total
                    total_std = TOTAL_STD.get(SPORT, 5.0)
                    if total_diff > 0:
                        over_prob = _ncdf(total_diff / total_std)
                        over_edge = (over_prob - 0.52) * 100
                        if over_edge >= min_edge:
                            won = actual_total > use_total
                            push_t = actual_total == use_total
                            units = kelly_units(over_edge, -110)
                            pnl = units * 0.909 if won else (-units if not push_t else 0)
                            total_picks.append({
                                'game': f"{home} vs {away}", 'date': commence[:10],
                                'pick': f"OVER {use_total}",
                                'edge': round(over_edge, 1),
                                'model_total': round(model_total, 2),
                                'market_total': use_total,
                                'actual_total': actual_total,
                                'won': won, 'push': push_t,
                                'units': units, 'pnl': round(pnl, 2),
                                'score': f"{h_score}-{a_score}",
                            })
                    elif total_diff < 0:
                        under_prob = _ncdf(-total_diff / total_std)
                        under_edge = (under_prob - 0.52) * 100
                        if under_edge >= min_edge:
                            won = actual_total < use_total
                            push_t = actual_total == use_total
                            units = kelly_units(under_edge, -110)
                            pnl = units * 0.909 if won else (-units if not push_t else 0)
                            total_picks.append({
                                'game': f"{home} vs {away}", 'date': commence[:10],
                                'pick': f"UNDER {use_total}",
                                'edge': round(under_edge, 1),
                                'model_total': round(model_total, 2),
                                'market_total': use_total,
                                'actual_total': actual_total,
                                'won': won, 'push': push_t,
                                'units': units, 'pnl': round(pnl, 2),
                                'score': f"{h_score}-{a_score}",
                            })

    # -- REPORT --
    if verbose:
        print(f"\n  {'='*70}")
        print(f"  COLLEGE BASEBALL BACKTEST — Last {days} days")
        print(f"  {len(all_games)} total games | {len(backtest_games)} with market data | min_edge={min_edge}%")
        if use_context:
            print(f"  Context: ON")
        print(f"  {'='*70}")

        _report_section("MONEYLINE", ml_picks, True)
        _report_section("RUN LINES (-1.5/+1.5)", rl_picks, True)
        _report_section("SPREADS (ATS)", spread_picks, True, is_spread=True)
        _report_section("TOTALS", total_picks, True, is_total=True)

        if use_context and any([context_impact['boosted'], context_impact['blocked']]):
            print(f"\n  CONTEXT IMPACT")
            print(f"  {'-'*50}")
            print(f"  Boosted (context pushed over threshold): {context_impact['boosted']}")
            print(f"  Blocked (context pulled below threshold): {context_impact['blocked']}")
            print(f"  Neutral (context present but didn't change pick): {context_impact['neutral']}")

        # Combined summary
        all_p = ml_picks + rl_picks + spread_picks + total_picks
        if all_p:
            total_pnl = sum(p['pnl'] for p in all_p)
            total_wagered = sum(p['units'] for p in all_p)
            wins = sum(1 for p in all_p if p.get('won') or p.get('covered'))
            losses = len(all_p) - wins - sum(1 for p in all_p if p.get('push'))
            print(f"\n  {'='*70}")
            print(f"  COMBINED: {wins}W-{losses}L | {total_pnl:+.1f}u on {total_wagered:.1f}u wagered", end='')
            if total_wagered > 0:
                print(f" ({total_pnl/total_wagered*100:+.1f}% ROI)")
            else:
                print()
            print(f"  {'='*70}")

        # -- DIAGNOSTICS --
        print(f"\n  {'='*70}")
        print(f"  DIAGNOSTICS & RECOMMENDATIONS")
        print(f"  {'='*70}")
        _diagnostics(ml_picks, rl_picks, spread_picks, total_picks)

    return {
        'games': len(backtest_games),
        'ml': ml_picks, 'rl': rl_picks,
        'spread': spread_picks, 'total': total_picks,
    }


def _diagnostics(ml_picks, rl_picks, spread_picks, total_picks):
    """Analyze results and flag issues with recommendations."""
    issues = []

    # ML diagnostics
    if ml_picks:
        ml_wins = sum(1 for p in ml_picks if p['won'])
        ml_total = len(ml_picks)
        ml_pnl = sum(p['pnl'] for p in ml_picks)
        ml_pct = ml_wins / ml_total if ml_total else 0

        # Check favorite vs dog performance
        fav_picks = [p for p in ml_picks if '-' in p['pick'].split('(')[1]]
        dog_picks = [p for p in ml_picks if '+' in p['pick'].split('(')[1]]
        fav_wins = sum(1 for p in fav_picks if p['won'])
        dog_wins = sum(1 for p in dog_picks if p['won'])
        fav_pnl = sum(p['pnl'] for p in fav_picks)
        dog_pnl = sum(p['pnl'] for p in dog_picks)

        if fav_picks:
            print(f"\n  ML Favorites: {fav_wins}W-{len(fav_picks)-fav_wins}L | {fav_pnl:+.1f}u ({fav_wins/len(fav_picks):.0%})")
        if dog_picks:
            print(f"  ML Underdogs: {dog_wins}W-{len(dog_picks)-dog_wins}L | {dog_pnl:+.1f}u ({dog_wins/len(dog_picks):.0%})")

        if ml_pct < 0.45 and ml_total >= 10:
            issues.append(f"  !! ML hit rate {ml_pct:.0%} is below breakeven. Consider raising min_edge or tightening dog caps.")
        if dog_picks and len(dog_picks) >= 5 and dog_wins / len(dog_picks) < 0.30:
            issues.append(f"  !! Underdog ML is {dog_wins}W-{len(dog_picks)-dog_wins}L ({dog_wins/len(dog_picks):.0%}). Consider disabling or capping at lower odds.")
        if fav_picks and len(fav_picks) >= 5 and fav_pnl > 0:
            issues.append(f"  OK Favorite ML is profitable ({fav_pnl:+.1f}u). This is the model's strength.")

        # Check edge calibration
        high_edge = [p for p in ml_picks if p['edge'] >= 10]
        low_edge = [p for p in ml_picks if p['edge'] < 10]
        if high_edge:
            he_wins = sum(1 for p in high_edge if p['won'])
            print(f"  ML High edge (10%+): {he_wins}W-{len(high_edge)-he_wins}L ({he_wins/len(high_edge):.0%})")
        if low_edge:
            le_wins = sum(1 for p in low_edge if p['won'])
            print(f"  ML Low edge (<10%): {le_wins}W-{len(low_edge)-le_wins}L ({le_wins/len(low_edge):.0%})")

    # Run line diagnostics
    if rl_picks:
        rl_wins = sum(1 for p in rl_picks if p['won'])
        rl_pnl = sum(p['pnl'] for p in rl_picks)
        minus_rl = [p for p in rl_picks if '-1.5' in p['pick']]
        plus_rl = [p for p in rl_picks if '+1.5' in p['pick']]

        if minus_rl:
            mw = sum(1 for p in minus_rl if p['won'])
            mp = sum(p['pnl'] for p in minus_rl)
            print(f"\n  RL -1.5 (fav): {mw}W-{len(minus_rl)-mw}L | {mp:+.1f}u")
        if plus_rl:
            pw = sum(1 for p in plus_rl if p['won'])
            pp = sum(p['pnl'] for p in plus_rl)
            print(f"  RL +1.5 (dog): {pw}W-{len(plus_rl)-pw}L | {pp:+.1f}u")

        if rl_picks and rl_pnl < -5 and len(rl_picks) >= 10:
            issues.append(f"  !! Run lines losing {rl_pnl:+.1f}u. WIN_BY_2_PCT ({WIN_BY_2_PCT}) may need calibration.")

    # Spread diagnostics
    if spread_picks:
        sp_wins = sum(1 for p in spread_picks if p['covered'])
        sp_pnl = sum(p['pnl'] for p in spread_picks)
        if sp_pnl < -5 and len(spread_picks) >= 10:
            issues.append(f"  !! Spreads losing {sp_pnl:+.1f}u. spread_std ({scfg.get('spread_std', 10.0)}) may be miscalibrated.")

    # Totals diagnostics
    if total_picks:
        t_wins = sum(1 for p in total_picks if p['won'])
        t_pnl = sum(p['pnl'] for p in total_picks)
        if t_pnl < -3 and len(total_picks) >= 5:
            issues.append(f"  !! Totals losing {t_pnl:+.1f}u. TOTAL_STD is {TOTAL_STD.get(SPORT, 5.0)} — may need adjustment.")
    else:
        issues.append(f"  -- No totals picks generated. TOTAL_STD={TOTAL_STD.get(SPORT, 5.0)} is very conservative.")

    if issues:
        print(f"\n  FLAGS:")
        for iss in issues:
            print(iss)
    else:
        print(f"\n  OK No major issues flagged.")


def _report_section(title, picks, verbose, is_spread=False, is_total=False):
    """Print a section of the backtest report."""
    if not verbose:
        return

    print(f"\n  {title}")
    print(f"  {'-'*50}")

    if not picks:
        print(f"  No picks at this edge threshold")
        return

    wins = sum(1 for p in picks if p.get('won') or (is_spread and p.get('covered')))
    pushes = sum(1 for p in picks if p.get('push', False))
    losses = len(picks) - wins - pushes
    total = wins + losses
    win_pct = wins / total if total > 0 else 0
    total_pnl = sum(p['pnl'] for p in picks)
    total_wagered = sum(p['units'] for p in picks)

    print(f"  Record: {wins}W-{losses}L-{pushes}P ({win_pct:.0%})")
    print(f"  P&L: {total_pnl:+.1f}u on {total_wagered:.1f}u wagered", end='')
    if total_wagered > 0:
        print(f" ({total_pnl/total_wagered*100:+.1f}% ROI)")
    else:
        print()

    # Edge buckets
    buckets = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0})
    for p in picks:
        if p.get('push'):
            continue
        e = p['edge']
        bk = '3-5%' if e < 5 else ('5-8%' if e < 8 else ('8-13%' if e < 13 else ('13-18%' if e < 18 else '18%+')))
        hit = p.get('won') or (is_spread and p.get('covered'))
        if hit:
            buckets[bk]['w'] += 1
        else:
            buckets[bk]['l'] += 1
        buckets[bk]['pnl'] += p['pnl']

    if buckets:
        print(f"\n  By edge size:")
        for bk in ['3-5%', '5-8%', '8-13%', '13-18%', '18%+']:
            if bk in buckets:
                b = buckets[bk]
                t = b['w'] + b['l']
                pct = b['w'] / t if t else 0
                print(f"    {bk:>6s}: {b['w']}W-{b['l']}L ({pct:.0%}) | {b['pnl']:+.1f}u")

    # Individual picks
    print(f"\n  Detail:")
    for p in picks:
        hit = p.get('won') or (is_spread and p.get('covered'))
        marker = 'W' if hit else ('P' if p.get('push') else 'L')
        line = f"    [{marker}] {p['date']} {p['pick']:35s} edge={p['edge']:5.1f}%"
        line += f" | {p['pnl']:+.1f}u | {p['score']}"
        if is_total:
            line += f" (model={p['model_total']:.1f} vs mkt={p['market_total']:.1f}, actual={p['actual_total']})"
        elif is_spread:
            line += f" (model={p['model_spread']:.2f} vs mkt={p['market_spread']:.1f})"
        else:
            line += f" (model={p['model_prob']:.0%} vs fair={p['fair_prob']:.0%})"
        if p.get('context'):
            line += f" [{', '.join(p['context'])}]"
        print(line)


scfg = SPORT_CONFIG.get(SPORT, {})


def main():
    import argparse
    parser = argparse.ArgumentParser(description='College baseball backtest')
    parser.add_argument('--days', type=int, default=30, help='Days to backtest')
    parser.add_argument('--min-edge', type=float, default=3.0, help='Minimum edge %%')
    parser.add_argument('--min-games', type=int, default=8, help='Min games per team')
    parser.add_argument('--context', action='store_true', help='Include context adjustments')
    parser.add_argument('--verbose', '-v', action='store_true', help='Extra detail')
    parser.add_argument('--all', action='store_true', help='Show all edges (min-edge=0)')
    args = parser.parse_args()

    if args.all:
        args.min_edge = 0.0

    conn = sqlite3.connect(DB_PATH)

    print("=" * 70)
    print(f"  COLLEGE BASEBALL BACKTEST")
    print(f"  Last {args.days} days | Min edge: {args.min_edge}% | Context: {'ON' if args.context else 'OFF'}")
    print("=" * 70)

    # Run without context first
    result = run_baseball_backtest(conn, days=args.days, min_edge=args.min_edge,
                                    min_games=args.min_games, use_context=False)

    # If --context flag, run again WITH context and compare
    if args.context and result:
        print(f"\n\n{'#'*70}")
        print(f"  RE-RUNNING WITH CONTEXT ADJUSTMENTS")
        print(f"{'#'*70}")
        result_ctx = run_baseball_backtest(conn, days=args.days, min_edge=args.min_edge,
                                            min_games=args.min_games, use_context=True)

        if result_ctx:
            # Compare
            base_pnl = sum(p['pnl'] for p in result['ml'] + result.get('rl', []) + result['spread'] + result['total'])
            ctx_pnl = sum(p['pnl'] for p in result_ctx['ml'] + result_ctx.get('rl', []) + result_ctx['spread'] + result_ctx['total'])
            print(f"\n  {'='*70}")
            print(f"  CONTEXT COMPARISON")
            print(f"  {'='*70}")
            print(f"  Without context: {base_pnl:+.1f}u")
            print(f"  With context:    {ctx_pnl:+.1f}u")
            print(f"  Delta:           {ctx_pnl - base_pnl:+.1f}u")

    conn.close()


if __name__ == '__main__':
    main()
