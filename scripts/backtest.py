"""
backtest.py — Measure model accuracy against historical results

Replays every game in the results table and asks:
  1. When Elo disagreed with the market, who was right?
  2. What would our P&L be if we'd bet every pick?
  3. Are the edges real or noise?

This is the #1 tool for proving (or disproving) the model works.

Usage:
    python backtest.py                     # Full backtest, all sports
    python backtest.py --sport nba         # NBA only
    python backtest.py --sport nhl         # NHL only
    python backtest.py --min-edge 3        # Only picks with 3%+ edge
"""
import sqlite3, os, math, sys
from datetime import datetime
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Import model components
from elo_engine import ELO_CONFIG, _expected_score, _mov_multiplier, get_elo_ratings
from model_engine import (
    SPORT_CONFIG, spread_to_cover_prob, spread_to_win_prob,
    american_to_implied_prob, _ncdf,
)
from scottys_edge import kelly_units, kelly_label


def _rebuild_elo_chronological(games, sport):
    """
    Rebuild Elo ratings game-by-game, making predictions BEFORE each update.
    This avoids look-ahead bias — we only use info available at game time.

    Returns list of prediction dicts with results.
    """
    cfg = ELO_CONFIG.get(sport)
    if not cfg:
        return []

    elos = defaultdict(lambda: cfg['initial_elo'])
    game_counts = defaultdict(int)
    predictions = []

    for home, away, h_score, a_score, margin, commence, closing_spread, closing_total in games:
        if margin is None:
            margin = (h_score or 0) - (a_score or 0)

        home_elo = elos[home] + cfg['home_advantage']
        away_elo = elos[away]

        # ── PREDICT (before seeing result) ──
        elo_spread = (away_elo - home_elo) / cfg['spread_per_elo']
        home_win_prob = _expected_score(home_elo, away_elo)

        # Does Elo disagree with the market?
        has_closing = closing_spread is not None
        if has_closing:
            spread_diff = elo_spread - closing_spread
            # Cover probability using calibrated spread_std
            std = SPORT_CONFIG.get(sport, {}).get('spread_std', 11.0)

            # Home spread cover: model says home is X better than market thinks
            home_cover_prob = _ncdf((closing_spread - elo_spread) / std)
            implied_cover = 0.52  # Standard -110 vig

            home_spread_edge = (home_cover_prob - implied_cover) * 100
            away_cover_prob = 1.0 - home_cover_prob
            away_spread_edge = (away_cover_prob - implied_cover) * 100

            # Did the home team cover?
            actual_home_margin = margin
            home_covered = (actual_home_margin + closing_spread) > 0
            away_covered = not home_covered and (actual_home_margin + closing_spread) != 0
            push = (actual_home_margin + closing_spread) == 0
        else:
            spread_diff = None
            home_spread_edge = 0
            away_spread_edge = 0
            home_covered = None
            away_covered = None
            push = None

        # Straight-up prediction
        predicted_home_win = home_win_prob > 0.5
        actual_home_win = margin > 0

        predictions.append({
            'home': home, 'away': away,
            'commence': commence,
            'elo_spread': round(elo_spread, 2),
            'closing_spread': closing_spread,
            'spread_diff': round(spread_diff, 2) if spread_diff is not None else None,
            'home_spread_edge': round(home_spread_edge, 1),
            'away_spread_edge': round(away_spread_edge, 1),
            'home_win_prob': round(home_win_prob, 3),
            'predicted_home_win': predicted_home_win,
            'actual_home_win': actual_home_win,
            'actual_margin': margin,
            'home_covered': home_covered,
            'away_covered': away_covered,
            'push': push,
            'closing_total': closing_total,
            'actual_total': (h_score or 0) + (a_score or 0),
            'home_games': game_counts[home],
            'away_games': game_counts[away],
        })

        # ── UPDATE (after seeing result) ──
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

    return predictions


def run_backtest(sport, min_edge=2.0, min_games=10, verbose=True):
    """
    Run full backtest for a sport.

    Only evaluates picks where:
    - Both teams have played min_games (cold start excluded)
    - Closing spread is available
    - Edge exceeds min_edge threshold
    """
    conn = sqlite3.connect(DB_PATH)

    games = conn.execute("""
        SELECT home, away, home_score, away_score, actual_margin,
               commence_time, closing_spread, closing_total
        FROM results
        WHERE sport=? AND completed=1
        AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY commence_time ASC
    """, (sport,)).fetchall()

    conn.close()

    if not games:
        if verbose:
            print(f"  ⚠ No results for {sport}")
        return None

    if verbose:
        print(f"\n  {'━'*60}")
        print(f"  {sport.upper()} BACKTEST")
        print(f"  {'━'*60}")
        print(f"  Total games: {len(games)}")

    predictions = _rebuild_elo_chronological(games, sport)

    # Filter to evaluable predictions
    evaluable = [p for p in predictions
                 if p['closing_spread'] is not None
                 and p['home_games'] >= min_games
                 and p['away_games'] >= min_games]

    if verbose:
        print(f"  Evaluable (both teams {min_games}+ games, has closing line): {len(evaluable)}")

    if not evaluable:
        if verbose:
            print(f"  ⚠ Not enough data with closing lines for backtest")
            print(f"     (Run: python historical_scores.py --odds-api to get closing lines)")
            # Fall back to straight-up win prediction
            _report_win_prediction(predictions, min_games, verbose)
        return None

    # ── ATS (Against the Spread) Analysis ──
    _report_ats(evaluable, min_edge, verbose)

    # ── Straight-Up Win Prediction ──
    _report_win_prediction(predictions, min_games, verbose)

    # ── Simulated P&L ──
    _report_pnl(evaluable, min_edge, verbose)

    return evaluable


def _report_win_prediction(predictions, min_games, verbose):
    """How well does Elo predict straight-up winners?"""
    qualified = [p for p in predictions
                 if p['home_games'] >= min_games
                 and p['away_games'] >= min_games
                 and p['actual_margin'] != 0]

    if not qualified:
        return

    correct = sum(1 for p in qualified if p['predicted_home_win'] == p['actual_home_win'])
    total = len(qualified)
    pct = correct / total if total else 0

    if verbose:
        print(f"\n  STRAIGHT-UP WIN PREDICTION")
        print(f"  ─────────────────────────")
        print(f"  Correct: {correct}/{total} ({pct:.1%})")

        # By confidence bucket
        buckets = defaultdict(lambda: {'correct': 0, 'total': 0})
        for p in qualified:
            conf = round(max(p['home_win_prob'], 1 - p['home_win_prob']), 1)
            bucket = f"{conf:.0%}"
            predicted_winner_won = p['predicted_home_win'] == p['actual_home_win']
            buckets[bucket]['total'] += 1
            if predicted_winner_won:
                buckets[bucket]['correct'] += 1

        print(f"\n  By confidence level:")
        for bucket in sorted(buckets.keys()):
            b = buckets[bucket]
            if b['total'] >= 5:
                pct = b['correct'] / b['total']
                print(f"    {bucket:>5s} confident: {b['correct']:3d}/{b['total']:3d} ({pct:.0%})")


def _report_ats(evaluable, min_edge, verbose):
    """How well does the model predict against the spread?"""
    # Picks where model had an edge
    home_picks = [p for p in evaluable if p['home_spread_edge'] >= min_edge]
    away_picks = [p for p in evaluable if p['away_spread_edge'] >= min_edge]
    all_picks = []

    for p in home_picks:
        all_picks.append({**p, 'side': 'home', 'edge': p['home_spread_edge'],
                          'covered': p['home_covered'], 'push': p['push']})
    for p in away_picks:
        all_picks.append({**p, 'side': 'away', 'edge': p['away_spread_edge'],
                          'covered': p['away_covered'], 'push': p['push']})

    if not all_picks:
        if verbose:
            print(f"\n  ATS: No picks at {min_edge}%+ edge threshold")
            # Try lower threshold
            for threshold in [1.0, 0.5]:
                h = [p for p in evaluable if p['home_spread_edge'] >= threshold]
                a = [p for p in evaluable if p['away_spread_edge'] >= threshold]
                count = len(h) + len(a)
                if count > 0:
                    print(f"    ({count} picks at {threshold}%+ edge)")
        return

    wins = sum(1 for p in all_picks if p['covered'] and not p['push'])
    losses = sum(1 for p in all_picks if not p['covered'] and not p['push'])
    pushes = sum(1 for p in all_picks if p['push'])
    total = wins + losses
    win_pct = wins / total if total else 0

    if verbose:
        print(f"\n  AGAINST THE SPREAD (edge ≥ {min_edge}%)")
        print(f"  ─────────────────────────────────────")
        print(f"  Record: {wins}W-{losses}L-{pushes}P ({win_pct:.1%})")
        print(f"  Picks evaluated: {len(all_picks)}")
        breakeven = 0.524  # At -110
        print(f"  Break-even at -110: 52.4%")
        if win_pct > breakeven:
            print(f"  ✅ PROFITABLE ({win_pct:.1%} > {breakeven:.1%})")
        else:
            print(f"  ❌ Below break-even ({win_pct:.1%} < {breakeven:.1%})")

        # By edge bucket
        edge_buckets = defaultdict(lambda: {'wins': 0, 'losses': 0})
        for p in all_picks:
            if p['push']:
                continue
            bucket = '2-5%' if p['edge'] < 5 else ('5-10%' if p['edge'] < 10 else '10%+')
            if p['covered']:
                edge_buckets[bucket]['wins'] += 1
            else:
                edge_buckets[bucket]['losses'] += 1

        if edge_buckets:
            print(f"\n  By edge size:")
            for bucket in ['2-5%', '5-10%', '10%+']:
                if bucket in edge_buckets:
                    b = edge_buckets[bucket]
                    total = b['wins'] + b['losses']
                    pct = b['wins'] / total if total else 0
                    print(f"    {bucket:>6s} edge: {b['wins']:3d}W-{b['losses']:3d}L ({pct:.0%})")


def _report_pnl(evaluable, min_edge, verbose):
    """Simulated P&L using Kelly sizing at -110."""
    home_picks = [p for p in evaluable if p['home_spread_edge'] >= min_edge]
    away_picks = [p for p in evaluable if p['away_spread_edge'] >= min_edge]

    total_wagered = 0.0
    total_profit = 0.0

    for p in home_picks:
        units = kelly_units(p['home_spread_edge'], -110)
        if units <= 0:
            continue
        total_wagered += units
        if p['push']:
            pass  # No P&L
        elif p['home_covered']:
            total_profit += units * 0.909  # Win at -110
        else:
            total_profit -= units

    for p in away_picks:
        units = kelly_units(p['away_spread_edge'], -110)
        if units <= 0:
            continue
        total_wagered += units
        if p['push']:
            pass
        elif p['away_covered']:
            total_profit += units * 0.909
        else:
            total_profit -= units

    if verbose and total_wagered > 0:
        roi = (total_profit / total_wagered) * 100
        print(f"\n  SIMULATED P&L (Kelly sizing at -110)")
        print(f"  ─────────────────────────────────────")
        print(f"  Total wagered: {total_wagered:.1f}u")
        print(f"  Net profit: {total_profit:+.1f}u")
        print(f"  ROI: {roi:+.1f}%")
        if total_profit > 0:
            print(f"  ✅ Positive expected value")
        else:
            print(f"  ❌ Negative — model edges not confirmed")


def run_all_backtests(sports=None, min_edge=2.0, min_games=10):
    """Run backtests for all sports."""
    if sports is None:
        sports = list(ELO_CONFIG.keys())

    print("=" * 60)
    print("  BACKTEST — How accurate is the model?")
    print(f"  Min edge: {min_edge}% | Min games per team: {min_games}")
    print("=" * 60)

    for sport in sports:
        run_backtest(sport, min_edge=min_edge, min_games=min_games)

    print(f"\n{'='*60}")
    print(f"  NOTE: ATS results require closing lines in the results table.")
    print(f"  If you see 'no closing lines', run:")
    print(f"    python historical_scores.py --odds-api --odds-days 45")
    print(f"  This costs ~4,050 API credits but enables full ATS backtesting.")
    print(f"{'='*60}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Backtest model accuracy')
    parser.add_argument('--sport', type=str, help='Sport to test')
    parser.add_argument('--min-edge', type=float, default=2.0, help='Minimum edge %% for picks')
    parser.add_argument('--min-games', type=int, default=10, help='Min games per team before evaluating')
    args = parser.parse_args()

    sports = None
    if args.sport:
        short_map = {
            'nba': 'basketball_nba', 'ncaab': 'basketball_ncaab',
            'nhl': 'icehockey_nhl', 'epl': 'soccer_epl',
            'seriea': 'soccer_italy_serie_a', 'liga': 'soccer_spain_la_liga',
        }
        sports = [short_map.get(args.sport, args.sport)]

    run_all_backtests(sports=sports, min_edge=args.min_edge, min_games=args.min_games)
