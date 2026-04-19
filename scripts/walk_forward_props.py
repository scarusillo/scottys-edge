"""
walk_forward_props.py — Fast prop walk-forward backtester.

Pre-loads all data into memory, then runs the walk-forward in pure Python.
Avoids repeated heavy SQL joins on 1.1M+ row tables.

Usage:
    python walk_forward_props.py           # compare 10% vs 20% edge floors
    python walk_forward_props.py --edge 15 # test specific edge floor
"""
import sqlite3
import os
import sys
import math
from collections import defaultdict

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

STAT_MAP = {
    'batter_hits': 'hits', 'batter_rbis': 'rbi', 'batter_runs_scored': 'runs',
    'batter_home_runs': 'hr', 'batter_strikeouts': 'batter_k',
    'pitcher_strikeouts': 'pitcher_k', 'pitcher_hits_allowed': 'pitcher_h_allowed',
    'pitcher_earned_runs': 'pitcher_er', 'pitcher_outs': 'pitcher_outs',
}
REVERSE_STAT_MAP = {v: k for k, v in STAT_MAP.items()}


def american_to_implied(odds):
    if odds > 0: return 100.0 / (odds + 100.0)
    elif odds < 0: return abs(odds) / (abs(odds) + 100.0)
    return 0.5


def payout_per_unit(odds):
    if odds > 0: return odds / 100.0
    elif odds < 0: return 100.0 / abs(odds)
    return 1.0


def load_data(db_path=DB):
    """Pre-load all data into memory for fast iteration."""
    conn = sqlite3.connect(db_path)
    print("Loading data...")

    # Box scores: {(player, stat_type): [(game_date, value), ...]} sorted by date DESC
    box_raw = conn.execute("""
        SELECT player, stat_type, game_date, stat_value
        FROM box_scores WHERE sport = 'baseball_mlb'
        ORDER BY game_date DESC
    """).fetchall()
    box_scores = defaultdict(list)
    for player, stat, gdate, val in box_raw:
        if val is not None:
            box_scores[(player, stat)].append((gdate, val))
    print(f"  Box scores: {len(box_raw)} rows, {len(box_scores)} player-stat combos")

    # Prop snapshots: pre-aggregate to best odds per (player, market, line, date)
    # Take the best (lowest for minus, highest for plus) odds across books for each prop
    prop_raw = conn.execute("""
        SELECT player, market, line, DATE(captured_at) as snap_date,
               MIN(odds) as best_minus, MAX(odds) as best_plus,
               AVG(odds) as avg_odds
        FROM prop_snapshots
        WHERE sport = 'baseball_mlb' AND side = 'Over'
        GROUP BY player, market, line, DATE(captured_at)
    """).fetchall()

    # For each prop, pick the representative odds (median-like)
    props = []  # list of (player, market, line, date, odds)
    for player, market, line, snap_date, best_minus, best_plus, avg_odds in prop_raw:
        odds = avg_odds  # Use average across books as representative
        props.append((player, market, line, snap_date, odds))
    print(f"  Props: {len(props)} unique (player, market, line, date) combos")

    # Results: actual game outcomes by (player, stat_type, date)
    results = {}
    for (player, stat), games in box_scores.items():
        for gdate, val in games:
            results[(player, stat, gdate)] = val

    conn.close()
    return box_scores, props, results


def run_walk_forward(box_scores, props, results, min_edge=10.0, min_odds=-150,
                     max_odds=140, window_days=3, max_picks_per_day=3):
    """
    Run walk-forward test. For each day's props, calculate edges using only
    box score data from BEFORE that day.
    """
    # Group props by date
    props_by_date = defaultdict(list)
    for player, market, line, snap_date, odds in props:
        if odds < min_odds or odds > max_odds:
            continue
        props_by_date[snap_date].append((player, market, line, odds))

    dates = sorted(props_by_date.keys())
    if not dates:
        return []

    # Accumulate results in windows
    daily_results = []  # (date, picks, wins, losses, pnl)

    for date in dates:
        day_props = props_by_date[date]
        day_candidates = []

        for player, market, line, odds in day_props:
            stat_type = STAT_MAP.get(market)
            if not stat_type:
                continue

            # Training data: box scores BEFORE this date
            all_games = box_scores.get((player, stat_type), [])
            training = [(g, v) for g, v in all_games if g < date]

            if len(training) < 15:
                continue

            # Recent 20 + full season hit rates (from training only)
            recent20 = [v for _, v in training[:20]]
            all_vals = [v for _, v in training]

            hr20 = sum(1 for v in recent20 if v > line) / len(recent20)
            full = sum(1 for v in all_vals if v > line) / len(all_vals)
            blended = 0.5 * hr20 + 0.5 * full

            implied = american_to_implied(odds)
            if implied <= 0:
                continue

            edge = (blended - implied) * 100.0
            if edge < min_edge:
                continue

            if blended < implied:
                continue

            # Get actual result
            actual = results.get((player, stat_type, date))
            if actual is None:
                continue

            won = actual > line
            wp = payout_per_unit(odds)
            pnl = wp * 5.0 if won else -5.0

            day_candidates.append({
                'player': player, 'stat': stat_type, 'line': line,
                'odds': odds, 'edge': edge, 'won': won, 'pnl': pnl,
            })

        # Cap: top N by edge per day
        day_candidates.sort(key=lambda x: x['edge'], reverse=True)
        selected = day_candidates[:max_picks_per_day]

        if selected:
            w = sum(1 for p in selected if p['won'])
            l = len(selected) - w
            pnl = sum(p['pnl'] for p in selected)
            daily_results.append((date, len(selected), w, l, pnl))

    # Aggregate into windows
    if not daily_results:
        return []

    windows = []
    i = 0
    while i < len(daily_results):
        window_picks = 0
        window_w, window_l, window_pnl = 0, 0, 0.0
        w_start = daily_results[i][0]
        for j in range(window_days):
            if i + j >= len(daily_results):
                break
            _, picks, w, l, pnl = daily_results[i + j]
            window_picks += picks
            window_w += w
            window_l += l
            window_pnl += pnl
        w_end = daily_results[min(i + window_days - 1, len(daily_results) - 1)][0]
        windows.append({
            'start': w_start, 'end': w_end, 'picks': window_picks,
            'w': window_w, 'l': window_l, 'pnl': window_pnl,
        })
        i += window_days

    return windows


def print_results(windows, label=""):
    cum_pnl = 0.0
    cum_w, cum_l = 0, 0
    total_picks = 0
    profitable = 0

    print(f"\n{'Window':>21s} {'Picks':>6s} {'W-L':>7s} {'Win%':>6s} {'P&L':>8s} {'Cum P&L':>9s}")
    print(f"{'-'*65}")

    for win in windows:
        cum_pnl += win['pnl']
        cum_w += win['w']
        cum_l += win['l']
        total_picks += win['picks']
        if win['pnl'] >= 0:
            profitable += 1
        wp = 100 * win['w'] / (win['w'] + win['l']) if (win['w'] + win['l']) > 0 else 0
        print(f"  {win['start']} to {win['end']} {win['picks']:6d} {win['w']:3d}-{win['l']:3d} {wp:5.1f}% {win['pnl']:+8.1f}u {cum_pnl:+9.1f}u")

    print(f"\n{'='*65}")
    print(f"SUMMARY {label}:")
    if windows:
        print(f"  Windows: {len(windows)} | Profitable: {profitable} ({100*profitable/len(windows):.0f}%)")
        print(f"  Total: {cum_w}W-{cum_l}L | {cum_pnl:+.1f}u | {total_picks} picks")
        if cum_w + cum_l > 0:
            print(f"  Win rate: {100*cum_w/(cum_w+cum_l):.1f}% | ROI: {100*cum_pnl/(total_picks*5):.1f}%")
        if len(windows) >= 3:
            pnls = [w['pnl'] for w in windows]
            mean = sum(pnls) / len(pnls)
            std = math.sqrt(sum((p - mean)**2 for p in pnls) / len(pnls))
            sharpe = mean / std if std > 0 else 0
            print(f"  Sharpe: {sharpe:.2f}")


def main():
    edge_arg = None
    if '--edge' in sys.argv:
        idx = sys.argv.index('--edge')
        if idx + 1 < len(sys.argv):
            edge_arg = float(sys.argv[idx + 1])

    box_scores, props, results = load_data()

    if edge_arg:
        print(f"\n{'='*65}")
        print(f"  EDGE FLOOR: {edge_arg}%")
        print(f"{'='*65}")
        windows = run_walk_forward(box_scores, props, results, min_edge=edge_arg)
        print_results(windows, f"(edge {edge_arg}%)")
    else:
        # Compare multiple edge floors
        for edge in [5.0, 10.0, 15.0, 20.0]:
            print(f"\n{'='*65}")
            print(f"  EDGE FLOOR: {edge}%")
            print(f"{'='*65}")
            windows = run_walk_forward(box_scores, props, results, min_edge=edge)
            print_results(windows, f"(edge {edge}%)")


if __name__ == '__main__':
    main()
