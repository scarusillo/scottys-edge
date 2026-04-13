"""
walk_forward.py — Walk-forward backtesting framework for Scotty's Edge.

Simulates model performance using rolling time windows where each test
period only uses data available BEFORE that period. Prevents overfitting
by ensuring no future data leaks into the evaluation.

Two modes:
  1. REPLAY: Re-evaluate the model's actual picks in rolling windows.
     "Would these picks have been profitable if we only knew what we
     knew at the time?"
  2. PARAMETER: Test a parameter change (e.g., prop edge floor 10% vs 20%)
     across rolling windows using historical odds + box score data.

Usage:
    python walk_forward.py replay --window 7 --step 7
    python walk_forward.py props --min-edge 10 --window 7
    python walk_forward.py props --min-edge 20 --window 7  # compare
"""
import sqlite3
import os
import sys
import math
from datetime import datetime, timedelta
from collections import defaultdict

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def american_to_implied(odds):
    if odds > 0: return 100.0 / (odds + 100.0)
    elif odds < 0: return abs(odds) / (abs(odds) + 100.0)
    return 0.5


def payout_per_unit(odds):
    if odds > 0: return odds / 100.0
    elif odds < 0: return 100.0 / abs(odds)
    return 1.0


# ═══════════════════════════════════════════════════════════════════
# MODE 1: REPLAY — Evaluate actual picks in rolling windows
# ═══════════════════════════════════════════════════════════════════

def replay_walk_forward(db_path=DB, window_days=7, step_days=7, min_units=3.5):
    """
    Split the season into rolling windows. For each window, show performance.
    This validates whether the model's edge is consistent across time periods
    or concentrated in lucky streaks.
    """
    conn = sqlite3.connect(db_path)

    # Get date range
    first = conn.execute("""
        SELECT MIN(DATE(created_at)) FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH') AND units >= ?
    """, (min_units,)).fetchone()[0]
    last = conn.execute("""
        SELECT MAX(DATE(created_at)) FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH') AND units >= ?
    """, (min_units,)).fetchone()[0]

    if not first or not last:
        print("No graded bets found.")
        conn.close()
        return

    start = datetime.strptime(first, '%Y-%m-%d')
    end = datetime.strptime(last, '%Y-%m-%d')

    print(f"WALK-FORWARD REPLAY — {window_days}d windows, {step_days}d steps")
    print(f"Season: {first} to {last}")
    print(f"{'='*80}\n")

    windows = []
    current = start
    cum_pnl = 0.0
    cum_w, cum_l = 0, 0

    while current + timedelta(days=window_days) <= end + timedelta(days=1):
        w_start = current.strftime('%Y-%m-%d')
        w_end = (current + timedelta(days=window_days - 1)).strftime('%Y-%m-%d')

        rows = conn.execute("""
            SELECT result, pnl_units, sport, market_type, clv
            FROM graded_bets
            WHERE DATE(created_at) BETWEEN ? AND ?
            AND result IN ('WIN','LOSS','PUSH') AND units >= ?
        """, (w_start, w_end, min_units)).fetchall()

        if rows:
            w = sum(1 for r in rows if r[0] == 'WIN')
            l = sum(1 for r in rows if r[0] == 'LOSS')
            pnl = sum(r[1] or 0 for r in rows)
            clvs = [r[4] for r in rows if r[4] is not None]
            avg_clv = sum(clvs) / len(clvs) if clvs else 0

            cum_pnl += pnl
            cum_w += w
            cum_l += l
            wp = 100 * w / (w + l) if (w + l) > 0 else 0

            windows.append({
                'start': w_start, 'end': w_end, 'bets': len(rows),
                'w': w, 'l': l, 'pnl': pnl, 'win_pct': wp,
                'avg_clv': avg_clv, 'cum_pnl': cum_pnl,
            })

        current += timedelta(days=step_days)

    # Print results
    print(f"{'Window':>21s} {'Bets':>5s} {'W-L':>7s} {'Win%':>6s} {'P&L':>8s} {'CLV':>6s} {'Cum P&L':>9s} {'Trend':>6s}")
    print(f"{'-'*75}")

    profitable_windows = 0
    for i, win in enumerate(windows):
        trend = "+" if win['pnl'] >= 0 else "-"
        profitable_windows += 1 if win['pnl'] >= 0 else 0
        print(f"  {win['start']} to {win['end']} {win['bets']:5d} {win['w']:3d}-{win['l']:3d} {win['win_pct']:5.1f}% {win['pnl']:+8.1f}u {win['avg_clv']:+6.2f} {win['cum_pnl']:+9.1f}u {trend:>6s}")

    # Summary
    total_windows = len(windows)
    print(f"\n{'='*75}")
    print(f"SUMMARY:")
    print(f"  Windows: {total_windows} | Profitable: {profitable_windows} ({100*profitable_windows/total_windows:.0f}%)" if total_windows else "  No windows")
    print(f"  Season: {cum_w}W-{cum_l}L | {cum_pnl:+.1f}u")

    # Consistency score: Sharpe-like ratio
    if len(windows) >= 3:
        pnls = [w['pnl'] for w in windows]
        mean_pnl = sum(pnls) / len(pnls)
        std_pnl = math.sqrt(sum((p - mean_pnl)**2 for p in pnls) / len(pnls))
        sharpe = mean_pnl / std_pnl if std_pnl > 0 else 0
        print(f"  Avg weekly P&L: {mean_pnl:+.1f}u | Std: {std_pnl:.1f}u | Sharpe: {sharpe:.2f}")
        print(f"  Sharpe > 0.5 = good consistency | > 1.0 = excellent | < 0 = losing")

    conn.close()
    return windows


# ═══════════════════════════════════════════════════════════════════
# MODE 2: PARAMETER TEST — Prop edge floor comparison
# ═══════════════════════════════════════════════════════════════════

STAT_MAP = {
    'batter_hits': 'hits', 'batter_rbis': 'rbi', 'batter_runs_scored': 'runs',
    'batter_home_runs': 'hr', 'batter_strikeouts': 'batter_k',
    'pitcher_strikeouts': 'pitcher_k', 'pitcher_hits_allowed': 'pitcher_h_allowed',
    'pitcher_earned_runs': 'pitcher_er', 'pitcher_outs': 'pitcher_outs',
}


def prop_walk_forward(db_path=DB, min_edge=10.0, max_odds=140, min_odds=-150,
                      window_days=3, train_days=0, max_picks_per_day=3):
    """
    Walk-forward test for prop model parameters.

    For each test window:
    1. Use box_scores from BEFORE the window to calculate hit rates
    2. Use prop_snapshots FROM the window to find edges
    3. Simulate picks using the parameter settings
    4. Grade against actual game results

    This is a TRUE out-of-sample test — hit rates are calculated from
    training data only, never peeking at the test period.
    """
    conn = sqlite3.connect(db_path)

    # Get date range from prop_snapshots
    first = conn.execute("SELECT MIN(DATE(captured_at)) FROM prop_snapshots WHERE sport = 'baseball_mlb'").fetchone()[0]
    last = conn.execute("SELECT MAX(DATE(captured_at)) FROM prop_snapshots WHERE sport = 'baseball_mlb'").fetchone()[0]

    if not first or not last:
        print("No prop snapshot data found.")
        conn.close()
        return

    start = datetime.strptime(first, '%Y-%m-%d') + timedelta(days=train_days)
    end = datetime.strptime(last, '%Y-%m-%d')

    print(f"WALK-FORWARD PROP TEST — edge floor {min_edge}%, odds [{min_odds} to +{max_odds}]")
    print(f"Data range: {first} to {last} | Training: {train_days}d | Window: {window_days}d")
    print(f"{'='*80}\n")

    windows = []
    current = start
    cum_pnl = 0.0
    cum_w, cum_l = 0, 0
    total_picks = 0

    while current + timedelta(days=window_days) <= end + timedelta(days=1):
        w_start = current.strftime('%Y-%m-%d')
        w_end = (current + timedelta(days=window_days - 1)).strftime('%Y-%m-%d')
        train_cutoff = current.strftime('%Y-%m-%d')

        # Get all unique (player, market, line, game_date) combos in this window
        # Join prop_snapshots with box_scores to get actual results
        props = conn.execute("""
            SELECT ps.player, ps.market, ps.line, ps.odds, DATE(ps.captured_at) as snap_date,
                   bs.stat_value as actual_value
            FROM prop_snapshots ps
            JOIN box_scores bs ON bs.player = ps.player
                AND bs.game_date = DATE(ps.captured_at)
                AND bs.sport = 'baseball_mlb'
            WHERE ps.sport = 'baseball_mlb' AND ps.side = 'Over'
            AND DATE(ps.captured_at) BETWEEN ? AND ?
            AND ps.odds BETWEEN ? AND ?
            AND bs.stat_type = (
                CASE ps.market
                    WHEN 'batter_hits' THEN 'hits'
                    WHEN 'batter_rbis' THEN 'rbi'
                    WHEN 'batter_runs_scored' THEN 'runs'
                    WHEN 'batter_home_runs' THEN 'hr'
                    WHEN 'batter_strikeouts' THEN 'batter_k'
                    WHEN 'pitcher_strikeouts' THEN 'pitcher_k'
                    WHEN 'pitcher_hits_allowed' THEN 'pitcher_h_allowed'
                    WHEN 'pitcher_earned_runs' THEN 'pitcher_er'
                    WHEN 'pitcher_outs' THEN 'pitcher_outs'
                    ELSE ps.market
                END
            )
            GROUP BY ps.player, ps.market, ps.line, DATE(ps.captured_at)
        """, (w_start, w_end, min_odds, max_odds)).fetchall()

        # Evaluate each prop using ONLY training data (before this window)
        daily_picks = defaultdict(list)

        for player, market, line, odds, snap_date, actual_value in props:
            stat_type = STAT_MAP.get(market)
            if not stat_type:
                continue

            # Get box score values BEFORE this window only (training data)
            values = conn.execute("""
                SELECT stat_value FROM box_scores
                WHERE player = ? AND stat_type = ? AND sport = 'baseball_mlb'
                AND game_date < ?
                ORDER BY game_date DESC LIMIT 40
            """, (player, stat_type, train_cutoff)).fetchall()

            values = [v[0] for v in values if v[0] is not None]
            if len(values) < 15:
                continue

            # Calculate hit rate from training data only
            recent20 = values[:20]
            hr20 = sum(1 for v in recent20 if v > line) / len(recent20)
            full = sum(1 for v in values if v > line) / len(values)
            blended = 0.5 * hr20 + 0.5 * full

            implied = american_to_implied(odds)
            if implied <= 0:
                continue

            edge = (blended - implied) * 100.0
            if edge < min_edge:
                continue

            # Hit rate must beat breakeven
            if blended < implied:
                continue

            won = actual_value > line
            wp = payout_per_unit(odds)
            pnl = wp * 5.0 if won else -5.0  # 5u bet

            daily_picks[snap_date].append({
                'player': player, 'stat': stat_type, 'line': line,
                'odds': odds, 'edge': edge, 'won': won, 'pnl': pnl,
                'hit_rate': blended, 'implied': implied,
            })

        # Apply daily pick cap (top N by edge per day)
        window_w, window_l, window_pnl = 0, 0, 0.0
        window_picks = 0
        for day, day_picks in sorted(daily_picks.items()):
            day_picks.sort(key=lambda x: x['edge'], reverse=True)
            for p in day_picks[:max_picks_per_day]:
                window_picks += 1
                if p['won']:
                    window_w += 1
                else:
                    window_l += 1
                window_pnl += p['pnl']

        if window_picks > 0:
            cum_pnl += window_pnl
            cum_w += window_w
            cum_l += window_l
            total_picks += window_picks
            wp = 100 * window_w / (window_w + window_l) if (window_w + window_l) > 0 else 0

            windows.append({
                'start': w_start, 'end': w_end, 'picks': window_picks,
                'w': window_w, 'l': window_l, 'pnl': window_pnl,
                'win_pct': wp, 'cum_pnl': cum_pnl,
            })

        current += timedelta(days=window_days)

    # Print results
    print(f"{'Window':>21s} {'Picks':>6s} {'W-L':>7s} {'Win%':>6s} {'P&L':>8s} {'Cum P&L':>9s}")
    print(f"{'-'*65}")

    profitable_windows = 0
    for win in windows:
        trend = "+" if win['pnl'] >= 0 else "-"
        profitable_windows += 1 if win['pnl'] >= 0 else 0
        print(f"  {win['start']} to {win['end']} {win['picks']:6d} {win['w']:3d}-{win['l']:3d} {win['win_pct']:5.1f}% {win['pnl']:+8.1f}u {win['cum_pnl']:+9.1f}u")

    total_windows = len(windows)
    print(f"\n{'='*65}")
    print(f"SUMMARY (edge floor {min_edge}%, odds [{min_odds} to +{max_odds}]):")
    if total_windows > 0:
        print(f"  Windows: {total_windows} | Profitable: {profitable_windows} ({100*profitable_windows/total_windows:.0f}%)")
        print(f"  Total: {cum_w}W-{cum_l}L | {cum_pnl:+.1f}u | {total_picks} picks")
        if cum_w + cum_l > 0:
            print(f"  Win rate: {100*cum_w/(cum_w+cum_l):.1f}% | ROI: {100*cum_pnl/(total_picks*5):.1f}%")

        if len(windows) >= 3:
            pnls = [w['pnl'] for w in windows]
            mean_pnl = sum(pnls) / len(pnls)
            std_pnl = math.sqrt(sum((p - mean_pnl)**2 for p in pnls) / len(pnls))
            sharpe = mean_pnl / std_pnl if std_pnl > 0 else 0
            print(f"  Sharpe: {sharpe:.2f} (>0.5 good, >1.0 excellent)")
    else:
        print("  No windows with qualifying picks.")

    conn.close()
    return windows


# ═══════════════════════════════════════════════════════════════════
# MODE 3: SPORT CONSISTENCY — Is each sport profitable across windows?
# ═══════════════════════════════════════════════════════════════════

def sport_walk_forward(db_path=DB, window_days=7, step_days=7, min_units=3.5):
    """
    Walk-forward by sport — shows if each sport is consistently profitable
    or just had one hot streak carrying the whole record.
    """
    conn = sqlite3.connect(db_path)

    sports = conn.execute("""
        SELECT DISTINCT sport FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH') AND units >= ?
        AND created_at >= '2026-04-01'
    """, (min_units,)).fetchall()

    print(f"WALK-FORWARD BY SPORT — {window_days}d windows (post-rebuild only)")
    print(f"{'='*80}\n")

    for (sport,) in sports:
        rows = conn.execute("""
            SELECT DATE(created_at) as dt, result, pnl_units
            FROM graded_bets
            WHERE sport = ? AND result IN ('WIN','LOSS','PUSH') AND units >= ?
            AND created_at >= '2026-04-01'
            ORDER BY created_at
        """, (sport, min_units)).fetchall()

        if len(rows) < 5:
            continue

        # Split into windows
        dates = sorted(set(r[0] for r in rows))
        if len(dates) < 2:
            continue

        start = datetime.strptime(dates[0], '%Y-%m-%d')
        end = datetime.strptime(dates[-1], '%Y-%m-%d')

        win_results = []
        current = start
        while current + timedelta(days=window_days) <= end + timedelta(days=1):
            w_start = current.strftime('%Y-%m-%d')
            w_end = (current + timedelta(days=window_days - 1)).strftime('%Y-%m-%d')

            w_rows = [r for r in rows if w_start <= r[0] <= w_end]
            if w_rows:
                w = sum(1 for r in w_rows if r[1] == 'WIN')
                l = sum(1 for r in w_rows if r[1] == 'LOSS')
                pnl = sum(r[2] or 0 for r in w_rows)
                win_results.append(pnl)

            current += timedelta(days=step_days)

        if not win_results:
            continue

        profitable = sum(1 for p in win_results if p >= 0)
        total = len(win_results)
        total_pnl = sum(win_results)
        mean = sum(win_results) / len(win_results)
        std = math.sqrt(sum((p - mean)**2 for p in win_results) / len(win_results)) if len(win_results) > 1 else 0
        sharpe = mean / std if std > 0 else 0

        label = sport.replace('baseball_', '').replace('icehockey_', '').replace('basketball_', '').replace('soccer_', '').upper()
        consistency = f"{profitable}/{total} windows profitable"
        print(f"  {label:25s} {total_pnl:+7.1f}u | {consistency:22s} | Sharpe: {sharpe:+.2f}")

    conn.close()


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python walk_forward.py replay [--window 7] [--step 7]")
        print("  python walk_forward.py props [--min-edge 10] [--max-odds 140] [--min-odds -150]")
        print("  python walk_forward.py sports")
        print("  python walk_forward.py compare  # compare prop edge 10% vs 20%")
        return

    mode = sys.argv[1]

    if mode == 'replay':
        window = int(_get_arg('--window', '7'))
        step = int(_get_arg('--step', '7'))
        replay_walk_forward(window_days=window, step_days=step)

    elif mode == 'props':
        min_edge = float(_get_arg('--min-edge', '10'))
        max_odds = int(_get_arg('--max-odds', '140'))
        min_odds = int(_get_arg('--min-odds', '-150'))
        prop_walk_forward(min_edge=min_edge, max_odds=max_odds, min_odds=min_odds)

    elif mode == 'sports':
        sport_walk_forward()

    elif mode == 'compare':
        print("COMPARING PROP EDGE FLOORS\n")
        print("=" * 80)
        print("  10% EDGE FLOOR (new)")
        print("=" * 80)
        prop_walk_forward(min_edge=10.0)
        print("\n\n")
        print("=" * 80)
        print("  20% EDGE FLOOR (old)")
        print("=" * 80)
        prop_walk_forward(min_edge=20.0)

    else:
        print(f"Unknown mode: {mode}")


def _get_arg(flag, default):
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


if __name__ == '__main__':
    main()
