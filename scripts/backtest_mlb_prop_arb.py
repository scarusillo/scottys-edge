"""Backtest PROP_BOOK_ARB for MLB:
  - batter_hits (BetRivers posts, others don't)
  - pitcher_strikeouts (FD + BR both post)
  - batter_total_bases (BetRivers posts; approximated with hits + 3*HR)

For each historical event+player where a SHARP book and SOFT book posted
different lines, simulate the arb fire (bet soft side at the soft book on
the easier line) and grade against actual box_score outcome.

Test multiple thresholds to find the profitability sweet spot.
"""
import os, sqlite3, statistics
from collections import defaultdict

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
SHARP_BOOKS = {'FanDuel', 'BetRivers'}
SOFT_BOOKS = {'DraftKings', 'BetMGM', 'Caesars', 'Fanatics', 'ESPN BET'}
MIN_ODDS = -150  # favorite floor
MAX_PROP_ODDS = 140


def american_payout(odds):
    if odds > 0: return odds / 100.0
    return 100.0 / abs(odds)


def simulate_market(market, stat_key, line_aggregator, event_date_start='2026-04-01'):
    """
    Returns list of simulated arb-fire outcomes:
      (event_id, player, threshold_bucket, side, line, odds, actual, result, pnl)
    """
    conn = sqlite3.connect(DB)
    # Pull props + actuals
    rows = conn.execute(f"""
        SELECT DATE(commence_time), event_id, player, book, line, odds, side
        FROM prop_snapshots
        WHERE sport='baseball_mlb' AND market=?
          AND DATE(captured_at) >= ?
          AND book IN ({','.join('?' for _ in list(SHARP_BOOKS) + list(SOFT_BOOKS))})
    """, (market, event_date_start, *list(SHARP_BOOKS), *list(SOFT_BOOKS))).fetchall()
    # Group by (event, player) -> per-book lines
    grouped = defaultdict(lambda: {'sharp': defaultdict(list), 'soft': defaultdict(list), 'dates': set()})
    for dt, eid, player, book, line, odds, side in rows:
        key = (eid, player)
        if book in SHARP_BOOKS:
            grouped[key]['sharp'][book].append((line, odds, side))
        else:
            grouped[key]['soft'][book].append((line, odds, side))
        grouped[key]['dates'].add(dt)
    # Get actuals
    actuals = {}
    ar = conn.execute(f"""
        SELECT DATE(game_date), player, {line_aggregator} FROM box_scores
        WHERE sport='baseball_mlb' AND DATE(game_date) >= ?
        GROUP BY game_date, player
    """, (event_date_start,)).fetchall()
    for dt, player, val in ar:
        actuals[(dt, player)] = val

    fires = []
    for (eid, player), data in grouped.items():
        if not data['sharp'] or not data['soft']:
            continue
        # Median lines per book, then median across sharp/soft pools
        sharp_per_book = [statistics.median(l for l, _, _ in entries) for entries in data['sharp'].values() if entries]
        soft_per_book = [statistics.median(l for l, _, _ in entries) for entries in data['soft'].values() if entries]
        if not sharp_per_book or not soft_per_book:
            continue
        sharp_line = statistics.median(sharp_per_book)
        soft_line = statistics.median(soft_per_book)
        gap = sharp_line - soft_line  # positive = sharp is higher
        if gap == 0:
            continue
        # Determine which side to bet and where
        if gap > 0:
            # Sharp sees higher total → bet OVER at the soft book (easier number)
            side_to_bet = 'Over'
        else:
            # Sharp sees lower total → bet UNDER at the soft book (easier number)
            side_to_bet = 'Under'
        # Find the soft book with the most favorable line + odds on that side
        best_soft = None
        for bk, entries in data['soft'].items():
            for ln, od, sd in entries:
                if sd.lower() != side_to_bet.lower(): continue
                if od < MIN_ODDS or od > MAX_PROP_ODDS: continue
                # We want the most extreme (farthest from sharp) line
                # For OVER: lowest line. For UNDER: highest line.
                if best_soft is None: best_soft = (bk, ln, od)
                elif side_to_bet == 'Over' and ln < best_soft[1]: best_soft = (bk, ln, od)
                elif side_to_bet == 'Under' and ln > best_soft[1]: best_soft = (bk, ln, od)
        if best_soft is None:
            continue
        bet_book, bet_line, bet_odds = best_soft

        # Get actual outcome
        dt = max(data['dates'])  # last date (game date)
        actual = actuals.get((dt, player))
        if actual is None:
            continue
        # Grade
        if side_to_bet == 'Over':
            if actual > bet_line: result = 'WIN'
            elif actual < bet_line: result = 'LOSS'
            else: result = 'PUSH'
        else:
            if actual < bet_line: result = 'WIN'
            elif actual > bet_line: result = 'LOSS'
            else: result = 'PUSH'
        # PnL at 5u stake
        stake = 5.0
        if result == 'WIN': pnl = stake * american_payout(bet_odds)
        elif result == 'LOSS': pnl = -stake
        else: pnl = 0.0
        fires.append({
            'date': dt, 'eid': eid, 'player': player, 'gap': gap,
            'sharp_line': sharp_line, 'soft_line': soft_line,
            'bet_book': bet_book, 'bet_line': bet_line, 'bet_odds': bet_odds,
            'bet_side': side_to_bet, 'actual': actual, 'result': result, 'pnl': pnl,
        })
    return fires


def bucket_report(title, fires):
    print()
    print(f"=" * 88)
    print(f"{title}: {len(fires)} simulated fires")
    print(f"=" * 88)
    if not fires:
        print("  (no simulated fires)")
        return
    # Overall
    w = sum(1 for f in fires if f['result']=='WIN')
    l = sum(1 for f in fires if f['result']=='LOSS')
    p = sum(1 for f in fires if f['result']=='PUSH')
    total_pnl = sum(f['pnl'] for f in fires)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  OVERALL: {w}W-{l}L-{p}P | {wr:.1f}% | {total_pnl:+.2f}u")
    # By threshold bucket
    buckets = [('0.0-0.5', 0.0, 0.5), ('0.5-1.0', 0.5, 1.0), ('1.0-1.5', 1.0, 1.5),
               ('1.5-2.0', 1.5, 2.0), ('2.0+', 2.0, 999)]
    print()
    print(f"  {'Gap':<10} {'N':>4} {'W-L-P':>10} {'Win%':>6} {'P/L':>8}")
    for lbl, lo, hi in buckets:
        sub = [f for f in fires if lo <= abs(f['gap']) < hi]
        if not sub: continue
        sw = sum(1 for f in sub if f['result']=='WIN')
        sl = sum(1 for f in sub if f['result']=='LOSS')
        sp_ = sum(1 for f in sub if f['result']=='PUSH')
        spnl = sum(f['pnl'] for f in sub)
        swr = sw/(sw+sl)*100 if (sw+sl) else 0
        print(f"  {lbl:<10} {len(sub):>4} {sw}-{sl}-{sp_:<5} {swr:>5.1f}% {spnl:>+7.2f}u")


def main():
    print("Scanning MLB prop_snapshots + box_scores for simulated arb fires...\n")
    # 1. batter_hits
    fires_hits = simulate_market('batter_hits', 'hits', 'SUM(stat_value) FILTER (WHERE stat_type=\'hits\')')
    bucket_report("BATTER_HITS (BetRivers sharp; DK/BetMGM/Caesars/Fanatics soft)", fires_hits)

    # 2. pitcher_strikeouts
    fires_k = simulate_market('pitcher_strikeouts', 'pitcher_k',
                               "SUM(stat_value) FILTER (WHERE stat_type='pitcher_k')")
    bucket_report("PITCHER_STRIKEOUTS (FD+BR sharp; all soft books)", fires_k)

    # 3. batter_total_bases — approximate with hits + 3*HR (missing 2B/3B)
    # This is a lower bound; actual TB will be slightly higher.
    fires_tb = simulate_market('batter_total_bases', 'total_bases_approx',
        "SUM(CASE WHEN stat_type='hits' THEN stat_value ELSE 0 END) + "
        "3 * SUM(CASE WHEN stat_type='hr' THEN stat_value ELSE 0 END)")
    bucket_report("BATTER_TOTAL_BASES (approx, missing 2B/3B data)", fires_tb)

    # Sample fires
    print()
    print(f"=" * 88)
    print("SAMPLE FIRES — batter_hits at gap >= 1.0")
    print(f"=" * 88)
    print(f"  {'Date':<12} {'Player':<22} {'Sharp':>6} {'Soft':>6} {'Bet':<6} {'Line':>5} {'Odds':>5} {'Act':>3} {'Res':>4} {'P/L':>6}")
    for f in sorted(fires_hits, key=lambda x: -abs(x['gap']))[:12]:
        if abs(f['gap']) < 1.0: continue
        print(f"  {f['date']:<12} {f['player'][:21]:<22} {f['sharp_line']:>6.1f} {f['soft_line']:>6.1f} {f['bet_side']:<6} {f['bet_line']:>5.1f} {f['bet_odds']:>5.0f} {f['actual']:>3.0f} {f['result']:>4} {f['pnl']:>+5.1f}u")


if __name__ == '__main__':
    main()
