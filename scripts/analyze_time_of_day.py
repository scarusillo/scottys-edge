"""Performance by sport x time-of-day.

Slices graded_bets (post-rebuild) by:
  - Sport
  - Hour-of-day bucket (early 6-8am, morning 9-11am, midday 12-2pm,
    afternoon 3-5pm, evening 6-8pm, late 9pm+)
  - Day-of-week (weekday vs weekend)

Flags:
  - Sport+window combos with >= 10 picks where P/L is meaningfully
    negative (worse than pure juice drag)
  - Sport+window combos with >= 10 picks where win rate >= 60%
    (structurally profitable zones to double down on)
"""
import os, sqlite3
from collections import defaultdict

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

BUCKETS = [
    ('early_06-08', 6, 9),     # 6am, 7am, 8am — pre-opener flow
    ('morning_09-11', 9, 12),  # 9am, 10am, 11am — post-opener flow
    ('midday_12-14', 12, 15),  # 12pm, 1pm, 2pm
    ('afternoon_15-17', 15, 18), # 3pm, 4pm, 5pm
    ('evening_18-20', 18, 21),   # 6pm, 7pm, 8pm
]


def bucket_for(hr):
    h = int(hr)
    for lbl, lo, hi in BUCKETS:
        if lo <= h < hi:
            return lbl
    return 'other'


def juice_expected_pl(n, win_rate=0.5, stake=5.0, odds=-110):
    """P/L expected purely from juice at given win rate.
    At -110 odds, break-even rate is ~52.4%. Below that, P/L is negative
    just from juice, independent of signal quality.
    """
    wins = n * win_rate
    losses = n - wins
    pay = 100.0 / abs(odds)
    return wins * stake * pay - losses * stake


def main():
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT sport, market_type, side_type,
               strftime('%H', created_at) hr,
               CASE strftime('%w', created_at)
                 WHEN '0' THEN 'weekend' WHEN '6' THEN 'weekend'
                 ELSE 'weekday' END dow,
               result, pnl_units, edge_pct
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-04-01'
          AND result IN ('WIN','LOSS','PUSH')
    """).fetchall()

    grid = defaultdict(lambda: {'W':0,'L':0,'P':0,'pnl':0.0,'n':0})
    sport_totals = defaultdict(lambda: {'W':0,'L':0,'P':0,'pnl':0.0,'n':0})
    bucket_totals = defaultdict(lambda: {'W':0,'L':0,'P':0,'pnl':0.0,'n':0})

    for sp, mt, st, hr, dow, res, pnl, ed in rows:
        b = bucket_for(hr)
        key = (sp, b)
        d = grid[key]
        d['n'] += 1
        d['pnl'] += pnl or 0
        if res == 'WIN': d['W'] += 1
        elif res == 'LOSS': d['L'] += 1
        elif res == 'PUSH': d['P'] += 1
        # aggregates
        sd = sport_totals[sp]; sd['n'] += 1; sd['pnl'] += pnl or 0
        if res == 'WIN': sd['W'] += 1
        elif res == 'LOSS': sd['L'] += 1
        elif res == 'PUSH': sd['P'] += 1
        bd = bucket_totals[b]; bd['n'] += 1; bd['pnl'] += pnl or 0
        if res == 'WIN': bd['W'] += 1
        elif res == 'LOSS': bd['L'] += 1
        elif res == 'PUSH': bd['P'] += 1

    # === GRID: Sport x Bucket ===
    print("=" * 92)
    print("GRID: Sport x Time-of-Day Bucket  (post-Apr-1)")
    print("=" * 92)
    sports_sorted = sorted(sport_totals.keys(), key=lambda s: -sport_totals[s]['pnl'])
    bucket_labels = [b[0] for b in BUCKETS]
    # Header
    hdr = f"  {'Sport':<28} " + " ".join(f"{b[:11]:>12}" for b in bucket_labels) + "  " + f"{'TOTAL':>10}"
    print(hdr)
    for sp in sports_sorted:
        row = f"  {sp:<28} "
        for b in bucket_labels:
            d = grid.get((sp, b))
            if not d or d['n'] == 0:
                row += "       —    "
            else:
                row += f" {d['n']:>3}/{d['pnl']:>+5.1f}u "
        total = sport_totals[sp]
        row += f"  {total['n']:>3}/{total['pnl']:>+5.1f}u"
        print(row)

    # Bucket totals
    print()
    print(f"  {'ALL SPORTS':<28} ", end="")
    grand_n = 0; grand_pnl = 0.0
    for b in bucket_labels:
        d = bucket_totals[b]
        print(f" {d['n']:>3}/{d['pnl']:>+5.1f}u ", end="")
        grand_n += d['n']; grand_pnl += d['pnl']
    print(f"  {grand_n:>3}/{grand_pnl:>+5.1f}u")

    # === HOT ZONES (>= 10 picks, positive P/L, win rate >= 58%) ===
    print()
    print("=" * 70)
    print("HOT ZONES (n>=10, win rate >=58%, P/L positive)")
    print("=" * 70)
    hot_any = False
    for (sp, b), d in sorted(grid.items(), key=lambda x: -x[1]['pnl']):
        if d['n'] < 10: continue
        wr = d['W'] / (d['W'] + d['L']) * 100 if (d['W'] + d['L']) else 0
        if wr >= 58 and d['pnl'] > 0:
            print(f"  {sp:<28} {b:<18} {d['n']:>3} picks | {d['W']}-{d['L']}-{d['P']} | {wr:.1f}% | {d['pnl']:+.1f}u")
            hot_any = True
    if not hot_any:
        print("  (none yet — look for n>=10 combos as samples grow)")

    # === COLD ZONES (>= 10 picks, P/L meaningfully negative) ===
    print()
    print("=" * 70)
    print("COLD ZONES (n>=10, P/L worse than expected from juice drag)")
    print("  Threshold: observed P/L at least 3u below break-even-win-rate expectation")
    print("=" * 70)
    cold_any = False
    for (sp, b), d in sorted(grid.items(), key=lambda x: x[1]['pnl']):
        if d['n'] < 10: continue
        wr = d['W'] / (d['W'] + d['L']) * 100 if (d['W'] + d['L']) else 0
        expected = juice_expected_pl(d['n'], win_rate=wr/100)
        # Only flag if significantly below juice-adjusted expectation
        if d['pnl'] < -5 and d['pnl'] < expected - 3:
            print(f"  {sp:<28} {b:<18} {d['n']:>3} picks | {d['W']}-{d['L']}-{d['P']} | {wr:.1f}% | {d['pnl']:+.1f}u  (below juice-adj exp by {d['pnl']-expected:+.1f}u)")
            cold_any = True
    if not cold_any:
        print("  (no statistically unusual cold zones at n>=10)")

    # === WEEKDAY VS WEEKEND per-sport ===
    print()
    print("=" * 70)
    print("WEEKDAY vs WEEKEND per sport (gross P/L)")
    print("=" * 70)
    dow_grid = defaultdict(lambda: defaultdict(lambda: {'W':0,'L':0,'pnl':0.0,'n':0}))
    for sp, mt, st, hr, dow, res, pnl, ed in rows:
        d = dow_grid[sp][dow]
        d['n'] += 1; d['pnl'] += pnl or 0
        if res == 'WIN': d['W'] += 1
        elif res == 'LOSS': d['L'] += 1
    print(f"  {'Sport':<28} {'Weekday':>20} {'Weekend':>20}")
    for sp in sorted(sport_totals.keys(), key=lambda s: -sport_totals[s]['pnl']):
        wd = dow_grid[sp].get('weekday', {'n':0,'W':0,'L':0,'pnl':0})
        we = dow_grid[sp].get('weekend', {'n':0,'W':0,'L':0,'pnl':0})
        wd_str = f"{wd['n']}/{wd['pnl']:+.1f}u" if wd['n'] else "—"
        we_str = f"{we['n']}/{we['pnl']:+.1f}u" if we['n'] else "—"
        print(f"  {sp:<28} {wd_str:>20} {we_str:>20}")


if __name__ == '__main__':
    main()
