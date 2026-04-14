"""Walk-forward backtest: What if we used steam signals to filter picks?"""
import sqlite3, re
from collections import defaultdict
from steam_engine import get_steam_signal

DB = '../data/betting_model.db'
conn = sqlite3.connect(DB)

# Get all graded bets post-rebuild with event_id from bets table
bets = conn.execute("""
    SELECT gb.id, gb.selection, gb.sport, gb.market_type, gb.side_type,
           gb.odds, gb.units, gb.result, gb.pnl_units, gb.line, gb.event_id
    FROM graded_bets gb
    WHERE gb.result IN ('WIN','LOSS') AND gb.units >= 3.5
    AND DATE(gb.created_at) >= '2026-03-04'
    AND gb.event_id IS NOT NULL
    ORDER BY gb.created_at
""").fetchall()

print(f"Bets to analyze: {len(bets)}\n")

# For each bet, compute what steam signal would have said
signal_counts = defaultdict(lambda: {'count': 0, 'w': 0, 'l': 0, 'pnl': 0.0})
signal_by_sport = defaultdict(lambda: defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0}))

sample_confirms = []
sample_opposes = []

for bet in bets:
    gid, sel, sport, mtype, side, odds, units, result, pnl, line, event_id = bet

    if not event_id or not line:
        continue

    # Map side to what steam_engine expects
    if mtype == 'TOTAL':
        steam_side = side  # OVER or UNDER
    elif mtype == 'SPREAD':
        steam_side = 'FAVORITE' if side in ('FAVORITE',) else 'DOG'
    else:
        steam_side = side  # ML team names not reliable here, skip

    if mtype == 'MONEYLINE':
        continue  # Skip ML for now

    signal, info = get_steam_signal(conn, sport, event_id, mtype, steam_side, line)

    signal_counts[signal]['count'] += 1
    if result == 'WIN':
        signal_counts[signal]['w'] += 1
        signal_by_sport[sport][signal]['w'] += 1
    else:
        signal_counts[signal]['l'] += 1
        signal_by_sport[sport][signal]['l'] += 1
    signal_counts[signal]['pnl'] += pnl
    signal_by_sport[sport][signal]['pnl'] += pnl

    if signal == 'SHARP_CONFIRMS' and len(sample_confirms) < 5:
        sample_confirms.append((sel, result, pnl, info))
    elif signal == 'SHARP_OPPOSES' and len(sample_opposes) < 5:
        sample_opposes.append((sel, result, pnl, info))

# Results by signal
print("=" * 80)
print(f"{'Signal':<20} {'Picks':<8} {'W-L':<10} {'Win%':<8} {'P/L':<12} {'ROI':<8}")
print("=" * 80)
total_all = sum(d['count'] for d in signal_counts.values())
for sig in ['SHARP_CONFIRMS', 'NO_MOVEMENT', 'SHARP_OPPOSES']:
    d = signal_counts[sig]
    if d['count'] == 0:
        continue
    wl = d['w'] + d['l']
    wr = (d['w'] / wl * 100) if wl > 0 else 0
    roi = (d['pnl'] / (wl * 5.0) * 100) if wl > 0 else 0
    print(f"{sig:<20} {d['count']:<8} {d['w']}W-{d['l']}L{'':<3} {wr:<6.1f}% {d['pnl']:>+7.1f}u    {roi:>+5.1f}%")

print(f"\n{'TOTAL':<20} {total_all}")

# Show samples
if sample_confirms:
    print(f"\n=== Sample SHARP_CONFIRMS ===")
    for sel, result, pnl, info in sample_confirms:
        mv = info.get('movement', info.get('max_shift_cents', '?'))
        print(f"  {result:4} {pnl:+5.1f}u  mv={mv}  {sel[:65]}")

if sample_opposes:
    print(f"\n=== Sample SHARP_OPPOSES ===")
    for sel, result, pnl, info in sample_opposes:
        mv = info.get('movement', info.get('max_shift_cents', '?'))
        print(f"  {result:4} {pnl:+5.1f}u  mv={mv}  {sel[:65]}")

# By sport
print(f"\n=== BY SPORT ===")
for sport in sorted(signal_by_sport.keys()):
    label = sport.replace('basketball_','').replace('icehockey_','').replace('baseball_','').replace('soccer_','').upper()
    total_picks = sum(d['w'] + d['l'] for d in signal_by_sport[sport].values())
    if total_picks < 5:
        continue
    print(f"\n{label}:")
    for sig in ['SHARP_CONFIRMS', 'NO_MOVEMENT', 'SHARP_OPPOSES']:
        d = signal_by_sport[sport].get(sig, {'w': 0, 'l': 0, 'pnl': 0})
        wl = d['w'] + d['l']
        if wl == 0:
            continue
        wr = (d['w'] / wl * 100) if wl > 0 else 0
        print(f"  {sig:<20} {d['w']}W-{d['l']}L{'':<3} {wr:<6.1f}% {d['pnl']:>+6.1f}u")

# Scenario: What if we BLOCKED all SHARP_OPPOSES picks?
print(f"\n{'=' * 80}")
print("SCENARIO: Block all SHARP_OPPOSES picks")
print(f"{'=' * 80}")
oppose = signal_counts['SHARP_OPPOSES']
kept = signal_counts['SHARP_CONFIRMS']['pnl'] + signal_counts['NO_MOVEMENT']['pnl']
blocked = oppose['pnl']
print(f"Blocked {oppose['count']} picks: {oppose['w']}W-{oppose['l']}L, {oppose['pnl']:+.1f}u")
print(f"Kept picks P/L: {kept:+.1f}u")
print(f"Original P/L (all picks): {kept + blocked:+.1f}u")
print(f"New P/L (after block): {kept:+.1f}u")
print(f"Net impact: {-blocked:+.1f}u (positive = blocking losers saved us)")

# Scenario: What if we SIZED UP on SHARP_CONFIRMS (5u -> 7u)?
print(f"\n{'=' * 80}")
print("SCENARIO: 1.4x stake on SHARP_CONFIRMS picks")
print(f"{'=' * 80}")
confirm = signal_counts['SHARP_CONFIRMS']
boost = confirm['pnl'] * 0.4  # 40% more units
print(f"Confirm picks: {confirm['w']}W-{confirm['l']}L, {confirm['pnl']:+.1f}u")
print(f"Boosted contribution: {boost:+.1f}u extra")
print(f"New total P/L: {(kept + blocked) + boost:+.1f}u (vs original {kept + blocked:+.1f}u)")

conn.close()
