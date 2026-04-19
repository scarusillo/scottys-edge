"""Analyze ALL bets this week - what makes winners different from losers."""
import sqlite3, os

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)

print("=" * 90)
print("  ALL BETS THIS WEEK - What Makes Winners Different From Losers")
print("=" * 90)

import sys as _sys
from datetime import datetime as _dt, timedelta as _td
_days = 30
for i, arg in enumerate(_sys.argv):
    if arg in ('--days', '-d') and i + 1 < len(_sys.argv):
        try: _days = int(_sys.argv[i + 1])
        except ValueError: pass
_cutoff = (_dt.now() - _td(days=_days)).strftime('%Y-%m-%d')
all_bets = conn.execute("""
    SELECT g.selection, g.sport, g.market_type, g.line, g.odds, g.edge_pct,
           g.confidence, g.units, g.result, g.pnl_units, g.clv, g.side_type,
           g.context_factors, g.timing, g.created_at, g.model_spread
    FROM graded_bets g WHERE g.created_at >= ? ORDER BY g.created_at
""", (_cutoff,)).fetchall()

wins = [b for b in all_bets if b[8] == 'WIN']
losses = [b for b in all_bets if b[8] == 'LOSS']
print(f"\n  {len(wins)}W-{len(losses)}L total\n")

# Compare characteristics
print("  -- WHAT SEPARATES WINNERS FROM LOSERS --\n")

avg_edge_w = sum(b[5] for b in wins) / len(wins) if wins else 0.0
avg_edge_l = sum(b[5] for b in losses) / len(losses) if losses else 0.0
higher = "Winners" if avg_edge_w > avg_edge_l else "LOSERS"
print(f"  Avg Edge:    Wins={avg_edge_w:.1f}%  Losses={avg_edge_l:.1f}%  ({higher} have higher edge)")

avg_units_w = sum(b[7] for b in wins) / len(wins) if wins else 0.0
avg_units_l = sum(b[7] for b in losses) / len(losses) if losses else 0.0
print(f"  Avg Units:   Wins={avg_units_w:.1f}u  Losses={avg_units_l:.1f}u")

clv_w = [b[10] for b in wins if b[10] is not None]
clv_l = [b[10] for b in losses if b[10] is not None]
avg_clv_w = sum(clv_w) / len(clv_w) if clv_w else 0
avg_clv_l = sum(clv_l) / len(clv_l) if clv_l else 0
print(f"  Avg CLV:     Wins={avg_clv_w:+.1f}  Losses={avg_clv_l:+.1f}")

# Timing
early_w = len([b for b in wins if b[13] == 'EARLY'])
late_w = len([b for b in wins if b[13] == 'LATE'])
early_l = len([b for b in losses if b[13] == 'EARLY'])
late_l = len([b for b in losses if b[13] == 'LATE'])
print(f"\n  Timing:")
if early_w + early_l > 0:
    print(f"    EARLY: {early_w}W-{early_l}L ({early_w/(early_w+early_l)*100:.0f}% WR)")
if late_w + late_l > 0:
    print(f"    LATE:  {late_w}W-{late_l}L ({late_w/(late_w+late_l)*100:.0f}% WR)")

# Side type
print(f"\n  Side Type:")
for side in ['DOG', 'FAVORITE', 'OVER', 'UNDER', 'PROP_OVER']:
    sw = len([b for b in wins if b[11] == side])
    sl = len([b for b in losses if b[11] == side])
    st = sw + sl
    if st >= 3:
        print(f"    {side:12s}: {sw}W-{sl}L ({sw/st*100:.0f}%)")

# Context factors
print("\n  -- CONTEXT FACTORS: Win Rate Per Factor (3+ appearances) --\n")
factor_results = {}
for b in all_bets:
    ctx = b[12]
    result = b[8]
    if ctx:
        for factor in ctx.split(' | '):
            key = factor.split('(')[0].strip() if '(' in factor else factor.strip()
            if key and len(key) > 2:
                if key not in factor_results:
                    factor_results[key] = [0, 0]
                if result == 'WIN':
                    factor_results[key][0] += 1
                elif result == 'LOSS':
                    factor_results[key][1] += 1

print(f"  {'Factor':40s} | {'W':>3s} | {'L':>3s} | {'WR':>5s} | Signal")
print(f"  {'-'*40} | {'-'*3} | {'-'*3} | {'-'*5} | {'-'*10}")
sorted_factors = sorted(factor_results.items(), key=lambda x: -(x[1][0]/(sum(x[1])) if sum(x[1]) > 2 else 0))
for k, v in sorted_factors:
    w, l = v
    t = w + l
    if t >= 3:
        wr = w / t * 100
        if wr >= 67 and t >= 5:
            signal = "STRONG"
        elif wr >= 60:
            signal = "GOOD"
        elif wr >= 50:
            signal = "NEUTRAL"
        elif wr >= 40:
            signal = "WEAK"
        else:
            signal = "TOXIC"
        print(f"  {k[:40]:40s} | {w:3d} | {l:3d} | {wr:4.0f}% | {signal}")

# Sport + market combos
print("\n  -- BEST SPORT/MARKET COMBOS (3+ bets) --\n")
combos = {}
for b in all_bets:
    sport = b[1].replace('basketball_', '').replace('icehockey_', '').replace('baseball_', '').replace('soccer_', '')
    mtype = b[2]
    key = f"{sport} {mtype}"
    if key not in combos:
        combos[key] = [0, 0, 0.0]
    if b[8] == 'WIN':
        combos[key][0] += 1
    elif b[8] == 'LOSS':
        combos[key][1] += 1
    combos[key][2] += b[9]

print(f"  {'Combo':35s} | {'Record':>10s} | {'WR':>5s} | {'PnL':>8s}")
print(f"  {'-'*35} | {'-'*10} | {'-'*5} | {'-'*8}")
for k, v in sorted(combos.items(), key=lambda x: -x[1][2]):
    w, l, pnl = v
    t = w + l
    if t >= 3:
        print(f"  {k:35s} | {w:2d}W-{l:2d}L    | {w/t*100:4.0f}% | {pnl:+7.1f}u")

# Edge calibration: are higher edges actually winning more?
print("\n  -- EDGE CALIBRATION: Does Higher Edge = More Wins? --\n")
for lo, hi, label in [(5, 12, '5-12%'), (12, 16, '12-16%'), (16, 20, '16-20%'), (20, 30, '20-30%'), (30, 100, '30%+')]:
    bucket = [b for b in all_bets if lo <= b[5] < hi]
    bw = len([b for b in bucket if b[8] == 'WIN'])
    bl = len([b for b in bucket if b[8] == 'LOSS'])
    bt = bw + bl
    if bt >= 3:
        pnl = sum(b[9] for b in bucket)
        print(f"  {label:8s}: {bw}W-{bl}L ({bw/bt*100:.0f}% WR) | {pnl:+.1f}u")

# What about odds ranges?
print("\n  -- ODDS CALIBRATION: Best Odds Range --\n")
for lo, hi, label in [(-200, -151, '-200 to -151'), (-150, -111, '-150 to -111'), (-110, -100, '-110 to -100'), (100, 130, '+100 to +130'), (131, 200, '+131 to +200'), (201, 500, '+201 to +500')]:
    bucket = [b for b in all_bets if lo <= b[4] <= hi]
    bw = len([b for b in bucket if b[8] == 'WIN'])
    bl = len([b for b in bucket if b[8] == 'LOSS'])
    bt = bw + bl
    if bt >= 3:
        pnl = sum(b[9] for b in bucket)
        print(f"  {label:16s}: {bw}W-{bl}L ({bw/bt*100:.0f}% WR) | {pnl:+.1f}u")

conn.close()
