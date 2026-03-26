"""
ncaab_deep_diagnostic.py — Find the ROOT CAUSE, not just the symptom

Part 1: Medium dogs — what is the model spread vs market spread?
  Is the model consistently wrong about how close the game should be?

Part 2: Context — which SPECIFIC factors are hurting in NCAAB?
  Is it all context or just certain adjustments?

Usage:
    python ncaab_deep_diagnostic.py
"""
import sqlite3
from datetime import datetime

conn = sqlite3.connect('../data/betting_model.db')

print("=" * 70)
print("  NCAAB DEEP DIAGNOSTIC — Finding the root cause")
print("=" * 70)

# ══════════════════════════════════════════════════════════════
# PART 1: MEDIUM DOGS — Model spread vs Market spread
# ══════════════════════════════════════════════════════════════

print("\n  ═══ PART 1: MEDIUM DOGS (spread +4 to +7.5) ═══")
print("  Question: Is the model spread consistently wrong for these games?\n")

med_dogs = conn.execute("""
    SELECT selection, result, pnl_units, model_spread, line, odds, 
           context_factors, created_at
    FROM graded_bets
    WHERE sport='basketball_ncaab' AND result NOT IN ('DUPLICATE','PENDING')
    AND DATE(created_at) >= '2026-03-04'
    AND spread_bucket = 'MED_DOG'
    ORDER BY created_at
""").fetchall()

print(f"  {len(med_dogs)} medium dog bets:\n")
print(f"  {'Selection':40s} {'Result':6s} {'P/L':>6s}  {'Model':>6s} {'Market':>6s} {'Gap':>5s}  Context")
print(f"  {'-'*40} {'-'*6} {'-'*6}  {'-'*6} {'-'*6} {'-'*5}  {'-'*30}")

total_gap = 0
for sel, result, pnl, model_sp, line, odds, ctx_factors, dt in med_dogs:
    gap = (model_sp - line) if model_sp and line else 0
    total_gap += abs(gap) if gap else 0
    ctx_str = ctx_factors[:40] if ctx_factors else ""
    ctx_adj_str = ""
    print(f"  {sel:40s} {result:6s} {pnl:+.1f}u  {model_sp or 0:+.1f}  {line or 0:+.1f}  {gap:+.1f}  {ctx_str}{ctx_adj_str}")

if med_dogs:
    avg_gap = total_gap / len(med_dogs)
    print(f"\n  Avg absolute gap between model and market: {avg_gap:.1f} points")

# Also show BIG DOGS for comparison
print("\n\n  ═══ COMPARISON: BIG DOGS (spread +8 and up) ═══")
print("  These are working at 9W-5L +16.4u — what's different?\n")

big_dogs = conn.execute("""
    SELECT selection, result, pnl_units, model_spread, line, odds,
           context_factors, created_at
    FROM graded_bets
    WHERE sport='basketball_ncaab' AND result NOT IN ('DUPLICATE','PENDING')
    AND DATE(created_at) >= '2026-03-04'
    AND spread_bucket = 'BIG_DOG'
    ORDER BY created_at
""").fetchall()

print(f"  {len(big_dogs)} big dog bets:\n")
print(f"  {'Selection':40s} {'Result':6s} {'P/L':>6s}  {'Model':>6s} {'Market':>6s} {'Gap':>5s}  Context")
print(f"  {'-'*40} {'-'*6} {'-'*6}  {'-'*6} {'-'*6} {'-'*5}  {'-'*30}")

for sel, result, pnl, model_sp, line, odds, ctx_factors, dt in big_dogs:
    gap = (model_sp - line) if model_sp and line else 0
    ctx_str = ctx_factors[:40] if ctx_factors else ""
    ctx_adj_str = ""
    print(f"  {sel:40s} {result:6s} {pnl:+.1f}u  {model_sp or 0:+.1f}  {line or 0:+.1f}  {gap:+.1f}  {ctx_str}{ctx_adj_str}")


# ══════════════════════════════════════════════════════════════
# PART 2: CONTEXT — Which specific factors are hurting?
# ══════════════════════════════════════════════════════════════

print("\n\n  ═══ PART 2: CONTEXT FACTORS IN NCAAB ═══")
print("  Question: Which specific adjustments are making picks worse?\n")

all_ncaab = conn.execute("""
    SELECT selection, result, pnl_units, context_factors,
           context_confirmed, model_spread, line, created_at
    FROM graded_bets
    WHERE sport='basketball_ncaab' AND result NOT IN ('DUPLICATE','PENDING')
    AND DATE(created_at) >= '2026-03-04'
    ORDER BY created_at
""").fetchall()

# Parse context factors and track performance per factor
factor_perf = {}
for sel, result, pnl, ctx_factors, ctx_confirmed, model_sp, line, dt in all_ncaab:
    if not ctx_factors:
        continue
    # Parse individual factors from the string
    factors = [f.strip() for f in ctx_factors.split('|') if f.strip()]
    for factor in factors:
        # Extract factor name (before the +/- number)
        parts = factor.split('(')
        factor_name = parts[0].strip()
        adj_val = 0
        if len(parts) > 1:
            try:
                adj_val = float(parts[1].replace(')', '').replace('+', ''))
            except:
                pass
        
        if factor_name not in factor_perf:
            factor_perf[factor_name] = {'W': 0, 'L': 0, 'pnl': 0, 'adj_total': 0, 'count': 0}
        if result == 'WIN':
            factor_perf[factor_name]['W'] += 1
        elif result == 'LOSS':
            factor_perf[factor_name]['L'] += 1
        factor_perf[factor_name]['pnl'] += (pnl or 0)
        factor_perf[factor_name]['adj_total'] += adj_val
        factor_perf[factor_name]['count'] += 1

print(f"  {'Factor':35s} {'Record':12s} {'P/L':>7s}  {'Avg Adj':>7s}")
print(f"  {'-'*35} {'-'*12} {'-'*7}  {'-'*7}")

for factor, d in sorted(factor_perf.items(), key=lambda x: x[1]['pnl']):
    t = d['W'] + d['L']
    if t == 0: continue
    wp = d['W'] / t * 100
    avg_adj = d['adj_total'] / d['count'] if d['count'] > 0 else 0
    print(f"  {factor:35s} {d['W']}W-{d['L']}L ({wp:.0f}%)  {d['pnl']:+.1f}u  {avg_adj:+.1f}")

# Show context adjustment SIZE vs outcome
print("\n\n  ═══ PART 3: CONTEXT ADJUSTMENT SIZE vs OUTCOME ═══")
print("  Are bigger adjustments making things worse?\n")

adj_buckets = {'No adj (0)': {'W':0,'L':0,'pnl':0}, 
               'Small (0.1-1.0)': {'W':0,'L':0,'pnl':0},
               'Medium (1.1-2.0)': {'W':0,'L':0,'pnl':0},
               'Large (2.0+)': {'W':0,'L':0,'pnl':0}}

for sel, result, pnl, ctx_factors, ctx_confirmed, model_sp, line, dt in all_ncaab:
    abs_adj = 0
    if abs_adj == 0:
        bucket = 'No adj (0)'
    elif abs_adj <= 1.0:
        bucket = 'Small (0.1-1.0)'
    elif abs_adj <= 2.0:
        bucket = 'Medium (1.1-2.0)'
    else:
        bucket = 'Large (2.0+)'
    
    if result == 'WIN': adj_buckets[bucket]['W'] += 1
    elif result == 'LOSS': adj_buckets[bucket]['L'] += 1
    adj_buckets[bucket]['pnl'] += (pnl or 0)

for bucket in ['No adj (0)', 'Small (0.1-1.0)', 'Medium (1.1-2.0)', 'Large (2.0+)']:
    d = adj_buckets[bucket]
    t = d['W'] + d['L']
    if t == 0: continue
    wp = d['W'] / t * 100
    print(f"  {bucket:20s}  {d['W']}W-{d['L']}L ({wp:.0f}%)  {d['pnl']:+.1f}u")


# Show March 4-8 vs March 9+ (before/after conf tournaments)
print("\n\n  ═══ PART 4: PRE-TOURNAMENT vs TOURNAMENT ═══")
print("  March 4-8 (regular season) vs March 9+ (conference tournaments)\n")

pre = {'W':0,'L':0,'pnl':0}
post = {'W':0,'L':0,'pnl':0}
for sel, result, pnl, ctx_factors, ctx_confirmed, model_sp, line, dt in all_ncaab:
    d = dt[:10] if dt else ''
    bucket = pre if d <= '2026-03-08' else post
    if result == 'WIN': bucket['W'] += 1
    elif result == 'LOSS': bucket['L'] += 1
    bucket['pnl'] += (pnl or 0)

t = pre['W'] + pre['L']
if t > 0:
    print(f"  March 4-8 (regular):     {pre['W']}W-{pre['L']}L ({pre['W']/t*100:.0f}%)  {pre['pnl']:+.1f}u")
t = post['W'] + post['L']
if t > 0:
    print(f"  March 9+ (tournament):   {post['W']}W-{post['L']}L ({post['W']/t*100:.0f}%)  {post['pnl']:+.1f}u")

conn.close()
print("\n" + "=" * 70)
