"""
ncaab_diagnostic.py — Deep dive into NCAAB performance

Breaks down NCAAB losses by:
  1. Elo confidence (HIGH vs MEDIUM vs LOW)
  2. Bet type (Spread vs ML)
  3. Spread bucket (small/med/big dog, small/med fav)
  4. Date range (early March vs conference tournament week)
  5. Context confirmed vs raw model
  6. Individual picks with Elo confidence of both teams

Usage:
    python ncaab_diagnostic.py
"""
import sqlite3
from datetime import datetime

conn = sqlite3.connect('../data/betting_model.db')

# Get all NCAAB graded bets since March 4
bets = conn.execute("""
    SELECT selection, result, pnl_units, market_type, side_type, 
           spread_bucket, context_confirmed, timing, created_at, line, odds
    FROM graded_bets
    WHERE sport='basketball_ncaab' AND result NOT IN ('DUPLICATE','PENDING')
    AND DATE(created_at) >= '2026-03-04'
    ORDER BY created_at
""").fetchall()

# Get Elo ratings with confidence
elo = {}
for team, rating, games, conf in conn.execute(
    "SELECT team, elo, games_played, confidence FROM elo_ratings WHERE sport='basketball_ncaab'"
).fetchall():
    elo[team] = {'elo': rating, 'games': games, 'conf': conf}

print("=" * 70)
print("  NCAAB DIAGNOSTIC — Why are we 23W-24L (-14.1u)?")
print("=" * 70)
print(f"\n  {len(bets)} total NCAAB bets since March 4\n")

# Helper to find team confidence from selection string
def get_team_conf(selection):
    """Try to match selection to a team and return Elo confidence."""
    best_match = None
    best_len = 0
    for team, data in elo.items():
        # Check if team name appears in selection
        words = team.split()
        # Try matching last 2 words (e.g., "Blue Devils")
        for n in range(len(words), 0, -1):
            phrase = ' '.join(words[-n:])
            if phrase in selection and len(phrase) > best_len:
                best_match = data
                best_len = len(phrase)
    return best_match

# ── 1. BY ELO CONFIDENCE ──
print("  ── BY ELO CONFIDENCE ──")
conf_buckets = {'HIGH': {'W':0,'L':0,'pnl':0}, 'MEDIUM': {'W':0,'L':0,'pnl':0}, 
                'LOW': {'W':0,'L':0,'pnl':0}, 'UNKNOWN': {'W':0,'L':0,'pnl':0}}

for sel, result, pnl, mtype, side, bucket, ctx, timing, dt, line, odds in bets:
    team_data = get_team_conf(sel)
    conf = team_data['conf'] if team_data else 'UNKNOWN'
    if result == 'WIN': conf_buckets[conf]['W'] += 1
    elif result == 'LOSS': conf_buckets[conf]['L'] += 1
    conf_buckets[conf]['pnl'] += (pnl or 0)

for conf in ['HIGH', 'MEDIUM', 'LOW', 'UNKNOWN']:
    d = conf_buckets[conf]
    t = d['W'] + d['L']
    if t == 0: continue
    wp = d['W']/t*100
    print(f"    {conf:8s}  {d['W']}W-{d['L']}L ({wp:.0f}%)  {d['pnl']:+.1f}u")

# ── 2. BY BET TYPE ──
print("\n  ── BY BET TYPE ──")
type_buckets = {}
for sel, result, pnl, mtype, side, bucket, ctx, timing, dt, line, odds in bets:
    if mtype not in type_buckets: type_buckets[mtype] = {'W':0,'L':0,'pnl':0}
    if result == 'WIN': type_buckets[mtype]['W'] += 1
    elif result == 'LOSS': type_buckets[mtype]['L'] += 1
    type_buckets[mtype]['pnl'] += (pnl or 0)

for mtype, d in sorted(type_buckets.items(), key=lambda x: x[1]['pnl']):
    t = d['W'] + d['L']
    if t == 0: continue
    wp = d['W']/t*100
    print(f"    {mtype:12s}  {d['W']}W-{d['L']}L ({wp:.0f}%)  {d['pnl']:+.1f}u")

# ── 3. BY SIDE TYPE ──
print("\n  ── BY SIDE TYPE ──")
side_buckets = {}
for sel, result, pnl, mtype, side, bucket, ctx, timing, dt, line, odds in bets:
    s = side or 'UNKNOWN'
    if s not in side_buckets: side_buckets[s] = {'W':0,'L':0,'pnl':0}
    if result == 'WIN': side_buckets[s]['W'] += 1
    elif result == 'LOSS': side_buckets[s]['L'] += 1
    side_buckets[s]['pnl'] += (pnl or 0)

for s, d in sorted(side_buckets.items(), key=lambda x: x[1]['pnl']):
    t = d['W'] + d['L']
    if t == 0: continue
    wp = d['W']/t*100
    print(f"    {s:12s}  {d['W']}W-{d['L']}L ({wp:.0f}%)  {d['pnl']:+.1f}u")

# ── 4. BY SPREAD BUCKET ──
print("\n  ── BY SPREAD BUCKET ──")
sb = {}
for sel, result, pnl, mtype, side, bucket, ctx, timing, dt, line, odds in bets:
    b = bucket or 'N/A'
    if b not in sb: sb[b] = {'W':0,'L':0,'pnl':0}
    if result == 'WIN': sb[b]['W'] += 1
    elif result == 'LOSS': sb[b]['L'] += 1
    sb[b]['pnl'] += (pnl or 0)

for b, d in sorted(sb.items(), key=lambda x: x[1]['pnl']):
    t = d['W'] + d['L']
    if t == 0: continue
    wp = d['W']/t*100
    print(f"    {b:20s}  {d['W']}W-{d['L']}L ({wp:.0f}%)  {d['pnl']:+.1f}u")

# ── 5. BY TIMING ──
print("\n  ── BY TIMING ──")
tb = {}
for sel, result, pnl, mtype, side, bucket, ctx, timing, dt, line, odds in bets:
    t_key = timing or 'UNKNOWN'
    if t_key not in tb: tb[t_key] = {'W':0,'L':0,'pnl':0}
    if result == 'WIN': tb[t_key]['W'] += 1
    elif result == 'LOSS': tb[t_key]['L'] += 1
    tb[t_key]['pnl'] += (pnl or 0)

for t_key, d in sorted(tb.items(), key=lambda x: x[1]['pnl']):
    t = d['W'] + d['L']
    if t == 0: continue
    wp = d['W']/t*100
    print(f"    {t_key:12s}  {d['W']}W-{d['L']}L ({wp:.0f}%)  {d['pnl']:+.1f}u")

# ── 6. BY CONTEXT ──
print("\n  ── CONTEXT CONFIRMED vs RAW ──")
ctx_b = {'Yes': {'W':0,'L':0,'pnl':0}, 'No': {'W':0,'L':0,'pnl':0}}
for sel, result, pnl, mtype, side, bucket, ctx, timing, dt, line, odds in bets:
    k = 'Yes' if ctx else 'No'
    if result == 'WIN': ctx_b[k]['W'] += 1
    elif result == 'LOSS': ctx_b[k]['L'] += 1
    ctx_b[k]['pnl'] += (pnl or 0)

for k, d in ctx_b.items():
    t = d['W'] + d['L']
    if t == 0: continue
    wp = d['W']/t*100
    print(f"    Context={k:3s}  {d['W']}W-{d['L']}L ({wp:.0f}%)  {d['pnl']:+.1f}u")

# ── 7. BY DATE (weekly) ──
print("\n  ── BY DATE ──")
date_b = {}
for sel, result, pnl, mtype, side, bucket, ctx, timing, dt, line, odds in bets:
    d = dt[:10] if dt else 'Unknown'
    if d not in date_b: date_b[d] = {'W':0,'L':0,'pnl':0}
    if result == 'WIN': date_b[d]['W'] += 1
    elif result == 'LOSS': date_b[d]['L'] += 1
    date_b[d]['pnl'] += (pnl or 0)

for d in sorted(date_b.keys()):
    data = date_b[d]
    t = data['W'] + data['L']
    if t == 0: continue
    wp = data['W']/t*100
    print(f"    {d}  {data['W']}W-{data['L']}L ({wp:.0f}%)  {data['pnl']:+.1f}u")

# ── 8. EVERY BET WITH TEAM ELO ──
print("\n  ── ALL NCAAB BETS (with Elo data) ──")
for sel, result, pnl, mtype, side, bucket, ctx, timing, dt, line, odds in bets:
    team_data = get_team_conf(sel)
    conf = team_data['conf'] if team_data else '?'
    elo_r = int(team_data['elo']) if team_data else 0
    games = team_data['games'] if team_data else 0
    icon = "W" if result == 'WIN' else "L"
    ctx_flag = " CTX" if ctx else ""
    print(f"    {icon} {sel:45s} {pnl:+.1f}u  Elo={elo_r} ({conf}/{games}g)  {mtype:8s} {side or '':10s} {dt[:10]}{ctx_flag}")

conn.close()
print("\n" + "=" * 70)
