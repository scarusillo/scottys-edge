"""
sport_diagnostic.py — Deep performance diagnostic across ALL sports

Analyzes every sport for:
  1. Overall record + P/L
  2. Context factor performance (which factors are helping/hurting)
  3. Spread bucket breakdown (where are edges real vs phantom)
  4. Side type performance (dogs/favs/overs/unders)
  5. Elo confidence performance (HIGH vs MEDIUM vs LOW)
  6. Timing performance (early vs late)
  7. Weekly trend (are things getting better or worse)

Flags any factor with 3+ bets and negative ROI as a WARNING.

Usage:
    python sport_diagnostic.py                  # All sports
    python sport_diagnostic.py --sport nhl      # Single sport
    python sport_diagnostic.py --short          # Summary only (for email)
"""
import sqlite3, sys
from datetime import datetime

DB_PATH = '../data/betting_model.db'
conn = sqlite3.connect(DB_PATH)

SPORT_LABELS = {
    'basketball_ncaab': 'NCAAB', 'basketball_nba': 'NBA', 'icehockey_nhl': 'NHL',
    'baseball_ncaa': 'NCAA Baseball', 'soccer_epl': 'EPL', 'soccer_italy_serie_a': 'Serie A',
    'soccer_spain_la_liga': 'La Liga', 'soccer_germany_bundesliga': 'Bundesliga',
    'soccer_france_ligue_one': 'Ligue 1', 'soccer_usa_mls': 'MLS',
    'soccer_uefa_champs_league': 'UCL',
}

short_mode = '--short' in sys.argv
sport_filter = None
for i, arg in enumerate(sys.argv):
    if arg == '--sport' and i + 1 < len(sys.argv):
        # Map short names to full sport keys
        s = sys.argv[i+1].lower()
        for key, label in SPORT_LABELS.items():
            if s in key or s == label.lower():
                sport_filter = key
                break

# Get all sports with graded bets
if sport_filter:
    sports = [sport_filter]
else:
    sports = [r[0] for r in conn.execute("""
        SELECT DISTINCT sport FROM graded_bets 
        WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND DATE(created_at) >= '2026-03-04'
        ORDER BY sport
    """).fetchall()]

# Get Elo ratings
elo_all = {}
for team, sport, conf in conn.execute("SELECT team, sport, confidence FROM elo_ratings").fetchall():
    if sport not in elo_all:
        elo_all[sport] = {}
    elo_all[sport][team] = conf

def get_team_conf(selection, sport):
    """Match selection to team Elo confidence."""
    sport_elo = elo_all.get(sport, {})
    best_match = None
    best_len = 0
    for team, conf in sport_elo.items():
        words = team.split()
        for n in range(len(words), 0, -1):
            phrase = ' '.join(words[-n:])
            if phrase in selection and len(phrase) > best_len:
                best_match = conf
                best_len = len(phrase)
    return best_match or 'UNKNOWN'

print("=" * 70)
print(f"  SCOTTY'S EDGE — SPORT DIAGNOSTIC")
print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
print(f"  Period: March 4, 2026 — Present")
print("=" * 70)

warnings = []

for sport in sports:
    label = SPORT_LABELS.get(sport, sport)
    
    bets = conn.execute("""
        SELECT selection, result, pnl_units, market_type, side_type,
               spread_bucket, context_factors, context_confirmed, 
               timing, model_spread, line, created_at, units
        FROM graded_bets
        WHERE sport=? AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
        AND DATE(created_at) >= '2026-03-04'
        ORDER BY created_at
    """, (sport,)).fetchall()
    
    if not bets:
        continue
    
    wins = sum(1 for b in bets if b[1] == 'WIN')
    losses = sum(1 for b in bets if b[1] == 'LOSS')
    pnl = sum(b[2] or 0 for b in bets)
    wagered = sum(b[12] or 0 for b in bets)
    roi = (pnl / wagered * 100) if wagered > 0 else 0
    wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    
    print(f"\n{'─' * 70}")
    print(f"  {label}: {wins}W-{losses}L ({wp:.0f}%) | {pnl:+.1f}u | ROI {roi:+.1f}%")
    print(f"{'─' * 70}")
    
    if short_mode and pnl >= 0:
        print(f"  Profitable — no issues detected.")
        continue
    
    # ── CONTEXT FACTORS ──
    factor_perf = {}
    for sel, result, p, mtype, side, bucket, ctx_factors, ctx_conf, timing, msp, line, dt, units in bets:
        if not ctx_factors:
            continue
        factors = [f.strip() for f in ctx_factors.split('|') if f.strip()]
        for factor in factors:
            parts = factor.split('(')
            factor_name = parts[0].strip()
            if factor_name not in factor_perf:
                factor_perf[factor_name] = {'W': 0, 'L': 0, 'pnl': 0, 'count': 0}
            if result == 'WIN': factor_perf[factor_name]['W'] += 1
            elif result == 'LOSS': factor_perf[factor_name]['L'] += 1
            factor_perf[factor_name]['pnl'] += (p or 0)
            factor_perf[factor_name]['count'] += 1
    
    if factor_perf:
        print(f"\n  Context Factors:")
        for fname, d in sorted(factor_perf.items(), key=lambda x: x[1]['pnl']):
            t = d['W'] + d['L']
            if t == 0: continue
            fwp = d['W'] / t * 100
            flag = " ⚠️ WARNING" if t >= 3 and d['pnl'] < -5 else ""
            print(f"    {fname:35s} {d['W']}W-{d['L']}L ({fwp:.0f}%)  {d['pnl']:+.1f}u{flag}")
            if t >= 3 and d['pnl'] < -5:
                warnings.append(f"{label}: {fname} is {d['W']}W-{d['L']}L ({d['pnl']:+.1f}u)")
    
    if short_mode:
        continue
    
    # ── SPREAD BUCKETS ──
    sb = {}
    for sel, result, p, mtype, side, bucket, ctx_factors, ctx_conf, timing, msp, line, dt, units in bets:
        b = bucket or 'N/A'
        if b not in sb: sb[b] = {'W': 0, 'L': 0, 'pnl': 0}
        if result == 'WIN': sb[b]['W'] += 1
        elif result == 'LOSS': sb[b]['L'] += 1
        sb[b]['pnl'] += (p or 0)
    
    if sb:
        print(f"\n  Spread Buckets:")
        for b, d in sorted(sb.items(), key=lambda x: x[1]['pnl']):
            t = d['W'] + d['L']
            if t == 0: continue
            bwp = d['W'] / t * 100
            flag = " ⚠️" if t >= 3 and d['pnl'] < -5 else ""
            print(f"    {b:20s} {d['W']}W-{d['L']}L ({bwp:.0f}%)  {d['pnl']:+.1f}u{flag}")
            if t >= 3 and d['pnl'] < -5:
                warnings.append(f"{label}: {b} spread bucket is {d['W']}W-{d['L']}L ({d['pnl']:+.1f}u)")
    
    # ── SIDE TYPE ──
    sides = {}
    for sel, result, p, mtype, side, bucket, ctx_factors, ctx_conf, timing, msp, line, dt, units in bets:
        s = side or 'UNKNOWN'
        if s not in sides: sides[s] = {'W': 0, 'L': 0, 'pnl': 0}
        if result == 'WIN': sides[s]['W'] += 1
        elif result == 'LOSS': sides[s]['L'] += 1
        sides[s]['pnl'] += (p or 0)
    
    if sides:
        print(f"\n  Side Type:")
        for s, d in sorted(sides.items(), key=lambda x: x[1]['pnl']):
            t = d['W'] + d['L']
            if t == 0: continue
            swp = d['W'] / t * 100
            print(f"    {s:12s} {d['W']}W-{d['L']}L ({swp:.0f}%)  {d['pnl']:+.1f}u")
    
    # ── ELO CONFIDENCE ──
    elo_perf = {'HIGH': {'W':0,'L':0,'pnl':0}, 'MEDIUM': {'W':0,'L':0,'pnl':0}, 
                'LOW': {'W':0,'L':0,'pnl':0}, 'UNKNOWN': {'W':0,'L':0,'pnl':0}}
    for sel, result, p, mtype, side, bucket, ctx_factors, ctx_conf, timing, msp, line, dt, units in bets:
        conf = get_team_conf(sel, sport)
        if result == 'WIN': elo_perf[conf]['W'] += 1
        elif result == 'LOSS': elo_perf[conf]['L'] += 1
        elo_perf[conf]['pnl'] += (p or 0)
    
    has_elo_data = any(elo_perf[c]['W'] + elo_perf[c]['L'] > 0 for c in ['HIGH', 'MEDIUM', 'LOW'])
    if has_elo_data:
        print(f"\n  Elo Confidence:")
        for conf in ['HIGH', 'MEDIUM', 'LOW', 'UNKNOWN']:
            d = elo_perf[conf]
            t = d['W'] + d['L']
            if t == 0: continue
            ewp = d['W'] / t * 100
            print(f"    {conf:10s} {d['W']}W-{d['L']}L ({ewp:.0f}%)  {d['pnl']:+.1f}u")
    
    # ── TIMING ──
    tb = {}
    for sel, result, p, mtype, side, bucket, ctx_factors, ctx_conf, timing, msp, line, dt, units in bets:
        t_key = timing or 'UNKNOWN'
        if t_key not in tb: tb[t_key] = {'W': 0, 'L': 0, 'pnl': 0}
        if result == 'WIN': tb[t_key]['W'] += 1
        elif result == 'LOSS': tb[t_key]['L'] += 1
        tb[t_key]['pnl'] += (p or 0)
    
    if tb:
        print(f"\n  Timing:")
        for t_key, d in sorted(tb.items(), key=lambda x: x[1]['pnl']):
            t = d['W'] + d['L']
            if t == 0: continue
            twp = d['W'] / t * 100
            print(f"    {t_key:12s} {d['W']}W-{d['L']}L ({twp:.0f}%)  {d['pnl']:+.1f}u")
    
    # ── DAILY TREND ──
    dates = {}
    for sel, result, p, mtype, side, bucket, ctx_factors, ctx_conf, timing, msp, line, dt, units in bets:
        d = dt[:10] if dt else 'Unknown'
        if d not in dates: dates[d] = {'W': 0, 'L': 0, 'pnl': 0}
        if result == 'WIN': dates[d]['W'] += 1
        elif result == 'LOSS': dates[d]['L'] += 1
        dates[d]['pnl'] += (p or 0)
    
    if dates:
        print(f"\n  Daily:")
        for d in sorted(dates.keys()):
            data = dates[d]
            t = data['W'] + data['L']
            if t == 0: continue
            dwp = data['W'] / t * 100
            print(f"    {d}  {data['W']}W-{data['L']}L ({dwp:.0f}%)  {data['pnl']:+.1f}u")

# ── WARNINGS SUMMARY ──
if warnings:
    print(f"\n{'=' * 70}")
    print(f"  ⚠️  WARNINGS — Action Items")
    print(f"{'=' * 70}")
    for w in warnings:
        print(f"  • {w}")
else:
    print(f"\n  ✅ No critical warnings detected.")

conn.close()
print(f"\n{'=' * 70}")
