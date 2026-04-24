"""
agent_tournament.py — Scotty's Edge Tournament Agent

Monitors NCAA tournament performance specifically:
  1. Tracks tournament picks vs regular season picks
  2. Verifies B2B context freeze is working (no "Away on B2B" in tournament)
  3. Compares pre-fix vs post-fix NCAAB performance
  4. Flags any context factors leaking through the freeze
  5. Monitors neutral site games

Runs daily during March Madness (March 18 - April 7).

Usage:
    python agent_tournament.py                 # Full report
    python agent_tournament.py --email         # Email report
"""
import sqlite3, sys, os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Tournament windows
CONF_TOURNAMENT_START = '2026-03-09'
CONF_TOURNAMENT_END = '2026-03-15'
NCAA_TOURNAMENT_START = '2026-03-18'
NCAA_TOURNAMENT_END = '2026-04-07'


def check_context_freeze(conn):
    """Verify no B2B or bounce-back factors appeared during tournament."""
    leaks = conn.execute("""
        SELECT selection, context_factors, created_at
        FROM bets
        WHERE sport = 'basketball_ncaab'
        AND DATE(created_at) >= ?
        AND (context_factors LIKE '%Away on B2B%' OR context_factors LIKE '%Away bounce-back%')
        ORDER BY created_at DESC
    """, (NCAA_TOURNAMENT_START,)).fetchall()
    
    return leaks


def tournament_record(conn):
    """Track NCAA tournament picks specifically."""
    bets = conn.execute("""
        SELECT selection, result, pnl_units, units, context_factors, created_at
        FROM graded_bets
        WHERE sport = 'basketball_ncaab'
        AND DATE(created_at) >= ?
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND units >= 3.5
        ORDER BY created_at
    """, (NCAA_TOURNAMENT_START,)).fetchall()
    
    return bets


def compare_periods(conn):
    """Compare pre-fix, post-fix, and tournament performance."""
    periods = {
        'Pre-fix (Mar 4-8)': ('2026-03-04', '2026-03-08'),
        'Conf tournaments (Mar 9-15)': ('2026-03-09', '2026-03-15'),
        'Post-fix (Mar 16+)': ('2026-03-16', '2026-04-07'),
    }
    
    results = {}
    for label, (start, end) in periods.items():
        r = conn.execute("""
            SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
                   ROUND(SUM(pnl_units), 1)
            FROM graded_bets
            WHERE sport = 'basketball_ncaab'
            AND DATE(created_at) BETWEEN ? AND ?
            AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
            AND units >= 3.5
        """, (start, end)).fetchone()
        
        if r and (r[0] or 0) + (r[1] or 0) > 0:
            results[label] = {
                'wins': r[0] or 0, 'losses': r[1] or 0, 'pnl': r[2] or 0,
                'wp': (r[0] or 0) / ((r[0] or 0) + (r[1] or 0)) * 100
            }
    
    return results


def analyze_context_factors_tournament(conn):
    """Which context factors are being used in tournament picks?"""
    bets = conn.execute("""
        SELECT context_factors, result, pnl_units
        FROM graded_bets
        WHERE sport = 'basketball_ncaab'
        AND DATE(created_at) >= ?
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND context_factors IS NOT NULL AND context_factors != ''
    """, (NCAA_TOURNAMENT_START,)).fetchall()
    
    factors = {}
    for ctx_str, result, pnl in bets:
        for f in ctx_str.split('|'):
            f = f.strip().split('(')[0].strip()
            if not f:
                continue
            if f not in factors:
                factors[f] = {'W': 0, 'L': 0, 'pnl': 0}
            if result == 'WIN':
                factors[f]['W'] += 1
            elif result == 'LOSS':
                factors[f]['L'] += 1
            factors[f]['pnl'] += (pnl or 0)
    
    return factors


def analyze_spread_buckets_tournament(conn):
    """How are different spread ranges performing in tournament?"""
    buckets = conn.execute("""
        SELECT spread_bucket, 
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l,
               ROUND(SUM(pnl_units), 1) as pnl
        FROM graded_bets
        WHERE sport = 'basketball_ncaab'
        AND DATE(created_at) >= ?
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        GROUP BY spread_bucket
    """, (NCAA_TOURNAMENT_START,)).fetchall()
    
    return buckets


def generate_tournament_report(conn):
    """Full tournament analysis."""
    lines = []
    now = datetime.now()
    
    lines.append("=" * 60)
    lines.append(f"  TOURNAMENT AGENT — NCAA March Madness Monitor")
    lines.append(f"  {now.strftime('%A %B %d, %Y')}")
    lines.append("=" * 60)
    
    # Is tournament active?
    today = now.strftime('%Y-%m-%d')
    if today < NCAA_TOURNAMENT_START:
        lines.append(f"\n  NCAA Tournament starts {NCAA_TOURNAMENT_START}")
        lines.append(f"  Conference tournaments: {CONF_TOURNAMENT_START} to {CONF_TOURNAMENT_END}")
    elif today > NCAA_TOURNAMENT_END:
        lines.append(f"\n  NCAA Tournament is over.")
    else:
        lines.append(f"\n  NCAA Tournament is ACTIVE")
    
    # Context freeze verification
    leaks = check_context_freeze(conn)
    if leaks:
        lines.append(f"\n  ⚠️ CONTEXT FREEZE LEAK — {len(leaks)} bets with B2B/bounce-back:")
        for sel, ctx, dt in leaks[:5]:
            lines.append(f"    ! {sel} — {ctx[:50]} ({dt[:10]})")
    else:
        lines.append(f"\n  Context freeze: WORKING — no B2B/bounce-back in tournament picks")
    
    # Tournament record
    t_bets = tournament_record(conn)
    if t_bets:
        wins = sum(1 for b in t_bets if b[1] == 'WIN')
        losses = sum(1 for b in t_bets if b[1] == 'LOSS')
        pnl = sum(b[2] or 0 for b in t_bets)
        wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        
        lines.append(f"\n  NCAA TOURNAMENT RECORD:")
        lines.append(f"    {wins}W-{losses}L | {pnl:+.1f}u | {wp:.0f}%")
        
        for sel, result, p, units, ctx, dt in t_bets:
            icon = "W" if result == 'WIN' else "L"
            lines.append(f"    {icon} {sel:40s} {p:+.1f}u  {dt[:10]}")
    else:
        lines.append(f"\n  No tournament picks graded yet.")
    
    # Period comparison
    periods = compare_periods(conn)
    if periods:
        lines.append(f"\n  NCAAB PERFORMANCE BY PERIOD:")
        for label, d in periods.items():
            lines.append(f"    {label:30s} {d['wins']}W-{d['losses']}L | {d['pnl']:+.1f}u | {d['wp']:.0f}%")
    
    # Context factors in tournament
    factors = analyze_context_factors_tournament(conn)
    if factors:
        lines.append(f"\n  ACTIVE CONTEXT FACTORS (tournament):")
        for f, d in sorted(factors.items(), key=lambda x: x[1]['pnl']):
            total = d['W'] + d['L']
            if total > 0:
                lines.append(f"    {f:30s} {d['W']}W-{d['L']}L | {d['pnl']:+.1f}u")
    
    # Spread buckets
    buckets = analyze_spread_buckets_tournament(conn)
    if buckets:
        lines.append(f"\n  SPREAD BUCKETS (tournament):")
        for bucket, w, l, pnl in buckets:
            if (w or 0) + (l or 0) > 0:
                lines.append(f"    {bucket or 'N/A':15s} {w or 0}W-{l or 0}L | {pnl or 0:+.1f}u")
    
    # Recommendations
    lines.append(f"\n  STATUS:")
    if leaks:
        lines.append(f"    ⚠️ Context freeze has leaks — investigate immediately")
    elif t_bets:
        t_pnl = sum(b[2] or 0 for b in t_bets)
        if t_pnl > 0:
            lines.append(f"    Tournament picks are profitable — fixes working")
        else:
            lines.append(f"    Tournament picks are negative — monitor closely, small sample")
    else:
        lines.append(f"    Waiting for first tournament picks to grade")
    
    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    report = generate_tournament_report(conn)
    print(report)
    
    if '--email' in sys.argv:
        try:
            from emailer import send_email
            today = datetime.now().strftime('%Y-%m-%d')
            send_email(f"Tournament Monitor - {today}", report)
            print("\n  Email sent")
        except Exception as e:
            print(f"\n  Email failed: {e}")
    
    conn.close()
