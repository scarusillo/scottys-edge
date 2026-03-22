"""
agent_totals.py — Scotty's Edge Totals Agent

Investigates over/under performance:
  1. When was the last totals pick?
  2. Are totals being generated but below threshold?
  3. What's the totals record by sport?
  4. Are totals edges systematically smaller than sides?
  5. Recommends adjustments if totals are underperforming

Usage:
    python agent_totals.py                     # Full analysis
    python agent_totals.py --email             # Email report
"""
import sqlite3, sys, os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def analyze_totals_history(conn):
    """Full totals pick history."""
    totals = conn.execute("""
        SELECT selection, sport, units, odds, created_at, market_type
        FROM bets
        WHERE market_type = 'TOTAL'
        AND DATE(created_at) >= '2026-03-04'
        ORDER BY created_at DESC
    """).fetchall()
    
    return totals


def analyze_totals_record(conn):
    """Graded totals performance."""
    record = conn.execute("""
        SELECT sport, result, pnl_units, units, selection, created_at
        FROM graded_bets
        WHERE market_type = 'TOTAL'
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND DATE(created_at) >= '2026-03-04'
        ORDER BY created_at
    """).fetchall()
    
    return record


def analyze_totals_by_sport(conn):
    """Break down totals by sport."""
    sports = conn.execute("""
        SELECT sport,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l,
               ROUND(SUM(pnl_units), 1) as pnl,
               ROUND(AVG(units), 1) as avg_units
        FROM graded_bets
        WHERE market_type = 'TOTAL'
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND DATE(created_at) >= '2026-03-04'
        GROUP BY sport
    """).fetchall()
    
    return sports


def analyze_over_vs_under(conn):
    """Compare overs vs unders performance."""
    results = {}
    for side in ['OVER', 'UNDER']:
        r = conn.execute("""
            SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
                   ROUND(SUM(pnl_units), 1)
            FROM graded_bets
            WHERE side_type = ?
            AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
            AND DATE(created_at) >= '2026-03-04'
        """, (side,)).fetchone()
        results[side] = {'wins': r[0] or 0, 'losses': r[1] or 0, 'pnl': r[2] or 0}
    
    return results


def analyze_recent_totals_generation(conn):
    """Check if totals are being generated at all (even below threshold)."""
    recent = conn.execute("""
        SELECT selection, units, sport, created_at
        FROM bets
        WHERE market_type = 'TOTAL'
        AND DATE(created_at) >= DATE('now', '-5 days')
        ORDER BY created_at DESC
    """).fetchall()
    
    return recent


def find_totals_gap(conn):
    """How many days since last totals pick?"""
    last = conn.execute("""
        SELECT MAX(DATE(created_at)) FROM bets
        WHERE market_type = 'TOTAL'
    """).fetchone()
    
    if last and last[0]:
        last_date = datetime.strptime(last[0], '%Y-%m-%d')
        gap = (datetime.now() - last_date).days
        return gap, last[0]
    
    return None, None


def generate_totals_report(conn):
    """Full totals analysis."""
    lines = []
    lines.append("=" * 60)
    lines.append(f"  TOTALS AGENT — Over/Under Analysis")
    lines.append(f"  {datetime.now().strftime('%A %B %d, %Y')}")
    lines.append("=" * 60)
    
    # Gap analysis
    gap, last_date = find_totals_gap(conn)
    if gap is not None:
        status = "NORMAL" if gap <= 2 else ("CONCERNING" if gap <= 5 else "STALE")
        lines.append(f"\n  Last totals pick: {last_date} ({gap} days ago) — {status}")
    else:
        lines.append(f"\n  No totals picks found.")
    
    # Recent generation
    recent = analyze_recent_totals_generation(conn)
    if recent:
        lines.append(f"\n  RECENT TOTALS (last 5 days):")
        for sel, units, sport, dt in recent:
            label = sport.split('_')[-1].upper()
            marker = " MAX PLAY" if units >= 4.5 else ""
            lines.append(f"    {dt[:10]}  {units:.1f}u  {sel:40s} {label}{marker}")
    else:
        lines.append(f"\n  No totals generated in last 5 days — model may be too conservative")
    
    # Graded record
    record = analyze_totals_record(conn)
    if record:
        wins = sum(1 for r in record if r[1] == 'WIN')
        losses = sum(1 for r in record if r[1] == 'LOSS')
        pnl = sum(r[2] or 0 for r in record)
        wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        
        lines.append(f"\n  TOTALS RECORD (all units):")
        lines.append(f"    {wins}W-{losses}L | {pnl:+.1f}u | {wp:.0f}%")
    
    # Over vs Under
    ovu = analyze_over_vs_under(conn)
    lines.append(f"\n  OVERS vs UNDERS:")
    for side, d in ovu.items():
        total = d['wins'] + d['losses']
        wp = d['wins'] / total * 100 if total > 0 else 0
        lines.append(f"    {side:6s}: {d['wins']}W-{d['losses']}L | {d['pnl']:+.1f}u | {wp:.0f}%")
    
    # By sport
    by_sport = analyze_totals_by_sport(conn)
    if by_sport:
        lines.append(f"\n  TOTALS BY SPORT:")
        for sport, w, l, pnl, avg_u in by_sport:
            label = sport.split('_')[-1].upper()
            lines.append(f"    {label:12s}: {w}W-{l}L | {pnl:+.1f}u | avg {avg_u:.1f}u")
    
    # Recommendations
    lines.append(f"\n  RECOMMENDATIONS:")
    
    if gap and gap >= 4:
        lines.append(f"    ! No totals picks in {gap} days")
        if recent:
            below = [r for r in recent if r[1] < 4.5]
            if below:
                lines.append(f"    > Totals ARE being generated but below 4.5u threshold")
                lines.append(f"    > Consider: totals edges are naturally smaller than sides")
                lines.append(f"    > Option: separate threshold for totals (e.g., 4.0u)")
        else:
            lines.append(f"    > No totals being generated at all — check totals model logic")
    
    if ovu.get('OVER', {}).get('pnl', 0) < -5:
        lines.append(f"    ! Overs are losing ({ovu['OVER']['pnl']:+.1f}u) — model may overestimate pace")
    if ovu.get('UNDER', {}).get('pnl', 0) > 5:
        lines.append(f"    + Unders are profitable — model finds value in slower games")
    
    if not (gap and gap >= 4) and not any(d.get('pnl', 0) < -5 for d in ovu.values()):
        lines.append(f"    Totals model is performing as expected.")
    
    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    report = generate_totals_report(conn)
    print(report)
    
    if '--email' in sys.argv:
        try:
            from emailer import send_email
            today = datetime.now().strftime('%Y-%m-%d')
            send_email(f"Totals Analysis - {today}", report)
            print("\n  Email sent")
        except Exception as e:
            print(f"\n  Email failed: {e}")
    
    conn.close()
