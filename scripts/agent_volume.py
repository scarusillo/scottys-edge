"""
agent_volume.py — Scotty's Edge Volume Agent

Investigates pick volume issues:
  1. How many games per sport were evaluated today?
  2. How many were filtered by divergence vs threshold vs no rating?
  3. What's the distribution of edge sizes? (are picks just barely missing 4.5u?)
  4. Compares current volume to historical average
  5. Recommends threshold or divergence changes if volume is abnormally low

Runs daily after the 5:30pm pick run to assess the full day.

Usage:
    python agent_volume.py                     # Full analysis
    python agent_volume.py --email             # Email report
"""
import sqlite3, sys, os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def analyze_daily_volume(conn, days_back=7):
    """Track daily pick volume trend."""
    daily = conn.execute("""
        SELECT DATE(created_at) as d, COUNT(*), 
               SUM(CASE WHEN units >= 3.5 THEN 1 ELSE 0 END) as max_plays,
               SUM(CASE WHEN units >= 4.0 AND units < 4.5 THEN 1 ELSE 0 END) as strong,
               SUM(CASE WHEN units < 4.0 THEN 1 ELSE 0 END) as below
        FROM bets
        WHERE DATE(created_at) >= DATE('now', ?)
        GROUP BY DATE(created_at)
        ORDER BY d
    """, (f'-{days_back} days',)).fetchall()
    
    return daily


def analyze_near_misses(conn, days_back=3):
    """Find picks that were close to 4.5u but didn't make it."""
    near = conn.execute("""
        SELECT selection, units, sport, odds, created_at
        FROM bets
        WHERE units >= 3.5 AND units < 4.5
        AND DATE(created_at) >= DATE('now', ?)
        ORDER BY units DESC
    """, (f'-{days_back} days',)).fetchall()
    
    return near


def analyze_sport_coverage(conn):
    """Check which sports are generating picks and which aren't."""
    # Last 7 days by sport
    sports = conn.execute("""
        SELECT sport, COUNT(*) as total,
               SUM(CASE WHEN units >= 3.5 THEN 1 ELSE 0 END) as max_plays,
               ROUND(AVG(units), 1) as avg_units
        FROM bets
        WHERE DATE(created_at) >= DATE('now', '-7 days')
        GROUP BY sport
        ORDER BY total DESC
    """).fetchall()
    
    return sports


def analyze_threshold_impact(conn):
    """What would the record look like at different thresholds?"""
    thresholds = [3.5, 4.0, 4.5, 5.0]
    results = []
    
    for thresh in thresholds:
        r = conn.execute("""
            SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l,
                   ROUND(SUM(pnl_units), 1) as pnl,
                   COUNT(*) as total
            FROM graded_bets
            WHERE DATE(created_at) >= '2026-03-04'
            AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
            AND units >= ?
        """, (thresh,)).fetchone()
        
        if r and r[3] > 0:
            wp = r[0] / (r[0] + r[1]) * 100 if (r[0] + r[1]) > 0 else 0
            wagered = conn.execute("""
                SELECT ROUND(SUM(units), 1) FROM graded_bets
                WHERE DATE(created_at) >= '2026-03-04'
                AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
                AND units >= ?
            """, (thresh,)).fetchone()[0] or 0
            roi = (r[2] / wagered * 100) if wagered > 0 else 0
            results.append({
                'threshold': thresh, 'wins': r[0], 'losses': r[1],
                'pnl': r[2], 'total': r[3], 'wp': wp, 'roi': roi,
            })
    
    return results


def generate_volume_report(conn):
    """Full volume analysis."""
    lines = []
    lines.append("=" * 60)
    lines.append(f"  VOLUME AGENT — Pick Volume Analysis")
    lines.append(f"  {datetime.now().strftime('%A %B %d, %Y')}")
    lines.append("=" * 60)
    
    # Daily volume trend
    daily = analyze_daily_volume(conn, days_back=10)
    lines.append(f"\n  DAILY VOLUME (last 10 days):")
    lines.append(f"  {'Date':12s} {'Total':>6s} {'MAX(4.5+)':>10s} {'STRONG':>8s} {'Below':>7s}")
    
    total_max = 0
    total_all = 0
    zero_days = 0
    for d, total, max_p, strong, below in daily:
        lines.append(f"  {d:12s} {total:6d} {max_p or 0:10d} {strong or 0:8d} {below or 0:7d}")
        total_max += (max_p or 0)
        total_all += total
        if (max_p or 0) == 0:
            zero_days += 1
    
    if daily:
        avg_max = total_max / len(daily)
        avg_all = total_all / len(daily)
        lines.append(f"\n  Avg MAX PLAYs/day: {avg_max:.1f}")
        lines.append(f"  Avg total picks/day: {avg_all:.1f}")
        lines.append(f"  Zero MAX PLAY days: {zero_days}/{len(daily)}")
    
    # Near misses
    near = analyze_near_misses(conn)
    if near:
        lines.append(f"\n  NEAR MISSES (3.5-4.4u, last 3 days):")
        for sel, units, sport, odds, dt in near[:10]:
            sport_label = sport.split('_')[-1].upper()
            lines.append(f"    {units:.1f}u  {sel:40s} {sport_label}")
        
        if len(near) > 3:
            lines.append(f"\n  {len(near)} picks were close to 4.5u threshold")
            lines.append(f"  At 4.0u threshold, these would have been posted")
    
    # Sport coverage
    sports = analyze_sport_coverage(conn)
    if sports:
        lines.append(f"\n  SPORT COVERAGE (last 7 days):")
        for sport, total, max_p, avg_u in sports:
            label = {
                'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
                'icehockey_nhl': 'NHL', 'baseball_ncaa': 'Baseball',
            }.get(sport, sport)
            lines.append(f"    {label:15s} {total:3d} total | {max_p or 0:2d} MAX PLAYs | avg {avg_u:.1f}u")
    
    # Threshold analysis
    thresholds = analyze_threshold_impact(conn)
    if thresholds:
        lines.append(f"\n  THRESHOLD ANALYSIS (what if?):")
        lines.append(f"  {'Threshold':>10s} {'Record':>10s} {'Win%':>6s} {'P/L':>8s} {'ROI':>7s} {'Picks':>6s}")
        for t in thresholds:
            marker = " <-- current" if t['threshold'] == 4.5 else ""
            lines.append(f"  {t['threshold']:>10.1f}u {t['wins']}W-{t['losses']}L  {t['wp']:5.1f}% {t['pnl']:+7.1f}u {t['roi']:+6.1f}% {t['total']:5d}{marker}")
    
    # Recommendations
    lines.append(f"\n  RECOMMENDATIONS:")
    
    if daily and total_max / len(daily) < 1.5:
        lines.append(f"    ! Volume is low ({total_max / len(daily):.1f} MAX PLAYs/day)")
        
        if near and len(near) >= 3:
            lines.append(f"    > Consider lowering threshold to 4.0u — {len(near)} near-misses in 3 days")
        
        # Check if specific sports are being choked
        for sport, total, max_p, avg_u in sports:
            if total >= 3 and (max_p or 0) == 0:
                label = sport.split('_')[-1].upper()
                lines.append(f"    > {label}: {total} picks but 0 MAX PLAYs — divergence may be too tight")
    else:
        lines.append(f"    Volume is healthy. No changes needed.")
    
    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    report = generate_volume_report(conn)
    print(report)
    
    if '--email' in sys.argv:
        try:
            from emailer import send_email
            today = datetime.now().strftime('%Y-%m-%d')
            send_email(f"Volume Analysis - {today}", report)
            print("\n  Email sent")
        except Exception as e:
            print(f"\n  Email failed: {e}")
    
    conn.close()
