"""
agent_analyst.py — Scotty's Edge Analyst Agent

Runs after every grade and produces a plain English briefing.
No AI API needed — uses rule-based data interpretation.

What it does:
  1. Reads diagnostic data across all sports
  2. Identifies trends (improving, declining, stable)
  3. Flags actionable warnings with specific recommendations
  4. Generates a morning briefing email
  5. Tracks week-over-week performance changes

Usage:
    python agent_analyst.py                    # Print briefing
    python agent_analyst.py --email            # Email briefing
"""
import sqlite3, sys, os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def get_performance(conn, days_back=None, start_date='2026-03-04'):
    """Get performance summary for a period."""
    if days_back:
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    else:
        cutoff = start_date
    
    bets = conn.execute("""
        SELECT sport, result, pnl_units, units, side_type, spread_bucket,
               timing, context_factors, market_type, created_at
        FROM graded_bets
        WHERE DATE(created_at) >= ? AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND units >= 4.5
    """, (cutoff,)).fetchall()
    
    wins = sum(1 for b in bets if b[1] == 'WIN')
    losses = sum(1 for b in bets if b[1] == 'LOSS')
    pnl = sum(b[2] or 0 for b in bets)
    wagered = sum(b[3] or 0 for b in bets)
    
    return {
        'bets': bets, 'wins': wins, 'losses': losses, 'pnl': pnl,
        'wagered': wagered, 'total': wins + losses,
        'wp': wins / (wins + losses) * 100 if (wins + losses) > 0 else 0,
        'roi': pnl / wagered * 100 if wagered > 0 else 0,
    }


def analyze_trends(conn):
    """Compare last 3 days vs last 7 days vs all-time."""
    all_time = get_performance(conn)
    last_7 = get_performance(conn, days_back=7)
    last_3 = get_performance(conn, days_back=3)
    
    return all_time, last_7, last_3


def analyze_sport_health(conn):
    """Check each sport's current trajectory."""
    issues = []
    strengths = []
    
    sports = conn.execute("""
        SELECT DISTINCT sport FROM graded_bets
        WHERE result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND DATE(created_at) >= '2026-03-04' AND units >= 4.5
    """).fetchall()
    
    for (sport,) in sports:
        bets = conn.execute("""
            SELECT result, pnl_units, created_at FROM graded_bets
            WHERE sport=? AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
            AND DATE(created_at) >= '2026-03-04' AND units >= 4.5
            ORDER BY created_at
        """, (sport,)).fetchall()
        
        if len(bets) < 3:
            continue
        
        wins = sum(1 for b in bets if b[0] == 'WIN')
        losses = sum(1 for b in bets if b[0] == 'LOSS')
        pnl = sum(b[1] or 0 for b in bets)
        wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        
        # Check recent trend (last 5 bets)
        recent = bets[-5:] if len(bets) >= 5 else bets
        recent_w = sum(1 for b in recent if b[0] == 'WIN')
        recent_pnl = sum(b[1] or 0 for b in recent)
        
        sport_label = {
            'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
            'icehockey_nhl': 'NHL', 'baseball_ncaa': 'Baseball',
        }.get(sport, sport)
        
        if pnl > 10:
            strengths.append(f"{sport_label}: {wins}W-{losses}L, +{pnl:.0f}u — strong performer")
        elif pnl < -5 and (wins + losses) >= 5:
            issues.append(f"{sport_label}: {wins}W-{losses}L, {pnl:+.0f}u — needs attention")
        
        if recent_pnl < -10:
            issues.append(f"{sport_label}: Last {len(recent)} picks: {recent_w}W-{len(recent)-recent_w}L, {recent_pnl:+.0f}u — cold streak")
    
    return strengths, issues


def analyze_context_health(conn):
    """Find context factors that are hurting performance."""
    warnings = []
    
    bets = conn.execute("""
        SELECT context_factors, result, pnl_units FROM graded_bets
        WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED')
        AND DATE(created_at) >= '2026-03-04' AND units >= 4.5
        AND context_factors IS NOT NULL AND context_factors != ''
    """).fetchall()
    
    factor_perf = {}
    for ctx_str, result, pnl in bets:
        factors = [f.strip().split('(')[0].strip() for f in ctx_str.split('|') if f.strip()]
        for f in factors:
            if f not in factor_perf:
                factor_perf[f] = {'W': 0, 'L': 0, 'pnl': 0}
            if result == 'WIN':
                factor_perf[f]['W'] += 1
            elif result == 'LOSS':
                factor_perf[f]['L'] += 1
            factor_perf[f]['pnl'] += (pnl or 0)
    
    for f, d in factor_perf.items():
        total = d['W'] + d['L']
        if total >= 3 and d['pnl'] < -5:
            warnings.append(f"'{f}' is {d['W']}W-{d['L']}L ({d['pnl']:+.1f}u) — investigate root cause")
    
    return warnings


def analyze_volume(conn):
    """Check if pick volume is normal."""
    notes = []
    
    # Daily volume for last 5 days
    daily = conn.execute("""
        SELECT DATE(created_at), COUNT(*) FROM bets
        WHERE DATE(created_at) >= DATE('now', '-5 days')
        AND units >= 4.5
        GROUP BY DATE(created_at)
        ORDER BY DATE(created_at)
    """).fetchall()
    
    if daily:
        avg_vol = sum(d[1] for d in daily) / len(daily)
        latest = daily[-1] if daily else None
        
        if avg_vol < 2:
            notes.append(f"Low volume: averaging {avg_vol:.1f} picks/day over last {len(daily)} days")
        
        # Check for 0-pick days
        zero_days = sum(1 for d in daily if d[1] == 0)
        if zero_days >= 2:
            notes.append(f"{zero_days} no-pick days in last 5 — check if model is too restrictive")
    
    return notes


def analyze_ungraded(conn):
    """Find bets that should have been graded but weren't."""
    ungraded = conn.execute("""
        SELECT b.selection, b.sport, b.created_at FROM bets b
        WHERE b.event_id NOT IN (
            SELECT DISTINCT event_id FROM graded_bets WHERE event_id IS NOT NULL
        )
        AND DATE(b.created_at) <= DATE('now', '-1 day')
        AND DATE(b.created_at) >= DATE('now', '-5 days')
        AND b.units >= 4.5
    """).fetchall()
    
    return ungraded


def generate_briefing(conn):
    """Generate the full morning briefing."""
    lines = []
    now = datetime.now()
    
    lines.append("=" * 60)
    lines.append(f"  SCOTTY'S EDGE — MORNING BRIEFING")
    lines.append(f"  {now.strftime('%A, %B %d, %Y  %I:%M %p')}")
    lines.append("=" * 60)
    
    # Overall performance
    all_time, last_7, last_3 = analyze_trends(conn)
    
    lines.append(f"\n  RECORD: {all_time['wins']}W-{all_time['losses']}L | "
                 f"{all_time['pnl']:+.1f}u | {all_time['wp']:.1f}% | ROI {all_time['roi']:+.1f}%")
    
    if last_7['total'] > 0:
        lines.append(f"  Last 7 days: {last_7['wins']}W-{last_7['losses']}L | {last_7['pnl']:+.1f}u")
    if last_3['total'] > 0:
        lines.append(f"  Last 3 days: {last_3['wins']}W-{last_3['losses']}L | {last_3['pnl']:+.1f}u")
    
    # Trend assessment
    if last_3['total'] >= 2:
        if last_3['pnl'] > 5:
            lines.append(f"\n  TREND: Hot streak — model is clicking")
        elif last_3['pnl'] < -5:
            lines.append(f"\n  TREND: Cold stretch — stay disciplined, review diagnostics")
        else:
            lines.append(f"\n  TREND: Steady — model performing as expected")
    
    # Sport health
    strengths, issues = analyze_sport_health(conn)
    if strengths or issues:
        lines.append(f"\n  SPORT HEALTH:")
        for s in strengths:
            lines.append(f"    + {s}")
        for i in issues:
            lines.append(f"    ! {i}")
    
    # Context warnings
    ctx_warnings = analyze_context_health(conn)
    if ctx_warnings:
        lines.append(f"\n  CONTEXT WARNINGS:")
        for w in ctx_warnings:
            lines.append(f"    ! {w}")
    
    # Volume check
    vol_notes = analyze_volume(conn)
    if vol_notes:
        lines.append(f"\n  VOLUME:")
        for n in vol_notes:
            lines.append(f"    ! {n}")
    
    # Ungraded bets
    ungraded = analyze_ungraded(conn)
    if ungraded:
        lines.append(f"\n  UNGRADED BETS ({len(ungraded)}):")
        for sel, sport, dt in ungraded[:5]:
            lines.append(f"    ? {sel} ({sport}) — {dt[:10]}")
        if len(ungraded) > 5:
            lines.append(f"    ... and {len(ungraded) - 5} more")
    
    # Today's outlook
    day_name = now.strftime('%A')
    if day_name == 'Monday':
        lines.append(f"\n  TODAY'S OUTLOOK: Monday — lightest slate of the week. 0-2 picks expected.")
    elif day_name in ('Tuesday', 'Wednesday', 'Thursday'):
        lines.append(f"\n  TODAY'S OUTLOOK: {day_name} — moderate slate. NCAA tournament games if in season.")
    elif day_name == 'Friday':
        lines.append(f"\n  TODAY'S OUTLOOK: Friday — full slate across NBA/NHL/Baseball.")
    elif day_name == 'Saturday':
        lines.append(f"\n  TODAY'S OUTLOOK: Saturday — biggest slate of the week. All sports active.")
    else:
        lines.append(f"\n  TODAY'S OUTLOOK: Sunday — moderate slate, fewer college games.")
    
    # Action items
    action_items = []
    if ctx_warnings:
        action_items.append("Review context warnings in next session")
    if ungraded:
        action_items.append(f"Grade {len(ungraded)} missing bets (ESPN may not have scores)")
    if vol_notes:
        action_items.append("Investigate low volume if it continues 2+ more days")
    
    if action_items:
        lines.append(f"\n  ACTION ITEMS:")
        for a in action_items:
            lines.append(f"    > {a}")
    else:
        lines.append(f"\n  No action items. System is running clean.")
    
    lines.append("\n" + "=" * 60)
    
    return "\n".join(lines)


if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    briefing = generate_briefing(conn)
    print(briefing)
    
    if '--email' in sys.argv:
        try:
            from emailer import send_email
            today = datetime.now().strftime('%Y-%m-%d')
            send_email(f"Morning Briefing - {today}", briefing)
            print("\n  Email sent")
        except Exception as e:
            print(f"\n  Email failed: {e}")
    
    conn.close()
