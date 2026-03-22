"""
agent_growth.py — Scotty's Edge Growth Agent

Tracks and optimizes your social media presence:
  1. Content performance — which pick types get the most engagement
  2. Posting schedule — optimal times based on your audience
  3. Record milestones — auto-generates celebration posts
  4. Weekly digest — summary for followers
  5. Caption optimization — what language/format performs best

Runs weekly (Sundays) or on demand.

Usage:
    python agent_growth.py                     # Full report
    python agent_growth.py --milestone         # Check for milestones
    python agent_growth.py --weekly            # Generate weekly digest
    python agent_growth.py --caption-tips      # Caption optimization
    python agent_growth.py --email             # Email the report
"""
import sqlite3, sys, os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def check_milestones(conn):
    """Check if we've hit any postable milestones."""
    milestones = []
    
    r = conn.execute("""
        SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
               ROUND(SUM(pnl_units), 1) as pnl,
               COUNT(*) as total
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND units >= 4.5
    """).fetchone()
    
    wins, losses, pnl, total = r
    wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    
    # Win milestones
    if wins and wins % 25 == 0:
        milestones.append({
            'type': 'wins',
            'value': wins,
            'caption': f"Milestone: {wins} wins tracked and verified.\n\n"
                       f"Current record: {wins}W-{losses}L | {pnl:+.1f}u | {wp:.0f}%\n\n"
                       f"Every pick posted before the game. Every result shown.\n\n"
                       f"#ScottysEdge #SportsBetting"
        })
    
    # Total picks milestones
    if total and total % 50 == 0:
        milestones.append({
            'type': 'total',
            'value': total,
            'caption': f"{total} picks tracked. Every single one.\n\n"
                       f"Record: {wins}W-{losses}L | {pnl:+.1f}u | {wp:.0f}%\n\n"
                       f"Transparency builds trust. Data builds edge.\n\n"
                       f"#ScottysEdge #SportsBetting"
        })
    
    # Profit milestones
    if pnl and pnl >= 50 and int(pnl) % 25 == 0:
        milestones.append({
            'type': 'profit',
            'value': pnl,
            'caption': f"+{pnl:.0f} units profit since launch.\n\n"
                       f"At $10/unit that's ${pnl*10:.0f}. At $50/unit that's ${pnl*50:,.0f}.\n\n"
                       f"The model doesn't care about feelings. It follows the data.\n\n"
                       f"#ScottysEdge #SportsBetting"
        })
    
    # Win streak
    recent = conn.execute("""
        SELECT result FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND units >= 4.5
        ORDER BY created_at DESC
    """).fetchall()
    
    streak = 0
    for (result,) in recent:
        if result == 'WIN':
            streak += 1
        else:
            break
    
    if streak >= 5:
        milestones.append({
            'type': 'streak',
            'value': streak,
            'caption': f"{streak} straight wins.\n\n"
                       f"The model is locked in. {wins}W-{losses}L overall.\n\n"
                       f"#ScottysEdge #SportsBetting"
        })
    
    return milestones


def generate_weekly_digest(conn):
    """Generate a weekly summary post for followers."""
    # Last 7 days
    week = conn.execute("""
        SELECT sport, result, pnl_units, units, selection
        FROM graded_bets
        WHERE DATE(created_at) >= DATE('now', '-7 days')
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND units >= 4.5
        ORDER BY created_at
    """).fetchall()
    
    if not week:
        return None
    
    wins = sum(1 for b in week if b[1] == 'WIN')
    losses = sum(1 for b in week if b[1] == 'LOSS')
    pnl = sum(b[2] or 0 for b in week)
    
    # Best and worst
    best = max(week, key=lambda x: x[2] or 0)
    worst = min(week, key=lambda x: x[2] or 0)
    
    # By sport
    sports = {}
    for sport, result, p, u, sel in week:
        label = {
            'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
            'icehockey_nhl': 'NHL', 'baseball_ncaa': 'Baseball',
        }.get(sport, sport)
        if label not in sports:
            sports[label] = {'W': 0, 'L': 0, 'pnl': 0}
        if result == 'WIN':
            sports[label]['W'] += 1
        elif result == 'LOSS':
            sports[label]['L'] += 1
        sports[label]['pnl'] += (p or 0)
    
    # Overall record
    overall = conn.execute("""
        SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
               ROUND(SUM(pnl_units), 1)
        FROM graded_bets WHERE DATE(created_at) >= '2026-03-04'
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED') AND units >= 4.5
    """).fetchone()
    
    wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    
    lines = []
    lines.append(f"Scotty's Edge — Weekly Recap")
    lines.append(f"")
    lines.append(f"This week: {wins}W-{losses}L | {pnl:+.1f}u | {wp:.0f}%")
    lines.append(f"")
    
    for sport, d in sorted(sports.items(), key=lambda x: x[1]['pnl'], reverse=True):
        lines.append(f"{sport}: {d['W']}W-{d['L']}L ({d['pnl']:+.1f}u)")
    
    lines.append(f"")
    lines.append(f"Best pick: {best[4]} ({best[2]:+.1f}u)")
    lines.append(f"Worst pick: {worst[4]} ({worst[2]:+.1f}u)")
    lines.append(f"")
    lines.append(f"Season: {overall[0]}W-{overall[1]}L | {overall[2]:+.1f}u")
    lines.append(f"")
    lines.append(f"Every pick tracked. Every loss shown.")
    lines.append(f"")
    lines.append(f"#ScottysEdge #SportsBetting")
    
    return "\n".join(lines)


def analyze_content_performance(conn):
    """Analyze which types of picks and posts perform best."""
    insights = []
    
    bets = conn.execute("""
        SELECT sport, side_type, market_type, result, pnl_units, timing,
               context_confirmed, units, created_at
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND units >= 4.5
    """).fetchall()
    
    if len(bets) < 10:
        return ["Not enough data yet (need 10+ graded picks)"]
    
    # Best sport to highlight
    sport_perf = {}
    for sport, side, mtype, result, pnl, timing, ctx, units, dt in bets:
        label = {
            'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
            'icehockey_nhl': 'NHL', 'baseball_ncaa': 'Baseball',
        }.get(sport, sport)
        if label not in sport_perf:
            sport_perf[label] = {'W': 0, 'L': 0, 'pnl': 0}
        if result == 'WIN':
            sport_perf[label]['W'] += 1
        elif result == 'LOSS':
            sport_perf[label]['L'] += 1
        sport_perf[label]['pnl'] += (pnl or 0)
    
    best_sport = max(sport_perf.items(), key=lambda x: x[1]['pnl'])
    insights.append(f"LEAD WITH: {best_sport[0]} picks — {best_sport[1]['W']}W-{best_sport[1]['L']}L, "
                    f"{best_sport[1]['pnl']:+.1f}u. Feature this sport prominently in posts.")
    
    # Dogs vs favorites
    dogs = [b for b in bets if b[1] and 'DOG' in str(b[1]).upper()]
    favs = [b for b in bets if b[1] and 'FAV' in str(b[1]).upper()]
    
    if dogs:
        dog_w = sum(1 for b in dogs if b[3] == 'WIN')
        dog_pnl = sum(b[4] or 0 for b in dogs)
        if dog_pnl > 5:
            insights.append(f"BRAND ANGLE: Dogs are {dog_w}W-{len(dogs)-dog_w}L, {dog_pnl:+.1f}u. "
                          f"'The model finds value where others don't' — lean into the underdog narrative.")
    
    # Timing
    early = [b for b in bets if b[5] and 'EARLY' in str(b[5]).upper()]
    late = [b for b in bets if b[5] and 'LATE' in str(b[5]).upper()]
    
    if early and late:
        early_pnl = sum(b[4] or 0 for b in early)
        late_pnl = sum(b[4] or 0 for b in late)
        if late_pnl > early_pnl + 10:
            insights.append(f"POSTING TIP: Late picks ({late_pnl:+.1f}u) outperform early ({early_pnl:+.1f}u). "
                          f"The 5:30pm picks are your money makers — hype the afternoon drop.")
    
    # Win rate messaging
    total = len(bets)
    wins = sum(1 for b in bets if b[3] == 'WIN')
    wp = wins / total * 100
    if wp >= 55:
        insights.append(f"HEADLINE STAT: {wp:.0f}% win rate across {total} tracked picks. "
                       f"Use this in your bio and every post header.")
    
    # Context-confirmed edge
    ctx_bets = [b for b in bets if b[6] and b[6] > 0]
    if ctx_bets:
        ctx_w = sum(1 for b in ctx_bets if b[3] == 'WIN')
        ctx_wp = ctx_w / len(ctx_bets) * 100
        if ctx_wp > wp:
            insights.append(f"TRUST BUILDER: Context-confirmed picks hit at {ctx_wp:.0f}% vs {wp:.0f}% overall. "
                          f"Mention context factors in captions — shows analytical depth.")
    
    return insights


def generate_caption_tips():
    """Generate tips for optimizing captions."""
    tips = [
        "HOOK: Start with the result, not the team name. '59% win rate' > 'NCAAB picks'",
        "ENGAGEMENT: Ask a question in stories. 'Would you have taken this dog at +200?'",
        "CONSISTENCY: Post results within 2 hours of games ending — followers expect it",
        "CREDIBILITY: Always show losses. 'Every pick tracked, every loss shown' is your differentiator",
        "FORMAT: Keep Instagram captions under 300 words. Put hashtags in first comment",
        "TIMING: Post picks 30-60 min before first game. Creates urgency",
        "NO-EDGE DAYS: These build more trust than wins. 'Discipline is the edge' resonates",
        "STREAKS: Post hot streaks immediately. Cold streaks get a 'model update' post instead",
    ]
    return tips


def generate_full_report(conn):
    """Generate complete growth report."""
    lines = []
    lines.append("=" * 60)
    lines.append(f"  SCOTTY'S EDGE — GROWTH AGENT REPORT")
    lines.append(f"  {datetime.now().strftime('%A %B %d, %Y')}")
    lines.append("=" * 60)
    
    # Milestones
    milestones = check_milestones(conn)
    if milestones:
        lines.append(f"\n  MILESTONES TO POST:")
        for m in milestones:
            lines.append(f"    [{m['type'].upper()}] — ready to post")
            lines.append(f"    Caption: {m['caption'][:100]}...")
    else:
        lines.append(f"\n  No new milestones.")
    
    # Content insights
    insights = analyze_content_performance(conn)
    if insights:
        lines.append(f"\n  CONTENT INSIGHTS:")
        for i in insights:
            lines.append(f"    {i}")
    
    # Weekly digest (if Sunday)
    if datetime.now().weekday() == 6:  # Sunday
        digest = generate_weekly_digest(conn)
        if digest:
            lines.append(f"\n  WEEKLY DIGEST (ready to post):")
            lines.append(f"  {'─'*40}")
            for line in digest.split('\n'):
                lines.append(f"    {line}")
            lines.append(f"  {'─'*40}")
    
    # Caption tips
    tips = generate_caption_tips()
    lines.append(f"\n  CAPTION OPTIMIZATION:")
    for t in tips[:3]:  # Show top 3 rotating tips
        lines.append(f"    {t}")
    
    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    
    if '--milestone' in sys.argv:
        milestones = check_milestones(conn)
        if milestones:
            for m in milestones:
                print(f"\n  [{m['type'].upper()}]")
                print(f"  {m['caption']}")
        else:
            print("  No milestones right now.")
    
    elif '--weekly' in sys.argv:
        digest = generate_weekly_digest(conn)
        if digest:
            print(digest)
        else:
            print("  No picks this week to summarize.")
    
    elif '--caption-tips' in sys.argv:
        tips = generate_caption_tips()
        for t in tips:
            print(f"  {t}")
    
    else:
        report = generate_full_report(conn)
        print(report)
        
        if '--email' in sys.argv:
            try:
                from emailer import send_email
                today = datetime.now().strftime('%Y-%m-%d')
                send_email(f"Growth Report - {today}", report)
                print("\n  Email sent")
            except Exception as e:
                print(f"\n  Email failed: {e}")
    
    conn.close()
