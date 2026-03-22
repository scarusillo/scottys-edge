"""
weekly_report.py — Automated Sunday Weekly Review

Compiles everything needed for weekly model review:
  1. 7-day grading summary (W/L, P&L, ROI by sport)
  2. CLV analysis (are we beating closing lines?)
  3. Backtest update (historical model accuracy)
  4. Threshold check (are soft/sharp tiers working?)
  5. Issues & flags

Scheduled: Sundays at 10:00 AM
Output: Email with full report — forward to Claude for review.

Usage:
    python weekly_report.py              # Generate + email
    python weekly_report.py --no-email   # Print only (no email)
"""
import sqlite3, os, sys
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# v12 FIX: Import from config.py — weekly_report had NHL and La Liga as SHARP,
# but they were reclassified to SOFT in v12 based on performance data.
from config import SOFT_MARKETS, SHARP_MARKETS


def _sport_label(sport):
    return sport.replace('basketball_', '').replace('icehockey_', '').replace('soccer_', '').upper()


def weekly_grading_summary(conn, days=7):
    """Pull graded bets from last N days, compile stats."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    bets = conn.execute("""
        SELECT sport, selection, market_type, result, pnl_units, edge_pct,
               confidence, units, odds, created_at, closing_line, clv
        FROM graded_bets
        WHERE DATE(created_at) >= ? AND result != 'DUPLICATE'
        ORDER BY created_at
    """, (cutoff,)).fetchall()
    
    if not bets:
        return "  No graded bets in the last 7 days.\n"
    
    lines = []
    
    # Overall
    wins = sum(1 for b in bets if b[3] == 'WIN')
    losses = sum(1 for b in bets if b[3] == 'LOSS')
    pushes = sum(1 for b in bets if b[3] == 'PUSH')
    total_pnl = sum(b[4] or 0 for b in bets)
    total_wagered = sum(b[7] or 0 for b in bets)
    roi = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0
    wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    
    lines.append(f"  OVERALL: {wins}W-{losses}L-{pushes}P ({wp:.1f}%)")
    lines.append(f"  P/L: {total_pnl:+.1f}u | Wagered: {total_wagered:.0f}u | ROI: {roi:+.1f}%")
    
    # CLV
    clv_values = [b[11] for b in bets if b[11] is not None]
    if clv_values:
        avg_clv = sum(clv_values) / len(clv_values)
        pos_clv = sum(1 for c in clv_values if c > 0)
        clv_rate = pos_clv / len(clv_values) * 100
        lines.append(f"\n  CLV: {avg_clv:+.2f} avg | {pos_clv}/{len(clv_values)} positive ({clv_rate:.0f}%)")
        if avg_clv > 0:
            lines.append(f"  ✅ BEATING CLOSING LINES — model edge confirmed")
        elif avg_clv > -0.5:
            lines.append(f"  ⚠️  CLV near zero — need more data")
        else:
            lines.append(f"  ❌ NEGATIVE CLV — model may need recalibration")
    else:
        lines.append(f"\n  ⚠️  NO CLV DATA — closing line tracking may be broken")
        lines.append(f"  ACTION: Check that opener scheduler (8am) is running")
    
    # By sport
    lines.append(f"\n  BY SPORT:")
    sports_data = defaultdict(lambda: {'W': 0, 'L': 0, 'pnl': 0, 'wager': 0, 'clv': []})
    for b in bets:
        sp = b[0]
        d = sports_data[sp]
        if b[3] == 'WIN': d['W'] += 1
        elif b[3] == 'LOSS': d['L'] += 1
        d['pnl'] += (b[4] or 0)
        d['wager'] += (b[7] or 0)
        if b[11] is not None: d['clv'].append(b[11])
    
    for sp in sorted(sports_data.keys()):
        d = sports_data[sp]
        total = d['W'] + d['L']
        wp = d['W'] / total * 100 if total > 0 else 0
        roi = d['pnl'] / d['wager'] * 100 if d['wager'] > 0 else 0
        tier = "SOFT" if sp in SOFT_MARKETS else "SHARP"
        clv_str = f"CLV {sum(d['clv'])/len(d['clv']):+.1f}" if d['clv'] else "no CLV"
        lines.append(f"    {_sport_label(sp):12s} {d['W']}W-{d['L']}L ({wp:.0f}%) | {d['pnl']:+.1f}u | ROI {roi:+.0f}% | {tier} | {clv_str}")
    
    # By market tier
    lines.append(f"\n  BY MARKET TIER:")
    soft_bets = [b for b in bets if b[0] in SOFT_MARKETS]
    sharp_bets = [b for b in bets if b[0] in SHARP_MARKETS]
    
    for label, subset in [("SOFT", soft_bets), ("SHARP", sharp_bets)]:
        if not subset:
            lines.append(f"    {label:6s}: no picks")
            continue
        w = sum(1 for b in subset if b[3] == 'WIN')
        l = sum(1 for b in subset if b[3] == 'LOSS')
        pnl = sum(b[4] or 0 for b in subset)
        wag = sum(b[7] or 0 for b in subset)
        roi = pnl / wag * 100 if wag > 0 else 0
        wp = w / (w + l) * 100 if (w + l) > 0 else 0
        lines.append(f"    {label:6s}: {w}W-{l}L ({wp:.0f}%) | {pnl:+.1f}u | ROI {roi:+.0f}%")
    
    # By market type
    lines.append(f"\n  BY BET TYPE:")
    type_data = defaultdict(lambda: {'W': 0, 'L': 0, 'pnl': 0})
    for b in bets:
        t = b[2]
        if b[3] == 'WIN': type_data[t]['W'] += 1
        elif b[3] == 'LOSS': type_data[t]['L'] += 1
        type_data[t]['pnl'] += (b[4] or 0)
    
    for t in ['SPREAD', 'MONEYLINE', 'TOTAL', 'PROP']:
        if t in type_data:
            d = type_data[t]
            total = d['W'] + d['L']
            wp = d['W'] / total * 100 if total > 0 else 0
            lines.append(f"    {t:12s} {d['W']}W-{d['L']}L ({wp:.0f}%) | {d['pnl']:+.1f}u")
    
    # Individual results
    lines.append(f"\n  ALL GRADED PICKS:")
    for b in bets:
        icon = '✅' if b[3] == 'WIN' else ('❌' if b[3] == 'LOSS' else '➖')
        clv_str = f"CLV {b[11]:+.1f}" if b[11] is not None else ""
        edge_str = f"Edge:{b[5]:.0f}%" if b[5] else ""
        lines.append(f"    {icon} {b[1]:40s} {b[3]:5s} {b[4]:+.1f}u  {edge_str:10s} {clv_str}")
    
    return '\n'.join(lines)


def backtest_summary(conn):
    """Run abbreviated backtest and return text summary."""
    lines = []
    
    try:
        from backtest import run_backtest
        import io, contextlib
        
        # Capture backtest output for each sport
        for sport, edge in [
            ('basketball_ncaab', 8.0),
            ('basketball_nba', 15.0),
            ('icehockey_nhl', 13.0),
        ]:
            f = io.StringIO()
            with contextlib.redirect_stdout(f):
                run_backtest(sport, min_edge=edge, min_games=10, verbose=True)
            output = f.getvalue()
            if output.strip():
                lines.append(output)
    except Exception as e:
        lines.append(f"  Backtest error: {e}")
    
    return '\n'.join(lines) if lines else "  No backtest data available yet.\n"


def issues_and_flags(conn):
    """Identify potential problems and suggest actions."""
    lines = []
    cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Check if grading is running
    graded_count = conn.execute(
        "SELECT COUNT(*) FROM graded_bets WHERE DATE(graded_at) >= ? AND result != 'DUPLICATE'",
        (cutoff,)
    ).fetchone()[0]
    
    if graded_count == 0:
        lines.append("  🔴 NO GRADED BETS — grading scheduler may not be running")
        lines.append("     Fix: verify ScottysEdge_Grade task in Task Scheduler")
    
    # Check CLV
    clv_count = conn.execute(
        "SELECT COUNT(*) FROM graded_bets WHERE clv IS NOT NULL AND DATE(graded_at) >= ?",
        (cutoff,)
    ).fetchone()[0]
    
    if graded_count > 0 and clv_count == 0:
        lines.append("  🔴 NO CLV DATA — closing line lookup is broken")
        lines.append("     Fix: bring this report to Claude for debugging")
    
    # Check bet volume
    bet_count = conn.execute(
        "SELECT COUNT(*) FROM bets WHERE DATE(created_at) >= ?",
        (cutoff,)
    ).fetchone()[0]
    
    if bet_count == 0:
        lines.append("  🔴 NO BETS LOGGED — model runs may not be firing")
    elif bet_count > 100:
        lines.append(f"  🟡 HIGH VOLUME — {bet_count} bets in 7 days. Check for duplicates.")
    
    # Check openers
    opener_count = conn.execute(
        "SELECT COUNT(*) FROM openers WHERE DATE(snapshot_date) >= ?",
        (cutoff,)
    ).fetchone()[0]
    
    if opener_count == 0:
        lines.append("  🟡 NO OPENERS — 8am opener scheduler may not be running")
        lines.append("     This breaks CLV tracking. Check ScottysEdge_Opener task.")
    
    # Check win rate by tier
    for tier_name, tier_set in [("SOFT", SOFT_MARKETS), ("SHARP", SHARP_MARKETS)]:
        bets = conn.execute("""
            SELECT result FROM graded_bets
            WHERE DATE(created_at) >= ? AND result IN ('WIN','LOSS') AND sport IN ({})
        """.format(','.join(f"'{s}'" for s in tier_set)), (cutoff,)).fetchall()
        
        if len(bets) >= 10:
            wins = sum(1 for b in bets if b[0] == 'WIN')
            wp = wins / len(bets) * 100
            if wp < 40:
                lines.append(f"  🟡 {tier_name} MARKET win rate {wp:.0f}% — below 40%, consider raising thresholds")
            elif wp > 55:
                lines.append(f"  🟢 {tier_name} MARKET win rate {wp:.0f}% — above break-even, model is working")
    
    if not lines:
        lines.append("  🟢 No issues detected — all systems nominal")
    
    return '\n'.join(lines)


def generate_weekly_report():
    """Build the full weekly report."""
    conn = sqlite3.connect(DB_PATH)
    
    now = datetime.now()
    week_start = (now - timedelta(days=7)).strftime('%Y-%m-%d')
    
    sections = []
    
    sections.append("=" * 62)
    sections.append(f"  SCOTTY'S EDGE v12 — WEEKLY REVIEW")
    sections.append(f"  Week of {week_start} to {now.strftime('%Y-%m-%d')}")
    sections.append(f"  Generated: {now.strftime('%A %I:%M %p EST')}")
    sections.append("=" * 62)
    
    # Section 1: Grading
    sections.append(f"\n{'─'*62}")
    sections.append(f"  1. WEEKLY PERFORMANCE")
    sections.append(f"{'─'*62}\n")
    sections.append(weekly_grading_summary(conn, days=7))
    
    # Section 2: Issues
    sections.append(f"\n{'─'*62}")
    sections.append(f"  2. SYSTEM HEALTH & FLAGS")
    sections.append(f"{'─'*62}\n")
    sections.append(issues_and_flags(conn))
    
    # Section 3: Backtest
    sections.append(f"\n{'─'*62}")
    sections.append(f"  3. BACKTEST UPDATE")
    sections.append(f"{'─'*62}\n")
    sections.append(backtest_summary(conn))
    
    # Section 4: Action items
    sections.append(f"\n{'─'*62}")
    sections.append(f"  4. FORWARD THIS EMAIL TO CLAUDE FOR REVIEW")
    sections.append(f"{'─'*62}")
    sections.append(f"  Copy/paste this entire email into Claude for:")
    sections.append(f"    - Threshold adjustments based on CLV data")
    sections.append(f"    - Sport-by-sport edge analysis")
    sections.append(f"    - Model enhancement recommendations")
    sections.append(f"    - Any bug fixes needed")
    sections.append(f"\n{'='*62}")
    
    conn.close()
    
    return '\n'.join(sections)


if __name__ == '__main__':
    report = generate_weekly_report()
    print(report)
    
    if '--no-email' not in sys.argv:
        from emailer import send_email
        today = datetime.now().strftime('%Y-%m-%d')
        send_email(f"📊 Scotty's Edge — Weekly Review {today}", report)
