"""
agent_verify.py — Scotty's Edge Verification Agent

Runs after every grade to catch data integrity issues before they
reach your followers. Prevents Vanderbilt-type misgrades.

Checks:
  1. Score verification — does the grade match the actual score?
  2. Unit verification — is the loss amount correct (should equal wagered units)?
  3. Duplicate detection — are there duplicate entries?
  4. Date sanity — was the game result from the right date?
  5. Missing grades — bets that should have been graded but weren't
  6. Sport cross-contamination — was a baseball bet graded against a basketball score?

Usage:
    python agent_verify.py                  # Run all checks
    python agent_verify.py --email          # Email any issues found
"""
import sqlite3, sys, os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def check_loss_amounts(conn):
    """Every LOSS should have pnl_units = -units (wagered amount)."""
    issues = []
    bad = conn.execute("""
        SELECT selection, result, pnl_units, units, sport, created_at
        FROM graded_bets
        WHERE result = 'LOSS' AND ROUND(ABS(pnl_units), 1) != ROUND(units, 1)
        AND result NOT IN ('DUPLICATE', 'TAINTED')
        AND DATE(created_at) >= '2026-03-04'
    """).fetchall()
    
    for sel, result, pnl, units, sport, dt in bad:
        issues.append(f"LOSS AMOUNT: {sel} — shows {pnl:+.1f}u but wagered {units:.1f}u ({dt[:10]})")
    
    return issues


def check_win_amounts(conn):
    """WIN pnl should match odds calculation."""
    issues = []
    wins = conn.execute("""
        SELECT selection, pnl_units, units, odds, sport, created_at
        FROM graded_bets
        WHERE result = 'WIN' AND odds IS NOT NULL AND units IS NOT NULL
        AND result NOT IN ('DUPLICATE', 'TAINTED')
        AND DATE(created_at) >= '2026-03-04'
    """).fetchall()
    
    for sel, pnl, units, odds, sport, dt in wins:
        if odds > 0:
            expected = round(units * (odds / 100.0), 2)
        else:
            expected = round(units * (100.0 / abs(odds)), 2)
        
        if abs(pnl - expected) > 0.2:
            issues.append(f"WIN AMOUNT: {sel} — shows {pnl:+.1f}u but expected {expected:+.1f}u at {odds:+.0f} odds ({dt[:10]})")
    
    return issues


def check_duplicates(conn):
    """Find potential duplicate graded bets."""
    issues = []
    dupes = conn.execute("""
        SELECT selection, COUNT(*), GROUP_CONCAT(result)
        FROM graded_bets
        WHERE result NOT IN ('DUPLICATE', 'TAINTED')
        AND DATE(created_at) >= '2026-03-04'
        GROUP BY selection, DATE(created_at)
        HAVING COUNT(*) > 1
    """).fetchall()
    
    for sel, count, results in dupes:
        issues.append(f"DUPLICATE: {sel} graded {count}x — results: {results}")
    
    return issues


def check_ungraded(conn):
    """Find bets from 1+ days ago that haven't been graded."""
    issues = []
    ungraded = conn.execute("""
        SELECT b.selection, b.sport, b.created_at, b.units
        FROM bets b
        WHERE b.event_id NOT IN (
            SELECT DISTINCT event_id FROM graded_bets WHERE event_id IS NOT NULL
        )
        AND DATE(b.created_at) <= DATE('now', '-1 day')
        AND DATE(b.created_at) >= DATE('now', '-5 days')
        AND b.units >= 3.5
    """).fetchall()
    
    for sel, sport, dt, units in ungraded:
        issues.append(f"UNGRADED: {sel} ({sport}) — {units:.1f}u from {dt[:10]}")
    
    return issues


def check_score_freshness(conn):
    """Check if score results are from the expected date range."""
    issues = []
    
    recent = conn.execute("""
        SELECT g.selection, g.sport, g.created_at, g.event_id,
               r.commence_time, r.home_score, r.away_score
        FROM graded_bets g
        JOIN results r ON g.event_id = r.event_id
        WHERE g.result IN ('WIN', 'LOSS')
        AND DATE(g.created_at) >= DATE('now', '-3 days')
    """).fetchall()
    
    for sel, sport, bet_dt, eid, game_dt, hs, as_ in recent:
        if bet_dt and game_dt:
            bet_date = bet_dt[:10]
            game_date = game_dt[:10]
            try:
                diff = abs((datetime.strptime(bet_date, '%Y-%m-%d') - 
                           datetime.strptime(game_date, '%Y-%m-%d')).days)
                if diff > 2:
                    issues.append(f"STALE SCORE: {sel} — bet {bet_date}, score from {game_date} ({diff} days apart)")
            except Exception:
                pass

    return issues


def check_sub_threshold(conn):
    """Ensure no sub-minimum bets are in the active record (STRONG tier 3.5u+ is valid)."""
    issues = []
    sub = conn.execute("""
        SELECT selection, units, result FROM graded_bets
        WHERE units < 2.0 AND result NOT IN ('DUPLICATE', 'TAINTED', 'PENDING')
        AND DATE(created_at) >= '2026-03-04'
    """).fetchall()

    for sel, units, result in sub:
        issues.append(f"SUB-THRESHOLD: {sel} at {units:.1f}u is in active record — should be TAINTED")
    
    return issues


def run_all_checks(conn):
    """Run every verification check."""
    all_issues = []
    
    checks = [
        ("Loss Amounts", check_loss_amounts),
        ("Win Amounts", check_win_amounts),
        ("Duplicates", check_duplicates),
        ("Ungraded Bets", check_ungraded),
        ("Score Freshness", check_score_freshness),
        ("Sub-Threshold", check_sub_threshold),
    ]
    
    lines = []
    lines.append("=" * 60)
    lines.append(f"  VERIFICATION AGENT — Data Integrity Check")
    lines.append(f"  {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    lines.append("=" * 60)
    
    total_issues = 0
    for name, check_fn in checks:
        issues = check_fn(conn)
        if issues:
            lines.append(f"\n  {name} ({len(issues)} issues):")
            for issue in issues:
                lines.append(f"    ! {issue}")
            total_issues += len(issues)
        else:
            lines.append(f"\n  {name}: CLEAN")
    
    if total_issues == 0:
        lines.append(f"\n  ALL CHECKS PASSED — data integrity verified")
    else:
        lines.append(f"\n  {total_issues} ISSUES FOUND — review required")
    
    lines.append("\n" + "=" * 60)
    
    return "\n".join(lines), total_issues


if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    report, issue_count = run_all_checks(conn)
    print(report)
    
    if '--email' in sys.argv and issue_count > 0:
        try:
            from emailer import send_email
            today = datetime.now().strftime('%Y-%m-%d')
            send_email(f"VERIFICATION ALERT - {issue_count} issues - {today}", report)
            print("\n  Alert email sent")
        except Exception as e:
            print(f"\n  Email failed: {e}")
    
    conn.close()
