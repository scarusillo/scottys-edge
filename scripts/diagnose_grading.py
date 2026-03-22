"""
diagnose_grading.py — Check why bets aren't being graded.

Run this:
    python diagnose_grading.py
"""
import sqlite3, os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def diagnose():
    conn = sqlite3.connect(DB_PATH)
    print(f"  Database: {os.path.abspath(DB_PATH)}")
    print(f"  Now: {datetime.now()}")
    
    cutoff = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    print(f"  Grader cutoff (3 days back): {cutoff}")

    # ── 1. What's in the bets table? ──
    print(f"\n{'='*60}")
    print(f"  BETS TABLE (last 5 days)")
    print(f"{'='*60}")
    
    all_bets = conn.execute("""
        SELECT id, sport, selection, market_type, line, odds, units,
               DATE(created_at) as bet_date, created_at
        FROM bets
        WHERE DATE(created_at) >= ?
        ORDER BY created_at DESC
    """, ((datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),)).fetchall()
    
    if not all_bets:
        print("  ⚠️  NO BETS IN TABLE for last 5 days!")
        print("  This is the problem — picks were displayed but never saved.")
    else:
        print(f"  Found {len(all_bets)} bets:")
        by_date = {}
        for b in all_bets:
            d = b[7]
            by_date.setdefault(d, []).append(b)
        
        for date in sorted(by_date.keys(), reverse=True):
            bets = by_date[date]
            print(f"\n  📅 {date} ({len(bets)} bets):")
            for b in bets:
                bid, sport, sel, mtype, line, odds, units, _, created = b
                label = sport.split('_')[-1].upper() if sport else '?'
                print(f"    id={bid:4d} | [{label:6s}] {sel:45s} | {mtype:10s} | {units:.1f}u | {created}")

    # ── 2. What's in graded_bets? ──
    print(f"\n{'='*60}")
    print(f"  GRADED_BETS TABLE (last 5 days)")
    print(f"{'='*60}")
    
    graded = conn.execute("""
        SELECT id, bet_id, selection, result, pnl_units, clv,
               DATE(created_at) as bet_date, graded_at
        FROM graded_bets
        WHERE DATE(created_at) >= ?
        ORDER BY graded_at DESC
    """, ((datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),)).fetchall()
    
    if graded:
        print(f"  Found {len(graded)} graded entries:")
        for g in graded:
            gid, bid, sel, result, pnl, clv, gdate, graded_at = g
            icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '➖')
            clv_str = f"CLV={clv:+.1f}" if clv is not None else "no CLV"
            print(f"    {icon} bet_id={bid:4d} | {sel:40s} | {result:8s} | {pnl:+.1f}u | {clv_str}")
    else:
        print("  No graded entries in last 5 days.")

    # ── 3. Match: which bets are ungraded? ──
    print(f"\n{'='*60}")
    print(f"  UNGRADED BETS (should be graded)")
    print(f"{'='*60}")
    
    ungraded = conn.execute("""
        SELECT id, sport, selection, market_type, line, units, created_at
        FROM bets
        WHERE DATE(created_at) >= ?
        AND id NOT IN (SELECT bet_id FROM graded_bets WHERE bet_id IS NOT NULL)
        ORDER BY created_at
    """, (cutoff,)).fetchall()
    
    if ungraded:
        print(f"  Found {len(ungraded)} ungraded bets:")
        for u in ungraded:
            uid, sport, sel, mtype, line, units, created = u
            label = sport.split('_')[-1].upper() if sport else '?'
            print(f"    id={uid:4d} | [{label}] {sel:40s} | {units:.1f}u | created: {created}")
    else:
        print("  ⚠️  NO UNGRADED BETS — this is why the grader shows nothing!")
        
        # Check if all bet IDs are in graded_bets
        if all_bets:
            bet_ids = [b[0] for b in all_bets]
            placeholders = ','.join('?' * len(bet_ids))
            in_graded = conn.execute(f"""
                SELECT bet_id FROM graded_bets 
                WHERE bet_id IN ({placeholders})
            """, bet_ids).fetchall()
            graded_ids = {r[0] for r in in_graded}
            
            not_in_graded = [bid for bid in bet_ids if bid not in graded_ids]
            if not_in_graded:
                print(f"  🔍 {len(not_in_graded)} bet IDs NOT in graded_bets but still not found by grader query!")
                print(f"     IDs: {not_in_graded[:10]}")
                print(f"     This suggests a GROUP BY or MIN(id) issue.")
            else:
                print(f"  📋 All {len(bet_ids)} bet IDs are already in graded_bets — everything was graded already.")

    # ── 4. Check for scores ──
    print(f"\n{'='*60}")
    print(f"  SCORES/RESULTS (last 3 days)")
    print(f"{'='*60}")
    
    results = conn.execute("""
        SELECT event_id, sport, home, away, home_score, away_score, completed,
               commence_time
        FROM results
        WHERE DATE(commence_time) >= ?
        AND completed = 1
        ORDER BY commence_time DESC
        LIMIT 20
    """, (cutoff,)).fetchall()
    
    if results:
        print(f"  Found {len(results)} completed games:")
        for r in results:
            eid, sport, home, away, hs, aws, comp, commence = r
            label = sport.split('_')[-1].upper() if sport else '?'
            print(f"    [{label}] {away} {aws} @ {home} {hs}")
    else:
        print("  ⚠️  NO COMPLETED GAMES in results table!")
        print("  The grader needs scores to grade bets.")
        
        # Check if scores were fetched at all
        total_results = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        print(f"  Total results in DB: {total_results}")

    conn.close()
    print(f"\n{'='*60}")


if __name__ == '__main__':
    diagnose()
