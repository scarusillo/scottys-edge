#!/usr/bin/env python3
"""
PROPS AUDIT — Diagnose why prop bets aren't being graded.

Checks:
  1. How many prop bets exist in the DB
  2. Whether they're being graded
  3. Why determine_result can't handle them
  4. What's needed to fix it

Usage:
    python props_audit.py
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

def main():
    conn = sqlite3.connect(DB_PATH)
    
    print("=" * 60)
    print("  PROPS AUDIT")
    print("=" * 60)
    
    # 1. How many prop bets?
    prop_bets = conn.execute("""
        SELECT COUNT(*) FROM bets WHERE market_type = 'PROP'
    """).fetchone()[0]
    print(f"\n  Prop bets in DB:       {prop_bets}")
    
    # 2. How many graded?
    graded_props = conn.execute("""
        SELECT COUNT(*) FROM graded_bets WHERE market_type = 'PROP'
    """).fetchone()[0]
    print(f"  Graded prop bets:      {graded_props}")
    
    # 3. How many ungraded?
    ungraded = conn.execute("""
        SELECT COUNT(*) FROM bets 
        WHERE market_type = 'PROP'
        AND id NOT IN (SELECT bet_id FROM graded_bets WHERE bet_id IS NOT NULL)
    """).fetchone()[0]
    print(f"  Ungraded prop bets:    {ungraded}")
    
    # 4. Show some examples
    samples = conn.execute("""
        SELECT sport, selection, line, odds, book, created_at
        FROM bets WHERE market_type = 'PROP'
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()
    
    if samples:
        print(f"\n  Recent prop bets:")
        for s in samples:
            sport, sel, line, odds, book, created = s
            print(f"    {created[:10]} | {sel} | {line} | {odds:+d} | {book}")
    else:
        print(f"\n  No prop bets found in DB.")
    
    # 5. Player results
    player_results = conn.execute("SELECT COUNT(*) FROM player_results").fetchone()[0]
    print(f"\n  Player results in DB:  {player_results}")
    
    # 6. API budget impact
    prop_sports = ['basketball_nba', 'basketball_ncaab', 'icehockey_nhl']
    prop_calls_per_run = 0
    for sp in prop_sports:
        events = conn.execute("""
            SELECT COUNT(DISTINCT event_id) FROM odds
            WHERE sport=? AND snapshot_date = (SELECT MAX(snapshot_date) FROM odds WHERE sport=?)
        """, (sp, sp)).fetchone()[0]
        calls = min(events, 15 if 'ncaab' in sp else events)
        prop_calls_per_run += calls
        print(f"  {sp}: ~{calls} prop API calls per run")
    
    print(f"  Total prop API calls:  ~{prop_calls_per_run * 2}/day (11am + 5pm)")
    
    # 7. Diagnosis
    print(f"\n  {'='*50}")
    print(f"  DIAGNOSIS")
    print(f"  {'='*50}")
    
    if prop_bets == 0:
        print(f"  No prop bets saved. Props engine may not be finding edges,")
        print(f"  or save_picks_to_db may not be saving prop picks.")
    elif graded_props == 0 and prop_bets > 0:
        print(f"  Props exist but NONE are graded.")
        print(f"  ROOT CAUSE: determine_result() in grader.py has no PROP handler.")
        print(f"  It falls through to 'PENDING' for all prop bets.")
        print(f"  ")
        print(f"  TO FIX: Need a box score data source (ESPN player stats API)")
        print(f"  to look up actual player stat lines. Without knowing if")
        print(f"  LeBron scored 28 or 22 points, we can't grade 'OVER 25.5 PTS'.")
        print(f"  ")
        print(f"  OPTIONS:")
        print(f"    A) Add ESPN box score scraping to grade props (recommended)")
        print(f"    B) Disable props to save API budget until box scores work")
        print(f"    C) Manual grading via a script that asks you for results")
    else:
        print(f"  {graded_props}/{prop_bets} props graded. Check for issues above.")
    
    conn.close()

if __name__ == '__main__':
    main()
