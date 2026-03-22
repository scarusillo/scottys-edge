"""
restore_feb28_bets.py — Re-insert the 2/28 picks that were lost to the purge.

These picks were displayed and recommended but their DB records were deleted
by the purge commands before grading could run.

This script matches team names against the results table to find real event_ids,
so the grader can properly grade them.

Also fixes: duplicate UNDER 3.5 Bundesliga bet (IDs 228/229).

Run once:
    python restore_feb28_bets.py
    python main.py grade
"""
import sqlite3, os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def restore():
    conn = sqlite3.connect(DB_PATH)
    
    # ── 1. Restore 2/28 picks ──
    # Format: (team_fragment_home, team_fragment_away, selection, book, line, odds, edge, conf, units)
    # team fragments are used to search the results table for the real event_id
    picks = [
        ('North Florida', 'Jacksonville', 
         'Jacksonville Dolphins +1.5', 'BetRivers', 1.5, -118, 18.3, 'ELITE', 5.0),
        ('Maryland-Eastern Shore', 'South Carolina St',
         'South Carolina St Bulldogs +6.5', 'FanDuel', 6.5, -115, 17.3, 'ELITE', 4.5),
        ('Colorado St', 'San José St',
         'San José St Spartans +7.5', 'BetMGM', 7.5, -102, 16.2, 'HIGH', 4.0),
        ('McNeese', 'New Orleans',
         'New Orleans Privateers +8.5', 'FanDuel', 8.5, -104, 16.4, 'HIGH', 4.0),
        ('Ball State', 'Northern Illinois',
         'Northern Illinois Huskies -1.5', 'BetRivers', -1.5, -108, 14.1, 'HIGH', 3.5),
        ('Seattle', 'Loyola Marymount',
         'Loyola Marymount Lions -1.0', 'Caesars', -1.0, -110, 13.7, 'HIGH', 3.5),
        ('SMU', 'Stanford',
         'Stanford Cardinal +1.5', 'BetRivers', 1.5, -106, 16.1, 'HIGH', 4.0),
        ('San Diego', 'Portland',
         'Portland Pilots +3.5', 'Caesars', 3.5, -110, 14.4, 'HIGH', 4.0),
    ]
    
    created_at = '2026-02-28T17:30:00.000000'
    inserted = 0
    not_found = 0
    
    for p in picks:
        home_frag, away_frag, sel, book, line, odds, edge, conf, units = p
        
        # Check if already exists
        existing = conn.execute("""
            SELECT id FROM bets WHERE selection = ? AND DATE(created_at) = '2026-02-28'
        """, (sel,)).fetchone()
        
        if existing:
            print(f"  ⏭️  Already exists: {sel}")
            continue
        
        # Find real event_id from results or market_consensus
        event_row = conn.execute("""
            SELECT event_id FROM results
            WHERE home LIKE ? AND away LIKE ?
            AND DATE(commence_time) >= '2026-02-28'
            LIMIT 1
        """, (f'%{home_frag}%', f'%{away_frag}%')).fetchone()
        
        if not event_row:
            # Try market_consensus as backup
            event_row = conn.execute("""
                SELECT event_id FROM market_consensus
                WHERE home LIKE ? AND away LIKE ?
                AND snapshot_date >= '2026-02-28'
                LIMIT 1
            """, (f'%{home_frag}%', f'%{away_frag}%')).fetchone()
        
        if not event_row:
            # Try swapping home/away
            event_row = conn.execute("""
                SELECT event_id FROM results
                WHERE home LIKE ? AND away LIKE ?
                AND DATE(commence_time) >= '2026-02-28'
                LIMIT 1
            """, (f'%{away_frag}%', f'%{home_frag}%')).fetchone()
        
        if not event_row:
            print(f"  ⚠️  No event_id found for: {sel} ({home_frag} vs {away_frag})")
            not_found += 1
            continue
        
        eid = event_row[0]
            
        conn.execute("""
            INSERT INTO bets (created_at, sport, event_id, market_type, selection,
                book, line, odds, model_prob, implied_prob, edge_pct, confidence, units)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (created_at, 'basketball_ncaab', eid, 'SPREAD', sel, book, line, odds,
              0.55, 0.50, edge, conf, units))
        inserted += 1
        print(f"  ✅ Restored: {sel} | {units:.1f}u | {book} | event={eid[:12]}...")
    
    # ── 2. Fix duplicate Bundesliga UNDER 3.5 (only if same event) ──
    dupes = conn.execute("""
        SELECT id, event_id, selection, sport FROM bets
        WHERE selection = 'UNDER 3.5' AND market_type = 'TOTAL'
        AND DATE(created_at) = '2026-03-01'
        ORDER BY id
    """).fetchall()
    
    if len(dupes) > 1:
        # Group by event_id — only remove if same event
        seen_events = {}
        for d in dupes:
            eid = d[1]
            if eid in seen_events:
                conn.execute("DELETE FROM bets WHERE id = ?", (d[0],))
                print(f"  🗑️  Removed duplicate: id={d[0]} UNDER 3.5 (same event as id={seen_events[eid]})")
            else:
                seen_events[eid] = d[0]
                
        if len(seen_events) == len(dupes):
            print(f"  ℹ️  Two UNDER 3.5 bets are for different games — both kept")
    
    conn.commit()
    conn.close()
    
    print(f"\n  ✅ Restored {inserted} bets from 2/28")
    if not_found:
        print(f"  ⚠️  {not_found} bets could not be matched (scores may not be fetched yet)")
    print(f"  Next: run 'python main.py grade' to grade them")


if __name__ == '__main__':
    restore()
