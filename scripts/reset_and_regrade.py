#!/usr/bin/env python3
"""
RESET & REGRADE — Wipe all graded bets and re-score from scratch.

Uses the new v12 logic:
  - Dedup by team/side (not full selection string)
  - Same-book CLV comparison
  - Split CLV reporting (spread pts vs ML implied%)
  - Team alias map for baseball grading

No picks change. No API cost. Just re-scores what already happened.

Usage:
    python reset_and_regrade.py              # Preview (dry run)
    python reset_and_regrade.py --confirm    # Actually do it
"""
import sqlite3
import os
import sys
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

def main():
    confirm = '--confirm' in sys.argv
    conn = sqlite3.connect(DB_PATH)
    
    # ── Step 1: Show current state ──
    total_bets = conn.execute("SELECT COUNT(*) FROM bets").fetchone()[0]
    total_graded = conn.execute("SELECT COUNT(*) FROM graded_bets").fetchone()[0]
    total_dupes = conn.execute("SELECT COUNT(*) FROM graded_bets WHERE result='DUPLICATE'").fetchone()[0]
    total_real = total_graded - total_dupes
    
    wins = conn.execute("SELECT COUNT(*) FROM graded_bets WHERE result='WIN'").fetchone()[0]
    losses = conn.execute("SELECT COUNT(*) FROM graded_bets WHERE result='LOSS'").fetchone()[0]
    pushes = conn.execute("SELECT COUNT(*) FROM graded_bets WHERE result='PUSH'").fetchone()[0]
    pnl = conn.execute("SELECT COALESCE(SUM(pnl_units), 0) FROM graded_bets WHERE result IN ('WIN','LOSS','PUSH')").fetchone()[0]
    
    print("=" * 60)
    print("  RESET & REGRADE — Clean Slate")
    print("=" * 60)
    print(f"\n  CURRENT STATE (DIRTY DATA):")
    print(f"    Total bets in DB:     {total_bets}")
    print(f"    Graded records:       {total_graded}")
    print(f"    Marked as DUPLICATE:  {total_dupes}")
    print(f"    Real graded:          {total_real}")
    print(f"    Record:               {wins}W-{losses}L-{pushes}P")
    print(f"    P/L:                  {pnl:+.1f}u")
    
    # ── Step 2: Preview what regrade will produce ──
    # Count unique bets by side (the new dedup logic)
    import re
    all_bets = conn.execute("""
        SELECT id, event_id, market_type, selection, created_at
        FROM bets ORDER BY created_at
    """).fetchall()
    
    seen_sides = {}
    unique_count = 0
    dupe_count = 0
    for bid, eid, mtype, sel, created in all_bets:
        if mtype == 'SPREAD':
            side = re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
        elif mtype == 'TOTAL':
            side = 'OVER' if 'OVER' in sel.upper() else 'UNDER'
        elif mtype == 'MONEYLINE':
            side = sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
        else:
            side = sel
        
        day = created[:10]
        key = f"{eid}|{mtype}|{side}|{day}"
        if key in seen_sides:
            dupe_count += 1
        else:
            seen_sides[key] = bid
            unique_count += 1
    
    print(f"\n  AFTER REGRADE (CLEAN DATA):")
    print(f"    Unique bets (by side): {unique_count}")
    print(f"    Duplicates to skip:    {dupe_count}")
    print(f"    Reduction:             {total_bets} → {unique_count} ({total_bets - unique_count} removed)")
    
    if not confirm:
        print(f"\n  ⚠ DRY RUN — no changes made.")
        print(f"  Run with --confirm to execute:")
        print(f"    python reset_and_regrade.py --confirm")
        conn.close()
        return
    
    # ── Step 3: Backup ──
    backup_path = DB_PATH.replace('.db', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db')
    print(f"\n  💾 Backing up database to: {os.path.basename(backup_path)}")
    import shutil
    shutil.copy2(DB_PATH, backup_path)
    
    # ── Step 4: Wipe graded_bets ──
    print(f"  🗑  Deleting {total_graded} graded_bets records...")
    conn.execute("DELETE FROM graded_bets")
    conn.commit()
    
    # ── Step 5: Regrade with full history ──
    print(f"  🔄 Regrading all {total_bets} bets with v12 logic...")
    
    # Import the grader with all fixes
    from grader import grade_bets, performance_report
    
    # Grade with a very large lookback to catch everything
    graded = grade_bets(conn, days_back=365)
    
    if graded:
        wins = sum(1 for g in graded if g['result'] == 'WIN')
        losses = sum(1 for g in graded if g['result'] == 'LOSS')
        pushes = sum(1 for g in graded if g['result'] == 'PUSH')
        pnl = sum(g['pnl'] for g in graded)
        
        clv_spread = [g['clv'] for g in graded if g['clv'] is not None and g.get('market_type') == 'SPREAD']
        clv_total = [g['clv'] for g in graded if g['clv'] is not None and g.get('market_type') == 'TOTAL']
        clv_ml = [g['clv'] for g in graded if g['clv'] is not None and g.get('market_type') == 'MONEYLINE']
        
        print(f"\n  ✅ CLEAN RECORD:")
        print(f"    Graded:  {len(graded)} unique bets")
        print(f"    Record:  {wins}W-{losses}L-{pushes}P ({wins/(wins+losses)*100:.1f}%)" if wins+losses > 0 else "")
        print(f"    P/L:     {pnl:+.1f}u")
        
        if clv_spread:
            avg_s = sum(clv_spread)/len(clv_spread)
            pos_s = sum(1 for c in clv_spread if c > 0)
            print(f"    Spread CLV:  {avg_s:+.1f} pts | +CLV: {pos_s}/{len(clv_spread)} ({pos_s/len(clv_spread)*100:.0f}%)")
        if clv_total:
            avg_t = sum(clv_total)/len(clv_total)
            pos_t = sum(1 for c in clv_total if c > 0)
            print(f"    Total CLV:   {avg_t:+.1f} pts | +CLV: {pos_t}/{len(clv_total)} ({pos_t/len(clv_total)*100:.0f}%)")
        if clv_ml:
            avg_m = sum(clv_ml)/len(clv_ml)
            pos_m = sum(1 for c in clv_ml if c > 0)
            print(f"    ML CLV:      {avg_m:+.1f} impl% | +CLV: {pos_m}/{len(clv_ml)} ({pos_m/len(clv_ml)*100:.0f}%)")
    else:
        print("  ⚠ No bets could be graded (missing scores?)")
    
    # ── Step 6: Generate fresh report ──
    print(f"\n  📊 Generating clean performance report...")
    report = performance_report(conn, days=30)
    print(report)
    
    conn.close()
    print(f"\n  ✅ Done. Backup at: {os.path.basename(backup_path)}")


if __name__ == '__main__':
    main()
