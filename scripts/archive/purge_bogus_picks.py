"""
purge_bogus_picks.py — Clean bogus picks from bugged model versions.

Removes:
  1. Soccer underdog ML picks from the draw-probability bug (edges 20%+)
  2. NCAAB totals with inflated edges from old TOTAL_STD=12 (edges 25%+)
  3. Any batch runs with 25+ picks from today (clearly bugged model)
  4. DUPLICATE graded entries

Usage:
    python purge_bogus_picks.py          # Preview what gets deleted
    python purge_bogus_picks.py --purge  # Actually delete
"""
import sqlite3, os, sys
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

def purge(dry_run=True):
    conn = sqlite3.connect(DB_PATH)
    mode = "PREVIEW" if dry_run else "PURGING"
    print(f"\n  {'='*55}")
    print(f"  BOGUS PICK CLEANUP — {mode}")
    print(f"  {'='*55}\n")

    ids_to_delete = set()

    # 1. Soccer ML underdog picks from draw bug
    soccer_ml = conn.execute("""
        SELECT id, sport, selection, odds, edge_pct, created_at
        FROM bets WHERE sport LIKE 'soccer_%'
        AND market_type = 'MONEYLINE' AND odds >= 300 AND edge_pct >= 20.0
    """).fetchall()
    print(f"  [1] Soccer underdog ML (draw bug): {len(soccer_ml)}")
    for b in soccer_ml:
        print(f"      ID {b[0]}: {b[2]:45s} +{b[3]} edge={b[4]}% ({b[5][:10]})")
        ids_to_delete.add(b[0])

    # 2. NCAAB totals with inflated edges (old TOTAL_STD=12)
    ncaab_totals = conn.execute("""
        SELECT id, sport, selection, odds, edge_pct, created_at
        FROM bets WHERE sport = 'basketball_ncaab'
        AND market_type = 'TOTAL' AND edge_pct >= 25.0
    """).fetchall()
    print(f"\n  [2] NCAAB totals inflated edges (≥25%): {len(ncaab_totals)}")
    for b in ncaab_totals:
        print(f"      ID {b[0]}: {b[2]:45s} edge={b[4]}% ({b[5][:10]})")
        ids_to_delete.add(b[0])

    # 3. Batch runs with 25+ picks (bugged model)
    today = datetime.now().strftime('%Y-%m-%d')
    today_picks = conn.execute("""
        SELECT id, selection, edge_pct, market_type, created_at
        FROM bets WHERE DATE(created_at) = ? ORDER BY created_at
    """, (today,)).fetchall()
    runs = {}
    for b in today_picks:
        ts = b[4][:16]
        runs.setdefault(ts, []).append(b)
    for ts, picks in runs.items():
        if len(picks) > 25:
            print(f"\n  [3] Bugged batch at {ts}: {len(picks)} picks")
            for b in picks[:3]:
                print(f"      ID {b[0]}: {b[1]:45s} edge={b[2]}%")
            if len(picks) > 3:
                print(f"      ... and {len(picks)-3} more")
            for b in picks:
                ids_to_delete.add(b[0])

    # 4. DUPLICATE graded entries
    dupes = conn.execute("SELECT COUNT(*) FROM graded_bets WHERE result='DUPLICATE'").fetchone()[0]
    print(f"\n  [4] DUPLICATE graded entries: {dupes}")

    print(f"\n  {'─'*55}")
    print(f"  TOTAL BETS TO DELETE: {len(ids_to_delete)}")

    if not dry_run and ids_to_delete:
        id_list = ','.join(str(i) for i in ids_to_delete)
        g = conn.execute(f"DELETE FROM graded_bets WHERE bet_id IN ({id_list})").rowcount
        d = conn.execute(f"DELETE FROM bets WHERE id IN ({id_list})").rowcount
        dd = 0
        if dupes:
            dd = conn.execute("DELETE FROM graded_bets WHERE result='DUPLICATE'").rowcount
        conn.commit()
        print(f"\n  ✅ Deleted {d} bogus bets, {g} graded entries, {dd} duplicates")
        print(f"  Database cleaned. Weekly report will be accurate.")
    elif dry_run:
        print(f"\n  Run with --purge to actually delete.")

    rem = conn.execute("SELECT COUNT(*) FROM bets").fetchone()[0]
    gr = conn.execute("SELECT COUNT(*) FROM graded_bets").fetchone()[0]
    print(f"\n  Remaining: {rem} bets, {gr} graded")
    conn.close()


def fix_soccer_ml_draws(dry_run=True):
    """Fix soccer ML bets graded as PUSH that should be LOSS (draws)."""
    conn = sqlite3.connect(DB_PATH)
    
    # Find soccer ML bets graded as PUSH
    bad_grades = conn.execute("""
        SELECT g.id, g.bet_id, b.sport, b.selection, g.result, g.pnl_units, b.units, b.odds
        FROM graded_bets g
        JOIN bets b ON g.bet_id = b.id
        WHERE b.sport LIKE 'soccer_%'
        AND b.market_type = 'MONEYLINE'
        AND g.result = 'PUSH'
    """).fetchall()
    
    print(f"\n  {'='*55}")
    print(f"  SOCCER ML DRAW FIX — {'PREVIEW' if dry_run else 'FIXING'}")
    print(f"  {'='*55}")
    print(f"\n  Soccer ML draws graded as PUSH (should be LOSS): {len(bad_grades)}")
    
    for g in bad_grades:
        gid, bid, sport, sel, result, pnl, units, odds = g
        print(f"    Graded ID {gid}: {sel:35s} PUSH → LOSS (was +0.0u, should be -{units:.1f}u)")
    
    if not dry_run and bad_grades:
        for g in bad_grades:
            gid, bid, sport, sel, result, pnl, units, odds = g
            conn.execute("""
                UPDATE graded_bets SET result = 'LOSS', pnl_units = ? WHERE id = ?
            """, (-units, gid))
        conn.commit()
        print(f"\n  ✅ Fixed {len(bad_grades)} soccer ML draws: PUSH → LOSS")
    elif dry_run and bad_grades:
        print(f"\n  Run with --purge to fix these.")
    else:
        print(f"\n  No soccer ML draws to fix.")
    
    conn.close()

if __name__ == '__main__':
    dry_run = '--purge' not in sys.argv
    purge(dry_run)
    fix_soccer_ml_draws(dry_run)
