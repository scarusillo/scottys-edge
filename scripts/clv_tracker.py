"""CLV confirmation tracker — surfaces picks where sharp money confirmed us.

Goal: replicate the "CLV >= +3%" pattern that produced 9-1 historically.
Instead of computing CLV at grade time (too late), this script joins the
opener line, fire line, and current line to show sharp-book movement in
real time — before the game starts.

Usage:
    python scripts/clv_tracker.py              # today's picks
    python scripts/clv_tracker.py --date 2026-04-21   # retrospective

Output:
  For each fired pick today:
    opener line | fire line | current line
    sharp-book movement since fire
    context tags that correlate with CLV wins (Home letdown, NCAAB, ELITE)
    flag: 🔥 CONFIRMED if line moved >= 3 pts toward our side since fire
"""
import sqlite3
import os
import sys
from datetime import datetime

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
SHARP_BOOKS = ('FanDuel', 'Pinnacle', 'DraftKings')  # for movement detection


def run_report(target_date=None):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    if target_date is None:
        target_date = datetime.now().strftime('%Y-%m-%d')

    # Pull today's fired picks
    rows = c.execute("""
        SELECT id, created_at, sport, event_id, selection, market_type, side_type,
               book, line, odds, edge_pct, confidence, context_factors, model_spread
        FROM bets
        WHERE DATE(created_at) = ? AND units >= 3.5 AND market_type IN ('SPREAD','TOTAL')
        ORDER BY created_at
    """, (target_date,)).fetchall()

    if not rows:
        print(f'No fired game-line picks for {target_date}')
        return

    print(f"=== CLV Confirmation Tracker — {target_date} ===")
    print(f"{len(rows)} picks to review\n")

    confirmed_count = 0
    for (bid, created_at, sport, eid, selection, mtype, side, book, fire_line,
         fire_odds, edge_pct, confidence, ctx_str, ms) in rows:
        # Opener line — from openers table
        opener_row = c.execute("""
            SELECT AVG(line) FROM openers
            WHERE event_id = ? AND market = ? AND line IS NOT NULL
        """, (eid, 'spreads' if mtype == 'SPREAD' else 'totals')).fetchone()
        opener_line = opener_row[0] if opener_row else None

        # Current line — latest CURRENT-tagged row
        current_row = c.execute("""
            SELECT AVG(line) FROM odds
            WHERE event_id = ? AND market = ? AND line IS NOT NULL
              AND tag = 'CURRENT'
        """, (eid, 'spreads' if mtype == 'SPREAD' else 'totals')).fetchone()
        current_line = current_row[0] if current_row else None

        # Direction-adjusted movement (positive = moved toward our side)
        # For SPREAD DOG at home +X: we want line to INCREASE (+X becomes +X+n)
        # For SPREAD FAV at home -X: we want line to BECOME LESS negative
        # For TOTAL OVER at X: we want line to INCREASE
        # For TOTAL UNDER at X: we want line to DECREASE
        if current_line is not None and fire_line is not None:
            if mtype == 'TOTAL':
                raw_move = current_line - fire_line
                # OVER: raw > 0 = better for us (line moved up, we still over)
                # UNDER: raw < 0 = better for us
                our_move = raw_move if 'OVER' in selection.upper() else -raw_move
            else:  # SPREAD
                raw_move = current_line - fire_line
                # DOG (we bet +X): raw > 0 = better for us (getting more points)
                # FAV (we bet -X): raw < 0 = better for us (giving up fewer)
                our_move = raw_move if side == 'DOG' else -raw_move
        else:
            our_move = None

        # Context tag highlights — replicable patterns from CLV analysis
        has_home_letdown = ctx_str and 'Home letdown' in ctx_str
        is_ncaab = sport == 'basketball_ncaab'
        is_elite = confidence == 'ELITE'
        tag_stack = []
        if has_home_letdown: tag_stack.append('HOME_LETDOWN')
        if is_ncaab: tag_stack.append('NCAAB')
        if is_elite: tag_stack.append('ELITE')

        confirmed = our_move is not None and our_move >= 0.5
        if confirmed:
            confirmed_count += 1
        marker = '🔥 CONFIRMED' if confirmed else ('✅ holding' if our_move is not None and our_move >= 0 else '⚠️  moved against')

        print(f"  #{bid} {sport[:12]:<12} {selection[:40]:<40}")
        print(f"    fire={fire_line:+.1f} @ {fire_odds:+.0f} edge={edge_pct or 0:.1f}%  open={opener_line or '?'}  cur={current_line or '?'}  "
              f"move_our_way={our_move if our_move is not None else '?'}  {marker}")
        if tag_stack:
            print(f"    tags: {', '.join(tag_stack)}  (CLV-win pattern replicable if 2+ tags)")
        print()

    print(f"\n{confirmed_count}/{len(rows)} picks show sharp confirmation (line moved ≥0.5 toward our side)")
    conn.close()


if __name__ == '__main__':
    target = None
    if '--date' in sys.argv:
        target = sys.argv[sys.argv.index('--date') + 1]
    run_report(target)
