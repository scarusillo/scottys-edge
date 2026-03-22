"""
purge_underdog_mls.py — Remove false underdog ML picks from the database.

These picks were caused by lowering MIN_UNITS from 3.0 to 2.0, which let
big underdog MLs (+300 and higher) through the filter. The "edges" on these
are mirages — small spread disagreements amplified by low base probabilities.

Run once:
    python purge_underdog_mls.py

Then re-run the model:
    python main.py run --email --twitter
"""
import sqlite3, os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def purge():
    conn = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime('%Y-%m-%d')

    # Find underdog ML picks (+300 or higher odds)
    bad_picks = conn.execute("""
        SELECT id, sport, selection, odds, edge_pct, units, created_at
        FROM bets
        WHERE market_type = 'MONEYLINE'
        AND odds >= 300
        AND DATE(created_at) = ?
    """, (today,)).fetchall()

    if not bad_picks:
        print("  No underdog ML picks to purge.")
        conn.close()
        return

    print(f"  Found {len(bad_picks)} underdog ML picks to remove:\n")
    for b in bad_picks:
        bid, sport, sel, odds, edge, units, created = b
        label = sport.split('_')[-1].upper()
        print(f"    ❌ [{label}] {sel} | {odds:+.0f} | Edge: {edge:.1f}% | {units:.1f}u")

    # Delete from bets
    ids = [b[0] for b in bad_picks]
    placeholders = ','.join('?' * len(ids))
    deleted_bets = conn.execute(f"""
        DELETE FROM bets 
        WHERE id IN ({placeholders})
    """, ids).rowcount

    # Also clean from graded_bets if any got graded already
    deleted_graded = conn.execute(f"""
        DELETE FROM graded_bets 
        WHERE bet_id IN ({placeholders})
    """, ids).rowcount

    conn.commit()
    conn.close()

    print(f"\n  ✅ Purged {deleted_bets} bets, {deleted_graded} graded entries")
    print(f"  Re-run: python main.py run --email --twitter")


if __name__ == '__main__':
    purge()
