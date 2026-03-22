"""
fix_consensus_dupes.py — One-time fix to remove duplicate market_consensus rows.

The bug: INSERT OR REPLACE requires a UNIQUE constraint to replace rows.
market_consensus had no UNIQUE constraint, so every run added NEW rows instead
of updating existing ones. After 10 runs, each game had 10 stale copies.

The model always read the oldest (first inserted) copy → stale lines.

This script:
  1. Counts duplicates
  2. Keeps only the LATEST row per event
  3. Shows what was cleaned

Run once:
    python fix_consensus_dupes.py
"""
import sqlite3, os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def fix():
    conn = sqlite3.connect(DB_PATH)

    # Count total rows
    total = conn.execute("SELECT COUNT(*) FROM market_consensus").fetchone()[0]

    # Count unique events
    unique = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT sport, event_id, tag FROM market_consensus
        )
    """).fetchone()[0]

    dupes = total - unique
    print(f"  market_consensus: {total} total rows, {unique} unique events")
    print(f"  Duplicates to remove: {dupes}")

    if dupes == 0:
        print("  ✅ No duplicates found.")
        conn.close()
        return

    # Show some examples
    examples = conn.execute("""
        SELECT sport, home, away, COUNT(*) as copies
        FROM market_consensus
        GROUP BY sport, event_id, tag
        HAVING copies > 1
        ORDER BY copies DESC
        LIMIT 5
    """).fetchall()

    if examples:
        print(f"\n  Worst offenders:")
        for sport, home, away, copies in examples:
            label = sport.split('_')[-1].upper()
            print(f"    [{label}] {away} @ {home}: {copies} copies")

    # Keep only the latest row (highest id) per event
    conn.execute("""
        DELETE FROM market_consensus
        WHERE id NOT IN (
            SELECT MAX(id) FROM market_consensus
            GROUP BY sport, event_id, tag
        )
    """)
    conn.commit()

    remaining = conn.execute("SELECT COUNT(*) FROM market_consensus").fetchone()[0]
    print(f"\n  ✅ Cleaned: {total} → {remaining} rows ({dupes} duplicates removed)")

    # Also add a UNIQUE index to prevent this from ever happening again
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mc_unique
            ON market_consensus(sport, event_id, tag)
        """)
        conn.commit()
        print(f"  ✅ Added UNIQUE index — duplicates can never accumulate again")
    except Exception as e:
        print(f"  ⚠ Could not add UNIQUE index: {e}")
        print(f"  (The DELETE+INSERT fix in odds_api.py handles this anyway)")

    conn.close()


if __name__ == '__main__':
    fix()
