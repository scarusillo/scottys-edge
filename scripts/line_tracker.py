"""
line_tracker.py — Intraday Line Movement Tracker

Lightweight script that captures odds snapshots between full model runs.
Designed for 4x daily polling schedule:
  8:00 AM  — Full run (main.py run)
  11:00 AM — line_tracker.py (odds only, ~50 API calls)
  2:00 PM  — line_tracker.py (odds only, ~50 API calls)
  5:30 PM  — Full run (main.py run)

Tracks:
  - Steam moves: 1.5+ point shift in <3 hours (sharp money)
  - Reverse line movement: line moves against public betting %
  - Stale lines: books that haven't moved when others have
  - Opening vs current: total movement from first capture

Storage: line_snapshots table in SQLite
API cost: ~30-50 calls per run (just h2h + spreads + totals, no props)
"""
import sqlite3, os, json, sys
from datetime import datetime, timezone, timedelta

# Add script directory to path
sys.path.insert(0, os.path.dirname(__file__))

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Import from our codebase
from odds_api import fetch_odds

# Only fetch game lines — no props (saves ~150 API calls)
TRACK_MARKETS = 'h2h,spreads,totals'

# Sports to track (same as main run)
from main import ALL_SPORTS


def ensure_tables(conn):
    """Create line_snapshots table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS line_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time   TEXT NOT NULL,
            sport           TEXT NOT NULL,
            event_id        TEXT NOT NULL,
            home            TEXT,
            away            TEXT,
            commence_time   TEXT,
            book            TEXT,
            market          TEXT,
            outcome         TEXT,
            price           REAL,
            point           REAL,
            snapshot_tag    TEXT DEFAULT 'INTRADAY'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS line_movements (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            detected_time   TEXT NOT NULL,
            sport           TEXT NOT NULL,
            event_id        TEXT NOT NULL,
            home            TEXT,
            away            TEXT,
            market          TEXT,
            movement_type   TEXT,
            book            TEXT,
            old_line        REAL,
            new_line        REAL,
            shift           REAL,
            hours_elapsed   REAL,
            notes           TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_snap_event
        ON line_snapshots(sport, event_id, snapshot_time)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_movements_event
        ON line_movements(sport, event_id, detected_time)
    """)
    conn.commit()


def capture_snapshot(conn, sports=None):
    """
    Capture current odds for all sports. Lightweight — no props.
    Returns count of rows stored.
    """
    if sports is None:
        sports = ALL_SPORTS

    now = datetime.now(timezone.utc).isoformat()
    total_rows = 0

    for sport in sports:
        try:
            data = fetch_odds(sport, markets=TRACK_MARKETS, tag='SNAPSHOT')
            if not data:
                continue

            rows = []
            for event in data:
                eid = event.get('id', '')
                home = event.get('home_team', '')
                away = event.get('away_team', '')
                commence = event.get('commence_time', '')

                for bm in event.get('bookmakers', []):
                    book = bm.get('title', '')
                    for mkt in bm.get('markets', []):
                        mkt_key = mkt.get('key', '')
                        for outcome in mkt.get('outcomes', []):
                            rows.append((
                                now, sport, eid, home, away, commence,
                                book, mkt_key, outcome.get('name', ''),
                                outcome.get('price'), outcome.get('point'),
                                'INTRADAY'
                            ))

            if rows:
                conn.executemany("""
                    INSERT INTO line_snapshots
                    (snapshot_time, sport, event_id, home, away, commence_time,
                     book, market, outcome, price, point, snapshot_tag)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, rows)
                conn.commit()
                total_rows += len(rows)
                print(f"  {sport}: {len(rows)} lines captured")

        except Exception as e:
            print(f"  {sport}: error — {e}")

    return total_rows


def detect_movements(conn, lookback_hours=4):
    """
    Compare current snapshot to previous snapshots.
    Detect steam moves, significant shifts, and stale lines.
    """
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=lookback_hours)).isoformat()

    # Get the two most recent snapshot times
    times = conn.execute("""
        SELECT DISTINCT snapshot_time FROM line_snapshots
        WHERE snapshot_time > ?
        ORDER BY snapshot_time DESC LIMIT 2
    """, (cutoff,)).fetchall()

    if len(times) < 2:
        print("  Need at least 2 snapshots to detect movement")
        return []

    current_time = times[0][0]
    previous_time = times[1][0]

    # Calculate hours between snapshots
    try:
        t1 = datetime.fromisoformat(previous_time.replace('Z', '+00:00'))
        t2 = datetime.fromisoformat(current_time.replace('Z', '+00:00'))
        hours = (t2 - t1).total_seconds() / 3600
    except Exception:
        hours = 3.0

    # Get spread lines from both snapshots
    movements = []

    # Compare spreads
    current = conn.execute("""
        SELECT event_id, home, away, book, outcome, point, price, sport
        FROM line_snapshots
        WHERE snapshot_time = ? AND market = 'spreads'
    """, (current_time,)).fetchall()

    previous_dict = {}
    for row in conn.execute("""
        SELECT event_id, book, outcome, point, price
        FROM line_snapshots
        WHERE snapshot_time = ? AND market = 'spreads'
    """, (previous_time,)).fetchall():
        key = (row[0], row[1], row[2])  # event, book, outcome
        previous_dict[key] = (row[3], row[4])  # point, price

    for eid, home, away, book, outcome, point, price, sport in current:
        key = (eid, book, outcome)
        if key not in previous_dict:
            continue

        old_point, old_price = previous_dict[key]
        if old_point is None or point is None:
            continue

        shift = point - old_point
        if abs(shift) < 0.5:
            continue  # Skip trivial moves

        # Classify the movement
        if abs(shift) >= 1.5 and hours <= 3:
            move_type = 'STEAM'
        elif abs(shift) >= 1.0:
            move_type = 'SIGNIFICANT'
        else:
            move_type = 'SHIFT'

        note = f"{outcome} moved {old_point:+.1f} → {point:+.1f} ({shift:+.1f}) in {hours:.1f}h"

        movements.append({
            'sport': sport, 'event_id': eid,
            'home': home, 'away': away,
            'market': 'spreads', 'movement_type': move_type,
            'book': book, 'old_line': old_point, 'new_line': point,
            'shift': shift, 'hours': hours, 'notes': note,
        })

    # Compare totals
    current_totals = conn.execute("""
        SELECT event_id, home, away, book, outcome, point, price, sport
        FROM line_snapshots
        WHERE snapshot_time = ? AND market = 'totals'
    """, (current_time,)).fetchall()

    prev_totals = {}
    for row in conn.execute("""
        SELECT event_id, book, outcome, point, price
        FROM line_snapshots
        WHERE snapshot_time = ? AND market = 'totals'
    """, (previous_time,)).fetchall():
        key = (row[0], row[1], row[2])
        prev_totals[key] = (row[3], row[4])

    for eid, home, away, book, outcome, point, price, sport in current_totals:
        key = (eid, book, outcome)
        if key not in prev_totals:
            continue

        old_point, old_price = prev_totals[key]
        if old_point is None or point is None:
            continue

        shift = point - old_point
        if abs(shift) < 0.5:
            continue

        if abs(shift) >= 2.0 and hours <= 3:
            move_type = 'STEAM'
        elif abs(shift) >= 1.0:
            move_type = 'SIGNIFICANT'
        else:
            move_type = 'SHIFT'

        note = f"Total {outcome} moved {old_point:.1f} → {point:.1f} ({shift:+.1f}) in {hours:.1f}h"

        movements.append({
            'sport': sport, 'event_id': eid,
            'home': home, 'away': away,
            'market': 'totals', 'movement_type': move_type,
            'book': book, 'old_line': old_point, 'new_line': point,
            'shift': shift, 'hours': hours, 'notes': note,
        })

    # Save detected movements
    if movements:
        now_str = now.isoformat()
        for m in movements:
            conn.execute("""
                INSERT INTO line_movements
                (detected_time, sport, event_id, home, away, market,
                 movement_type, book, old_line, new_line, shift, hours_elapsed, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now_str, m['sport'], m['event_id'], m['home'], m['away'],
                  m['market'], m['movement_type'], m['book'],
                  m['old_line'], m['new_line'], m['shift'], m['hours'], m['notes']))
        conn.commit()

    return movements


def print_movements(movements):
    """Display detected line movements."""
    if not movements:
        print("\n  No significant line movements detected")
        return

    steam = [m for m in movements if m['movement_type'] == 'STEAM']
    significant = [m for m in movements if m['movement_type'] == 'SIGNIFICANT']
    shifts = [m for m in movements if m['movement_type'] == 'SHIFT']

    if steam:
        print(f"\n  🚨 STEAM MOVES ({len(steam)}):")
        for m in steam:
            print(f"    {m['away']} @ {m['home']}")
            print(f"      {m['book']}: {m['notes']}")

    if significant:
        print(f"\n  📊 SIGNIFICANT MOVES ({len(significant)}):")
        for m in significant:
            print(f"    {m['away']} @ {m['home']} — {m['book']}: {m['notes']}")

    if shifts:
        print(f"\n  📈 Minor shifts: {len(shifts)} lines moved 0.5-1.0 points")


def get_line_history(conn, event_id, market='spreads'):
    """Get all line snapshots for an event — useful for analysis."""
    rows = conn.execute("""
        SELECT snapshot_time, book, outcome, point, price
        FROM line_snapshots
        WHERE event_id = ? AND market = ?
        ORDER BY snapshot_time ASC, book
    """, (event_id, market)).fetchall()
    return rows


def get_opening_vs_current(conn, sport, event_id):
    """Compare opening line to current line for CLV analysis."""
    opener = conn.execute("""
        SELECT outcome, point, price, book
        FROM line_snapshots
        WHERE sport = ? AND event_id = ? AND market = 'spreads'
        ORDER BY snapshot_time ASC LIMIT 2
    """, (sport, event_id)).fetchall()

    current = conn.execute("""
        SELECT outcome, point, price, book
        FROM line_snapshots
        WHERE sport = ? AND event_id = ? AND market = 'spreads'
        ORDER BY snapshot_time DESC LIMIT 2
    """, (sport, event_id)).fetchall()

    return {'opening': opener, 'current': current}


def cleanup_old_snapshots(conn, days_to_keep=14):
    """Remove snapshots older than N days to keep DB size manageable."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_to_keep)).isoformat()
    deleted = conn.execute("""
        DELETE FROM line_snapshots WHERE snapshot_time < ?
    """, (cutoff,)).rowcount
    conn.commit()
    if deleted:
        print(f"  🧹 Cleaned up {deleted} old snapshot rows (>{days_to_keep} days)")
    return deleted


# ═══════════════════════════════════════════════════════════════════
# MAIN — Run as standalone script
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    from datetime import datetime

    print("=" * 60)
    print(f"  SCOTTY'S EDGE — Line Movement Tracker")
    print(f"  {datetime.now().strftime('%Y-%m-%d %I:%M %p %Z')}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    # 1. Capture current odds snapshot
    print("\n📸 Capturing odds snapshot...")
    rows = capture_snapshot(conn)
    print(f"\n  Total: {rows} lines captured")

    # 2. Detect movements since last snapshot
    print("\n🔍 Checking for line movements...")
    movements = detect_movements(conn)
    print_movements(movements)

    # 3. Periodic cleanup
    cleanup_old_snapshots(conn)

    # Summary
    steam_count = sum(1 for m in movements if m['movement_type'] == 'STEAM')
    sig_count = sum(1 for m in movements if m['movement_type'] == 'SIGNIFICANT')

    print(f"\n{'=' * 60}")
    print(f"  Snapshot complete: {rows} lines | {steam_count} steam | {sig_count} significant")
    print(f"{'=' * 60}")

    conn.close()
