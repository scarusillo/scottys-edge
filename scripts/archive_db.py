"""Archive DB helpers for backtests.

Main DB keeps 7 days of odds/props/prop_snapshots live. Older rows are
moved to data/betting_model_archive.db by the retention job in cmd_grade.
Backtest scripts that need deep history should use these helpers to
transparently query both databases.

Tables in archive DB:
  - odds_archive            (same schema as main.odds)
  - props_archive           (same schema as main.props)
  - prop_snapshots_archive  (same schema as main.prop_snapshots)

Usage:

    import sqlite3
    from archive_db import attach_archive, full_odds_query

    conn = sqlite3.connect('data/betting_model.db')
    attach_archive(conn)  # Now conn can see arc.* tables

    # Full-history odds for a game (last 7 days from main + older from arc)
    rows = full_odds_query(conn, event_id='abc123').fetchall()
"""
import os
import sqlite3

DEFAULT_MAIN_DB = 'data/betting_model.db'
DEFAULT_ARCHIVE_DB = 'data/betting_model_archive.db'


def archive_path(main_db_path: str = DEFAULT_MAIN_DB) -> str:
    """Sibling path: data/betting_model_archive.db next to the main DB."""
    return os.path.join(os.path.dirname(main_db_path) or '.', 'betting_model_archive.db')


def attach_archive(conn: sqlite3.Connection, archive_db_path: str = None) -> None:
    """Attach the archive DB as alias 'arc'. Idempotent.

    After this, SQL can reference:
      - main.odds              live (7d)        or plain `odds`
      - arc.odds_archive       pre-7d history
      - main.props, arc.props_archive
      - main.prop_snapshots, arc.prop_snapshots_archive
    """
    path = archive_db_path or archive_path()
    if not os.path.exists(path):
        raise FileNotFoundError(f'archive DB not found at {path}')
    # Detect if already attached
    dbs = [r[1] for r in conn.execute('PRAGMA database_list').fetchall()]
    if 'arc' in dbs:
        return
    conn.execute(f"ATTACH DATABASE '{path}' AS arc")


def full_odds_query(conn: sqlite3.Connection, sport: str = None,
                    event_id: str = None, since: str = None):
    """Return a cursor over live + archived odds rows combined.

    Args:
      sport:    optional sport filter (e.g. 'basketball_nba')
      event_id: optional event filter
      since:    optional min snapshot_date (inclusive, YYYY-MM-DD)

    Returns: sqlite3.Cursor with columns matching the odds schema.
    """
    attach_archive(conn)
    clauses = []
    params = []
    if sport:
        clauses.append('sport = ?'); params.append(sport)
    if event_id:
        clauses.append('event_id = ?'); params.append(event_id)
    if since:
        clauses.append('snapshot_date >= ?'); params.append(since)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    # UNION ALL preserves duplicates; we never store the same row in both DBs
    # because the retention job moves-and-deletes, so this is safe.
    sql = f"""
        SELECT * FROM odds {where}
        UNION ALL
        SELECT * FROM arc.odds_archive {where}
        ORDER BY snapshot_date, snapshot_time
    """
    return conn.execute(sql, params * 2)


def full_props_query(conn: sqlite3.Connection, sport: str = None,
                     event_id: str = None, since: str = None):
    """Live + archived props combined. Same contract as full_odds_query."""
    attach_archive(conn)
    clauses = []
    params = []
    if sport:
        clauses.append('sport = ?'); params.append(sport)
    if event_id:
        clauses.append('event_id = ?'); params.append(event_id)
    if since:
        clauses.append('snapshot_date >= ?'); params.append(since)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    sql = f"""
        SELECT * FROM props {where}
        UNION ALL
        SELECT * FROM arc.props_archive {where}
        ORDER BY snapshot_date, snapshot_time
    """
    return conn.execute(sql, params * 2)


def full_prop_snapshots_query(conn: sqlite3.Connection, sport: str = None,
                              event_id: str = None, player: str = None,
                              since: str = None):
    """Live + archived prop_snapshots combined.

    `since` filters on captured_at (ISO datetime string).
    """
    attach_archive(conn)
    clauses = []
    params = []
    if sport:
        clauses.append('sport = ?'); params.append(sport)
    if event_id:
        clauses.append('event_id = ?'); params.append(event_id)
    if player:
        clauses.append('player = ?'); params.append(player)
    if since:
        clauses.append('captured_at >= ?'); params.append(since)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    sql = f"""
        SELECT * FROM prop_snapshots {where}
        UNION ALL
        SELECT * FROM arc.prop_snapshots_archive {where}
        ORDER BY captured_at
    """
    return conn.execute(sql, params * 2)


def archive_stats(conn: sqlite3.Connection) -> dict:
    """Report row counts + date ranges in both DBs. Useful for sanity checks."""
    attach_archive(conn)
    out = {}
    for label, tbl, date_col in [
        ('live.odds',             'odds',                        'snapshot_date'),
        ('arc.odds_archive',      'arc.odds_archive',            'snapshot_date'),
        ('live.props',            'props',                       'snapshot_date'),
        ('arc.props_archive',     'arc.props_archive',           'snapshot_date'),
        ('live.prop_snapshots',   'prop_snapshots',              'captured_at'),
        ('arc.prop_snapshots_archive','arc.prop_snapshots_archive','captured_at'),
    ]:
        try:
            r = conn.execute(f'SELECT COUNT(*), MIN({date_col}), MAX({date_col}) FROM {tbl}').fetchone()
            out[label] = {'count': r[0], 'min': r[1], 'max': r[2]}
        except sqlite3.OperationalError as e:
            out[label] = {'error': str(e)}
    return out


if __name__ == '__main__':
    # Quick CLI: python archive_db.py → prints stats
    conn = sqlite3.connect(DEFAULT_MAIN_DB)
    stats = archive_stats(conn)
    print(f"{'table':35s} {'count':>12s}  {'min':<20s}  {'max':<20s}")
    print('-' * 90)
    for label, s in stats.items():
        if 'error' in s:
            print(f"{label:35s}  ERROR: {s['error']}")
        else:
            print(f"{label:35s} {s['count']:>12,}  {s['min'] or '':<20s}  {s['max'] or '':<20s}")
