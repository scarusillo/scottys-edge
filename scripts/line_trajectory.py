"""Multi-point line-trajectory features for picks.

Today (v25.80) we record `opener_move` — a single delta between opener and
fire. That collapses the entire line history into one number. Three very
different scenarios all produce the same `opener_move=+0.5`:
  A. Late steam — flat all day, +0.5 in the last hour (sharp move)
  B. Steady drift — +0.05 per hour over 12 hours (retail flow)
  C. Overshoot+revert — went to +1.0, came back to +0.5 (we're catching the tail)

This module computes 3 features that distinguish those scenarios:
  late_move_share — fraction of total |move| that happened in last hour before fire
  n_steps         — count of distinct line values seen between opener and fire
  max_overshoot   — peak distance the line traveled minus where it ended up

Usage (called from backfill scripts + future fire-time logic):
    from line_trajectory import compute_trajectory
    feats = compute_trajectory(conn, event_id, 'TOTAL', '2026-04-23T15:00:00',
                               side_type='UNDER', selection='Phillies@Cubs UNDER 9.0')
    if feats:
        print(feats['late_move_share'], feats['n_steps'], feats['max_overshoot'])

Returns None if insufficient snapshots (<2) or unsupported market_type.

For backfills covering bets older than 7 days, the caller should ATTACH the
archive DB as 'arc' first (see scripts/archive_db.py:attach_archive). Without
that, only main-DB odds are queried and old bets will return None.
"""
import os
import sqlite3
from datetime import datetime, timedelta


def _direction_sign(market_type, side_type, selection):
    """Returns +1 if line moving UP is good for our side, -1 if DOWN is good.
    Matches the convention used by opener_move."""
    sel_u = (selection or '').upper()
    if market_type == 'TOTAL':
        if 'OVER' in sel_u: return 1
        if 'UNDER' in sel_u: return -1
        return None
    if market_type == 'SPREAD':
        if side_type == 'DOG': return 1
        if side_type == 'FAVORITE': return -1
        return None
    return None


def _parse_fire_dt(fire_time_iso):
    """Parse ISO timestamp into a naive UTC datetime."""
    s = (fire_time_iso or '').replace('Z', '').replace('+00:00', '')
    s = s[:19]  # strip any sub-seconds
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def compute_trajectory(conn, event_id, market_type, fire_time_iso,
                       side_type=None, selection=None):
    """Returns trajectory feature dict, or None if insufficient data.

    Caller should ATTACH archive DB as 'arc' for bets older than 7 days.
    """
    if market_type not in ('SPREAD', 'TOTAL'):
        return None
    sign = _direction_sign(market_type, side_type, selection)
    if sign is None:
        return None
    fire_dt = _parse_fire_dt(fire_time_iso)
    if not fire_dt:
        return None
    market = 'spreads' if market_type == 'SPREAD' else 'totals'

    # Opener line (averaged across books)
    opener_row = conn.execute(
        "SELECT AVG(line) FROM openers WHERE event_id=? AND market=? AND line IS NOT NULL",
        (event_id, market)).fetchone()
    opener_line = opener_row[0] if opener_row and opener_row[0] is not None else None
    if opener_line is None:
        return None

    # Detect whether archive is attached
    dbs = [r[1] for r in conn.execute("PRAGMA database_list").fetchall()]
    has_arc = 'arc' in dbs

    fire_str = fire_dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Pull all snapshots <= fire time, averaged across books per snapshot
    if has_arc:
        sql = """
            SELECT ts, AVG(line) AS avg_line FROM (
                SELECT snapshot_date || 'T' || snapshot_time AS ts, line FROM odds
                WHERE event_id=? AND market=? AND line IS NOT NULL
                  AND (snapshot_date || 'T' || snapshot_time) <= ?
                UNION ALL
                SELECT snapshot_date || 'T' || snapshot_time AS ts, line FROM arc.odds_archive
                WHERE event_id=? AND market=? AND line IS NOT NULL
                  AND (snapshot_date || 'T' || snapshot_time) <= ?
            )
            GROUP BY ts ORDER BY ts
        """
        params = (event_id, market, fire_str, event_id, market, fire_str)
    else:
        sql = """
            SELECT snapshot_date || 'T' || snapshot_time AS ts, AVG(line) AS avg_line
            FROM odds
            WHERE event_id=? AND market=? AND line IS NOT NULL
              AND (snapshot_date || 'T' || snapshot_time) <= ?
            GROUP BY ts ORDER BY ts
        """
        params = (event_id, market, fire_str)
    snaps = conn.execute(sql, params).fetchall()

    if len(snaps) < 2:
        return None

    # Direction-adjusted "our_move" series relative to opener
    series = [(ts, sign * (line - opener_line)) for ts, line in snaps]
    final_move = series[-1][1]

    # ── Feature 1: late_move_share ──
    # |move in last hour before fire| / |total move from opener|.
    # 0 = no move in last hour.  1 = all move happened in last hour.
    hour_ago = fire_dt - timedelta(hours=1)
    hour_ago_str = hour_ago.strftime('%Y-%m-%dT%H:%M:%S')
    move_at_hour_ago = 0.0  # default: opener was the reference
    for ts, mv in series:
        if ts <= hour_ago_str:
            move_at_hour_ago = mv
        else:
            break
    delta_last_hour = final_move - move_at_hour_ago
    total_move = final_move  # relative to opener, which is series[0]
    if abs(total_move) < 0.01:
        late_move_share = 0.0
    else:
        late_move_share = min(1.0, abs(delta_last_hour) / abs(total_move))

    # ── Feature 2: n_steps ──
    # Distinct line values seen, rounded to 0.5 (filter sub-half-point noise).
    # Lower = sharper (one decisive move). Higher = drift.
    distinct = {round(opener_line * 2) / 2}
    for ts, mv in series:
        distinct.add(round((opener_line + mv * sign) * 2) / 2)
    n_steps = len(distinct)

    # ── Feature 3: max_overshoot ──
    # Max value the our_move series reached minus the final move.
    # >0 means line traveled further toward us at some point and then reverted.
    max_move = max(mv for _, mv in series)
    max_overshoot = max(0.0, max_move - final_move)

    return {
        'opener_line': round(opener_line, 3),
        'final_move': round(final_move, 3),
        'late_move_share': round(late_move_share, 3),
        'n_steps': n_steps,
        'max_overshoot': round(max_overshoot, 3),
        'snapshots_used': len(series),
    }


if __name__ == '__main__':
    # CLI smoke test on a few recent bets
    DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(DB, timeout=30)
    try:
        from archive_db import attach_archive
        attach_archive(conn)
    except Exception:
        pass
    rows = conn.execute("""
        SELECT id, created_at, sport, event_id, market_type, side_type, selection,
               line, opener_line, opener_move
        FROM bets
        WHERE opener_move IS NOT NULL AND market_type IN ('SPREAD','TOTAL')
        ORDER BY id DESC LIMIT 10
    """).fetchall()
    print(f"{'id':>5s} {'sport':14s} {'sel':40s} {'opener_move':>11s} {'late':>5s} {'steps':>5s} {'over':>5s} {'snaps':>5s}")
    for r in rows:
        bid, created, sport, eid, mtype, st, sel, line, ol, om = r
        feats = compute_trajectory(conn, eid, mtype, created, side_type=st, selection=sel)
        if feats:
            print(f"{bid:>5d} {sport[:14]:14s} {sel[:40]:40s} {om:>+10.2f}  "
                  f"{feats['late_move_share']:>5.2f} {feats['n_steps']:>5d} "
                  f"{feats['max_overshoot']:>5.2f} {feats['snapshots_used']:>5d}")
        else:
            print(f"{bid:>5d} {sport[:14]:14s} {sel[:40]:40s} (insufficient data)")
