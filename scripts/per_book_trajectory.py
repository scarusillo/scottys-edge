"""Layer 2 line-movement analysis — per-book originator detection.

Layer 1 (line_trajectory.py) measures the SHAPE of how the consensus line
moved (n_steps, late_move_share, max_overshoot). It tells us "stable" vs
"drift" but not WHO moved the line.

Layer 2 looks at each NY-legal book individually and answers:
  - Which book moved first?
  - Did sharp books move while soft books stayed flat? (sharp signal)
  - Did soft books move while sharp books stayed flat? (retail signal)
  - Did all books move together within minutes? (steam / news)
  - Are sharp and soft books diverged in price? (value gap)

Sharp/soft convention (from main.py:1053):
  SHARP = FanDuel, BetRivers
  SOFT  = DraftKings, BetMGM, Caesars, Fanatics, ESPN BET

Usage:
    from per_book_trajectory import compute_per_book_trajectory, classify_move

    per_book = compute_per_book_trajectory(conn, event_id, 'TOTAL',
                                           fire_time, 'UNDER', 'PHI@CHC UNDER 9.0')
    cls = classify_move(per_book)
    # cls['classification']     → 'SHARP_LEAD' / 'SOFT_LEAD' / 'STEAM' / 'DIVERGENT'
    # cls['originator_book']    → string or None
    # cls['move_breadth']       → int (count of books that moved >= 0.25)
    # cls['sharp_soft_divergence'] → float (sharp_avg_line - soft_avg_line at fire)

Caller should ATTACH archive DB as 'arc' for bets older than 7 days
(see scripts/archive_db.py:attach_archive).
"""
import os
import sqlite3
from datetime import datetime, timedelta

SHARP_BOOKS = ('FanDuel', 'BetRivers')
SOFT_BOOKS  = ('DraftKings', 'BetMGM', 'Caesars', 'Fanatics', 'ESPN BET')
ALL_NY_BOOKS = SHARP_BOOKS + SOFT_BOOKS

# Thresholds
MOVE_THRESHOLD = 0.25       # |move| at or above this counts as "the book moved"
STEAM_WINDOW_MIN = 30       # if all movers' first-move times are within this many minutes → STEAM
DIVERGENT_THRESHOLD = 0.5   # |sharp_avg - soft_avg| at or above this → DIVERGENT


def _direction_sign(market_type, side_type, selection):
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


def _parse_dt(iso):
    s = (iso or '').replace('Z', '').replace('+00:00', '')[:19]
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def compute_per_book_trajectory(conn, event_id, market_type, fire_time_iso,
                                side_type=None, selection=None):
    """Returns dict mapping book → {opener_line, fire_line, our_move, first_move_iso, n_snapshots}.
    Only includes NY-legal books that have at least 1 snapshot.
    Returns None if market_type unsupported or direction undeterminable.
    """
    if market_type not in ('SPREAD', 'TOTAL'):
        return None
    sign = _direction_sign(market_type, side_type, selection)
    if sign is None:
        return None
    fire_dt = _parse_dt(fire_time_iso)
    if not fire_dt:
        return None
    market = 'spreads' if market_type == 'SPREAD' else 'totals'
    fire_str = fire_dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Per-book opener (avg across snapshots tagged opener; fall back to earliest line)
    book_data = {}

    # Detect archive
    dbs = [r[1] for r in conn.execute("PRAGMA database_list").fetchall()]
    has_arc = 'arc' in dbs

    for book in ALL_NY_BOOKS:
        # Opener (from openers table — already book-specific)
        opener_row = conn.execute(
            "SELECT line FROM openers WHERE event_id=? AND market=? AND book=? "
            "AND line IS NOT NULL ORDER BY snapshot_date, timestamp LIMIT 1",
            (event_id, market, book)).fetchone()
        opener_line = opener_row[0] if opener_row and opener_row[0] is not None else None

        # Time series of (timestamp, line) for this book up to fire
        if has_arc:
            sql = """
                SELECT ts, line FROM (
                    SELECT snapshot_date || 'T' || snapshot_time AS ts, line FROM odds
                    WHERE event_id=? AND market=? AND book=? AND line IS NOT NULL
                      AND (snapshot_date || 'T' || snapshot_time) <= ?
                    UNION ALL
                    SELECT snapshot_date || 'T' || snapshot_time AS ts, line
                    FROM arc.odds_archive
                    WHERE event_id=? AND market=? AND book=? AND line IS NOT NULL
                      AND (snapshot_date || 'T' || snapshot_time) <= ?
                ) ORDER BY ts
            """
            params = (event_id, market, book, fire_str,
                      event_id, market, book, fire_str)
        else:
            sql = """
                SELECT snapshot_date || 'T' || snapshot_time AS ts, line FROM odds
                WHERE event_id=? AND market=? AND book=? AND line IS NOT NULL
                  AND (snapshot_date || 'T' || snapshot_time) <= ?
                ORDER BY ts
            """
            params = (event_id, market, book, fire_str)
        snaps = conn.execute(sql, params).fetchall()
        if not snaps and opener_line is None:
            continue
        # Fall back to earliest snapshot as opener if openers table missing
        if opener_line is None and snaps:
            opener_line = snaps[0][1]
        if opener_line is None:
            continue
        fire_line = snaps[-1][1] if snaps else opener_line
        our_move = sign * (fire_line - opener_line)

        # First time this book's line moved >= MOVE_THRESHOLD/2 from opener
        first_move_iso = None
        for ts, line in snaps:
            if abs(line - opener_line) >= MOVE_THRESHOLD / 2:
                first_move_iso = ts
                break

        book_data[book] = {
            'opener_line': round(opener_line, 3),
            'fire_line': round(fire_line, 3),
            'our_move': round(our_move, 3),
            'first_move_iso': first_move_iso,
            'n_snapshots': len(snaps),
        }
    return book_data if book_data else None


def classify_move(per_book_data):
    """Given output of compute_per_book_trajectory, classify the move type.

    Returns dict with:
      classification     str   one of STABLE / SHARP_LEAD / SOFT_LEAD / STEAM / DIVERGENT / MIXED
      originator_book    str   first book to move >= MOVE_THRESHOLD/2 (None if no movers)
      move_breadth       int   count of books with |our_move| >= MOVE_THRESHOLD
      sharp_movers       int   count of SHARP books that moved
      soft_movers        int   count of SOFT books that moved
      sharp_soft_divergence float (sharp avg fire_line) - (soft avg fire_line)
                              positive = sharps higher, negative = softs higher
    """
    if not per_book_data:
        return None

    movers = {b: d for b, d in per_book_data.items() if abs(d['our_move']) >= MOVE_THRESHOLD}
    sharp_movers = [b for b in movers if b in SHARP_BOOKS]
    soft_movers = [b for b in movers if b in SOFT_BOOKS]

    # Originator = book with earliest first_move_iso that's also a mover
    originator = None
    earliest_iso = None
    for b, d in movers.items():
        if d['first_move_iso']:
            if earliest_iso is None or d['first_move_iso'] < earliest_iso:
                earliest_iso = d['first_move_iso']
                originator = b

    # Sharp vs soft divergence at fire time (positive = sharps line higher)
    sharp_avg = None
    soft_avg = None
    sharp_lines = [d['fire_line'] for b, d in per_book_data.items() if b in SHARP_BOOKS]
    soft_lines  = [d['fire_line'] for b, d in per_book_data.items() if b in SOFT_BOOKS]
    if sharp_lines:
        sharp_avg = sum(sharp_lines) / len(sharp_lines)
    if soft_lines:
        soft_avg = sum(soft_lines) / len(soft_lines)
    divergence = round(sharp_avg - soft_avg, 3) if (sharp_avg is not None and soft_avg is not None) else None

    # Direction concordance — do all movers agree on sign?
    if movers:
        signs = set(1 if d['our_move'] > 0 else -1 for d in movers.values())
        all_agree = len(signs) == 1
    else:
        all_agree = True

    # Classify
    if not movers:
        cls = 'STABLE'
    elif divergence is not None and abs(divergence) >= DIVERGENT_THRESHOLD:
        cls = 'DIVERGENT'
    elif not all_agree:
        cls = 'MIXED'
    else:
        # Steam test: did all movers' first-move times cluster within window?
        first_times = [_parse_dt(d['first_move_iso']) for d in movers.values()
                       if d['first_move_iso']]
        if len(first_times) >= 2:
            spread_min = (max(first_times) - min(first_times)).total_seconds() / 60
            is_steam = spread_min <= STEAM_WINDOW_MIN and len(movers) >= 4
        else:
            is_steam = False

        if is_steam:
            cls = 'STEAM'
        elif sharp_movers and not soft_movers:
            cls = 'SHARP_LEAD'
        elif soft_movers and not sharp_movers:
            cls = 'SOFT_LEAD'
        elif sharp_movers and soft_movers:
            # Both moved — was sharp first?
            if originator in SHARP_BOOKS:
                cls = 'SHARP_LEAD'
            else:
                cls = 'SOFT_LEAD'
        else:
            cls = 'MIXED'

    return {
        'classification': cls,
        'originator_book': originator,
        'move_breadth': len(movers),
        'sharp_movers': len(sharp_movers),
        'soft_movers': len(soft_movers),
        'sharp_soft_divergence': divergence,
    }


if __name__ == '__main__':
    DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(DB, timeout=30)
    try:
        from archive_db import attach_archive
        attach_archive(conn)
    except Exception:
        pass
    rows = conn.execute("""
        SELECT id, created_at, sport, event_id, market_type, side_type, selection,
               opener_move, n_steps
        FROM bets
        WHERE opener_move IS NOT NULL AND late_move_share IS NOT NULL
        ORDER BY id DESC LIMIT 15
    """).fetchall()
    print(f"{'id':>5s} {'sport':12s} {'sel':38s} {'opener':>7s} {'cls':12s} {'orig':12s} {'breadth':>7s} {'sh/so':>6s} {'div':>6s}")
    for r in rows:
        bid, ct, sport, eid, mt, st, sel, om, ns = r
        pb = compute_per_book_trajectory(conn, eid, mt, ct, side_type=st, selection=sel)
        cls = classify_move(pb) if pb else None
        if cls:
            div = f"{cls['sharp_soft_divergence']:+.2f}" if cls['sharp_soft_divergence'] is not None else '—'
            print(f"{bid:>5d} {sport[:12]:12s} {sel[:38]:38s} {om:>+6.2f} "
                  f"{cls['classification']:12s} {(cls['originator_book'] or '—')[:12]:12s} "
                  f"{cls['move_breadth']:>7d} {cls['sharp_movers']}/{cls['soft_movers']:<3d} {div:>6s}")
        else:
            print(f"{bid:>5d} {sport[:12]:12s} {sel[:38]:38s} (no data)")
