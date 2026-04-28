"""v25.96 backfill: re-derive closing_odds + clv_odds_pct for graded rows that
were graded before the get_closing_line NULL-odds fix. Idempotent.

Targets graded_bets WHERE closing_odds IS NULL AND result IN ('WIN','LOSS','PUSH')
since 2026-04-15. Uses the existing grader compute_clv_split helper so math
matches live grading. Pulls closing snapshots from the live `odds` table first;
if the event has been tiered out (per v25.79), falls back to the archive DB
`odds_archive` table via ATTACH (read-only fallback, no DDL on live DB).
"""
import sqlite3, os, sys, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from grader import compute_clv_split, _market_key

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
ARCHIVE = os.path.join(os.path.dirname(__file__), '..', 'data',
                       'betting_model_archive.db')


def _normalize_selection(selection, market):
    """Mirror grader.get_closing_line normalization."""
    odds_selection = selection
    if selection.startswith('OVER ') or selection.startswith('Over '):
        odds_selection = 'Over'
    elif selection.startswith('UNDER ') or selection.startswith('Under '):
        odds_selection = 'Under'
    elif market == 'spreads':
        odds_selection = re.sub(r'\s*[+-]?\d+\.?\d*$', '', selection).strip()
    elif market == 'h2h':
        odds_selection = (selection.replace(' ML (cross-mkt)', '')
                                    .replace(' ML', '').strip())
    if '@' in odds_selection and ('OVER' in selection or 'UNDER' in selection):
        odds_selection = 'Over' if 'OVER' in selection else 'Under'
    return odds_selection


def _closing_from_table(cur, table_name, event_id, market, selection,
                       bet_book=None):
    """Same logic as grader.get_closing_line but parameterized on table.
    Returns (line, odds) or (None, None)."""
    odds_sel = _normalize_selection(selection, market)

    # Commence-time filter to exclude in-game live odds
    ct_row = cur.execute(
        f"SELECT commence_time FROM {table_name} "
        f"WHERE event_id=? AND commence_time IS NOT NULL LIMIT 1",
        (event_id,)).fetchone()
    ct_filter = " AND (snapshot_date || ' ' || snapshot_time) < ?"
    ct_params = (ct_row[0],) if ct_row and ct_row[0] else ()
    if not ct_params:
        ct_filter = ""

    # Priority 1: same book
    if bet_book:
        row = cur.execute(
            f"""SELECT line, odds FROM {table_name}
                WHERE event_id=? AND market=? AND selection=? AND book=?{ct_filter}
                ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1""",
            (event_id, market, odds_sel, bet_book) + ct_params).fetchone()
        if row and row[1] is not None:
            return row[0], row[1]

    # Priority 2: consensus across books with non-NULL odds (the v25.96 fix)
    rows = cur.execute(
        f"""SELECT o.line, o.odds FROM {table_name} o
            INNER JOIN (
                SELECT book, MAX(snapshot_date || ' ' || snapshot_time) AS m
                FROM {table_name}
                WHERE event_id=? AND market=? AND selection=?{ct_filter}
                GROUP BY book) latest ON o.book = latest.book
                  AND (o.snapshot_date || ' ' || o.snapshot_time) = latest.m
            WHERE o.event_id=? AND o.market=? AND o.selection=?{ct_filter}""",
        (event_id, market, odds_sel) + ct_params
        + (event_id, market, odds_sel) + ct_params).fetchall()

    rows_with_odds = [r for r in rows if r[1] is not None]
    if rows_with_odds and market in ('spreads', 'totals'):
        valid_lines = sorted([r[0] for r in rows_with_odds if r[0] is not None])
        if valid_lines:
            median_line = valid_lines[len(valid_lines) // 2]
            best = min(rows_with_odds,
                       key=lambda r: abs((r[0] or 0) - median_line))
            return median_line, best[1]
    elif rows_with_odds and market == 'h2h':
        # Median by implied prob — odds only; line stays None
        def _impl(o):
            o = float(o)
            if o > 0: return 100.0 / (o + 100.0)
            if o < 0: return abs(o) / (abs(o) + 100.0)
            return 0.5
        sorted_rows = sorted(rows_with_odds, key=lambda r: _impl(r[1]))
        median_row = sorted_rows[len(sorted_rows) // 2]
        return None, median_row[1]
    return None, None


def backfill(since='2026-04-15', dry_run=False):
    conn = sqlite3.connect(DB)
    conn.execute("ATTACH DATABASE ? AS arc", (ARCHIVE,))
    cur = conn.cursor()

    cur.execute("""
        SELECT id, bet_id, sport, event_id, selection, market_type, book,
               line, odds
        FROM graded_bets
        WHERE created_at >= ? AND result IN ('WIN','LOSS','PUSH')
          AND closing_odds IS NULL AND market_type != 'MONEYLINE'
        ORDER BY created_at
    """, (since,))
    rows = cur.fetchall()
    print(f'Eligible rows: {len(rows)}')

    fixed_live = 0
    fixed_arc = 0
    still_null = 0
    for r in rows:
        gid, bid, sport, eid, sel, mtype, book, line, odds = r
        market_key = _market_key(mtype, sel)

        # Try live odds first
        new_cl, new_co = _closing_from_table(cur, 'odds', eid, market_key,
                                              sel, bet_book=book)
        source = 'live'
        if new_co is None:
            new_cl, new_co = _closing_from_table(cur, 'arc.odds_archive', eid,
                                                  market_key, sel, bet_book=book)
            source = 'archive'
        if new_co is None:
            still_null += 1
            continue

        new_clv_line, new_clv_odds = compute_clv_split(
            line, new_cl, mtype, sel, bet_odds=odds, closing_odds=new_co)

        if dry_run:
            print(f'  would patch #{bid} ({source}) {sel[:50]:50} '
                  f'cl={new_cl} co={new_co} clv_o={new_clv_odds}')
        else:
            cur.execute("""
                UPDATE graded_bets
                   SET closing_line = COALESCE(?, closing_line),
                       closing_odds = ?,
                       clv_line     = COALESCE(?, clv_line),
                       clv_odds_pct = ?
                 WHERE id = ?
            """, (new_cl, new_co, new_clv_line, new_clv_odds, gid))
            if bid is not None:
                cur.execute("""
                    UPDATE bets
                       SET closing_line = COALESCE(?, closing_line),
                           closing_odds = ?,
                           clv_line     = COALESCE(?, clv_line),
                           clv_odds_pct = ?
                     WHERE id = ?
                """, (new_cl, new_co, new_clv_line, new_clv_odds, bid))

        if source == 'live': fixed_live += 1
        else: fixed_arc += 1

    if not dry_run:
        conn.commit()
    conn.execute("DETACH DATABASE arc")
    conn.close()
    print(f'Patched from live:    {fixed_live}')
    print(f'Patched from archive: {fixed_arc}')
    print(f'Still NULL (no odds anywhere): {still_null}')


if __name__ == '__main__':
    dry = '--dry-run' in sys.argv
    since = '2026-04-15'
    for a in sys.argv:
        if a.startswith('--since='):
            since = a.split('=', 1)[1]
    backfill(since=since, dry_run=dry)
