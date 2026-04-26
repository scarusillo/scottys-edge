"""
Pick-grading admin CLI commands — manual scrub (TAINTED) and unscrub.
Used when the auto-grader gets the wrong answer or when a pick needs to
be nullified post-fire (rainout, lineup change after fire, etc.).

Extracted from main.py in v26.0 Phase 8 (CLI modularization).

Re-exported from main for back-compat — `from main import cmd_X` keeps
working so the dispatcher in main.py + any external scripts that imported
these directly are unchanged.
"""
import os
import sys


def cmd_scrub(args):
    """Scrub a bet: mark bets.units=0/result='TAINTED' and write a matching
    full-column graded_bets row so it's excluded from every downstream query.

    Usage: python main.py scrub <bet_id> [reason]

    Replaces the ad-hoc direct-SQL scrub pattern that left minimal graded_bets
    rows and tripped the code auditor. One path, full column set, audit-traceable.
    """
    import sqlite3
    from datetime import datetime
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

    if not args or args[0].startswith('--'):
        print("Usage: python main.py scrub <bet_id> [reason]")
        return
    try:
        bid = int(args[0])
    except ValueError:
        print(f"bet_id must be an integer, got: {args[0]}"); return
    reason = ' '.join(args[1:]).strip() if len(args) > 1 else 'manual scrub'

    conn = sqlite3.connect(db)
    try:
        bet = conn.execute("""
            SELECT id, created_at, sport, event_id, market_type, selection,
                   book, line, odds, edge_pct, confidence, units, result,
                   side_type, spread_bucket, edge_bucket, timing,
                   context_factors, context_confirmed, market_tier,
                   model_spread, day_of_week
            FROM bets WHERE id = ?
        """, (bid,)).fetchone()
        if not bet:
            print(f"Bet id={bid} not found."); return

        (_id, created, sport, eid, mtype, sel, book, line, odds, edge, conf,
         units, result, side_type, spread_bucket, edge_bucket, timing,
         context, context_confirmed, market_tier, model_spread, dow) = bet

        if result == 'TAINTED' and units == 0:
            existing = conn.execute("SELECT id FROM graded_bets WHERE bet_id=?", (bid,)).fetchone()
            if existing:
                print(f"Bet {bid} already scrubbed (bets + graded_bets consistent). No-op.")
                return
            print(f"Bet {bid} is TAINTED in bets but missing graded_bets row — backfilling.")

        tagged_ctx = (context or '').strip()
        scrub_tag = f'SCRUB: {reason}'
        new_ctx = f'{tagged_ctx} | {scrub_tag}'.strip(' |') if tagged_ctx else scrub_tag

        now = datetime.now().isoformat()
        conn.execute("""
            UPDATE bets SET units=0, result='TAINTED', context_factors=?
            WHERE id=?
        """, (new_ctx, bid))

        existing = conn.execute("SELECT id, result FROM graded_bets WHERE bet_id=?", (bid,)).fetchone()
        if existing:
            conn.execute("""
                UPDATE graded_bets SET result='TAINTED', units=0, pnl_units=0,
                    context_factors=?, graded_at=?
                WHERE bet_id=?
            """, (new_ctx, now, bid))
            print(f"Updated existing graded_bets row (was {existing[1]}) to TAINTED.")
        else:
            conn.execute("""
                INSERT INTO graded_bets (graded_at, bet_id, sport, event_id, selection,
                    market_type, book, line, odds, edge_pct, confidence, units,
                    result, pnl_units, closing_line, clv, created_at,
                    side_type, spread_bucket, edge_bucket, timing,
                    context_factors, context_confirmed, market_tier, model_spread, day_of_week)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now, bid, sport, eid, sel, mtype, book, line, odds, edge, conf,
                  0, 'TAINTED', 0, None, None, created,
                  side_type, spread_bucket, edge_bucket, timing,
                  new_ctx, context_confirmed, market_tier, model_spread, dow))
            print(f"Inserted new graded_bets row marked TAINTED.")

        conn.commit()
        print(f"✓ Scrubbed bet {bid}: {sel[:60]} — reason: {reason}")
    finally:
        conn.close()




def cmd_unscrub(args):
    """Reverse a scrub: strip the SCRUB tag from context_factors, clear result
    so grader can re-compute, and delete the TAINTED graded_bets row.

    Usage: python main.py unscrub <bet_id>

    Use when a previously-scrubbed bet turns out to be valid after all. The
    next grade run will re-grade normally. Without this, manual SQL reversals
    leave the SCRUB tag in context_factors, which (a) misleads future audits
    and (b) will now force TAINTED at grade time per the v25.34 safety net.
    """
    import sqlite3, re as _re
    from datetime import datetime
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

    if not args or args[0].startswith('--'):
        print("Usage: python main.py unscrub <bet_id>")
        return
    try:
        bid = int(args[0])
    except ValueError:
        print(f"bet_id must be an integer, got: {args[0]}"); return

    conn = sqlite3.connect(db)
    try:
        bet = conn.execute("SELECT id, selection, context_factors, units FROM bets WHERE id = ?", (bid,)).fetchone()
        if not bet:
            print(f"Bet id={bid} not found."); return
        _, sel, ctx, units = bet

        # Strip SCRUB: ... segment from context (handles both ' | SCRUB:' suffix and standalone)
        new_ctx = _re.sub(r'\s*\|\s*SCRUB:\s*[^|]*', '', ctx or '').strip(' |')
        new_ctx = _re.sub(r'^SCRUB:\s*[^|]*\|\s*', '', new_ctx).strip(' |')
        if new_ctx == (ctx or ''):
            print(f"No SCRUB tag found in bet {bid}'s context_factors. Nothing to strip.")

        # Clear result on bets so grader re-computes; note units are not restored
        # here because scrub zeroed them. Caller must pass --units <n> if needed.
        target_units = units
        if '--units' in args:
            try:
                target_units = float(args[args.index('--units') + 1])
            except Exception:
                pass

        conn.execute("UPDATE bets SET result=NULL, units=?, context_factors=? WHERE id=?",
                     (target_units, new_ctx, bid))
        # Delete the TAINTED graded_bets row — next grade run will re-insert
        deleted = conn.execute("DELETE FROM graded_bets WHERE bet_id=? AND result='TAINTED'", (bid,)).rowcount
        conn.commit()
        print(f"✓ Unscrubbed bet {bid}: {sel[:60]}")
        print(f"  Stripped SCRUB tag. Deleted {deleted} TAINTED graded_bets row(s).")
        print(f"  Next `python main.py grade` will re-grade this bet normally.")
        if target_units == 0:
            print(f"  WARNING: units=0. Pass `--units <n>` to restore original stake.")
    finally:
        conn.close()


