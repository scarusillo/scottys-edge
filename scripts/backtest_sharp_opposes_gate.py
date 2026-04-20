"""Dry-run SHARP_OPPOSES_BLOCK gate against post-Apr-1 graded_bets.

Verifies the gate would have caught the expected NHL + NCAA BB picks and
shows what it WOULD have done to the historical result.
"""
import os, sys, sqlite3
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from steam_engine import get_steam_signal

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

SHARP_OPPOSES_BLOCK_SPORTS = {'icehockey_nhl', 'baseball_ncaa'}


def main():
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT id, bet_id, sport, event_id, market_type, side_type, selection,
               line, odds, units, result, pnl_units, created_at
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-04-01'
          AND market_type IN ('TOTAL','SPREAD','MONEYLINE')
          AND result IN ('WIN','LOSS','PUSH')
        ORDER BY created_at
    """).fetchall()

    blocked = []
    allowed_but_opposes = defaultdict(list)

    for r in rows:
        (_id, bid, sport, eid, mt, st, sel, ln, od, un, res, pnl, ca) = r
        if ln is None or eid is None:
            continue
        sel_l = (sel or '').lower()
        side_hint = (st or '').upper()
        if mt == 'TOTAL':
            steam_side = 'OVER' if 'OVER' in side_hint or 'over' in sel_l else 'UNDER'
        elif mt == 'SPREAD':
            if side_hint in ('FAVORITE','DOG'):
                steam_side = side_hint
            else:
                steam_side = 'FAVORITE' if (ln is not None and ln < 0) else 'DOG'
        else:
            continue
        sig, info = get_steam_signal(conn, sport, eid, mt, steam_side, ln, od)
        if sig != 'SHARP_OPPOSES':
            continue
        movement = info.get('movement', 0)
        if sport in SHARP_OPPOSES_BLOCK_SPORTS:
            blocked.append((bid, sport, sel, mt, ln, od, un, res, pnl, movement, ca))
        else:
            allowed_but_opposes[sport].append((bid, sel, mt, ln, od, un, res, pnl, movement, ca))

    print(f"=" * 74)
    print(f"BLOCKED PICKS (would have been skipped): {len(blocked)}")
    print(f"=" * 74)
    w = sum(1 for x in blocked if x[7] == 'WIN')
    l = sum(1 for x in blocked if x[7] == 'LOSS')
    p = sum(1 for x in blocked if x[7] == 'PUSH')
    pnl_saved = sum(x[8] or 0 for x in blocked) * -1  # negative of actual pnl = saved
    print(f"  Original record: {w}W-{l}L-{p}P  |  Actual P/L: {sum(x[8] or 0 for x in blocked):+.2f}u")
    print(f"  Net savings from block: {pnl_saved:+.2f}u")
    print()
    print("  Detail:")
    print(f"  {'Date':<12} {'Sport':<16} {'Selection':<45} {'Move':>5} {'Res':>4} {'P/L':>7}")
    for b in sorted(blocked, key=lambda x: x[10]):
        bid, sp, sel, mt, ln, od, un, res, pnl, mv, ca = b
        date = ca[:10] if ca else '?'
        print(f"  {date:<12} {sp[:15]:<16} {(sel or '')[:44]:<45} {mv:>+5.1f} {res:>4} {pnl or 0:>+6.1f}u")

    print()
    print(f"=" * 74)
    print(f"ALLOWED SHARP_OPPOSES (tracked but not blocked):")
    print(f"=" * 74)
    for sport, picks in sorted(allowed_but_opposes.items()):
        w = sum(1 for x in picks if x[6] == 'WIN')
        l = sum(1 for x in picks if x[6] == 'LOSS')
        pnl = sum(x[7] or 0 for x in picks)
        print(f"  {sport:<28} {len(picks):>3} picks  {w}W-{l}L  {pnl:+.2f}u")
    print()
    print("These picks would still fire. Morning agent should grade them")
    print("weekly to decide if they should join the block list.")


if __name__ == '__main__':
    main()
