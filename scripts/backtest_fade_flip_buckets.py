"""Backtest SPREAD/TOTAL fade-flip across edge buckets and movement bands.

For every SHARP_OPPOSES pick (line moved against our side past threshold),
simulate the flipped bet (opposite side, same line, assumed -110 odds).
Bucket the results by:
  1. Original edge_pct of the firing pick (how bold was our model?)
  2. Line movement magnitude (how confidently did sharps disagree?)
  3. Combined (both together)

Flipped bet outcome: invert WIN<->LOSS, PUSH stays PUSH.
Flipped stake: same 5u assumption. Flipped odds: -110 (avg for game lines).
"""
import os, sys, sqlite3
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from steam_engine import get_steam_signal

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

FLIP_ODDS = -110  # assumption for flipped side (typical game-line juice)
FLIP_STAKE = 5.0  # standard stake


def flip_outcome(res):
    if res == 'WIN':
        return 'LOSS'
    if res == 'LOSS':
        return 'WIN'
    return 'PUSH'


def flip_pnl(res, stake=FLIP_STAKE, odds=FLIP_ODDS):
    """Compute P/L for the flipped bet."""
    flipped = flip_outcome(res)
    if flipped == 'PUSH':
        return 0.0
    if flipped == 'WIN':
        # -110 odds pays 100/110 per unit stake
        return stake * (100.0 / abs(odds))
    return -stake  # LOSS


def bucket_edge(ed):
    if ed is None:
        return 'NULL'
    if ed < 12: return '08-12%'
    if ed < 16: return '12-16%'
    if ed < 20: return '16-20%'
    if ed < 25: return '20-25%'
    return '25%+'


def bucket_move(m):
    m = abs(m)
    if m < 0.6: return '0.5 (min thr)'
    if m < 1.0: return '0.5-1.0'
    if m < 1.5: return '1.0-1.5'
    if m < 2.0: return '1.5-2.0'
    return '2.0+'


def summarize(label, records):
    if not records:
        print(f"  {label:<20} (no picks)")
        return
    n = len(records)
    w = sum(1 for r in records if r['flip_res'] == 'WIN')
    l = sum(1 for r in records if r['flip_res'] == 'LOSS')
    p = sum(1 for r in records if r['flip_res'] == 'PUSH')
    pnl = sum(r['flip_pnl'] for r in records)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  {label:<20} {n:>3} | {w}W-{l}L-{p}P | {wr:>5.1f}% | {pnl:>+7.2f}u")


def main():
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT id, bet_id, sport, event_id, market_type, side_type, selection,
               line, odds, units, result, pnl_units, edge_pct, created_at
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-04-01'
          AND market_type IN ('TOTAL','SPREAD')
          AND result IN ('WIN','LOSS','PUSH')
        ORDER BY created_at
    """).fetchall()

    # Collect all SHARP_OPPOSES picks
    opposes = []
    for r in rows:
        (_id, bid, sport, eid, mt, st, sel, ln, od, un, res, pnl, ed, ca) = r
        if ln is None or eid is None:
            continue
        sel_l = (sel or '').lower()
        side_hint = (st or '').upper()
        if mt == 'TOTAL':
            steam_side = 'OVER' if 'OVER' in side_hint or 'over' in sel_l else 'UNDER'
        elif mt == 'SPREAD':
            if side_hint in ('FAVORITE', 'DOG'):
                steam_side = side_hint
            else:
                steam_side = 'FAVORITE' if (ln < 0) else 'DOG'
        else:
            continue
        sig, info = get_steam_signal(conn, sport, eid, mt, steam_side, ln, od)
        if sig != 'SHARP_OPPOSES':
            continue
        movement = info.get('movement', 0)
        opposes.append({
            'sport': sport, 'sel': sel, 'market': mt, 'line': ln, 'odds': od,
            'units': un, 'result': res, 'pnl_orig': pnl or 0.0,
            'edge': ed, 'movement': movement,
            'flip_res': flip_outcome(res), 'flip_pnl': flip_pnl(res),
        })

    print(f"Total SHARP_OPPOSES picks (TOTAL/SPREAD): {len(opposes)}")
    orig_pnl = sum(o['pnl_orig'] for o in opposes)
    flip_pnl_total = sum(o['flip_pnl'] for o in opposes)
    print(f"Original P/L (what actually happened): {orig_pnl:+.2f}u")
    print(f"Universal flip P/L (flip EVERY opposes): {flip_pnl_total:+.2f}u")
    print(f"Universal block P/L (block EVERY opposes): {-orig_pnl:+.2f}u")
    print()

    # === BUCKET BY ORIGINAL EDGE ===
    print("=" * 70)
    print("BY ORIGINAL EDGE_PCT (how bold was our model?)")
    print("=" * 70)
    by_edge = defaultdict(list)
    for o in opposes:
        by_edge[bucket_edge(o['edge'])].append(o)
    for lbl in ('08-12%','12-16%','16-20%','20-25%','25%+','NULL'):
        summarize(lbl, by_edge.get(lbl, []))

    # === BUCKET BY MOVEMENT MAGNITUDE ===
    print()
    print("=" * 70)
    print("BY LINE MOVEMENT MAGNITUDE (how far against us?)")
    print("=" * 70)
    by_move = defaultdict(list)
    for o in opposes:
        by_move[bucket_move(o['movement'])].append(o)
    for lbl in ('0.5 (min thr)','0.5-1.0','1.0-1.5','1.5-2.0','2.0+'):
        summarize(lbl, by_move.get(lbl, []))

    # === COMBINED: EDGE × MOVEMENT ===
    print()
    print("=" * 70)
    print("COMBINED: original edge × movement magnitude")
    print("=" * 70)
    combined = defaultdict(list)
    for o in opposes:
        combined[(bucket_edge(o['edge']), bucket_move(o['movement']))].append(o)
    print(f"  {'Edge':<10} {'Move':<16} {'N':>3} {'W-L-P':>10} {'Win%':>6} {'P/L':>8}")
    for (eb, mb), recs in sorted(combined.items()):
        w = sum(1 for r in recs if r['flip_res'] == 'WIN')
        l = sum(1 for r in recs if r['flip_res'] == 'LOSS')
        p = sum(1 for r in recs if r['flip_res'] == 'PUSH')
        pnl = sum(r['flip_pnl'] for r in recs)
        wr = w / (w + l) * 100 if (w + l) else 0
        print(f"  {eb:<10} {mb:<16} {len(recs):>3} {w}-{l}-{p:<6} {wr:>5.1f}% {pnl:>+7.2f}u")

    # === FLIP vs BLOCK vs LET RUN — head-to-head ===
    print()
    print("=" * 70)
    print("STRATEGY COMPARISON PER EDGE BUCKET")
    print("  'Fire': do nothing, let the pick run (actual history)")
    print("  'Block': skip the pick (save the loss or forgo the win)")
    print("  'Flip': bet the opposite side at -110")
    print("=" * 70)
    print(f"  {'Edge bucket':<12} {'N':>3} {'Fire':>9} {'Block':>9} {'Flip':>9} {'Best':>10}")
    for lbl in ('08-12%','12-16%','16-20%','20-25%','25%+'):
        recs = by_edge.get(lbl, [])
        if not recs:
            continue
        fire = sum(r['pnl_orig'] for r in recs)
        block = -fire
        flip = sum(r['flip_pnl'] for r in recs)
        best_val = max(fire, block, flip)
        if best_val == flip: best = 'FLIP'
        elif best_val == block: best = 'BLOCK'
        else: best = 'FIRE'
        print(f"  {lbl:<12} {len(recs):>3} {fire:>+7.2f}u {block:>+7.2f}u {flip:>+7.2f}u {best:>10}")


if __name__ == '__main__':
    main()
