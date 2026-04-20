"""Backtest: generalize PROP_FADE_FLIP's model-vs-market-median logic to
SPREAD and TOTAL (game lines). Simulates what would have happened if every
historical pick where the median market line was on the opposite side of
our bet line got flipped to the opposite side.

Signal (analog of PROP_FADE_FLIP):
  - Median line across books for this event+market
  - Our bet line (what we fired at)
  - Gap = |our_bet_line - market_median|
  - Market disagrees when median is on opposite side of bet line from our pick direction
  - If gap > threshold AND market disagrees → FLIP

Unlike SHARP_OPPOSES (opener -> current movement), this is a STATIC snapshot
comparison. Matches the logic that flipped Pritchard / Cunningham / Dosunmu.
"""
import os, sys, sqlite3, statistics
from collections import defaultdict

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
FLIP_ODDS = -110
FLIP_STAKE = 5.0

THRESHOLDS = {
    'baseball_mlb':     {'total': 0.5,  'spread': 0.5},
    'baseball_ncaa':    {'total': 1.0,  'spread': 1.0},
    'basketball_nba':   {'total': 2.0,  'spread': 1.5},
    'basketball_ncaab': {'total': 2.0,  'spread': 2.0},
    'icehockey_nhl':    {'total': 0.5,  'spread': 0.5},
    'soccer_epl':       {'total': 0.25, 'spread': 0.5},
    'soccer_italy_serie_a':  {'total': 0.25, 'spread': 0.5},
    'soccer_spain_la_liga':  {'total': 0.25, 'spread': 0.5},
    'soccer_germany_bundesliga': {'total': 0.25, 'spread': 0.5},
    'soccer_france_ligue_one':   {'total': 0.25, 'spread': 0.5},
    'soccer_uefa_champs_league': {'total': 0.25, 'spread': 0.5},
    'soccer_usa_mls':       {'total': 0.25, 'spread': 0.5},
    'soccer_mexico_ligamx': {'total': 0.25, 'spread': 0.5},
}
DEFAULT_THR = {'total': 1.0, 'spread': 1.0}


def _thr(sport, mkt):
    m = mkt.lower()
    key = 'spread' if 'spread' in m else 'total'
    return THRESHOLDS.get(sport, DEFAULT_THR).get(key, 1.0)


def flip_outcome(res):
    if res == 'WIN': return 'LOSS'
    if res == 'LOSS': return 'WIN'
    return 'PUSH'


def flip_pnl(res):
    f = flip_outcome(res)
    if f == 'PUSH': return 0.0
    if f == 'WIN': return FLIP_STAKE * (100.0 / abs(FLIP_ODDS))
    return -FLIP_STAKE


def market_median_for_bet(conn, event_id, market_type, bet_selection):
    """Compute median line across all books for this event+market+side."""
    market_map = {'SPREAD': 'spreads', 'TOTAL': 'totals'}
    mkt = market_map.get(market_type)
    if not mkt:
        return None
    sel_up = (bet_selection or '').upper()
    if market_type == 'TOTAL':
        # Find OVER vs UNDER entries; we want the line (which is the same for both sides)
        rows = conn.execute("""
            SELECT DISTINCT book, line FROM odds
            WHERE event_id=? AND market=? AND line IS NOT NULL
        """, (event_id, mkt)).fetchall()
        if not rows:
            return None
        # Group by book, take median per book, then median across books
        per_book = defaultdict(list)
        for b, ln in rows:
            per_book[b].append(ln)
        book_medians = [statistics.median(v) for v in per_book.values() if v]
        return statistics.median(book_medians) if len(book_medians) >= 3 else None
    elif market_type == 'SPREAD':
        # For spreads we want the median for the specific team our bet is on
        # Use a LIKE match on the selection strip (up to line)
        import re
        team = re.sub(r'\s*[+-]?\d+\.?\d*$', '', bet_selection or '').strip()
        if not team:
            return None
        rows = conn.execute("""
            SELECT DISTINCT book, line FROM odds
            WHERE event_id=? AND market=? AND selection LIKE ? AND line IS NOT NULL
        """, (event_id, mkt, f'%{team}%')).fetchall()
        if not rows:
            return None
        per_book = defaultdict(list)
        for b, ln in rows:
            per_book[b].append(ln)
        book_medians = [statistics.median(v) for v in per_book.values() if v]
        return statistics.median(book_medians) if len(book_medians) >= 3 else None
    return None


def pick_direction(market_type, selection, line):
    if market_type == 'TOTAL':
        return 'OVER' if 'OVER' in (selection or '').upper() else 'UNDER'
    return 'FAVORITE' if (line is not None and line < 0) else 'DOG'


def market_disagrees(direction, bet_line, market_median, market_type):
    """Is market median on opposite side of bet line from our pick?"""
    if market_median is None or bet_line is None:
        return False
    if market_type == 'TOTAL':
        if direction == 'OVER':
            return market_median < bet_line  # market says lower total = UNDER side
        return market_median > bet_line      # UNDER bet; market says OVER side
    # SPREAD: line is the bet line for our chosen team (e.g. -3.5 for favorite)
    # If market_median is HIGHER (e.g. -2.5) than our -3.5 → market less bullish on favorite
    if direction == 'FAVORITE':
        return market_median > bet_line  # market gives favorite less of a spread
    return market_median < bet_line      # DOG; market thinks dog covers less


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

    flip_candidates = []
    skipped_no_median = 0
    skipped_not_disagree = 0
    skipped_under_threshold = 0

    for r in rows:
        (_id, bid, sport, eid, mt, st, sel, ln, od, un, res, pnl, ed, ca) = r
        if ln is None or eid is None:
            continue
        direction = pick_direction(mt, sel, ln)
        mm = market_median_for_bet(conn, eid, mt, sel)
        if mm is None:
            skipped_no_median += 1
            continue
        if not market_disagrees(direction, ln, mm, mt):
            skipped_not_disagree += 1
            continue
        gap = abs(ln - mm)
        thr = _thr(sport, mt)
        # Record candidate with full data for bucketing
        flip_candidates.append({
            'sport': sport, 'market': mt, 'sel': sel, 'line': ln, 'odds': od,
            'units': un, 'result': res, 'pnl_orig': pnl or 0.0,
            'edge': ed, 'direction': direction,
            'market_median': mm, 'gap': gap, 'threshold': thr,
            'meets_threshold': gap >= thr,
            'flip_res': flip_outcome(res), 'flip_pnl': flip_pnl(res),
            'created_at': ca,
        })
        if gap < thr:
            skipped_under_threshold += 1

    print(f"Total SPREAD/TOTAL graded (post-Apr-1): {len(rows)}")
    print(f"Skipped: no market_median (<3 books): {skipped_no_median}")
    print(f"Skipped: market_median on SAME side as us: {skipped_not_disagree}")
    print(f"Flip candidates (market disagrees): {len(flip_candidates)}")
    print(f"  — of those, meeting sport threshold: {sum(1 for c in flip_candidates if c['meets_threshold'])}")
    print()

    # === RAW VIEW: All candidates ===
    print("=" * 74)
    print("ALL FLIP CANDIDATES (regardless of gap threshold)")
    print("=" * 74)
    orig_pnl = sum(c['pnl_orig'] for c in flip_candidates)
    flip_tot = sum(c['flip_pnl'] for c in flip_candidates)
    w = sum(1 for c in flip_candidates if c['flip_res'] == 'WIN')
    l = sum(1 for c in flip_candidates if c['flip_res'] == 'LOSS')
    p = sum(1 for c in flip_candidates if c['flip_res'] == 'PUSH')
    print(f"  {len(flip_candidates)} picks | Flip {w}W-{l}L-{p}P | Flip P/L {flip_tot:+.2f}u | Orig P/L {orig_pnl:+.2f}u")
    print()

    # === BUCKETING BY GAP SIZE (in threshold multiples) ===
    print("=" * 74)
    print("BY GAP SIZE (relative to sport threshold)")
    print("=" * 74)
    bands = [
        ('below_thr', lambda g, t: g < t),
        ('1x-1.5x_thr', lambda g, t: t <= g < 1.5 * t),
        ('1.5x-2x_thr', lambda g, t: 1.5 * t <= g < 2 * t),
        ('2x-3x_thr', lambda g, t: 2 * t <= g < 3 * t),
        ('3x+_thr', lambda g, t: g >= 3 * t),
    ]
    print(f"  {'Gap band':<15} {'N':>3} {'W-L-P':>10} {'Win%':>6} {'Orig P/L':>10} {'Flip P/L':>10} {'Best':>10}")
    for lbl, pred in bands:
        recs = [c for c in flip_candidates if pred(c['gap'], c['threshold'])]
        if not recs:
            continue
        w = sum(1 for r in recs if r['flip_res'] == 'WIN')
        l = sum(1 for r in recs if r['flip_res'] == 'LOSS')
        p = sum(1 for r in recs if r['flip_res'] == 'PUSH')
        wr = w / (w + l) * 100 if (w + l) else 0
        orig = sum(r['pnl_orig'] for r in recs)
        flip = sum(r['flip_pnl'] for r in recs)
        block = -orig
        best_val = max(orig, block, flip)
        if best_val == flip: best = 'FLIP'
        elif best_val == block: best = 'BLOCK'
        else: best = 'FIRE'
        print(f"  {lbl:<15} {len(recs):>3} {w}-{l}-{p:<6} {wr:>5.1f}% {orig:>+8.2f}u {flip:>+8.2f}u {best:>10}")

    # === BY SPORT (only threshold-meeting) ===
    print()
    print("=" * 74)
    print("BY SPORT (gap >= sport threshold only)")
    print("=" * 74)
    meeting = [c for c in flip_candidates if c['meets_threshold']]
    by_sport = defaultdict(list)
    for c in meeting:
        by_sport[c['sport']].append(c)
    print(f"  {'Sport':<28} {'N':>3} {'W-L-P':>10} {'Orig':>9} {'Flip':>9} {'Best':>10}")
    for sp, recs in sorted(by_sport.items()):
        w = sum(1 for r in recs if r['flip_res'] == 'WIN')
        l = sum(1 for r in recs if r['flip_res'] == 'LOSS')
        p = sum(1 for r in recs if r['flip_res'] == 'PUSH')
        orig = sum(r['pnl_orig'] for r in recs)
        flip = sum(r['flip_pnl'] for r in recs)
        block = -orig
        best_val = max(orig, block, flip)
        if best_val == flip: best = 'FLIP'
        elif best_val == block: best = 'BLOCK'
        else: best = 'FIRE'
        print(f"  {sp:<28} {len(recs):>3} {w}-{l}-{p:<6} {orig:>+7.2f}u {flip:>+7.2f}u {best:>10}")

    # === OVERLAP WITH SHARP_OPPOSES_BLOCK ===
    print()
    print("=" * 74)
    print("OVERLAP WITH SHARP_OPPOSES_BLOCK (NHL + NCAA BB)")
    print("=" * 74)
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from steam_engine import get_steam_signal
    SB_SPORTS = {'icehockey_nhl', 'baseball_ncaa'}
    also_sharp_opposes = 0
    only_fade = 0
    for c in flip_candidates:
        if not c['meets_threshold']:
            continue
        # would SHARP_OPPOSES_BLOCK have fired on this pick?
        eid_row = conn.execute(
            "SELECT event_id FROM graded_bets WHERE sport=? AND selection=? AND line=? AND DATE(created_at) = DATE(?) LIMIT 1",
            (c['sport'], c['sel'], c['line'], c['created_at'])
        ).fetchone()
        if not eid_row:
            continue
        eid = eid_row[0]
        side_hint = c['direction']
        sig, _info = get_steam_signal(conn, c['sport'], eid, c['market'], side_hint, c['line'], c['odds'])
        if c['sport'] in SB_SPORTS and sig == 'SHARP_OPPOSES':
            also_sharp_opposes += 1
        else:
            only_fade += 1
    print(f"  Fade-flip candidates (meeting threshold): {sum(1 for c in flip_candidates if c['meets_threshold'])}")
    print(f"  Also caught by SHARP_OPPOSES_BLOCK:       {also_sharp_opposes}")
    print(f"  ONLY caught by fade-flip (unique):        {only_fade}")


if __name__ == '__main__':
    main()
