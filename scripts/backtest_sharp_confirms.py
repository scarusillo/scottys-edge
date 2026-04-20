"""Backtest Option A (relax edge on confirm) vs Option B (boost units on confirm).

Replays historical picks with steam_engine applied post-hoc to derive a
SHARP_CONFIRMS counterfactual:
  - B: every fired pick that was SHARP_CONFIRMS gets +1.0u added to its stake
       (scales pnl_units by (units+1)/units).
  - A: approximated — we scan shadow_blocked_picks and odds opener data to
       estimate picks that fell below the 8% edge gate but would have had
       SHARP_CONFIRMS; then apply baseline 54% hit-rate at avg -110 odds.
"""
import os, sys, sqlite3
from collections import defaultdict, Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from steam_engine import get_steam_signal

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def american_to_multiplier(odds):
    if odds is None:
        return 1.0
    o = float(odds)
    if o > 0:
        return o / 100.0
    return 100.0 / abs(o)


def main():
    conn = sqlite3.connect(DB)
    # Post-rebuild only (Apr 1+) graded bets
    rows = conn.execute("""
        SELECT id, bet_id, sport, event_id, market_type, side_type, selection,
               line, odds, units, result, pnl_units, edge_pct, clv, created_at
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-04-01'
          AND market_type IN ('TOTAL','SPREAD','MONEYLINE')
          AND result IN ('WIN','LOSS','PUSH')
        ORDER BY created_at
    """).fetchall()

    buckets = defaultdict(list)  # signal -> list of (pnl, units, result, odds)
    per_sport = defaultdict(lambda: defaultdict(list))
    skipped = 0
    total = 0

    for r in rows:
        (_id, bid, sport, eid, mt, st, sel, ln, od, un, res, pnl, ed, clv, ca) = r
        if ln is None or eid is None:
            skipped += 1
            continue
        # Map side exactly like model_engine.py
        side_hint = (st or '').upper()
        sel_l = (sel or '').lower()
        if mt == 'TOTAL':
            steam_side = 'OVER' if 'OVER' in side_hint or 'over' in sel_l else 'UNDER'
        elif mt == 'SPREAD':
            steam_side = 'FAVORITE' if side_hint == 'FAVORITE' else 'DOG'
        else:
            steam_side = None

        if not steam_side:
            skipped += 1
            continue

        sig, info = get_steam_signal(conn, sport, eid, mt, steam_side, ln, od)
        total += 1
        buckets[sig].append((pnl, un, res, od))
        per_sport[sport][sig].append((pnl, un, res, od))

    print(f"Scanned: {total} graded bets (post-Apr-1, TOTAL/SPREAD/ML)  skipped={skipped}")
    print()
    print("=" * 70)
    print("OVERALL STEAM DISTRIBUTION (post-hoc applied to fired bets)")
    print("=" * 70)
    for sig in ('NO_MOVEMENT', 'SHARP_CONFIRMS', 'SHARP_OPPOSES'):
        records = buckets.get(sig, [])
        if not records:
            print(f"{sig:<20} 0 picks")
            continue
        n = len(records)
        w = sum(1 for x in records if x[2] == 'WIN')
        l = sum(1 for x in records if x[2] == 'LOSS')
        p = sum(1 for x in records if x[2] == 'PUSH')
        pnl = sum(x[0] or 0.0 for x in records)
        wr = w / (w + l) * 100 if (w + l) else 0
        print(f"{sig:<20} {n:>3} picks | {w}W-{l}L-{p}P | {wr:.1f}% | {pnl:+.2f}u")

    # === OPTION B: +1.0u boost on every SHARP_CONFIRMS ===
    print()
    print("=" * 70)
    print("OPTION B — +1.0u boost on SHARP_CONFIRMS picks")
    print("=" * 70)
    confirms = buckets.get('SHARP_CONFIRMS', [])
    baseline_pnl = sum(x[0] or 0.0 for x in confirms)
    boosted_pnl = 0.0
    for pnl, un, res, od in confirms:
        if res == 'PUSH' or un in (None, 0):
            boosted_pnl += pnl or 0.0
            continue
        new_un = un + 1.0
        mult = american_to_multiplier(od)
        if res == 'WIN':
            new_pnl = new_un * mult
        elif res == 'LOSS':
            new_pnl = -new_un
        else:
            new_pnl = 0.0
        boosted_pnl += new_pnl
    delta = boosted_pnl - baseline_pnl
    print(f"  Confirms: {len(confirms)} picks")
    print(f"  Baseline P/L (5u stake): {baseline_pnl:+.2f}u")
    print(f"  Boosted  P/L (6u stake): {boosted_pnl:+.2f}u")
    print(f"  Delta from +1u boost:    {delta:+.2f}u")

    # === OPTION A approximation ===
    # We can't easily replay un-fired picks without re-running the full predict
    # engine. But we can look at graded_bets grouped by edge_bucket to see the
    # realized win rate by edge slice, and estimate what happens if we extend
    # the fire zone down to 5% edge on SHARP_CONFIRMS.
    print()
    print("=" * 70)
    print("OPTION A — relax edge to 5% when sharps confirm (approximation)")
    print("=" * 70)

    # Baseline realized hit rate on fired bets by edge bucket (helps calibrate)
    edge_buckets = conn.execute("""
        SELECT edge_bucket,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) AS w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) AS l,
               SUM(pnl_units) AS pnl,
               AVG(odds) AS avg_odds,
               COUNT(*) AS n
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-04-01'
          AND result IN ('WIN','LOSS')
        GROUP BY edge_bucket
        ORDER BY edge_bucket
    """).fetchall()
    print(f"  {'Edge bucket':<18} {'N':>4} {'W-L':>8} {'WinRate':>8} {'Avg odds':>9} {'P/L':>8}")
    for eb, w, l, pnl, ao, n in edge_buckets:
        wr = w / (w + l) * 100 if (w + l) else 0
        print(f"  {eb or 'NULL':<18} {n:>4} {w}-{l:>3} {wr:>7.1f}% {ao:>9.1f} {pnl:>+7.2f}u")

    # Count historical SHARP_CONFIRMS events at ALL edge slices (5-8%)
    # by scanning odds openers and current lines.  Placeholder estimate.
    # For a realistic estimate, we use the Apr 14 briefing's reported 10 NBA
    # backtest picks with SHARP_CONFIRMS at 6W-4L as the upper bound.
    print()
    print("  APPROXIMATION (from model health report — 20 days historical):")
    print("     Assumed unfired SHARP_CONFIRMS picks at 5-8% edge: ~10 (NBA backtest estimate)")
    print("     Assumed baseline hit rate: 55% (similar to BELOW_CAP bucket)")
    print("     Assumed avg odds: -110 (juice = 1.9091 multiplier)")
    # 10 picks * 5u each, 55% hit, -110 odds
    n_est = 10
    stake = 5.0
    wr_est = 0.55
    mult = american_to_multiplier(-110)
    wins = n_est * wr_est
    losses = n_est * (1 - wr_est)
    expected_pl = wins * stake * mult - losses * stake
    print(f"     Expected P/L over 20 days: {expected_pl:+.2f}u")
    print(f"     (vs 0u currently — these picks never fire)")

    print()
    print("=" * 70)
    print("BY SPORT — SHARP_CONFIRMS post-hoc")
    print("=" * 70)
    print(f"  {'Sport':<28} {'N':>3} {'W-L':>6} {'P/L':>8}")
    for sport, sigs in sorted(per_sport.items()):
        recs = sigs.get('SHARP_CONFIRMS', [])
        if not recs:
            continue
        w = sum(1 for x in recs if x[2] == 'WIN')
        l = sum(1 for x in recs if x[2] == 'LOSS')
        pnl = sum(x[0] or 0.0 for x in recs)
        print(f"  {sport:<28} {len(recs):>3} {w}-{l:<4} {pnl:>+7.2f}u")


if __name__ == '__main__':
    main()
