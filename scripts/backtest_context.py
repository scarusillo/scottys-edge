"""
backtest_context.py — Compare OLD (ms += ctx) vs NEW (ms -= ctx) context sign

Replays the full spread pipeline over the last 45 days with both sign modes.
Shows favorites vs underdogs, by spread bucket, by sport.
"""
import sqlite3, io, sys, os
from datetime import datetime, timedelta

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
_atexit_conn = None

def _get_conn():
    global _atexit_conn
    if _atexit_conn is None:
        import atexit
        _atexit_conn = sqlite3.connect(DB_PATH)
        atexit.register(_atexit_conn.close)
    return _atexit_conn

conn = _get_conn()

from elo_engine import get_elo_ratings, blended_spread
from model_engine import (
    SPORT_CONFIG, spread_to_cover_prob, american_to_implied_prob,
    compute_model_spread, get_latest_ratings,
)
from scottys_edge import scottys_edge_assessment, minimum_play_threshold
from context_engine import get_context_adjustments

DAYS_BACK = 45
cutoff = (datetime.now() - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')
SPORTS = ['basketball_ncaab', 'basketball_nba', 'icehockey_nhl']


def run_spread_backtest(sign_mode):
    results_all = []

    for sp in SPORTS:
        ratings = get_latest_ratings(conn, sp)
        elo_data = get_elo_ratings(conn, sp)
        if not elo_data:
            continue

        min_pv = minimum_play_threshold(sp, False)
        cfg = SPORT_CONFIG.get(sp, {})
        max_div = cfg.get('max_spread_divergence', 4.5)

        games = conn.execute("""
            SELECT mc.event_id, mc.commence_time, mc.home, mc.away,
                   mc.best_home_spread, mc.best_home_spread_odds,
                   mc.best_away_spread, mc.best_away_spread_odds,
                   r.actual_margin, r.home_score, r.away_score
            FROM market_consensus mc
            JOIN results r ON mc.event_id = r.event_id
            WHERE mc.sport=? AND r.completed=1 AND r.commence_time >= ?
            AND r.home_score IS NOT NULL AND mc.best_home_spread IS NOT NULL
            AND mc.snapshot_date = (
                SELECT MAX(mc2.snapshot_date) FROM market_consensus mc2
                WHERE mc2.event_id = mc.event_id AND mc2.sport = mc.sport
            )
        """, (sp, cutoff)).fetchall()

        seen = set()
        for g in games:
            eid, commence, home, away = g[0], g[1], g[2], g[3]
            mkt_hs, mkt_hs_odds = g[4], g[5]
            mkt_as, mkt_as_odds = g[6], g[7]
            margin = g[8]

            if margin is None or mkt_hs is None:
                continue

            # Compute model spread
            ms = compute_model_spread(home, away, ratings, sp)
            if ms is None:
                continue

            # Blend with Elo
            _neutral = (sp == 'basketball_ncaab'
                        and (datetime.now().month == 3
                             or (datetime.now().month == 4 and datetime.now().day <= 7)))
            elo_ms = blended_spread(home, away, elo_data, ratings, sp, conn,
                                    neutral_site=_neutral)
            if elo_ms is not None:
                ms = elo_ms

            # Divergence check
            if abs(ms - mkt_hs) > max_div:
                continue

            # Context adjustment
            try:
                ctx = get_context_adjustments(conn, sp, home, away, eid, commence, 'SPREAD')
                if ctx['spread_adj'] != 0:
                    if sign_mode == 'OLD':
                        ms += ctx['spread_adj']
                    else:
                        ms -= ctx['spread_adj']
            except Exception:
                pass

            # Evaluate both sides
            for side, mkt_line, mkt_odds, is_home in [
                ('home', mkt_hs, mkt_hs_odds, True),
                ('away', mkt_as, mkt_as_odds, False),
            ]:
                if mkt_line is None or mkt_odds is None:
                    continue

                k = f"{eid}|{side}"
                if k in seen:
                    continue

                side_ms = ms if is_home else -ms
                wa = scottys_edge_assessment(side_ms, mkt_line, mkt_odds, sp)

                if wa['is_play'] and wa['point_value_pct'] >= min_pv:
                    units = wa['units']
                    if units < 4.5:
                        continue

                    seen.add(k)

                    # Grade
                    if is_home:
                        cover_margin = margin + mkt_line
                    else:
                        cover_margin = -margin + mkt_line

                    if cover_margin > 0:
                        result = 'WIN'
                        pnl = units * (100 / abs(mkt_odds) if mkt_odds < 0 else mkt_odds / 100)
                    elif cover_margin < 0:
                        result = 'LOSS'
                        pnl = -units
                    else:
                        result = 'PUSH'
                        pnl = 0

                    is_fav = mkt_line < 0
                    team = home if is_home else away

                    results_all.append({
                        'sport': sp, 'team': team, 'line': mkt_line,
                        'edge': wa['point_value_pct'], 'units': units,
                        'result': result, 'pnl': pnl,
                        'is_fav': is_fav, 'margin': margin,
                        'date': (commence or '')[:10],
                    })

    return results_all


def summarize(results, label):
    print(f"\n{'=' * 70}")
    print(f"  {label}")
    print(f"{'=' * 70}")

    w = sum(1 for r in results if r['result'] == 'WIN')
    l = sum(1 for r in results if r['result'] == 'LOSS')
    p = sum(1 for r in results if r['result'] == 'PUSH')
    pnl = sum(r['pnl'] for r in results)
    wag = sum(r['units'] for r in results if r['result'] != 'PUSH')
    roi = (pnl / wag * 100) if wag else 0

    print(f"  OVERALL: {w}W-{l}L-{p}P | {pnl:+.1f}u | ROI: {roi:+.1f}%")

    # Favorites vs Dogs
    favs = [r for r in results if r['is_fav']]
    dogs = [r for r in results if not r['is_fav']]

    fw = sum(1 for r in favs if r['result'] == 'WIN')
    fl = sum(1 for r in favs if r['result'] == 'LOSS')
    fpnl = sum(r['pnl'] for r in favs)
    fwag = sum(r['units'] for r in favs if r['result'] != 'PUSH')
    froi = (fpnl / fwag * 100) if fwag else 0

    dw = sum(1 for r in dogs if r['result'] == 'WIN')
    dl = sum(1 for r in dogs if r['result'] == 'LOSS')
    dpnl = sum(r['pnl'] for r in dogs)
    dwag = sum(r['units'] for r in dogs if r['result'] != 'PUSH')
    droi = (dpnl / dwag * 100) if dwag else 0

    print(f"  FAVORITES: {fw}W-{fl}L | {fpnl:+.1f}u | ROI: {froi:+.1f}%")
    print(f"  UNDERDOGS: {dw}W-{dl}L | {dpnl:+.1f}u | ROI: {droi:+.1f}%")

    # By spread bucket
    print()
    buckets = [
        ('Small fav (0 to -3.5)', -3.5, 0),
        ('Med fav (-4 to -7.5)', -7.5, -3.5),
        ('Big fav (-8+)', -50, -7.5),
        ('Small dog (0 to +3.5)', 0, 3.5),
        ('Med dog (+4 to +7.5)', 3.5, 7.5),
        ('Big dog (+8+)', 7.5, 50),
    ]
    for lbl, lo, hi in buckets:
        sub = [r for r in results if lo <= r['line'] < hi]
        if sub:
            sw = sum(1 for r in sub if r['result'] == 'WIN')
            sl = sum(1 for r in sub if r['result'] == 'LOSS')
            spnl = sum(r['pnl'] for r in sub)
            swag = sum(r['units'] for r in sub if r['result'] != 'PUSH')
            sroi = (spnl / swag * 100) if swag else 0
            print(f"    {lbl:30s}: {sw}W-{sl}L | {spnl:+.1f}u | ROI: {sroi:+.1f}%")

    # By sport
    print()
    for sp in SPORTS:
        sub = [r for r in results if r['sport'] == sp]
        if sub:
            sw = sum(1 for r in sub if r['result'] == 'WIN')
            sl = sum(1 for r in sub if r['result'] == 'LOSS')
            spnl = sum(r['pnl'] for r in sub)
            swag = sum(r['units'] for r in sub if r['result'] != 'PUSH')
            sroi = (spnl / swag * 100) if swag else 0
            sfavs = [r for r in sub if r['is_fav']]
            sfw = sum(1 for r in sfavs if r['result'] == 'WIN')
            sfl = sum(1 for r in sfavs if r['result'] == 'LOSS')
            sdogs = [r for r in sub if not r['is_fav']]
            sdw = sum(1 for r in sdogs if r['result'] == 'WIN')
            sdl = sum(1 for r in sdogs if r['result'] == 'LOSS')
            print(f"  {sp:25s}: {sw}W-{sl}L | {spnl:+.1f}u | ROI: {sroi:+.1f}%")
            print(f"    {'favs':>27s}: {sfw}W-{sfl}L | dogs: {sdw}W-{sdl}L")


if __name__ == '__main__':
    print(f"  45-DAY SPREAD BACKTEST: OLD vs NEW context sign")
    print(f"  Cutoff: {cutoff}")
    print(f"  Sports: {', '.join(SPORTS)}")

    print("\n  Running OLD (ms += ctx)...")
    old = run_spread_backtest('OLD')
    print(f"  Running NEW (ms -= ctx)...")
    new = run_spread_backtest('NEW')

    summarize(old, 'OLD (ms += ctx) -- BROKEN SIGN')
    summarize(new, 'NEW (ms -= ctx) -- FIXED SIGN')

    # Delta summary
    old_pnl = sum(r['pnl'] for r in old)
    new_pnl = sum(r['pnl'] for r in new)
    print(f"\n{'=' * 70}")
    print(f"  DELTA: {new_pnl - old_pnl:+.1f}u improvement from sign fix")
    print(f"  OLD total: {len(old)} picks, NEW total: {len(new)} picks")
    print(f"{'=' * 70}")
    conn.close()
