"""Multi-sport backtest of the Context Model Phase 4.

Loops over NBA, NHL, NCAA Baseball (sports with DIVERGENCE_GATE blocks),
applies each sport's configured max_spread_divergence threshold, and
reports per-sport + total results.
"""
import os, sys, sqlite3, re
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from context_model import compute_context_spread

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

SPORT_MAX_DIV = {
    'basketball_nba': 4.0,
    'basketball_ncaab': 4.5,
    'icehockey_nhl': 2.5,
    'baseball_ncaa': 5.0,  # default (config doesn't list)
    'baseball_mlb': 5.0,
}

STAKE = 5.0
ODDS = -110


def pnl(w):
    if w == 'WIN': return STAKE * (100/abs(ODDS))
    if w == 'LOSS': return -STAKE
    return 0.0


def main():
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT DISTINCT sp.sport, sp.event_id, sp.reason,
                        r.home, r.away, r.home_score, r.away_score,
                        DATE(r.commence_time) comm_date
        FROM shadow_blocked_picks sp
        JOIN results r ON r.event_id = sp.event_id
        WHERE sp.market_type='SPREAD'
          AND DATE(sp.created_at) >= '2026-04-06'
          AND sp.reason LIKE 'DIVERGENCE_GATE%'
          AND r.home_score IS NOT NULL
    """).fetchall()
    print(f"Total blocked spread events with results: {len(rows)}\n")

    per_sport = defaultdict(lambda: {
        'total': 0, 'unblocked': 0, 'still_blocked': 0,
        'ctx_w': 0, 'ctx_l': 0, 'ctx_push': 0, 'ctx_pnl': 0.0,
        'fade_w': 0, 'fade_l': 0, 'fade_pnl': 0.0,
        'agree': 0, 'sample_unblocks': [],
    })

    for sport, eid, rsn, home, away, hs, as_, comm_date in rows:
        ms_m = re.search(r'ms=([-+]?[\d.]+)', rsn)
        mkt_m = re.search(r'mkt_sp=([-+]?[\d.]+)', rsn)
        if not (ms_m and mkt_m): continue
        ms_elo = float(ms_m.group(1))
        mkt_hs = float(mkt_m.group(1))
        margin = hs - as_

        max_div = SPORT_MAX_DIV.get(sport, 5.0)
        d = per_sport[sport]
        d['total'] += 1

        try:
            ms_ctx, info = compute_context_spread(conn, sport, home, away,
                                                    eid, ms_elo, comm_date)
        except Exception as e:
            continue

        ctx_div = abs(ms_ctx - mkt_hs)
        home_covers = margin > -mkt_hs
        pushed = (margin == -mkt_hs)

        # Fade flip direction: opposite of Elo
        elo_wants_home = ms_elo < mkt_hs
        fade_home = not elo_wants_home
        if pushed: fade_result = 'PUSH'
        elif fade_home: fade_result = 'WIN' if home_covers else 'LOSS'
        else: fade_result = 'LOSS' if home_covers else 'WIN'
        d['fade_pnl'] += pnl(fade_result)
        if fade_result == 'WIN': d['fade_w'] += 1
        elif fade_result == 'LOSS': d['fade_l'] += 1

        if ctx_div > max_div:
            d['still_blocked'] += 1
            continue

        d['unblocked'] += 1
        ctx_wants_home = ms_ctx < mkt_hs
        if pushed: ctx_result = 'PUSH'
        elif ctx_wants_home: ctx_result = 'WIN' if home_covers else 'LOSS'
        else: ctx_result = 'LOSS' if home_covers else 'WIN'
        d['ctx_pnl'] += pnl(ctx_result)
        if ctx_result == 'WIN': d['ctx_w'] += 1
        elif ctx_result == 'LOSS': d['ctx_l'] += 1
        else: d['ctx_push'] += 1
        if ctx_wants_home == fade_home: d['agree'] += 1
        if len(d['sample_unblocks']) < 6:
            d['sample_unblocks'].append(
                (away, home, ms_elo, ms_ctx, mkt_hs, margin, ctx_result, fade_result))

    # Report
    print(f"  {'Sport':<22} {'N':>3} {'Unbl':>4} {'Ctx W-L':>8} {'Ctx%':>5} {'Ctx P/L':>9} {'Fade W-L':>9} {'Fade P/L':>9} {'Delta':>8}")
    print('-' * 90)
    tot_unb = 0; tot_ctx_pnl = 0; tot_fade_pnl = 0; tot_w = 0; tot_l = 0
    for sport, d in sorted(per_sport.items(), key=lambda x: -x[1]['unblocked']):
        if d['unblocked'] == 0: continue
        wr = d['ctx_w']/(d['ctx_w']+d['ctx_l'])*100 if (d['ctx_w']+d['ctx_l']) else 0
        # Fade P/L only on unblocked subset for apples-to-apples — recompute
        # Actually just show fade's total for all blocked picks this sport
        # (that's the current strategy)
        print(f"  {sport:<22} {d['total']:>3} {d['unblocked']:>4} "
              f"{d['ctx_w']:>3}-{d['ctx_l']:<4} {wr:>4.1f}% {d['ctx_pnl']:>+7.2f}u "
              f"{d['fade_w']:>3}-{d['fade_l']:<5} {d['fade_pnl']:>+7.2f}u "
              f"{d['ctx_pnl'] - d['fade_pnl']:>+7.2f}u")
        tot_unb += d['unblocked']
        tot_ctx_pnl += d['ctx_pnl']
        tot_fade_pnl += d['fade_pnl']
        tot_w += d['ctx_w']; tot_l += d['ctx_l']
    total_n = sum(d['total'] for d in per_sport.values())
    total_wr = tot_w/(tot_w+tot_l)*100 if (tot_w+tot_l) else 0
    print('-' * 90)
    print(f"  {'TOTAL':<22} {total_n:>3} {tot_unb:>4}   {tot_w}-{tot_l:<4} {total_wr:>4.1f}% {tot_ctx_pnl:>+7.2f}u "
          f"                 {tot_fade_pnl:>+7.2f}u")

    print()
    print("Sample unblocks per sport:")
    for sport, d in per_sport.items():
        if not d['sample_unblocks']: continue
        print(f"\n  [{sport}]")
        print(f"  {'Away':<22} {'Home':<22} {'Elo':>6} {'Ctx':>6} {'Mkt':>6} {'M':>4} {'Ctx':>4} {'Fade':>4}")
        for row in d['sample_unblocks']:
            a, h, e, c, m, mg, cr, fr = row
            print(f"  {a[:21]:<22} {h[:21]:<22} {e:>+6.1f} {c:>+6.1f} {m:>+6.1f} {mg:>+4d} {cr:>4} {fr:>4}")


if __name__ == '__main__':
    main()
