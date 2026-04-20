"""Backtest the Context Model (Phase 1) against DIVERGENCE_GATE-blocked picks.

For each blocked NBA spread pick in the last 14 days:
  1. Compute ms_context (injury + form adjusted)
  2. Check if context_div <= max_div (unblocks)
  3. If unblocked: determine pick direction (based on ms_context vs market)
  4. Look up actual game result + grade the pick
  5. Aggregate W/L and P/L at -110 odds, 5u stakes
"""
import os, sys, sqlite3, re
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from context_model import compute_context_spread

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
MAX_DIV_NBA = 5.0
STAKE = 5.0
FADE_ODDS = -110


def pnl(w, stake=STAKE, odds=FADE_ODDS):
    if w == 'WIN': return stake * (100/abs(odds))
    if w == 'LOSS': return -stake
    return 0.0


def main():
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT DISTINCT sp.event_id, sp.reason, r.home, r.away, r.home_score, r.away_score,
                        DATE(r.commence_time) comm_date
        FROM shadow_blocked_picks sp
        JOIN results r ON r.event_id = sp.event_id
        WHERE sp.sport='basketball_nba' AND sp.market_type='SPREAD'
          AND DATE(sp.created_at) >= '2026-04-06'
          AND sp.reason LIKE 'DIVERGENCE_GATE%'
          AND r.home_score IS NOT NULL
    """).fetchall()

    print(f"Testing {len(rows)} DIVERGENCE_GATE-blocked NBA spreads\n")

    total_n = 0
    unblocked = []
    still_blocked = 0
    elo_model_wins = 0
    context_wins = 0
    fade_wins = 0  # current behavior

    for eid, rsn, home, away, hs, as_, comm_date in rows:
        ms_m = re.search(r'ms=([-+]?[\d.]+)', rsn)
        mkt_m = re.search(r'mkt_sp=([-+]?[\d.]+)', rsn)
        if not (ms_m and mkt_m): continue
        ms_elo = float(ms_m.group(1))
        mkt_hs = float(mkt_m.group(1))
        margin = hs - as_

        total_n += 1

        ms_ctx, info = compute_context_spread(conn, 'basketball_nba', home, away,
                                               eid, ms_elo, comm_date)

        ctx_div = abs(ms_ctx - mkt_hs)
        raw_div = abs(ms_elo - mkt_hs)

        # Current behavior: fade flip fires → bet opposite of Elo side
        elo_wants_home = ms_elo < mkt_hs  # Elo more bullish on home
        # Fade bets opposite: home if Elo wanted away, away if Elo wanted home
        fade_home = not elo_wants_home
        home_covers = margin > -mkt_hs  # home covers the spread
        if fade_home:
            fade_result = 'WIN' if home_covers else ('LOSS' if not home_covers else 'PUSH')
            if margin == -mkt_hs: fade_result = 'PUSH'
        else:
            fade_result = 'LOSS' if home_covers else ('WIN' if not home_covers else 'PUSH')
            if margin == -mkt_hs: fade_result = 'PUSH'
        if fade_result == 'WIN': fade_wins += 1

        if ctx_div <= MAX_DIV_NBA:
            # UNBLOCKED: Context says this is within threshold
            # Context model pick: if ms_ctx < mkt_hs, Context more bullish on home → bet home
            ctx_wants_home = ms_ctx < mkt_hs
            if ctx_wants_home:
                ctx_result = 'WIN' if home_covers else 'LOSS'
            else:
                ctx_result = 'LOSS' if home_covers else 'WIN'
            if margin == -mkt_hs: ctx_result = 'PUSH'
            if ctx_result == 'WIN': context_wins += 1
            unblocked.append({
                'home': home, 'away': away, 'ms_elo': ms_elo, 'ms_ctx': ms_ctx,
                'mkt': mkt_hs, 'margin': margin, 'raw_div': raw_div, 'ctx_div': ctx_div,
                'ctx_wants_home': ctx_wants_home, 'fade_home': fade_home,
                'ctx_result': ctx_result, 'fade_result': fade_result,
                'info': info,
            })
        else:
            still_blocked += 1

    print(f"RESULTS:")
    print(f"  Total blocked picks analyzed: {total_n}")
    print(f"  Context UNBLOCKS (ctx_div <= {MAX_DIV_NBA}): {len(unblocked)}")
    print(f"  Context STILL BLOCKS: {still_blocked}")
    print()

    if unblocked:
        ctx_w = sum(1 for u in unblocked if u['ctx_result']=='WIN')
        ctx_l = sum(1 for u in unblocked if u['ctx_result']=='LOSS')
        ctx_p = sum(1 for u in unblocked if u['ctx_result']=='PUSH')
        ctx_pnl = sum(pnl(u['ctx_result']) for u in unblocked)
        # If we'd still fade flip on these instead:
        fade_w_unb = sum(1 for u in unblocked if u['fade_result']=='WIN')
        fade_l_unb = sum(1 for u in unblocked if u['fade_result']=='LOSS')
        fade_pnl_unb = sum(pnl(u['fade_result']) for u in unblocked)

        print(f"On UNBLOCKED picks ({len(unblocked)}):")
        print(f"  Context Model own-pick:  {ctx_w}-{ctx_l}-{ctx_p} ({ctx_w/(ctx_w+ctx_l)*100:.1f}%) | P/L {ctx_pnl:+.2f}u")
        print(f"  Fade flip (current):     {fade_w_unb}-{fade_l_unb}     ({fade_w_unb/(fade_w_unb+fade_l_unb)*100:.1f}%) | P/L {fade_pnl_unb:+.2f}u")
        print(f"  Delta (Context vs Fade): {ctx_pnl - fade_pnl_unb:+.2f}u")
        print()

        # Agree / disagree between Context and Fade direction?
        agree = sum(1 for u in unblocked if (u['ctx_wants_home'] == u['fade_home']))
        print(f"Context vs Fade direction alignment: {agree}/{len(unblocked)} agree")
        print()
        print("Sample unblocked picks:")
        print(f"  {'Away':<20} {'Home':<20} {'Elo':>6} {'Ctx':>6} {'Mkt':>6} {'Margin':>7} {'Ctx':>4} {'Fade':>4}")
        for u in unblocked[:15]:
            print(f"  {u['away'][:19]:<20} {u['home'][:19]:<20} {u['ms_elo']:>+6.1f} {u['ms_ctx']:>+6.1f} {u['mkt']:>+6.1f} {u['margin']:>+7d} {u['ctx_result']:>4} {u['fade_result']:>4}")


if __name__ == '__main__':
    main()
