"""Phase A backtest: Context Model as an OWN-PICK engine (Path 2).

Current state: v25.39 Context Model only fires when Elo diverges from market
past max_div and Context brings it back within threshold (Path 1 "rescue").

Phase A question: What if we ran Context on games where Elo AGREES with market
(non-divergent)? On games where Context disagrees with market by a meaningful
margin, would betting Context's side win?

Method (walk-forward-ish):
  1. Pull completed NHL/MLS/EPL games in the last 30 days
  2. For each, get stored model_spread (Elo) and best_home_spread (market)
     from market_consensus (tag='CURRENT', pre-game)
  3. SKIP if |ms_elo - mkt_hs| > max_div (already covered by Path 1)
  4. Compute ms_context from context_model.py against that date
  5. If |ms_context - mkt_hs| > ctx_threshold → candidate pick
  6. Pick Context's preferred side, grade against actual result
  7. Report W-L-PnL per sport and combined

Caveat: Context's injury signal uses today's injuries table, not historical.
Form/H2H/rest use time-filtered queries and walk forward cleanly. Expect
the injury signal to be noisy for past games — true live behavior likely
slightly better (real-time injuries).
"""
import os, sys, sqlite3
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from context_model import compute_context_spread

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

SPORT_MAX_DIV = {
    'icehockey_nhl': 2.5,
    'soccer_usa_mls': 1.0,
    'soccer_epl': 1.0,
}

# Context-edge thresholds — how much must Context disagree with market
# before we'd fire. Expressed in the same units as the spread (goals).
CTX_EDGE = {
    'icehockey_nhl': 1.0,    # ~0.5 std — meaningful but not extreme
    'soccer_usa_mls': 0.5,
    'soccer_epl': 0.5,
}

STAKE = 5.0
ODDS = -110


def pnl(outcome):
    if outcome == 'WIN': return STAKE * (100 / abs(ODDS))
    if outcome == 'LOSS': return -STAKE
    return 0.0


def grade_spread(home_score, away_score, home_line, pick_team, home, away):
    """Return WIN/LOSS/PUSH for a spread bet on pick_team at home_line (home)."""
    margin = home_score - away_score  # home margin
    # pick_team covers if their side of the line beats the margin
    if pick_team == home:
        # home covers if margin > -home_line (e.g., home -5 needs margin > 5)
        if margin > -home_line: return 'WIN'
        if margin == -home_line: return 'PUSH'
        return 'LOSS'
    else:
        # away covers if -margin > home_line (e.g., away +5 needs -margin > -5, i.e. loses by <5)
        if -margin > home_line: return 'WIN'
        if -margin == home_line: return 'PUSH'
        return 'LOSS'


def main():
    conn = sqlite3.connect(DB)

    rows = conn.execute("""
        SELECT mc.sport, mc.event_id, mc.home, mc.away,
               mc.best_home_spread, mc.best_home_spread_odds,
               mc.best_away_spread, mc.best_away_spread_odds,
               mc.model_spread, mc.snapshot_date,
               r.home_score, r.away_score, r.commence_time
        FROM market_consensus mc
        JOIN results r ON r.event_id = mc.event_id
        WHERE mc.sport IN ('icehockey_nhl','soccer_usa_mls','soccer_epl')
          AND mc.tag = 'CURRENT'
          AND r.home_score IS NOT NULL
          AND r.completed = 1
          AND DATE(r.commence_time) >= DATE('now', '-30 days')
          AND mc.model_spread IS NOT NULL
          AND mc.best_home_spread IS NOT NULL
        GROUP BY mc.event_id
    """).fetchall()

    print(f"Total completed games with market_consensus data (30d): {len(rows)}\n")

    per_sport = defaultdict(lambda: {
        'total': 0, 'non_divergent': 0,
        'ctx_candidate': 0, 'ctx_w': 0, 'ctx_l': 0, 'ctx_push': 0, 'ctx_pnl': 0.0,
        'sample': [],
    })

    for sport, eid, home, away, hl, hodds, al, aodds, ms_elo, snap, hs, as_, commence in rows:
        d = per_sport[sport]
        d['total'] += 1
        mkt_hs = hl  # best home spread
        max_div = SPORT_MAX_DIV.get(sport, 2.5)
        elo_div = abs(ms_elo - mkt_hs)
        if elo_div > max_div:
            continue  # Path 1 territory
        d['non_divergent'] += 1

        # Run Context
        commence_date = (commence[:10] if commence else snap)
        try:
            ms_ctx, info = compute_context_spread(conn, sport, home, away, eid, ms_elo, commence_date)
        except Exception:
            continue

        ctx_disagreement = abs(ms_ctx - mkt_hs)
        if ctx_disagreement < CTX_EDGE.get(sport, 1.0):
            continue

        # Context picks: ms_ctx < mkt_hs means Context is MORE bullish on home → bet home
        pick_home = (ms_ctx < mkt_hs)
        pick_team = home if pick_home else away
        pick_line = mkt_hs if pick_home else -mkt_hs

        outcome = grade_spread(hs, as_, mkt_hs, pick_team, home, away)
        d['ctx_candidate'] += 1
        if outcome == 'WIN': d['ctx_w'] += 1
        elif outcome == 'LOSS': d['ctx_l'] += 1
        else: d['ctx_push'] += 1
        d['ctx_pnl'] += pnl(outcome)

        if len(d['sample']) < 15:
            d['sample'].append(
                f"  {commence_date} {away}@{home}  ms_elo={ms_elo:+.1f} "
                f"ms_ctx={ms_ctx:+.1f} mkt={mkt_hs:+.1f}  "
                f"pick {pick_team} {pick_line:+g}  actual {hs}-{as_}  {outcome}"
            )

    # Report
    print('=' * 85)
    print(f"{'Sport':22s}  {'Games':>6s}  {'NonDiv':>6s}  {'Cand':>5s}  {'W-L-P':>8s}  {'WR':>6s}  {'P/L':>8s}")
    print('=' * 85)
    total = {'w':0,'l':0,'p':0,'pnl':0.0,'n':0}
    for sport, d in sorted(per_sport.items()):
        n = d['ctx_w'] + d['ctx_l']
        wr = f"{d['ctx_w']/n*100:.1f}%" if n else '-'
        print(f"{sport:22s}  {d['total']:>6d}  {d['non_divergent']:>6d}  "
              f"{d['ctx_candidate']:>5d}  {d['ctx_w']:>2d}-{d['ctx_l']:>2d}-{d['ctx_push']:>2d}  "
              f"{wr:>6s}  {d['ctx_pnl']:>+7.2f}u")
        total['w'] += d['ctx_w']; total['l'] += d['ctx_l']; total['p'] += d['ctx_push']
        total['pnl'] += d['ctx_pnl']; total['n'] += d['ctx_candidate']
    n = total['w'] + total['l']
    wr = f"{total['w']/n*100:.1f}%" if n else '-'
    print('-' * 85)
    print(f"{'TOTAL':22s}  {'':>6s}  {'':>6s}  {total['n']:>5d}  "
          f"{total['w']:>2d}-{total['l']:>2d}-{total['p']:>2d}  {wr:>6s}  {total['pnl']:>+7.2f}u")

    # Sample picks
    print()
    for sport, d in sorted(per_sport.items()):
        if d['sample']:
            print(f"\nSample Context picks — {sport}:")
            for line in d['sample']:
                print(line)


if __name__ == '__main__':
    main()
