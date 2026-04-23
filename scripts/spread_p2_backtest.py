"""DATA_SPREAD CONTEXT_STANDALONE (v25.44) inverse backtest.

Current scope (model_engine.py:2089):
  NHL (thresh 0.5): claimed 159 picks 57.2% WR +73.6u in Phase A
  NBA (thresh 2.5): claimed 79 picks 55.7% WR +25.0u in Phase A
  Serie A (0.5):    claimed 12 picks 66.7% WR +12.3u in Phase A

Live fires since launch (v25.44 ~2026-04-20):
  Zero grade history — both NBA DATA_SPREAD picks fired today and were scrubbed.

For each graded game in scope, compute what Context would have projected,
compare to market, simulate FOLLOW vs FADE at the spread-bet level.
"""
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from context_spread_model import compute_context_spread

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()

SPORT_THRESHOLDS = {
    'basketball_nba': 2.5,
    'icehockey_nhl': 0.5,
    'soccer_italy_serie_a': 0.5,
}


def resolve_spread_cover(home_spread, home_score, away_score):
    """Given a HOME spread (negative = home favored), return:
        'HOME' if home covers, 'AWAY' if away covers, 'PUSH' if exactly on line.

    Home covers if (home_score + home_spread) > away_score.
    """
    net = (home_score + home_spread) - away_score
    if abs(net) < 0.001:
        return 'PUSH'
    return 'HOME' if net > 0 else 'AWAY'


# Pull graded games in scope with market spread + outcome
q = """SELECT r.sport, r.home, r.away, r.event_id, r.commence_time,
              r.home_score, r.away_score,
              (SELECT AVG(line) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.home || '%' AND line IS NOT NULL) as home_spread,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.home || '%') as home_odds,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.away || '%') as away_odds
       FROM results r
       WHERE r.sport IN ({}) AND r.completed=1
         AND r.home_score IS NOT NULL AND r.away_score IS NOT NULL
         AND r.commence_time >= date('now','-90 days')
         AND r.event_id IN (SELECT DISTINCT event_id FROM odds WHERE market='spreads' AND line IS NOT NULL)
       LIMIT 2000""".format(','.join('?' * len(SPORT_THRESHOLDS)))

rows = c.execute(q, list(SPORT_THRESHOLDS.keys())).fetchall()
print(f'Games pulled: {len(rows)}')

dataset = []
errors = 0
for sport, home, away, eid, commence, hs, as_, home_spread, home_odds, away_odds in rows:
    if home_spread is None or hs is None or as_ is None:
        continue
    try:
        # Need ms_elo — pull from power_ratings or use market as proxy.
        # For backtest purposes, using market spread as ms_elo proxy is fine
        # since we're measuring ctx disagreement from market, not from Elo.
        # (In production, compute_context_spread takes ms_elo and adjusts;
        # if ms_elo == market then adjustments == disagreement.)
        ms_ctx, info = compute_context_spread(
            conn, sport, home, away, eid, home_spread, commence[:10] if commence else None)
        disagreement = abs(ms_ctx - home_spread)
        if disagreement < 0.1:
            continue
        # Context direction: ms_ctx < home_spread → Context more bullish on home → bet HOME
        ctx_pick_side = 'HOME' if ms_ctx < home_spread else 'AWAY'
        actual_cover = resolve_spread_cover(home_spread, hs, as_)

        use_odds_follow = home_odds if ctx_pick_side == 'HOME' else away_odds
        if use_odds_follow is None or use_odds_follow == 0:
            use_odds_follow = -110

        if actual_cover == 'PUSH':
            follow_pnl = 0
        elif actual_cover == ctx_pick_side:
            follow_pnl = (100 / abs(use_odds_follow)) if use_odds_follow < 0 else (use_odds_follow / 100)
        else:
            follow_pnl = -1

        fade_side = 'AWAY' if ctx_pick_side == 'HOME' else 'HOME'
        use_odds_fade = away_odds if fade_side == 'AWAY' else home_odds
        if use_odds_fade is None or use_odds_fade == 0:
            use_odds_fade = -110
        if actual_cover == 'PUSH':
            fade_pnl = 0
        elif actual_cover == fade_side:
            fade_pnl = (100 / abs(use_odds_fade)) if use_odds_fade < 0 else (use_odds_fade / 100)
        else:
            fade_pnl = -1

        dataset.append({
            'sport': sport,
            'home': home, 'away': away,
            'home_spread': home_spread,
            'disagreement': disagreement,
            'abs_disagreement': abs(disagreement),
            'ctx_pick_side': ctx_pick_side,
            'ctx_pick_is_dog': (ctx_pick_side == 'HOME' and home_spread > 0) or (ctx_pick_side == 'AWAY' and home_spread < 0),
            'actual_cover': actual_cover,
            'follow_pnl': follow_pnl,
            'fade_pnl': fade_pnl,
        })
    except Exception as e:
        errors += 1

print(f'Dataset: {len(dataset)}  (errors skipped: {errors})')
print()


def summarize(label, subset):
    if not subset:
        return
    n = len(subset)
    f_w = sum(1 for d in subset if d['follow_pnl'] > 0)
    f_l = sum(1 for d in subset if d['follow_pnl'] < 0)
    f_ev = sum(d['follow_pnl'] for d in subset)
    fd_ev = sum(d['fade_pnl'] for d in subset)
    f_wr = f_w / (f_w + f_l) * 100 if (f_w + f_l) else 0
    winner = 'FADE' if fd_ev > f_ev else ('FOLLOW' if f_ev > fd_ev else 'TIE')
    print(f'  {label:<45} n={n:<3} FOLLOW {f_w}-{f_l} WR={f_wr:.1f}% EV={f_ev:+.2f}u | '
          f'FADE={fd_ev:+.2f}u  {winner}')


print('=== H1: FOLLOW vs FADE per sport ===')
for sport in SPORT_THRESHOLDS:
    subset = [d for d in dataset if d['sport'] == sport]
    if not subset:
        continue
    summarize(f'{sport}', subset)

print()
print('=== H2: Per-sport × disagreement bucket (at/above current threshold) ===')
for sport, th in SPORT_THRESHOLDS.items():
    sub = [d for d in dataset if d['sport'] == sport and d['abs_disagreement'] >= th]
    summarize(f'{sport} gap >= {th} (current threshold)', sub)

print()
print('=== H3: disagreement buckets per sport ===')
for sport, th in SPORT_THRESHOLDS.items():
    print(f'  -- {sport} (current threshold {th}) --')
    for lo, hi in [(0.1, 0.5), (0.5, 1.0), (1.0, 2.0), (2.0, 3.0), (3.0, 5.0), (5.0, 99)]:
        sub = [d for d in dataset if d['sport'] == sport and lo <= d['abs_disagreement'] < hi]
        if not sub:
            continue
        summarize(f'   gap {lo}-{hi}', sub)

print()
print('=== H4: Dog vs Favorite side per sport (at/above current threshold) ===')
for sport, th in SPORT_THRESHOLDS.items():
    at_th = [d for d in dataset if d['sport'] == sport and d['abs_disagreement'] >= th]
    dogs = [d for d in at_th if d['ctx_pick_is_dog']]
    favs = [d for d in at_th if not d['ctx_pick_is_dog']]
    summarize(f'{sport} Ctx picks DOG (at threshold)', dogs)
    summarize(f'{sport} Ctx picks FAV (at threshold)', favs)

print()
print('=== H5: What threshold MAXIMIZES cumulative FOLLOW P/L per sport? ===')
for sport in SPORT_THRESHOLDS:
    best = None
    for th in [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0]:
        sub = [d for d in dataset if d['sport'] == sport and d['abs_disagreement'] >= th]
        if len(sub) < 5:
            continue
        ev = sum(d['follow_pnl'] for d in sub)
        w = sum(1 for d in sub if d['follow_pnl'] > 0)
        l = sum(1 for d in sub if d['follow_pnl'] < 0)
        wr = w / (w + l) * 100 if (w + l) else 0
        if best is None or ev > best['ev']:
            best = {'th': th, 'n': len(sub), 'ev': ev, 'wr': wr, 'w': w, 'l': l}
    if best:
        print(f'  {sport}: best threshold = {best["th"]:.2f}  '
              f'n={best["n"]}  {best["w"]}-{best["l"]}  WR={best["wr"]:.1f}%  EV={best["ev"]:+.2f}u')

conn.close()
