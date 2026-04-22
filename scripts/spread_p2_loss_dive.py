"""Surgical loss analysis on DATA_SPREAD Path 2 backtest sample.

Goal: find WHERE the losses concentrate. If a specific cohort loses big,
we can add a gate. If losses are evenly distributed, no surgical fix works.

Dimensions sliced:
  - Spread magnitude (small vs blowout spreads)
  - Home favored vs away favored
  - Day-of-week
  - Market odds (heavy juice vs plus)
  - Context disagreement direction (home-favoring vs away-favoring)
  - Cover margin (how close to the line was the actual outcome)
  - Per-game specifics
"""
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from context_model import compute_context_spread

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()

SPORT_THRESHOLDS = {
    'basketball_nba': 2.5,
    'icehockey_nhl': 0.5,
    'soccer_italy_serie_a': 0.5,
}

q = """SELECT r.sport, r.home, r.away, r.event_id, r.commence_time,
              r.home_score, r.away_score,
              (SELECT AVG(line) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.home || '%' AND line IS NOT NULL) as home_spread,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.home || '%') as home_odds,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.away || '%') as away_odds
       FROM results r
       WHERE r.sport IN ({}) AND r.completed=1
         AND r.home_score IS NOT NULL AND r.away_score IS NOT NULL
         AND r.commence_time >= date('now','-90 days')
         AND r.event_id IN (SELECT DISTINCT event_id FROM odds WHERE market='spreads' AND line IS NOT NULL)""".format(
    ','.join('?' * len(SPORT_THRESHOLDS)))

rows = c.execute(q, list(SPORT_THRESHOLDS.keys())).fetchall()

dataset = []
for sport, home, away, eid, commence, hs, as_, home_spread, home_odds, away_odds in rows:
    if home_spread is None or hs is None or as_ is None:
        continue
    try:
        ms_ctx, info = compute_context_spread(
            conn, sport, home, away, eid, home_spread, commence[:10] if commence else None)
        disagreement = ms_ctx - home_spread
        abs_dis = abs(disagreement)
        th = SPORT_THRESHOLDS[sport]
        if abs_dis < th:
            continue  # Only fire-eligible picks

        # Context bets HOME if ms_ctx < home_spread (more negative = home more favored)
        pick_side = 'HOME' if ms_ctx < home_spread else 'AWAY'
        # Actual cover: home_score + home_spread > away_score → HOME covered
        net = (hs + home_spread) - as_
        if abs(net) < 0.001:
            actual = 'PUSH'
        elif net > 0:
            actual = 'HOME'
        else:
            actual = 'AWAY'

        won = (actual == pick_side)
        pushed = (actual == 'PUSH')

        # Odds for the pick side
        use_odds = home_odds if pick_side == 'HOME' else away_odds
        if use_odds is None or use_odds == 0:
            use_odds = -110
        if pushed:
            pnl = 0
        elif won:
            pnl = (100 / abs(use_odds)) if use_odds < 0 else (use_odds / 100)
        else:
            pnl = -1

        # Cover margin = how much we covered by (positive = easy cover, negative = close/lost)
        if pick_side == 'HOME':
            cover_margin = net
        else:
            cover_margin = -net

        # Day of week from commence
        from datetime import datetime as _dt
        try:
            dow = _dt.fromisoformat(commence.replace('Z', '+00:00')).strftime('%a')
        except Exception:
            dow = '?'

        dataset.append({
            'sport': sport,
            'home': home, 'away': away,
            'home_spread': home_spread,
            'abs_spread': abs(home_spread),
            'home_odds': home_odds,
            'away_odds': away_odds,
            'pick_side': pick_side,
            'pick_odds': use_odds,
            'disagreement': disagreement,
            'abs_dis': abs_dis,
            'won': won,
            'pushed': pushed,
            'pnl': pnl,
            'cover_margin': cover_margin,
            'dow': dow,
            'is_home_favored': home_spread < 0,
            'pick_is_dog': (pick_side == 'HOME' and home_spread > 0) or (pick_side == 'AWAY' and home_spread < 0),
            # Dominance signal
            'form_adj': info.get('form_adj', 0) or 0,
            'mom_adj': info.get('momentum_adj', 0) or 0,
            'inj_adj': info.get('injury_adj', 0) or 0,
            'h2h_adj': info.get('h2h_adj', 0) or 0,
            'hca_adj': info.get('hca_adj', 0) or 0,
            'inj_amp': info.get('injury_amp_adj', 0) or 0,
        })
    except Exception:
        pass

print(f'Dataset: {len(dataset)} fire-eligible picks')
print()


def summarize(label, subset):
    if not subset:
        return
    n = len(subset)
    w = sum(1 for d in subset if d['won'])
    l = sum(1 for d in subset if not d['won'] and not d['pushed'])
    ev = sum(d['pnl'] for d in subset)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f'  {label:<45} n={n:<3} {w}W-{l}L WR={wr:.1f}% EV={ev:+.2f}u')


# By dominance signal
print('=== By DOMINANCE signal (largest absolute adjustment drove the fire) ===')
dom_buckets = {}
for d in dataset:
    candidates = {'form': abs(d['form_adj']), 'momentum': abs(d['mom_adj']),
                  'injury': abs(d['inj_adj']), 'h2h': abs(d['h2h_adj']),
                  'hca': abs(d['hca_adj']), 'injury_amp': abs(d['inj_amp'])}
    if all(v == 0 for v in candidates.values()):
        dom = 'other'
    else:
        dom = max(candidates, key=candidates.get)
    dom_buckets.setdefault(dom, []).append(d)
for dom, sub in sorted(dom_buckets.items(), key=lambda x: -sum(d['pnl'] for d in x[1])):
    summarize(f'dominance={dom}', sub)

print()
print('=== By SPREAD magnitude (how big is the market line) ===')
for lo, hi in [(0, 3), (3, 6), (6, 9), (9, 13), (13, 999)]:
    sub = [d for d in dataset if lo <= d['abs_spread'] < hi]
    if sub:
        summarize(f'|spread| {lo}-{hi}', sub)

print()
print('=== By COVER MARGIN (how close to line) — shows if we lose by a lot or by a hair ===')
buckets = [('WIN big (+10+)', lambda d: d['pnl'] > 0 and d['cover_margin'] >= 10),
           ('WIN close (+0 to +10)', lambda d: d['pnl'] > 0 and 0 < d['cover_margin'] < 10),
           ('LOSS close (0 to -5)', lambda d: d['pnl'] < 0 and d['cover_margin'] >= -5),
           ('LOSS medium (-5 to -10)', lambda d: d['pnl'] < 0 and -10 <= d['cover_margin'] < -5),
           ('LOSS big (-10+)', lambda d: d['pnl'] < 0 and d['cover_margin'] < -10)]
for label, fn in buckets:
    sub = [d for d in dataset if fn(d)]
    if sub:
        n = len(sub)
        total_pnl = sum(d['pnl'] for d in sub)
        print(f'  {label:<28} n={n:<3} P/L={total_pnl:+.2f}u')

print()
print('=== By HOME-FAVORED vs AWAY-FAVORED ===')
for label, fn in [('Home favored (market home<0)', lambda d: d['is_home_favored']),
                   ('Away favored (market home>0)', lambda d: not d['is_home_favored'])]:
    sub = [d for d in dataset if fn(d)]
    if sub: summarize(label, sub)

print()
print('=== By CONTEXT bet direction ===')
for label, fn in [('Context picks HOME', lambda d: d['pick_side'] == 'HOME'),
                   ('Context picks AWAY', lambda d: d['pick_side'] == 'AWAY')]:
    sub = [d for d in dataset if fn(d)]
    if sub: summarize(label, sub)

print()
print('=== By DOG vs FAVORITE pick ===')
for label, fn in [('Pick is DOG', lambda d: d['pick_is_dog']),
                   ('Pick is FAVORITE', lambda d: not d['pick_is_dog'])]:
    sub = [d for d in dataset if fn(d)]
    if sub: summarize(label, sub)

print()
print('=== Individual losers ranked by cover_margin worst-first (biggest blowout losses) ===')
losers = sorted([d for d in dataset if not d['won'] and not d['pushed']],
                key=lambda d: d['cover_margin'])[:15]
for d in losers:
    print(f'  {d["sport"][-8:]:<8} {d["home"][:15]:<15} {d["home_spread"]:+.1f} vs {d["away"][:15]:<15}  pick={d["pick_side"]}  dis={d["disagreement"]:+.1f}  cover={d["cover_margin"]:+.1f}')

print()
print('=== Combined signal: dominance × pick_is_dog ===')
dom_dog = {}
for d in dataset:
    candidates = {'form': abs(d['form_adj']), 'momentum': abs(d['mom_adj']),
                  'injury': abs(d['inj_adj']), 'h2h': abs(d['h2h_adj'])}
    if all(v == 0 for v in candidates.values()):
        dom = 'other'
    else:
        dom = max(candidates, key=candidates.get)
    key = (dom, 'DOG' if d['pick_is_dog'] else 'FAV')
    dom_dog.setdefault(key, []).append(d)
for (dom, dfav), sub in sorted(dom_dog.items(), key=lambda x: -sum(d['pnl'] for d in x[1])):
    if len(sub) < 3: continue
    summarize(f'{dom} × {dfav}', sub)

conn.close()
