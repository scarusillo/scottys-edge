"""DATA_SPREAD calibration analysis — measure HOW wrong Context is.

For each fire-eligible game, compute:
  market_prediction = -home_spread (margin home is expected to win by)
  context_prediction = -ms_ctx (margin Context expects home to win by)
  actual_margin = home_score - away_score

Then: market_error = |market_pred - actual|, context_error = |context_pred - actual|.

If context_error < market_error → Context added value.
If context_error > market_error → Context hurt us.

Then slice by adjustment component: when form_adj is large, does Context add
or subtract value? Tells us which adjustments are overweighted.
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
              (SELECT AVG(line) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.home || '%' AND line IS NOT NULL) as home_spread
       FROM results r
       WHERE r.sport IN ({}) AND r.completed=1
         AND r.home_score IS NOT NULL AND r.away_score IS NOT NULL
         AND r.commence_time >= date('now','-90 days')
         AND r.event_id IN (SELECT DISTINCT event_id FROM odds WHERE market='spreads' AND line IS NOT NULL)""".format(
    ','.join('?' * len(SPORT_THRESHOLDS)))

rows = c.execute(q, list(SPORT_THRESHOLDS.keys())).fetchall()

dataset = []
for sport, home, away, eid, commence, hs, as_, home_spread in rows:
    if home_spread is None or hs is None or as_ is None:
        continue
    try:
        ms_ctx, info = compute_context_spread(
            conn, sport, home, away, eid, home_spread, commence[:10] if commence else None)
        # ms_ctx and home_spread are BOTH "home spread" (negative = home favored)
        # Convert to "expected home margin" by negating: -home_spread = expected margin
        market_pred = -home_spread
        context_pred = -ms_ctx
        actual_margin = hs - as_
        market_err = abs(market_pred - actual_margin)
        context_err = abs(context_pred - actual_margin)
        # How much "better" was Context? Positive = Context won the prediction
        edge = market_err - context_err
        disagreement = ms_ctx - home_spread
        abs_dis = abs(disagreement)

        # Only analyze fire-eligible picks
        th = SPORT_THRESHOLDS[sport]
        if abs_dis < th:
            continue

        dataset.append({
            'sport': sport,
            'home_spread': home_spread,
            'market_pred': market_pred,
            'context_pred': context_pred,
            'actual_margin': actual_margin,
            'market_err': market_err,
            'context_err': context_err,
            'edge': edge,  # positive = Context beat market
            'abs_dis': abs_dis,
            'form_adj': info.get('form_adj', 0) or 0,
            'mom_adj': info.get('momentum_adj', 0) or 0,
            'inj_adj': info.get('injury_adj', 0) or 0,
            'hca_adj': info.get('hca_adj', 0) or 0,
            'h2h_adj': info.get('h2h_adj', 0) or 0,
            'inj_amp_adj': info.get('injury_amp_adj', 0) or 0,
            'is_home_fav': home_spread < 0,
        })
    except Exception as e:
        pass

print(f'Dataset: {len(dataset)} fire-eligible picks')
print()

# ═══ 1. Overall: does Context beat market on average? ═══
total_market_err = sum(d['market_err'] for d in dataset)
total_context_err = sum(d['context_err'] for d in dataset)
n = len(dataset)
print('=== Aggregate prediction accuracy ===')
print(f'  Market avg absolute error: {total_market_err/n:.2f} points')
print(f'  Context avg absolute error: {total_context_err/n:.2f} points')
print(f'  Context better by: {(total_market_err-total_context_err)/n:+.2f} pts/game')
if total_context_err > total_market_err:
    print(f'  ⚠ Context is WORSE on average than market by {(total_context_err-total_market_err)/n:.2f} pts/game')
else:
    print(f'  ✓ Context is BETTER on average')

# ═══ 2. By adjustment size: when form_adj is big, is Context more accurate? ═══
print()
print('=== When each adjustment is LARGE, is Context more or less accurate than market? ===')
print(f'{"Adjustment":<15} {"threshold":>10} {"N":>4} {"mkt_err":>8} {"ctx_err":>8} {"ctx_better":>11}')
for adj_name, key in [('form_adj', 'form_adj'), ('mom_adj', 'mom_adj'),
                       ('inj_adj', 'inj_adj'), ('hca_adj', 'hca_adj'),
                       ('h2h_adj', 'h2h_adj')]:
    for thresh in [0.5, 1.0, 2.0, 3.0, 5.0]:
        sub = [d for d in dataset if abs(d[key]) >= thresh]
        if len(sub) < 5:
            continue
        mkt = sum(d['market_err'] for d in sub) / len(sub)
        ctx = sum(d['context_err'] for d in sub) / len(sub)
        delta = mkt - ctx
        arrow = '✅' if delta > 0.5 else ('⚠️ ' if delta < -0.5 else '~')
        print(f'  |{adj_name}|>={thresh:<6} n={len(sub):<3} mkt={mkt:.2f} ctx={ctx:.2f}  delta={delta:+.2f} {arrow}')
    print()

# ═══ 3. By disagreement size: do bigger gaps actually produce better Context predictions? ═══
print('=== By disagreement magnitude — does Context get better at larger gaps? ===')
print(f'{"Gap bucket":<15} {"N":>4} {"mkt_err":>8} {"ctx_err":>8} {"context wins":>14}')
for lo, hi in [(0.5, 1.5), (1.5, 2.5), (2.5, 4.0), (4.0, 6.0), (6.0, 99)]:
    sub = [d for d in dataset if lo <= d['abs_dis'] < hi]
    if len(sub) < 3:
        continue
    mkt = sum(d['market_err'] for d in sub) / len(sub)
    ctx = sum(d['context_err'] for d in sub) / len(sub)
    ctx_wins = sum(1 for d in sub if d['context_err'] < d['market_err'])
    print(f'  gap {lo}-{hi:<5} n={len(sub):<3} mkt_err={mkt:.2f} ctx_err={ctx:.2f}  ctx_beats_mkt={ctx_wins}/{len(sub)}')

# ═══ 4. By sport ═══
print()
print('=== Per sport: Context vs market accuracy ===')
for sport in SPORT_THRESHOLDS:
    sub = [d for d in dataset if d['sport'] == sport]
    if not sub:
        continue
    mkt = sum(d['market_err'] for d in sub) / len(sub)
    ctx = sum(d['context_err'] for d in sub) / len(sub)
    ctx_wins = sum(1 for d in sub if d['context_err'] < d['market_err'])
    print(f'  {sport:<30} n={len(sub):<3} mkt={mkt:.2f} ctx={ctx:.2f}  ctx_beats_mkt={ctx_wins}/{len(sub)}')

# ═══ 5. Home-favored vs away-favored ═══
print()
print('=== Home-favored vs Away-favored games ===')
for label, fn in [('Home favored', lambda d: d['is_home_fav']),
                   ('Away favored', lambda d: not d['is_home_fav'])]:
    sub = [d for d in dataset if fn(d)]
    if not sub:
        continue
    mkt = sum(d['market_err'] for d in sub) / len(sub)
    ctx = sum(d['context_err'] for d in sub) / len(sub)
    ctx_wins = sum(1 for d in sub if d['context_err'] < d['market_err'])
    print(f'  {label:<15} n={len(sub):<3} mkt={mkt:.2f} ctx={ctx:.2f}  ctx_beats_mkt={ctx_wins}/{len(sub)}')

# ═══ 6. What Context got WRONG direction vs right direction ═══
print()
print('=== Context direction correctness ===')
# Direction correct: sign(market - actual) vs sign(context - actual)
# Better framed: did Context push closer to actual, or further?
pulled_toward = sum(1 for d in dataset if
                     abs(d['context_pred'] - d['actual_margin']) <
                     abs(d['market_pred'] - d['actual_margin']))
pulled_away = sum(1 for d in dataset if
                   abs(d['context_pred'] - d['actual_margin']) >
                   abs(d['market_pred'] - d['actual_margin']))
print(f'  Context pulled closer to actual: {pulled_toward}/{len(dataset)} = {pulled_toward/len(dataset)*100:.0f}%')
print(f'  Context pushed further from actual: {pulled_away}/{len(dataset)} = {pulled_away/len(dataset)*100:.0f}%')

# Compute optimal Context weight multiplier: if we scaled all Context adjustments by X%,
# what X minimizes total error?
print()
print('=== Optimal Context-adjustment scaling factor ===')
print('  (if we scaled all adjustments by X%, what X minimizes error?)')
best = None
for scale_pct in range(0, 210, 10):
    scale = scale_pct / 100.0
    scaled_err = 0
    for d in dataset:
        scaled_ctx = d['market_pred'] + (d['context_pred'] - d['market_pred']) * scale
        scaled_err += abs(scaled_ctx - d['actual_margin'])
    avg = scaled_err / len(dataset)
    if best is None or avg < best['err']:
        best = {'scale': scale_pct, 'err': avg}
    if scale_pct % 25 == 0:
        print(f'  scale={scale_pct}% avg_err={avg:.2f}')
print(f'  >>> BEST scale: {best["scale"]}% (avg err {best["err"]:.2f} pts)')

conn.close()
