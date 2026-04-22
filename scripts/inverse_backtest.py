"""Inverse / fade backtest — what if we bet OPPOSITE Context direction?

Hypothesis tests:
  H1: Fade Context in soccer (Context calibration biased one way)
  H2: Fade Context overall (model is systematically wrong direction)
  H3: Per-sport fade vs follow split — find where fade wins
"""
import sqlite3, sys, os
sys.path.insert(0, os.path.dirname(__file__))
from context_engine import get_context_adjustments

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()

ALL_SPORTS = [
    'baseball_mlb', 'baseball_ncaa',
    'basketball_nba', 'basketball_ncaab',
    'icehockey_nhl',
    'soccer_usa_mls', 'soccer_spain_la_liga', 'soccer_germany_bundesliga',
    'soccer_france_ligue_one', 'soccer_epl', 'soccer_italy_serie_a',
    'soccer_uefa_champs_league',
]

q = """SELECT r.sport, r.home, r.away, r.event_id, r.commence_time, r.actual_total,
              (SELECT AVG(line) FROM odds WHERE event_id = r.event_id AND market='totals' AND line IS NOT NULL) as mkt,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='totals' AND selection LIKE '%Over%') as over_odds,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='totals' AND selection LIKE '%Under%') as under_odds
       FROM results r
       WHERE r.sport IN ({}) AND r.completed=1 AND r.actual_total IS NOT NULL
         AND r.commence_time >= date('now','-90 days')
         AND r.event_id IN (SELECT DISTINCT event_id FROM odds WHERE market='totals' AND line IS NOT NULL)
       LIMIT 400""".format(','.join('?' * len(ALL_SPORTS)))

rows = c.execute(q, ALL_SPORTS).fetchall()
print(f'Games pulled: {len(rows)}')

dataset = []
for sport, home, away, eid, commence, actual, mkt, over_odds, under_odds in rows:
    if not mkt:
        continue
    try:
        ret = get_context_adjustments(conn, sport, home, away, eid, commence, market_type='TOTAL')
        total_adj = 0
        if isinstance(ret, tuple):
            total_adj = ret[1] if len(ret) > 1 else 0
        elif isinstance(ret, dict):
            total_adj = ret.get('total_adj', 0)
        if total_adj is None:
            total_adj = 0
        gap = total_adj
        if abs(gap) < 0.1:
            continue
        ctx_direction = 'OVER' if gap > 0 else 'UNDER'
        actual_dir = 'OVER' if actual > mkt else ('UNDER' if actual < mkt else 'PUSH')
        use_odds = over_odds if ctx_direction == 'OVER' else under_odds
        if use_odds is None or use_odds == 0:
            use_odds = -110
        # FOLLOW: bet ctx_direction
        if actual_dir == 'PUSH':
            follow_pnl = 0
        elif actual_dir == ctx_direction:
            follow_pnl = (100 / abs(use_odds)) if use_odds < 0 else (use_odds / 100)
        else:
            follow_pnl = -1
        # FADE: bet opposite
        fade_dir = 'UNDER' if ctx_direction == 'OVER' else 'OVER'
        fade_odds = under_odds if fade_dir == 'UNDER' else over_odds
        if fade_odds is None or fade_odds == 0:
            fade_odds = -110
        if actual_dir == 'PUSH':
            fade_pnl = 0
        elif actual_dir == fade_dir:
            fade_pnl = (100 / abs(fade_odds)) if fade_odds < 0 else (fade_odds / 100)
        else:
            fade_pnl = -1
        dataset.append({
            'sport': sport, 'gap': gap, 'abs_gap': abs(gap),
            'ctx_direction': ctx_direction, 'actual_dir': actual_dir,
            'follow_pnl': follow_pnl, 'fade_pnl': fade_pnl
        })
    except Exception:
        pass

print(f'Dataset: {len(dataset)}')
print()

# ═══ H1: FADE soccer specifically ═══
print('=== H1: FADE CONTEXT (all soccer leagues combined, last 90d) ===')
soccer = [d for d in dataset if d['sport'].startswith('soccer_')]
if soccer:
    f_w = sum(1 for d in soccer if d['follow_pnl'] > 0)
    f_l = sum(1 for d in soccer if d['follow_pnl'] < 0)
    f_ev = sum(d['follow_pnl'] for d in soccer)
    fade_w = sum(1 for d in soccer if d['fade_pnl'] > 0)
    fade_l = sum(1 for d in soccer if d['fade_pnl'] < 0)
    fade_ev = sum(d['fade_pnl'] for d in soccer)
    n = len(soccer)
    print(f'  FOLLOW: n={n} {f_w}W-{f_l}L  EV={f_ev:+.2f}u ({f_ev/n:+.3f}u/pick)')
    print(f'  FADE:   n={n} {fade_w}W-{fade_l}L  EV={fade_ev:+.2f}u ({fade_ev/n:+.3f}u/pick)')

print()
print('=== H2: FADE CONTEXT (ALL sports combined, last 90d) ===')
n_all = len(dataset)
f_ev = sum(d['follow_pnl'] for d in dataset)
f_w = sum(1 for d in dataset if d['follow_pnl'] > 0)
f_l = sum(1 for d in dataset if d['follow_pnl'] < 0)
fade_ev = sum(d['fade_pnl'] for d in dataset)
fade_w = sum(1 for d in dataset if d['fade_pnl'] > 0)
fade_l = sum(1 for d in dataset if d['fade_pnl'] < 0)
print(f'  FOLLOW: n={n_all} {f_w}W-{f_l}L  EV={f_ev:+.2f}u ({f_ev/n_all:+.3f}u/pick)')
print(f'  FADE:   n={n_all} {fade_w}W-{fade_l}L  EV={fade_ev:+.2f}u ({fade_ev/n_all:+.3f}u/pick)')

print()
print('=== H3: FADE vs FOLLOW split per sport (min n=5) ===')
print(f'{"Sport":<28} {"N":>3} {"FOLLOW WR":>10} {"FOLLOW EV":>11} {"FADE WR":>9} {"FADE EV":>11}  Winner')
for sport in ALL_SPORTS:
    sub = [d for d in dataset if d['sport'] == sport]
    if len(sub) < 5:
        continue
    n = len(sub)
    f_w = sum(1 for d in sub if d['follow_pnl'] > 0)
    f_l = sum(1 for d in sub if d['follow_pnl'] < 0)
    f_ev = sum(d['follow_pnl'] for d in sub)
    fade_w = sum(1 for d in sub if d['fade_pnl'] > 0)
    fade_l = sum(1 for d in sub if d['fade_pnl'] < 0)
    fade_ev = sum(d['fade_pnl'] for d in sub)
    f_wr = f_w / (f_w + f_l) * 100 if (f_w + f_l) else 0
    fade_wr = fade_w / (fade_w + fade_l) * 100 if (fade_w + fade_l) else 0
    winner = 'FADE' if fade_ev > f_ev else ('FOLLOW' if f_ev > fade_ev else 'TIE')
    print(f'  {sport:<28} {n:>3} {f_wr:>8.1f}% {f_ev:>+9.2f}u {fade_wr:>7.1f}% {fade_ev:>+9.2f}u  {winner}')

print()
print('=== H3b: FADE vs FOLLOW per sport × Context direction ===')
print(f'{"Sport":<28} {"CtxDir":<6} {"N":>3} {"FOLLOW EV":>11} {"FADE EV":>11}  Winner')
for sport in ALL_SPORTS:
    for ctx_dir in ('OVER', 'UNDER'):
        sub = [d for d in dataset if d['sport'] == sport and d['ctx_direction'] == ctx_dir]
        if len(sub) < 3:
            continue
        n = len(sub)
        f_ev = sum(d['follow_pnl'] for d in sub)
        fade_ev = sum(d['fade_pnl'] for d in sub)
        winner = 'FADE' if fade_ev > f_ev else ('FOLLOW' if f_ev > fade_ev else 'TIE')
        print(f'  {sport:<28} {ctx_dir:<6} {n:>3} {f_ev:>+9.2f}u {fade_ev:>+9.2f}u  {winner}')

print()
print('=== H4: Gap-bucket analysis — is fade profitable at SMALL gaps (noise zone)? ===')
for lo, hi in [(0.10, 0.30), (0.30, 0.50), (0.50, 0.75), (0.75, 1.00),
               (1.00, 1.50), (1.50, 2.00), (2.00, 99)]:
    sub = [d for d in dataset if lo <= d['abs_gap'] < hi]
    if not sub:
        continue
    n = len(sub)
    f_ev = sum(d['follow_pnl'] for d in sub)
    fade_ev = sum(d['fade_pnl'] for d in sub)
    winner = 'FADE' if fade_ev > f_ev else ('FOLLOW' if f_ev > fade_ev else 'TIE')
    print(f'  gap {lo:.2f}-{hi:.2f}  n={n:<3} FOLLOW EV={f_ev:+.2f}u  FADE EV={fade_ev:+.2f}u  {winner}')

conn.close()
