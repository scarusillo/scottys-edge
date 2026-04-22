"""Temporary backtest script for Context Model calibration."""
import sqlite3, sys, os
sys.path.insert(0, os.path.dirname(__file__))
from context_engine import get_context_adjustments

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()

SPORTS = ['baseball_mlb', 'soccer_usa_mls', 'soccer_spain_la_liga',
          'soccer_germany_bundesliga', 'soccer_france_ligue_one',
          'icehockey_nhl', 'basketball_nba']

q = """SELECT r.sport, r.home, r.away, r.event_id, r.commence_time, r.actual_total,
              (SELECT AVG(line) FROM odds WHERE event_id = r.event_id AND market='totals' AND line IS NOT NULL) as mkt,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='totals' AND selection LIKE '%Over%') as over_odds,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='totals' AND selection LIKE '%Under%') as under_odds
       FROM results r
       WHERE r.sport IN ({}) AND r.completed=1 AND r.actual_total IS NOT NULL
         AND r.commence_time >= date('now','-30 days')
         AND r.event_id IN (SELECT DISTINCT event_id FROM odds WHERE market='totals' AND line IS NOT NULL)
       LIMIT 800""".format(','.join('?' * len(SPORTS)))

rows = c.execute(q, SPORTS).fetchall()
print(f'Games: {len(rows)}')

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
        if abs(gap) < 0.3:
            continue
        direction = 'OVER' if gap > 0 else 'UNDER'
        actual_dir = 'OVER' if actual > mkt else ('UNDER' if actual < mkt else 'PUSH')
        use_odds = over_odds if direction == 'OVER' else under_odds
        if use_odds is None or use_odds == 0:
            use_odds = -110
        if actual_dir == 'PUSH':
            follow_pnl = 0
        elif actual_dir == direction:
            follow_pnl = (100 / abs(use_odds)) if use_odds < 0 else (use_odds / 100)
        else:
            follow_pnl = -1
        fade_dir = 'UNDER' if direction == 'OVER' else 'OVER'
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
            'sport': sport, 'gap': gap, 'abs_gap': abs(gap), 'direction': direction,
            'actual_dir': actual_dir, 'follow_pnl': follow_pnl, 'fade_pnl': fade_pnl
        })
    except Exception:
        pass

print(f'Dataset: {len(dataset)}')
print()
print('=== FOLLOW (bet Context direction) ===')
for lo, hi in [(0.3, 0.5), (0.5, 0.75), (0.75, 1.0), (1.0, 1.5), (1.5, 2.0), (2.0, 3.0), (3.0, 99)]:
    sub = [d for d in dataset if lo <= d['abs_gap'] < hi]
    if not sub:
        continue
    w = sum(1 for d in sub if d['follow_pnl'] > 0)
    l = sum(1 for d in sub if d['follow_pnl'] < 0)
    ev = sum(d['follow_pnl'] for d in sub)
    n = len(sub)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f'  gap {lo:.2f}-{hi:.2f} n={n} {w}W-{l}L WR={wr:.1f}% EV={ev:+.2f}u ({ev/n:+.3f}u/pick)')

print()
print('=== FADE (bet OPPOSITE of Context) ===')
for lo, hi in [(0.3, 0.5), (0.5, 0.75), (0.75, 1.0), (1.0, 1.5), (1.5, 2.0), (2.0, 3.0), (3.0, 99)]:
    sub = [d for d in dataset if lo <= d['abs_gap'] < hi]
    if not sub:
        continue
    w = sum(1 for d in sub if d['fade_pnl'] > 0)
    l = sum(1 for d in sub if d['fade_pnl'] < 0)
    ev = sum(d['fade_pnl'] for d in sub)
    n = len(sub)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f'  gap {lo:.2f}-{hi:.2f} n={n} {w}W-{l}L WR={wr:.1f}% EV={ev:+.2f}u ({ev/n:+.3f}u/pick)')

print()
print('=== BY SPORT + DIRECTION (FOLLOW) ===')
for s in sorted(set(d['sport'] for d in dataset)):
    for direction in ('OVER', 'UNDER'):
        sub = [d for d in dataset if d['sport'] == s and d['direction'] == direction]
        if not sub:
            continue
        w = sum(1 for d in sub if d['follow_pnl'] > 0)
        l = sum(1 for d in sub if d['follow_pnl'] < 0)
        ev = sum(d['follow_pnl'] for d in sub)
        n = len(sub)
        wr = w / (w + l) * 100 if (w + l) else 0
        print(f'  {s:<30} Ctx {direction:<5} n={n:<3} {w}W-{l}L WR={wr:.1f}% EV={ev:+.2f}u')

conn.close()
