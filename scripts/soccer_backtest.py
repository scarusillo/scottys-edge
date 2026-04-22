"""Soccer Context Model threshold calibration — per league, per direction."""
import sqlite3, sys, os
sys.path.insert(0, os.path.dirname(__file__))
from context_engine import get_context_adjustments

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()

# Include all soccer leagues currently in Context Total scope + EPL/Serie A/UCL for comparison
SPORTS = [
    'soccer_usa_mls', 'soccer_spain_la_liga', 'soccer_germany_bundesliga',
    'soccer_france_ligue_one', 'soccer_epl', 'soccer_italy_serie_a',
    'soccer_uefa_champs_league'
]

# 90-day window for better sample
q = """SELECT r.sport, r.home, r.away, r.event_id, r.commence_time, r.actual_total,
              (SELECT AVG(line) FROM odds WHERE event_id = r.event_id AND market='totals' AND line IS NOT NULL) as mkt,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='totals' AND selection LIKE '%Over%') as over_odds,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='totals' AND selection LIKE '%Under%') as under_odds
       FROM results r
       WHERE r.sport IN ({}) AND r.completed=1 AND r.actual_total IS NOT NULL
         AND r.commence_time >= date('now','-90 days')
         AND r.event_id IN (SELECT DISTINCT event_id FROM odds WHERE market='totals' AND line IS NOT NULL)
       LIMIT 2000""".format(','.join('?' * len(SPORTS)))

rows = c.execute(q, SPORTS).fetchall()
print(f'Soccer games with market totals in 90d: {len(rows)}')

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
        direction = 'OVER' if gap > 0 else 'UNDER'
        actual_dir = 'OVER' if actual > mkt else ('UNDER' if actual < mkt else 'PUSH')
        use_odds = over_odds if direction == 'OVER' else under_odds
        if use_odds is None or use_odds == 0:
            use_odds = -110
        if actual_dir == 'PUSH':
            pnl = 0
        elif actual_dir == direction:
            pnl = (100 / abs(use_odds)) if use_odds < 0 else (use_odds / 100)
        else:
            pnl = -1
        dataset.append({
            'sport': sport, 'gap': gap, 'abs_gap': abs(gap),
            'direction': direction, 'actual_dir': actual_dir, 'pnl': pnl,
            'mkt': mkt, 'actual': actual
        })
    except Exception:
        pass

print(f'Dataset with Context gap >= 0.1: {len(dataset)}')
print()

# For each soccer league, find the sweet-spot threshold
# by direction (OVER / UNDER / COMBINED)
LEAGUE_LABELS = {
    'soccer_usa_mls': 'MLS',
    'soccer_spain_la_liga': 'La Liga',
    'soccer_germany_bundesliga': 'Bundesliga',
    'soccer_france_ligue_one': 'Ligue 1',
    'soccer_epl': 'EPL',
    'soccer_italy_serie_a': 'Serie A',
    'soccer_uefa_champs_league': 'UCL',
}

for sport in SPORTS:
    label = LEAGUE_LABELS.get(sport, sport)
    subset = [d for d in dataset if d['sport'] == sport]
    if not subset:
        print(f'=== {label}: NO DATA in 90d ===\n')
        continue
    print(f'=== {label} — n={len(subset)} total in 90d ===')
    for direction in ('OVER', 'UNDER', 'BOTH'):
        if direction == 'BOTH':
            sub_dir = subset
        else:
            sub_dir = [d for d in subset if d['direction'] == direction]
        if not sub_dir:
            continue
        print(f'  {direction}:')
        for lo, hi in [(0.10, 0.30), (0.30, 0.50), (0.50, 0.75), (0.75, 1.00),
                       (1.00, 1.25), (1.25, 1.50), (1.50, 99)]:
            sub = [d for d in sub_dir if lo <= d['abs_gap'] < hi]
            if not sub:
                continue
            w = sum(1 for d in sub if d['pnl'] > 0)
            l = sum(1 for d in sub if d['pnl'] < 0)
            ev = sum(d['pnl'] for d in sub)
            n = len(sub)
            wr = w / (w + l) * 100 if (w + l) else 0
            marker = ' ← PROFITABLE' if (w + l >= 5 and wr >= 52.4) else ''
            print(f'    gap {lo:.2f}-{hi:.2f}: n={n:<3} {w}W-{l}L WR={wr:.1f}% EV={ev:+.2f}u ({ev/n:+.3f}u/pick){marker}')
    print()

# CUMULATIVE analysis — for each threshold ≥ X, what's the performance
print('=== CUMULATIVE sweet-spot analysis (gap >= X, by league x direction) ===')
for sport in SPORTS:
    label = LEAGUE_LABELS.get(sport, sport)
    subset = [d for d in dataset if d['sport'] == sport]
    if not subset:
        continue
    print(f'\n{label}:')
    for direction in ('OVER', 'UNDER'):
        sub_dir = [d for d in subset if d['direction'] == direction]
        if len(sub_dir) < 3:
            print(f'  {direction}: n={len(sub_dir)} (sample too small for threshold search)')
            continue
        print(f'  {direction}:')
        for thresh in [0.3, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0]:
            sub = [d for d in sub_dir if d['abs_gap'] >= thresh]
            if not sub:
                continue
            w = sum(1 for d in sub if d['pnl'] > 0)
            l = sum(1 for d in sub if d['pnl'] < 0)
            ev = sum(d['pnl'] for d in sub)
            n = len(sub)
            wr = w / (w + l) * 100 if (w + l) else 0
            verdict = 'PROFIT' if (n >= 5 and ev > 0) else ('LOSING' if ev < 0 else 'BREAKEVEN')
            print(f'    >={thresh}: n={n:<3} {w}W-{l}L WR={wr:.1f}% EV={ev:+.2f}u {verdict}')

conn.close()
