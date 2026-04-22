"""Steam-chase backtest — fire on sharp-book movement, no model required.

Logic: if sharp-book (FanDuel as proxy — DraftKings soft) moved its line
between opener and close in direction Y by threshold T, did direction Y
actually cover at the opener line?

If WR >= 54% at some movement threshold, we have a new channel that fires
regardless of our Elo/Context model — pure market-pattern recognition.

Sports: NBA, NHL, MLB, NCAAB, NCAA Baseball.
Markets: spreads (home line) + totals (over/under line).
"""
import sqlite3
import os
from collections import defaultdict

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()

SPORTS = ['baseball_mlb', 'icehockey_nhl', 'basketball_nba',
          'basketball_ncaab', 'baseball_ncaa', 'soccer_usa_mls',
          'soccer_italy_serie_a', 'soccer_epl']
SHARP_BOOK = 'FanDuel'  # Our sharp proxy


def analyze_sport(sport):
    """Measure steam-chase P/L for this sport."""
    # Pull games with opener + closing/current data + results
    q = """
        SELECT r.event_id, r.home, r.away, r.home_score, r.away_score,
               r.commence_time,
               (SELECT line FROM openers WHERE event_id = r.event_id
                 AND book = ? AND market = 'spreads'
                 AND selection LIKE '%' || r.home || '%' LIMIT 1) as sharp_opener_spread_home,
               (SELECT AVG(line) FROM odds WHERE event_id = r.event_id
                 AND book = ? AND market = 'spreads' AND tag = 'CURRENT'
                 AND selection LIKE '%' || r.home || '%') as sharp_current_spread_home,
               (SELECT line FROM openers WHERE event_id = r.event_id
                 AND book = ? AND market = 'totals'
                 AND selection LIKE '%Over%' LIMIT 1) as sharp_opener_total,
               (SELECT AVG(line) FROM odds WHERE event_id = r.event_id
                 AND book = ? AND market = 'totals' AND tag = 'CURRENT'
                 AND selection LIKE '%Over%') as sharp_current_total
        FROM results r
        WHERE r.sport = ? AND r.completed = 1
          AND r.home_score IS NOT NULL AND r.away_score IS NOT NULL
          AND r.commence_time >= date('now','-120 days')
    """
    rows = c.execute(q, (SHARP_BOOK, SHARP_BOOK, SHARP_BOOK, SHARP_BOOK, sport)).fetchall()

    spread_candidates = []
    total_candidates = []
    for (eid, home, away, hs, as_, commence,
         sp_open, sp_cur, tot_open, tot_cur) in rows:
        # SPREAD steam
        if sp_open is not None and sp_cur is not None and sp_open != sp_cur:
            # Positive move = home_spread got more negative = HOME more favored (sharps on HOME)
            move = sp_open - sp_cur  # e.g., -7 opener, -8 current → move = +1 (HOME steam)
            # If move > 0: sharps on HOME; if move < 0: sharps on AWAY
            steam_side = 'HOME' if move > 0 else 'AWAY'
            abs_move = abs(move)
            # Would we cover at opener line betting steam_side?
            net = (hs + sp_open) - as_
            if abs(net) < 0.001:
                cover = 'PUSH'
            elif net > 0:
                cover = 'HOME'
            else:
                cover = 'AWAY'
            if cover == 'PUSH':
                pnl = 0
            elif cover == steam_side:
                pnl = 100/110  # Assume -110 juice
            else:
                pnl = -1
            spread_candidates.append({'move': abs_move, 'pnl': pnl})
        # TOTAL steam
        if tot_open is not None and tot_cur is not None and tot_open != tot_cur:
            # Positive move = total went UP (sharps on OVER)
            move = tot_cur - tot_open
            steam_side = 'OVER' if move > 0 else 'UNDER'
            abs_move = abs(move)
            actual = hs + as_
            if abs(actual - tot_open) < 0.001:
                cover = 'PUSH'
            elif actual > tot_open:
                cover = 'OVER'
            else:
                cover = 'UNDER'
            if cover == 'PUSH':
                pnl = 0
            elif cover == steam_side:
                pnl = 100/110
            else:
                pnl = -1
            total_candidates.append({'move': abs_move, 'pnl': pnl})
    return spread_candidates, total_candidates


def report(label, candidates):
    if not candidates:
        print(f'  {label}: no data')
        return
    print(f'  {label} (total games with movement: {len(candidates)})')
    print(f'    {"threshold":<12} {"N":>4} {"W-L":>7} {"WR":>6} {"P/L":>8}')
    for th in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        sub = [d for d in candidates if d['move'] >= th]
        if len(sub) < 5:
            continue
        w = sum(1 for d in sub if d['pnl'] > 0)
        l = sum(1 for d in sub if d['pnl'] < 0)
        ev = sum(d['pnl'] for d in sub)
        wr = w / (w + l) * 100 if (w + l) else 0
        mark = '🔥' if wr >= 54 else ''
        print(f'    >={th:.1f}        {len(sub):>4} {w}-{l:<3} {wr:>5.1f}% {ev:>+7.2f}u {mark}')


for sport in SPORTS:
    print(f'\n=== {sport} ===')
    spreads, totals = analyze_sport(sport)
    report('SPREADS (sharp moves HOME line)', spreads)
    report('TOTALS (sharp moves line up/down)', totals)

# Combined
print('\n=== COMBINED across all sports ===')
all_spreads = []
all_totals = []
for sport in SPORTS:
    s, t = analyze_sport(sport)
    all_spreads.extend(s)
    all_totals.extend(t)
report('All spread steams', all_spreads)
report('All total steams', all_totals)

conn.close()
