"""Spread threshold sweep — find the sweet spot for each sport.

For each sport with recent spread market data, simulate: at threshold X%,
which historical picks would have fired, and what was the W/L + P/L?
Find the threshold that maximizes cumulative P/L.

Unlike our shipped 20% threshold, some sports (MLB runlines, NHL playoffs,
low-variance markets) may be profitable at 8-15% if the edge signal is
directionally correct even when magnitude is small.
"""
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()

SPORTS = ['baseball_mlb', 'icehockey_nhl', 'basketball_nba',
          'basketball_ncaab', 'baseball_ncaa']


def compute_edge(model_prob, market_odds):
    """(model_prob - implied) / implied."""
    if market_odds is None or market_odds == 0:
        return None
    if market_odds > 0:
        implied = 100 / (market_odds + 100)
    else:
        implied = abs(market_odds) / (abs(market_odds) + 100)
    if implied == 0:
        return None
    return (model_prob - implied) / implied * 100


def spread_to_win_prob(model_spread, home_spread):
    """Rough conversion: how often does (model_spread) cover (home_spread)?
    Model says ms points, market has home_spread. If ms < home_spread, home
    should cover more than market implies (home more favored).
    Convert to probability using a sport-agnostic sigmoid around the line.
    """
    # We compute "how many points better than market is our projection?"
    diff = home_spread - model_spread  # positive = home MORE favored than market
    # Use simple sigmoid: each 3pts of diff ≈ 10% probability shift
    # Classic spread cover probability at par is ~50%; shift by diff
    import math
    shift = diff * 0.0333  # 3pts → 10% shift
    return max(0.05, min(0.95, 0.5 + shift))


# For each sport, pull all historical games with both Elo ratings + market data
# + outcome. Then simulate what we'd fire at each threshold.
for sport in SPORTS:
    print(f'\n=== {sport} — threshold sweep (last 120 days) ===')

    q = """SELECT r.home, r.away, r.commence_time,
                  r.home_score, r.away_score,
                  (SELECT AVG(line) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.home || '%' AND line IS NOT NULL) as home_sp,
                  (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.home || '%') as home_odds,
                  (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.away || '%') as away_odds,
                  (SELECT final_rating FROM power_ratings WHERE team = r.home AND sport = r.sport ORDER BY run_timestamp DESC LIMIT 1) as h_pr,
                  (SELECT final_rating FROM power_ratings WHERE team = r.away AND sport = r.sport ORDER BY run_timestamp DESC LIMIT 1) as a_pr
           FROM results r
           WHERE r.sport = ? AND r.completed = 1
             AND r.home_score IS NOT NULL AND r.away_score IS NOT NULL
             AND r.commence_time >= date('now','-120 days')
             AND r.event_id IN (SELECT DISTINCT event_id FROM odds WHERE market='spreads' AND line IS NOT NULL)"""
    rows = c.execute(q, (sport,)).fetchall()
    n = len(rows)
    if n == 0:
        print(f'  No data.')
        continue

    # Build pick candidates
    candidates = []
    for (home, away, commence, hs, as_, home_sp, home_odds, away_odds, h_pr, a_pr) in rows:
        if home_sp is None or hs is None or as_ is None:
            continue
        if h_pr is None or a_pr is None:
            continue
        # Model spread: home_rating - away_rating flipped to match home_sp convention
        # (negative home_sp = home favored)
        rating_diff = h_pr - a_pr
        model_spread = -rating_diff  # home_sp convention
        # Edge = how much better home cover than market implies
        prob_home = spread_to_win_prob(model_spread, home_sp)
        prob_away = 1 - prob_home
        # If model_spread < home_sp → home more favored → fire HOME
        # Else → fire AWAY
        if model_spread < home_sp:
            pick_side = 'HOME'
            pick_odds = home_odds if home_odds else -110
            pick_prob = prob_home
        else:
            pick_side = 'AWAY'
            pick_odds = away_odds if away_odds else -110
            pick_prob = prob_away
        edge = compute_edge(pick_prob, pick_odds)
        if edge is None:
            continue
        # Resolve cover
        net = (hs + home_sp) - as_
        if abs(net) < 0.001:
            cover = 'PUSH'
        elif net > 0:
            cover = 'HOME'
        else:
            cover = 'AWAY'
        # P/L
        if cover == 'PUSH':
            pnl = 0
        elif cover == pick_side:
            pnl = (100 / abs(pick_odds)) if pick_odds < 0 else (pick_odds / 100)
        else:
            pnl = -1
        candidates.append({'edge': edge, 'pnl': pnl, 'pick_side': pick_side,
                            'home_sp': home_sp, 'cover': cover})

    print(f'  Total graded games with data: {len(candidates)}')
    if not candidates:
        continue

    # Sweep thresholds
    print(f'  {"threshold":<12} {"N fired":>8} {"W-L":>7} {"WR":>6} {"P/L":>9} {"EV/pick":>8}')
    best = None
    for th in [5, 8, 10, 12, 15, 18, 20, 25]:
        sub = [d for d in candidates if d['edge'] >= th]
        if len(sub) < 3:
            continue
        w = sum(1 for d in sub if d['pnl'] > 0)
        l = sum(1 for d in sub if d['pnl'] < 0)
        pnl = sum(d['pnl'] for d in sub)
        wr = w / (w + l) * 100 if (w + l) else 0
        ev = pnl / len(sub)
        mark = '🔥' if wr >= 54 and len(sub) >= 10 else ''
        print(f'  >= {th:>2}%        {len(sub):>8} {w}-{l:<3} {wr:>5.1f}% {pnl:>+8.2f}u {ev:>+7.3f}u {mark}')
        if best is None or pnl > best['pnl']:
            best = {'th': th, 'n': len(sub), 'w': w, 'l': l, 'pnl': pnl, 'wr': wr, 'ev': ev}
    if best:
        print(f'  >>> BEST: threshold={best["th"]}%  n={best["n"]}  {best["w"]}-{best["l"]}  WR={best["wr"]:.1f}%  P/L={best["pnl"]:+.2f}u  EV/pick={best["ev"]:+.3f}u')

conn.close()
