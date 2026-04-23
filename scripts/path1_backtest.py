"""ELO_DIVERGENCE_RESCUE backtest — measure SPREAD_FADE_FLIP + Context veto efficacy.

For each historical game where Elo diverged from market significantly:
  1. Would Context have AGREED with Elo or with market?
  2. If Context agreed with MARKET (Case A): fade flip fires — did it win?
     Also: would the vetoed edge pick have lost (proving veto saved us)?
  3. If Context agreed with ELO (Case B): edge pick fires — did it win?

This lets us answer: "Is ELO_DIVERGENCE_RESCUE (Context as safety net) actually saving us
money, or is it adding noise?"
"""
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from context_spread_model import compute_context_spread

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()

SPORTS = ['basketball_nba', 'icehockey_nhl', 'baseball_mlb',
          'soccer_italy_serie_a', 'soccer_usa_mls']
# Max_div threshold per sport — from model_engine's SPORT_CONFIG.
# Above this, ELO_DIVERGENCE_RESCUE logic kicks in.
MAX_DIV = {'basketball_nba': 4.0, 'icehockey_nhl': 1.5, 'baseball_mlb': 1.5,
           'soccer_italy_serie_a': 0.6, 'soccer_usa_mls': 0.6}

q = """SELECT r.sport, r.home, r.away, r.event_id, r.commence_time,
              r.home_score, r.away_score,
              (SELECT AVG(line) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.home || '%' AND line IS NOT NULL) as home_spread,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.home || '%') as home_odds,
              (SELECT AVG(odds) FROM odds WHERE event_id = r.event_id AND market='spreads' AND selection LIKE '%' || r.away || '%') as away_odds,
              (SELECT final_rating FROM power_ratings WHERE team = r.home AND sport = r.sport ORDER BY run_timestamp DESC LIMIT 1) as home_pr,
              (SELECT final_rating FROM power_ratings WHERE team = r.away AND sport = r.sport ORDER BY run_timestamp DESC LIMIT 1) as away_pr
       FROM results r
       WHERE r.sport IN ({}) AND r.completed=1
         AND r.home_score IS NOT NULL AND r.away_score IS NOT NULL
         AND r.commence_time >= date('now','-90 days')
         AND r.event_id IN (SELECT DISTINCT event_id FROM odds WHERE market='spreads' AND line IS NOT NULL)""".format(
    ','.join('?' * len(SPORTS)))

rows = c.execute(q, SPORTS).fetchall()
print(f'Candidate games: {len(rows)}')

case_a = []  # Context agrees with market (fade flip fires)
case_b = []  # Context agrees with Elo (edge pick fires)
neutral = []  # not divergent enough to trigger path 1

for sport, home, away, eid, commence, hs, as_, home_spread, home_odds, away_odds, home_pr, away_pr in rows:
    if home_spread is None or hs is None or as_ is None:
        continue
    if home_pr is None or away_pr is None:
        continue  # Can't compute Elo spread
    # Elo spread = home rating - away rating (in points, via simple diff).
    # This is rough but mirrors what bootstrap gives model_engine.
    ms_elo = -(home_pr - away_pr)  # home_spread convention: negative = home favored
    # Context computed from Elo as baseline
    try:
        ms_ctx, info = compute_context_spread(
            conn, sport, home, away, eid, ms_elo, commence[:10] if commence else None)
    except Exception:
        continue

    elo_div = abs(ms_elo - home_spread)
    ctx_vs_market = abs(ms_ctx - home_spread)
    ctx_vs_elo = abs(ms_ctx - ms_elo)

    max_div = MAX_DIV.get(sport, 1.5)
    if elo_div < max_div:
        continue  # Not a ELO_DIVERGENCE_RESCUE trigger scenario

    # Actual cover resolution
    net = (hs + home_spread) - as_
    if abs(net) < 0.001:
        actual_cover = 'PUSH'
    elif net > 0:
        actual_cover = 'HOME'
    else:
        actual_cover = 'AWAY'

    # Elo would have said bet which side?
    # ms_elo < home_spread → Elo thinks home is MORE favored → bet HOME
    elo_pick = 'HOME' if ms_elo < home_spread else 'AWAY'
    # Fade flip bets OPPOSITE of elo
    fade_flip_pick = 'AWAY' if elo_pick == 'HOME' else 'HOME'

    # Does Context agree with market or with Elo?
    # Context agrees with MARKET if ctx_vs_market < ctx_vs_elo
    ctx_agrees_market = ctx_vs_market < ctx_vs_elo

    # Pick the odds for whichever side fires
    def pnl(side):
        odds = home_odds if side == 'HOME' else away_odds
        if odds is None or odds == 0:
            odds = -110
        if actual_cover == 'PUSH':
            return 0
        if actual_cover == side:
            return (100 / abs(odds)) if odds < 0 else (odds / 100)
        return -1

    record = {
        'sport': sport,
        'home': home, 'away': away,
        'home_spread': home_spread,
        'ms_elo': ms_elo,
        'ms_ctx': ms_ctx,
        'elo_div': elo_div,
        'ctx_vs_mkt': ctx_vs_market,
        'ctx_vs_elo': ctx_vs_elo,
        'elo_pick': elo_pick,
        'fade_pick': fade_flip_pick,
        'actual_cover': actual_cover,
        'ctx_agrees_market': ctx_agrees_market,
        'elo_pnl': pnl(elo_pick),
        'fade_pnl': pnl(fade_flip_pick),
    }
    if ctx_agrees_market:
        case_a.append(record)
    else:
        case_b.append(record)

print(f'Case A (Context agrees with MARKET — fade flip fires): n={len(case_a)}')
print(f'Case B (Context agrees with ELO — edge pick fires): n={len(case_b)}')
print()


def summarize(label, subset, pnl_key='elo_pnl'):
    if not subset: return
    n = len(subset)
    w = sum(1 for d in subset if d[pnl_key] > 0)
    l = sum(1 for d in subset if d[pnl_key] < 0)
    ev = sum(d[pnl_key] for d in subset)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f'  {label:<50} n={n:<3} {w}W-{l}L WR={wr:.1f}% EV={ev:+.2f}u')


print('=== CASE A: Context agreed with MARKET (Elo appeared wrong) ===')
print('What happens if we FIRE FADE_FLIP (bet opposite of Elo)?')
summarize('Case A — FADE FLIP record', case_a, 'fade_pnl')
print('What happens if we had fired the EDGE PICK instead (Elo side)?')
summarize('Case A — EDGE PICK (hypothetical, ELO_DIVERGENCE_RESCUE prevents this)', case_a, 'elo_pnl')
delta_a = sum(d['fade_pnl'] for d in case_a) - sum(d['elo_pnl'] for d in case_a)
print(f'  ELO_DIVERGENCE_RESCUE value added in Case A: fade_flip vs edge_pick = {delta_a:+.2f}u over {len(case_a)} games')

print()
print('=== CASE B: Context agreed with ELO (both signals vs market) ===')
print('What happens if we FIRE EDGE PICK (Elo side)?')
summarize('Case B — EDGE PICK (both models agree)', case_b, 'elo_pnl')
print('What happens if we had fired FADE FLIP instead (the opposite)?')
summarize('Case B — FADE FLIP (hypothetical, v25.60 vetoes this)', case_b, 'fade_pnl')
delta_b = sum(d['elo_pnl'] for d in case_b) - sum(d['fade_pnl'] for d in case_b)
print(f'  ELO_DIVERGENCE_RESCUE (v25.60) value in Case B: edge_pick vs fade_flip = {delta_b:+.2f}u over {len(case_b)} games')

print()
print('=== COMBINED ELO_DIVERGENCE_RESCUE net value ===')
# Compare "ELO_DIVERGENCE_RESCUE logic" (fire fade in A, edge in B) vs "no ELO_DIVERGENCE_RESCUE" (always edge pick)
path1_total = sum(d['fade_pnl'] for d in case_a) + sum(d['elo_pnl'] for d in case_b)
no_path1_total = sum(d['elo_pnl'] for d in case_a) + sum(d['elo_pnl'] for d in case_b)
print(f'  ELO_DIVERGENCE_RESCUE logic total P/L: {path1_total:+.2f}u')
print(f'  No ELO_DIVERGENCE_RESCUE (always edge pick): {no_path1_total:+.2f}u')
print(f'  ELO_DIVERGENCE_RESCUE value added: {path1_total - no_path1_total:+.2f}u')

# Also: what if we ALWAYS faded Elo regardless of Context
all_fade = sum(d['fade_pnl'] for d in case_a) + sum(d['fade_pnl'] for d in case_b)
print(f'  Always fade Elo (no Context check): {all_fade:+.2f}u')

print()
print('=== Per sport breakdown ===')
for sport in SPORTS:
    a_sub = [d for d in case_a if d['sport'] == sport]
    b_sub = [d for d in case_b if d['sport'] == sport]
    if not (a_sub or b_sub): continue
    a_ev = sum(d['fade_pnl'] for d in a_sub)
    b_ev = sum(d['elo_pnl'] for d in b_sub)
    print(f'  {sport:<30} Case A n={len(a_sub):<3} fade_ev={a_ev:+.2f}u  |  Case B n={len(b_sub):<3} edge_ev={b_ev:+.2f}u')

conn.close()
