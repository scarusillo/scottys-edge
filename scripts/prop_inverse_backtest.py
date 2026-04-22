"""Prop model inverse backtest — FOLLOW vs FADE across sport/stat/edge buckets.

Unlike Context Model backtest (which simulated "what if we'd fired"), props
already have real live data. For each graded prop, we compute:
  follow_pnl = actual pnl_units
  fade_pnl   = approximate inverse at -110 juice on the other side
                 (if actual WIN: fade = -1u; if actual LOSS: fade = +0.91u)

Slices inspected:
  H1: Fade per sport × direction
  H2: Fade per stat
  H3: Fade per side_type channel (PROP_OVER, PROP_FADE_FLIP, etc.)
  H4: Fade per edge bucket
  H5: Fade per sport × stat × direction (surgical)
"""
import sqlite3
import os
import re

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()

STAT_PATTERNS = [
    ('SOG', r'SOG|SHOTS ON GOAL'),
    ('PTS', r'\bpts\b|POINTS|\bPts\b'),
    ('AST', r'ASSIST|\bAST\b|\bAssists\b'),
    ('REB', r'\bREB\b|rebound|REBOUND'),
    ('HITS', r'\bhits\b|\bHITS\b'),
    ('HR', r'\bHR\b|HOME RUN'),
    ('RBI', r'\bRBI\b'),
    ('RUNS', r'\bruns\b|\bRUNS\b'),
    ('K', r'STRIKEOUT|\bKs\b|\bks\b'),
    ('BLK', r'BLOCK|\bBLKS\b'),
    ('OUTS', r'pitcher_outs|PITCHER_OUTS|OUTS RECORDED'),
    ('HA', r'HITS ALLOWED|HA'),
    ('3PT', r'THREE|3-PT|\bTHREES\b'),
    ('TD', r'\bTD\b|TOUCHDOWN'),
]


def classify_stat(selection):
    s = selection or ''
    for label, pattern in STAT_PATTERNS:
        if re.search(pattern, s, re.IGNORECASE):
            return label
    return 'OTHER'


def direction(selection):
    s = (selection or '').upper()
    if 'OVER' in s:
        return 'OVER'
    if 'UNDER' in s:
        return 'UNDER'
    return '?'


rows = c.execute("""
    SELECT sport, selection, side_type, edge_pct, odds, result, pnl_units, clv
    FROM graded_bets
    WHERE market_type='PROP' AND units >= 3.5
      AND result IN ('WIN', 'LOSS', 'PUSH')
      AND created_at >= '2026-03-04'
""").fetchall()
print(f'Prop picks: {len(rows)}')

dataset = []
for sport, selection, side_type, edge_pct, odds, result, pnl_units, clv in rows:
    stat = classify_stat(selection)
    dir_ = direction(selection)
    # Approximate fade pnl at -110 juice on the fade side.
    # If picked result WIN: fade would have LOST 1 unit (we'd be on losing side)
    # If picked result LOSS: fade would have WON at -110 (assuming 5u stake: 5 × 100/110 = 4.55u, divide by 5 for per-unit)
    # Normalize to per-unit pnl (matches graded_bets.pnl_units which is pre-5u stake * 1u units_each)
    # Actually pnl_units IS already in units. WIN at -110 = +0.91, LOSS = -1.
    if result == 'PUSH':
        fade_pnl = 0
    elif result == 'WIN':
        fade_pnl = -5.0  # bet 5u, lost 5u
    else:  # LOSS
        fade_pnl = 5.0 * (100 / 110)  # ~4.55u
    dataset.append({
        'sport': sport, 'selection': selection, 'stat': stat, 'direction': dir_,
        'side_type': side_type or '(null)', 'edge_pct': edge_pct or 0, 'odds': odds,
        'result': result, 'follow_pnl': pnl_units, 'fade_pnl': fade_pnl, 'clv': clv or 0,
    })


def slice_summary(subset, label):
    if not subset:
        return
    n = len(subset)
    f_w = sum(1 for d in subset if d['follow_pnl'] > 0)
    f_l = sum(1 for d in subset if d['follow_pnl'] < 0)
    f_ev = sum(d['follow_pnl'] for d in subset)
    fd_w = sum(1 for d in subset if d['fade_pnl'] > 0)
    fd_l = sum(1 for d in subset if d['fade_pnl'] < 0)
    fd_ev = sum(d['fade_pnl'] for d in subset)
    winner = 'FADE' if fd_ev > f_ev else ('FOLLOW' if f_ev > fd_ev else 'TIE')
    f_wr = f_w / (f_w + f_l) * 100 if (f_w + f_l) else 0
    print(f'  {label:<35} n={n:<3} FOLLOW {f_w}-{f_l} WR={f_wr:.1f}% EV={f_ev:+.2f}u | '
          f'FADE EV={fd_ev:+.2f}u  {winner}')


print()
print('=== H1: FOLLOW vs FADE per sport × direction ===')
for sport in sorted(set(d['sport'] for d in dataset)):
    for dir_ in ('OVER', 'UNDER'):
        sub = [d for d in dataset if d['sport'] == sport and d['direction'] == dir_]
        if len(sub) < 3:
            continue
        slice_summary(sub, f'{sport} {dir_}')

print()
print('=== H2: per stat ===')
for stat in ['PTS', 'SOG', 'REB', 'AST', 'K', 'HITS', 'HR', 'RBI', 'RUNS', 'BLK', 'OUTS', 'HA', '3PT', 'OTHER']:
    sub = [d for d in dataset if d['stat'] == stat]
    if len(sub) < 3:
        continue
    slice_summary(sub, f'{stat}')

print()
print('=== H3: per side_type channel ===')
for st in sorted(set(d['side_type'] for d in dataset)):
    sub = [d for d in dataset if d['side_type'] == st]
    if len(sub) < 3:
        continue
    slice_summary(sub, st)

print()
print('=== H4: per edge bucket ===')
for lo, hi in [(5, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 40), (40, 999)]:
    sub = [d for d in dataset if lo <= d['edge_pct'] < hi]
    if len(sub) < 3:
        continue
    slice_summary(sub, f'edge {lo}-{hi}%')

print()
print('=== H5: per sport × stat × direction (min n=3) ===')
cells = {}
for d in dataset:
    key = (d['sport'], d['stat'], d['direction'])
    cells.setdefault(key, []).append(d)
for key, sub in sorted(cells.items(), key=lambda x: sum(d['follow_pnl'] for d in x[1])):
    if len(sub) < 3:
        continue
    slice_summary(sub, f'{key[0]} {key[1]} {key[2]}')

print()
print('=== H6: OVER direction by edge bucket (testing "market shades OVER" hypothesis) ===')
overs = [d for d in dataset if d['direction'] == 'OVER']
for lo, hi in [(5, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 999)]:
    sub = [d for d in overs if lo <= d['edge_pct'] < hi]
    if len(sub) < 3:
        continue
    slice_summary(sub, f'OVER edge {lo}-{hi}%')

print()
print('=== H7: By PROP_OVER + plus-odds vs minus-odds ===')
for dir_ in ('OVER', 'UNDER'):
    for label, sel in [('plus-odds (+100 or higher)', lambda d: d['odds'] > 0),
                        ('minus-odds (-110 to -150)', lambda d: -150 <= d['odds'] <= -110),
                        ('minus-odds tighter (-110)', lambda d: d['odds'] == -110)]:
        sub = [d for d in dataset if d['direction'] == dir_ and sel(d)]
        if len(sub) < 3:
            continue
        slice_summary(sub, f'{dir_} {label}')

conn.close()
