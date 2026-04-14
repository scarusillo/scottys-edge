"""Backtest: How would starter-adjusted team_ratings change MLB/NHL total picks?"""
import sqlite3, re
from collections import defaultdict
from team_ratings_engine import get_team_ratings
from starter_adjust import get_starter_adjustment

DB = '../data/betting_model.db'
conn = sqlite3.connect(DB)

bets = conn.execute("""
    SELECT gb.id, gb.selection, gb.sport, gb.market_type, gb.side_type,
           gb.odds, gb.units, gb.result, gb.pnl_units, gb.line, gb.event_id,
           DATE(gb.created_at) as dt
    FROM graded_bets gb
    WHERE gb.result IN ('WIN','LOSS') AND gb.units >= 3.5
    AND gb.market_type = 'TOTAL'
    AND gb.sport IN ('baseball_mlb', 'icehockey_nhl')
    AND DATE(gb.created_at) >= '2026-03-04'
    ORDER BY gb.created_at
""").fetchall()

print(f"MLB + NHL total bets: {len(bets)}\n")

results = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0, 'count': 0})
flipped = []

for bet in bets:
    gid, sel, sport, mtype, side, odds, units, result, pnl, line, event_id, dt = bet

    m = re.match(r'^(.+?)@(.+?)\s+(OVER|UNDER)\s+([\d.]+)', sel)
    if not m: continue
    away = m.group(1).strip()
    home = m.group(2).strip()
    direction = m.group(3)
    line = float(m.group(4))

    # League avg
    lr = conn.execute(
        'SELECT AVG(actual_total) FROM results WHERE sport=? AND completed=1',
        (sport,)).fetchone()
    la = lr[0] if lr and lr[0] else None
    if not la: continue
    lpt = la / 2

    tr = get_team_ratings(conn, sport)
    if home not in tr or away not in tr: continue

    h_r = tr[home]
    a_r = tr[away]

    # BASE model (no starter adjustment)
    exp_h = lpt * h_r['home_off'] * a_r['away_def']
    exp_a = lpt * a_r['away_off'] * h_r['home_def']
    base_total = (exp_h + exp_a) * 0.55 + la * 0.45

    # ADJUSTED model (with starter multipliers)
    h_mult, a_mult, info = get_starter_adjustment(conn, sport, home, away,
                                                   event_id=event_id, game_date=dt)

    # Apply mults: home_def is how much home allows (affected by home's pitcher/goalie)
    # away_def is how much away allows (affected by away's pitcher/goalie)
    h_def_adj = h_r['home_def'] * h_mult
    a_def_adj = a_r['away_def'] * a_mult

    exp_h_adj = lpt * h_r['home_off'] * a_def_adj
    exp_a_adj = lpt * a_r['away_off'] * h_def_adj
    adj_total = (exp_h_adj + exp_a_adj) * 0.55 + la * 0.45

    # Compare to market line
    base_over = base_total > line
    adj_over = adj_total > line
    base_agrees = (direction == 'OVER' and base_over) or (direction == 'UNDER' and not base_over)
    adj_agrees = (direction == 'OVER' and adj_over) or (direction == 'UNDER' and not adj_over)

    key = f'{sport}_base'
    results[key]['count'] += 1
    if result == 'WIN':
        results[key]['w'] += 1
    else:
        results[key]['l'] += 1
    results[key]['pnl'] += pnl

    key_adj = f'{sport}_adj'
    results[key_adj]['count'] += 1
    if base_agrees != adj_agrees:
        # Direction changed
        if adj_agrees:
            # Adjusted model still takes this pick — same result
            if result == 'WIN':
                results[key_adj]['w'] += 1
            else:
                results[key_adj]['l'] += 1
            results[key_adj]['pnl'] += pnl
        else:
            # Adjusted model would NOT take this pick
            flipped.append({
                'sel': sel, 'sport': sport, 'result': result, 'pnl': pnl,
                'base': round(base_total, 1), 'adj': round(adj_total, 1),
                'line': line, 'dir': direction, 'info': info
            })
    else:
        if result == 'WIN':
            results[key_adj]['w'] += 1
        else:
            results[key_adj]['l'] += 1
        results[key_adj]['pnl'] += pnl

# Print comparison
print("=" * 80)
print(f"{'Sport':<15} {'Model':<10} {'W-L':<12} {'Win%':<8} {'P/L':<10}")
print("=" * 80)

for sport in ['baseball_mlb', 'icehockey_nhl']:
    base = results[f'{sport}_base']
    adj = results[f'{sport}_adj']
    label = sport.replace('baseball_', '').replace('icehockey_', '').upper()
    bwr = (base['w'] / (base['w'] + base['l']) * 100) if (base['w'] + base['l']) else 0
    awr = (adj['w'] / (adj['w'] + adj['l']) * 100) if (adj['w'] + adj['l']) else 0
    print(f"{label:<15} {'Base':<10} {base['w']}W-{base['l']}L{'':<5} {bwr:<6.1f}% {base['pnl']:>+7.1f}u")
    print(f"{label:<15} {'Adjusted':<10} {adj['w']}W-{adj['l']}L{'':<5} {awr:<6.1f}% {adj['pnl']:>+7.1f}u")
    delta = adj['pnl'] - base['pnl']
    print(f"{label:<15} {'Delta':<10} {'':<12} {'':<8} {delta:>+7.1f}u")
    print()

print(f"\n=== FLIPPED PICKS (adjusted model would block) ===")
print(f"Total flipped: {len(flipped)}")
if flipped:
    flipped_w = sum(1 for f in flipped if f['result'] == 'WIN')
    flipped_l = sum(1 for f in flipped if f['result'] == 'LOSS')
    flipped_pnl = sum(f['pnl'] for f in flipped)
    print(f"Would block: {flipped_w}W-{flipped_l}L, {flipped_pnl:+.1f}u")
    print(f"Impact of blocking: {-flipped_pnl:+.1f}u (positive = avoided losses)\n")
    for f in flipped[:15]:
        hp = f['info'].get('home_starter', '?')
        ap = f['info'].get('away_starter', '?')
        h_era = f['info'].get('home_era')
        a_era = f['info'].get('away_era')
        h_era_s = f"{h_era:.2f}" if h_era else '?'
        a_era_s = f"{a_era:.2f}" if a_era else '?'
        print(f"  {f['result']:4} {f['pnl']:+5.1f}u | base={f['base']} adj={f['adj']} line={f['line']} {f['dir']}")
        print(f"         {f['sel'][:55]}")
        print(f"         H: {hp} ({h_era_s}) | A: {ap} ({a_era_s})")

conn.close()
