"""Compare old vs new team ratings on all historical total bets."""
import sqlite3, re
from collections import defaultdict
from model_engine import _weighted_team_stats
from team_ratings_engine import get_team_ratings
from elo_engine import get_elo_ratings

DB = '../data/betting_model.db'
conn = sqlite3.connect(DB)

bets = conn.execute("""
    SELECT gb.id, gb.selection, gb.sport, gb.side_type,
           gb.odds, gb.units, gb.result, gb.pnl_units
    FROM graded_bets gb
    WHERE gb.result IN ('WIN','LOSS') AND gb.units >= 3.5
    AND gb.market_type = 'TOTAL'
    AND DATE(gb.created_at) >= '2026-03-04'
    ORDER BY gb.created_at
""").fetchall()

print(f"Total TOTAL bets to analyze: {len(bets)}\n")

tr_cache = {}
elo_cache = {}
league_cache = {}

def get_tr(sport):
    if sport not in tr_cache:
        tr_cache[sport] = get_team_ratings(conn, sport) or {}
    return tr_cache[sport]

def get_elo(sport):
    if sport not in elo_cache:
        elo_cache[sport] = get_elo_ratings(conn, sport) or {}
    return elo_cache[sport]

def get_league_avg(sport):
    if sport not in league_cache:
        r = conn.execute("""
            SELECT AVG(actual_total) FROM results
            WHERE sport=? AND completed=1 AND actual_total IS NOT NULL
        """, (sport,)).fetchone()
        league_cache[sport] = r[0] if r and r[0] else None
    return league_cache[sport]

sport_results = defaultdict(lambda: {
    'old_w': 0, 'old_l': 0, 'old_pnl': 0.0,
    'new_w': 0, 'new_l': 0, 'new_pnl': 0.0,
    'changed': 0, 'total': 0
})

changed_picks = []
skipped = 0

for bet in bets:
    gid, sel, sport, side, odds, units, result, pnl = bet

    # Skip soccer — uses different path in estimate_model_total
    if 'soccer' in sport:
        continue

    m = re.match(r'^(.+?)@(.+?)\s+(OVER|UNDER)\s+([\d.]+)', sel)
    if not m:
        skipped += 1
        continue

    away_team = m.group(1).strip()
    home_team = m.group(2).strip()
    direction = m.group(3)
    line = float(m.group(4))

    league_avg = get_league_avg(sport)
    if not league_avg:
        skipped += 1
        continue
    league_per_team = league_avg / 2

    # OLD method: _weighted_team_stats
    elo_data = get_elo(sport)
    h_stats = _weighted_team_stats(conn, home_team, sport, elo_ratings=elo_data)
    a_stats = _weighted_team_stats(conn, away_team, sport, elo_ratings=elo_data)

    if not h_stats or not a_stats:
        skipped += 1
        continue

    h_off_old = h_stats['home_offense'] * h_stats['elo_adj']
    h_def_old = h_stats['home_defense'] / h_stats['elo_adj']
    a_off_old = a_stats['away_offense'] * a_stats['elo_adj']
    a_def_old = a_stats['away_defense'] / a_stats['elo_adj']

    h_atk_old = h_off_old / league_per_team
    a_atk_old = a_off_old / league_per_team
    h_def_r_old = h_def_old / league_per_team
    a_def_r_old = a_def_old / league_per_team

    exp_h_old = league_per_team * h_atk_old * a_def_r_old
    exp_a_old = league_per_team * a_atk_old * h_def_r_old
    old_indep = exp_h_old + exp_a_old

    blend = 0.60 if 'basketball' in sport else 0.55
    old_model = old_indep * blend + league_avg * (1 - blend)

    # NEW method: precomputed team_ratings
    tr = get_tr(sport)
    if home_team in tr and away_team in tr:
        h_r = tr[home_team]
        a_r = tr[away_team]
        exp_h_new = league_per_team * h_r['home_off'] * a_r['away_def']
        exp_a_new = league_per_team * a_r['away_off'] * h_r['home_def']
        new_indep = exp_h_new + exp_a_new
        new_model = new_indep * blend + league_avg * (1 - blend)
    else:
        new_model = old_model

    # Would the direction call change?
    old_over = old_model > line
    new_over = new_model > line
    old_agrees = (direction == 'OVER' and old_over) or (direction == 'UNDER' and not old_over)
    new_agrees = (direction == 'OVER' and new_over) or (direction == 'UNDER' and not new_over)

    sr = sport_results[sport]
    sr['total'] += 1

    # Record old results (always the actual)
    if result == 'WIN':
        sr['old_w'] += 1
    else:
        sr['old_l'] += 1
    sr['old_pnl'] += pnl

    if old_agrees != new_agrees:
        sr['changed'] += 1
        if not new_agrees:
            # New model would NOT have taken this pick
            changed_picks.append({
                'sel': sel, 'sport': sport, 'result': result, 'pnl': pnl,
                'old_total': round(old_model, 1), 'new_total': round(new_model, 1),
                'line': line, 'direction': direction
            })
        else:
            # New model agrees — same result
            if result == 'WIN':
                sr['new_w'] += 1
            else:
                sr['new_l'] += 1
            sr['new_pnl'] += pnl
    else:
        # Same decision
        if result == 'WIN':
            sr['new_w'] += 1
        else:
            sr['new_l'] += 1
        sr['new_pnl'] += pnl

# Print results
print(f"{'Sport':<25} {'Old W-L':<12} {'Old P/L':<10} {'New W-L':<12} {'New P/L':<10} {'Flipped':<8}")
print("-" * 80)

tot = {'ow': 0, 'ol': 0, 'op': 0.0, 'nw': 0, 'nl': 0, 'np': 0.0, 'ch': 0}

for sport in sorted(sport_results.keys()):
    sr = sport_results[sport]
    label = sport.replace('basketball_', '').replace('icehockey_', '').replace('baseball_', '').upper()
    ow, ol, op = sr['old_w'], sr['old_l'], sr['old_pnl']
    nw, nl, np_ = sr['new_w'], sr['new_l'], sr['new_pnl']
    print(f"{label:<25} {ow}W-{ol}L{'':<5} {op:>+7.1f}u   {nw}W-{nl}L{'':<5} {np_:>+7.1f}u   {sr['changed']}")
    tot['ow'] += ow; tot['ol'] += ol; tot['op'] += op
    tot['nw'] += nw; tot['nl'] += nl; tot['np'] += np_; tot['ch'] += sr['changed']

print("-" * 80)
print(f"{'TOTAL':<25} {tot['ow']}W-{tot['ol']}L{'':<5} {tot['op']:>+7.1f}u   {tot['nw']}W-{tot['nl']}L{'':<5} {tot['np']:>+7.1f}u   {tot['ch']}")
print(f"\nSkipped: {skipped} (missing data/soccer)")

if changed_picks:
    blocked_pnl = sum(c['pnl'] for c in changed_picks)
    blocked_w = sum(1 for c in changed_picks if c['result'] == 'WIN')
    blocked_l = sum(1 for c in changed_picks if c['result'] == 'LOSS')
    print(f"\n=== PICKS THAT WOULD HAVE BEEN BLOCKED ({len(changed_picks)}) ===")
    print(f"Blocked: {blocked_w}W-{blocked_l}L, {blocked_pnl:+.1f}u")
    print(f"Impact: {'POSITIVE' if blocked_pnl < 0 else 'NEGATIVE'} (blocking losers = good)")
    print()
    for c in changed_picks:
        emoji = "avoided" if c['result'] == 'LOSS' else "missed"
        print(f"  {c['result']:4} {c['pnl']:+5.1f}u ({emoji}) | old={c['old_total']} new={c['new_total']} line={c['line']} {c['direction']} | {c['sel'][:65]}")

conn.close()
