"""FADE backtest — bet against model on high-divergence tennis picks (post-v25.81)."""
import sqlite3, re, json
from collections import defaultdict

conn = sqlite3.connect('data/betting_model.db')
c = conn.cursor()

parse = re.compile(r'div=([\-\d\.]+),\s*ms=([\+\-\d\.]+),\s*mkt_sp=([\+\-\d\.]+)')

rows = c.execute("""
    SELECT DATE(MIN(created_at)) d, sport, event_id, selection, MIN(reason) reason
    FROM shadow_blocked_picks
    WHERE sport LIKE 'tennis%'
      AND DATE(created_at) BETWEEN '2026-04-23' AND '2026-04-24'
      AND (reason LIKE '%insufficient_elo%' OR reason LIKE '%post_elo%')
    GROUP BY sport, event_id
""").fetchall()

def get_mls(event_id):
    r = c.execute("SELECT home, away, best_home_ml, best_away_ml FROM market_consensus WHERE event_id=? LIMIT 1", (event_id,)).fetchone()
    if r: return {'home': r[0], 'away': r[1], 'home_ml': r[2], 'away_ml': r[3]}
    return None

def matches(p, n):
    if not p or not n: return False
    pl = p.lower(); nl = n.lower()
    return pl in nl or nl in pl or any(t in nl for t in pl.split() if len(t) >= 4)

def grade_match(sport, event_id, p1, p2, date):
    res = c.execute("SELECT event_id, home, away, winner FROM results WHERE event_id=? AND sport=? AND completed=1", (event_id, sport)).fetchone()
    if not res:
        res = c.execute("SELECT event_id, home, away, winner FROM results WHERE sport=? AND completed=1 AND DATE(commence_time)=? AND ((home=? AND away=?) OR (home=? AND away=?)) LIMIT 1", (sport, date, p1, p2, p2, p1)).fetchone()
    if not res: return None
    espn_eid, home, away, winner = res
    tm = c.execute("SELECT set_scores FROM tennis_metadata WHERE event_id=?", (espn_eid,)).fetchone()
    if not tm or not tm[0]: return None
    sets = json.loads(tm[0])
    h_games = sum(s[0] for s in sets); a_games = sum(s[1] for s in sets)
    return {'home': home, 'away': away, 'winner': winner, 'h_games': h_games, 'a_games': a_games}

def pl_at(odds, won, stake=1.0):
    if won:
        return stake * (odds/100 if odds > 0 else 100/abs(odds))
    return -stake

universe = []
for d, sport, event_id, sel, reason in rows:
    m = parse.search(reason)
    if not m: continue
    if ' vs ' not in sel: continue
    p1, p2 = [s.strip() for s in sel.split(' vs ', 1)]
    div = float(m.group(1)); ms = float(m.group(2)); mkt_sp = float(m.group(3))
    if ms < mkt_sp: follow_player, follow_line = p1, mkt_sp
    elif ms > mkt_sp: follow_player, follow_line = p2, -mkt_sp
    else: continue
    fade_player = p2 if follow_player == p1 else p1
    fade_line = -follow_line  # opposite spread

    g = grade_match(sport, event_id, p1, p2, d)
    if not g: continue

    # Compute FOLLOW spread result
    if matches(follow_player, g['home']): follow_margin = g['h_games'] - g['a_games']
    elif matches(follow_player, g['away']): follow_margin = g['a_games'] - g['h_games']
    else: continue
    follow_spread_adj = follow_margin + follow_line
    follow_spread_won = follow_spread_adj > 0
    follow_spread_push = follow_spread_adj == 0
    follow_ml_won = matches(follow_player, g['winner']) if g['winner'] else False

    # FADE = mirror
    fade_spread_won = (not follow_spread_won) and (not follow_spread_push)
    fade_spread_push = follow_spread_push
    fade_ml_won = not follow_ml_won

    # Get MLs
    mls = get_mls(event_id)
    follow_ml = mls['home_ml'] if (mls and matches(follow_player, mls['home'])) else (mls['away_ml'] if mls else None)
    fade_ml = mls['home_ml'] if (mls and matches(fade_player, mls['home'])) else (mls['away_ml'] if mls else None)

    universe.append({
        'date': d, 'sport': sport, 'p1': p1, 'p2': p2, 'div': div,
        'reason': 'insufficient_elo' if 'insufficient_elo' in reason else 'post_elo',
        'follow_player': follow_player, 'fade_player': fade_player,
        'follow_line': follow_line, 'fade_line': fade_line,
        'follow_ml': follow_ml, 'fade_ml': fade_ml,
        'follow_spread_won': follow_spread_won, 'follow_spread_push': follow_spread_push,
        'fade_spread_won': fade_spread_won, 'fade_spread_push': fade_spread_push,
        'follow_ml_won': follow_ml_won, 'fade_ml_won': fade_ml_won,
    })

print(f'Total post-overhaul observations: {len(universe)}\n')

# === FADE vs FOLLOW by divergence bucket ===
print('=== FADE vs FOLLOW BY DIVERGENCE BUCKET (post-v25.81 model) ===\n')

buckets = [('0-2.5', 0, 2.5), ('2.5-3.0', 2.5, 3.0), ('3.0-3.5', 3.0, 3.5),
           ('3.5-4.0', 3.5, 4.0), ('4.0-5.0', 4.0, 5.0), ('5.0+', 5.0, 99)]

print('-- SPREAD -110 (always in odds scope) --')
print(f'{"Bucket":<10} | {"n":>3} | {"FOLLOW W-L":<10} | {"FOLLOW P/L":>10} | {"FADE W-L":<10} | {"FADE P/L":>10} | {"FADE - FOLLOW":>15}')
print('-' * 100)
for label, lo, hi in buckets:
    sub = [r for r in universe if lo <= r['div'] < hi]
    if not sub:
        print(f'{label:<10} | (none)')
        continue
    fw = sum(1 for r in sub if r['follow_spread_won'])
    fl = sum(1 for r in sub if not r['follow_spread_won'] and not r['follow_spread_push'])
    f_pl = fw * 0.91 - fl * 1.0
    aw = sum(1 for r in sub if r['fade_spread_won'])
    al = sum(1 for r in sub if not r['fade_spread_won'] and not r['fade_spread_push'])
    a_pl = aw * 0.91 - al * 1.0
    print(f'{label:<10} | {len(sub):>3} | {fw}-{fl}        | {f_pl:>+8.2f}u | {aw}-{al}        | {a_pl:>+8.2f}u | {a_pl-f_pl:>+12.2f}u')

print('\n-- ML (FADE side filtered to in-scope [-150, +140]) --')
print(f'{"Bucket":<10} | {"n":>3} | {"# fade in-scope":<15} | {"FADE W-L":<10} | {"FADE P/L":>10} | {"avg fade ML":>11}')
print('-' * 90)
for label, lo, hi in buckets:
    sub = [r for r in universe if lo <= r['div'] < hi]
    if not sub: continue
    fade_in = [r for r in sub if r['fade_ml'] is not None and -150 <= r['fade_ml'] <= 140]
    if not fade_in:
        print(f'{label:<10} | {len(sub):>3} | 0               | -          | -          | -')
        continue
    aw = sum(1 for r in fade_in if r['fade_ml_won'])
    al = len(fade_in) - aw
    a_pl = sum(pl_at(r['fade_ml'], r['fade_ml_won']) for r in fade_in)
    avg_ml = sum(r['fade_ml'] for r in fade_in) / len(fade_in)
    print(f'{label:<10} | {len(sub):>3} | {len(fade_in):<15} | {aw}-{al}        | {a_pl:>+8.2f}u | {avg_ml:>+10.0f}')

# Overall summary
print('\n=== OVERALL FADE vs FOLLOW SUMMARY ===\n')
print('SPREAD -110:')
fw_all = sum(1 for r in universe if r['follow_spread_won'])
fl_all = sum(1 for r in universe if not r['follow_spread_won'] and not r['follow_spread_push'])
fp_all = sum(1 for r in universe if r['follow_spread_push'])
aw_all = sum(1 for r in universe if r['fade_spread_won'])
al_all = sum(1 for r in universe if not r['fade_spread_won'] and not r['fade_spread_push'])
print(f'  FOLLOW: {fw_all}-{fl_all}-{fp_all}, P/L={fw_all*0.91 - fl_all:+.2f}u')
print(f'  FADE  : {aw_all}-{al_all}-{fp_all}, P/L={aw_all*0.91 - al_all:+.2f}u')

print('\nML in-scope FADE only (the actually bettable subset):')
fade_in_all = [r for r in universe if r['fade_ml'] is not None and -150 <= r['fade_ml'] <= 140]
if fade_in_all:
    aw = sum(1 for r in fade_in_all if r['fade_ml_won'])
    al = len(fade_in_all) - aw
    a_pl = sum(pl_at(r['fade_ml'], r['fade_ml_won']) for r in fade_in_all)
    avg_ml = sum(r['fade_ml'] for r in fade_in_all) / len(fade_in_all)
    print(f'  n={len(fade_in_all)}, W-L={aw}-{al}, WR={100*aw/len(fade_in_all):.1f}%, P/L={a_pl:+.2f}u, avg_fade_ML={avg_ml:+.0f}')

# By tour
print('\n=== By tour ===')
for tour in ('atp', 'wta'):
    sub = [r for r in universe if tour in r['sport']]
    if not sub: continue
    fw = sum(1 for r in sub if r['follow_spread_won'])
    fl = sum(1 for r in sub if not r['follow_spread_won'] and not r['follow_spread_push'])
    aw = sum(1 for r in sub if r['fade_spread_won'])
    al = sum(1 for r in sub if not r['fade_spread_won'] and not r['fade_spread_push'])
    print(f'  {tour.upper()} (n={len(sub)}): FOLLOW {fw}-{fl} {fw*0.91-fl:+.2f}u | FADE {aw}-{al} {aw*0.91-al:+.2f}u')

# Show all picks with full detail
print('\n=== ALL post-overhaul observations (detailed) ===')
print(f'{"Date":<11} | {"Tour":<4} | {"Match":<35} | {"div":>4} | {"FOLLOW player":<18} | {"Spread":>6} | {"Spread res":<10} | {"FADE ML":>7} | {"FADE won":<8}')
for r in sorted(universe, key=lambda x: (x['date'], x['div'])):
    sp_res = 'WIN' if r['follow_spread_won'] else ('PUSH' if r['follow_spread_push'] else 'LOSS')
    fade_ml_str = f"{r['fade_ml']:+.0f}" if r['fade_ml'] is not None else 'N/A'
    print(f"{r['date']:<11} | {('ATP' if 'atp' in r['sport'] else 'WTA'):<4} | {r['p1'][:16]+' v '+r['p2'][:16]:<35} | {r['div']:>4.1f} | {r['follow_player'][:18]:<18} | {r['follow_line']:>+6.1f} | {sp_res:<10} | {fade_ml_str:>7} | {str(r['fade_ml_won']):<8}")
