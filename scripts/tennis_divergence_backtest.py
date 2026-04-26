"""Post-v25.81 tennis divergence backtest. Run once, prints to stdout."""
import sqlite3, re, json
from collections import defaultdict

conn = sqlite3.connect('data/betting_model.db')
c = conn.cursor()

parse = re.compile(r'div=([\-\d\.]+),\s*ms=([\+\-\d\.]+),\s*mkt_sp=([\+\-\d\.]+)')

# Pull all post-v25.81 evaluation rows (Apr 23-24, completed games)
rows = c.execute("""
    SELECT DATE(MIN(created_at)) d, sport, event_id, selection, MIN(reason) reason
    FROM shadow_blocked_picks
    WHERE sport LIKE 'tennis%'
      AND DATE(created_at) BETWEEN '2026-04-23' AND '2026-04-24'
      AND (reason LIKE '%insufficient_elo%' OR reason LIKE '%post_elo%')
    GROUP BY sport, event_id
""").fetchall()

fires = c.execute("""
    SELECT DATE(b.created_at) d, b.sport, b.event_id, b.selection, b.market_type, b.line, b.odds, b.book
    FROM bets b
    WHERE b.sport LIKE 'tennis%'
      AND DATE(b.created_at) BETWEEN '2026-04-23' AND '2026-04-24'
      AND b.units >= 3.5
      AND (b.context_factors NOT LIKE '%BLOCK_BACKTEST%' OR b.context_factors IS NULL)
""").fetchall()

def get_ml(event_id, player_name):
    r = c.execute("SELECT home, away, best_home_ml, best_away_ml FROM market_consensus WHERE event_id=? LIMIT 1", (event_id,)).fetchone()
    if r:
        h, a, hml, aml = r
        pl = (player_name or '').lower()
        if h and (pl in h.lower() or h.lower() in pl): return hml
        if a and (pl in a.lower() or a.lower() in pl): return aml
    return None

def matches(p, n):
    pl = p.lower(); nl = n.lower()
    return pl in nl or nl in pl or any(t in nl for t in pl.split() if len(t) >= 4)

def grade_match(sport, event_id, p1, p2, bet_player, bet_line, date):
    res = c.execute("SELECT event_id, home, away, winner FROM results WHERE event_id=? AND sport=? AND completed=1", (event_id, sport)).fetchone()
    if not res:
        res = c.execute("SELECT event_id, home, away, winner FROM results WHERE sport=? AND completed=1 AND DATE(commence_time)=? AND ((home=? AND away=?) OR (home=? AND away=?)) LIMIT 1", (sport, date, p1, p2, p2, p1)).fetchone()
    if not res: return None
    espn_eid, home, away, winner = res
    tm = c.execute("SELECT set_scores FROM tennis_metadata WHERE event_id=?", (espn_eid,)).fetchone()
    if not tm or not tm[0]: return None
    sets = json.loads(tm[0])
    h_games = sum(s[0] for s in sets); a_games = sum(s[1] for s in sets)
    if matches(bet_player, home): margin = h_games - a_games
    elif matches(bet_player, away): margin = a_games - h_games
    else: return None
    spread_adj = margin + bet_line
    spread_res = 'WIN' if spread_adj > 0 else ('LOSS' if spread_adj < 0 else 'PUSH')
    won = matches(bet_player, winner) if winner else False
    return {'spread_result': spread_res, 'ml_won': won, 'margin': margin}

universe = []
for d, sport, event_id, sel, reason in rows:
    m = parse.search(reason)
    if not m: continue
    if ' vs ' not in sel: continue
    p1, p2 = [s.strip() for s in sel.split(' vs ', 1)]
    div = float(m.group(1))
    ms = float(m.group(2)); mkt_sp = float(m.group(3))
    if ms < mkt_sp: bet_player, bet_line = p1, mkt_sp
    elif ms > mkt_sp: bet_player, bet_line = p2, -mkt_sp
    else: continue
    ml = get_ml(event_id, bet_player)
    g = grade_match(sport, event_id, p1, p2, bet_player, bet_line, d)
    if not g: continue
    universe.append({'date': d, 'sport': sport, 'p1': p1, 'p2': p2,
        'bet_player': bet_player, 'bet_line': bet_line, 'div': div,
        'reason': 'insufficient_elo' if 'insufficient_elo' in reason else 'post_elo',
        'ml': ml, 'spread_result': g['spread_result'], 'ml_won': g['ml_won'],
        'source': 'BLOCKED', 'margin': g['margin']})

for d, sport, event_id, sel, mt, line, odds, book in fires:
    if mt == 'MONEYLINE':
        bet_player = sel.replace(' ML', '').strip()
    else:
        bet_player = re.sub(r'\s*[+\-]\d+\.?\d*\s*$', '', sel).strip()
    mc = c.execute("SELECT home, away FROM market_consensus WHERE event_id=? LIMIT 1", (event_id,)).fetchone()
    if not mc: continue
    h, a = mc
    g = grade_match(sport, event_id, h, a, bet_player, line if line else 0, d)
    if not g: continue
    universe.append({'date': d, 'sport': sport, 'p1': h, 'p2': a,
        'bet_player': bet_player, 'bet_line': line or 0, 'div': None,
        'reason': 'cleared', 'ml': odds if mt == 'MONEYLINE' else None,
        'spread_result': g['spread_result'], 'ml_won': g['ml_won'],
        'source': 'LIVE', 'margin': g['margin']})

print(f'Total post-overhaul tennis observations: {len(universe)}')
print(f'  BLOCKED: {sum(1 for r in universe if r["source"]=="BLOCKED")}')
print(f'  LIVE   : {sum(1 for r in universe if r["source"]=="LIVE")}')

print('\n=== PERFORMANCE BY DIVERGENCE BUCKET (Apr 23-24, post-v25.81 model) ===\n')
print('Format: SPREAD_-110 | ML_in-scope_(-150,+140)')
print(f'{"Bucket":<10} | {"n":>3} | {"SP W-L-P":<10} | {"SP WR":>6} | {"SP P/L":>8} | {"#in_scope":>9} | {"ML W-L":<7} | {"ML P/L":>8} | {"ML WR":>6}')
print('-' * 110)

buckets = [('0-2.5', 0, 2.5), ('2.5-3.0', 2.5, 3.0), ('3.0-3.5', 3.0, 3.5),
           ('3.5-4.0', 3.5, 4.0), ('4.0-5.0', 4.0, 5.0), ('5.0+', 5.0, 99)]

for label, lo, hi in buckets:
    sub = [r for r in universe if r['source'] == 'BLOCKED' and r['div'] is not None and lo <= r['div'] < hi]
    if not sub:
        print(f'{label:<10} | {0:>3} | (none)')
        continue
    w_sp = sum(1 for r in sub if r['spread_result'] == 'WIN')
    l_sp = sum(1 for r in sub if r['spread_result'] == 'LOSS')
    p_sp = sum(1 for r in sub if r['spread_result'] == 'PUSH')
    sp_pl = w_sp * 0.91 - l_sp * 1.0
    sp_wr = 100 * w_sp / (w_sp + l_sp) if (w_sp + l_sp) else 0

    ml_sub = [r for r in sub if r['ml'] is not None and -150 <= r['ml'] <= 140]
    w_ml = sum(1 for r in ml_sub if r['ml_won'])
    l_ml = sum(1 for r in ml_sub if not r['ml_won'])
    ml_pl = sum((r['ml']/100 if r['ml']>0 else 100/abs(r['ml'])) if r['ml_won'] else -1 for r in ml_sub)
    ml_wr = 100 * w_ml / (w_ml + l_ml) if (w_ml + l_ml) else 0

    print(f'{label:<10} | {len(sub):>3} | {w_sp:>2}-{l_sp:>2}-{p_sp:>2}    | {sp_wr:>5.1f}% | {sp_pl:>+7.2f}u | {len(ml_sub):>9} | {w_ml}-{l_ml}    | {ml_pl:>+7.2f}u | {ml_wr:>5.1f}%')

print('\n=== Live fires (post-overhaul actual fires) ===')
for r in universe:
    if r['source'] == 'LIVE':
        print(f'  {r["date"]} {r["sport"][:25]:<25} | {r["bet_player"][:20]:<20} | ML={r["ml"]} | margin={r["margin"]} | spread={r["spread_result"]} ml_won={r["ml_won"]}')

print('\n=== "IF DIVERGENCE CAP = X" SIMULATION (ML in-scope only) ===')
print('Includes BLOCKED that would have passed at cap + LIVE fires')
print(f'{"Cap":<7} | {"# fires":<7} | {"W-L":<7} | {"WR":>6} | {"P/L":>8} | {"ROI":>6}')
print('-' * 60)

for cap in [2.5, 3.0, 3.5, 4.0, 5.0, 99]:
    pass_sub = [r for r in universe if r['source'] == 'BLOCKED' and r['div'] is not None and r['div'] < cap and r['ml'] is not None and -150 <= r['ml'] <= 140]
    live_in = [r for r in universe if r['source'] == 'LIVE' and r['ml'] is not None]
    all_pass = pass_sub + live_in
    if not all_pass:
        print(f'{cap:<7} | 0')
        continue
    w = sum(1 for r in all_pass if r['ml_won'])
    l = sum(1 for r in all_pass if not r['ml_won'])
    pl = sum((r['ml']/100 if r['ml']>0 else 100/abs(r['ml'])) if r['ml_won'] else -1 for r in all_pass)
    wr = 100*w/(w+l) if (w+l) else 0
    roi = 100*pl/len(all_pass) if all_pass else 0
    print(f'{cap:<7} | {len(all_pass):<7} | {w}-{l}     | {wr:>5.1f}% | {pl:>+7.2f}u | {roi:>+5.1f}%')

print('\n=== By gate reason (ML in-scope only) ===')
for rt in ('insufficient_elo', 'post_elo'):
    sub = [r for r in universe if r['source'] == 'BLOCKED' and r['reason'] == rt and r['ml'] is not None and -150 <= r['ml'] <= 140]
    if not sub:
        print(f'  {rt}: n=0')
        continue
    w = sum(1 for r in sub if r['ml_won'])
    l = sum(1 for r in sub if not r['ml_won'])
    pl = sum((r['ml']/100 if r['ml']>0 else 100/abs(r['ml'])) if r['ml_won'] else -1 for r in sub)
    print(f'  {rt}: n={len(sub)}, W={w}, L={l}, P/L={pl:+.2f}u')

print('\n=== By tour (ML in-scope only) ===')
for tour in ('atp', 'wta'):
    sub = [r for r in universe if r['source'] == 'BLOCKED' and tour in r['sport'] and r['ml'] is not None and -150 <= r['ml'] <= 140]
    if not sub:
        print(f'  {tour.upper()}: n=0')
        continue
    w = sum(1 for r in sub if r['ml_won'])
    l = sum(1 for r in sub if not r['ml_won'])
    pl = sum((r['ml']/100 if r['ml']>0 else 100/abs(r['ml'])) if r['ml_won'] else -1 for r in sub)
    print(f'  {tour.upper()}: n={len(sub)}, W={w}, L={l}, P/L={pl:+.2f}u')

# All-blocks SPREAD detail
print('\n=== All blocks at SPREAD-110 by tour ===')
for tour in ('atp', 'wta'):
    sub = [r for r in universe if r['source'] == 'BLOCKED' and tour in r['sport']]
    if not sub: continue
    w = sum(1 for r in sub if r['spread_result'] == 'WIN')
    l = sum(1 for r in sub if r['spread_result'] == 'LOSS')
    p = sum(1 for r in sub if r['spread_result'] == 'PUSH')
    pl = w*0.91 - l*1.0
    print(f'  {tour.upper()}: n={len(sub)}, W-L-P={w}-{l}-{p}, P/L={pl:+.2f}u')

print('\n=== Picks per day breakdown ===')
day_buckets = defaultdict(lambda: {'n':0, 'in_scope':0, 'div_under_3':0})
for r in universe:
    if r['source'] != 'BLOCKED': continue
    day_buckets[r['date']]['n'] += 1
    if r['ml'] is not None and -150 <= r['ml'] <= 140:
        day_buckets[r['date']]['in_scope'] += 1
    if r['div'] is not None and r['div'] < 3.0:
        day_buckets[r['date']]['div_under_3'] += 1
for d in sorted(day_buckets):
    b = day_buckets[d]
    print(f'  {d}: total_blocked={b["n"]}, in_scope_ML={b["in_scope"]}, div<3.0={b["div_under_3"]}')
