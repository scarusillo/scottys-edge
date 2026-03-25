import sqlite3, os, sys
sys.path.insert(0, '.')
from datetime import datetime, timedelta, timezone
from collections import defaultdict

DB_PATH = os.path.join('.', '..', 'data', 'betting_model.db')
from props_engine import compute_consensus, find_consensus_edges, detect_stale_lines, MIN_BOOKS_FOR_CONSENSUS, PROP_LABEL, EXCLUDED_BOOKS, STAT_TYPE_MAP
from scottys_edge import kelly_units, get_star_rating
from box_scores import lookup_player_stat, PROP_TO_STAT

conn = sqlite3.connect(DB_PATH)
cutoff = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
date_rows = conn.execute('SELECT DISTINCT DATE(captured_at) as d FROM prop_snapshots WHERE d >= ? ORDER BY d', (cutoff,)).fetchall()

all_picks = []
for (day_str,) in date_rows:
    rows = conn.execute('''SELECT sport, event_id, commence_time, home, away, book, market, player, side, line, odds, implied_prob
        FROM prop_snapshots WHERE DATE(captured_at) = ? AND sport IN ("basketball_nba","basketball_ncaab","icehockey_nhl")''', (day_str,)).fetchall()
    if not rows: continue
    groups = defaultdict(list)
    game_info = {}
    for r in rows:
        sport, eid, commence, home, away, book, market, player, side, line_val, odds, imp = r
        groups[(eid, player, market, line_val)].append({'book': book, 'line': line_val, 'side': side, 'odds': odds, 'implied_prob': imp})
        game_info[eid] = {'sport': sport, 'home': home, 'away': away, 'commence': commence}
    seen = set()
    for (eid, player, market, line_val), lines in groups.items():
        gi = game_info.get(eid, {})
        book_lines = defaultdict(lambda: {'book': None, 'line': line_val})
        for e in lines:
            bk = e['book']; book_lines[bk]['book'] = bk; book_lines[bk]['line'] = e['line']
            if e['side'] == 'Over': book_lines[bk]['over_odds'] = e['odds']
            else: book_lines[bk]['under_odds'] = e['odds']
        bl_list = [v for v in book_lines.values() if v.get('over_odds') and v.get('under_odds')]
        if len(bl_list) < MIN_BOOKS_FOR_CONSENSUS: continue
        consensus = compute_consensus(bl_list)
        if not consensus: continue
        edges = find_consensus_edges(consensus, min_edge=3.0)
        stale = detect_stale_lines(consensus, gi.get('sport', ''))
        for ce in edges:
            if ce['book'] in EXCLUDED_BOOKS: continue
            base_edge = ce['edge_pct']
            stale_bonus = 0.0
            if ce['book'] in stale:
                si = stale[ce['book']]
                if ce['side'] == 'OVER' and si['book_line'] < si['consensus_line']:
                    stale_bonus = min(10.0, si['stale_amount'] * 3.0)
                elif ce['side'] == 'UNDER' and si['book_line'] > si['consensus_line']:
                    stale_bonus = min(10.0, si['stale_amount'] * 3.0)
            final_edge = base_edge + min(stale_bonus * 0.20, 4.0)
            if final_edge < 5.5: continue
            stars = get_star_rating(final_edge)
            if stars < 2.0: continue
            label = PROP_LABEL.get(market, market.replace('player_','').upper())
            units = kelly_units(edge_pct=final_edge, odds=ce['odds'], fraction=0.25)
            dk = f'{eid}|{player}|{market}'
            if dk in seen: continue
            seen.add(dk)
            stat_type_key = PROP_TO_STAT.get(label.upper(), STAT_TYPE_MAP.get(market, label.lower()))
            actual = lookup_player_stat(conn, player, stat_type_key, day_str, sport=gi.get('sport',''))
            if actual is None: continue
            side = ce['side']
            if side == 'OVER': result = 'WIN' if actual > line_val else 'LOSS' if actual < line_val else 'PUSH'
            else: result = 'WIN' if actual < line_val else 'LOSS' if actual > line_val else 'PUSH'
            dec = ce['odds']/100.0 if ce['odds']>0 else 100.0/abs(ce['odds'])
            pnl = units * dec if result=='WIN' else -units if result=='LOSS' else 0
            all_picks.append({
                'date': day_str, 'sport': gi.get('sport',''), 'player': player,
                'market': market, 'label': label, 'side': side, 'line': line_val,
                'actual': actual, 'book': ce['book'], 'odds': ce['odds'],
                'edge': final_edge, 'base_edge': base_edge, 'stale_bonus': stale_bonus,
                'units': units, 'result': result, 'pnl': pnl,
                'book_count': consensus['book_count'],
                'home': gi.get('home',''), 'away': gi.get('away',''),
                'consensus_line': consensus.get('consensus_line', line_val),
            })
conn.close()

print(f"Total graded picks: {len(all_picks)}")
print()

# 1. BY SPORT + SIDE
print("=" * 80)
print("BY SPORT + SIDE (OVER vs UNDER)")
print("=" * 80)
for sport in sorted(set(p['sport'] for p in all_picks)):
    for side in ('OVER', 'UNDER'):
        subset = [p for p in all_picks if p['sport']==sport and p['side']==side and p['result'] in ('WIN','LOSS')]
        if not subset: continue
        w = sum(1 for p in subset if p['result']=='WIN')
        l = sum(1 for p in subset if p['result']=='LOSS')
        pnl = sum(p['pnl'] for p in subset)
        print(f"  {sport:25} {side:6} {w}W-{l}L ({w/(w+l)*100:.0f}%) | {pnl:+.1f}u")

# 2. BY MARKET + SIDE
print()
print("=" * 80)
print("BY MARKET TYPE + SIDE")
print("=" * 80)
for mkt in sorted(set(p['label'] for p in all_picks)):
    for side in ('OVER', 'UNDER'):
        subset = [p for p in all_picks if p['label']==mkt and p['side']==side and p['result'] in ('WIN','LOSS')]
        if not subset: continue
        w = sum(1 for p in subset if p['result']=='WIN')
        l = sum(1 for p in subset if p['result']=='LOSS')
        pnl = sum(p['pnl'] for p in subset)
        avg_edge = sum(p['edge'] for p in subset) / len(subset)
        avg_odds = sum(p['odds'] for p in subset) / len(subset)
        print(f"  {mkt:15} {side:6} {w}W-{l}L ({w/(w+l)*100:.0f}%) | {pnl:+.1f}u | avg_edge={avg_edge:.1f}% avg_odds={avg_odds:+.0f}")

# 3. BY BOOK
print()
print("=" * 80)
print("BY BOOK")
print("=" * 80)
book_stats = defaultdict(lambda: {'w':0,'l':0,'pnl':0.0})
for p in all_picks:
    if p['result'] not in ('WIN','LOSS'): continue
    b = p['book']
    if p['result']=='WIN': book_stats[b]['w']+=1; book_stats[b]['pnl']+=p['pnl']
    else: book_stats[b]['l']+=1; book_stats[b]['pnl']+=p['pnl']
for b, s in sorted(book_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
    wp = s['w']/(s['w']+s['l'])*100
    print(f"  {b:20} {s['w']}W-{s['l']}L ({wp:.0f}%) | {s['pnl']:+.1f}u")

# 4. BY EDGE BUCKET + SIDE
print()
print("=" * 80)
print("BY EDGE BUCKET + SIDE")
print("=" * 80)
for lo, hi, lbl in [(5,10,'5-10%'),(10,15,'10-15%'),(15,20,'15-20%'),(20,30,'20-30%'),(30,100,'30%+')]:
    for side in ('OVER','UNDER'):
        subset = [p for p in all_picks if lo<=p['edge']<hi and p['side']==side and p['result'] in ('WIN','LOSS')]
        if not subset: continue
        w = sum(1 for p in subset if p['result']=='WIN')
        l = sum(1 for p in subset if p['result']=='LOSS')
        pnl = sum(p['pnl'] for p in subset)
        print(f"  Edge {lbl:8} {side:6} {w}W-{l}L ({w/(w+l)*100:.0f}%) | {pnl:+.1f}u")

# 5. BY BOOK COUNT (consensus strength)
print()
print("=" * 80)
print("BY BOOK COUNT (consensus strength)")
print("=" * 80)
for bc in sorted(set(p['book_count'] for p in all_picks)):
    subset = [p for p in all_picks if p['book_count']==bc and p['result'] in ('WIN','LOSS')]
    if not subset: continue
    w = sum(1 for p in subset if p['result']=='WIN')
    l = sum(1 for p in subset if p['result']=='LOSS')
    pnl = sum(p['pnl'] for p in subset)
    print(f"  {bc} books: {w}W-{l}L ({w/(w+l)*100:.0f}%) | {pnl:+.1f}u")

# 6. BY ODDS RANGE
print()
print("=" * 80)
print("BY ODDS RANGE")
print("=" * 80)
for lo, hi, lbl in [(-200,-130,'Heavy fav (-200 to -130)'),(-129,-100,'Light fav (-129 to -100)'),(100,150,'Small dog (+100 to +150)'),(151,250,'Med dog (+151 to +250)'),(251,500,'Big dog (+251 to +500)')]:
    subset = [p for p in all_picks if lo<=p['odds']<=hi and p['result'] in ('WIN','LOSS')]
    if not subset: continue
    w = sum(1 for p in subset if p['result']=='WIN')
    l = sum(1 for p in subset if p['result']=='LOSS')
    pnl = sum(p['pnl'] for p in subset)
    print(f"  {lbl:35} {w}W-{l}L ({w/(w+l)*100:.0f}%) | {pnl:+.1f}u")

# 7. EVERY SINGLE LOSS — detailed
print()
print("=" * 80)
print("EVERY LOSS — DETAILED")
print("=" * 80)
losses = [p for p in all_picks if p['result']=='LOSS']
losses.sort(key=lambda x: x['pnl'])
for p in losses:
    miss = abs(p['actual'] - p['line'])
    direction = 'needed MORE' if p['side']=='OVER' else 'needed LESS'
    print(f"  {p['date']} | {p['player']:25} {p['side']:5} {p['line']:5} {p['label']:10} | actual={p['actual']:5} miss={miss:.1f} {direction}")
    print(f"           | {p['book']:15} odds={p['odds']:+4} edge={p['edge']:.1f}% books={p['book_count']} | {p['home']} vs {p['away']}")
    print()

# 8. EVERY WIN — detailed
print()
print("=" * 80)
print("EVERY WIN — DETAILED")
print("=" * 80)
wins = [p for p in all_picks if p['result']=='WIN']
wins.sort(key=lambda x: x['pnl'], reverse=True)
for p in wins:
    margin = abs(p['actual'] - p['line'])
    print(f"  {p['date']} | {p['player']:25} {p['side']:5} {p['line']:5} | actual={p['actual']:5} margin={margin:.1f} +{p['pnl']:.1f}u")
    print(f"           | {p['book']:15} odds={p['odds']:+4} edge={p['edge']:.1f}% {p['label']} | {p['home']} vs {p['away']}")
    print()

# 9. STALE LINE ANALYSIS
print()
print("=" * 80)
print("STALE LINE PICKS vs NON-STALE")
print("=" * 80)
for stale_flag in (True, False):
    subset = [p for p in all_picks if (p['stale_bonus']>0)==stale_flag and p['result'] in ('WIN','LOSS')]
    if not subset: continue
    lbl = 'STALE LINE' if stale_flag else 'NO STALE'
    w = sum(1 for p in subset if p['result']=='WIN')
    l = sum(1 for p in subset if p['result']=='LOSS')
    pnl = sum(p['pnl'] for p in subset)
    print(f"  {lbl:15} {w}W-{l}L ({w/(w+l)*100:.0f}%) | {pnl:+.1f}u")

# 10. LINE VALUE ANALYSIS - low lines vs high lines
print()
print("=" * 80)
print("BY LINE VALUE (low lines vs high lines)")
print("=" * 80)
for lo, hi, lbl in [(0,1.5,'Very low (0-1.5)'),(1.5,5.5,'Low (1.5-5.5)'),(5.5,15,'Medium (5.5-15)'),(15,50,'High (15-50)')]:
    for side in ('OVER','UNDER'):
        subset = [p for p in all_picks if lo<=p['line']<hi and p['side']==side and p['result'] in ('WIN','LOSS')]
        if not subset: continue
        w = sum(1 for p in subset if p['result']=='WIN')
        l = sum(1 for p in subset if p['result']=='LOSS')
        pnl = sum(p['pnl'] for p in subset)
        print(f"  Line {lbl:20} {side:6} {w}W-{l}L ({w/(w+l)*100:.0f}%) | {pnl:+.1f}u")
