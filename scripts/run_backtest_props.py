"""Quick prop backtest with all upstream fixes applied."""
import sqlite3, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from datetime import datetime, timedelta, timezone
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
from props_engine import (compute_consensus, find_consensus_edges, detect_stale_lines,
    MIN_BOOKS_FOR_CONSENSUS, MIN_BOOKS_BY_SPORT, PROP_LABEL, EXCLUDED_BOOKS, NY_LEGAL_BOOKS,
    STAT_TYPE_MAP, american_to_implied)
from scottys_edge import kelly_units, get_star_rating
from box_scores import lookup_player_stat, PROP_TO_STAT

conn = sqlite3.connect(DB_PATH)
cutoff = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
date_rows = conn.execute('SELECT DISTINCT DATE(captured_at) as d FROM prop_snapshots WHERE d >= ? ORDER BY d', (cutoff,)).fetchall()

all_picks = []
funnel = defaultdict(int)

for (day_str,) in date_rows:
    rows = conn.execute("""SELECT sport, event_id, commence_time, home, away, book, market,
        player, side, line, odds, implied_prob FROM prop_snapshots WHERE DATE(captured_at) = ?""",
        (day_str,)).fetchall()
    if not rows:
        continue
    groups = defaultdict(list)
    game_info = {}
    for r in rows:
        sport, eid, commence, home, away, book, market, player, side, line_val, odds, imp = r
        groups[(eid, player, market, line_val)].append(
            {'book': book, 'line': line_val, 'side': side, 'odds': odds, 'implied_prob': imp})
        game_info[eid] = {'sport': sport, 'home': home, 'away': away}

    seen = set()
    for (eid, player, market, line_val), lines in groups.items():
        gi = game_info.get(eid, {})
        sp = gi.get('sport', '')
        funnel['total'] += 1

        _min_books = MIN_BOOKS_BY_SPORT.get(sp, MIN_BOOKS_FOR_CONSENSUS)
        book_lines_dict = defaultdict(lambda: {'book': None, 'line': line_val})
        for e in lines:
            bk = e['book']
            book_lines_dict[bk]['book'] = bk
            book_lines_dict[bk]['line'] = e['line']
            if e['side'] == 'Over':
                book_lines_dict[bk]['over_odds'] = e['odds']
            else:
                book_lines_dict[bk]['under_odds'] = e['odds']
        bl_list = [v for v in book_lines_dict.values() if v.get('over_odds') and v.get('under_odds')]

        if len(bl_list) < _min_books:
            funnel['killed_min_books'] += 1
            continue
        funnel['pass_min_books'] += 1

        consensus = compute_consensus(bl_list)
        if not consensus:
            continue
        edges = find_consensus_edges(consensus, min_edge=3.0)
        if not edges:
            funnel['killed_no_edge'] += 1
            continue
        funnel['has_edge'] += 1

        for ce in edges:
            book = ce['book']
            side = ce['side']
            base_edge = ce['edge_pct']

            # EXCLUDED BOOK RESCUE
            if book in EXCLUDED_BOOKS:
                best_legal = None
                best_legal_odds = None
                for bl in bl_list:
                    bk = bl.get('book', '')
                    if bk in EXCLUDED_BOOKS or bk not in NY_LEGAL_BOOKS:
                        continue
                    if side == 'OVER' and bl.get('over_odds'):
                        if best_legal_odds is None or bl['over_odds'] > best_legal_odds:
                            best_legal = bk
                            best_legal_odds = bl['over_odds']
                    elif side == 'UNDER' and bl.get('under_odds'):
                        if best_legal_odds is None or bl['under_odds'] > best_legal_odds:
                            best_legal = bk
                            best_legal_odds = bl['under_odds']
                if best_legal and best_legal_odds:
                    legal_imp = american_to_implied(best_legal_odds)
                    legal_edge = (ce['model_prob'] - legal_imp) * 100 if legal_imp else 0
                    if legal_edge >= 3.0:
                        book = best_legal
                        ce = dict(ce)
                        ce['book'] = best_legal
                        ce['odds'] = best_legal_odds
                        ce['edge_pct'] = legal_edge
                        base_edge = legal_edge
                    else:
                        continue
                else:
                    continue

            if base_edge < 5.5:
                continue
            stars = get_star_rating(base_edge)
            if stars < 2.0:
                continue

            label = PROP_LABEL.get(market, market.replace('player_', '').upper())
            units = kelly_units(edge_pct=base_edge, odds=ce['odds'], fraction=0.25)
            dk = f'{eid}|{player}|{market}'
            if dk in seen:
                continue
            seen.add(dk)
            funnel['deduped'] += 1

            # 4 downstream filters
            if side != 'OVER':
                funnel['killed_under'] += 1
                continue
            if book == 'FanDuel':
                funnel['killed_fanduel'] += 1
                continue
            if consensus['book_count'] >= 7:
                funnel['killed_7books'] += 1
                continue
            if 151 <= ce['odds'] <= 250:
                funnel['killed_med_dog'] += 1
                continue
            funnel['pass_all_filters'] += 1

            # Grade
            stat_type_key = PROP_TO_STAT.get(label.upper(), STAT_TYPE_MAP.get(market, label.lower()))
            actual = lookup_player_stat(conn, player, stat_type_key, day_str, sport=sp)
            if actual is None:
                funnel['no_box_score'] += 1
                continue
            funnel['graded'] += 1

            if actual > line_val:
                result = 'WIN'
            elif actual < line_val:
                result = 'LOSS'
            else:
                result = 'PUSH'
            dec = ce['odds'] / 100.0 if ce['odds'] > 0 else 100.0 / abs(ce['odds'])
            pnl = units * dec if result == 'WIN' else -units if result == 'LOSS' else 0
            all_picks.append({
                'date': day_str, 'sport': sp, 'player': player, 'label': label,
                'side': side, 'line': line_val, 'actual': actual, 'book': book,
                'odds': ce['odds'], 'edge': base_edge, 'units': units,
                'result': result, 'pnl': pnl, 'book_count': consensus['book_count'],
                'home': gi.get('home', ''), 'away': gi.get('away', ''),
            })

conn.close()

# RESULTS
graded = [p for p in all_picks if p['result'] in ('WIN', 'LOSS')]
w = sum(1 for p in graded if p['result'] == 'WIN')
l = sum(1 for p in graded if p['result'] == 'LOSS')
pnl = sum(p['pnl'] for p in graded)
wag = sum(p['units'] for p in graded)
roi = pnl / wag * 100 if wag > 0 else 0

print('=' * 70)
print('  PROP BACKTEST — ALL UPSTREAM FIXES APPLIED')
print('  sport-specific min_books | excluded book rescue | NHL box scores')
print('=' * 70)
print(f'  Record: {w}W-{l}L ({w/(w+l)*100:.1f}%)' if (w+l) > 0 else '  No graded picks')
print(f'  P&L: {pnl:+.1f}u | Wagered: {wag:.1f}u | ROI: {roi:+.1f}%')
print()

print('  FUNNEL:')
for k, v in sorted(funnel.items(), key=lambda x: -x[1]):
    print(f'    {k:25} {v:>7}')

print()
print('  BY SPORT:')
sport_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0, 'wag': 0})
for p in graded:
    sp = p['sport']
    if p['result'] == 'WIN':
        sport_stats[sp]['w'] += 1; sport_stats[sp]['pnl'] += p['pnl']; sport_stats[sp]['wag'] += p['units']
    else:
        sport_stats[sp]['l'] += 1; sport_stats[sp]['pnl'] += p['pnl']; sport_stats[sp]['wag'] += p['units']
for sp, s in sorted(sport_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
    wp = s['w'] / (s['w'] + s['l']) * 100 if (s['w'] + s['l']) > 0 else 0
    sr = s['pnl'] / s['wag'] * 100 if s['wag'] > 0 else 0
    print(f'    {sp:30} {s["w"]}W-{s["l"]}L ({wp:.0f}%) | {s["pnl"]:+.1f}u | ROI {sr:+.1f}%')

print()
print('  BY MARKET:')
mkt = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0})
for p in graded:
    mk = p['label']
    if p['result'] == 'WIN':
        mkt[mk]['w'] += 1; mkt[mk]['pnl'] += p['pnl']
    else:
        mkt[mk]['l'] += 1; mkt[mk]['pnl'] += p['pnl']
for mk, s in sorted(mkt.items(), key=lambda x: x[1]['pnl'], reverse=True):
    wp = s['w'] / (s['w'] + s['l']) * 100 if (s['w'] + s['l']) > 0 else 0
    print(f'    {mk:15} {s["w"]}W-{s["l"]}L ({wp:.0f}%) | {s["pnl"]:+.1f}u')

print()
print('  BY BOOK:')
bk_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0})
for p in graded:
    b = p['book']
    if p['result'] == 'WIN':
        bk_stats[b]['w'] += 1; bk_stats[b]['pnl'] += p['pnl']
    else:
        bk_stats[b]['l'] += 1; bk_stats[b]['pnl'] += p['pnl']
for b, s in sorted(bk_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
    wp = s['w'] / (s['w'] + s['l']) * 100 if (s['w'] + s['l']) > 0 else 0
    print(f'    {b:20} {s["w"]}W-{s["l"]}L ({wp:.0f}%) | {s["pnl"]:+.1f}u')

print()
print('  ALL PICKS:')
for p in sorted(graded, key=lambda x: x['pnl'], reverse=True):
    icon = 'W' if p['result'] == 'WIN' else 'L'
    margin = abs(p['actual'] - p['line'])
    print(f'  [{icon}] {p["date"]} {p["player"]:25} OVER {p["line"]:5} {p["label"]:12} '
          f'actual={p["actual"]:5} margin={margin:.1f} | {p["pnl"]:+.1f}u | '
          f'{p["book"]} {p["odds"]:+.0f} edge={p["edge"]:.1f}% bks={p["book_count"]} | {p["sport"]}')
