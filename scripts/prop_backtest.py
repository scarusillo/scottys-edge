"""Backtest prop edges by odds bucket and edge bucket using box_scores + prop_snapshots."""
import sqlite3
import os

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

def american_to_implied(odds):
    if odds > 0: return 100.0 / (odds + 100.0)
    elif odds < 0: return abs(odds) / (abs(odds) + 100.0)
    return 0.5

def payout_per_unit(odds):
    if odds > 0: return odds / 100.0
    elif odds < 0: return 100.0 / abs(odds)
    return 1.0

# Maps prop_snapshots market name -> box_scores stat_type
STAT_MAP = {
    'batter_hits': 'hits',
    'batter_rbis': 'rbi',
    'batter_runs_scored': 'runs',
    'batter_total_bases': None,  # Not directly in box_scores, skip
    'batter_home_runs': 'hr',
    'batter_strikeouts': 'batter_k',
    'pitcher_strikeouts': 'pitcher_k',
    'pitcher_hits_allowed': 'pitcher_h_allowed',
    'pitcher_earned_runs': 'pitcher_er',
    'pitcher_outs': 'pitcher_outs',
}

def run():
    db = sqlite3.connect(DB)

    # Get players with 20+ games (distinct game_dates)
    players = db.execute("""
        SELECT player, COUNT(DISTINCT game_date) as games
        FROM box_scores WHERE sport = 'baseball_mlb'
        GROUP BY player HAVING COUNT(DISTINCT game_date) >= 20
    """).fetchall()
    print(f"MLB players with 20+ games: {len(players)}")

    # Cache box scores per player per stat_type
    # {player: {stat_type: [values ordered by game_date DESC]}}
    player_stats = {}
    for player, _ in players:
        player_stats[player] = {}
        rows = db.execute("""
            SELECT stat_type, stat_value, game_date
            FROM box_scores WHERE player = ? AND sport = 'baseball_mlb'
            ORDER BY game_date DESC
        """, (player,)).fetchall()
        by_stat = {}
        for st, sv, gd in rows:
            if st not in by_stat:
                by_stat[st] = []
            by_stat[st].append(sv)
        player_stats[player] = by_stat

    # Evaluate all prop snapshots from last 14 days
    all_results = []
    for player, _ in players:
        stats = player_stats.get(player, {})
        if not stats:
            continue

        props = db.execute("""
            SELECT market, line, odds, book
            FROM prop_snapshots
            WHERE player = ? AND side = 'Over' AND sport = 'baseball_mlb'
            AND DATE(captured_at) >= DATE('now', '-14 days')
            GROUP BY market, line, book
            HAVING captured_at = MAX(captured_at)
        """, (player,)).fetchall()

        for market, line, odds, book in props:
            stat_type = STAT_MAP.get(market)
            if not stat_type:
                continue
            values = stats.get(stat_type, [])
            if len(values) < 20:
                continue

            recent20 = values[:20]
            hit_rate_20 = sum(1 for v in recent20 if v > line) / len(recent20)
            full_rate = sum(1 for v in values if v > line) / len(values)
            blended = 0.5 * hit_rate_20 + 0.5 * full_rate

            implied = american_to_implied(odds)
            if implied <= 0:
                continue

            edge = (blended - implied) * 100.0
            if edge <= 0:
                continue

            # EV per unit
            wp = payout_per_unit(odds)
            ev_per_unit = blended * wp - (1 - blended) * 1.0

            all_results.append({
                'player': player, 'market': market, 'line': line,
                'odds': odds, 'book': book, 'hit_rate': blended,
                'implied': implied, 'edge': edge, 'ev_per_unit': ev_per_unit,
            })

    print(f"Positive-edge props found: {len(all_results)}\n")

    # Edge buckets
    edge_buckets = {'0-5%': [], '5-10%': [], '10-15%': [], '15-20%': [], '20%+': []}
    for r in all_results:
        e = r['edge']
        if e >= 20: edge_buckets['20%+'].append(r)
        elif e >= 15: edge_buckets['15-20%'].append(r)
        elif e >= 10: edge_buckets['10-15%'].append(r)
        elif e >= 5: edge_buckets['5-10%'].append(r)
        else: edge_buckets['0-5%'].append(r)

    print("BY EDGE BUCKET:")
    print(f"  {'Bucket':>10s} {'Count':>6s} {'Avg EV/u':>9s} {'Avg Hit%':>9s} {'Avg Imp%':>9s} {'Avg Edge':>9s} {'Avg Odds':>9s}")
    print("  " + "-" * 65)
    for eb in ['0-5%', '5-10%', '10-15%', '15-20%', '20%+']:
        picks = edge_buckets[eb]
        if not picks:
            continue
        n = len(picks)
        avg_ev = sum(p['ev_per_unit'] for p in picks) / n
        avg_hr = sum(p['hit_rate'] for p in picks) / n
        avg_imp = sum(p['implied'] for p in picks) / n
        avg_edge = sum(p['edge'] for p in picks) / n
        avg_odds = sum(p['odds'] for p in picks) / n
        print(f"  {eb:>10s} {n:6d} {avg_ev:+9.3f} {avg_hr:9.1%} {avg_imp:9.1%} {avg_edge:+9.1f}% {avg_odds:+9.0f}")

    # Odds buckets
    odds_buckets = {'<-150': [], '-150 to -101': [], '-100 to +100': [], '+101 to +120': [], '+121 to +140': [], '+141+': []}
    for r in all_results:
        o = r['odds']
        if o < -150: odds_buckets['<-150'].append(r)
        elif o < -100: odds_buckets['-150 to -101'].append(r)
        elif o <= 100: odds_buckets['-100 to +100'].append(r)
        elif o <= 120: odds_buckets['+101 to +120'].append(r)
        elif o <= 140: odds_buckets['+121 to +140'].append(r)
        else: odds_buckets['+141+'].append(r)

    print(f"\nBY ODDS BUCKET:")
    print(f"  {'Bucket':>15s} {'Count':>6s} {'Avg EV/u':>9s} {'Avg Hit%':>9s} {'Avg Imp%':>9s} {'Avg Edge':>9s}")
    print("  " + "-" * 55)
    for ob in ['<-150', '-150 to -101', '-100 to +100', '+101 to +120', '+121 to +140', '+141+']:
        picks = odds_buckets[ob]
        if not picks:
            continue
        n = len(picks)
        avg_ev = sum(p['ev_per_unit'] for p in picks) / n
        avg_hr = sum(p['hit_rate'] for p in picks) / n
        avg_imp = sum(p['implied'] for p in picks) / n
        avg_edge = sum(p['edge'] for p in picks) / n
        print(f"  {ob:>15s} {n:6d} {avg_ev:+9.3f} {avg_hr:9.1%} {avg_imp:9.1%} {avg_edge:+9.1f}%")

    # Cross table: edge x odds
    print(f"\nCROSS TABLE (count | avg EV per unit):")
    obs = ['<-150', '-150 to -101', '-100 to +100', '+101 to +120', '+121 to +140', '+141+']
    header = f"  {'':>10s}"
    for ob in obs:
        header += f" {ob:>14s}"
    print(header)
    print("  " + "-" * 100)

    def get_ob(odds):
        if odds < -150: return '<-150'
        elif odds < -100: return '-150 to -101'
        elif odds <= 100: return '-100 to +100'
        elif odds <= 120: return '+101 to +120'
        elif odds <= 140: return '+121 to +140'
        else: return '+141+'

    def get_eb(edge):
        if edge >= 20: return '20%+'
        elif edge >= 15: return '15-20%'
        elif edge >= 10: return '10-15%'
        elif edge >= 5: return '5-10%'
        else: return '0-5%'

    for eb in ['0-5%', '5-10%', '10-15%', '15-20%', '20%+']:
        line_out = f"  {eb:>10s}"
        for ob in obs:
            matches = [r for r in all_results if get_eb(r['edge']) == eb and get_ob(r['odds']) == ob]
            if matches:
                n = len(matches)
                ev = sum(m['ev_per_unit'] for m in matches) / n
                line_out += f" {n:>5d}|{ev:+.3f}  "
            else:
                line_out += f" {'--':>14s}"
        print(line_out)

    # Minus odds deep dive
    minus_props = [r for r in all_results if r['odds'] < -100]
    print(f"\n\nMINUS ODDS DEEP DIVE ({len(minus_props)} props):")
    print(f"  {'Market':>25s} {'Line':>5s} {'Odds':>6s} {'Hit%':>6s} {'Imp%':>6s} {'Edge':>6s} {'EV/u':>7s} {'Player':>20s}")
    print("  " + "-" * 90)
    minus_sorted = sorted(minus_props, key=lambda r: r['ev_per_unit'], reverse=True)
    for r in minus_sorted[:30]:
        print(f"  {r['market']:>25s} {r['line']:5.1f} {r['odds']:+6.0f} {r['hit_rate']:6.1%} {r['implied']:6.1%} {r['edge']:+6.1f}% {r['ev_per_unit']:+7.3f} {r['player'][:20]:>20s}")

    db.close()

if __name__ == '__main__':
    run()
