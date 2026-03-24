"""Quick soccer backtest — last 45 days of completed games."""
import sqlite3, os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

def run_backtest(days_back=45):
    conn = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

    soccer_leagues = ['soccer_epl', 'soccer_germany_bundesliga', 'soccer_italy_serie_a',
                      'soccer_spain_la_liga', 'soccer_france_ligue_one', 'soccer_usa_mls',
                      'soccer_mexico_ligamx']

    from model_engine import SPORT_CONFIG

    results_by_sport = {}
    all_bets = []

    for sport in soccer_leagues:
        cfg = SPORT_CONFIG.get(sport, SPORT_CONFIG.get('soccer_epl'))
        hfa = cfg.get('home_court', 0.4)

        pr = {}
        for row in conn.execute('SELECT team, final_rating FROM power_ratings WHERE sport=?', (sport,)):
            pr[row[0]] = row[1]
        elo_data = {}
        for row in conn.execute('SELECT team, elo, games_played, confidence FROM elo_ratings WHERE sport=?', (sport,)):
            elo_data[row[0]] = {'rating': row[1], 'games': row[2], 'confidence': row[3]}

        games = conn.execute('''
            SELECT event_id, home, away, home_score, away_score, commence_time,
                   actual_total, actual_margin
            FROM results WHERE sport=? AND completed=1 AND commence_time >= ?
            ORDER BY commence_time
        ''', (sport, cutoff)).fetchall()

        sw = sl = sp_count = 0
        spnl = 0.0

        for game in games:
            eid, home, away, hs, as_, commence, act_total, act_margin = game
            if act_margin is None or act_total is None:
                continue

            mc = conn.execute('''
                SELECT best_home_spread, best_away_spread, best_home_spread_odds, best_away_spread_odds,
                       best_over_total, best_under_total, best_over_odds, best_under_odds
                FROM market_consensus WHERE event_id=?
                ORDER BY snapshot_date DESC LIMIT 1
            ''', (eid,)).fetchone()
            if not mc:
                continue

            h_spread, a_spread = mc[0], mc[1]
            total_line = mc[4]

            h_pr = pr.get(home)
            a_pr = pr.get(away)
            if h_pr is None or a_pr is None:
                continue
            ms = (h_pr - a_pr) + hfa

            if home in elo_data and away in elo_data:
                h_elo = elo_data[home]['rating']
                a_elo = elo_data[away]['rating']
                elo_spread = (h_elo - a_elo) / 160.0
                games_min = min(elo_data[home].get('games', 0), elo_data[away].get('games', 0))
                elo_w = min(1.0, games_min / 15.0)
                ms = ms * (1 - elo_w * 0.5) + elo_spread * (elo_w * 0.5)

            # SPREAD
            if h_spread is not None:
                divergence = ms - h_spread
                if abs(divergence) >= 0.3:
                    if divergence < -0.3:
                        cover = act_margin + h_spread
                        if cover > 0:
                            sw += 1; spnl += 4.5
                            all_bets.append(('W', 'SPREAD', sport, f"{home} {h_spread:+.1f}", 4.5))
                        elif cover < 0:
                            sl += 1; spnl -= 5.0
                            all_bets.append(('L', 'SPREAD', sport, f"{home} {h_spread:+.1f}", -5.0))
                        else:
                            sp_count += 1
                    elif divergence > 0.3 and a_spread is not None:
                        cover = -act_margin + a_spread
                        if cover > 0:
                            sw += 1; spnl += 4.5
                            all_bets.append(('W', 'SPREAD', sport, f"{away} {a_spread:+.1f}", 4.5))
                        elif cover < 0:
                            sl += 1; spnl -= 5.0
                            all_bets.append(('L', 'SPREAD', sport, f"{away} {a_spread:+.1f}", -5.0))
                        else:
                            sp_count += 1

            # TOTAL
            if total_line is not None:
                h_avg = conn.execute(
                    'SELECT AVG(home_score + away_score) FROM results WHERE sport=? AND (home=? OR away=?) AND completed=1',
                    (sport, home, home)).fetchone()[0]
                a_avg = conn.execute(
                    'SELECT AVG(home_score + away_score) FROM results WHERE sport=? AND (home=? OR away=?) AND completed=1',
                    (sport, away, away)).fetchone()[0]
                if h_avg and a_avg:
                    model_total = (h_avg + a_avg) / 2
                    diff = model_total - total_line
                    if abs(diff) >= 0.3:
                        if diff > 0.3:
                            if act_total > total_line:
                                sw += 1; spnl += 4.5
                                all_bets.append(('W', 'TOTAL', sport, f"OVER {total_line}", 4.5))
                            elif act_total < total_line:
                                sl += 1; spnl -= 5.0
                                all_bets.append(('L', 'TOTAL', sport, f"OVER {total_line}", -5.0))
                            else:
                                sp_count += 1
                        else:
                            if act_total < total_line:
                                sw += 1; spnl += 4.5
                                all_bets.append(('W', 'TOTAL', sport, f"UNDER {total_line}", 4.5))
                            elif act_total > total_line:
                                sl += 1; spnl -= 5.0
                                all_bets.append(('L', 'TOTAL', sport, f"UNDER {total_line}", -5.0))
                            else:
                                sp_count += 1

        results_by_sport[sport] = (sw, sl, sp_count, spnl)

    total_w = sum(v[0] for v in results_by_sport.values())
    total_l = sum(v[1] for v in results_by_sport.values())
    total_p = sum(v[2] for v in results_by_sport.values())
    total_pnl = sum(v[3] for v in results_by_sport.values())

    print('=' * 60)
    print(f'  SOCCER BACKTEST - Last {days_back} Days')
    print('=' * 60)
    wp = total_w / (total_w + total_l) * 100 if (total_w + total_l) > 0 else 0
    wagered = (total_w + total_l) * 5.0
    roi = total_pnl / wagered * 100 if wagered > 0 else 0
    print(f'  Overall: {total_w}W-{total_l}L-{total_p}P ({wp:.1f}%) | {total_pnl:+.1f}u | ROI {roi:+.1f}%')

    print(f'\n  By League:')
    for sp, (w, l, p, pnl) in sorted(results_by_sport.items(), key=lambda x: x[1][3], reverse=True):
        wp2 = w / (w + l) * 100 if (w + l) > 0 else 0
        name = sp.replace('soccer_', '').upper()
        print(f'    {name:30s} {w:2d}W-{l:2d}L-{p}P ({wp2:5.1f}%) | {pnl:+.1f}u')

    spread_bets = [b for b in all_bets if b[1] == 'SPREAD']
    total_bets = [b for b in all_bets if b[1] == 'TOTAL']

    print(f'\n  By Market:')
    for label, bets in [('SPREAD', spread_bets), ('TOTAL', total_bets)]:
        w = sum(1 for b in bets if b[0] == 'W')
        l = sum(1 for b in bets if b[0] == 'L')
        pnl = sum(b[4] for b in bets)
        wp2 = w / (w + l) * 100 if (w + l) > 0 else 0
        print(f'    {label:10s} {w:2d}W-{l:2d}L ({wp2:5.1f}%) | {pnl:+.1f}u')

    print(f'\n  Spread by League:')
    for sp in soccer_leagues:
        bets = [b for b in spread_bets if b[2] == sp]
        if not bets: continue
        w = sum(1 for b in bets if b[0] == 'W')
        l = sum(1 for b in bets if b[0] == 'L')
        pnl = sum(b[4] for b in bets)
        wp2 = w / (w + l) * 100 if (w + l) > 0 else 0
        name = sp.replace('soccer_', '').upper()
        print(f'    {name:30s} {w:2d}W-{l:2d}L ({wp2:5.1f}%) | {pnl:+.1f}u')

    print(f'\n  Totals by League:')
    for sp in soccer_leagues:
        bets = [b for b in total_bets if b[2] == sp]
        if not bets: continue
        w = sum(1 for b in bets if b[0] == 'W')
        l = sum(1 for b in bets if b[0] == 'L')
        pnl = sum(b[4] for b in bets)
        wp2 = w / (w + l) * 100 if (w + l) > 0 else 0
        name = sp.replace('soccer_', '').upper()
        print(f'    {name:30s} {w:2d}W-{l:2d}L ({wp2:5.1f}%) | {pnl:+.1f}u')

    conn.close()

if __name__ == '__main__':
    run_backtest(45)
