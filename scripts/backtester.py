"""
backtester.py — Universal backtest tool for Scotty's Edge

Works for any sport/market combo. Grades historical picks against actual results
using the same model logic as model_engine.py.

Usage:
    python backtester.py                           # all sports, all markets, 45 days
    python backtester.py --sport nba               # NBA only
    python backtester.py --sport all --market SPREAD --days 60
    python backtester.py --min-edge 15 --verbose   # only 15%+ edge, show each bet
"""
import sqlite3, math, os, argparse
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

from model_engine import SPORT_CONFIG

# ── All active sports ──
ALL_SPORTS = [
    'basketball_nba', 'basketball_ncaab', 'icehockey_nhl', 'baseball_ncaa',
    'soccer_epl', 'soccer_germany_bundesliga', 'soccer_italy_serie_a',
    'soccer_spain_la_liga', 'soccer_france_ligue_one', 'soccer_usa_mls',
]

# Shorthand aliases for --sport flag
SPORT_ALIASES = {
    'nba': ['basketball_nba'],
    'ncaab': ['basketball_ncaab'],
    'cbb': ['basketball_ncaab'],
    'nhl': ['icehockey_nhl'],
    'baseball': ['baseball_ncaa'],
    'soccer': [s for s in ALL_SPORTS if s.startswith('soccer_')],
    'epl': ['soccer_epl'],
    'mls': ['soccer_usa_mls'],
    'all': ALL_SPORTS,
}

ALL_MARKETS = ['SPREAD', 'TOTAL', 'MONEYLINE']

# ── Sizing: 5.0u MAX PLAY at -110 ──
UNIT_SIZE = 5.0
WIN_PNL = 4.55   # 5.0 * (100/110)
LOSS_PNL = -5.0


# ── Math helpers (same as model_engine) ──

def _ncdf(z):
    if z > 6: return 1.0
    if z < -6: return 0.0
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if z >= 0 else -1
    t = 1.0 / (1.0 + p * abs(z))
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-z * z / 2)
    return 0.5 * (1.0 + sign * y)


def spread_to_win_prob(spread, sport):
    """Win probability from model spread using ml_scale."""
    s = SPORT_CONFIG.get(sport, SPORT_CONFIG['basketball_nba']).get('ml_scale', 7.5)
    return 1.0 / (1.0 + math.exp(spread / s))


def american_to_implied_prob(odds):
    if odds is None:
        return None
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0)


def devig_ml_odds(home_odds, away_odds):
    """Remove vig from ML odds to get fair probabilities."""
    h_imp = american_to_implied_prob(home_odds)
    a_imp = american_to_implied_prob(away_odds)
    if h_imp is None or a_imp is None:
        return None, None
    total = h_imp + a_imp
    if total <= 0:
        return h_imp, a_imp
    return h_imp / total, a_imp / total


# ── Core backtest logic ──

def load_ratings(conn, sport):
    """Load power ratings and Elo for a sport."""
    pr = {}
    for row in conn.execute('SELECT team, final_rating FROM power_ratings WHERE sport=?', (sport,)):
        pr[row[0]] = row[1]

    elo_data = {}
    for row in conn.execute('SELECT team, elo, games_played, confidence FROM elo_ratings WHERE sport=?', (sport,)):
        elo_data[row[0]] = {'rating': row[1], 'games': row[2], 'confidence': row[3]}

    return pr, elo_data


def build_model_spread(home, away, pr, elo_data, sport):
    """Build blended model spread from power ratings + Elo + HFA."""
    h_pr = pr.get(home)
    a_pr = pr.get(away)
    if h_pr is None or a_pr is None:
        return None

    cfg = SPORT_CONFIG.get(sport, SPORT_CONFIG['basketball_nba'])
    hfa = cfg.get('home_court', 2.5)

    # Power rating spread (negative = home favored)
    ms = (h_pr - a_pr) + hfa

    # Blend with Elo using confidence weighting
    if home in elo_data and away in elo_data:
        h_elo = elo_data[home]['rating']
        a_elo = elo_data[away]['rating']
        elo_spread = (h_elo - a_elo) / 160.0
        games_min = min(elo_data[home].get('games', 0), elo_data[away].get('games', 0))
        elo_w = min(1.0, games_min / 15.0)
        ms = ms * (1 - elo_w * 0.5) + elo_spread * (elo_w * 0.5)

    return ms


def edge_bucket(edge_pct):
    """Categorize edge into buckets."""
    if edge_pct >= 20.0:
        return '20%+'
    elif edge_pct >= 15.0:
        return '15-20%'
    elif edge_pct >= 10.0:
        return '10-15%'
    else:
        return '<10%'


def run_backtest(sports, markets, days_back, min_edge_pct, verbose):
    conn = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

    all_bets = []  # (result, market, sport, description, pnl, edge_pct)

    for sport in sports:
        cfg = SPORT_CONFIG.get(sport, SPORT_CONFIG.get('basketball_nba'))
        spread_std = cfg.get('spread_std', 11.0)

        pr, elo_data = load_ratings(conn, sport)

        games = conn.execute('''
            SELECT event_id, home, away, home_score, away_score, commence_time,
                   actual_total, actual_margin
            FROM results WHERE sport=? AND completed=1 AND commence_time >= ?
            ORDER BY commence_time
        ''', (sport, cutoff)).fetchall()

        for game in games:
            eid, home, away, hs, as_, commence, act_total, act_margin = game

            mc = conn.execute('''
                SELECT best_home_spread, best_away_spread, best_home_spread_odds, best_away_spread_odds,
                       best_over_total, best_under_total, best_over_odds, best_under_odds,
                       best_home_ml, best_away_ml
                FROM market_consensus WHERE event_id=?
                ORDER BY snapshot_date DESC LIMIT 1
            ''', (eid,)).fetchone()
            if not mc:
                continue

            h_spread, a_spread = mc[0], mc[1]
            h_spread_odds, a_spread_odds = mc[2], mc[3]
            total_line, under_total = mc[4], mc[5]
            over_odds, under_odds = mc[6], mc[7]
            home_ml_odds, away_ml_odds = mc[8], mc[9]

            ms = build_model_spread(home, away, pr, elo_data, sport)
            if ms is None:
                continue

            # ── SPREAD ──
            if 'SPREAD' in markets and h_spread is not None and act_margin is not None:
                divergence = ms - h_spread
                # Edge % = divergence / spread_std as cover probability shift
                cover_prob = _ncdf(abs(divergence) / spread_std)
                edge_pct = (cover_prob - 0.5) * 200  # convert to percentage above 50%

                if abs(divergence) >= 0.3 and edge_pct >= min_edge_pct:
                    if divergence < -0.3:
                        # Model says home stronger than market
                        cover = act_margin + h_spread
                        side_desc = f"{home} {h_spread:+.1f}"
                        if cover > 0:
                            all_bets.append(('W', 'SPREAD', sport, side_desc, WIN_PNL, edge_pct, commence))
                        elif cover < 0:
                            all_bets.append(('L', 'SPREAD', sport, side_desc, LOSS_PNL, edge_pct, commence))
                        # push = skip

                    elif divergence > 0.3 and a_spread is not None:
                        # Model says away stronger than market
                        cover = -act_margin + a_spread
                        side_desc = f"{away} {a_spread:+.1f}"
                        if cover > 0:
                            all_bets.append(('W', 'SPREAD', sport, side_desc, WIN_PNL, edge_pct, commence))
                        elif cover < 0:
                            all_bets.append(('L', 'SPREAD', sport, side_desc, LOSS_PNL, edge_pct, commence))

            # ── TOTAL ──
            if 'TOTAL' in markets and total_line is not None and act_total is not None:
                h_avg = conn.execute(
                    'SELECT AVG(home_score + away_score) FROM results WHERE sport=? AND (home=? OR away=?) AND completed=1',
                    (sport, home, home)).fetchone()[0]
                a_avg = conn.execute(
                    'SELECT AVG(home_score + away_score) FROM results WHERE sport=? AND (home=? OR away=?) AND completed=1',
                    (sport, away, away)).fetchone()[0]

                if h_avg and a_avg:
                    model_total = (h_avg + a_avg) / 2
                    diff = model_total - total_line
                    edge_pct = abs(diff) / total_line * 100 if total_line > 0 else 0

                    if abs(diff) >= 0.3 and edge_pct >= min_edge_pct:
                        if diff > 0.3:
                            if act_total > total_line:
                                all_bets.append(('W', 'TOTAL', sport, f"OVER {total_line}", WIN_PNL, edge_pct, commence))
                            elif act_total < total_line:
                                all_bets.append(('L', 'TOTAL', sport, f"OVER {total_line}", LOSS_PNL, edge_pct, commence))
                        else:
                            if act_total < total_line:
                                all_bets.append(('W', 'TOTAL', sport, f"UNDER {total_line}", WIN_PNL, edge_pct, commence))
                            elif act_total > total_line:
                                all_bets.append(('L', 'TOTAL', sport, f"UNDER {total_line}", LOSS_PNL, edge_pct, commence))

            # ── MONEYLINE ──
            if 'MONEYLINE' in markets and home_ml_odds is not None and away_ml_odds is not None and act_margin is not None:
                model_home_wp = spread_to_win_prob(ms, sport)
                model_away_wp = 1.0 - model_home_wp

                market_home_fair, market_away_fair = devig_ml_odds(home_ml_odds, away_ml_odds)
                if market_home_fair is None:
                    continue

                home_edge = (model_home_wp - market_home_fair) / market_home_fair * 100
                away_edge = (model_away_wp - market_away_fair) / market_away_fair * 100

                # Take the side with bigger positive edge
                if home_edge > away_edge and home_edge >= min_edge_pct:
                    home_won = act_margin > 0
                    # Payout based on actual ML odds
                    if home_ml_odds > 0:
                        ml_win_pnl = UNIT_SIZE * (home_ml_odds / 100.0)
                    else:
                        ml_win_pnl = UNIT_SIZE * (100.0 / abs(home_ml_odds))
                    if home_won:
                        all_bets.append(('W', 'MONEYLINE', sport, f"{home} ML ({home_ml_odds:+.0f})", ml_win_pnl, home_edge, commence))
                    elif act_margin < 0:
                        all_bets.append(('L', 'MONEYLINE', sport, f"{home} ML ({home_ml_odds:+.0f})", LOSS_PNL, home_edge, commence))
                    # push (margin=0) skipped

                elif away_edge >= min_edge_pct:
                    away_won = act_margin < 0
                    if away_ml_odds > 0:
                        ml_win_pnl = UNIT_SIZE * (away_ml_odds / 100.0)
                    else:
                        ml_win_pnl = UNIT_SIZE * (100.0 / abs(away_ml_odds))
                    if away_won:
                        all_bets.append(('W', 'MONEYLINE', sport, f"{away} ML ({away_ml_odds:+.0f})", ml_win_pnl, away_edge, commence))
                    elif act_margin > 0:
                        all_bets.append(('L', 'MONEYLINE', sport, f"{away} ML ({away_ml_odds:+.0f})", LOSS_PNL, away_edge, commence))

    conn.close()

    # ── Print report ──
    print_report(all_bets, sports, markets, days_back, min_edge_pct, verbose)


def _record_line(bets):
    """Build a record string from a list of bets."""
    w = sum(1 for b in bets if b[0] == 'W')
    l = sum(1 for b in bets if b[0] == 'L')
    pnl = sum(b[4] for b in bets)
    wagered = (w + l) * UNIT_SIZE
    wp = w / (w + l) * 100 if (w + l) > 0 else 0
    roi = pnl / wagered * 100 if wagered > 0 else 0
    return f"{w}W-{l}L ({wp:5.1f}%) | {pnl:+.1f}u | ROI {roi:+.1f}%"


def _short_sport(sport):
    """Readable sport name."""
    aliases = {
        'basketball_nba': 'NBA',
        'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL',
        'baseball_ncaa': 'Baseball',
        'soccer_epl': 'EPL',
        'soccer_germany_bundesliga': 'Bundesliga',
        'soccer_italy_serie_a': 'Serie A',
        'soccer_spain_la_liga': 'La Liga',
        'soccer_france_ligue_one': 'Ligue 1',
        'soccer_usa_mls': 'MLS',
    }
    return aliases.get(sport, sport)


def print_report(all_bets, sports, markets, days_back, min_edge_pct, verbose):
    print()
    print('=' * 70)
    print(f"  SCOTTY'S EDGE BACKTEST — Last {days_back} Days")
    if min_edge_pct > 0:
        print(f"  Min Edge Filter: {min_edge_pct}%")
    print(f"  Sports: {', '.join(_short_sport(s) for s in sports)}")
    print(f"  Markets: {', '.join(markets)}")
    print('=' * 70)

    if not all_bets:
        print("  No bets found matching criteria.")
        print('=' * 70)
        return

    # ── Overall ──
    print(f"\n  OVERALL:  {_record_line(all_bets)}")
    print(f"  Total bets: {len(all_bets)}")

    # ── By Sport ──
    sport_groups = {}
    for b in all_bets:
        sport_groups.setdefault(b[2], []).append(b)

    if len(sport_groups) > 1:
        print(f"\n  {'BY SPORT':─^66}")
        for sport in sorted(sport_groups, key=lambda s: sum(b[4] for b in sport_groups[s]), reverse=True):
            bets = sport_groups[sport]
            print(f"    {_short_sport(sport):20s} {_record_line(bets)}")

    # ── By Market ──
    market_groups = {}
    for b in all_bets:
        market_groups.setdefault(b[1], []).append(b)

    if len(market_groups) > 1:
        print(f"\n  {'BY MARKET':─^66}")
        for mkt in sorted(market_groups, key=lambda m: sum(b[4] for b in market_groups[m]), reverse=True):
            bets = market_groups[mkt]
            print(f"    {mkt:20s} {_record_line(bets)}")

    # ── By Sport + Market ──
    combo_groups = {}
    for b in all_bets:
        key = (b[2], b[1])
        combo_groups.setdefault(key, []).append(b)

    if len(combo_groups) > 2:
        print(f"\n  {'BY SPORT + MARKET':─^66}")
        for (sport, mkt) in sorted(combo_groups, key=lambda k: sum(b[4] for b in combo_groups[k]), reverse=True):
            bets = combo_groups[(sport, mkt)]
            label = f"{_short_sport(sport)} {mkt}"
            print(f"    {label:25s} {_record_line(bets)}")

    # ── By Edge Bucket ──
    bucket_groups = {}
    for b in all_bets:
        bkt = edge_bucket(b[5])
        bucket_groups.setdefault(bkt, []).append(b)

    bucket_order = ['<10%', '10-15%', '15-20%', '20%+']
    print(f"\n  {'BY EDGE BUCKET':─^66}")
    for bkt in bucket_order:
        if bkt in bucket_groups:
            bets = bucket_groups[bkt]
            print(f"    {bkt:20s} {_record_line(bets)}")

    # ── Best / Worst combos ──
    if len(combo_groups) >= 3:
        sorted_combos = sorted(combo_groups.items(), key=lambda kv: sum(b[4] for b in kv[1]), reverse=True)
        print(f"\n  {'TOP 3 COMBOS':─^66}")
        for (sport, mkt), bets in sorted_combos[:3]:
            label = f"{_short_sport(sport)} {mkt}"
            print(f"    {label:25s} {_record_line(bets)}")

        print(f"\n  {'BOTTOM 3 COMBOS':─^66}")
        for (sport, mkt), bets in sorted_combos[-3:]:
            label = f"{_short_sport(sport)} {mkt}"
            print(f"    {label:25s} {_record_line(bets)}")

    # ── Verbose: individual bets ──
    if verbose:
        print(f"\n  {'ALL BETS':─^66}")
        sorted_bets = sorted(all_bets, key=lambda b: b[6] or '')
        for b in sorted_bets:
            result, mkt, sport, desc, pnl, epct, dt = b
            marker = 'W' if result == 'W' else 'X'
            print(f"    [{marker}] {_short_sport(sport):10s} {mkt:10s} {desc:35s} {pnl:+.1f}u  edge {epct:.1f}%  {dt or ''}")

    print()
    print('=' * 70)


def main():
    parser = argparse.ArgumentParser(description="Scotty's Edge Universal Backtester")
    parser.add_argument('--sport', type=str, default='all',
                        help='Sport key or alias (nba, ncaab, nhl, baseball, soccer, epl, mls, all)')
    parser.add_argument('--market', type=str, default='SPREAD,TOTAL,MONEYLINE',
                        help='Comma-separated markets: SPREAD,TOTAL,MONEYLINE')
    parser.add_argument('--days', type=int, default=45, help='Days to look back (default: 45)')
    parser.add_argument('--min-edge', type=float, default=0, help='Minimum edge %% to count a pick')
    parser.add_argument('--verbose', action='store_true', help='Show each individual bet')

    args = parser.parse_args()

    # Resolve sport
    sport_key = args.sport.lower().strip()
    if sport_key in SPORT_ALIASES:
        sports = SPORT_ALIASES[sport_key]
    elif sport_key in ALL_SPORTS:
        sports = [sport_key]
    else:
        print(f"Unknown sport: {args.sport}")
        print(f"Available: {', '.join(list(SPORT_ALIASES.keys()) + ALL_SPORTS)}")
        return

    # Resolve markets
    markets = [m.strip().upper() for m in args.market.split(',')]
    for m in markets:
        if m not in ALL_MARKETS:
            print(f"Unknown market: {m}. Available: {', '.join(ALL_MARKETS)}")
            return

    run_backtest(sports, markets, args.days, args.min_edge, args.verbose)


if __name__ == '__main__':
    main()
