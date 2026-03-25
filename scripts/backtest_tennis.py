"""
backtest_tennis.py — Tennis-specific backtester for Scotty's Edge

Replays the model's Elo-based predictions against historical tennis results.
Reports by surface, tournament tier, edge bucket, and tour (ATP/WTA).

Tennis differences from team sports:
  - Players, not teams (home/away = player1/player2)
  - Surface-split Elo (hard/clay/grass)
  - No home advantage (neutral venues)
  - ML is the primary market (2-way, no draws)
  - Spreads are in games (not sets)

Usage:
    python backtest_tennis.py                    # All tennis results
    python backtest_tennis.py --tour atp         # ATP only
    python backtest_tennis.py --surface clay     # Clay only
    python backtest_tennis.py --days 90          # Last 90 days
    python backtest_tennis.py --verbose          # Show each bet
"""
import sqlite3, math, os, argparse
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def _ncdf(z):
    if z > 6: return 1.0
    if z < -6: return 0.0
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if z >= 0 else -1
    t = 1.0 / (1.0 + p * abs(z))
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-z * z / 2)
    return 0.5 * (1.0 + sign * y)


def american_to_implied_prob(odds):
    if odds is None:
        return None
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0)


def devig_ml_odds(p1_odds, p2_odds):
    """Remove vig from 2-way ML odds."""
    h = american_to_implied_prob(p1_odds)
    a = american_to_implied_prob(p2_odds)
    if h is None or a is None:
        return None, None
    total = h + a
    if total <= 0:
        return h, a
    return h / total, a / total


def run_backtest(tour=None, surface=None, days_back=None, min_edge=5.0,
                 verbose=False):
    """
    Backtest tennis Elo ML predictions against historical results.

    For each completed match with stored odds:
    1. Look up surface-split Elo for both players
    2. Compute Elo win probability
    3. Compare to de-vigged ML odds
    4. If edge >= min_edge, simulate the bet
    5. Grade against actual result
    """
    from config import TENNIS_SURFACES, TENNIS_LABELS
    from elo_engine import get_elo_ratings, ELO_CONFIG

    conn = sqlite3.connect(DB_PATH)

    # Build filter
    sport_filter = "sport LIKE 'tennis_%'"
    params = []
    if tour:
        sport_filter = f"sport LIKE 'tennis_{tour}_%'"
    if surface:
        surface_keys = [k for k, v in TENNIS_SURFACES.items() if v == surface]
        if surface_keys:
            placeholders = ','.join(['?' for _ in surface_keys])
            sport_filter += f" AND sport IN ({placeholders})"
            params.extend(surface_keys)

    date_filter = ""
    if days_back:
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        date_filter = f" AND commence_time >= '{cutoff}'"

    # Load all completed tennis matches with results
    matches = conn.execute(f"""
        SELECT sport, event_id, commence_time, home, away,
               home_score, away_score, winner, actual_total
        FROM results
        WHERE {sport_filter} AND completed = 1
        AND home_score IS NOT NULL AND away_score IS NOT NULL
        {date_filter}
        ORDER BY commence_time ASC
    """, params).fetchall()

    if not matches:
        print("  No tennis results found. Run historical_scores.py first.")
        conn.close()
        return

    print(f"\n{'='*70}")
    print(f"  TENNIS BACKTEST — {len(matches)} matches")
    if tour:
        print(f"  Tour: {tour.upper()}")
    if surface:
        print(f"  Surface: {surface}")
    print(f"  Min edge: {min_edge}%")
    print(f"{'='*70}\n")

    # Load surface-split Elo ratings
    elo_cache = {}
    for skey in ELO_CONFIG:
        if skey.startswith('tennis_'):
            elo_cache[skey] = get_elo_ratings(conn, skey)

    # Tracking
    bets = []
    by_surface = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0, 'edges': []})
    by_tour = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0})
    by_edge_bucket = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0})

    for sport, eid, commence, p1, p2, p1_sets, p2_sets, winner, total_games in matches:
        # Determine surface and Elo key
        surf = TENNIS_SURFACES.get(sport, 'hard')
        tour_type = 'atp' if '_atp_' in sport else 'wta'
        elo_key = f'tennis_{tour_type}_{surf}'

        elo_data = elo_cache.get(elo_key, {})
        if not elo_data:
            continue

        p1_elo = elo_data.get(p1, {})
        p2_elo = elo_data.get(p2, {})

        if not p1_elo or not p2_elo:
            continue
        if p1_elo.get('confidence') == 'LOW' or p2_elo.get('confidence') == 'LOW':
            continue

        # Compute Elo win probability (no home advantage)
        # Tennis uses scale=150 (less random than team sports)
        elo_diff = p1_elo['elo'] - p2_elo['elo']
        elo_scale = ELO_CONFIG.get(elo_key, {}).get('elo_scale', 400)
        p1_prob = 1.0 / (1.0 + 10 ** (-elo_diff / elo_scale))
        p2_prob = 1.0 - p1_prob

        # Get ML odds — match by player names + date (event IDs differ ESPN vs Odds API)
        match_date = commence[:10] if commence else ''

        # Try market_consensus: match by player names (home/away may be swapped)
        odds_row = conn.execute("""
            SELECT best_home_ml, best_away_ml, home, away
            FROM market_consensus
            WHERE sport = ? AND snapshot_date >= ? AND snapshot_date <= ?
            AND ((home = ? AND away = ?) OR (home = ? AND away = ?))
            ORDER BY snapshot_date DESC LIMIT 1
        """, (sport, match_date, match_date, p1, p2, p2, p1)).fetchone()

        if not odds_row or odds_row[0] is None or odds_row[1] is None:
            # Try closing lines from results table
            cl = conn.execute("""
                SELECT closing_ml_home, closing_ml_away
                FROM results WHERE event_id = ? AND closing_ml_home IS NOT NULL
            """, (eid,)).fetchone()
            if cl and cl[0] is not None:
                odds_row = (cl[0], cl[1], p1, p2)

        if not odds_row or odds_row[0] is None or odds_row[1] is None:
            # Try raw odds table by player names
            odds_row = conn.execute("""
                SELECT
                    MAX(CASE WHEN selection = ? THEN odds END),
                    MAX(CASE WHEN selection = ? THEN odds END)
                FROM odds
                WHERE sport = ? AND market = 'h2h'
                AND snapshot_date >= ? AND snapshot_date <= ?
            """, (p1, p2, sport, match_date, match_date)).fetchone()
            if odds_row and odds_row[0] is not None:
                odds_row = (odds_row[0], odds_row[1], p1, p2)

        if not odds_row or odds_row[0] is None or odds_row[1] is None:
            continue

        # Handle potential name swap (market_consensus home may != results home)
        mc_home = odds_row[2] if len(odds_row) > 2 else p1
        if mc_home == p1:
            p1_ml, p2_ml = odds_row[0], odds_row[1]
        else:
            p1_ml, p2_ml = odds_row[1], odds_row[0]
        p1_fair, p2_fair = devig_ml_odds(p1_ml, p2_ml)
        if p1_fair is None or p2_fair is None:
            continue

        # Check for edges on both sides
        for player, prob, fair, ml, opp in [
            (p1, p1_prob, p1_fair, p1_ml, p2),
            (p2, p2_prob, p2_fair, p2_ml, p1),
        ]:
            edge = (prob - fair) * 100
            if edge < min_edge:
                continue

            # Simulate the bet
            won = (winner == player)
            if ml > 0:
                pnl = ml / 100.0 if won else -1.0
            else:
                pnl = 100.0 / abs(ml) if won else -1.0

            # Kelly sizing
            if ml > 0:
                b = ml / 100.0
            else:
                b = 100.0 / abs(ml)
            p = prob
            full_kelly = (b * p - (1 - p)) / b
            units = max(0.5, min(5.0, full_kelly * 0.125 * 100))
            units = round(units * 2) / 2
            actual_pnl = units * (pnl if won else -1.0) if ml < 0 else units * pnl

            bet = {
                'sport': sport, 'surface': surf, 'tour': tour_type,
                'player': player, 'opponent': opp,
                'edge': round(edge, 1), 'prob': round(prob, 3),
                'fair': round(fair, 3), 'ml': ml,
                'won': won, 'pnl': round(actual_pnl, 2),
                'units': units,
                'tournament': TENNIS_LABELS.get(sport, sport),
                'date': commence[:10] if commence else '',
            }
            bets.append(bet)

            # Aggregate
            result = 'w' if won else 'l'
            by_surface[surf][result] += 1
            by_surface[surf]['pnl'] += actual_pnl
            by_surface[surf]['edges'].append(edge)
            by_tour[tour_type][result] += 1
            by_tour[tour_type]['pnl'] += actual_pnl

            bucket = f"{int(edge // 5) * 5}-{int(edge // 5) * 5 + 5}%"
            by_edge_bucket[bucket][result] += 1
            by_edge_bucket[bucket]['pnl'] += actual_pnl

    conn.close()

    if not bets:
        print("  No qualifying bets found (edge >= {min_edge}%).")
        print("  This likely means no historical odds are stored for tennis.")
        print("  Run: python main.py opener  (during an active tournament)")
        return

    # ── Results ──
    total_w = sum(1 for b in bets if b['won'])
    total_l = len(bets) - total_w
    total_pnl = sum(b['pnl'] for b in bets)
    total_wagered = sum(b['units'] for b in bets)
    roi = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0

    print(f"  OVERALL: {total_w}W-{total_l}L ({total_w/(total_w+total_l)*100:.1f}%)")
    print(f"  PnL: {total_pnl:+.1f}u | Wagered: {total_wagered:.1f}u | ROI: {roi:+.1f}%")

    print(f"\n  BY SURFACE:")
    print(f"  {'Surface':10s} {'W':>4s} {'L':>4s} {'Win%':>6s} {'PnL':>8s} {'Avg Edge':>9s}")
    print(f"  {'─'*45}")
    for surf in ['hard', 'clay', 'grass']:
        s = by_surface[surf]
        w, l = s['w'], s['l']
        if w + l == 0:
            continue
        avg_edge = sum(s['edges']) / len(s['edges']) if s['edges'] else 0
        print(f"  {surf:10s} {w:4d} {l:4d} {w/(w+l)*100:5.1f}% {s['pnl']:+7.1f}u {avg_edge:8.1f}%")

    print(f"\n  BY TOUR:")
    print(f"  {'Tour':10s} {'W':>4s} {'L':>4s} {'Win%':>6s} {'PnL':>8s}")
    print(f"  {'─'*35}")
    for t in ['atp', 'wta']:
        s = by_tour[t]
        w, l = s['w'], s['l']
        if w + l == 0:
            continue
        print(f"  {t.upper():10s} {w:4d} {l:4d} {w/(w+l)*100:5.1f}% {s['pnl']:+7.1f}u")

    print(f"\n  BY EDGE BUCKET:")
    print(f"  {'Bucket':10s} {'W':>4s} {'L':>4s} {'Win%':>6s} {'PnL':>8s}")
    print(f"  {'─'*35}")
    for bucket in sorted(by_edge_bucket.keys()):
        s = by_edge_bucket[bucket]
        w, l = s['w'], s['l']
        if w + l == 0:
            continue
        print(f"  {bucket:10s} {w:4d} {l:4d} {w/(w+l)*100:5.1f}% {s['pnl']:+7.1f}u")

    if verbose:
        print(f"\n  INDIVIDUAL BETS:")
        print(f"  {'Date':12s} {'Tournament':20s} {'Player':25s} {'Edge':>6s} {'ML':>6s} {'Result':>7s} {'PnL':>7s}")
        print(f"  {'─'*90}")
        for b in sorted(bets, key=lambda x: x['date']):
            result = '✅ WIN' if b['won'] else '❌ LOSS'
            print(f"  {b['date']:12s} {b['tournament']:20s} {b['player']:25s} "
                  f"{b['edge']:5.1f}% {b['ml']:+5.0f} {result:>7s} {b['pnl']:+6.1f}u")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Tennis backtest')
    parser.add_argument('--tour', choices=['atp', 'wta'], help='ATP or WTA only')
    parser.add_argument('--surface', choices=['hard', 'clay', 'grass'], help='Surface filter')
    parser.add_argument('--days', type=int, help='Days back to test')
    parser.add_argument('--min-edge', type=float, default=5.0, help='Minimum edge %% (default 5)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show individual bets')
    args = parser.parse_args()

    run_backtest(
        tour=args.tour,
        surface=args.surface,
        days_back=args.days,
        min_edge=args.min_edge,
        verbose=args.verbose,
    )
