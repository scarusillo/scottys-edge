"""
backtest_prop_model.py — Backtest the player prop projection model

Replays prop_snapshots day by day:
  1. For each day, use box_scores available BEFORE that day as training data
  2. Project each player's stats using the model
  3. Compare projections to the prop lines from that day
  4. Grade against actual box score results

Usage:
    python backtest_prop_model.py                  # Full backtest
    python backtest_prop_model.py --days 45        # Last 45 days
    python backtest_prop_model.py --sport nba      # NBA only
"""
import sqlite3, os, sys, math
from datetime import datetime, timedelta, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

from player_prop_model import (
    get_player_baseline, get_opponent_defense, get_player_context,
    calculate_prop_edge, _ncdf, _get_player_team, _match_team,
    MIN_EDGE_PCT, MIN_STARS, DEFAULT_STD, DECAY_RATE, MIN_PLAYER_GAMES,
)
from props_engine import (
    american_to_implied, STAT_TYPE_MAP, PROP_LABEL,
    EXCLUDED_BOOKS, NY_LEGAL_BOOKS,
)
from scottys_edge import get_star_rating, kelly_units
from box_scores import lookup_player_stat, PROP_TO_STAT


def backtest_prop_model(days=45, sport_filter=None, min_edge=None):
    if min_edge is None:
        min_edge = MIN_EDGE_PCT

    conn = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    date_rows = conn.execute("""
        SELECT DISTINCT DATE(captured_at) as d FROM prop_snapshots
        WHERE d >= ? ORDER BY d
    """, (cutoff,)).fetchall()

    if not date_rows:
        print("No prop snapshot data found.")
        return

    print("=" * 70)
    print(f"  PROP MODEL BACKTEST — {len(date_rows)} days")
    print(f"  Edge threshold: {min_edge}% | Kelly: 1/4 (0.25)")
    print("=" * 70)

    all_picks = []
    daily_summary = []

    for (day_str,) in date_rows:
        # Get prop snapshots for this day
        sport_clause = f"AND sport = '{sport_filter}'" if sport_filter else ""
        rows = conn.execute(f"""
            SELECT sport, event_id, commence_time, home, away,
                   book, market, player, side, line, odds
            FROM prop_snapshots
            WHERE DATE(captured_at) = ?
            {sport_clause}
        """, (day_str,)).fetchall()

        if not rows:
            continue

        # Group by (event_id, player, market) — collect OVER lines
        grouped = defaultdict(list)
        game_info = {}
        for sport, eid, commence, home, away, book, market, player, side, line_val, odds in rows:
            if side != 'Over':
                continue  # OVER only
            game_info[eid] = {'sport': sport, 'home': home, 'away': away, 'commence': commence}
            grouped[(eid, player, market)].append({
                'book': book, 'line': line_val, 'odds': odds,
            })

        day_picks = []
        _team_cache = {}

        for (eid, player, market), entries in grouped.items():
            gi = game_info.get(eid)
            if not gi:
                continue

            sport = gi['sport']
            home = gi['home']
            away = gi['away']
            commence = gi['commence']

            stat_type = STAT_TYPE_MAP.get(market)
            if not stat_type:
                continue

            # Get player team
            cache_key = (player, sport)
            if cache_key not in _team_cache:
                _team_cache[cache_key] = _get_player_team(conn, player, sport)
            player_team_raw = _team_cache[cache_key]
            if not player_team_raw:
                continue

            player_team = _match_team(player_team_raw, home, away)
            if not player_team:
                continue
            opponent = away if player_team == home else home

            # Get baseline
            baseline = get_player_baseline(conn, player, stat_type, sport)
            if not baseline:
                continue

            # Get opponent defense
            opp_def = get_opponent_defense(conn, opponent, stat_type, sport)
            opp_mult = opp_def['multiplier'] if opp_def else 1.0

            # Get context
            ctx = get_player_context(conn, player_team, home, away, sport, commence)
            ctx_mult = ctx['combined_mult']

            projection = max(0.0, baseline['avg'] * opp_mult * ctx_mult)
            std = baseline['std']

            # Find best legal book OVER line
            legal = [e for e in entries
                     if e['book'] not in EXCLUDED_BOOKS and e['book'] in NY_LEGAL_BOOKS]
            if not legal:
                continue

            # Use the best odds available
            best = max(legal, key=lambda x: x['odds'])
            line_val = best['line']
            odds = best['odds']
            book = best['book']

            edge = calculate_prop_edge(projection, std, line_val, odds)
            if edge < min_edge:
                continue

            stars = get_star_rating(edge)
            if stars < MIN_STARS:
                continue

            units = kelly_units(edge_pct=edge, odds=odds, fraction=0.25)
            label = PROP_LABEL.get(market, market.replace('player_', '').upper())

            # Grade against actual box score
            stat_key = PROP_TO_STAT.get(label.upper(), stat_type)
            actual = lookup_player_stat(conn, player, stat_key, day_str, sport=sport)
            if actual is None:
                continue

            if actual > line_val:
                result = 'WIN'
            elif actual < line_val:
                result = 'LOSS'
            else:
                result = 'PUSH'

            dec = odds / 100.0 if odds > 0 else 100.0 / abs(odds)
            pnl = units * dec if result == 'WIN' else -units if result == 'LOSS' else 0

            day_picks.append({
                'date': day_str, 'sport': sport, 'player': player,
                'label': label, 'side': 'OVER', 'line': line_val,
                'actual': actual, 'projection': projection,
                'book': book, 'odds': odds, 'edge': edge,
                'units': units, 'result': result, 'pnl': pnl,
                'opp_mult': opp_mult, 'ctx_mult': ctx_mult,
                'std': std, 'games': baseline['games'],
                'home': home, 'away': away,
            })

        # Dedup: best edge per player+stat per event
        seen = set()
        deduped = []
        day_picks.sort(key=lambda x: x['edge'], reverse=True)
        for p in day_picks:
            dk = f"{p['date']}|{p['player']}|{p['label']}"
            if dk in seen:
                continue
            seen.add(dk)
            deduped.append(p)

        # Per-game cap: max 3 props per game
        game_counts = {}
        capped = []
        for p in deduped:
            gk = f"{p['date']}|{p['home']}|{p['away']}"
            if game_counts.get(gk, 0) >= 3:
                continue
            game_counts[gk] = game_counts.get(gk, 0) + 1
            capped.append(p)

        all_picks.extend(capped)

        # Daily summary
        graded = [p for p in capped if p['result'] in ('WIN', 'LOSS')]
        if graded:
            dw = sum(1 for p in graded if p['result'] == 'WIN')
            dl = sum(1 for p in graded if p['result'] == 'LOSS')
            dpnl = sum(p['pnl'] for p in graded)
            daily_summary.append((day_str, dw, dl, dpnl))

    conn.close()

    # ═══ RESULTS ═══
    graded = [p for p in all_picks if p['result'] in ('WIN', 'LOSS')]
    w = sum(1 for p in graded if p['result'] == 'WIN')
    l = sum(1 for p in graded if p['result'] == 'LOSS')
    pnl = sum(p['pnl'] for p in graded)
    wag = sum(p['units'] for p in graded)
    roi = pnl / wag * 100 if wag > 0 else 0
    wp = w / (w + l) * 100 if (w + l) > 0 else 0

    report = []
    report.append("")
    report.append("=" * 70)
    report.append(f"  PLAYER PROP MODEL BACKTEST — {len(date_rows)} days")
    report.append(f"  Min Edge: {min_edge}% | Kelly: 1/4 | Per-game cap: 3")
    report.append("=" * 70)
    report.append(f"  Record: {w}W-{l}L ({wp:.1f}%)")
    report.append(f"  P&L: {pnl:+.1f}u | Wagered: {wag:.1f}u | ROI: {roi:+.1f}%")
    report.append("")

    # By sport
    report.append("  ── BY SPORT ──")
    sport_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0, 'wag': 0})
    for p in graded:
        sp = p['sport']
        if p['result'] == 'WIN':
            sport_stats[sp]['w'] += 1; sport_stats[sp]['pnl'] += p['pnl']; sport_stats[sp]['wag'] += p['units']
        else:
            sport_stats[sp]['l'] += 1; sport_stats[sp]['pnl'] += p['pnl']; sport_stats[sp]['wag'] += p['units']
    for sp, s in sorted(sport_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
        swp = s['w'] / (s['w'] + s['l']) * 100 if (s['w'] + s['l']) > 0 else 0
        sr = s['pnl'] / s['wag'] * 100 if s['wag'] > 0 else 0
        report.append(f"    {sp:30} {s['w']}W-{s['l']}L ({swp:.0f}%) | {s['pnl']:+.1f}u | ROI {sr:+.1f}%")

    # By market
    report.append("")
    report.append("  ── BY MARKET ──")
    mkt_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0, 'wag': 0})
    for p in graded:
        mk = p['label']
        if p['result'] == 'WIN':
            mkt_stats[mk]['w'] += 1; mkt_stats[mk]['pnl'] += p['pnl']; mkt_stats[mk]['wag'] += p['units']
        else:
            mkt_stats[mk]['l'] += 1; mkt_stats[mk]['pnl'] += p['pnl']; mkt_stats[mk]['wag'] += p['units']
    for mk, s in sorted(mkt_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
        swp = s['w'] / (s['w'] + s['l']) * 100 if (s['w'] + s['l']) > 0 else 0
        sr = s['pnl'] / s['wag'] * 100 if s['wag'] > 0 else 0
        report.append(f"    {mk:15} {s['w']}W-{s['l']}L ({swp:.0f}%) | {s['pnl']:+.1f}u | ROI {sr:+.1f}%")

    # By edge bucket
    report.append("")
    report.append("  ── BY EDGE BUCKET ──")
    for lo, hi, label in [(6, 10, '6-10%'), (10, 15, '10-15%'), (15, 20, '15-20%'), (20, 30, '20-30%'), (30, 100, '30%+')]:
        br = [p for p in graded if lo <= p['edge'] < hi]
        if not br:
            continue
        bw = sum(1 for p in br if p['result'] == 'WIN')
        bl = sum(1 for p in br if p['result'] == 'LOSS')
        bp = sum(p['pnl'] for p in br)
        bwg = sum(p['units'] for p in br)
        bwp = bw / (bw + bl) * 100 if (bw + bl) > 0 else 0
        broi = bp / bwg * 100 if bwg > 0 else 0
        report.append(f"    Edge {label:8} {bw}W-{bl}L ({bwp:.0f}%) | {bp:+.1f}u | ROI {broi:+.1f}%")

    # By book
    report.append("")
    report.append("  ── BY BOOK ──")
    bk_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0})
    for p in graded:
        b = p['book']
        if p['result'] == 'WIN':
            bk_stats[b]['w'] += 1; bk_stats[b]['pnl'] += p['pnl']
        else:
            bk_stats[b]['l'] += 1; bk_stats[b]['pnl'] += p['pnl']
    for b, s in sorted(bk_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
        bwp = s['w'] / (s['w'] + s['l']) * 100 if (s['w'] + s['l']) > 0 else 0
        report.append(f"    {b:20} {s['w']}W-{s['l']}L ({bwp:.0f}%) | {s['pnl']:+.1f}u")

    # Daily breakdown
    if daily_summary:
        report.append("")
        report.append("  ── DAILY BREAKDOWN ──")
        for day_str, dw, dl, dpnl in daily_summary:
            icon = '+' if dpnl >= 0 else '-'
            bar = '#' * max(1, int(abs(dpnl) / 2))
            report.append(f"    {day_str}  {dw}W-{dl}L  {dpnl:+.1f}u  {'G' if dpnl >= 0 else 'R'} {bar}")

    # Top wins
    wins = [p for p in graded if p['result'] == 'WIN']
    if wins:
        report.append("")
        report.append("  ── TOP 10 WINS ──")
        for p in sorted(wins, key=lambda x: x['pnl'], reverse=True)[:10]:
            report.append(f"    +{p['pnl']:.1f}u  {p['player']} OVER {p['line']} {p['label']} "
                          f"(proj={p['projection']:.1f} actual={p['actual']}) "
                          f"@ {p['book']} [{p['sport']}] edge={p['edge']:.1f}%")

    # Worst losses
    losses = [p for p in graded if p['result'] == 'LOSS']
    if losses:
        report.append("")
        report.append("  ── WORST 10 LOSSES ──")
        for p in sorted(losses, key=lambda x: x['pnl'])[:10]:
            report.append(f"    {p['pnl']:.1f}u  {p['player']} OVER {p['line']} {p['label']} "
                          f"(proj={p['projection']:.1f} actual={p['actual']}) "
                          f"@ {p['book']} [{p['sport']}] edge={p['edge']:.1f}%")

    # Projection accuracy
    if graded:
        report.append("")
        report.append("  ── PROJECTION ACCURACY ──")
        errors = [abs(p['projection'] - p['actual']) for p in graded]
        avg_err = sum(errors) / len(errors)
        # How often does the projection correctly predict over/under the line?
        correct_direction = sum(1 for p in graded
                                if (p['projection'] > p['line'] and p['actual'] > p['line'])
                                or (p['projection'] < p['line'] and p['actual'] < p['line']))
        dir_pct = correct_direction / len(graded) * 100
        report.append(f"    Avg absolute error: {avg_err:.1f}")
        report.append(f"    Direction accuracy: {correct_direction}/{len(graded)} ({dir_pct:.1f}%)")

    report.append("")
    report.append("=" * 70)

    output = "\n".join(report)
    print(output)
    return output


if __name__ == '__main__':
    d = 45
    sport = None
    me = None
    for i, arg in enumerate(sys.argv):
        if arg == '--days' and i + 1 < len(sys.argv):
            d = int(sys.argv[i + 1])
        if arg == '--sport' and i + 1 < len(sys.argv):
            sport = sys.argv[i + 1]
            sport_map = {'nba': 'basketball_nba', 'ncaab': 'basketball_ncaab', 'nhl': 'icehockey_nhl'}
            sport = sport_map.get(sport, sport)
        if arg == '--min-edge' and i + 1 < len(sys.argv):
            me = float(sys.argv[i + 1])

    backtest_prop_model(days=d, sport_filter=sport, min_edge=me)
