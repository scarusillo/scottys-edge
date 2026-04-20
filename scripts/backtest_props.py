"""
backtest_props.py — Replay prop snapshots to measure consensus edge accuracy

For each day with prop snapshot data:
  1. Replay the consensus engine on that day's snapshots
  2. Find edges (same logic as live props_engine)
  3. Grade against ESPN box scores (actual player stats)
  4. Track P&L at 1/4 Kelly sizing

Usage:
    python backtest_props.py                  # Full backtest
    python backtest_props.py --days 45        # Last 45 days
    python backtest_props.py --sport nba      # NBA only
"""
import sqlite3, os, sys, math, re
from datetime import datetime, timedelta, timezone
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Import consensus logic
from props_engine import (
    compute_consensus, find_consensus_edges, detect_stale_lines,
    american_to_implied, remove_vig, median, MIN_BOOKS_FOR_CONSENSUS,
    PROP_LABEL, EXCLUDED_BOOKS, NY_LEGAL_BOOKS, STAT_TYPE_MAP,
)
from scottys_edge import kelly_units, get_star_rating
from box_scores import lookup_player_stat, PROP_TO_STAT

# ─── Filters (mirror main.py _merge_and_select) ───
PROP_MIN_UNITS = 2.0
PROP_MIN_EDGE = 8.0
PROP_MIN_EDGE_THREES = 12.0
LOW_LINE_MARKETS = {'player_threes', 'player_shots_on_goal', 'player_power_play_points',
                    'player_blocked_shots', 'player_blocks', 'player_steals',
                    'player_shots', 'player_shots_on_target'}
MAX_PROPS_PER_GAME = 3
MAX_SAME_STAT_PER_GAME = 2
MIN_UNITS_FOR_CARD = 4.5  # MAX PLAY threshold


def american_to_decimal(odds):
    if odds > 0:
        return odds / 100.0
    return 100.0 / abs(odds)


def backtest_props(days=45, sport_filter=None):
    conn = sqlite3.connect(DB_PATH)

    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    # Get all distinct dates with snapshots
    date_rows = conn.execute("""
        SELECT DISTINCT DATE(captured_at) as d FROM prop_snapshots_all
        WHERE d >= ? ORDER BY d
    """, (cutoff,)).fetchall()

    if not date_rows:
        print("No prop snapshot data found.")
        return

    print("=" * 70)
    print(f"  PROP BACKTEST — {len(date_rows)} days ({date_rows[0][0]} to {date_rows[-1][0]})")
    print(f"  Kelly: 1/4 (0.25) | MIN_EDGE: {PROP_MIN_EDGE}% | MIN_UNITS: {MIN_UNITS_FOR_CARD}u")
    print("=" * 70)

    all_results = []
    daily_summary = []

    for (day_str,) in date_rows:
        # Get all snapshots for this day
        sport_clause = "AND sport = ?" if sport_filter else ""
        params = (day_str, sport_filter) if sport_filter else (day_str,)
        rows = conn.execute(f"""
            SELECT sport, event_id, commence_time, home, away,
                   book, market, player, side, line, odds, implied_prob
            FROM prop_snapshots_all
            WHERE DATE(captured_at) = ?
            AND sport IN ('basketball_nba', 'basketball_ncaab', 'icehockey_nhl')
            {sport_clause}
        """, params).fetchall()

        if not rows:
            continue

        # Group by (event_id, player, market, line_value)
        groups = defaultdict(list)
        game_info = {}
        for r in rows:
            sport, eid, commence, home, away, book, market, player, side, line_val, odds, imp = r
            key = (eid, player, market, line_val)
            groups[key].append({
                'book': book, 'line': line_val, 'side': side,
                'odds': odds, 'implied_prob': imp,
            })
            game_info[eid] = {'sport': sport, 'commence': commence, 'home': home, 'away': away}

        # Build consensus for each group and find edges
        day_picks = []
        for (eid, player, market, line_val), lines in groups.items():
            gi = game_info.get(eid, {})

            # Build over/under pairs per book
            book_lines = defaultdict(lambda: {'book': None, 'line': line_val})
            for entry in lines:
                bk = entry['book']
                book_lines[bk]['book'] = bk
                book_lines[bk]['line'] = entry['line']
                if entry['side'] == 'Over':
                    book_lines[bk]['over_odds'] = entry['odds']
                else:
                    book_lines[bk]['under_odds'] = entry['odds']

            bl_list = [v for v in book_lines.values() if v.get('over_odds') and v.get('under_odds')]
            if len(bl_list) < MIN_BOOKS_FOR_CONSENSUS:
                continue

            consensus = compute_consensus(bl_list)
            if not consensus:
                continue

            edges = find_consensus_edges(consensus, min_edge=3.0)
            stale = detect_stale_lines(consensus, gi.get('sport', ''))

            for ce in edges:
                book = ce['book']
                side = ce['side']
                base_edge = ce['edge_pct']

                if book in EXCLUDED_BOOKS:
                    continue

                # Movement and stale bonuses (simplified — no opener data in backtest)
                stale_bonus = 0.0
                if book in stale:
                    si = stale[book]
                    if side == 'OVER' and si['book_line'] < si['consensus_line']:
                        stale_bonus = min(10.0, si['stale_amount'] * 3.0)
                    elif side == 'UNDER' and si['book_line'] > si['consensus_line']:
                        stale_bonus = min(10.0, si['stale_amount'] * 3.0)

                final_edge = base_edge + min(stale_bonus * 0.20, 4.0)

                if final_edge < 5.5:
                    continue

                stars = get_star_rating(final_edge)
                if stars < 2.0:
                    continue

                label = PROP_LABEL.get(market, market.replace('player_', '').upper())
                units = kelly_units(edge_pct=final_edge, odds=ce['odds'], fraction=0.25)

                # Apply same filters as main.py
                if units < PROP_MIN_UNITS:
                    continue
                min_edge_req = PROP_MIN_EDGE_THREES if market in LOW_LINE_MARKETS else PROP_MIN_EDGE
                if final_edge < min_edge_req:
                    continue

                day_picks.append({
                    'date': day_str,
                    'sport': gi.get('sport', ''),
                    'event_id': eid,
                    'player': player,
                    'market': market,
                    'label': label,
                    'side': side,
                    'line': line_val,
                    'book': book,
                    'odds': ce['odds'],
                    'edge_pct': final_edge,
                    'units': units,
                    'stars': stars,
                    'home': gi.get('home', ''),
                    'away': gi.get('away', ''),
                })

        # Dedup: same player + stat + game = keep best
        seen = set()
        deduped = []
        day_picks.sort(key=lambda x: x['edge_pct'], reverse=True)
        for p in day_picks:
            dk = f"{p['event_id']}|{p['player']}|{p['market']}"
            if dk in seen:
                continue
            seen.add(dk)
            deduped.append(p)

        # Per-game cap
        game_counts = {}
        capped = []
        for p in deduped:
            eid = p['event_id']
            if game_counts.get(eid, 0) >= MAX_PROPS_PER_GAME:
                continue
            game_counts[eid] = game_counts.get(eid, 0) + 1
            capped.append(p)

        # Per-stat-type cap per game
        stat_counts = {}
        final_picks = []
        for p in capped:
            sk = f"{p['event_id']}|{p['market']}"
            if stat_counts.get(sk, 0) >= MAX_SAME_STAT_PER_GAME:
                continue
            stat_counts[sk] = stat_counts.get(sk, 0) + 1
            final_picks.append(p)

        # Grade against box scores
        day_w, day_l, day_pnl, day_graded = 0, 0, 0.0, 0
        for p in final_picks:
            stat_type_key = PROP_TO_STAT.get(p['label'].upper(), p['label'].lower())
            # Also try the market key directly
            if stat_type_key == p['label'].lower():
                stat_type_key = STAT_TYPE_MAP.get(p['market'], p['label'].lower())

            actual = lookup_player_stat(conn, p['player'], stat_type_key, p['date'], sport=p['sport'])

            if actual is None:
                p['result'] = 'NO_DATA'
                continue

            day_graded += 1
            p['actual'] = actual

            if p['side'] == 'OVER':
                if actual > p['line']:
                    p['result'] = 'WIN'
                elif actual < p['line']:
                    p['result'] = 'LOSS'
                else:
                    p['result'] = 'PUSH'
            else:
                if actual < p['line']:
                    p['result'] = 'WIN'
                elif actual > p['line']:
                    p['result'] = 'LOSS'
                else:
                    p['result'] = 'PUSH'

            if p['result'] == 'WIN':
                payout = p['units'] * american_to_decimal(p['odds'])
                p['pnl'] = round(payout, 2)
                day_w += 1
                day_pnl += payout
            elif p['result'] == 'LOSS':
                p['pnl'] = -p['units']
                day_l += 1
                day_pnl -= p['units']
            else:
                p['pnl'] = 0.0

            # Only track MAX PLAY qualifying picks
            if p['units'] >= MIN_UNITS_FOR_CARD:
                all_results.append(p)

        if day_graded > 0:
            # Count only MAX PLAY picks for daily summary
            max_play_picks = [p for p in final_picks if p.get('result') in ('WIN','LOSS','PUSH') and p['units'] >= MIN_UNITS_FOR_CARD]
            mp_w = sum(1 for p in max_play_picks if p['result'] == 'WIN')
            mp_l = sum(1 for p in max_play_picks if p['result'] == 'LOSS')
            mp_pnl = sum(p.get('pnl', 0) for p in max_play_picks)
            if max_play_picks:
                daily_summary.append((day_str, mp_w, mp_l, mp_pnl, len(max_play_picks)))

    conn.close()

    # ── RESULTS ──
    if not all_results:
        print("\n  No graded prop picks found (need box score data).")
        print(f"  Total picks generated across all days: check box_scores coverage.")
        # Show stats on all picks without grading filter
        print("\n  Generating ungraded summary for reference...")
        return _generate_report([], daily_summary, days, sport_filter)

    return _generate_report(all_results, daily_summary, days, sport_filter)


def _generate_report(all_results, daily_summary, days, sport_filter):
    wins = [r for r in all_results if r['result'] == 'WIN']
    losses = [r for r in all_results if r['result'] == 'LOSS']
    pushes = [r for r in all_results if r['result'] == 'PUSH']

    total_pnl = sum(r.get('pnl', 0) for r in all_results)
    total_wagered = sum(r['units'] for r in all_results if r['result'] in ('WIN', 'LOSS'))
    roi = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0
    win_pct = len(wins) / (len(wins) + len(losses)) * 100 if (len(wins) + len(losses)) > 0 else 0

    report = []
    report.append("")
    report.append("=" * 70)
    report.append(f"  PROP BACKTEST RESULTS — MAX PLAY PICKS ONLY (>= {MIN_UNITS_FOR_CARD}u)")
    report.append("=" * 70)
    report.append(f"  Record: {len(wins)}W-{len(losses)}L-{len(pushes)}P ({win_pct:.1f}%)")
    report.append(f"  P&L: {total_pnl:+.1f}u | Wagered: {total_wagered:.1f}u | ROI: {roi:+.1f}%")
    report.append(f"  Kelly: 1/4 (0.25) | Min Edge: {PROP_MIN_EDGE}%")
    report.append("")

    # By sport
    report.append("  ── BY SPORT ──")
    sport_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0, 'wager': 0.0})
    for r in all_results:
        sp = r['sport']
        if r['result'] == 'WIN':
            sport_stats[sp]['w'] += 1
            sport_stats[sp]['pnl'] += r['pnl']
            sport_stats[sp]['wager'] += r['units']
        elif r['result'] == 'LOSS':
            sport_stats[sp]['l'] += 1
            sport_stats[sp]['pnl'] += r['pnl']
            sport_stats[sp]['wager'] += r['units']
    for sp, s in sorted(sport_stats.items()):
        wp = s['w'] / (s['w'] + s['l']) * 100 if (s['w'] + s['l']) > 0 else 0
        sr = s['pnl'] / s['wager'] * 100 if s['wager'] > 0 else 0
        report.append(f"    {sp:30} {s['w']}W-{s['l']}L ({wp:.0f}%) | {s['pnl']:+.1f}u | ROI {sr:+.1f}%")

    # By market type
    report.append("")
    report.append("  ── BY MARKET TYPE ──")
    market_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0, 'wager': 0.0})
    for r in all_results:
        mk = r['label']
        if r['result'] == 'WIN':
            market_stats[mk]['w'] += 1
            market_stats[mk]['pnl'] += r['pnl']
            market_stats[mk]['wager'] += r['units']
        elif r['result'] == 'LOSS':
            market_stats[mk]['l'] += 1
            market_stats[mk]['pnl'] += r['pnl']
            market_stats[mk]['wager'] += r['units']
    for mk, s in sorted(market_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
        wp = s['w'] / (s['w'] + s['l']) * 100 if (s['w'] + s['l']) > 0 else 0
        sr = s['pnl'] / s['wager'] * 100 if s['wager'] > 0 else 0
        report.append(f"    {mk:20} {s['w']}W-{s['l']}L ({wp:.0f}%) | {s['pnl']:+.1f}u | ROI {sr:+.1f}%")

    # By side (OVER vs UNDER)
    report.append("")
    report.append("  ── BY SIDE ──")
    for side in ('OVER', 'UNDER'):
        sr = [r for r in all_results if r['side'] == side and r['result'] in ('WIN', 'LOSS')]
        sw = sum(1 for r in sr if r['result'] == 'WIN')
        sl = sum(1 for r in sr if r['result'] == 'LOSS')
        sp = sum(r.get('pnl', 0) for r in sr)
        swg = sum(r['units'] for r in sr)
        wp = sw / (sw + sl) * 100 if (sw + sl) > 0 else 0
        sroi = sp / swg * 100 if swg > 0 else 0
        report.append(f"    {side:20} {sw}W-{sl}L ({wp:.0f}%) | {sp:+.1f}u | ROI {sroi:+.1f}%")

    # By edge bucket
    report.append("")
    report.append("  ── BY EDGE BUCKET ──")
    buckets = [(8, 12, '8-12%'), (12, 16, '12-16%'), (16, 20, '16-20%'), (20, 100, '20%+')]
    for lo, hi, label in buckets:
        br = [r for r in all_results if lo <= r['edge_pct'] < hi and r['result'] in ('WIN', 'LOSS')]
        bw = sum(1 for r in br if r['result'] == 'WIN')
        bl = sum(1 for r in br if r['result'] == 'LOSS')
        bp = sum(r.get('pnl', 0) for r in br)
        bwg = sum(r['units'] for r in br)
        wp = bw / (bw + bl) * 100 if (bw + bl) > 0 else 0
        broi = bp / bwg * 100 if bwg > 0 else 0
        report.append(f"    Edge {label:12} {bw}W-{bl}L ({wp:.0f}%) | {bp:+.1f}u | ROI {broi:+.1f}%")

    # Daily summary
    if daily_summary:
        report.append("")
        report.append("  ── DAILY BREAKDOWN ──")
        for day_str, w, l, pnl, total in daily_summary:
            bar = "█" * max(1, int(abs(pnl)))
            sign = "+" if pnl >= 0 else "-"
            report.append(f"    {day_str}  {w}W-{l}L  {pnl:+.1f}u  {'🟢' if pnl >= 0 else '🔴'} {bar}")

    # Top wins and worst losses
    if wins:
        report.append("")
        report.append("  ── TOP 5 WINS ──")
        for r in sorted(wins, key=lambda x: x['pnl'], reverse=True)[:5]:
            report.append(f"    +{r['pnl']:.1f}u  {r['player']} {r['side']} {r['line']} {r['label']} (actual: {r.get('actual','?')}) @ {r['book']} [{r['sport']}]")

    if losses:
        report.append("")
        report.append("  ── WORST 5 LOSSES ──")
        for r in sorted(losses, key=lambda x: x['pnl'])[:5]:
            report.append(f"    {r['pnl']:.1f}u  {r['player']} {r['side']} {r['line']} {r['label']} (actual: {r.get('actual','?')}) @ {r['book']} [{r['sport']}]")

    report.append("")
    report.append("=" * 70)

    output = "\n".join(report)
    print(output)
    return output


if __name__ == '__main__':
    d = 45
    sport = None
    for i, arg in enumerate(sys.argv):
        if arg == '--days' and i + 1 < len(sys.argv):
            d = int(sys.argv[i + 1])
        if arg == '--sport' and i + 1 < len(sys.argv):
            sport = sys.argv[i + 1]
            # Expand shorthand
            sport_map = {'nba': 'basketball_nba', 'ncaab': 'basketball_ncaab', 'nhl': 'icehockey_nhl'}
            sport = sport_map.get(sport, sport)

    backtest_props(days=d, sport_filter=sport)
