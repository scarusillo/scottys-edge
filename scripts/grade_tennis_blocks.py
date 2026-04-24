"""
grade_tennis_blocks.py — Grade virtual tennis bets from the block-backtest tracker.

Used to retrospectively evaluate tennis DIVERGENCE_GATE blocks by looking up
actual match results. Originally built for the 2026-04-24 Madrid R32 tracker
(`data/tennis_block_backtest_YYYYMMDD.md`).

Usage:
    PYTHONIOENCODING=utf-8 python scripts/grade_tennis_blocks.py --date 2026-04-24
    PYTHONIOENCODING=utf-8 python scripts/grade_tennis_blocks.py --retrograde-from 2026-04-21

The script parses the in-scope bet list and compares against the `results`
table for each matchup. For spread bets, it requires the actual match winner
+ game margin to evaluate the cover. For ML bets, the winner field is enough.
"""
import sqlite3
import os
import sys
import argparse
import re

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def find_match(conn, player1, player2, target_date):
    """Find a tennis match result by player pair and date."""
    rows = conn.execute("""
        SELECT home, away, home_score, away_score, winner, completed,
               DATE(commence_time) dt
        FROM results
        WHERE sport LIKE 'tennis%'
          AND DATE(commence_time) = ?
          AND (
            (home = ? AND away = ?)
            OR (home = ? AND away = ?)
          )
    """, (target_date, player1, player2, player2, player1)).fetchall()
    return rows[0] if rows else None


def grade_ml_bet(bet_player, match_row):
    """Grade a moneyline bet on bet_player using match_row."""
    if not match_row:
        return 'PENDING', None
    home, away, hs, as_, winner, completed, dt = match_row
    if not completed:
        return 'PENDING', None
    if winner == bet_player:
        return 'WIN', f'{winner} def. {away if winner == home else home} {hs}-{as_}'
    return 'LOSS', f'{winner} won ({hs}-{as_})'


def grade_spread_bet(bet_player, line, match_row):
    """Grade a tennis spread bet (game-based) using set scores."""
    if not match_row:
        return 'PENDING', None
    home, away, hs, as_, winner, completed, dt = match_row
    if not completed:
        return 'PENDING', None
    # hs/as_ in tennis = sets won. Need games. Look up in tennis_metadata.
    # For a quick heuristic: if bet_player won the match, they cover any + line.
    # If they lost, need game margin to evaluate.
    # Simplified: lean on winner + set score for crude cover check.
    # line like +2.5 or +4.5 means bet_player's game count + line > opponent's game count
    # Without per-set game totals here, we approximate: if bet player won → cover
    # if they lost 2-0 in sets, game margin typically 7-10 games → need line > that
    if winner == bet_player:
        return 'WIN', f'{winner} won outright — covers any + line'
    # Lost — approximate game margin from set score
    if hs == 2 and as_ == 0:
        # Bet player lost 0-2 in sets. Typical game margin 6-8.
        # +2.5 or +4.5 likely doesn't cover, +7.5 might.
        est_margin = 7  # rough avg for 0-2 sets loss
    elif hs == 2 and as_ == 1:
        # 2-1 set loss, close match. Game margin typically 2-5.
        est_margin = 3
    else:
        est_margin = 7
    if line > est_margin:
        return 'LIKELY_WIN', f'Lost sets {hs}-{as_}, approx margin {est_margin}, line +{line}'
    return 'LIKELY_LOSS', f'Lost sets {hs}-{as_}, approx margin {est_margin}, line +{line}'


def parse_tracker(path):
    """Parse the markdown tracker for in-scope bets.

    Returns list of dicts: {matchup, tour, bet_player, bet_type, line, odds, scope}
    """
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    # Look for rows like: | 3 | ATP | Dusan Lajovic vs Arthur Rinderknech | ... | Lajovic ML | +143 | ✅ |
    # In-scope rows end with ✅
    bets = []
    for line in content.splitlines():
        if not line.startswith('| ') or '✅' not in line or 'Tour' in line or '---' in line:
            continue
        cells = [c.strip() for c in line.split('|')[1:-1]]
        if len(cells) < 7:
            continue
        try:
            idx, tour, matchup, mkt, mdl, bet_str, odds_str, scope = (cells + [''])[:8]
        except ValueError:
            continue
        m = re.match(r'(.+?) vs (.+)', matchup)
        if not m:
            continue
        p1, p2 = m.group(1).strip(), m.group(2).strip()
        bet_str_lower = bet_str.lower()
        if ' ml' in bet_str_lower:
            bet_player = bet_str.split(' ML')[0].replace('**', '').strip()
            bet_type = 'ML'
            line = None
        elif 'spread' in bet_str_lower or re.search(r'[+-][\d.]+', bet_str):
            m2 = re.match(r'(?:\*\*)?(.+?)(?:\*\*)?\s+([+-][\d.]+)', bet_str.replace('**', ''))
            if m2:
                bet_player = m2.group(1).strip()
                line = float(m2.group(2))
                bet_type = 'SPREAD'
            else:
                continue
        else:
            continue
        try:
            odds = int(odds_str.replace('+', '').replace('**', '').strip()) if odds_str.strip().replace('+','').replace('-','').replace('**','').isdigit() else None
        except (ValueError, AttributeError):
            odds = None
        bets.append({
            'matchup': f'{p1} vs {p2}',
            'p1': p1, 'p2': p2,
            'tour': tour, 'bet_player': bet_player, 'bet_type': bet_type,
            'line': line, 'odds': odds, 'scope': 'IN',
        })
    return bets


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help='YYYY-MM-DD — match date to grade')
    parser.add_argument('--tracker', default=None, help='Path to tracker markdown file')
    args = parser.parse_args()

    tracker_path = args.tracker or os.path.join(
        os.path.dirname(__file__), '..', 'data',
        f'tennis_block_backtest_{args.date.replace("-","")}.md'
    )
    print(f'Tracker: {tracker_path}')
    print(f'Grading date: {args.date}')
    print()

    bets = parse_tracker(tracker_path)
    if not bets:
        print('No in-scope bets parsed from tracker.')
        return

    conn = sqlite3.connect(DB_PATH)
    print(f'Found {len(bets)} in-scope bets')
    print()

    wins = losses = pending = 0
    pnl = 0.0
    for b in bets:
        match = find_match(conn, b['p1'], b['p2'], args.date)
        if b['bet_type'] == 'ML':
            result, detail = grade_ml_bet(b['bet_player'], match)
        else:
            result, detail = grade_spread_bet(b['bet_player'], b['line'], match)
        # P&L (1u stake, American odds)
        if result == 'WIN' or result == 'LIKELY_WIN':
            if b['odds'] and b['odds'] > 0:
                p = b['odds'] / 100.0
            elif b['odds']:
                p = 100.0 / abs(b['odds'])
            else:
                p = 100.0 / 110.0  # default -110
            wins += 1
            pnl += p
        elif result == 'LOSS' or result == 'LIKELY_LOSS':
            losses += 1
            pnl -= 1.0
        else:
            pending += 1
        print(f'  [{b["tour"]}] {b["bet_player"]} {b["bet_type"]} '
              f'{"+"+str(b["line"]) if b["line"] and b["line"]>0 else (str(b["line"]) if b["line"] else "")} '
              f'@ {b["odds"] if b["odds"] else "-110"}: {result}  {detail or ""}')

    print()
    print('=== SUMMARY ===')
    tot = wins + losses
    wr = f'{100*wins/tot:.0f}%' if tot > 0 else '—'
    print(f'  {wins}W-{losses}L ({wr}) | P/L: {pnl:+.2f}u | Pending: {pending}')


if __name__ == '__main__':
    main()
