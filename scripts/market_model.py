"""
market_model.py — Model B: Cross-book disagreement engine.

Runs alongside Model A (model_engine.py) as a shadow factor. Identifies
edges by comparing sharp book consensus pricing against soft book odds.
No power ratings, no Elo, no context — purely market-vs-market.

Data-driven book tiers measured from historical line movement:
  SHARP: BetRivers, BetMGM, BetUS (open closest to closing consensus)
  SOFT:  FanDuel, Fanatics, Caesars, DraftKings (open furthest from close)

v25.16: Shadow mode only — tags Model A picks with "Model B agrees/disagrees"
        but does not affect pick selection.
"""
import sqlite3
import os
import statistics
from datetime import datetime, timezone, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ═══════════════════════════════════════════════════════════════════
# BOOK TIERS — data-driven from historical opener-to-close analysis
# BetRivers 0.662, BetMGM 0.722, BetUS 0.689 (sharpest openers)
# DraftKings 0.944, FanDuel 0.983, Caesars 0.988, Fanatics 1.104
# ═══════════════════════════════════════════════════════════════════

SHARP_BOOKS = {'BetRivers', 'BetMGM'}  # NY-legal sharp books only
SOFT_BOOKS = {'FanDuel', 'Fanatics', 'Caesars', 'DraftKings'}

# Minimum books needed for a valid consensus
MIN_SHARP_BOOKS = 2
MIN_TOTAL_BOOKS = 4

# Edge thresholds
MIN_DISAGREE_PCT = 3.0   # Minimum sharp-vs-soft disagreement to flag
STRONG_DISAGREE_PCT = 6.0  # Strong disagreement — high confidence signal


def american_to_implied(odds):
    """Convert American odds to implied probability."""
    if odds > 0:
        return 100.0 / (odds + 100.0)
    elif odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    return 0.5


def implied_to_american(prob):
    """Convert implied probability back to American odds."""
    if prob <= 0 or prob >= 1:
        return 0
    if prob >= 0.5:
        return round(-100 * prob / (1 - prob))
    else:
        return round(100 * (1 - prob) / prob)


def get_current_odds_by_book(conn, event_id, market, selection):
    """
    Get the most recent odds snapshot for each book on a specific market.

    Returns: dict of {book: {'line': float, 'odds': float, 'implied': float, 'snap': str}}
    """
    rows = conn.execute("""
        SELECT o.book, o.line, o.odds, o.snapshot_date, o.snapshot_time
        FROM odds o
        INNER JOIN (
            SELECT book, MAX(snapshot_date || ' ' || snapshot_time) as max_snap
            FROM odds
            WHERE event_id = ? AND market = ? AND selection = ?
            GROUP BY book
        ) latest ON o.book = latest.book
            AND (o.snapshot_date || ' ' || o.snapshot_time) = latest.max_snap
        WHERE o.event_id = ? AND o.market = ? AND o.selection = ?
    """, (event_id, market, selection, event_id, market, selection)).fetchall()

    result = {}
    for book, line, odds, sd, st in rows:
        impl = american_to_implied(odds)
        if impl and impl > 0:
            result[book] = {
                'line': line,
                'odds': odds,
                'implied': impl,
                'snap': f"{sd} {st}" if sd and st else None,
            }
    return result


def calculate_book_disagreement(book_odds):
    """
    Calculate the disagreement between sharp and soft books.

    Returns: {
        'sharp_consensus': float (implied prob),
        'soft_prices': dict of {book: implied prob},
        'edges': dict of {book: edge_pct},  # positive = soft book is offering value
        'best_soft_book': str,
        'best_edge': float,
        'sharp_line': float,
        'agreement_level': 'STRONG' | 'MODERATE' | 'WEAK' | 'NONE',
    }
    """
    sharp_implieds = []
    sharp_lines = []
    soft_prices = {}

    for book, data in book_odds.items():
        if book in SHARP_BOOKS:
            sharp_implieds.append(data['implied'])
            sharp_lines.append(data['line'])
        elif book in SOFT_BOOKS:
            soft_prices[book] = data

    if len(sharp_implieds) < MIN_SHARP_BOOKS:
        return None  # Not enough sharp books for consensus

    if not soft_prices:
        return None  # No soft books to compare against

    sharp_consensus = statistics.mean(sharp_implieds)
    sharp_lines = [l for l in sharp_lines if l is not None]
    sharp_line = statistics.median(sharp_lines) if sharp_lines else None

    # Calculate edge for each soft book
    edges = {}
    for book, data in soft_prices.items():
        # Edge = sharp thinks it's MORE likely than soft book prices
        # If sharp consensus is 55% but FanDuel implies 50%, that's +5% edge at FanDuel
        edge = (sharp_consensus - data['implied']) * 100.0
        edges[book] = round(edge, 2)

    # Best soft book edge
    if edges:
        best_book = max(edges, key=edges.get)
        best_edge = edges[best_book]
    else:
        best_book = None
        best_edge = 0

    # Agreement level
    if best_edge >= STRONG_DISAGREE_PCT:
        level = 'STRONG'
    elif best_edge >= MIN_DISAGREE_PCT:
        level = 'MODERATE'
    elif best_edge > 0:
        level = 'WEAK'
    else:
        level = 'NONE'

    return {
        'sharp_consensus': round(sharp_consensus, 4),
        'soft_prices': {b: round(d['implied'], 4) for b, d in soft_prices.items()},
        'soft_odds': {b: d['odds'] for b, d in soft_prices.items()},
        'edges': edges,
        'best_soft_book': best_book,
        'best_edge': best_edge,
        'sharp_line': sharp_line,
        'agreement_level': level,
    }


def evaluate_pick(conn, event_id, market, selection):
    """
    Evaluate a single pick from Model A using Model B's cross-book analysis.

    Args:
        conn: sqlite3 connection
        event_id: the event
        market: 'totals', 'spreads', or 'h2h'
        selection: 'Over', 'Under', team name, etc.

    Returns: dict with Model B assessment, or None if insufficient data
    """
    book_odds = get_current_odds_by_book(conn, event_id, market, selection)

    if len(book_odds) < MIN_TOTAL_BOOKS:
        return {
            'model_b_agrees': None,
            'reason': f'Insufficient books ({len(book_odds)} < {MIN_TOTAL_BOOKS})',
            'detail': None,
        }

    result = calculate_book_disagreement(book_odds)
    if not result:
        return {
            'model_b_agrees': None,
            'reason': 'Not enough sharp books for consensus',
            'detail': None,
        }

    agrees = result['best_edge'] >= MIN_DISAGREE_PCT
    return {
        'model_b_agrees': agrees,
        'agreement_level': result['agreement_level'],
        'sharp_consensus': result['sharp_consensus'],
        'best_soft_book': result['best_soft_book'],
        'best_edge': result['best_edge'],
        'edges': result['edges'],
        'soft_odds': result['soft_odds'],
        'reason': f"Sharp consensus {result['sharp_consensus']:.1%} vs best soft {result['best_soft_book']} {result['soft_prices'].get(result['best_soft_book'], 0):.1%} = {result['best_edge']:+.1f}% gap" if agrees else f"Books agree (gap {result['best_edge']:+.1f}%)",
        'detail': result,
    }


def evaluate_prop(conn, event_id, market, player, side, line):
    """
    Evaluate a prop pick using cross-book disagreement on prop_snapshots.

    Similar to evaluate_pick but queries prop_snapshots instead of odds.
    """
    rows = conn.execute("""
        SELECT ps.book, ps.odds, ps.captured_at
        FROM prop_snapshots ps
        INNER JOIN (
            SELECT book, MAX(captured_at) as max_cap
            FROM prop_snapshots
            WHERE event_id = ? AND market = ? AND player = ? AND side = ? AND line = ?
            GROUP BY book
        ) latest ON ps.book = latest.book AND ps.captured_at = latest.max_cap
        WHERE ps.event_id = ? AND ps.market = ? AND ps.player = ? AND ps.side = ? AND ps.line = ?
    """, (event_id, market, player, side, line,
          event_id, market, player, side, line)).fetchall()

    book_odds = {}
    for book, odds, cap in rows:
        impl = american_to_implied(odds)
        if impl and impl > 0:
            book_odds[book] = {
                'line': line,
                'odds': odds,
                'implied': impl,
                'snap': cap,
            }

    if len(book_odds) < MIN_TOTAL_BOOKS:
        return {
            'model_b_agrees': None,
            'reason': f'Insufficient books ({len(book_odds)})',
            'detail': None,
        }

    result = calculate_book_disagreement(book_odds)
    if not result:
        return {
            'model_b_agrees': None,
            'reason': 'Not enough sharp books',
            'detail': None,
        }

    agrees = result['best_edge'] >= MIN_DISAGREE_PCT
    return {
        'model_b_agrees': agrees,
        'agreement_level': result['agreement_level'],
        'best_edge': result['best_edge'],
        'best_soft_book': result['best_soft_book'],
        'edges': result['edges'],
        'reason': f"Sharp-soft gap: {result['best_edge']:+.1f}%" if agrees else f"Books agree ({result['best_edge']:+.1f}%)",
        'detail': result,
    }


def tag_picks_with_model_b(conn, picks):
    """
    Tag a list of Model A picks with Model B's assessment.

    Modifies each pick dict in-place, adding:
        - model_b_agrees: True/False/None
        - model_b_level: STRONG/MODERATE/WEAK/NONE
        - model_b_reason: human-readable explanation
        - model_b_edge: the cross-book edge percentage

    Args:
        conn: sqlite3 connection
        picks: list of pick dicts from Model A

    Returns: summary dict with counts
    """
    agree_count = 0
    disagree_count = 0
    unknown_count = 0

    for p in picks:
        mtype = p.get('market_type', '')
        eid = p.get('event_id', '')
        selection = p.get('selection', '')

        if mtype == 'PROP':
            # Parse prop selection
            import re
            m = re.match(r'^(.+?)\s+(OVER|UNDER)\s+([\d.]+)\s+', selection, re.IGNORECASE)
            if m:
                player = m.group(1).strip()
                side = m.group(2).capitalize()
                line = float(m.group(3))
                # Map back to API market name
                stat = selection.split()[-1].upper()
                market_map = {
                    'HITS': 'batter_hits', 'RBIS': 'batter_rbis', 'RUNS': 'batter_runs_scored',
                    'THREES': 'player_threes', 'POINTS': 'player_points',
                    'REBOUNDS': 'player_rebounds', 'ASSISTS': 'player_assists',
                    'BLOCKS': 'player_blocks', 'STEALS': 'player_steals',
                    'STRIKEOUTS': 'pitcher_strikeouts', 'OUTS': 'pitcher_outs',
                }
                api_market = market_map.get(stat, stat.lower())
                result = evaluate_prop(conn, eid, api_market, player, side, line)
            else:
                result = {'model_b_agrees': None, 'reason': 'Could not parse prop', 'detail': None}
        else:
            # Game line — map market_type to odds market
            market_name = {'TOTAL': 'totals', 'SPREAD': 'spreads', 'MONEYLINE': 'h2h'}.get(mtype, 'totals')

            # Normalize selection for odds table lookup
            if 'OVER' in selection.upper():
                odds_sel = 'Over'
            elif 'UNDER' in selection.upper():
                odds_sel = 'Under'
            else:
                import re as _re
                odds_sel = _re.sub(r'\s*[+-]?\d+\.?\d*$', '', selection).strip()
                odds_sel = odds_sel.replace(' ML (cross-mkt)', '').replace(' ML', '').strip()

            result = evaluate_pick(conn, eid, market_name, odds_sel)

        # Tag the pick
        p['model_b_agrees'] = result.get('model_b_agrees')
        p['model_b_level'] = result.get('agreement_level', 'UNKNOWN')
        p['model_b_reason'] = result.get('reason', '')
        p['model_b_edge'] = result.get('best_edge', 0)

        if result.get('model_b_agrees') is True:
            agree_count += 1
        elif result.get('model_b_agrees') is False:
            disagree_count += 1
        else:
            unknown_count += 1

    return {
        'agree': agree_count,
        'disagree': disagree_count,
        'unknown': unknown_count,
        'total': len(picks),
    }


def generate_shadow_report(picks):
    """
    Generate a text report of Model B's assessment for the grade email.

    Returns: string report
    """
    lines = []
    lines.append("MODEL B — CROSS-BOOK DISAGREEMENT (SHADOW)")
    lines.append("=" * 50)

    agree = [p for p in picks if p.get('model_b_agrees') is True]
    disagree = [p for p in picks if p.get('model_b_agrees') is False]
    unknown = [p for p in picks if p.get('model_b_agrees') is None]

    lines.append(f"  Agrees: {len(agree)} | Disagrees: {len(disagree)} | Unknown: {len(unknown)}")
    lines.append("")

    for p in picks:
        sel = p.get('selection', '')[:45]
        mb = p.get('model_b_agrees')
        level = p.get('model_b_level', '')
        edge = p.get('model_b_edge', 0)
        reason = p.get('model_b_reason', '')

        if mb is True:
            icon = "AGREE"
        elif mb is False:
            icon = "DISAGREE"
        else:
            icon = "???"

        lines.append(f"  [{icon:8s}] {sel}")
        lines.append(f"             {reason}")
        lines.append("")

    return "\n".join(lines)


if __name__ == '__main__':
    """Test Model B on today's picks."""
    conn = sqlite3.connect(DB_PATH)

    # Get today's bets
    rows = conn.execute("""
        SELECT selection, sport, market_type, event_id, book, odds, edge_pct
        FROM bets
        WHERE DATE(created_at) = DATE('now', 'localtime')
        AND result IS NULL
        ORDER BY created_at
    """).fetchall()

    picks = []
    for sel, sport, mtype, eid, book, odds, edge in rows:
        picks.append({
            'selection': sel, 'sport': sport, 'market_type': mtype,
            'event_id': eid, 'book': book, 'odds': odds, 'edge_pct': edge,
        })

    if not picks:
        print("No active picks today.")
    else:
        summary = tag_picks_with_model_b(conn, picks)
        report = generate_shadow_report(picks)
        print(report)
        print(f"\nSummary: {summary}")

    conn.close()
