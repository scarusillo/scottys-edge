"""
props_engine.py v11 — Scotty's Edge Player Props Engine

PHILOSOPHY: We don't "predict" player stats. The MARKET does that.
Our edge comes from finding books that disagree with the market consensus —
the same methodology for finding edges on game lines.

SIGNALS:
  1. CONSENSUS EDGE — One book's odds are out of line with 5+ other books
  2. LINE MOVEMENT  — Sharp money moved the line; one book is stale
  3. STALE LINE     — Book hasn't updated while others moved 1+ points
  4. HISTORICAL     — Our accumulated data shows player over/under tendencies

WORKS FOR: Every player the API returns. No hardcoded player list.
"""
import sqlite3, os, math
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from scottys_edge import get_star_rating, stars_to_units, kelly_units, kelly_label

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Books NOT available in NY — we still use their data for consensus,
# but never RECOMMEND betting at these books
EXCLUDED_BOOKS = {'Bovada', 'BetOnline.ag', 'BetUS', 'MyBookie.ag', 'LowVig.ag'}
NY_LEGAL_BOOKS = {'DraftKings', 'FanDuel', 'BetMGM', 'Caesars', 'BetRivers',
                  'Bally Bet', 'ESPN BET', 'PointsBet', 'Fanatics'}

PROP_LABEL = {
    'player_points': 'POINTS', 'player_rebounds': 'REBOUNDS',
    'player_assists': 'ASSISTS', 'player_threes': 'THREES',
    'player_blocks': 'BLOCKS', 'player_steals': 'STEALS',
    # NHL markets
    'player_shots_on_goal': 'SOG', 'player_power_play_points': 'PPP',
    'player_blocked_shots': 'BLK_SHOTS',
    # Soccer markets
    'player_shots': 'SHOTS', 'player_shots_on_target': 'SOT',
    # MLB batting
    'batter_hits': 'HITS', 'batter_total_bases': 'TOTAL_BASES',
    'batter_home_runs': 'HOME_RUNS', 'batter_rbis': 'RBIS',
    'batter_runs_scored': 'RUNS', 'batter_strikeouts': 'STRIKEOUTS',
    'batter_stolen_bases': 'STOLEN_BASES', 'batter_walks': 'WALKS',
    # MLB pitching
    'pitcher_strikeouts': 'PITCHER_STRIKEOUTS', 'pitcher_outs': 'PITCHER_OUTS',
    'pitcher_hits_allowed': 'HITS_ALLOWED', 'pitcher_earned_runs': 'EARNED_RUNS',
    'pitcher_walks': 'PITCHER_WALKS',
}

# ═══════════════════════════════════════════════════════════════════
# MATH HELPERS
# ═══════════════════════════════════════════════════════════════════

def american_to_implied(odds):
    """Convert American odds to implied probability (0-1)."""
    if odds is None: return None
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)

def remove_vig(over_imp, under_imp):
    """Remove vig to get true probability from both sides of a line."""
    total = over_imp + under_imp
    if total == 0: return 0.5, 0.5
    return over_imp / total, under_imp / total

def median(values):
    """Simple median calculation."""
    if not values: return None
    s = sorted(values)
    n = len(s)
    if n % 2 == 0:
        return (s[n//2 - 1] + s[n//2]) / 2
    return s[n//2]


# ═══════════════════════════════════════════════════════════════════
# SIGNAL 1: CONSENSUS EDGE
# The core principle — find books that disagree with the market
# ═══════════════════════════════════════════════════════════════════

def compute_consensus(player_lines):
    """
    Given all books' lines for a player/prop, compute market consensus.

    player_lines: list of dicts with keys: book, line, over_odds, under_odds

    Returns dict with consensus_line, consensus_over_prob, consensus_under_prob,
    book_count, and the original lines list.
    """
    if len(player_lines) < 2:
        return None

    lines = [pl['line'] for pl in player_lines if pl['line'] is not None]
    if not lines:
        return None

    cons_line = median(lines)

    # Compute vig-removed probabilities for each book
    true_over_probs = []
    true_under_probs = []

    for pl in player_lines:
        over_imp = american_to_implied(pl.get('over_odds'))
        under_imp = american_to_implied(pl.get('under_odds'))
        if over_imp and under_imp and over_imp > 0 and under_imp > 0:
            true_o, true_u = remove_vig(over_imp, under_imp)
            true_over_probs.append(true_o)
            true_under_probs.append(true_u)
        elif over_imp and over_imp > 0:
            true_over_probs.append(over_imp)
        elif under_imp and under_imp > 0:
            true_under_probs.append(under_imp)

    cons_over = median(true_over_probs) if true_over_probs else 0.5
    cons_under = median(true_under_probs) if true_under_probs else 0.5

    # Normalize
    total = cons_over + cons_under
    if total > 0:
        cons_over /= total
        cons_under /= total

    return {
        'consensus_line': cons_line,
        'consensus_over_prob': cons_over,
        'consensus_under_prob': cons_under,
        'book_count': len(player_lines),
        'lines': player_lines,
    }


def find_consensus_edges(consensus, min_edge=3.0):
    """
    Find books offering better odds than consensus suggests.

    If consensus says OVER has 55% true probability but Book X has OVER at +110
    (implied 47.6%), that's a 7.4% edge.
    """
    edges = []
    if not consensus:
        return edges

    for pl in consensus['lines']:
        book = pl['book']
        line = pl['line']

        # Check OVER edge
        over_odds = pl.get('over_odds')
        if over_odds is not None:
            over_imp = american_to_implied(over_odds) or 0
            if over_imp > 0:
                # Adjust consensus prob for line difference from this book's line
                line_diff = consensus['consensus_line'] - line  # positive = this line is lower (easier over)
                adj_prob = consensus['consensus_over_prob']
                if line_diff != 0:
                    # Each 0.5 point shift ~ 2-4% probability for points
                    adj_prob = min(0.95, max(0.05, adj_prob + line_diff * 0.04))

                edge = (adj_prob - over_imp) * 100
                if edge >= min_edge:
                    edges.append({
                        'book': book, 'line': line, 'odds': over_odds,
                        'side': 'OVER', 'model_prob': adj_prob,
                        'implied_prob': over_imp, 'edge_pct': edge,
                    })

        # Check UNDER edge
        under_odds = pl.get('under_odds')
        if under_odds is not None:
            under_imp = american_to_implied(under_odds) or 0
            if under_imp > 0:
                line_diff = line - consensus['consensus_line']  # positive = this line is higher (easier under)
                adj_prob = consensus['consensus_under_prob']
                if line_diff != 0:
                    adj_prob = min(0.95, max(0.05, adj_prob + line_diff * 0.04))

                edge = (adj_prob - under_imp) * 100
                if edge >= min_edge:
                    edges.append({
                        'book': book, 'line': line, 'odds': under_odds,
                        'side': 'UNDER', 'model_prob': adj_prob,
                        'implied_prob': under_imp, 'edge_pct': edge,
                    })

    return edges


# ═══════════════════════════════════════════════════════════════════
# SIGNAL 2: LINE MOVEMENT (sharp money)
# ═══════════════════════════════════════════════════════════════════

def get_line_movement(conn, event_id, player, market):
    """Check if the line has moved since we first saw it."""
    try:
        opener = conn.execute("""
            SELECT opening_line, opening_over_odds, opening_under_odds
            FROM prop_openers
            WHERE event_id=? AND player=? AND market=?
        """, (event_id, player, market)).fetchone()

        if not opener:
            return None

        return {
            'opening_line': opener[0],
            'opening_over_odds': opener[1],
            'opening_under_odds': opener[2],
        }
    except:
        return None


def score_line_movement(opener_info, current_consensus, side):
    """
    Score the line movement signal.

    If line moved from 19.5 to 21.5, sharps bet OVER — OVER is stronger.
    If we can still get 19.5 at a stale book, that's huge value.

    Returns: bonus edge % (-5 to +8)
    """
    if not opener_info or not current_consensus:
        return 0.0

    opening = opener_info['opening_line']
    current = current_consensus['consensus_line']

    if opening is None or current is None:
        return 0.0

    movement = current - opening  # positive = line went UP

    if side == 'OVER' and movement > 0:
        # Line went up = sharps bet over = confirms our over
        return min(8.0, abs(movement) * 3.0)
    elif side == 'UNDER' and movement < 0:
        # Line went down = sharps bet under = confirms our under
        return min(8.0, abs(movement) * 3.0)

    # Movement goes AGAINST our side — penalty
    if side == 'OVER' and movement < 0:
        return max(-5.0, movement * 2.0)
    elif side == 'UNDER' and movement > 0:
        return max(-5.0, -movement * 2.0)

    return 0.0


# ═══════════════════════════════════════════════════════════════════
# SIGNAL 3: STALE LINE DETECTION
# ═══════════════════════════════════════════════════════════════════

def detect_stale_lines(consensus, market='player_points'):
    """
    If 5+ books have line at 21.5 but one book has 19.5,
    that book hasn't updated — stale line — free money.
    """
    if not consensus or consensus['book_count'] < 3:
        return {}

    # Threshold depends on prop type
    threshold = {'player_points': 1.5, 'player_rebounds': 1.0,
                 'player_assists': 1.0, 'player_threes': 0.5,
                 'player_blocks': 0.5, 'player_steals': 0.5,
                 'player_shots_on_goal': 0.5, 'player_power_play_points': 0.5,
                 'player_blocked_shots': 0.5,
                 'player_shots': 0.5, 'player_shots_on_target': 0.5}.get(market, 1.5)

    cons_line = consensus['consensus_line']
    stale = {}

    for pl in consensus['lines']:
        if pl['line'] is None:
            continue
        diff = abs(pl['line'] - cons_line)
        if diff >= threshold:
            stale[pl['book']] = {
                'book_line': pl['line'],
                'consensus_line': cons_line,
                'stale_amount': diff,
            }

    return stale


# ═══════════════════════════════════════════════════════════════════
# SIGNAL 4: HISTORICAL PLAYER DATA
# ═══════════════════════════════════════════════════════════════════

def get_player_history(conn, player, stat_type, limit=20):
    """
    Get accumulated player results from our database.
    Returns over_rate and average actual value.
    """
    try:
        rows = conn.execute("""
            SELECT actual_value, prop_line, result
            FROM player_results
            WHERE player=? AND stat_type=?
            ORDER BY game_date DESC
            LIMIT ?
        """, (player, stat_type, limit)).fetchall()

        if len(rows) < 3:
            return None  # Not enough data yet

        overs = sum(1 for r in rows if r[2] == 'OVER')
        actuals = [r[0] for r in rows if r[0] is not None]
        lines = [r[1] for r in rows if r[1] is not None]
        avg_actual = sum(actuals) / len(actuals) if actuals else 0
        avg_line = sum(lines) / len(lines) if lines else 0

        return {
            'games': len(rows),
            'over_rate': overs / len(rows),
            'avg_actual': avg_actual,
            'avg_line': avg_line,
            'diff': avg_actual - avg_line,
        }
    except:
        return None


# ═══════════════════════════════════════════════════════════════════
# MAIN EVALUATION ENGINE
# ═══════════════════════════════════════════════════════════════════

STAT_TYPE_MAP = {
    'player_points': 'pts', 'player_rebounds': 'reb',
    'player_assists': 'ast', 'player_threes': 'threes',
    'player_blocks': 'blk', 'player_steals': 'stl',
    # NHL
    'player_shots_on_goal': 'sog', 'player_power_play_points': 'ppp',
    'player_blocked_shots': 'blk_shots',
    # Soccer
    'player_shots': 'shots', 'player_shots_on_target': 'sot',
    # MLB batting
    'batter_hits': 'hits', 'batter_total_bases': 'total_bases',
    'batter_home_runs': 'hr', 'batter_rbis': 'rbi',
    'batter_runs_scored': 'runs', 'batter_strikeouts': 'batter_k',
    'batter_stolen_bases': 'stolen_bases', 'batter_walks': 'walks',
    # MLB pitching
    'pitcher_strikeouts': 'pitcher_k', 'pitcher_outs': 'pitcher_outs',
    'pitcher_hits_allowed': 'pitcher_h_allowed',
    'pitcher_earned_runs': 'pitcher_er', 'pitcher_walks': 'pitcher_bb',
}

# Sport-specific minimum books for consensus.
# NBA has 7-8 books posting props; 3-book minimum is fine.
# NHL/NCAAB/soccer have fewer books; 2-book minimum captures real edges.
# Backtest 3/23: min_books=3 killed 73.7% of all prop groups including
# ALL soccer props and most NHL/NCAAB. Lowering to 2 for thin markets.
MIN_BOOKS_FOR_CONSENSUS = 3  # Default (NBA)
MIN_BOOKS_BY_SPORT = {
    'basketball_nba': 3,
    'basketball_ncaab': 2,
    'icehockey_nhl': 2,
    'soccer_epl': 2, 'soccer_italy_serie_a': 2, 'soccer_spain_la_liga': 2,
    'soccer_germany_bundesliga': 2, 'soccer_france_ligue_one': 2,
    'soccer_usa_mls': 2, 'soccer_uefa_champs_league': 2,
    'soccer_mexico_ligamx': 2,
}


def evaluate_props(conn=None):
    """
    Scotty's Edge prop evaluation — works for ALL players the API returns.

    For every player prop today:
      1. Group all books' lines for that player/prop/game
      2. Compute market consensus (true probability, vig-removed)
      3. Find outlier books offering better value (consensus edge)
      4. Check line movement from openers (sharp money signal)
      5. Detect stale lines (key edge method)
      6. Check our historical data for this player
      7. Combine signals into final weighted edge
    """
    close = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close = True

    now_utc = datetime.now(timezone.utc)
    window_start = (now_utc - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Ensure new tables exist
    _ensure_tables(conn)

    # Get ALL prop lines from today
    rows = conn.execute("""
        SELECT sport, event_id, commence_time, home, away,
               book, market, selection, line, odds
        FROM props
        WHERE market IN ('player_points','player_rebounds','player_assists','player_threes',
                         'player_blocks','player_steals',
                         'player_shots_on_goal','player_power_play_points','player_blocked_shots',
                         'player_shots','player_shots_on_target')
        AND commence_time >= ?
        ORDER BY commence_time
    """, (window_start,)).fetchall()

    if not rows:
        total = conn.execute("SELECT COUNT(*) FROM props").fetchone()[0]
        print(f"  No upcoming player props. ({total} total in DB)")
        if close: conn.close()
        return []

    # ── Step 1: Parse and group all lines by (event_id, player, market, line_value) ──
    # CRITICAL FIX: Books offer MULTIPLE lines per player (e.g., 0.5 AND 1.5 threes).
    # These are different markets — UNDER 0.5 (zero threes) vs UNDER 1.5 (zero or one).
    # Must only compare books offering the SAME line value.
    grouped = defaultdict(lambda: defaultdict(lambda: {'over_odds': None, 'under_odds': None, 'line': None, 'book': None}))
    game_info = {}

    for r in rows:
        sport, eid, commence, home, away, book, market, selection, line, odds = r
        if line is None or odds is None:
            continue

        # Skip started games
        try:
            gt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
            if gt < now_utc - timedelta(minutes=5):
                continue
        except:
            pass

        # Parse player name and side
        player, side = None, None
        if ' - Over' in selection:
            player = selection.split(' - Over')[0].strip()
            side = 'Over'
        elif ' - Under' in selection:
            player = selection.split(' - Under')[0].strip()
            side = 'Under'
        if not player or not side:
            continue

        game_info[eid] = {'sport': sport, 'home': home, 'away': away, 'commence': commence}
        # Group by LINE VALUE — 0.5 threes and 1.5 threes are different markets
        key = (eid, player, market, line)
        entry = grouped[key][book]
        entry['line'] = line
        entry['book'] = book
        if side == 'Over':
            entry['over_odds'] = odds
        else:
            entry['under_odds'] = odds

    total_players = len(set(k[1] for k in grouped))
    total_groups = len(grouped)
    print(f"  Analyzing {total_players} players across {total_groups} prop markets...")

    # ── Step 2: Evaluate each player/prop group ──
    picks = []
    signal_counts = {'consensus': 0, 'movement': 0, 'stale': 0, 'history': 0}

    for (eid, player, market, line_val), book_lines in grouped.items():
        pl_list = [v for v in book_lines.values() if v.get('book')]
        sport = game_info.get(eid, {}).get('sport', '')
        _min_books = MIN_BOOKS_BY_SPORT.get(sport, MIN_BOOKS_FOR_CONSENSUS)
        if len(pl_list) < _min_books:
            continue

        gi = game_info.get(eid, {})
        stat_type = STAT_TYPE_MAP.get(market, market)

        # Skip soccer props with 0.5 lines — these are binary events (did they get 1 assist?)
        # Not enough volume or predictive signal. Soccer props only viable at 1.5+ lines.
        sport = gi.get('sport', '')
        if 'soccer' in sport and line_val is not None and line_val <= 0.5:
            continue

        # Signal 1: Market Consensus
        consensus = compute_consensus(pl_list)
        if not consensus:
            continue

        consensus_edges = find_consensus_edges(consensus, min_edge=3.0)
        if consensus_edges:
            signal_counts['consensus'] += len(consensus_edges)

        # Signal 2: Line Movement
        opener = get_line_movement(conn, eid, player, market)

        # Signal 3: Stale Lines
        stale = detect_stale_lines(consensus, market)
        if stale:
            signal_counts['stale'] += 1

        # Signal 4: Historical
        history = get_player_history(conn, player, stat_type)
        if history:
            signal_counts['history'] += 1

        # Combine signals for each consensus edge
        for ce in consensus_edges:
            book = ce['book']
            side = ce['side']
            line_val = ce['line']
            base_edge = ce['edge_pct']

            # Movement bonus/penalty
            movement_bonus = score_line_movement(opener, consensus, side)
            if movement_bonus != 0:
                signal_counts['movement'] += 1

            # Stale line bonus
            stale_bonus = 0.0
            if book in stale:
                si = stale[book]
                # Only bonus if stale direction matches our side
                if side == 'OVER' and si['book_line'] < si['consensus_line']:
                    stale_bonus = min(10.0, si['stale_amount'] * 3.0)
                elif side == 'UNDER' and si['book_line'] > si['consensus_line']:
                    stale_bonus = min(10.0, si['stale_amount'] * 3.0)

            # Historical bonus
            hist_bonus = 0.0
            if history and history['games'] >= 5:
                if side == 'OVER' and history['over_rate'] > 0.55:
                    hist_bonus = min(5.0, (history['over_rate'] - 0.5) * 20)
                elif side == 'UNDER' and history['over_rate'] < 0.45:
                    hist_bonus = min(5.0, (0.5 - history['over_rate']) * 20)

            # Skip high-odds props — no graded data above +200
            if ce['odds'] > 200:
                continue

            # FINAL EDGE: Base consensus edge + bonuses from confirming signals
            # v11 fix: was base*0.50 which killed every edge under 11%.
            # Now: full base edge + capped bonuses from movement/stale/history.
            final_edge = (
                base_edge +                                 # Full consensus edge (primary signal)
                min(movement_bonus * 0.30, 5.0) +          # Sharp money confirmation (capped)
                min(stale_bonus * 0.20, 4.0) +             # Stale line bonus (capped)
                min(hist_bonus * 0.15, 3.0)                # Historical tendency (capped)
            )
            final_edge = min(final_edge, 25.0)  # Cap extreme edges

            if final_edge < 5.5:
                continue

            stars = get_star_rating(final_edge)
            if stars < 2.0:
                continue

            # If edge is at an excluded book, find the best legal book instead.
            # Backtest 3/23: 59% of real edges were at excluded books (Bovada etc).
            # The edge still exists if legal books have similar odds on the same side.
            if book in EXCLUDED_BOOKS:
                # Find best legal book offering the same side
                best_legal = None
                best_legal_odds = None
                for bl in consensus.get('lines', pl_list):
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
                    # Recalculate edge with the legal book's odds
                    legal_imp = american_to_implied(best_legal_odds)
                    legal_edge = (ce['model_prob'] - legal_imp) * 100 if legal_imp else 0
                    if legal_edge >= 3.0:
                        book = best_legal
                        ce = dict(ce)  # copy to avoid mutating
                        ce['book'] = best_legal
                        ce['odds'] = best_legal_odds
                        ce['implied_prob'] = legal_imp
                        ce['edge_pct'] = legal_edge
                        base_edge = legal_edge
                        # Recalculate final edge with new base
                        final_edge = (
                            base_edge +
                            min(movement_bonus * 0.30, 5.0) +
                            min(stale_bonus * 0.20, 4.0) +
                            min(hist_bonus * 0.15, 3.0)
                        )
                        if final_edge < 5.5:
                            continue
                        stars = get_star_rating(final_edge)
                        if stars < 2.0:
                            continue
                    else:
                        continue  # Legal book doesn't have enough edge
                else:
                    continue  # No legal book offers this side

            conf = 'ELITE' if stars >= 2.5 else 'HIGH'
            label = PROP_LABEL.get(market, market.replace('player_', '').upper())

            est_time = ''
            try:
                gt = datetime.fromisoformat(gi.get('commence', '').replace('Z', '+00:00'))
                est = gt - timedelta(hours=5)
                est_time = est.strftime('%I:%M %p EST')
            except:
                pass

            # Build notes showing signals
            notes_parts = [f"Consensus={consensus['consensus_line']}"]
            notes_parts.append(f"Books={consensus['book_count']}")
            notes_parts.append(f"CEdge={base_edge:.1f}%")
            if movement_bonus != 0:
                arrow = "↑" if movement_bonus > 0 else "↓"
                notes_parts.append(f"Move={arrow}{abs(movement_bonus):.1f}%")
            if stale_bonus > 0:
                notes_parts.append(f"STALE={stale_bonus:.1f}%")
            if hist_bonus > 0 and history:
                notes_parts.append(f"Hist={history['over_rate']:.0%}O({history['games']}g)")
            if opener and opener.get('opening_line'):
                notes_parts.append(f"Open={opener['opening_line']}")

            picks.append({
                'sport': gi.get('sport', ''),
                'event_id': eid,
                'commence': gi.get('commence', ''),
                'home': gi.get('home', ''),
                'away': gi.get('away', ''),
                'market_type': 'PROP',
                'selection': f"{player} {side} {line_val} {label}",
                'book': book,
                'line': line_val,
                'odds': ce['odds'],
                'model_spread': None,
                'model_prob': round(ce['model_prob'], 4),
                'implied_prob': round(ce['implied_prob'], 4),
                'edge_pct': round(final_edge, 2),
                'star_rating': stars,
                'units': kelly_units(edge_pct=final_edge, odds=ce['odds'], fraction=0.25),  # 1/4 Kelly for props (raised from 1/8)
                'confidence': conf,
                'spread_or_ml': 'PROP',
                'timing': 'EARLY' if stale_bonus > 0 else 'STANDARD',
                'notes': ' | '.join(notes_parts) +
                         f" | {gi.get('home','')} vs {gi.get('away','')} {est_time}",
                '_signals': {
                    'consensus_edge': round(base_edge, 2),
                    'movement_bonus': round(movement_bonus, 2),
                    'stale_bonus': round(stale_bonus, 2),
                    'hist_bonus': round(hist_bonus, 2),
                    'final_edge': round(final_edge, 2),
                    'book_count': consensus['book_count'],
                },
            })

    # ── Dedup: Best play per player per prop type per event ──
    seen = set()
    deduped = []
    picks.sort(key=lambda x: x['edge_pct'], reverse=True)

    for p in picks:
        sel = p['selection']
        parts = sel.split()
        # Find OVER/UNDER to split player name from rest
        dedup_key = p['event_id'] + '|'
        for i, part in enumerate(parts):
            if part in ('OVER', 'UNDER'):
                dedup_key += ' '.join(parts[:i]) + '|' + parts[-1]
                break

        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        deduped.append(p)

    deduped.sort(key=lambda x: x['star_rating'] * 100 + x['edge_pct'], reverse=True)

    print(f"  ✅ {len(deduped)} prop plays found (Scotty's Edge consensus method, {total_players} players analyzed)")
    active = [k for k, v in signal_counts.items() if v > 0]
    if active:
        print(f"     Active signals: {', '.join(f'{k}({v})' for k, v in signal_counts.items() if v > 0)}")
    if close: conn.close()
    return deduped


def _ensure_tables(conn):
    """Create new v10 tables if they don't exist (safe migration)."""
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS prop_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at TEXT NOT NULL, sport TEXT NOT NULL,
            event_id TEXT NOT NULL, commence_time TEXT,
            home TEXT NOT NULL, away TEXT NOT NULL,
            book TEXT NOT NULL, market TEXT NOT NULL,
            player TEXT NOT NULL, side TEXT NOT NULL,
            line REAL, odds REAL, implied_prob REAL)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ps_player ON prop_snapshots(player, market, event_id)")

        conn.execute("""CREATE TABLE IF NOT EXISTS prop_openers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_seen TEXT NOT NULL, sport TEXT NOT NULL,
            event_id TEXT NOT NULL, player TEXT NOT NULL,
            market TEXT NOT NULL, opening_line REAL,
            opening_over_odds REAL, opening_under_odds REAL,
            UNIQUE(event_id, player, market))""")

        conn.execute("""CREATE TABLE IF NOT EXISTS player_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date TEXT NOT NULL, sport TEXT NOT NULL,
            event_id TEXT NOT NULL, player TEXT NOT NULL,
            stat_type TEXT NOT NULL, actual_value REAL,
            prop_line REAL, result TEXT,
            UNIQUE(event_id, player, stat_type))""")

        conn.commit()
    except:
        pass  # Tables already exist


# ═══════════════════════════════════════════════════════════════════
# DISPLAY
# ═══════════════════════════════════════════════════════════════════

def print_props(picks):
    if not picks:
        print("  No prop edges found.")
        return
    print(f"\n  ── PLAYER PROPS ({len(picks)} plays) {'─'*30}")
    for p in picks:
        icon = '🔥' if p['confidence'] == 'ELITE' else '🔥'
        timing = '⚡STALE' if p.get('timing') == 'EARLY' else ''
        print(f"\n  {icon} {p['selection']}  [{p['star_rating']}★ {p['confidence']}] {timing}")
        print(f"     Book: {p['book']} | Odds: {p['odds']:+.0f} | Edge: {p['edge_pct']:.1f}%")
        signals = p.get('_signals', {})
        if signals:
            parts = [f"Consensus={signals.get('consensus_edge',0):.1f}%"]
            if signals.get('movement_bonus', 0) != 0:
                parts.append(f"Movement={signals['movement_bonus']:+.1f}%")
            if signals.get('stale_bonus', 0) > 0:
                parts.append(f"STALE={signals['stale_bonus']:.1f}%")
            if signals.get('hist_bonus', 0) > 0:
                parts.append(f"History={signals['hist_bonus']:.1f}%")
            print(f"     Signals: {' + '.join(parts)} => Final={signals.get('final_edge',0):.1f}%")
        print(f"     {p['notes']}")


if __name__ == '__main__':
    picks = evaluate_props()
    print_props(picks)
