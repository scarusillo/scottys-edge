"""
player_prop_model.py — Scotty's Edge Player Prop Projection Engine

Unlike props_engine.py (which finds books that disagree with each other),
this module builds its OWN projections from box score data and compares
to the market — the same philosophy as model_engine.py for game lines.

Flow:
  1. Player baseline: recency-weighted average from last 20 box score games
  2. Opponent adjustment: does this opponent allow more/less of this stat?
  3. Context: B2B fatigue, pace, home/away
  4. Projection: baseline × opponent_mult × context_mult
  5. Edge: CDF probability that actual > market line, minus implied prob
  6. Fire: OVER only, edge >= 6%, 1/4 Kelly

Data sources:
  - box_scores: player stat lines per game (pts, reb, ast, threes, blk, stl, sog, hockey_pts)
  - results: opponent defensive averages
  - context_engine: B2B, pace, home/away
  - props: current market lines from sportsbooks
"""
import sqlite3, os, math
from datetime import datetime, timedelta, timezone
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

from scottys_edge import get_star_rating, kelly_units
from props_engine import (
    american_to_implied, STAT_TYPE_MAP, PROP_LABEL,
    EXCLUDED_BOOKS, NY_LEGAL_BOOKS,
)

# ═══════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

MIN_PLAYER_GAMES = 5
MIN_OPP_GAMES = 10
DECAY_RATE = 0.92  # 0.92^10 = 0.43 → game 10 ago has 43% weight

# Population-level std defaults (used when player has < 8 games)
DEFAULT_STD = {
    'pts': 6.0, 'reb': 2.5, 'ast': 2.0, 'threes': 1.2,
    'blk': 0.8, 'stl': 0.7,
    'sog': 1.5, 'hockey_pts': 0.8, 'blocked_shots': 0.8,
    # MLB batting — high variance sports, these are empirical ranges
    'hits': 0.9, 'hr': 0.5, 'rbi': 1.2, 'runs': 0.8,
    'walks': 0.7, 'batter_k': 0.9, 'total_bases': 1.5,
    'stolen_bases': 0.4, 'at_bats': 0.8,
    # MLB pitching
    'pitcher_k': 2.5, 'pitcher_outs': 4.0, 'pitcher_ip': 1.5,
    'pitcher_er': 1.5, 'pitcher_h_allowed': 1.8, 'pitcher_bb': 1.0,
}

# Context multipliers
B2B_FATIGUE_MULT = 0.90
HOME_BOOST = 1.02
AWAY_PENALTY = 0.98

# Edge thresholds
# Backtest 3/23: 30% worked on stale lines but overconfidence cap (0.75 prob)
# limits max live edge to ~22.6%. 30% is mathematically unreachable.
# Live graded data: 20-30% edge is 6W-3L +15.7u (sweet spot).
# 15-20% is 3W-2L -0.8u (breakeven). Set to 18% — captures the profitable
# range while the MAX_PROP_PICKS=5 cap keeps volume manageable.
MIN_EDGE_PCT = 18.0
MIN_STARS = 2.0
MAX_PROP_PICKS = 5  # Max props per card (top N by edge)


# ═══════════════════════════════════════════════════════════════════
# MATH — Normal CDF (same as model_engine.py)
# ═══════════════════════════════════════════════════════════════════

def _ncdf(z):
    """Abramowitz & Stegun approximation of normal CDF."""
    if z > 6: return 1.0
    if z < -6: return 0.0
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if z >= 0 else -1
    x = abs(z)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1)*t * math.exp(-x*x/2.0)
    return 0.5 * (1.0 + sign * y)


# ═══════════════════════════════════════════════════════════════════
# PLAYER BASELINE — Recency-weighted rolling average
# ═══════════════════════════════════════════════════════════════════

def get_player_baseline(conn, player, stat_type, sport, limit=20):
    """
    Compute recency-weighted average and std from box_scores.
    Returns None if insufficient data.
    """
    rows = conn.execute("""
        SELECT stat_value FROM box_scores
        WHERE player = ? AND stat_type = ? AND sport = ?
        ORDER BY game_date DESC
        LIMIT ?
    """, (player, stat_type, sport, limit)).fetchall()

    if len(rows) < MIN_PLAYER_GAMES:
        return None

    values = [r[0] for r in rows]
    weights = [DECAY_RATE ** i for i in range(len(values))]
    total_w = sum(weights)

    # Weighted average
    avg = sum(v * w for v, w in zip(values, weights)) / total_w

    # Weighted standard deviation
    # Use actual data when available (5-7 games), blended with population default.
    # Only fall back to pure default if we somehow have < 5 games (shouldn't happen).
    variance = sum(w * (v - avg) ** 2 for v, w in zip(values, weights)) / total_w
    empirical_std = max(0.5, math.sqrt(variance))
    if len(values) >= 8:
        std = empirical_std
    else:
        # Blend: weight empirical by (games/8), fill remainder with population default
        pop_std = DEFAULT_STD.get(stat_type, 3.0)
        blend = len(values) / 8.0
        std = empirical_std * blend + pop_std * (1.0 - blend)

    return {'avg': round(avg, 2), 'std': round(std, 2), 'games': len(values), 'values': values}


# ═══════════════════════════════════════════════════════════════════
# OPPONENT DEFENSE — How much of this stat does the opponent allow?
# ═══════════════════════════════════════════════════════════════════

def get_opponent_defense(conn, opponent, stat_type, sport):
    """
    Compare how much stat_type the opponent allows vs league average.
    Returns a multiplier (>1.0 = opponent allows more = boost projection).
    """
    # League average: avg stat_value per player per game
    league = conn.execute("""
        SELECT AVG(stat_value), COUNT(DISTINCT espn_game_id)
        FROM box_scores WHERE stat_type = ? AND sport = ?
    """, (stat_type, sport)).fetchone()

    if not league or not league[0]:
        return None
    league_avg = league[0]

    # Opponent-allowed: avg stat_value for players NOT on this team,
    # in games where this team played
    opp = conn.execute("""
        SELECT AVG(bs.stat_value), COUNT(DISTINCT bs.espn_game_id)
        FROM box_scores bs
        WHERE bs.stat_type = ?
          AND bs.sport = ?
          AND bs.espn_game_id IN (
              SELECT DISTINCT espn_game_id FROM box_scores
              WHERE team = ? AND sport = ?
          )
          AND bs.team != ?
    """, (stat_type, sport, opponent, sport, opponent)).fetchone()

    if not opp or not opp[0]:
        return None

    opp_avg = opp[0]
    opp_games = opp[1] or 0

    if opp_games < MIN_OPP_GAMES:
        return None

    # Raw multiplier
    raw_mult = opp_avg / league_avg if league_avg > 0 else 1.0

    # Regress toward 1.0 for small samples
    confidence = min(opp_games, 30) / 30.0
    multiplier = 1.0 + (raw_mult - 1.0) * confidence

    return {
        'multiplier': round(multiplier, 3),
        'opp_avg': round(opp_avg, 2),
        'league_avg': round(league_avg, 2),
        'games': opp_games,
    }


# ═══════════════════════════════════════════════════════════════════
# CONTEXT — B2B, pace, home/away
# ═══════════════════════════════════════════════════════════════════

def get_player_context(conn, team, home, away, sport, commence):
    """
    Gather context adjustments applicable at the player level.
    Returns a combined multiplier.
    """
    combined = 1.0
    factors = {}

    # B2B detection
    try:
        from context_engine import _days_since_last_game
        rest = _days_since_last_game(conn, team, sport, commence)
        if rest is not None and rest <= 1.2:
            combined *= B2B_FATIGUE_MULT
            factors['b2b'] = True
    except (ImportError, Exception):
        pass

    # Pace of play
    try:
        from context_engine import pace_of_play_adjustment
        pace_adj, pace_info = pace_of_play_adjustment(conn, home, away, sport)
        if pace_adj != 0:
            # Convert team-level total adjustment to player multiplier
            # Get league average total to normalize
            league_total = conn.execute("""
                SELECT AVG(actual_total) FROM results
                WHERE sport = ? AND completed = 1 AND actual_total IS NOT NULL
            """, (sport,)).fetchone()
            if league_total and league_total[0]:
                pace_mult = 1.0 + (pace_adj / league_total[0]) * 0.5
                pace_mult = max(0.90, min(1.10, pace_mult))
                combined *= pace_mult
                factors['pace'] = round(pace_adj, 1)
    except (ImportError, Exception):
        pass

    # Home/away
    is_home = (team == home)
    if is_home:
        combined *= HOME_BOOST
        factors['venue'] = 'home'
    else:
        combined *= AWAY_PENALTY
        factors['venue'] = 'away'

    return {'combined_mult': round(combined, 3), 'factors': factors}


# ═══════════════════════════════════════════════════════════════════
# PROJECTION — Combine baseline × opponent × context
# ═══════════════════════════════════════════════════════════════════

def project_player_stat(conn, player, stat_type, sport, team, opponent, home, away, commence):
    """
    Project a player's stat value for a specific game.
    Returns None if insufficient data.
    """
    baseline = get_player_baseline(conn, player, stat_type, sport)
    if not baseline:
        return None

    opp_def = get_opponent_defense(conn, opponent, stat_type, sport)
    opp_mult = opp_def['multiplier'] if opp_def else 1.0

    ctx = get_player_context(conn, team, home, away, sport, commence)
    ctx_mult = ctx['combined_mult']

    projection = baseline['avg'] * opp_mult * ctx_mult
    projection = max(0.0, projection)

    return {
        'projection': round(projection, 2),
        'std': baseline['std'],
        'games': baseline['games'],
        'baseline_avg': baseline['avg'],
        'opp_mult': opp_mult,
        'ctx_mult': ctx_mult,
        'factors': ctx.get('factors', {}),
        'values': baseline.get('values', []),
    }


# ═══════════════════════════════════════════════════════════════════
# EDGE CALCULATION — CDF probability vs implied probability
# ═══════════════════════════════════════════════════════════════════

def _binary_over_prob(projection, line):
    """
    For binary lines (0.5), estimate P(actual >= 1) directly from the
    player's average rather than using a normal CDF which is inappropriate
    for discrete 0-or-1+ outcomes.

    Uses a Poisson-like model: P(X >= 1) = 1 - e^(-avg).
    This is well-suited for low-count stats like blocks and steals.
    """
    # Poisson P(X=0) = e^(-lambda), so P(X >= 1) = 1 - e^(-lambda)
    return 1.0 - math.exp(-projection)


def calculate_prop_edge(projection, std, market_line, odds, season_values=None):
    """
    Calculate edge for an OVER bet.
    Uses Poisson model for binary 0.5 lines (blocks, steals, etc.)
    and normal CDF for continuous lines (points, rebounds, assists).

    Applies overconfidence cap: when model_prob > 0.70, blend with market
    implied prob to prevent claiming huge edges on volatile props.
    Hard cap at 0.75 unless season hit rate over the line is >= 65%.
    """
    diff = projection - market_line
    if diff <= 0:
        return 0.0, 0.0  # No OVER edge

    if std <= 0:
        return 0.0, 0.0

    # Binary lines: use Poisson instead of normal CDF
    if market_line == 0.5:
        raw_prob = _binary_over_prob(projection, market_line)
    else:
        z = diff / std
        raw_prob = _ncdf(z)

    implied = american_to_implied(odds)
    if not implied or implied <= 0:
        return 0.0, 0.0

    # --- Overconfidence cap ---
    # Compute season hit rate over the line if box score values provided
    season_hit_rate = None
    if season_values and len(season_values) >= 5:
        hits = sum(1 for v in season_values if v > market_line)
        season_hit_rate = hits / len(season_values)

    prob = raw_prob

    # Blend with market implied prob when model is very confident
    if prob > 0.70:
        prob = 0.6 * prob + 0.4 * implied

    # Hard cap at 0.75 unless season hit rate justifies higher
    if prob > 0.75:
        if season_hit_rate is None or season_hit_rate < 0.65:
            prob = 0.75

    edge_pct = (prob - implied) * 100.0
    return max(0.0, round(edge_pct, 2)), round(prob, 4)


# ═══════════════════════════════════════════════════════════════════
# PLAYER TEAM LOOKUP
# ═══════════════════════════════════════════════════════════════════

def _get_player_team(conn, player, sport):
    """Get the player's current team from most recent box score."""
    row = conn.execute("""
        SELECT team FROM box_scores
        WHERE player = ? AND sport = ?
        ORDER BY game_date DESC LIMIT 1
    """, (player, sport)).fetchone()
    return row[0] if row else None


def _match_team(team_name, home, away):
    """Fuzzy match a box score team name to home or away."""
    if not team_name:
        return None
    tn = team_name.lower()
    # Try exact
    if tn == home.lower():
        return home
    if tn == away.lower():
        return away
    # Try substring
    for name in [home, away]:
        # Match last word (e.g., "Celtics" in "Boston Celtics")
        parts = name.split()
        if parts and parts[-1].lower() in tn:
            return name
        if parts and tn in name.lower():
            return name
    return None


# ═══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT — Generate prop projections
# ═══════════════════════════════════════════════════════════════════

def generate_prop_projections(conn=None):
    """
    Scan today's props, project each player's stats, and generate OVER picks
    where our projection exceeds the market line by enough.
    """
    close = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close = True

    now_utc = datetime.now(timezone.utc)
    window_start = (now_utc - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Get today's prop lines
    rows = conn.execute("""
        SELECT sport, event_id, commence_time, home, away,
               book, market, selection, line, odds
        FROM props
        WHERE market IN ('player_points','player_rebounds','player_assists','player_threes',
                         'player_blocks','player_steals',
                         'player_shots_on_goal','player_power_play_points','player_blocked_shots',
                         'player_shots','player_shots_on_target',
                         'batter_hits','batter_total_bases','batter_home_runs','batter_rbis',
                         'batter_runs_scored','batter_strikeouts','batter_stolen_bases','batter_walks',
                         'pitcher_strikeouts','pitcher_outs','pitcher_hits_allowed',
                         'pitcher_earned_runs','pitcher_walks')
        AND commence_time >= ?
        ORDER BY commence_time
    """, (window_start,)).fetchall()

    if not rows:
        if close:
            conn.close()
        return []

    # Parse: group by (event_id, player, market)
    # For each group, collect all books' OVER lines
    grouped = defaultdict(list)  # key: (eid, player, market) → list of {book, line, odds}
    game_info = {}

    for sport, eid, commence, home, away, book, market, selection, line, odds in rows:
        # Parse player name and side from selection
        player = None
        side = None
        if ' - Over' in selection:
            player = selection.split(' - Over')[0].strip()
            side = 'Over'
        elif ' - Under' in selection:
            player = selection.split(' - Under')[0].strip()
            side = 'Under'
        if not player or side != 'Over':
            continue  # OVER only

        game_info[eid] = {'sport': sport, 'home': home, 'away': away, 'commence': commence}
        grouped[(eid, player, market)].append({
            'book': book, 'line': line, 'odds': odds,
        })

    # Process each player/market group
    picks = []
    projected_count = 0
    edge_count = 0

    # Cache player teams and opponent defense to avoid repeated queries
    _team_cache = {}
    _opp_cache = {}

    for (eid, player, market), book_entries in grouped.items():
        gi = game_info.get(eid)
        if not gi:
            continue

        sport = gi['sport']
        home = gi['home']
        away = gi['away']
        commence = gi['commence']

        # Map market to stat_type
        stat_type = STAT_TYPE_MAP.get(market)
        if not stat_type:
            continue

        # Get player's team
        cache_key = (player, sport)
        if cache_key not in _team_cache:
            _team_cache[cache_key] = _get_player_team(conn, player, sport)
        player_team_raw = _team_cache[cache_key]

        if not player_team_raw:
            continue

        # Match to home/away
        player_team = _match_team(player_team_raw, home, away)
        if not player_team:
            continue
        opponent = away if player_team == home else home

        # Project this player's stat
        proj = project_player_stat(conn, player, stat_type, sport,
                                   player_team, opponent, home, away, commence)
        if not proj:
            continue
        projected_count += 1

        # Find best OVER line across legal books
        # Use median line as the market consensus
        legal_entries = [e for e in book_entries if e['book'] not in EXCLUDED_BOOKS]
        if not legal_entries:
            continue

        # For each book offering this OVER
        for entry in legal_entries:
            book = entry['book']
            line = entry['line']
            odds = entry['odds']

            if book not in NY_LEGAL_BOOKS:
                continue

            edge, capped_prob = calculate_prop_edge(
                proj['projection'], proj['std'], line, odds,
                season_values=proj.get('values'),
            )
            if edge < MIN_EDGE_PCT:
                continue

            stars = get_star_rating(edge)
            if stars < MIN_STARS:
                continue

            edge_count += 1
            label = PROP_LABEL.get(market, market.replace('player_', '').upper())
            units = kelly_units(edge_pct=edge, odds=odds, fraction=0.25)
            conf = 'ELITE' if stars >= 2.5 else 'HIGH'

            notes = (f"Proj={proj['projection']:.1f} Mkt={line} "
                     f"OppDef={proj['opp_mult']:.2f} Ctx={proj['ctx_mult']:.2f} "
                     f"Std={proj['std']:.1f} Games={proj['games']} "
                     f"Edge={edge:.1f}% | {home} vs {away}")

            picks.append({
                'sport': sport,
                'event_id': eid,
                'commence': commence,
                'home': home,
                'away': away,
                'market_type': 'PROP',
                'selection': f"{player} OVER {line} {label}",
                'book': book,
                'line': line,
                'odds': odds,
                'model_spread': None,
                'model_prob': capped_prob,
                'implied_prob': round(american_to_implied(odds) or 0, 4),
                'edge_pct': round(edge, 2),
                'star_rating': stars,
                'units': units,
                'confidence': conf,
                'spread_or_ml': 'PROP',
                'timing': 'STANDARD',
                'notes': notes,
                '_signals': {
                    'projection': proj['projection'],
                    'baseline_avg': proj['baseline_avg'],
                    'std': proj['std'],
                    'opp_mult': proj['opp_mult'],
                    'ctx_mult': proj['ctx_mult'],
                    'edge_pct': edge,
                    'book_count': len(legal_entries),
                },
                '_source': 'MODEL',
            })

    # Dedup: best edge per player per stat per event
    seen = set()
    deduped = []
    picks.sort(key=lambda x: x['edge_pct'], reverse=True)
    for p in picks:
        # Extract player name from selection
        sel = p['selection']
        parts = sel.split(' OVER ')
        dk = f"{p['event_id']}|{parts[0]}|{sel.split()[-1]}"
        if dk in seen:
            continue
        seen.add(dk)
        deduped.append(p)

    # Cap at MAX_PROP_PICKS (already sorted by edge desc)
    final = deduped[:MAX_PROP_PICKS]

    print(f"  Player Prop Model: {projected_count} players projected, "
          f"{edge_count} edges found, {len(deduped)} qualifying, {len(final)} selected (cap={MAX_PROP_PICKS})")

    if close:
        conn.close()
    return final


# ═══════════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    picks = generate_prop_projections(conn)
    if picks:
        print(f"\n{'='*70}")
        print(f"  {len(picks)} PROP PROJECTIONS")
        print(f"{'='*70}")
        for p in sorted(picks, key=lambda x: x['edge_pct'], reverse=True):
            print(f"  {p['selection']:40} {p['book']:15} odds={p['odds']:+.0f} "
                  f"edge={p['edge_pct']:.1f}% units={p['units']:.1f} | {p['notes']}")
    else:
        print("  No prop projections found.")
    conn.close()
