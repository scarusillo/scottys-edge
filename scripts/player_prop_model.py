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
from box_scores import get_player_batting_order

# ═══════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

MIN_PLAYER_GAMES = 15  # v24: Was 5 — too low, firing on early-season noise (Murakami 10 games)
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
# v24: Unified 20% edge floor. Graded data: 20%+ is 6W-3L +15.7u.
# 15-20% was 3W-2L -0.8u (breakeven noise). Only MAX PLAYs.
MIN_EDGE_PCT = 20.0
MIN_STARS = 2.0
MAX_PROP_PICKS = 3  # v24: Reduced from 5 — props are a selective add-on, not the main card
MAX_PROP_ODDS = 200  # No props above +200 (no data to support higher)
MAX_PROP_EDGE = 25.0  # Cap edge like game lines — extreme edges are overestimates


# ═══════════════════════════════════════════════════════════════════
# BATTING ORDER POSITIONAL ADJUSTMENT (MLB only)
# ═══════════════════════════════════════════════════════════════════
# P(>=1 stat) by batting order position (empirical from 30 games, 60 team-games).
# A leadoff hitter has 23% RBI rate vs 46% for the 3-spot — the model must
# account for this or it will fire identical projections for mismatched slots.

BATTING_ORDER_RATES = {
    'rbi':  {1: 0.23, 2: 0.40, 3: 0.46, 4: 0.36, 5: 0.42, 6: 0.31, 7: 0.39, 8: 0.38, 9: 0.34},
    'runs': {1: 0.46, 2: 0.43, 3: 0.40, 4: 0.35, 5: 0.39, 6: 0.33, 7: 0.34, 8: 0.36, 9: 0.32},
    'hits': {1: 0.60, 2: 0.61, 3: 0.64, 4: 0.59, 5: 0.61, 6: 0.55, 7: 0.56, 8: 0.46, 9: 0.60},
}
BATTING_ORDER_AVG = {
    'rbi':  0.37,   # avg P(>=1 RBI) across all 9 positions
    'runs': 0.38,
    'hits': 0.58,
}

# Pre-computed multipliers: position_rate / league_avg_rate
# e.g., leadoff RBI = 0.23 / 0.37 = 0.622 (38% penalty)
# e.g., 3-hole RBI  = 0.46 / 0.37 = 1.243 (24% boost)
BATTING_ORDER_MULTIPLIERS = {}
for _stat, _rates in BATTING_ORDER_RATES.items():
    _avg = BATTING_ORDER_AVG[_stat]
    BATTING_ORDER_MULTIPLIERS[_stat] = {
        pos: round(rate / _avg, 3) for pos, rate in _rates.items()
    }

# Stat types that receive batting order adjustment (batter counting stats only)
BAT_ORDER_ELIGIBLE_STATS = {'rbi', 'runs', 'hits'}


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
# MLB MATCHUP — Opposing pitcher (for batters) / lineup (for pitchers)
# ═══════════════════════════════════════════════════════════════════

# Batter stat types that should be adjusted by opposing pitcher quality
BATTER_STATS = {'hits', 'hr', 'rbi', 'runs', 'walks', 'batter_k', 'total_bases',
                'stolen_bases', 'at_bats'}
# Pitcher stat types that should be adjusted by opposing lineup quality
PITCHER_STATS = {'pitcher_k', 'pitcher_outs', 'pitcher_ip', 'pitcher_er',
                 'pitcher_h_allowed', 'pitcher_bb'}

# Which pitcher metric suppresses/boosts which batter stat.
# Lower ERA/WHIP = harder for batters. Higher K/9 = more Ks for batters.
_PITCHER_METRIC = {
    'hits': 'whip',       # WHIP directly measures hits+walks allowed per IP
    'total_bases': 'era',  # ERA proxies overall damage allowed
    'hr': 'era',
    'rbi': 'era',
    'runs': 'era',
    'walks': 'whip',
    'batter_k': 'k9',     # High K/9 pitcher = MORE batter Ks (inverse)
    'stolen_bases': None,  # Not pitcher-dependent
    'at_bats': None,
}

# Lineup quality metric for pitcher props
_LINEUP_METRIC = {
    'pitcher_k': 'batter_k',       # Lineup that strikes out a lot = more pitcher Ks
    'pitcher_outs': None,           # Not lineup-dependent (pitcher's own workload)
    'pitcher_ip': None,
    'pitcher_er': 'runs',           # Lineup that scores runs = more earned runs
    'pitcher_h_allowed': 'hits',    # Lineup that gets hits = more hits allowed
    'pitcher_bb': 'walks',          # Lineup that draws walks = more walks
}

# League average benchmarks (updated from current data)
_LEAGUE_AVG = {'era': 4.14, 'k9': 9.21, 'whip': 1.30}


def get_opposing_pitcher_mult(conn, opponent, home, away, stat_type, commence):
    """
    For BATTER props: adjust projection based on the opposing starting pitcher.
    A soft pitcher (high ERA/WHIP) boosts batter projections.
    An ace (low ERA/WHIP) suppresses them.
    Returns a multiplier (>1.0 = favorable matchup, <1.0 = tough matchup).
    """
    metric = _PITCHER_METRIC.get(stat_type)
    if not metric:
        return 1.0, None

    # Find today's probable pitcher for the opponent
    game_date = commence[:10] if commence else datetime.now().strftime('%Y-%m-%d')
    # Opponent is pitching — find their starter
    pitcher_row = conn.execute("""
        SELECT home_pitcher, away_pitcher,
               home_pitcher_season_era, away_pitcher_season_era,
               home_pitcher_season_k9, away_pitcher_season_k9,
               home_pitcher_season_whip, away_pitcher_season_whip,
               home, away
        FROM mlb_probable_pitchers
        WHERE game_date = ? AND (
            (home = ? AND away = ?) OR (home = ? AND away = ?)
        )
        ORDER BY fetched_at DESC LIMIT 1
    """, (game_date, home, away, away, home)).fetchone()

    if not pitcher_row:
        return 1.0, None

    # Determine which pitcher the batter faces (opponent's pitcher)
    if opponent == pitcher_row[8]:  # opponent is home
        sp_name = pitcher_row[0]
        sp_era = pitcher_row[2]
        sp_k9 = pitcher_row[4]
        sp_whip = pitcher_row[6]
    else:  # opponent is away
        sp_name = pitcher_row[1]
        sp_era = pitcher_row[3]
        sp_k9 = pitcher_row[5]
        sp_whip = pitcher_row[7]

    if not sp_name:
        return 1.0, None

    # ═══ RECENCY BLEND: use last 3 starts to capture current form ═══
    # Season ERA can mask a pitcher who's hot right now (e.g., 1.38 recent vs 4.0 season)
    # Blend: 50% recent form + 50% season, if recent data available
    recent_era = None
    recent_k9 = None
    try:
        recent_starts = conn.execute("""
            SELECT innings_pitched, earned_runs, strikeouts
            FROM pitcher_stats
            WHERE pitcher_name LIKE ? AND is_starter = 1
            AND innings_pitched >= 3.0
            ORDER BY game_date DESC LIMIT 3
        """, (f"%{sp_name}%",)).fetchall()
        if len(recent_starts) >= 2:
            total_ip = sum(r[0] for r in recent_starts)
            total_er = sum(r[1] for r in recent_starts)
            total_k = sum(r[2] for r in recent_starts)
            if total_ip > 0:
                recent_era = (total_er / total_ip) * 9.0
                recent_k9 = (total_k / total_ip) * 9.0
    except Exception:
        pass

    # Get the relevant metric — blend season + recent
    if metric == 'era':
        if recent_era is not None and sp_era and sp_era > 0:
            blended_era = recent_era * 0.5 + sp_era * 0.5
            raw_mult = blended_era / _LEAGUE_AVG['era']
        elif sp_era and sp_era > 0:
            raw_mult = sp_era / _LEAGUE_AVG['era']
        else:
            return 1.0, None
    elif metric == 'whip' and sp_whip and sp_whip > 0:
        raw_mult = sp_whip / _LEAGUE_AVG['whip']
    elif metric == 'k9':
        if recent_k9 is not None and sp_k9 and sp_k9 > 0:
            blended_k9 = recent_k9 * 0.5 + sp_k9 * 0.5
            raw_mult = blended_k9 / _LEAGUE_AVG['k9']
        elif sp_k9 and sp_k9 > 0:
            raw_mult = sp_k9 / _LEAGUE_AVG['k9']
        else:
            return 1.0, None
    else:
        return 1.0, None

    # Regress toward 1.0 — don't let a single pitcher stat swing too hard
    # Cap at ±20% adjustment
    mult = 1.0 + (raw_mult - 1.0) * 0.6  # 60% weight on the signal
    mult = max(0.80, min(1.20, mult))

    return round(mult, 3), sp_name


def get_opposing_lineup_mult(conn, team, opponent, stat_type, sport):
    """
    For PITCHER props: adjust projection based on the opposing lineup quality.
    A lineup that strikes out a lot = more pitcher Ks.
    A lineup that gets lots of hits = more hits allowed.
    Returns a multiplier.
    """
    lineup_stat = _LINEUP_METRIC.get(stat_type)
    if not lineup_stat:
        return 1.0

    # Get the opposing team's batting average for the relevant stat
    # compared to league average (same approach as get_opponent_defense)
    league = conn.execute("""
        SELECT AVG(stat_value), COUNT(DISTINCT espn_game_id)
        FROM box_scores WHERE stat_type = ? AND sport = ?
    """, (lineup_stat, sport)).fetchone()

    if not league or not league[0]:
        return 1.0
    league_avg = league[0]

    # Opponent's batting in this stat
    opp = conn.execute("""
        SELECT AVG(stat_value), COUNT(DISTINCT espn_game_id)
        FROM box_scores
        WHERE stat_type = ? AND sport = ? AND team = ?
    """, (lineup_stat, sport, opponent)).fetchone()

    if not opp or not opp[0]:
        return 1.0

    opp_avg = opp[0]
    opp_games = opp[1] or 0
    if opp_games < MIN_OPP_GAMES:
        return 1.0

    raw_mult = opp_avg / league_avg if league_avg > 0 else 1.0

    # Regress toward 1.0 for sample size
    confidence = min(opp_games, 30) / 30.0
    mult = 1.0 + (raw_mult - 1.0) * confidence * 0.6  # 60% weight
    mult = max(0.85, min(1.15, mult))

    return round(mult, 3)


# ═══════════════════════════════════════════════════════════════════
# PROJECTION — Combine baseline × opponent × context × matchup
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

    # v24: MLB matchup adjustments
    matchup_mult = 1.0
    matchup_detail = None
    if 'baseball' in sport:
        if stat_type in BATTER_STATS:
            matchup_mult, matchup_detail = get_opposing_pitcher_mult(
                conn, opponent, home, away, stat_type, commence)
        elif stat_type in PITCHER_STATS:
            matchup_mult = get_opposing_lineup_mult(
                conn, team, opponent, stat_type, sport)

    # v25: Batting order positional adjustment (MLB batters only)
    # Only apply when today's position DIFFERS from the player's historical norm.
    # The baseline already reflects their usual batting spot (last 20 games).
    # A power hitter like Cruz who always bats leadoff already has a low RBI
    # baseline — applying the leadoff penalty again would double-penalize.
    # The multiplier catches lineup CHANGES: moved up = boost, moved down = penalty.
    bat_order_mult = 1.0
    bat_order_pos = None
    if 'baseball' in sport and stat_type in BAT_ORDER_ELIGIBLE_STATS:
        bat_pos = get_player_batting_order(conn, player, sport, home, away, commence)
        if bat_pos and stat_type in BATTING_ORDER_MULTIPLIERS:
            bat_order_pos = bat_pos
            # Get historical norm — most common position over last 10 games
            _hist_pos = None
            try:
                _hist_row = conn.execute("""
                    SELECT bat_order, COUNT(*) as cnt FROM batting_order
                    WHERE player = ? AND is_starter = 1
                    GROUP BY bat_order ORDER BY cnt DESC LIMIT 1
                """, (player,)).fetchone()
                if _hist_row:
                    _hist_pos = _hist_row[0]
            except Exception:
                pass

            if _hist_pos and _hist_pos != bat_pos:
                # Player moved — apply relative adjustment (today vs historical)
                _today_rate = BATTING_ORDER_MULTIPLIERS[stat_type].get(bat_pos, 1.0)
                _hist_rate = BATTING_ORDER_MULTIPLIERS[stat_type].get(_hist_pos, 1.0)
                bat_order_mult = _today_rate / _hist_rate if _hist_rate != 0 else 1.0
            # If same position or no history, mult stays 1.0 (baseline already reflects it)

    projection = baseline['avg'] * opp_mult * ctx_mult * matchup_mult * bat_order_mult
    projection = max(0.0, projection)

    return {
        'projection': round(projection, 2),
        'std': baseline['std'],
        'games': baseline['games'],
        'baseline_avg': baseline['avg'],
        'opp_mult': opp_mult,
        'ctx_mult': ctx_mult,
        'matchup_mult': matchup_mult,
        'matchup_detail': matchup_detail,
        'bat_order_mult': bat_order_mult,
        'bat_order_pos': bat_order_pos,
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
    window_start = now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

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

    # v25: ALL MLB props have stale morning lines that reprice as lineups lock.
    # Batter props move -5 to -7%; pitcher props (hits_allowed, earned_runs)
    # also move as opposing lineups are confirmed. Lines stabilize ~2-3 hours
    # before first pitch. Only evaluate MLB props within this window.
    MLB_PROP_WINDOW_HOURS = 3

    for sport, eid, commence, home, away, book, market, selection, line, odds in rows:
        # Skip started games
        try:
            gt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
            if gt < now_utc - timedelta(minutes=5):
                continue
        except Exception:
            gt = None

        # v25: MLB prop timing gate — only fire within 3 hours of game time
        if gt and 'baseball' in (sport or ''):
            hours_until = (gt - now_utc).total_seconds() / 3600
            if hours_until > MLB_PROP_WINDOW_HOURS:
                continue

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

        # ═══ INJURY GATE: Skip props for players ruled Out/Doubtful ═══
        try:
            _inj_status = conn.execute("""
                SELECT status FROM injuries
                WHERE sport=? AND player LIKE ? AND report_date=?
                AND status IN ('Out','OUT','Doubtful','DOUBTFUL')
                LIMIT 1
            """, (sport, f'%{player.split()[-1]}%', datetime.now(timezone.utc).strftime('%Y-%m-%d'))).fetchone()
            if _inj_status:
                continue  # Player ruled out — skip prop
        except Exception:
            pass

        # ═══ BLOWOUT GATE: Block counting-stat props when player's team is big underdog ═══
        # In blowouts, counting stats collapse: no RBI in shutouts, early hooks kill K's
        BLOWOUT_GATED_STATS = {'rbi', 'runs', 'hits', 'total_bases', 'hr',
                               'pitcher_k', 'pitcher_outs', 'pitcher_h_allowed'}
        BLOWOUT_ML_THRESHOLD = 160  # +160 underdog or worse
        if sport == 'baseball_mlb' and stat_type in BLOWOUT_GATED_STATS:
            try:
                # Use CURRENT ML (latest pre-game line), fall back to OPENER
                _ml_row = conn.execute("""
                    SELECT best_home_ml, best_away_ml, model_spread, home
                    FROM market_consensus
                    WHERE event_id = ?
                    AND tag IN ('CURRENT', 'OPENER')
                    ORDER BY CASE tag WHEN 'CURRENT' THEN 1 ELSE 2 END
                    LIMIT 1
                """, (eid,)).fetchone()
                if _ml_row:
                    _h_ml, _a_ml, _ms, _mc_home = _ml_row
                    # Sanity check: skip absurd live lines (>+1000) and use OPENER
                    if _h_ml is not None and abs(_h_ml) > 1000:
                        _ml_row = conn.execute("""
                            SELECT best_home_ml, best_away_ml, model_spread, home
                            FROM market_consensus
                            WHERE event_id = ? AND tag = 'OPENER'
                            LIMIT 1
                        """, (eid,)).fetchone()
                        if _ml_row:
                            _h_ml, _a_ml, _ms, _mc_home = _ml_row
                    # Get the moneyline for the player's team
                    if player_team == _mc_home:
                        team_ml = _h_ml
                    else:
                        team_ml = _a_ml
                    # Block if team is +160 underdog or worse (big dog)
                    if team_ml is not None and team_ml >= BLOWOUT_ML_THRESHOLD:
                        continue
                    # Fallback: model_spread (negative = home favored)
                    if team_ml is None and _ms is not None:
                        team_spread = _ms if player_team == _mc_home else -_ms
                        if team_spread > 1.5:  # team projected to lose by 1.5+
                            continue
            except Exception:
                pass

        # Project this player's stat
        proj = project_player_stat(conn, player, stat_type, sport,
                                   player_team, opponent, home, away, commence)
        if not proj:
            continue
        projected_count += 1

        # ═══ HIT RATE CHECK: actual hit rate must beat implied breakeven ═══
        # The model projects averages, but binary props (0.5 lines) need hit RATE.
        # A player averaging 0.6 RBI might only clear 0.5 in 33% of games if
        # the distribution is lumpy (many 0s, few big games).
        _hit_rate_data = proj.get('values', [])

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

            # Skip high-odds props — no data to support +200 and beyond
            if odds > MAX_PROP_ODDS:
                continue

            # v25: Cross-book +200 hard cap — applies to ALL books, not just soft.
            # If ANY book carrying this prop has odds >+200, the market is saying
            # <33% probability. Even if our book is at +199, the prop is a longshot.
            _all_book_entries = [e for e in legal_entries if e['line'] == line]
            if any(e['odds'] > 200 for e in _all_book_entries):
                continue  # Market consensus: longshot prop

            # Hit rate gate: check actual clearing rate vs implied breakeven
            if _hit_rate_data and len(_hit_rate_data) >= 10:
                _hits = sum(1 for v in _hit_rate_data if v > line)
                _hit_rate = _hits / len(_hit_rate_data)
                _implied_break = american_to_implied(odds) or 0.5
                if _hit_rate < _implied_break:
                    continue  # Actual hit rate below breakeven — no real edge

            edge, capped_prob = calculate_prop_edge(
                proj['projection'], proj['std'], line, odds,
                season_values=proj.get('values'),
            )
            edge = min(edge, MAX_PROP_EDGE)  # Cap extreme edges
            if edge < MIN_EDGE_PCT:
                continue

            # v25: Cross-book validation for soft books (Fanatics, Caesars, FanDuel).
            # Soft books offer inflated prop odds (e.g., Fanatics cashback). If the
            # sharpest book on the same prop implies no edge, the "edge" is fake.
            # Two checks:
            #   1. Sharp book must show >=20% edge on the same prop
            #   2. No book can have odds >+200 (hard cap — means market thinks <33% prob)
            SOFT_PROP_BOOKS = {'Fanatics', 'Caesars', 'FanDuel'}
            SHARP_PROP_BOOKS = {'DraftKings', 'BetMGM', 'BetRivers'}
            if book in SOFT_PROP_BOOKS:
                _sharp_entries = [e for e in legal_entries
                                 if e['book'] in SHARP_PROP_BOOKS
                                 and e['line'] == line]
                if _sharp_entries:
                    # Hard cap: if ANY sharp book has odds >+200, kill the pick
                    _any_over_200 = any(e['odds'] > 200 for e in _sharp_entries)
                    if _any_over_200:
                        continue  # Sharp book at +200+ = market says <33% prob

                    # Soft check: sharp book must confirm >=20% edge
                    _sharp_best = max(e['odds'] for e in _sharp_entries)
                    _sharp_edge, _ = calculate_prop_edge(
                        proj['projection'], proj['std'], line, _sharp_best,
                        season_values=proj.get('values'),
                    )
                    _sharp_edge = min(_sharp_edge, MAX_PROP_EDGE)
                    if _sharp_edge < 20.0:
                        continue  # Sharp book doesn't confirm edge

            stars = get_star_rating(edge)
            if stars < MIN_STARS:
                continue

            edge_count += 1
            label = PROP_LABEL.get(market, market.replace('player_', '').upper())
            units = kelly_units(edge_pct=edge, odds=odds, fraction=0.25)
            conf = 'ELITE' if stars >= 2.5 else 'HIGH'

            matchup_str = ""
            if proj.get('matchup_mult', 1.0) != 1.0:
                sp = proj.get('matchup_detail')
                matchup_str = f" Matchup={proj['matchup_mult']:.2f}"
                if sp:
                    matchup_str += f" (vs {sp})"
            batorder_str = ""
            if proj.get('bat_order_pos'):
                batorder_str = f" BatOrder=#{proj['bat_order_pos']}({proj['bat_order_mult']:.2f}x)"
            notes = (f"Proj={proj['projection']:.1f} Mkt={line} "
                     f"OppDef={proj['opp_mult']:.2f} Ctx={proj['ctx_mult']:.2f}"
                     f"{matchup_str}{batorder_str} "
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

    # v24: Prop correlation detection — flag players with multiple props showing edge
    # When 2+ props on the same player all lean over, the market is underrating
    # that player's full game. The qualifying pick gets a confidence boost.
    _player_edges = defaultdict(list)  # player → list of (stat, edge, projection, line)
    for p in deduped:
        sel = p['selection']
        parts = sel.split(' OVER ')
        if len(parts) == 2:
            player_name = parts[0].strip()
            stat_label = sel.split()[-1]
            _player_edges[player_name].append({
                'stat': stat_label,
                'edge': p['edge_pct'],
                'projection': p.get('_signals', {}).get('projection', 0),
                'line': p.get('line', 0),
                'selection': sel,
            })

    # Also check near-miss edges (props that projected over but didn't clear threshold)
    # These are in the full picks list before dedup filtering
    _near_miss_edges = defaultdict(list)
    for p in picks:
        sel = p['selection']
        parts = sel.split(' OVER ')
        if len(parts) == 2:
            player_name = parts[0].strip()
            stat_label = sel.split()[-1]
            edge = p['edge_pct']
            if edge >= 8.0 and edge < MIN_EDGE_PCT:  # Near-miss: 8-18% edge
                _near_miss_edges[player_name].append({
                    'stat': stat_label,
                    'edge': edge,
                    'selection': sel,
                })

    # Tag correlated stacks
    correlated_players = {}
    for player, edges in _player_edges.items():
        # Count qualifying + near-miss props for this player
        near = _near_miss_edges.get(player, [])
        total_signals = len(edges) + len(near)
        if total_signals >= 2:
            qualifying_stats = [e['stat'] for e in edges]
            near_stats = [e['stat'] for e in near]
            correlated_players[player] = {
                'qualifying': edges,
                'near_miss': near,
                'total_signals': total_signals,
            }

    # Apply correlation boost: add note to qualifying picks
    for p in deduped:
        sel = p['selection']
        parts = sel.split(' OVER ')
        if len(parts) != 2:
            continue
        player_name = parts[0].strip()
        if player_name in correlated_players:
            cp = correlated_players[player_name]
            all_stats = [e['stat'] for e in cp['qualifying']] + [e['stat'] for e in cp['near_miss']]
            other_stats = [s for s in all_stats if s != sel.split()[-1]]
            if other_stats:
                stack_note = f"CORRELATED STACK ({cp['total_signals']} props lean over: {', '.join(set(all_stats))})"
                p['notes'] = stack_note + ' | ' + p.get('notes', '')
                p['_correlated'] = True
                p['_correlated_stats'] = list(set(all_stats))

    if correlated_players:
        print(f"  Prop correlations: {len(correlated_players)} players with stacked edges")
        for player, cp in correlated_players.items():
            q_stats = ', '.join(e['stat'] + f' ({e["edge"]:.0f}%)' for e in cp['qualifying'])
            n_stats = ', '.join(e['stat'] + f' ({e["edge"]:.0f}%)' for e in cp['near_miss'])
            print(f"    {player}: qualifying=[{q_stats}] near-miss=[{n_stats}]")

    # v24: Diversity cap — max 2 picks per stat type to force variety.
    # Without this, 19 RBI props at 25% fill all 5 slots and block
    # Sale Ks (24.4%), Wembanyama blocks (21.8%), Embiid threes (21.7%).
    MAX_PER_STAT = 2
    # v25: Daily caps — check what we've already bet today across all runs.
    # Without this, each hourly run adds 3 more props and RBIs pile up.
    DAILY_MAX_PROPS = 4
    DAILY_MAX_PER_STAT = 2
    _today_props = {}
    _today_total = 0
    try:
        _today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        _existing = conn.execute("""
            SELECT selection FROM bets
            WHERE market_type = 'PROP' AND DATE(created_at) = ? AND result IS NULL
        """, (_today_str,)).fetchall()
        _today_total = len(_existing)
        for _ep in _existing:
            _estat = _ep[0].split()[-1]  # last word = stat type (RBIS, THREES, etc.)
            _today_props[_estat] = _today_props.get(_estat, 0) + 1
    except Exception:
        pass

    _daily_remaining = max(0, DAILY_MAX_PROPS - _today_total)
    if _daily_remaining == 0:
        print(f"  Player Prop Model: daily cap reached ({_today_total} props already today)")
        if close:
            conn.close()
        return []

    stat_counts = defaultdict(int)
    diverse_picks = []
    for p in deduped:
        stat = p['selection'].split()[-1]
        # Check per-run cap
        if stat_counts[stat] >= MAX_PER_STAT:
            continue
        # Check daily per-stat cap
        if _today_props.get(stat, 0) + stat_counts[stat] >= DAILY_MAX_PER_STAT:
            continue
        diverse_picks.append(p)
        stat_counts[stat] += 1

    final = diverse_picks[:min(MAX_PROP_PICKS, _daily_remaining)]

    print(f"  Player Prop Model: {projected_count} players projected, "
          f"{edge_count} edges found, {len(deduped)} qualifying, {len(final)} selected (cap={MAX_PROP_PICKS}, max {MAX_PER_STAT}/stat)")

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
