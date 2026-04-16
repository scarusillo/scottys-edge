"""
career_stats.py — ESPN career stats lookup for prop model regression.

v25.18: The prop model projects from a 20-game box score window. For players
with small samples (< 30 games in DB), the window can be wildly unrepresentative.
Example: Stanton had 1 HR in 27 DB games (4%) but career rate is ~25%.

This module fetches career per-game averages from ESPN and caches them in a
DB table. The prop model blends career stats with box score projection when
the sample is small, preventing the model from treating power hitters like
slap hitters (or vice versa).

Flow:
  1. Prop model calls get_career_stat(player, stat_type, sport)
  2. We check career_stats_cache table (fresh if < 7 days old)
  3. If stale/missing, look up ESPN athlete ID from a recent box score game
  4. Fetch career splits from ESPN (season=0 = career totals)
  5. Cache and return career per-game average for the requested stat

ESPN endpoints:
  MLB: site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{id}/splits?season=0
  NBA: site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{id}/splits?season=0
  NHL: site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/{id}/splits?season=0
"""

import sqlite3, os, json, math
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Maps our box_score stat_type → ESPN splits field name + how to compute per-game
# MLB splits: atBats, runs, hits, doubles, triples, homeRuns, RBIs, walks, strikeouts, stolenBases
# NBA splits: gamesPlayed, avgPoints, avgRebounds, avgAssists, avgBlocks, avgSteals,
#             avgThreePointFieldGoalsMade-avgThreePointFieldGoalsAttempted
# NHL splits: games, goals, assists, points, shotsTotal

CAREER_STAT_MAP = {
    'baseball_mlb': {
        # MLB career splits don't include "games" — use atBats as denominator.
        # Divide by avg AB/game (~3.6) to convert career totals to per-game rate.
        'hits':   {'field': 'hits',     'total_field': 'atBats', 'type': 'rate_ab'},
        'hr':     {'field': 'homeRuns', 'total_field': 'atBats', 'type': 'rate_ab'},
        'runs':   {'field': 'runs',     'total_field': 'atBats', 'type': 'rate_ab'},
        'rbi':    {'field': 'RBIs',     'total_field': 'atBats', 'type': 'rate_ab'},
        'walks':  {'field': 'walks',    'total_field': 'atBats', 'type': 'rate_ab'},
        'batter_k': {'field': 'strikeouts', 'total_field': 'atBats', 'type': 'rate_ab'},
        'stolen_bases': {'field': 'stolenBases', 'total_field': 'atBats', 'type': 'rate_ab'},
    },
    'basketball_nba': {
        'pts':    {'field': 'avgPoints',   'type': 'avg'},
        'reb':    {'field': 'avgRebounds', 'type': 'avg'},
        'ast':    {'field': 'avgAssists',  'type': 'avg'},
        'blk':    {'field': 'avgBlocks',   'type': 'avg'},
        'stl':    {'field': 'avgSteals',   'type': 'avg'},
        'threes': {'field': 'avgThreePointFieldGoalsMade-avgThreePointFieldGoalsAttempted', 'type': 'split_first'},
    },
    'icehockey_nhl': {
        'sog':        {'field': 'shotsTotal', 'total_field': 'games', 'type': 'rate'},
        'hockey_pts': {'field': 'points',     'total_field': 'games', 'type': 'rate'},
        'blocked_shots': None,  # Not available in ESPN career splits
    },
}

# ESPN sport path mapping
ESPN_SPORT_PATH = {
    'baseball_mlb': 'baseball/mlb',
    'basketball_nba': 'basketball/nba',
    'icehockey_nhl': 'hockey/nhl',
}

# MLB splits use "games" as first field (not labeled). Index-based extraction.
MLB_SPLIT_NAMES = ['atBats', 'runs', 'hits', 'doubles', 'triples', 'homeRuns',
                   'RBIs', 'walks', 'hitByPitch', 'strikeouts', 'stolenBases',
                   'caughtStealing', 'avg', 'onBasePct', 'slugAvg', 'OPS']

CACHE_DAYS = 7  # Refresh career stats weekly


def _init_cache_table(conn):
    """Create career stats cache table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS career_stats_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player TEXT NOT NULL,
            sport TEXT NOT NULL,
            stat_type TEXT NOT NULL,
            career_avg REAL,
            career_games INTEGER,
            espn_athlete_id TEXT,
            fetched_at TEXT NOT NULL,
            UNIQUE(player, sport, stat_type)
        )
    """)
    conn.commit()


def _get_espn_athlete_id(conn, player, sport):
    """Look up ESPN athlete ID from a recent box score game summary."""
    # Check cache first
    cached = conn.execute("""
        SELECT espn_athlete_id FROM career_stats_cache
        WHERE player = ? AND sport = ? AND espn_athlete_id IS NOT NULL
        LIMIT 1
    """, (player, sport)).fetchone()
    if cached and cached[0]:
        return cached[0]

    # Find a recent game this player appeared in
    game = conn.execute("""
        SELECT espn_game_id FROM box_scores
        WHERE player = ? AND sport = ?
        ORDER BY game_date DESC LIMIT 1
    """, (player, sport)).fetchone()
    if not game:
        return None

    espn_path = ESPN_SPORT_PATH.get(sport)
    if not espn_path:
        return None

    # Fetch game summary to get athlete ID
    import urllib.request
    try:
        url = f'https://site.api.espn.com/apis/site/v2/sports/{espn_path}/summary?event={game[0]}'
        data = json.loads(urllib.request.urlopen(url, timeout=10).read())

        # Search through boxscore players
        for team in data.get('boxscore', {}).get('players', []):
            for stat_group in team.get('statistics', []):
                for athlete in stat_group.get('athletes', []):
                    if athlete.get('athlete', {}).get('displayName') == player:
                        return athlete['athlete']['id']
    except Exception:
        pass

    return None


def _fetch_career_splits(espn_id, sport):
    """Fetch recent-weighted career splits from ESPN.

    v25.18: Uses last 3 seasons weighted by recency instead of full career.
    This prevents rookie/development years from dragging down projections
    for players who have leveled up (e.g., Maxey 20 ppg rookie → 28 ppg now).

    Weights: current season 50%, last season 30%, 2 seasons ago 20%.
    Falls back to full career (season=0) if season-specific data unavailable.
    """
    espn_path = ESPN_SPORT_PATH.get(sport)
    if not espn_path:
        return None

    import urllib.request
    from datetime import datetime

    current_year = datetime.now().year
    seasons = [current_year, current_year - 1, current_year - 2]
    weights = [0.50, 0.30, 0.20]

    season_data = []
    for year in seasons:
        try:
            url = f'https://site.web.api.espn.com/apis/common/v3/sports/{espn_path}/athletes/{espn_id}/splits?season={year}'
            data = json.loads(urllib.request.urlopen(url, timeout=10).read())
            names = data.get('names', [])
            all_splits = data.get('splitCategories', [{}])[0].get('splits', [{}])[0]
            stats = all_splits.get('stats', [])
            if names and stats:
                season_data.append(dict(zip(names, stats)))
            else:
                season_data.append(None)
        except Exception:
            season_data.append(None)

    # If we got at least 2 seasons, compute weighted average
    valid_seasons = [(sd, w) for sd, w in zip(season_data, weights) if sd is not None]

    if len(valid_seasons) >= 2:
        # Return a synthetic splits dict with weighted values
        # Use the first valid season's keys as template
        template = valid_seasons[0][0]
        result = {}
        total_weight = sum(w for _, w in valid_seasons)

        for key in template:
            vals = []
            ws = []
            for sd, w in valid_seasons:
                v = sd.get(key)
                if v is None:
                    continue
                try:
                    # Handle compound fields like "7.5-16.4"
                    if '-' in str(v) and not str(v).startswith('-') and '.' in str(v):
                        # NBA compound stat like "2.4-6.3" — take first (made)
                        fv = float(str(v).split('-')[0])
                    elif v.startswith('.'):
                        fv = float(v)
                    else:
                        fv = float(str(v).replace(',', ''))
                    vals.append(fv)
                    ws.append(w)
                except (ValueError, TypeError, AttributeError):
                    continue

            if vals and ws:
                tw = sum(ws)
                weighted = sum(v * w for v, w in zip(vals, ws)) / tw
                # Format back: integers stay integers, decimals stay decimals
                if key in ('avg', 'onBasePct', 'slugAvg', 'OPS', 'fieldGoalPct',
                           'threePointFieldGoalPct', 'freeThrowPct', 'faceoffPercent'):
                    result[key] = f'.{int(weighted * 1000):03d}' if weighted < 1 else str(round(weighted, 3))
                elif any(c in key.lower() for c in ['avg', 'pct']):
                    result[key] = str(round(weighted, 1))
                else:
                    result[key] = str(int(round(weighted)))
            else:
                result[key] = template.get(key)

        return result

    # Fallback to full career
    try:
        url = f'https://site.web.api.espn.com/apis/common/v3/sports/{espn_path}/athletes/{espn_id}/splits?season=0'
        data = json.loads(urllib.request.urlopen(url, timeout=10).read())
        names = data.get('names', [])
        all_splits = data.get('splitCategories', [{}])[0].get('splits', [{}])[0]
        stats = all_splits.get('stats', [])
        if not names or not stats:
            return None
        return dict(zip(names, stats))
    except Exception:
        return None


def _parse_career_avg(splits, stat_info, sport):
    """Extract per-game average from career splits dict."""
    if not splits or not stat_info:
        return None, None

    stat_type = stat_info.get('type')

    if stat_type == 'avg':
        # NBA: already per-game averages
        field = stat_info['field']
        val = splits.get(field)
        if val is None:
            return None, None
        try:
            return float(val), int(float(splits.get('gamesPlayed', 0)))
        except (ValueError, TypeError):
            return None, None

    elif stat_type == 'split_first':
        # NBA threes: field is "2.4-6.3" format, take first number (made per game)
        field = stat_info['field']
        val = splits.get(field, '')
        try:
            made = float(val.split('-')[0])
            return made, int(float(splits.get('gamesPlayed', 0)))
        except (ValueError, TypeError, IndexError):
            return None, None

    elif stat_type == 'rate':
        # NHL: career totals, divide by games
        field = stat_info['field']
        games_field = stat_info.get('total_field', 'games')
        total = splits.get(field)
        games = splits.get(games_field)
        if total is None or games is None:
            return None, None
        try:
            total_f = float(total.replace(',', ''))
            games_f = float(games.replace(',', ''))
            if games_f <= 0:
                return None, None
            return round(total_f / games_f, 3), int(games_f)
        except (ValueError, TypeError):
            return None, None

    elif stat_type == 'rate_ab':
        # MLB: career totals ÷ atBats, then × avg_AB_per_game to get per-game rate.
        # ESPN MLB career splits don't include "games" field.
        # avg AB/game varies by player (~3.2-4.0). We use the player's own
        # career AB ÷ an estimated games count derived from their box_scores.
        field = stat_info['field']
        ab_field = stat_info.get('total_field', 'atBats')
        total = splits.get(field)
        career_ab = splits.get(ab_field)
        if total is None or career_ab is None:
            return None, None
        try:
            total_f = float(total.replace(',', ''))
            ab_f = float(career_ab.replace(',', ''))
            if ab_f <= 0:
                return None, None
            # Estimate games from AB. Use 3.6 AB/game as MLB average.
            est_games = ab_f / 3.6
            per_game = total_f / est_games
            return round(per_game, 3), int(est_games)
        except (ValueError, TypeError):
            return None, None

    return None, None


def get_career_stat(conn, player, stat_type, sport):
    """
    Get career per-game average for a player/stat.
    Returns (career_avg, career_games) or (None, None) if unavailable.
    Caches results for CACHE_DAYS.
    """
    _init_cache_table(conn)

    # Check cache
    cutoff = (datetime.now() - timedelta(days=CACHE_DAYS)).isoformat()
    cached = conn.execute("""
        SELECT career_avg, career_games FROM career_stats_cache
        WHERE player = ? AND sport = ? AND stat_type = ? AND fetched_at > ?
    """, (player, sport, stat_type, cutoff)).fetchone()
    if cached:
        return cached[0], cached[1]

    # Check if we support this sport/stat
    sport_map = CAREER_STAT_MAP.get(sport, {})
    stat_info = sport_map.get(stat_type)
    if stat_info is None:
        return None, None

    # Get ESPN athlete ID
    espn_id = _get_espn_athlete_id(conn, player, sport)
    if not espn_id:
        # Cache the miss so we don't retry every run
        try:
            conn.execute("""
                INSERT OR REPLACE INTO career_stats_cache
                (player, sport, stat_type, career_avg, career_games, espn_athlete_id, fetched_at)
                VALUES (?, ?, ?, NULL, NULL, NULL, ?)
            """, (player, sport, stat_type, datetime.now().isoformat()))
            conn.commit()
        except Exception:
            pass
        return None, None

    # Fetch career splits
    splits = _fetch_career_splits(espn_id, sport)
    if not splits:
        return None, None

    career_avg, career_games = _parse_career_avg(splits, stat_info, sport)

    # Cache result
    try:
        conn.execute("""
            INSERT OR REPLACE INTO career_stats_cache
            (player, sport, stat_type, career_avg, career_games, espn_athlete_id, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (player, sport, stat_type, career_avg, career_games, espn_id,
              datetime.now().isoformat()))
        conn.commit()
    except Exception:
        pass

    return career_avg, career_games


# ═══════════════════════════════════════════════════════════════════
# CAREER REGRESSION — blend career avg into projection
# ═══════════════════════════════════════════════════════════════════

# When box_scores has < CAREER_BLEND_THRESHOLD games, blend career avg in.
# At exactly CAREER_BLEND_THRESHOLD games, career weight = 0 (all box score).
# At MIN_PLAYER_GAMES (15), career weight = CAREER_MAX_WEIGHT.
# Linear interpolation between.
# v25.18: Sport-specific career regression weights.
# MLB: Power profiles are stable — career HR rate is highly predictive. Strong regression.
# NHL: SOG volume is a player trait, goals/pts are line-dependent. Medium regression.
# NBA: Players change roles rapidly (rookie→star). Box score window is usually right.
#      Only regress when sample is very small. Light regression.
CAREER_BLEND_CONFIG = {
    'baseball_mlb': {
        'threshold': 40,   # Games in DB above which career has 0 weight
        'max_weight': 0.40, # Strong — power profiles are stable across seasons
    },
    'icehockey_nhl': {
        'threshold': 35,
        'max_weight': 0.30, # Medium — SOG is stable, goals are streaky
    },
    'basketball_nba': {
        'threshold': 25,   # Lower threshold — NBA box scores are reliable faster
        'max_weight': 0.20, # Light — current role matters more than career history
    },
}
CAREER_BLEND_THRESHOLD = 40   # Default fallback
CAREER_MAX_WEIGHT = 0.35      # Default fallback


def career_regressed_projection(box_score_avg, box_score_games, career_avg, career_games,
                                sport=None):
    """
    Blend box score projection with career average, sport-aware.

    Returns adjusted projection. If career data unavailable, returns box_score_avg unchanged.

    Sport-specific behavior:
      MLB: Strong regression (40% max). Power profiles are stable.
           15 games → 40% career. 40+ games → 0%.
      NHL: Medium regression (30% max). SOG is a trait, goals are streaky.
           15 games → 30% career. 35+ games → 0%.
      NBA: Light regression (20% max). Current role > career history.
           15 games → 20% career. 25+ games → 0%.
    """
    if career_avg is None or career_games is None or career_games < 50:
        return box_score_avg  # Not enough career data to be meaningful

    # Get sport-specific config
    config = CAREER_BLEND_CONFIG.get(sport, {})
    threshold = config.get('threshold', CAREER_BLEND_THRESHOLD)
    max_weight = config.get('max_weight', CAREER_MAX_WEIGHT)

    if box_score_games >= threshold:
        return box_score_avg  # Enough recent data, no regression needed

    # Linear interpolation: more DB games → less career weight
    min_games = 15  # MIN_PLAYER_GAMES from player_prop_model
    weight_range = threshold - min_games
    if weight_range <= 0:
        return box_score_avg
    games_above_min = max(0, box_score_games - min_games)
    career_weight = max(0, max_weight * (1.0 - games_above_min / weight_range))

    blended = (1.0 - career_weight) * box_score_avg + career_weight * career_avg
    return round(blended, 3)


if __name__ == '__main__':
    """Test career stats lookup."""
    conn = sqlite3.connect(DB_PATH)

    test_cases = [
        ('Giancarlo Stanton', 'runs', 'baseball_mlb'),
        ('Giancarlo Stanton', 'hr', 'baseball_mlb'),
        ('Tyrese Maxey', 'pts', 'basketball_nba'),
        ('Zach Hyman', 'sog', 'icehockey_nhl'),
        ('Gary Payton II', 'pts', 'basketball_nba'),
    ]

    for player, stat, sport in test_cases:
        avg, games = get_career_stat(conn, player, stat, sport)
        # Get box score avg for comparison
        rows = conn.execute("""
            SELECT stat_value FROM box_scores
            WHERE player = ? AND stat_type = ? AND sport = ?
            ORDER BY game_date DESC LIMIT 20
        """, (player, stat, sport)).fetchall()
        bs_avg = sum(r[0] for r in rows) / len(rows) if rows else 0
        bs_games = len(rows)

        print(f'{player} ({stat}):')
        print(f'  Box score: {bs_avg:.2f}/game ({bs_games} games)')
        print(f'  Career:    {avg}/game ({games} games)')
        config = CAREER_BLEND_CONFIG.get(sport, {})
        threshold = config.get('threshold', CAREER_BLEND_THRESHOLD)
        max_wt = config.get('max_weight', CAREER_MAX_WEIGHT)
        if avg and bs_games < threshold:
            blended = career_regressed_projection(bs_avg, bs_games, avg, games, sport=sport)
            weight_range = threshold - 15
            cw = max(0, max_wt * (1.0 - max(0, bs_games - 15) / weight_range)) if weight_range > 0 else 0
            print(f'  Blended:   {blended:.2f}/game (career weight: {cw:.0%}, sport={sport})')
        print()

    conn.close()
