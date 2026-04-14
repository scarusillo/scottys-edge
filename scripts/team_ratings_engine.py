"""
team_ratings_engine.py — Offensive/defensive power ratings from game results

Computes per-team ratings normalized to league average:
  - off_rating: scoring rate vs league avg (1.12 = 12% above avg offense)
  - def_rating: allowed rate vs league avg (0.90 = allows 10% fewer than avg)
  - pace: avg total points in team's games
  - net_rating: off_rating - def_rating

Features:
  - Exponential decay weighting (half-life configurable per sport)
  - Home/away splits
  - Opponent quality adjustment via Elo ratings
  - Rebuilt daily alongside Elo in the grade pipeline

Usage:
    python team_ratings_engine.py                # Build for all sports
    python team_ratings_engine.py --sport nba    # Just NBA
"""
import sqlite3, math, os, sys
from datetime import datetime
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ── Sport-specific parameters ──

TEAM_RATINGS_CONFIG = {
    'basketball_nba':              {'window': 25, 'half_life': 10, 'min_games': 8,  'min_split': 4},
    'basketball_ncaab':            {'window': 20, 'half_life': 8,  'min_games': 6,  'min_split': 3},
    'icehockey_nhl':               {'window': 25, 'half_life': 10, 'min_games': 10, 'min_split': 4},
    'baseball_mlb':                {'window': 20, 'half_life': 10, 'min_games': 10, 'min_split': 4},
    'baseball_ncaa':               {'window': 15, 'half_life': 8,  'min_games': 6,  'min_split': 3},
    'soccer_epl':                  {'window': 12, 'half_life': 6,  'min_games': 6,  'min_split': 3},
    'soccer_italy_serie_a':        {'window': 12, 'half_life': 6,  'min_games': 6,  'min_split': 3},
    'soccer_spain_la_liga':        {'window': 12, 'half_life': 6,  'min_games': 6,  'min_split': 3},
    'soccer_germany_bundesliga':   {'window': 12, 'half_life': 6,  'min_games': 6,  'min_split': 3},
    'soccer_france_ligue_one':     {'window': 12, 'half_life': 6,  'min_games': 6,  'min_split': 3},
    'soccer_uefa_champs_league':   {'window': 10, 'half_life': 5,  'min_games': 4,  'min_split': 2},
    'soccer_usa_mls':              {'window': 12, 'half_life': 6,  'min_games': 6,  'min_split': 3},
    'soccer_mexico_ligamx':        {'window': 12, 'half_life': 6,  'min_games': 6,  'min_split': 3},
}

# Default for sports not listed above
DEFAULT_CONFIG = {'window': 20, 'half_life': 10, 'min_games': 8, 'min_split': 4}


def _get_config(sport):
    """Get config for a sport, falling back to default."""
    return TEAM_RATINGS_CONFIG.get(sport, DEFAULT_CONFIG)


def _ensure_table(conn):
    """Create team_ratings table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS team_ratings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sport           TEXT NOT NULL,
            team            TEXT NOT NULL,
            off_rating      REAL,
            def_rating      REAL,
            home_off        REAL,
            home_def        REAL,
            away_off        REAL,
            away_def        REAL,
            pace            REAL,
            net_rating      REAL,
            games_used      INTEGER,
            confidence      TEXT DEFAULT 'LOW',
            last_updated    TEXT,
            UNIQUE(sport, team)
        )
    """)
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tr_sport ON team_ratings(sport, team)")
    except Exception:
        pass


def build_team_ratings(sport, conn=None, verbose=True):
    """
    Build offensive/defensive ratings for all teams in a sport.

    Ratings are normalized to league average (1.0 = average).
    Uses exponential decay weighting and opponent quality adjustment.
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close_conn = True

    _ensure_table(conn)
    cfg = _get_config(sport)
    window = cfg['window']
    half_life = cfg['half_life']
    min_games = cfg['min_games']
    min_split = cfg['min_split']

    # Get all completed results for this sport
    rows = conn.execute("""
        SELECT home, away, home_score, away_score, commence_time
        FROM results
        WHERE sport=? AND completed=1
        AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY commence_time DESC
    """, (sport,)).fetchall()

    if not rows:
        if verbose:
            print(f"  {sport}: no results found")
        if close_conn:
            conn.close()
        return {}

    # Load Elo ratings for opponent quality adjustment
    elo_data = {}
    try:
        from elo_engine import get_elo_ratings
        elo_data = get_elo_ratings(conn, sport) or {}
    except Exception:
        pass

    # Collect games per team (most recent first, capped at window)
    team_games = defaultdict(list)
    for home, away, hs, as_, ct in rows:
        if len(team_games[home]) < window:
            team_games[home].append({
                'is_home': True, 'scored': hs, 'allowed': as_,
                'opponent': away, 'time': ct
            })
        if len(team_games[away]) < window:
            team_games[away].append({
                'is_home': False, 'scored': as_, 'allowed': hs,
                'opponent': home, 'time': ct
            })

    # League average scoring per team per game
    all_scores = []
    for home, away, hs, as_, ct in rows:
        all_scores.append(hs)
        all_scores.append(as_)
    league_avg_per_team = sum(all_scores) / len(all_scores) if all_scores else 1.0

    if league_avg_per_team == 0:
        league_avg_per_team = 1.0  # safety

    # Build ratings for each team
    ratings = {}
    now = datetime.now().isoformat()

    for team, games in team_games.items():
        if len(games) < min_games:
            continue

        # Accumulators with exponential decay
        off_sum, def_sum, total_w = 0.0, 0.0, 0.0
        h_off_sum, h_def_sum, h_w = 0.0, 0.0, 0.0
        a_off_sum, a_def_sum, a_w = 0.0, 0.0, 0.0
        pace_sum, pace_w = 0.0, 0.0

        for i, g in enumerate(games):
            weight = math.pow(0.5, i / half_life)

            scored = g['scored']
            allowed = g['allowed']
            opp = g['opponent']

            # Opponent quality adjustment via Elo
            if elo_data and opp in elo_data:
                opp_elo = elo_data[opp].get('elo', 1500)
                elo_factor = 1.0 + (opp_elo - 1500) / 5000
                scored = scored / elo_factor      # deflate vs weak opponents
                allowed = allowed * elo_factor    # inflate vs weak opponents

            # Overall
            off_sum += scored * weight
            def_sum += allowed * weight
            total_w += weight
            pace_sum += (g['scored'] + g['allowed']) * weight  # raw pace, no Elo adj
            pace_w += weight

            # Home/away splits
            if g['is_home']:
                h_off_sum += scored * weight
                h_def_sum += allowed * weight
                h_w += weight
            else:
                a_off_sum += scored * weight
                a_def_sum += allowed * weight
                a_w += weight

        if total_w == 0:
            continue

        avg_off = off_sum / total_w
        avg_def = def_sum / total_w
        avg_pace = pace_sum / pace_w if pace_w > 0 else 0

        # Normalize to league average (1.0 = average)
        off_rating = avg_off / league_avg_per_team
        def_rating = avg_def / league_avg_per_team

        # Home/away splits (fall back to overall if insufficient games)
        if h_w >= min_split:
            home_off = (h_off_sum / h_w) / league_avg_per_team
            home_def = (h_def_sum / h_w) / league_avg_per_team
        else:
            home_off = off_rating
            home_def = def_rating

        if a_w >= min_split:
            away_off = (a_off_sum / a_w) / league_avg_per_team
            away_def = (a_def_sum / a_w) / league_avg_per_team
        else:
            away_off = off_rating
            away_def = def_rating

        net_rating = off_rating - def_rating

        # Confidence based on games used
        gp = len(games)
        if gp >= min_games * 2:
            conf = 'HIGH'
        elif gp >= min_games:
            conf = 'MEDIUM'
        else:
            conf = 'LOW'

        ratings[team] = {
            'off_rating': round(off_rating, 4),
            'def_rating': round(def_rating, 4),
            'home_off': round(home_off, 4),
            'home_def': round(home_def, 4),
            'away_off': round(away_off, 4),
            'away_def': round(away_def, 4),
            'pace': round(avg_pace, 2),
            'net_rating': round(net_rating, 4),
            'games': gp,
            'confidence': conf,
        }

        # Save to DB
        conn.execute("""
            INSERT OR REPLACE INTO team_ratings
            (sport, team, off_rating, def_rating, home_off, home_def,
             away_off, away_def, pace, net_rating, games_used, confidence, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (sport, team, off_rating, def_rating, home_off, home_def,
              away_off, away_def, avg_pace, net_rating, gp, conf, now))

    conn.commit()

    if verbose:
        print(f"  {sport}: {len(ratings)} teams rated (league avg {league_avg_per_team:.1f} pts/team)")

    if close_conn:
        conn.close()

    return ratings


def get_team_ratings(conn, sport):
    """Retrieve team ratings for a sport. Returns dict keyed by team name."""
    try:
        rows = conn.execute("""
            SELECT team, off_rating, def_rating, home_off, home_def,
                   away_off, away_def, pace, net_rating, games_used, confidence
            FROM team_ratings WHERE sport=?
        """, (sport,)).fetchall()
        return {r[0]: {
            'off_rating': r[1], 'def_rating': r[2],
            'home_off': r[3], 'home_def': r[4],
            'away_off': r[5], 'away_def': r[6],
            'pace': r[7], 'net_rating': r[8],
            'games': r[9], 'confidence': r[10],
        } for r in rows}
    except Exception:
        return {}


def build_all_team_ratings(sports=None, verbose=True):
    """Build team ratings for all (or specified) sports."""
    conn = sqlite3.connect(DB_PATH)
    _ensure_table(conn)

    if sports is None:
        # Get all sports with results
        sport_rows = conn.execute("""
            SELECT DISTINCT sport FROM results WHERE completed=1
            AND home_score IS NOT NULL
        """).fetchall()
        sports = [r[0] for r in sport_rows]

    # Skip tennis — individual sport, no off/def concept
    sports = [s for s in sports if 'tennis' not in s]

    if verbose:
        print(f"Building team ratings for {len(sports)} sports...")

    total_teams = 0
    for sp in sorted(sports):
        ratings = build_team_ratings(sp, conn=conn, verbose=verbose)
        total_teams += len(ratings)

    if verbose:
        print(f"  Total: {total_teams} teams across {len(sports)} sports")

    conn.close()
    return total_teams


# ── CLI ──

if __name__ == '__main__':
    sport_filter = None
    if '--sport' in sys.argv:
        idx = sys.argv.index('--sport')
        if idx + 1 < len(sys.argv):
            alias = sys.argv[idx + 1].lower()
            SPORT_ALIASES = {
                'nba': 'basketball_nba', 'ncaab': 'basketball_ncaab',
                'nhl': 'icehockey_nhl', 'mlb': 'baseball_mlb',
                'ncaa': 'baseball_ncaa', 'epl': 'soccer_epl',
                'seriea': 'soccer_italy_serie_a', 'laliga': 'soccer_spain_la_liga',
                'bundesliga': 'soccer_germany_bundesliga', 'ligue1': 'soccer_france_ligue_one',
                'ucl': 'soccer_uefa_champs_league', 'mls': 'soccer_usa_mls',
                'ligamx': 'soccer_mexico_ligamx',
            }
            sport_filter = SPORT_ALIASES.get(alias, alias)

    if sport_filter:
        conn = sqlite3.connect(DB_PATH)
        _ensure_table(conn)
        build_team_ratings(sport_filter, conn=conn)
        conn.close()
    else:
        build_all_team_ratings()
