"""
soccer_standings.py — Scrape and cache soccer league standings from ESPN

Fetches current standings for major soccer leagues and stores them in SQLite.
Provides motivation-factor multipliers based on table position (relegation,
title race, dead rubber) for use in the prediction model.

Usage:
    python soccer_standings.py                    # Fetch all leagues
    python soccer_standings.py --sport epl        # Fetch one league
    python soccer_standings.py --sport mls        # MLS only
    python soccer_standings.py --verbose          # Extra logging
"""
import sqlite3, json, os, sys, time
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ESPN standings API — uses v2 standings endpoint
LEAGUE_SLUGS = {
    'soccer_epl':                'eng.1',
    'soccer_italy_serie_a':      'ita.1',
    'soccer_spain_la_liga':      'esp.1',
    'soccer_germany_bundesliga': 'ger.1',
    'soccer_france_ligue_one':   'fra.1',
    'soccer_usa_mls':            'usa.1',
}

# Short aliases for CLI convenience
SPORT_ALIASES = {
    'epl':        'soccer_epl',
    'serie_a':    'soccer_italy_serie_a',
    'la_liga':    'soccer_spain_la_liga',
    'bundesliga': 'soccer_germany_bundesliga',
    'ligue_one':  'soccer_france_ligue_one',
    'mls':        'soccer_usa_mls',
}

STANDINGS_URL = 'https://site.api.espn.com/apis/v2/sports/soccer/{slug}/standings'

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS soccer_standings (
    sport TEXT NOT NULL,
    team TEXT NOT NULL,
    rank INTEGER,
    points INTEGER,
    wins INTEGER,
    draws INTEGER,
    losses INTEGER,
    goals_for INTEGER,
    goals_against INTEGER,
    goal_diff INTEGER,
    games_played INTEGER,
    updated_at TEXT,
    UNIQUE(sport, team)
)
"""


def _ensure_table(conn):
    """Create the soccer_standings table if it doesn't exist."""
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()


def _data_is_fresh(conn, sport, max_age_hours=12):
    """Return True if standings for this sport were updated within max_age_hours."""
    row = conn.execute(
        "SELECT updated_at FROM soccer_standings WHERE sport = ? LIMIT 1",
        (sport,)
    ).fetchone()
    if not row or not row[0]:
        return False
    try:
        updated = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
        return (datetime.now() - updated) < timedelta(hours=max_age_hours)
    except ValueError:
        return False


def _fetch_espn_standings(slug, verbose=True):
    """Fetch standings JSON from ESPN API for a given league slug."""
    url = STANDINGS_URL.format(slug=slug)
    req = Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    })
    try:
        resp = urlopen(req, timeout=20)
        data = json.loads(resp.read().decode())
        return data
    except (URLError, HTTPError) as e:
        if verbose:
            print(f"  [WARN] ESPN standings fetch failed for {slug}: {e}")
        return None
    except Exception as e:
        if verbose:
            print(f"  [WARN] Unexpected error fetching {slug}: {e}")
        return None


def _parse_stat(stats_list, stat_name, default=0):
    """Extract a stat value from ESPN's stats array by name."""
    for s in stats_list:
        if s.get('name') == stat_name or s.get('abbreviation') == stat_name:
            try:
                return int(float(s.get('value', default)))
            except (ValueError, TypeError):
                return default
    return default


def _parse_standings(data, sport, verbose=True):
    """Parse ESPN standings JSON into a list of team dicts."""
    teams = []
    try:
        # ESPN standings structure: children[] -> standings -> entries[]
        # or sometimes: standings -> entries[] directly
        children = data.get('children', [])
        if not children:
            # Flat structure (some leagues)
            entries = data.get('standings', {}).get('entries', [])
            if entries:
                children = [{'standings': {'entries': entries}}]

        rank_counter = 0
        for group in children:
            standings = group.get('standings', {})
            entries = standings.get('entries', [])
            for entry in entries:
                rank_counter += 1
                team_info = entry.get('team', {})
                team_name = team_info.get('displayName', team_info.get('name', 'Unknown'))
                stats = entry.get('stats', [])

                row = {
                    'sport': sport,
                    'team': team_name,
                    'rank': _parse_stat(stats, 'rank', rank_counter),
                    'points': _parse_stat(stats, 'points', 0),
                    'wins': _parse_stat(stats, 'wins', 0),
                    'draws': _parse_stat(stats, 'draws', 0) or _parse_stat(stats, 'ties', 0),
                    'losses': _parse_stat(stats, 'losses', 0),
                    'goals_for': _parse_stat(stats, 'pointsFor', 0),
                    'goals_against': _parse_stat(stats, 'pointsAgainst', 0),
                    'goal_diff': _parse_stat(stats, 'pointDifferential', 0),
                    'games_played': _parse_stat(stats, 'gamesPlayed', 0),
                }

                # Fallback: compute goal_diff if not present
                if row['goal_diff'] == 0 and (row['goals_for'] or row['goals_against']):
                    row['goal_diff'] = row['goals_for'] - row['goals_against']

                # Fallback: compute games_played if not present
                if row['games_played'] == 0:
                    row['games_played'] = row['wins'] + row['draws'] + row['losses']

                teams.append(row)

    except Exception as e:
        if verbose:
            print(f"  [WARN] Error parsing standings for {sport}: {e}")

    return teams


def fetch_standings(sport=None, verbose=True):
    """Fetch and store standings for one or all leagues.

    Args:
        sport: Sport key like 'soccer_epl' or alias like 'epl'. None = all leagues.
        verbose: Print progress info.
    """
    conn = sqlite3.connect(DB_PATH)
    _ensure_table(conn)

    # Resolve which leagues to fetch
    if sport:
        # Accept aliases
        resolved = SPORT_ALIASES.get(sport, sport)
        if resolved not in LEAGUE_SLUGS:
            if verbose:
                print(f"Unknown sport: {sport}")
                print(f"Valid: {', '.join(list(LEAGUE_SLUGS.keys()) + list(SPORT_ALIASES.keys()))}")
            conn.close()
            return
        leagues = {resolved: LEAGUE_SLUGS[resolved]}
    else:
        leagues = LEAGUE_SLUGS

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_teams = 0

    for sport_key, slug in leagues.items():
        # Check freshness
        if _data_is_fresh(conn, sport_key):
            if verbose:
                print(f"  {sport_key}: data is fresh (< 12h old), skipping")
            continue

        if verbose:
            print(f"  Fetching {sport_key} ({slug})...")

        data = _fetch_espn_standings(slug, verbose=verbose)
        if not data:
            continue

        teams = _parse_standings(data, sport_key, verbose=verbose)
        if not teams:
            if verbose:
                print(f"  [WARN] No teams parsed for {sport_key}")
            continue

        # INSERT OR REPLACE
        for t in teams:
            conn.execute("""
                INSERT OR REPLACE INTO soccer_standings
                    (sport, team, rank, points, wins, draws, losses,
                     goals_for, goals_against, goal_diff, games_played, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                t['sport'], t['team'], t['rank'], t['points'],
                t['wins'], t['draws'], t['losses'],
                t['goals_for'], t['goals_against'], t['goal_diff'],
                t['games_played'], now_str
            ))

        conn.commit()
        total_teams += len(teams)

        if verbose:
            print(f"  {sport_key}: {len(teams)} teams stored")

        # Rate limit between leagues
        time.sleep(0.5)

    conn.close()
    if verbose:
        print(f"  Done — {total_teams} teams updated")


def _fuzzy_match(name, candidates):
    """Simple fuzzy team name matching. Returns best match or None."""
    name_lower = name.lower().strip()

    # Exact match
    for c in candidates:
        if c.lower().strip() == name_lower:
            return c

    # Substring match (team name contained in DB name or vice versa)
    for c in candidates:
        c_lower = c.lower().strip()
        if name_lower in c_lower or c_lower in name_lower:
            return c

    # Word overlap match
    name_words = set(name_lower.split())
    best, best_score = None, 0
    for c in candidates:
        c_words = set(c.lower().strip().split())
        overlap = len(name_words & c_words)
        if overlap > best_score:
            best_score = overlap
            best = c

    return best if best_score > 0 else None


def get_team_position(conn, team_name, sport):
    """Look up a team's standings position.

    Returns dict with rank, points, games_played, is_relegation, is_title_race,
    is_dead_rubber — or None if team not found.
    """
    rows = conn.execute(
        "SELECT team, rank, points, games_played FROM soccer_standings WHERE sport = ?",
        (sport,)
    ).fetchall()

    if not rows:
        return None

    # Fuzzy match the team name
    candidates = [r[0] for r in rows]
    matched = _fuzzy_match(team_name, candidates)
    if not matched:
        return None

    # Find the matched row
    team_row = None
    for r in rows:
        if r[0] == matched:
            team_row = r
            break

    if not team_row:
        return None

    team, rank, points, games_played = team_row

    # Leader points
    leader_points = max(r[2] for r in rows)
    total_teams = len(rows)

    # Relegation: bottom 3
    is_relegation = rank > (total_teams - 3)

    # Title race: top 3 AND within 6 points of leader
    is_title_race = rank <= 3 and (leader_points - points) <= 6

    # Dead rubber: mid-table safe — not in title race, not in relegation
    is_dead_rubber = not is_relegation and not is_title_race

    return {
        'team': team,
        'rank': rank,
        'points': points,
        'games_played': games_played,
        'is_relegation': is_relegation,
        'is_title_race': is_title_race,
        'is_dead_rubber': is_dead_rubber,
    }


def get_motivation_factor(conn, home, away, sport):
    """Return (home_motivation, away_motivation) multipliers based on table position.

    Relegation battle  = 1.10 (fighting for survival)
    Title race         = 1.05 (pushing for the title)
    Dead rubber        = 0.85 (nothing to play for)
    Normal / unknown   = 1.00

    Only applies after matchday 20 (games_played >= 20) to avoid early-season noise.
    """
    home_factor = 1.0
    away_factor = 1.0

    home_pos = get_team_position(conn, home, sport)
    away_pos = get_team_position(conn, away, sport)

    if home_pos and home_pos['games_played'] >= 20:
        if home_pos['is_relegation']:
            home_factor = 1.10
        elif home_pos['is_title_race']:
            home_factor = 1.05
        elif home_pos['is_dead_rubber']:
            home_factor = 0.85

    if away_pos and away_pos['games_played'] >= 20:
        if away_pos['is_relegation']:
            away_factor = 1.10
        elif away_pos['is_title_race']:
            away_factor = 1.05
        elif away_pos['is_dead_rubber']:
            away_factor = 0.85

    return home_factor, away_factor


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Fetch soccer league standings from ESPN')
    parser.add_argument('--sport', type=str, default=None,
                        help='Sport key or alias (epl, mls, serie_a, la_liga, bundesliga, ligue_one)')
    parser.add_argument('--verbose', action='store_true', default=True,
                        help='Print progress (default: True)')
    parser.add_argument('--force', action='store_true',
                        help='Force refresh even if data is fresh')
    args = parser.parse_args()

    print("Soccer Standings Fetcher")
    print("=" * 40)

    if args.force:
        # Delete existing data to force re-fetch
        conn = sqlite3.connect(DB_PATH)
        _ensure_table(conn)
        if args.sport:
            resolved = SPORT_ALIASES.get(args.sport, args.sport)
            conn.execute("DELETE FROM soccer_standings WHERE sport = ?", (resolved,))
        else:
            conn.execute("DELETE FROM soccer_standings")
        conn.commit()
        conn.close()
        print("  Cleared cached data (--force)")

    fetch_standings(sport=args.sport, verbose=args.verbose)
