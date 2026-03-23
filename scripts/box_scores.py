"""
box_scores.py — ESPN Box Score Fetcher for Prop Grading

Fetches player stat lines from ESPN's free API after games complete.
This is the missing piece that lets the grader resolve prop bets.

ESPN endpoints (free, no auth):
  NBA:   /apis/site/v2/sports/basketball/nba/summary?event={id}
  NCAAB: /apis/site/v2/sports/basketball/mens-college-basketball/summary?event={id}
  NHL:   /apis/site/v2/sports/hockey/nhl/summary?event={id}

Flow:
  1. Get yesterday's completed ESPN game IDs from scoreboard
  2. For each game with ungraded prop bets, fetch box score
  3. Extract player stats → store in box_scores table
  4. Grader looks up actual values to grade props

API cost: $0 (ESPN is free)
Integrated into: main.py grade command

Usage:
    python box_scores.py                    # Fetch yesterday's box scores
    python box_scores.py --days 3           # Last 3 days
    python box_scores.py --sport nba        # NBA only
"""
import sqlite3
import json
import os
import sys
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ESPN summary endpoints by sport
ESPN_SUMMARY = {
    'basketball_nba': 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={}',
    'basketball_ncaab': 'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={}',
    'icehockey_nhl': 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event={}',
}

ESPN_SCOREBOARD = {
    'basketball_nba': 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={}',
    'basketball_ncaab': 'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={}',
    'icehockey_nhl': 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={}',
}

# Map our prop market types to ESPN box score stat column names
# ESPN basketball stat order: MIN, FG, 3PT, FT, OREB, DREB, REB, AST, STL, BLK, TO, PF, PTS
# ESPN hockey stat order varies but typically: G, A, PTS, +/-, PIM, SOG, ...

BASKETBALL_STAT_MAP = {
    'player_points': 'PTS',
    'player_rebounds': 'REB',
    'player_assists': 'AST',
    'player_threes': '3PT',     # Need to parse "2-5" → 2
    'player_blocks': 'BLK',
    'player_steals': 'STL',
}

HOCKEY_STAT_MAP = {
    'player_shots_on_goal': 'SOG',
    'player_points': 'PTS',     # Hockey points (G+A)
    'player_power_play_points': 'PPP',
    'player_blocked_shots': 'BS',
}


def ensure_table(conn):
    """Create box_scores table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS box_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at TEXT NOT NULL,
            game_date TEXT NOT NULL,
            sport TEXT NOT NULL,
            espn_game_id TEXT NOT NULL,
            team TEXT NOT NULL,
            player TEXT NOT NULL,
            stat_type TEXT NOT NULL,
            stat_value REAL,
            UNIQUE(espn_game_id, player, stat_type)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_bs_player
        ON box_scores(player, stat_type, game_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_bs_game
        ON box_scores(espn_game_id, sport)
    """)
    conn.commit()


def _fetch_json(url):
    """Fetch JSON from ESPN with retry."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    for attempt in range(3):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except (URLError, HTTPError) as e:
            if attempt < 2:
                time.sleep(2)
            else:
                print(f"    ESPN fetch failed: {e}")
                return None
        except Exception as e:
            print(f"    ESPN error: {e}")
            return None


def _get_espn_game_ids(sport, game_date):
    """Get ESPN game IDs from the scoreboard for a given date."""
    url_template = ESPN_SCOREBOARD.get(sport)
    if not url_template:
        return []

    date_str = game_date.strftime('%Y%m%d')
    url = url_template.format(date_str)
    data = _fetch_json(url)
    if not data:
        return []

    games = []
    for event in data.get('events', []):
        espn_id = event.get('id', '')
        status = event.get('status', {}).get('type', {}).get('state', '')
        if status != 'post':
            continue  # Only completed games

        # Extract team names
        competitors = event.get('competitions', [{}])[0].get('competitors', [])
        home_team = ''
        away_team = ''
        for c in competitors:
            name = c.get('team', {}).get('displayName', '')
            if c.get('homeAway') == 'home':
                home_team = name
            else:
                away_team = name

        games.append({
            'espn_id': espn_id,
            'home': home_team,
            'away': away_team,
            'date': game_date.strftime('%Y-%m-%d'),
        })

    return games


def _parse_basketball_box(data, sport, espn_id, game_date):
    """Parse basketball box score into player stat rows."""
    rows = []
    boxscore = data.get('boxscore', {})
    players_sections = boxscore.get('players', [])

    for section in players_sections:
        team_name = section.get('team', {}).get('displayName', '')
        for stat_group in section.get('statistics', []):
            stat_names = stat_group.get('names', [])
            # Build index map: stat_name → column index
            idx_map = {name: i for i, name in enumerate(stat_names)}

            for athlete_data in stat_group.get('athletes', []):
                player_name = athlete_data.get('athlete', {}).get('displayName', '')
                stats = athlete_data.get('stats', [])

                if not player_name or not stats:
                    continue

                # Skip DNP (stats will be empty or all zeros with short list)
                if len(stats) < len(stat_names):
                    continue

                now = datetime.now().isoformat()

                # Extract each stat we care about
                for prop_market, espn_col in BASKETBALL_STAT_MAP.items():
                    if espn_col not in idx_map:
                        continue

                    raw_val = stats[idx_map[espn_col]]

                    # Parse value — some are "2-5" format (made-attempted)
                    if isinstance(raw_val, str) and '-' in raw_val:
                        # "2-5" → take the made count (first number)
                        try:
                            val = float(raw_val.split('-')[0])
                        except ValueError:
                            continue
                    else:
                        try:
                            val = float(raw_val)
                        except (ValueError, TypeError):
                            continue

                    # Map to our stat_type names (pts, reb, ast, threes, etc.)
                    stat_type_map = {
                        'player_points': 'pts',
                        'player_rebounds': 'reb',
                        'player_assists': 'ast',
                        'player_threes': 'threes',
                        'player_blocks': 'blk',
                        'player_steals': 'stl',
                    }
                    stat_type = stat_type_map.get(prop_market, prop_market)

                    rows.append((
                        now, game_date, sport, espn_id,
                        team_name, player_name, stat_type, val
                    ))

    return rows


def _parse_hockey_box(data, sport, espn_id, game_date):
    """Parse hockey box score into player stat rows.

    ESPN NHL uses 'labels' not 'names' for column headers.
    Skater labels: BS, HT, TK, +/-, TOI, PPTOI, SHTOI, ESTOI, SHFT, G, YTDG, A, S, SM, SOG, FW, FL, FO%, GV, PN, PIM
    Points (G+A) must be computed since ESPN doesn't provide a PTS column for skaters.
    """
    rows = []
    boxscore = data.get('boxscore', {})
    players_sections = boxscore.get('players', [])

    for section in players_sections:
        team_name = section.get('team', {}).get('displayName', '')
        for stat_group in section.get('statistics', []):
            # ESPN NHL uses 'labels' instead of 'names'
            stat_labels = stat_group.get('labels', []) or stat_group.get('names', [])
            idx_map = {name: i for i, name in enumerate(stat_labels)}

            # Skip goalie sections (they have GA, SA, SV% etc)
            if 'GA' in idx_map or 'SV%' in idx_map:
                continue

            for athlete_data in stat_group.get('athletes', []):
                player_name = athlete_data.get('athlete', {}).get('displayName', '')
                stats = athlete_data.get('stats', [])

                if not player_name or not stats:
                    continue

                now = datetime.now().isoformat()

                # SOG (shots on goal)
                if 'SOG' in idx_map:
                    try:
                        val = float(stats[idx_map['SOG']])
                        rows.append((now, game_date, sport, espn_id, team_name, player_name, 'sog', val))
                    except (ValueError, TypeError, IndexError):
                        pass

                # Points = Goals + Assists (ESPN doesn't have PTS for skaters)
                g_val, a_val = 0.0, 0.0
                if 'G' in idx_map:
                    try:
                        g_val = float(stats[idx_map['G']])
                    except (ValueError, TypeError, IndexError):
                        pass
                if 'A' in idx_map:
                    try:
                        a_val = float(stats[idx_map['A']])
                    except (ValueError, TypeError, IndexError):
                        pass
                pts = g_val + a_val
                rows.append((now, game_date, sport, espn_id, team_name, player_name, 'hockey_pts', pts))

                # Blocked shots
                if 'BS' in idx_map:
                    try:
                        val = float(stats[idx_map['BS']])
                        rows.append((now, game_date, sport, espn_id, team_name, player_name, 'blocked_shots', val))
                    except (ValueError, TypeError, IndexError):
                        pass

                # Power play points — ESPN doesn't have PPP directly.
                # Would need PPTOI + goal/assist tracking. Skip for now.

    return rows


def fetch_box_scores(conn, sport, game_date):
    """Fetch box scores for all completed games on a given date."""
    ensure_table(conn)

    games = _get_espn_game_ids(sport, game_date)
    if not games:
        return 0

    total_rows = 0
    for game in games:
        espn_id = game['espn_id']

        # Skip if already fetched
        existing = conn.execute(
            "SELECT COUNT(*) FROM box_scores WHERE espn_game_id=?",
            (espn_id,)
        ).fetchone()[0]
        if existing > 0:
            continue

        # Fetch summary
        url_template = ESPN_SUMMARY.get(sport)
        if not url_template:
            continue

        url = url_template.format(espn_id)
        data = _fetch_json(url)
        if not data:
            continue

        # Parse based on sport
        date_str = game_date.strftime('%Y-%m-%d')
        if 'basketball' in sport:
            rows = _parse_basketball_box(data, sport, espn_id, date_str)
        elif 'hockey' in sport:
            rows = _parse_hockey_box(data, sport, espn_id, date_str)
        else:
            continue

        if rows:
            for row in rows:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO box_scores
                        (fetched_at, game_date, sport, espn_game_id, team, player, stat_type, stat_value)
                        VALUES (?,?,?,?,?,?,?,?)
                    """, row)
                except Exception:
                    pass
            conn.commit()
            total_rows += len(rows)

        # Rate limit — be nice to ESPN
        time.sleep(0.5)

    return total_rows


def fetch_all_box_scores(days_back=2, sports=None):
    """Fetch box scores for recent days across all prop sports."""
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    if sports is None:
        sports = list(ESPN_SUMMARY.keys())

    total = 0
    for day_offset in range(days_back):
        game_date = datetime.now() - timedelta(days=day_offset + 1)

        for sport in sports:
            count = fetch_box_scores(conn, sport, game_date)
            if count > 0:
                print(f"    {sport} {game_date.strftime('%Y-%m-%d')}: {count} player stats")
            total += count

    # Summary
    total_in_db = conn.execute("SELECT COUNT(*) FROM box_scores").fetchone()[0]
    distinct_games = conn.execute("SELECT COUNT(DISTINCT espn_game_id) FROM box_scores").fetchone()[0]
    distinct_players = conn.execute("SELECT COUNT(DISTINCT player) FROM box_scores").fetchone()[0]
    print(f"    Box scores in DB: {total_in_db} stats | {distinct_games} games | {distinct_players} players")

    conn.close()
    return total


# ═══════════════════════════════════════════════════════════════════
# PLAYER STAT LOOKUP — Used by the grader to resolve prop bets
# ═══════════════════════════════════════════════════════════════════

def _normalize_player_name(name):
    """Normalize player name for fuzzy matching.
    
    Handles common differences:
      'LeBron James' vs 'L. James'
      'De'Aaron Fox' vs 'DeAaron Fox'
      'Nickeil Alexander-Walker' vs 'Nickeil Alexander Walker'
    """
    if not name:
        return ''
    # Remove punctuation
    import string
    cleaned = name.translate(str.maketrans('', '', string.punctuation))
    # Lowercase
    cleaned = cleaned.lower().strip()
    # Remove extra spaces
    cleaned = ' '.join(cleaned.split())
    return cleaned


def lookup_player_stat(conn, player_name, stat_type, game_date, sport=None):
    """
    Look up a player's actual stat value from box scores.
    
    Tries exact match first, then fuzzy match by last name + first initial.
    
    Args:
        conn: DB connection
        player_name: e.g., "LeBron James"
        stat_type: e.g., "pts", "reb", "ast", "threes", "sog"
        game_date: "YYYY-MM-DD"
        sport: optional sport filter
    
    Returns: float stat value, or None if not found
    """
    ensure_table(conn)
    
    # Try exact match first
    query = """
        SELECT stat_value, player FROM box_scores
        WHERE stat_type=? AND game_date=?
    """
    params = [stat_type, game_date]
    if sport:
        query += " AND sport=?"
        params.append(sport)

    all_rows = conn.execute(query, params).fetchall()
    
    if not all_rows:
        return None

    # Exact match
    for val, db_player in all_rows:
        if db_player.lower() == player_name.lower():
            return val

    # Fuzzy match: normalize both names
    norm_target = _normalize_player_name(player_name)
    
    for val, db_player in all_rows:
        norm_db = _normalize_player_name(db_player)
        
        # Exact normalized match
        if norm_target == norm_db:
            return val
    
    # Last name match + first initial
    target_parts = player_name.strip().split()
    if len(target_parts) >= 2:
        target_last = target_parts[-1].lower()
        target_first_init = target_parts[0][0].lower()
        
        for val, db_player in all_rows:
            db_parts = db_player.strip().split()
            if len(db_parts) >= 2:
                db_last = db_parts[-1].lower()
                db_first_init = db_parts[0][0].lower()
                
                if target_last == db_last and target_first_init == db_first_init:
                    return val
    
    # Last resort: just last name (risky but catches edge cases)
    if len(target_parts) >= 2:
        target_last = target_parts[-1].lower()
        matches = [(val, db_player) for val, db_player in all_rows
                   if db_player.strip().split()[-1].lower() == target_last]
        if len(matches) == 1:
            return matches[0][0]
    
    return None


# ═══════════════════════════════════════════════════════════════════
# PROP BET GRADING HELPER
# ═══════════════════════════════════════════════════════════════════

# Map prop selection labels back to box score stat types
PROP_TO_STAT = {
    'POINTS': 'pts', 'PTS': 'pts',
    'REBOUNDS': 'reb', 'REB': 'reb',
    'ASSISTS': 'ast', 'AST': 'ast',
    'THREES': 'threes', '3PT': 'threes',
    'BLOCKS': 'blk', 'BLK': 'blk',
    'STEALS': 'stl', 'STL': 'stl',
    'SOG': 'sog',
    'PPP': 'ppp',
    'BLK_SHOTS': 'blocked_shots',
}


def grade_prop(conn, selection, line, game_date, sport=None):
    """
    Grade a prop bet using box score data.
    
    selection: e.g., "LeBron James OVER 25.5 POINTS"
    line: e.g., 25.5
    game_date: "YYYY-MM-DD"
    
    Returns: 'WIN', 'LOSS', 'PUSH', or 'PENDING' (no box score data)
    """
    if not selection or line is None:
        return 'PENDING'
    
    # Parse selection: "Player Name OVER/UNDER 25.5 STAT_LABEL"
    parts = selection.split()
    player_name = None
    side = None
    stat_label = None
    
    for i, part in enumerate(parts):
        if part in ('OVER', 'UNDER'):
            player_name = ' '.join(parts[:i])
            side = part
            # Stat label is the last word(s) after the number
            remaining = parts[i+1:]
            # Skip the number
            for j, r in enumerate(remaining):
                try:
                    float(r)
                except ValueError:
                    stat_label = ' '.join(remaining[j:])
                    break
            break
    
    if not player_name or not side or not stat_label:
        return 'PENDING'
    
    # Map label to stat type
    stat_type = PROP_TO_STAT.get(stat_label.upper().strip())
    if not stat_type:
        # Try first word only
        stat_type = PROP_TO_STAT.get(stat_label.split()[0].upper())
    if not stat_type:
        return 'PENDING'
    
    # Look up actual value
    actual = lookup_player_stat(conn, player_name, stat_type, game_date, sport)
    
    if actual is None:
        return 'PENDING'
    
    # Grade
    if side == 'OVER':
        if actual > line:
            return 'WIN'
        elif actual < line:
            return 'LOSS'
        return 'PUSH'
    elif side == 'UNDER':
        if actual < line:
            return 'WIN'
        elif actual > line:
            return 'LOSS'
        return 'PUSH'
    
    return 'PENDING'


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    days = 2
    sports = None
    
    if '--days' in sys.argv:
        idx = sys.argv.index('--days')
        days = int(sys.argv[idx + 1])
    
    if '--sport' in sys.argv:
        idx = sys.argv.index('--sport')
        sport_map = {
            'nba': 'basketball_nba', 'ncaab': 'basketball_ncaab',
            'nhl': 'icehockey_nhl',
        }
        sports = [sport_map.get(sys.argv[idx + 1], sys.argv[idx + 1])]
    
    print("=" * 60)
    print(f"  ESPN BOX SCORE FETCHER — Last {days} days")
    print("=" * 60)
    print()
    
    total = fetch_all_box_scores(days_back=days, sports=sports)
    print(f"\n  Fetched {total} new player stats")
    
    # Test lookup if we have data
    conn = sqlite3.connect(DB_PATH)
    sample = conn.execute("""
        SELECT player, stat_type, stat_value, game_date FROM box_scores
        ORDER BY fetched_at DESC LIMIT 5
    """).fetchall()
    
    if sample:
        print(f"\n  Sample stats:")
        for player, stat, val, date in sample:
            print(f"    {player}: {val} {stat} ({date})")
    
    conn.close()
