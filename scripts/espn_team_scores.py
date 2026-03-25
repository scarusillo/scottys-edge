"""
espn_team_scores.py — Fetch missing scores via ESPN team schedule endpoint

ESPN's scoreboard misses games (especially college baseball doubleheaders).
This uses the team-specific schedule endpoint which returns ALL games.

Called automatically by the grader when a score can't be found.
Can also be run standalone to backfill.

Usage:
    python espn_team_scores.py                           # Backfill missing from last 3 days
    python espn_team_scores.py --team "Vanderbilt" --sport baseball_ncaa
"""
import sqlite3, json, os, sys, time
from datetime import datetime, timedelta
from urllib.request import Request, urlopen

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

ESPN_SPORT_MAP = {
    'baseball_ncaa': 'baseball/college-baseball',
    'basketball_ncaab': 'basketball/mens-college-basketball',
    'basketball_nba': 'basketball/nba',
    'icehockey_nhl': 'hockey/nhl',
}

# Cache team IDs so we don't look them up every time
_team_id_cache = {}

# Odds API → ESPN name mismatches
TEAM_NAME_ALIASES = {
    'South Carolina Upstate Spartans': 'USC Upstate Spartans',
    'CSU Fullerton Titans': 'Cal State Fullerton Titans',
    'CSU Northridge Matadors': 'Cal State Northridge Matadors',
    'CSU Bakersfield Roadrunners': 'Cal State Bakersfield Roadrunners',
    'Florida St Seminoles': 'Florida State Seminoles',
    'Michigan St Spartans': 'Michigan State Spartans',
    'Ohio St Buckeyes': 'Ohio State Buckeyes',
    'Arizona St Sun Devils': 'Arizona State Sun Devils',
    'Oregon St Beavers': 'Oregon State Beavers',
    'Oklahoma St Cowboys': 'Oklahoma State Cowboys',
    'San Diego St Aztecs': 'San Diego State Aztecs',
    'Penn St Nittany Lions': 'Penn State Nittany Lions',
    'Nicholls St Colonels': 'Nicholls Colonels',
    'McNeese St Cowboys': 'McNeese Cowboys',
    'SE Missouri St Redhawks': 'Southeast Missouri State Redhawks',
    "Hawaii Rainbow Warriors": "Hawai'i Rainbow Warriors",
    'Grand Canyon Antelopes': 'Grand Canyon Lopes',
    'UMass Minutemen': 'Massachusetts Minutemen',
}


def _get_team_id(team_name, sport):
    """Look up ESPN team ID by name."""
    # Resolve known name mismatches
    team_name = TEAM_NAME_ALIASES.get(team_name, team_name)
    
    cache_key = f"{sport}|{team_name}"
    if cache_key in _team_id_cache:
        return _team_id_cache[cache_key]
    
    espn_sport = ESPN_SPORT_MAP.get(sport)
    if not espn_sport:
        return None
    
    # Search by team name
    search_term = team_name.split()[-1]  # Use mascot name for search
    if len(search_term) < 4:
        search_term = team_name.split()[0]  # Use school name if mascot too short
    
    url = f"https://site.api.espn.com/apis/site/v2/sports/{espn_sport}/teams?limit=500"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        resp = urlopen(req, timeout=20)
        data = json.loads(resp.read().decode())
        
        for sport_data in data.get('sports', []):
            for league in sport_data.get('leagues', []):
                for team in league.get('teams', []):
                    t = team.get('team', team)
                    name = t.get('displayName', t.get('name', ''))
                    tid = t.get('id')
                    # Cache all teams while we're here
                    _team_id_cache[f"{sport}|{name}"] = tid
                    
                    # Check for match
                    if name.lower() == team_name.lower():
                        return tid
                    # Fuzzy: check if mascot matches
                    if team_name.split()[-1].lower() in name.lower() and team_name.split()[0].lower() in name.lower():
                        _team_id_cache[cache_key] = tid
                        return tid
    except Exception as e:
        pass
    
    return None


def _fetch_team_schedule(team_id, sport, season=None):
    """Fetch full season schedule for a team."""
    espn_sport = ESPN_SPORT_MAP.get(sport)
    if not espn_sport or not team_id:
        return []
    
    url = f"https://site.api.espn.com/apis/site/v2/sports/{espn_sport}/teams/{team_id}/schedule"
    if season:
        url += f"?season={season}"
    
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        resp = urlopen(req, timeout=20)
        data = json.loads(resp.read().decode())
        return data.get('events', [])
    except Exception as e:
        return []


def fetch_missing_score(team_name, sport, bet_date):
    """
    Look up a specific team's game result for a given date.
    
    Returns: (home, away, home_score, away_score) or None
    """
    team_id = _get_team_id(team_name, sport)
    if not team_id:
        return None
    
    events = _fetch_team_schedule(team_id, sport)
    if not events:
        return None
    
    # Find games on the bet date
    for event in events:
        event_date = str(event.get('date', ''))[:10]
        if event_date != bet_date:
            continue
        
        for comp in event.get('competitions', []):
            status = comp.get('status', {}).get('type', {}).get('name', '')
            if status != 'STATUS_FINAL':
                continue
            
            competitors = comp.get('competitors', [])
            if len(competitors) != 2:
                continue
            
            home_team = away_team = None
            home_score = away_score = None
            
            for c in competitors:
                t = c.get('team', {})
                name = t.get('displayName', t.get('name', ''))
                score_data = c.get('score', {})
                
                # Score can be a dict with 'value' or a simple string
                if isinstance(score_data, dict):
                    score = int(float(score_data.get('value', 0)))
                elif score_data is not None:
                    try:
                        score = int(float(str(score_data)))
                    except:
                        score = 0
                else:
                    score = 0
                
                if c.get('homeAway') == 'home':
                    home_team = name
                    home_score = score
                else:
                    away_team = name
                    away_score = score
            
            if home_team and away_team and home_score is not None and away_score is not None:
                # Check if this involves our team
                if (team_name.split()[-1].lower() in home_team.lower() or 
                    team_name.split()[-1].lower() in away_team.lower()):
                    return (home_team, away_team, home_score, away_score)
    
    return None


def backfill_missing(days_back=3, verbose=True):
    """
    Find all ungraded bets and try to fetch their scores via team endpoint.
    """
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    
    # Find bets that haven't been graded yet
    ungraded = conn.execute("""
        SELECT DISTINCT b.event_id, b.selection, b.sport, b.created_at
        FROM bets b
        LEFT JOIN graded_bets g ON b.event_id = g.event_id AND g.result NOT IN ('DUPLICATE', 'PENDING')
        WHERE g.event_id IS NULL
        AND DATE(b.created_at) >= DATE('now', ?)
        AND b.sport IN ('baseball_ncaa', 'basketball_ncaab')
    """, (f'-{days_back} days',)).fetchall()
    
    if not ungraded:
        if verbose:
            print("  No ungraded bets found")
        conn.close()
        return 0
    
    if verbose:
        print(f"  Found {len(ungraded)} ungraded bets to look up")
    
    inserted = 0
    for eid, selection, sport, created in ungraded:
        bet_date = created[:10]
        
        # Get team names from odds table
        teams_row = conn.execute("""
            SELECT DISTINCT home, away FROM odds WHERE event_id=? LIMIT 1
        """, (eid,)).fetchone()
        
        if not teams_row:
            teams_row = conn.execute("""
                SELECT DISTINCT home, away FROM market_consensus WHERE event_id=? LIMIT 1
            """, (eid,)).fetchone()
        
        if not teams_row:
            continue
        
        home_name, away_name = teams_row
        
        # Check if score already exists in results
        existing = conn.execute("""
            SELECT rowid FROM results WHERE sport=? AND completed=1
            AND ((home LIKE ? OR home LIKE ?) AND (away LIKE ? OR away LIKE ?))
            AND DATE(commence_time) BETWEEN DATE(?) AND DATE(?, '+1 day')
        """, (sport, f'%{home_name.split()[-1]}%', f'%{home_name}%',
              f'%{away_name.split()[-1]}%', f'%{away_name}%',
              bet_date, bet_date)).fetchone()
        
        if existing:
            continue
        
        if verbose:
            print(f"  Looking up: {home_name} vs {away_name} ({bet_date})...")
        
        # Try home team first, then away
        result = fetch_missing_score(home_name, sport, bet_date)
        if not result:
            result = fetch_missing_score(away_name, sport, bet_date)
        
        if result:
            r_home, r_away, r_hscore, r_ascore = result
            event_id = f"espn_team_{sport}_{bet_date}_{r_home}_{r_away}".replace(' ', '_')[:100]
            
            conn.execute("""
                INSERT OR IGNORE INTO results (event_id, sport, home, away, home_score, away_score, completed, commence_time)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            """, (event_id, sport, r_home, r_away, r_hscore, r_ascore, f"{bet_date}T20:00:00Z"))
            inserted += 1
            
            if verbose:
                print(f"    + {r_away} {r_ascore} @ {r_home} {r_hscore}")
            
            time.sleep(0.5)
        else:
            if verbose:
                print(f"    Not found on ESPN team endpoint")
    
    conn.commit()
    conn.close()
    
    if verbose:
        print(f"  ESPN team endpoint: {inserted} new results")
    
    return inserted


def backfill_thin_teams(sport, min_games=8, max_lookups=50, verbose=True):
    """
    Proactively backfill teams with fewer than min_games results.
    This improves Elo accuracy by filling ESPN scoreboard coverage gaps
    using the team-specific schedule endpoint (which returns ALL games).

    Only looks up teams that appear in today's odds (upcoming games),
    so we focus on teams we'll actually need ratings for.
    """
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")

    if sport not in ESPN_SPORT_MAP:
        conn.close()
        return 0

    # Find teams with thin data that have upcoming games
    thin_teams = conn.execute("""
        SELECT team, cnt FROM (
            SELECT team, COUNT(*) as cnt FROM (
                SELECT home as team FROM results WHERE sport=? AND completed=1
                UNION ALL
                SELECT away as team FROM results WHERE sport=? AND completed=1
            ) GROUP BY team
        ) WHERE cnt < ?
        ORDER BY cnt ASC
    """, (sport, sport, min_games)).fetchall()

    if not thin_teams:
        conn.close()
        return 0

    # Only backfill teams that appear in upcoming odds (active teams)
    upcoming = set()
    for row in conn.execute("SELECT DISTINCT home, away FROM odds WHERE sport=?", (sport,)).fetchall():
        upcoming.add(row[0])
        upcoming.add(row[1])

    thin_active = [(t, c) for t, c in thin_teams if t in upcoming]
    if not thin_active:
        conn.close()
        return 0

    if verbose:
        print(f"  ESPN backfill: {len(thin_active)} active teams with <{min_games} games in {sport}")

    inserted = 0
    looked_up = 0
    for team_name, game_count in thin_active:
        if looked_up >= max_lookups:
            break

        team_id = _get_team_id(team_name, sport)
        if not team_id:
            continue

        looked_up += 1
        events = _fetch_team_schedule(team_id, sport)

        for event in events:
            for comp in event.get('competitions', []):
                status = comp.get('status', {}).get('type', {}).get('name', '')
                if status != 'STATUS_FINAL':
                    continue

                competitors = comp.get('competitors', [])
                if len(competitors) != 2:
                    continue

                home_team = away_team = None
                home_score = away_score = None
                event_date = str(event.get('date', ''))[:10]

                for c in competitors:
                    t = c.get('team', {})
                    name = t.get('displayName', t.get('name', ''))
                    score_data = c.get('score', {})
                    if isinstance(score_data, dict):
                        score = int(float(score_data.get('value', 0)))
                    elif score_data is not None:
                        try:
                            score = int(float(str(score_data)))
                        except Exception:
                            score = 0
                    else:
                        score = 0

                    if c.get('homeAway') == 'home':
                        home_team = name
                        home_score = score
                    else:
                        away_team = name
                        away_score = score

                if not (home_team and away_team and home_score is not None and away_score is not None):
                    continue

                # Check if this result already exists
                existing = conn.execute("""
                    SELECT 1 FROM results WHERE sport=? AND completed=1
                    AND home=? AND away=? AND DATE(commence_time)=DATE(?)
                """, (sport, home_team, away_team, event_date)).fetchone()

                if existing:
                    continue

                event_id = f"espn_team_{sport}_{event_date}_{home_team}_{away_team}".replace(' ', '_')[:100]
                commence = event.get('date', f"{event_date}T20:00:00Z")

                conn.execute("""
                    INSERT INTO results (event_id, sport, home, away, home_score, away_score,
                                        completed, commence_time, actual_total, actual_margin)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                """, (event_id, sport, home_team, away_team, home_score, away_score,
                      commence, home_score + away_score, home_score - away_score))
                inserted += 1

        time.sleep(0.3)

    conn.commit()
    conn.close()

    if verbose and inserted:
        print(f"  ESPN backfill: +{inserted} games for {looked_up} thin teams")

    return inserted


if __name__ == '__main__':
    team = None
    sport = 'baseball_ncaa'
    days = 3
    
    for i, arg in enumerate(sys.argv):
        if arg == '--team' and i + 1 < len(sys.argv):
            team = sys.argv[i + 1]
        elif arg == '--sport' and i + 1 < len(sys.argv):
            sport = sys.argv[i + 1]
        elif arg == '--days' and i + 1 < len(sys.argv):
            days = int(sys.argv[i + 1])
    
    if team:
        print(f"  Looking up {team} ({sport})...")
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        for date in [today, yesterday]:
            result = fetch_missing_score(team, sport, date)
            if result:
                print(f"  {date}: {result[1]} {result[3]} @ {result[0]} {result[2]}")
    else:
        print("  Backfilling missing scores via ESPN team endpoint...")
        n = backfill_missing(days_back=days)
        print(f"  Done: {n} new results")
