"""
ncaa_scores.py — Backup score source from ncaa.com

ESPN's college baseball API misses games (especially doubleheaders).
This scrapes ncaa.com/scoreboard which has EVERY D1 game.

Called automatically by the grader when ESPN can't find a score.
Can also be run standalone to backfill missing results.

Usage:
    python ncaa_scores.py                           # Pull yesterday's scores
    python ncaa_scores.py --days 3                  # Pull last 3 days
    python ncaa_scores.py --date 2026-03-15         # Pull specific date
    python ncaa_scores.py --sport baseball          # Baseball only (default)
    python ncaa_scores.py --sport basketball        # Basketball too
"""
import sqlite3, json, re, sys, os, time
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# NCAA.com scoreboard URLs
NCAA_ENDPOINTS = {
    'baseball_ncaa': {
        'url': 'https://www.ncaa.com/scoreboard/baseball/d1',
        'sport_key': 'baseball_ncaa',
    },
    'basketball_ncaab': {
        'url': 'https://www.ncaa.com/scoreboard/basketball-men/d1',
        'sport_key': 'basketball_ncaab',
    },
}

# NCAA.com also has a JSON API endpoint
NCAA_API = {
    'baseball_ncaa': 'https://data.ncaa.com/casablanca/scoreboard/baseball/d1/{date}/scoreboard.json',
    'basketball_ncaab': 'https://data.ncaa.com/casablanca/scoreboard/basketball-men/d1/{date}/scoreboard.json',
}


def _fetch_ncaa_json(sport, date_str):
    """Try the NCAA JSON API first (cleaner than scraping HTML)."""
    template = NCAA_API.get(sport)
    if not template:
        return None
    
    # NCAA API uses YYYY/MM/DD format
    formatted = date_str.replace('-', '/')
    url = template.format(date=formatted)
    
    req = Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    })
    try:
        resp = urlopen(req, timeout=15)
        data = json.loads(resp.read().decode())
        return data
    except Exception as e:
        return None


def _fetch_ncaa_html(sport, date_str):
    """Fallback: scrape the HTML scoreboard page."""
    cfg = NCAA_ENDPOINTS.get(sport)
    if not cfg:
        return None
    
    # URL format: /scoreboard/baseball/d1/2026/03/15/all-conf
    formatted = date_str.replace('-', '/')
    url = f"{cfg['url']}/{formatted}/all-conf"
    
    req = Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    })
    try:
        resp = urlopen(req, timeout=15)
        return resp.read().decode()
    except Exception as e:
        return None


def _parse_ncaa_json(data, sport):
    """Parse NCAA JSON API response into game results."""
    games = []
    
    if not data:
        return games
    
    # The NCAA API returns games under 'games' key
    game_list = data.get('games', [])
    
    for game_data in game_list:
        try:
            game = game_data.get('game', game_data)
            
            # Check if game is final
            status = game.get('gameState', '') or game.get('currentPeriod', '')
            if 'final' not in str(status).lower() and 'f' != str(status).lower():
                # Also check status object
                game_status = game.get('status', {})
                if isinstance(game_status, dict):
                    state = game_status.get('state', '')
                    if 'final' not in state.lower():
                        continue
                elif 'final' not in str(game_status).lower():
                    continue
            
            home = game.get('home', {})
            away = game.get('away', {})
            
            # Get team names — try multiple fields
            home_name = (home.get('names', {}).get('full', '') or 
                        home.get('teamName', '') or
                        home.get('name', '') or
                        f"{home.get('school', '')} {home.get('mascot', '')}").strip()
            away_name = (away.get('names', {}).get('full', '') or
                        away.get('teamName', '') or
                        away.get('name', '') or
                        f"{away.get('school', '')} {away.get('mascot', '')}").strip()
            
            if not home_name or not away_name:
                continue
            
            # Get scores
            home_score = home.get('score', home.get('currentScore'))
            away_score = away.get('score', away.get('currentScore'))
            
            if home_score is None or away_score is None:
                continue
            
            try:
                home_score = int(home_score)
                away_score = int(away_score)
            except (ValueError, TypeError):
                continue
            
            games.append({
                'home': home_name,
                'away': away_name,
                'home_score': home_score,
                'away_score': away_score,
            })
        except Exception:
            continue
    
    return games


def _parse_ncaa_html(html, sport):
    """Parse NCAA HTML scoreboard into game results."""
    games = []
    
    if not html:
        return games
    
    # Look for game data in script tags (NCAA often embeds JSON in page)
    json_matches = re.findall(r'window\.__NEXT_DATA__\s*=\s*({.*?})\s*</script>', html, re.DOTALL)
    if json_matches:
        try:
            next_data = json.loads(json_matches[0])
            # Navigate the Next.js data structure
            props = next_data.get('props', {}).get('pageProps', {})
            game_data = props.get('games', []) or props.get('scoreboard', {}).get('games', [])
            return _parse_ncaa_json({'games': game_data}, sport)
        except Exception:
            pass
    
    # Regex fallback: look for score patterns in HTML
    # Pattern: team name followed by score
    game_blocks = re.findall(
        r'class="[^"]*gamePod[^"]*".*?</(?:div|section)>',
        html, re.DOTALL | re.IGNORECASE
    )
    
    for block in game_blocks:
        teams = re.findall(r'class="[^"]*teamName[^"]*"[^>]*>([^<]+)', block)
        scores = re.findall(r'class="[^"]*score[^"]*"[^>]*>(\d+)', block)
        
        if len(teams) >= 2 and len(scores) >= 2:
            games.append({
                'away': teams[0].strip(),
                'home': teams[1].strip(),
                'away_score': int(scores[0]),
                'home_score': int(scores[1]),
            })
    
    return games


def _normalize_ncaa_name(ncaa_name, conn, sport):
    """Map NCAA.com team names to our database names."""
    if not ncaa_name:
        return ncaa_name
    
    # Direct lookup in results/market_consensus
    for table in ['results', 'market_consensus']:
        try:
            row = conn.execute(f"""
                SELECT home FROM {table} WHERE sport=? AND LOWER(home) LIKE ?
                UNION
                SELECT away FROM {table} WHERE sport=? AND LOWER(away) LIKE ?
                LIMIT 1
            """, (sport, f'%{ncaa_name.lower()}%', sport, f'%{ncaa_name.lower()}%')).fetchone()
            if row:
                return row[0]
        except Exception:
            pass
    
    # Try last word (mascot)
    words = ncaa_name.split()
    if len(words) >= 2:
        mascot = words[-1]
        school = words[0]
        try:
            row = conn.execute("""
                SELECT DISTINCT home FROM results WHERE sport=? 
                AND LOWER(home) LIKE ? AND LOWER(home) LIKE ?
                LIMIT 1
            """, (sport, f'%{school.lower()}%', f'%{mascot.lower()}%')).fetchone()
            if row:
                return row[0]
        except Exception:
            pass
    
    return ncaa_name


def fetch_ncaa_scores(sport='baseball_ncaa', date_str=None, days_back=1, verbose=True):
    """
    Fetch scores from NCAA.com and insert missing results.
    
    Returns count of new results inserted.
    """
    conn = sqlite3.connect(DB_PATH)
    
    if date_str:
        dates = [date_str]
    else:
        dates = []
        for d in range(days_back):
            dt = datetime.now() - timedelta(days=d+1)
            dates.append(dt.strftime('%Y-%m-%d'))
    
    total_inserted = 0
    
    for date in dates:
        if verbose:
            print(f"  NCAA.com: Fetching {sport} for {date}...")
        
        # Try JSON API first
        data = _fetch_ncaa_json(sport, date)
        games = _parse_ncaa_json(data, sport) if data else []
        
        # Fallback to HTML scraping
        if not games:
            html = _fetch_ncaa_html(sport, date)
            games = _parse_ncaa_html(html, sport)
        
        if verbose:
            print(f"    Found {len(games)} completed games")
        
        inserted = 0
        for game in games:
            home = _normalize_ncaa_name(game['home'], conn, sport)
            away = _normalize_ncaa_name(game['away'], conn, sport)
            h_score = game['home_score']
            a_score = game['away_score']
            
            # Check if already in DB
            existing = conn.execute("""
                SELECT rowid FROM results
                WHERE sport=? AND completed=1
                AND ((home=? AND away=?) OR (home=? AND away=?))
                AND DATE(commence_time) = DATE(?)
            """, (sport, home, away, away, home, date)).fetchone()
            
            if existing:
                continue
            
            # Also check with original names
            existing2 = conn.execute("""
                SELECT rowid FROM results
                WHERE sport=? AND completed=1
                AND ((home LIKE ? AND away LIKE ?) OR (home LIKE ? AND away LIKE ?))
                AND DATE(commence_time) = DATE(?)
            """, (sport, 
                  f"%{game['home'].split()[-1]}%", f"%{game['away'].split()[-1]}%",
                  f"%{game['away'].split()[-1]}%", f"%{game['home'].split()[-1]}%",
                  date)).fetchone()
            
            if existing2:
                continue
            
            # Insert
            event_id = f"ncaa_{sport}_{date}_{home}_{away}".replace(' ', '_')[:100]
            conn.execute("""
                INSERT INTO results (event_id, sport, home, away, home_score, away_score, completed, commence_time)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            """, (event_id, sport, home, away, h_score, a_score, f"{date}T20:00:00Z"))
            inserted += 1
            
            if verbose:
                print(f"    + {away} {a_score} @ {home} {h_score}")
        
        conn.commit()
        total_inserted += inserted
        time.sleep(0.5)
    
    if verbose:
        print(f"  NCAA.com: {total_inserted} new results inserted")
    
    conn.close()
    return total_inserted


if __name__ == '__main__':
    sport = 'baseball_ncaa'
    days_back = 1
    date_str = None
    
    for i, arg in enumerate(sys.argv):
        if arg == '--sport' and i + 1 < len(sys.argv):
            s = sys.argv[i+1].lower()
            if 'basketball' in s:
                sport = 'basketball_ncaab'
            elif 'baseball' in s:
                sport = 'baseball_ncaa'
        elif arg == '--days' and i + 1 < len(sys.argv):
            days_back = int(sys.argv[i+1])
        elif arg == '--date' and i + 1 < len(sys.argv):
            date_str = sys.argv[i+1]
    
    print(f"  Fetching {sport} from NCAA.com...")
    n = fetch_ncaa_scores(sport=sport, date_str=date_str, days_back=days_back)
    print(f"  Done: {n} new results")
