"""
historical_scores.py — Pull full-season game results from ESPN (FREE)

ESPN's public scoreboard API requires no API key and returns completed games
with final scores. This gives us hundreds of real results to build proper
Elo ratings that are INDEPENDENT of market lines.

Usage:
    python historical_scores.py              # Pull all sports, full season
    python historical_scores.py --sport nba  # Just NBA
    python historical_scores.py --days 30    # Last 30 days only
"""
import sqlite3, json, os, sys, time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ESPN scoreboard endpoints (public, no auth needed)
ESPN_ENDPOINTS = {
    'basketball_nba': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard',
        'season_start': '2025-10-22',  # 2025-26 NBA season
    },
    'basketball_ncaab': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard',
        'season_start': '2025-11-04',
    },
    'icehockey_nhl': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard',
        'season_start': '2025-10-04',
    },
    'soccer_epl': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard',
        'season_start': '2025-08-16',
    },
    'soccer_italy_serie_a': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/soccer/ita.1/scoreboard',
        'season_start': '2025-08-17',
    },
    'soccer_spain_la_liga': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/soccer/esp.1/scoreboard',
        'season_start': '2025-08-15',
    },
    'soccer_germany_bundesliga': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/scoreboard',
        'season_start': '2025-08-23',
    },
    'soccer_france_ligue_one': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/soccer/fra.1/scoreboard',
        'season_start': '2025-08-16',
    },
    'soccer_uefa_champs_league': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard',
        'season_start': '2025-09-17',
    },
    'soccer_usa_mls': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard',
        'season_start': '2025-02-22',
    },
    'soccer_mexico_ligamx': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/soccer/mex.1/scoreboard',
        'season_start': '2025-07-11',  # Liga MX Apertura 2025
    },
    'baseball_ncaa': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/scoreboard',
        'season_start': '2026-02-14',  # 2026 college baseball season
    },
}

# Team name mapping: ESPN name → Odds API name
# ESPN sometimes uses different team names than the Odds API.
# We populate this from the teams/team_aliases tables if available,
# then fall back to fuzzy matching.
_name_cache = {}

def _load_name_mappings(conn):
    """Load team name mappings from DB."""
    global _name_cache
    try:
        rows = conn.execute("SELECT espn_name, odds_api_name FROM team_aliases WHERE espn_name IS NOT NULL").fetchall()
        for espn, odds in rows:
            _name_cache[espn.lower()] = odds
    except:
        pass  # Table might not have espn_name column yet
    
    # Hardcoded mappings for common mismatches
    HARDCODED = {
        # NBA
        'la clippers': 'Los Angeles Clippers',
        'la lakers': 'Los Angeles Lakers',
        
        # NHL
        'montréal canadiens': 'Montreal Canadiens',
        'montreal canadiens': 'Montreal Canadiens',
        'utah hockey club': 'Utah Hockey Club',
        
        # NCAAB - these vary wildly between sources
        # ESPN tends to use short names, Odds API uses full names
    }
    for k, v in HARDCODED.items():
        _name_cache[k] = v


def _normalize_team_name(espn_name, conn, sport):
    """Convert ESPN team name to Odds API name, using DB lookups and fuzzy matching."""
    if not espn_name:
        return espn_name
    
    # Check cache first
    key = espn_name.lower().strip()
    if key in _name_cache:
        return _name_cache[key]
    
    # Try exact match in market_consensus or results
    for table in ['market_consensus', 'results']:
        try:
            row = conn.execute(f"""
                SELECT home FROM {table} WHERE sport=? AND LOWER(home)=?
                UNION
                SELECT away FROM {table} WHERE sport=? AND LOWER(away)=?
                LIMIT 1
            """, (sport, key, sport, key)).fetchone()
            if row:
                _name_cache[key] = row[0]
                return row[0]
        except:
            pass
    
    # Try substring match (ESPN "Celtics" → "Boston Celtics")
    try:
        words = espn_name.split()
        for word in reversed(words):  # Try last word first (usually the mascot)
            if len(word) < 4:
                continue
            row = conn.execute("""
                SELECT DISTINCT home FROM market_consensus WHERE sport=? AND LOWER(home) LIKE ?
                UNION
                SELECT DISTINCT away FROM market_consensus WHERE sport=? AND LOWER(away) LIKE ?
                LIMIT 1
            """, (sport, f'%{word.lower()}%', sport, f'%{word.lower()}%')).fetchone()
            if row:
                _name_cache[key] = row[0]
                return row[0]
    except:
        pass
    
    # Return as-is (will still be useful for Elo even without exact mapping)
    _name_cache[key] = espn_name
    return espn_name


def _fetch_espn_scoreboard(url, date_str, sport=None):
    """Fetch one day of ESPN scoreboard data."""
    full_url = f"{url}?dates={date_str}"
    # v12.2 FIX: ESPN scoreboard only returns "featured" games by default.
    # Adding groups=50 (D1) and limit=900 returns ALL D1 games.
    # Without this, college baseball was missing 60%+ of games and
    # the grader was matching bets to old results from wrong dates.
    if sport and ('ncaa' in sport or 'college' in sport):
        full_url += "&groups=50&limit=900"
    req = Request(full_url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        resp = urlopen(req, timeout=15)
        return json.loads(resp.read().decode())
    except (URLError, HTTPError, json.JSONDecodeError) as e:
        return None


def fetch_season_scores(sport, days_back=None, verbose=True):
    """
    Pull all completed games for a sport from ESPN.
    
    Returns count of new results inserted into the results table.
    """
    cfg = ESPN_ENDPOINTS.get(sport)
    if not cfg:
        if verbose:
            print(f"  ⚠ No ESPN endpoint for {sport}")
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    _load_name_mappings(conn)
    
    # Determine date range
    if days_back:
        start_date = datetime.now() - timedelta(days=days_back)
    else:
        start_date = datetime.strptime(cfg['season_start'], '%Y-%m-%d')
    
    end_date = datetime.now() - timedelta(days=1)  # Yesterday (today's games may not be complete)
    
    if verbose:
        total_days = (end_date - start_date).days + 1
        print(f"  📅 Pulling {sport} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({total_days} days)")
    
    inserted = 0
    skipped = 0
    errors = 0
    current = start_date
    
    while current <= end_date:
        date_str = current.strftime('%Y%m%d')
        
        data = _fetch_espn_scoreboard(cfg['url'], date_str, sport=sport)
        if not data:
            errors += 1
            current += timedelta(days=1)
            time.sleep(0.3)
            continue
        
        events = data.get('events', [])
        
        for event in events:
            # Only completed games
            status = event.get('status', {}).get('type', {}).get('completed', False)
            if not status:
                continue
            
            competitions = event.get('competitions', [])
            if not competitions:
                continue
            comp = competitions[0]
            
            # Extract teams and scores
            competitors = comp.get('competitors', [])
            if len(competitors) != 2:
                continue
            
            home_data = next((c for c in competitors if c.get('homeAway') == 'home'), None)
            away_data = next((c for c in competitors if c.get('homeAway') == 'away'), None)
            
            if not home_data or not away_data:
                continue
            
            home_name_raw = home_data.get('team', {}).get('displayName', '')
            away_name_raw = away_data.get('team', {}).get('displayName', '')
            home_score = int(home_data.get('score', 0))
            away_score = int(away_data.get('score', 0))
            
            if home_score == 0 and away_score == 0:
                continue  # Bad data
            
            # Normalize team names to match Odds API
            home = _normalize_team_name(home_name_raw, conn, sport)
            away = _normalize_team_name(away_name_raw, conn, sport)
            
            # Generate a stable event ID from ESPN data
            espn_id = event.get('id', '')
            event_id = f"espn_{sport}_{espn_id}"
            
            commence_time = event.get('date', current.isoformat() + 'Z')
            
            winner = home if home_score > away_score else (away if away_score > home_score else 'DRAW')
            margin = home_score - away_score
            total = home_score + away_score
            
            # Check if we already have this result
            existing = conn.execute(
                "SELECT id FROM results WHERE sport=? AND home=? AND away=? AND commence_time LIKE ?",
                (sport, home, away, commence_time[:10] + '%')
            ).fetchone()
            
            if existing:
                skipped += 1
                continue
            
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO results
                        (sport, event_id, commence_time, home, away,
                         home_score, away_score, winner, completed,
                         actual_total, actual_margin, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,1,?,?,?)
                """, (sport, event_id, commence_time, home, away,
                      home_score, away_score, winner,
                      total, margin,
                      datetime.now().isoformat()))
                inserted += 1
            except sqlite3.IntegrityError:
                skipped += 1
        
        current += timedelta(days=1)
        time.sleep(0.25)  # Be nice to ESPN
    
    conn.commit()
    
    # Report
    total_results = conn.execute(
        "SELECT COUNT(*) FROM results WHERE sport=?", (sport,)
    ).fetchone()[0]
    
    if verbose:
        print(f"  ✅ {sport}: +{inserted} new results ({skipped} skipped, {errors} fetch errors)")
        print(f"     Total results in DB: {total_results}")
    
    conn.close()
    return inserted


def fetch_historical_odds_api(sport, days_back=30, verbose=True):
    """
    Pull historical closing lines from the Odds API (costs 10x credits).
    
    Use AFTER pulling ESPN scores to add closing lines to existing results.
    Only call this if you want CLV tracking — Elo ratings don't need it.
    
    Cost: 30 credits per day per sport (h2h + spreads + totals, us region)
    """
    from odds_api import _api_get, DB_PATH
    
    conn = sqlite3.connect(DB_PATH)
    updated = 0
    
    if verbose:
        estimated_cost = days_back * 30
        print(f"  💰 Historical odds for {sport}: ~{estimated_cost} API credits for {days_back} days")
        print(f"     This adds closing lines to existing results for CLV tracking")
    
    for day_offset in range(days_back, 0, -1):
        target = datetime.now() - timedelta(days=day_offset)
        # Snapshot 2 hours before typical game time (to get near-closing lines)
        # NBA/NCAAB: games around 7pm ET = midnight UTC, so snapshot at 10pm UTC
        date_str = target.strftime('%Y-%m-%dT22:00:00Z')
        
        try:
            data = _api_get(f"/historical/sports/{sport}/odds", {
                'date': date_str,
                'regions': 'us',
                'markets': 'h2h,spreads,totals',
                'oddsFormat': 'american',
            })
        except Exception as e:
            if verbose:
                print(f"    ⚠ {target.strftime('%Y-%m-%d')}: {e}")
            continue
        
        if not data or 'data' not in data:
            continue
        
        events = data.get('data', [])
        for event in events:
            event_id = event.get('id', '')
            home = event.get('home_team', '')
            away = event.get('away_team', '')
            
            closing_spread = None
            closing_total = None
            closing_ml_home = None
            closing_ml_away = None
            
            for bk in event.get('bookmakers', []):
                for mkt in bk.get('markets', []):
                    for outcome in mkt.get('outcomes', []):
                        if mkt['key'] == 'spreads' and outcome['name'] == home:
                            closing_spread = outcome.get('point')
                        elif mkt['key'] == 'totals' and outcome['name'] == 'Over':
                            closing_total = outcome.get('point')
                        elif mkt['key'] == 'h2h':
                            if outcome['name'] == home:
                                closing_ml_home = outcome.get('price')
                            elif outcome['name'] == away:
                                closing_ml_away = outcome.get('price')
                    
                    # Take first bookmaker that has data
                    if closing_spread is not None:
                        break
            
            # Update existing results with closing lines
            if closing_spread is not None or closing_total is not None:
                conn.execute("""
                    UPDATE results SET
                        closing_spread = COALESCE(?, closing_spread),
                        closing_total = COALESCE(?, closing_total),
                        closing_ml_home = COALESCE(?, closing_ml_home),
                        closing_ml_away = COALESCE(?, closing_ml_away)
                    WHERE sport=? AND home=? AND away=?
                    AND commence_time LIKE ?
                """, (closing_spread, closing_total, closing_ml_home, closing_ml_away,
                      sport, home, away, target.strftime('%Y-%m-%d') + '%'))
                updated += 1
            
            # Also store in market_consensus for the totals model
            if closing_total is not None:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO market_consensus
                            (sport, event_id, commence_time, home, away,
                             best_over_total, best_under_total, snapshot_date)
                        VALUES (?,?,?,?,?,?,?,?)
                    """, (sport, event_id, event.get('commence_time', ''),
                          home, away, closing_total, closing_total,
                          target.strftime('%Y-%m-%d')))
                except:
                    pass
        
        time.sleep(0.5)  # Rate limiting
    
    conn.commit()
    if verbose:
        print(f"  ✅ Updated {updated} results with closing lines")
    conn.close()
    return updated


def fetch_all_historical(sports=None, days_back=None, verbose=True):
    """Pull historical scores for all configured sports."""
    if sports is None:
        sports = list(ESPN_ENDPOINTS.keys())
    
    print("=" * 60)
    print("  HISTORICAL SCORES — ESPN (FREE, no API credits)")
    print("=" * 60)
    
    total = 0
    for sport in sports:
        total += fetch_season_scores(sport, days_back=days_back, verbose=verbose)
    
    print(f"\n  📊 Total new results: {total}")
    
    # Show summary
    conn = sqlite3.connect(DB_PATH)
    print(f"\n  DATABASE SUMMARY:")
    for sport in sports:
        cnt = conn.execute("SELECT COUNT(*) FROM results WHERE sport=?", (sport,)).fetchone()[0]
        print(f"    {sport:30s} {cnt:4d} games")
    conn.close()
    
    return total


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Pull historical scores from ESPN (free)')
    parser.add_argument('--sport', type=str, help='Specific sport key (e.g., basketball_nba)')
    parser.add_argument('--days', type=int, default=None, help='Days back (default: full season)')
    parser.add_argument('--odds-api', action='store_true', help='Also pull historical odds (costs API credits)')
    parser.add_argument('--odds-days', type=int, default=30, help='Days of historical odds to pull')
    args = parser.parse_args()
    
    sports = [args.sport] if args.sport else None
    fetch_all_historical(sports=sports, days_back=args.days)
    
    if args.odds_api:
        print()
        print("=" * 60)
        print("  HISTORICAL ODDS — Odds API (costs credits)")
        print("=" * 60)
        target_sports = [args.sport] if args.sport else list(ESPN_ENDPOINTS.keys())
        for sport in target_sports:
            fetch_historical_odds_api(sport, days_back=args.odds_days)
