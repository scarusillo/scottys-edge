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
    # Tennis — ATP and WTA
    'tennis_atp': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard',
        'season_start': '2026-01-06',  # Australian Open qualifying
    },
    'tennis_wta': {
        'url': 'https://site.api.espn.com/apis/site/v2/sports/tennis/wta/scoreboard',
        'season_start': '2026-01-06',
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


# ── Tennis tournament name → Odds API sport key mapping ──
# ESPN uses full tournament names; Odds API uses sport keys.
_TENNIS_TOURNAMENT_MAP = {
    # ATP
    'australian open': 'tennis_atp_aus_open_singles',
    'roland garros': 'tennis_atp_french_open',
    'french open': 'tennis_atp_french_open',
    'wimbledon': 'tennis_atp_wimbledon',
    'us open': 'tennis_atp_us_open',
    'indian wells': 'tennis_atp_indian_wells',
    'bnp paribas open': 'tennis_atp_indian_wells',
    'miami open': 'tennis_atp_miami_open',
    'monte carlo': 'tennis_atp_monte_carlo_masters',
    'rolex monte-carlo masters': 'tennis_atp_monte_carlo_masters',
    'madrid open': 'tennis_atp_madrid_open',
    'mutua madrid open': 'tennis_atp_madrid_open',
    'italian open': 'tennis_atp_italian_open',
    'internazionali bnl': 'tennis_atp_italian_open',
    'canadian open': 'tennis_atp_canadian_open',
    'national bank open': 'tennis_atp_canadian_open',
    'cincinnati open': 'tennis_atp_cincinnati_open',
    'western & southern open': 'tennis_atp_cincinnati_open',
    'shanghai masters': 'tennis_atp_shanghai_masters',
    'rolex shanghai masters': 'tennis_atp_shanghai_masters',
    'paris masters': 'tennis_atp_paris_masters',
    'rolex paris masters': 'tennis_atp_paris_masters',
    'dubai': 'tennis_atp_dubai',
    'dubai duty free': 'tennis_atp_dubai',
    'qatar open': 'tennis_atp_qatar_open',
    'china open': 'tennis_atp_china_open',
}

# WTA versions (appended separately so WTA scoreboard maps correctly)
_TENNIS_TOURNAMENT_MAP_WTA = {
    'australian open': 'tennis_wta_aus_open_singles',
    'roland garros': 'tennis_wta_french_open',
    'french open': 'tennis_wta_french_open',
    'wimbledon': 'tennis_wta_wimbledon',
    'us open': 'tennis_wta_us_open',
    'indian wells': 'tennis_wta_indian_wells',
    'bnp paribas open': 'tennis_wta_indian_wells',
    'miami open': 'tennis_wta_miami_open',
    'madrid open': 'tennis_wta_madrid_open',
    'mutua madrid open': 'tennis_wta_madrid_open',
    'italian open': 'tennis_wta_italian_open',
    'internazionali bnl': 'tennis_wta_italian_open',
    'canadian open': 'tennis_wta_canadian_open',
    'national bank open': 'tennis_wta_canadian_open',
    'cincinnati open': 'tennis_wta_cincinnati_open',
    'western & southern open': 'tennis_wta_cincinnati_open',
    'dubai': 'tennis_wta_dubai',
    'dubai duty free': 'tennis_wta_dubai',
    'qatar open': 'tennis_wta_qatar_open',
    'china open': 'tennis_wta_china_open',
    'wuhan open': 'tennis_wta_wuhan_open',
}


def _map_tennis_tournament(tournament_name, tour='atp'):
    """Map ESPN tournament name to Odds API sport key."""
    name_lower = tournament_name.lower().strip()
    tmap = _TENNIS_TOURNAMENT_MAP_WTA if tour == 'wta' else _TENNIS_TOURNAMENT_MAP
    # Try exact substring match
    for key_phrase, sport_key in tmap.items():
        if key_phrase in name_lower:
            return sport_key
    # Fallback: generic key
    return f'tennis_{tour}_{name_lower.replace(" ", "_")}'


def _parse_tennis_round(header):
    """Normalize ESPN round header to short code."""
    h = header.lower().strip()
    round_map = {
        'final': 'F', 'finals': 'F',
        'semifinals': 'SF', 'semi-finals': 'SF',
        'quarterfinals': 'QF', 'quarter-finals': 'QF',
        'round of 16': 'R4', '4th round': 'R4', 'fourth round': 'R4',
        'round of 32': 'R3', '3rd round': 'R3', 'third round': 'R3',
        'round of 64': 'R2', '2nd round': 'R2', 'second round': 'R2',
        'round of 128': 'R1', '1st round': 'R1', 'first round': 'R1',
    }
    for phrase, code in round_map.items():
        if phrase in h:
            return code
    return header[:10]


def fetch_tennis_scores(tour='atp', days_back=None, verbose=True):
    """
    Pull completed tennis match results from ESPN.

    ESPN tennis has different JSON nesting than team sports:
    events[] → one tournament per event
    events[].groupings[] → round groupings (Men's Singles, etc.)
    events[].groupings[].competitions[] → individual matches

    Stores results with player names as home/away (convention: first listed = home).
    Also populates tennis_metadata with surface, round, set scores, total games.
    """
    from config import TENNIS_SURFACES, TENNIS_BEST_OF

    sport_key = f'tennis_{tour}'
    cfg = ESPN_ENDPOINTS.get(sport_key)
    if not cfg:
        if verbose:
            print(f"  ⚠ No ESPN endpoint for {sport_key}")
        return 0

    conn = sqlite3.connect(DB_PATH)

    # Ensure tennis_metadata table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tennis_metadata (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id        TEXT NOT NULL UNIQUE,
            tournament      TEXT,
            surface         TEXT,
            round           TEXT,
            best_of         INTEGER,
            set_scores      TEXT,
            total_games     INTEGER,
            player1_rank    INTEGER,
            player2_rank    INTEGER,
            match_duration_min INTEGER
        )
    """)

    if days_back:
        start_date = datetime.now() - timedelta(days=days_back)
    else:
        start_date = datetime.strptime(cfg['season_start'], '%Y-%m-%d')

    end_date = datetime.now() - timedelta(days=1)

    if verbose:
        total_days = (end_date - start_date).days + 1
        print(f"  🎾 Pulling {tour.upper()} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({total_days} days)")

    inserted = 0
    skipped = 0
    errors = 0
    current = start_date

    while current <= end_date:
        date_str = current.strftime('%Y%m%d')
        data = _fetch_espn_scoreboard(cfg['url'], date_str)
        if not data:
            errors += 1
            current += timedelta(days=1)
            time.sleep(0.3)
            continue

        for event in data.get('events', []):
            tournament_name = event.get('name', '')
            odds_api_key = _map_tennis_tournament(tournament_name, tour)
            surface = TENNIS_SURFACES.get(odds_api_key, 'hard')
            best_of = TENNIS_BEST_OF.get(odds_api_key, 3)

            # Tennis uses groupings (Men's Singles, Women's Singles, etc.)
            groupings = event.get('groupings', [])
            # If no groupings, try competitions directly (some ESPN formats)
            if not groupings:
                groupings = [{'grouping': {'displayName': 'Singles'}, 'competitions': event.get('competitions', [])}]

            for grouping in groupings:
                # ESPN tennis uses grouping.displayName (not header)
                grp_info = grouping.get('grouping', {})
                group_name = grp_info.get('displayName', '') or grouping.get('header', '')
                group_lower = group_name.lower()
                # Only singles matches (skip doubles)
                if 'double' in group_lower:
                    continue
                # Filter by gender: ATP endpoint → Men's Singles only, WTA → Women's Singles only
                if tour == 'atp' and 'women' in group_lower:
                    continue
                if tour == 'wta' and 'men' in group_lower and 'women' not in group_lower:
                    continue

                for comp in grouping.get('competitions', []):
                    status = comp.get('status', {}).get('type', {}).get('completed', False)
                    if not status:
                        continue

                    competitors = comp.get('competitors', [])
                    if len(competitors) != 2:
                        continue

                    # Extract player info
                    p1 = competitors[0]
                    p2 = competitors[1]
                    p1_name = p1.get('athlete', {}).get('displayName', '') or p1.get('team', {}).get('displayName', '')
                    p2_name = p2.get('athlete', {}).get('displayName', '') or p2.get('team', {}).get('displayName', '')

                    if not p1_name or not p2_name:
                        continue

                    # Compute set counts and total games from linescores
                    # ESPN tennis: score field is often None; use linescores + winner flag
                    p1_linescores = p1.get('linescores', [])
                    p2_linescores = p2.get('linescores', [])

                    set_scores = []
                    total_games = 0
                    p1_sets = 0
                    p2_sets = 0
                    num_sets = max(len(p1_linescores), len(p2_linescores))

                    for si in range(num_sets):
                        s1 = int(p1_linescores[si].get('value', 0)) if si < len(p1_linescores) else 0
                        s2 = int(p2_linescores[si].get('value', 0)) if si < len(p2_linescores) else 0
                        set_scores.append([s1, s2])
                        total_games += s1 + s2
                        # Determine set winner
                        p1_won_set = p1_linescores[si].get('winner', False) if si < len(p1_linescores) else False
                        if p1_won_set:
                            p1_sets += 1
                        else:
                            p2_sets += 1

                    if p1_sets == 0 and p2_sets == 0:
                        continue  # Walkover or bad data

                    # Rankings
                    p1_rank = None
                    p2_rank = None
                    try:
                        p1_rank = int(p1.get('curatedRank', {}).get('current', 0)) or None
                    except (ValueError, TypeError):
                        pass
                    try:
                        p2_rank = int(p2.get('curatedRank', {}).get('current', 0)) or None
                    except (ValueError, TypeError):
                        pass

                    # Determine round from competition status description
                    comp_round = comp.get('status', {}).get('type', {}).get('description', '')
                    match_round = _parse_tennis_round(comp_round) if comp_round else ''

                    # Generate event ID
                    espn_id = comp.get('id', '') or event.get('id', '')
                    event_id = f"espn_tennis_{tour}_{espn_id}"

                    commence_time = comp.get('date', '') or event.get('date', current.isoformat() + 'Z')

                    winner = p1_name if p1_sets > p2_sets else p2_name
                    margin = p1_sets - p2_sets

                    # Check for duplicates
                    existing = conn.execute(
                        "SELECT id FROM results WHERE sport=? AND home=? AND away=? AND commence_time LIKE ?",
                        (odds_api_key, p1_name, p2_name, commence_time[:10] + '%')
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
                        """, (odds_api_key, event_id, commence_time,
                              p1_name, p2_name,
                              p1_sets, p2_sets, winner,
                              total_games, margin,
                              datetime.now().isoformat()))

                        # Store tennis metadata
                        conn.execute("""
                            INSERT OR IGNORE INTO tennis_metadata
                                (event_id, tournament, surface, round, best_of,
                                 set_scores, total_games, player1_rank, player2_rank)
                            VALUES (?,?,?,?,?,?,?,?,?)
                        """, (event_id, tournament_name, surface, match_round,
                              best_of, json.dumps(set_scores), total_games,
                              p1_rank, p2_rank))

                        inserted += 1
                    except sqlite3.IntegrityError:
                        skipped += 1

        current += timedelta(days=1)
        time.sleep(0.25)

    conn.commit()

    total_results = conn.execute(
        "SELECT COUNT(*) FROM results WHERE sport LIKE 'tennis_%'"
    ).fetchone()[0]

    if verbose:
        print(f"  ✅ tennis_{tour}: +{inserted} new results ({skipped} skipped, {errors} fetch errors)")
        print(f"     Total tennis results in DB: {total_results}")

    conn.close()
    return inserted


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
        # Tennis uses a separate parser due to different ESPN JSON structure
        if sport in ('tennis_atp', 'tennis_wta'):
            tour = sport.replace('tennis_', '')
            total += fetch_tennis_scores(tour=tour, days_back=days_back, verbose=verbose)
        else:
            total += fetch_season_scores(sport, days_back=days_back, verbose=verbose)

    print(f"\n  📊 Total new results: {total}")

    # Show summary
    conn = sqlite3.connect(DB_PATH)
    print(f"\n  DATABASE SUMMARY:")
    for sport in sports:
        if sport.startswith('tennis_'):
            # Tennis results are stored under per-tournament keys
            cnt = conn.execute("SELECT COUNT(*) FROM results WHERE sport LIKE ?",
                               (f'tennis_{sport.replace("tennis_", "")}%',)).fetchone()[0]
        else:
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
