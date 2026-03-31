"""
pitcher_scraper.py — ESPN Pitcher Data Collector + Rotation Predictor

College baseball ESPN coverage is sparse (~1-2% of games have box score data),
but the data structure is clear when available:
  - starter=True flag identifies the starting pitcher
  - Full pitching line: IP, H, R, ER, BB, K, HR, PC-ST, ERA

Strategy:
  1. BACKFILL: Scan all completed games, extract pitcher data where available
  2. DAILY: After games complete, scrape new pitcher stats
  3. ROTATION MAP: Track which pitchers start on which days
     - College baseball uses predictable 3-man rotations:
       Friday = ace, Saturday = #2, Sunday = #3, midweek = #4/bullpen
  4. PITCHING QUALITY: Compute team pitching quality by day-of-week
     from runs allowed data (available for ALL games via results table)
  5. EXPOSE: get_pitcher_context() for model_engine integration

ESPN endpoints (free, no auth):
  Summary: /apis/site/v2/sports/baseball/college-baseball/summary?event={id}
  Scoreboard: /apis/site/v2/sports/baseball/college-baseball/scoreboard?dates={YYYYMMDD}&groups=50&limit=900

Usage:
    python pitcher_scraper.py                    # Backfill full season
    python pitcher_scraper.py --days 7           # Last 7 days only
    python pitcher_scraper.py --analyze          # Show rotation analysis
    python pitcher_scraper.py --team "Vanderbilt" # Single team analysis
"""
import sqlite3, json, os, sys, io, time, math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_ET = ZoneInfo('America/New_York')
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from collections import defaultdict

# Fix Windows console encoding
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

ESPN_SCOREBOARD = 'https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/scoreboard'
ESPN_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/summary?event={}'
ESPN_MLB_SCOREBOARD = 'https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard'
ESPN_MLB_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event={}'
ESPN_NHL_SCOREBOARD = 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard'
ESPN_NHL_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event={}'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

SEASON_START = '2026-02-14'

# ═══════════════════════════════════════════════════════════════════
# DATABASE SETUP
# ═══════════════════════════════════════════════════════════════════

def ensure_tables(conn):
    """Create pitcher-related tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pitcher_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date TEXT NOT NULL,
            espn_event_id TEXT NOT NULL,
            team TEXT NOT NULL,
            pitcher_name TEXT NOT NULL,
            espn_athlete_id TEXT,
            is_starter INTEGER DEFAULT 0,
            innings_pitched REAL,
            hits INTEGER,
            runs INTEGER,
            earned_runs INTEGER,
            walks INTEGER,
            strikeouts INTEGER,
            home_runs INTEGER,
            pitch_count INTEGER,
            era REAL,
            fetched_at TEXT,
            UNIQUE(espn_event_id, team, pitcher_name)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ps_team
        ON pitcher_stats(team, game_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ps_starter
        ON pitcher_stats(team, is_starter, game_date)
    """)

    # Day-of-week pitching quality (derived from ALL games via results table)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS team_pitching_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team TEXT NOT NULL,
            day_of_week INTEGER NOT NULL,
            games_count INTEGER DEFAULT 0,
            avg_runs_allowed REAL,
            avg_total REAL,
            starter_era REAL,
            updated_at TEXT,
            UNIQUE(team, day_of_week)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tpq_team
        ON team_pitching_quality(team, day_of_week)
    """)
    conn.commit()


# ═══════════════════════════════════════════════════════════════════
# ESPN FETCHING
# ═══════════════════════════════════════════════════════════════════

def _fetch_json(url):
    """Fetch JSON from ESPN with retry."""
    for attempt in range(3):
        try:
            req = Request(url, headers=HEADERS)
            with urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except (URLError, HTTPError) as e:
            if attempt < 2:
                time.sleep(2)
            else:
                return None
        except Exception:
            return None


def _get_completed_game_ids(date_str):
    """Get ESPN event IDs for completed games on a date."""
    url = f"{ESPN_SCOREBOARD}?dates={date_str}&groups=50&limit=900"
    data = _fetch_json(url)
    if not data:
        return []

    games = []
    for event in data.get('events', []):
        status = event.get('status', {}).get('type', {}).get('completed', False)
        if not status:
            continue

        eid = event.get('id', '')
        comps = event.get('competitions', [{}])[0].get('competitors', [])
        home = away = ''
        for c in comps:
            name = c.get('team', {}).get('displayName', '')
            if c.get('homeAway') == 'home':
                home = name
            else:
                away = name

        date = event.get('date', '')[:10]
        games.append({'espn_id': eid, 'home': home, 'away': away, 'date': date})

    return games


def _parse_pitchers_from_summary(data, espn_event_id, game_date):
    """Extract pitcher stats from ESPN summary response."""
    rows = []
    boxscore = data.get('boxscore', {})

    for section in boxscore.get('players', []):
        team = section.get('team', {}).get('displayName', '')

        for stat_group in section.get('statistics', []):
            stat_names = stat_group.get('names', [])

            # Only pitching stats (identified by 'IP' column)
            if 'IP' not in stat_names:
                continue

            idx = {name: i for i, name in enumerate(stat_names)}

            for athlete_data in stat_group.get('athletes', []):
                athlete = athlete_data.get('athlete', {})
                name = athlete.get('displayName', '')
                aid = str(athlete.get('id', ''))
                is_starter = 1 if athlete_data.get('starter', False) else 0
                stats = athlete_data.get('stats', [])

                if not name or not stats or len(stats) < len(stat_names):
                    continue

                def _get(col, default=None):
                    if col in idx:
                        try:
                            return float(stats[idx[col]])
                        except (ValueError, TypeError):
                            return default
                    return default

                # Parse pitch count from "PC-ST" format (e.g., "54-33")
                pc = None
                if 'PC' in idx:
                    pc = _get('PC')
                elif 'PC-ST' in idx:
                    try:
                        pc = int(str(stats[idx['PC-ST']]).split('-')[0])
                    except (ValueError, IndexError):
                        pass

                rows.append({
                    'game_date': game_date,
                    'espn_event_id': espn_event_id,
                    'team': team,
                    'pitcher_name': name,
                    'espn_athlete_id': aid,
                    'is_starter': is_starter,
                    'innings_pitched': _get('IP'),
                    'hits': _get('H'),
                    'runs': _get('R'),
                    'earned_runs': _get('ER'),
                    'walks': _get('BB'),
                    'strikeouts': _get('K'),
                    'home_runs': _get('HR'),
                    'pitch_count': pc,
                    'era': _get('ERA'),
                })

    return rows


# ═══════════════════════════════════════════════════════════════════
# SCRAPING PIPELINE
# ═══════════════════════════════════════════════════════════════════

def scrape_pitcher_data(days_back=None, verbose=True):
    """
    Scrape pitcher data from ESPN game summaries.

    Returns count of new pitcher records inserted.
    """
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    # Determine date range
    if days_back:
        start = datetime.now() - timedelta(days=days_back)
    else:
        start = datetime.strptime(SEASON_START, '%Y-%m-%d')

    end = datetime.now() - timedelta(days=1)

    if verbose:
        total_days = (end - start).days + 1
        print(f"  ⚾ Scanning {total_days} days for pitcher data...")

    total_games = 0
    games_with_data = 0
    pitchers_inserted = 0

    current = start
    while current <= end:
        date_str = current.strftime('%Y%m%d')
        games = _get_completed_game_ids(date_str)
        total_games += len(games)

        for game in games:
            # Skip if we already have pitcher data for this game
            existing = conn.execute(
                "SELECT COUNT(*) FROM pitcher_stats WHERE espn_event_id=?",
                (game['espn_id'],)
            ).fetchone()[0]
            if existing > 0:
                continue

            # Fetch game summary
            summary = _fetch_json(ESPN_SUMMARY.format(game['espn_id']))
            if not summary:
                time.sleep(0.3)
                continue

            # Parse pitcher data
            pitchers = _parse_pitchers_from_summary(
                summary, game['espn_id'], game['date']
            )

            if pitchers:
                games_with_data += 1
                for p in pitchers:
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO pitcher_stats
                                (game_date, espn_event_id, team, pitcher_name,
                                 espn_athlete_id, is_starter, innings_pitched,
                                 hits, runs, earned_runs, walks, strikeouts,
                                 home_runs, pitch_count, era, fetched_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            p['game_date'], p['espn_event_id'], p['team'],
                            p['pitcher_name'], p['espn_athlete_id'], p['is_starter'],
                            p['innings_pitched'], p['hits'], p['runs'],
                            p['earned_runs'], p['walks'], p['strikeouts'],
                            p['home_runs'], p['pitch_count'], p['era'],
                            datetime.now().isoformat()
                        ))
                        pitchers_inserted += 1
                    except sqlite3.IntegrityError:
                        pass

                if verbose:
                    starters = [p for p in pitchers if p['is_starter']]
                    starter_names = ', '.join(p['pitcher_name'] for p in starters)
                    print(f"    ✅ {game['away']} @ {game['home']} ({game['date']}) — "
                          f"{len(pitchers)} pitchers, starters: {starter_names}")

            time.sleep(0.3)  # Be nice to ESPN

        current += timedelta(days=1)
        time.sleep(0.25)

    conn.commit()

    if verbose:
        total_in_db = conn.execute("SELECT COUNT(*) FROM pitcher_stats").fetchone()[0]
        starters_in_db = conn.execute(
            "SELECT COUNT(*) FROM pitcher_stats WHERE is_starter=1"
        ).fetchone()[0]
        print(f"\n  ⚾ Pitcher scrape complete:")
        print(f"    Games scanned: {total_games}")
        print(f"    Games with pitcher data: {games_with_data}")
        print(f"    New pitcher records: {pitchers_inserted}")
        print(f"    Total in DB: {total_in_db} ({starters_in_db} starters)")

    conn.close()
    return pitchers_inserted


# ═══════════════════════════════════════════════════════════════════
# DAY-OF-WEEK PITCHING QUALITY (from results — works for ALL games)
# ═══════════════════════════════════════════════════════════════════

def build_pitching_quality(verbose=True):
    """
    Build team pitching quality by day-of-week from results table.

    This works for ALL games (not just the ~1% with ESPN box data)
    because it uses runs allowed from the results table.

    College baseball rotation pattern:
      0=Mon (midweek), 1=Tue (midweek), 2=Wed (midweek),
      3=Thu (midweek), 4=Fri (ace), 5=Sat (#2), 6=Sun (#3)
    """
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    # Get all baseball results
    rows = conn.execute("""
        SELECT home, away, home_score, away_score, commence_time
        FROM results
        WHERE sport='baseball_ncaa' AND completed=1
        AND home_score IS NOT NULL AND away_score IS NOT NULL
    """).fetchall()

    if not rows:
        if verbose:
            print("  ⚠ No baseball results to analyze")
        conn.close()
        return

    # Aggregate runs allowed by team + day of week
    # team -> dow -> [runs_allowed_list]
    team_dow = defaultdict(lambda: defaultdict(list))
    # Also track totals
    team_dow_total = defaultdict(lambda: defaultdict(list))

    for home, away, h_score, a_score, commence in rows:
        try:
            dt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
            dow = dt.astimezone(_ET).weekday()  # 0=Mon through 6=Sun (local ET)
        except (ValueError, AttributeError):
            continue

        total = h_score + a_score
        # Home team allowed a_score runs, away team allowed h_score runs
        team_dow[home][dow].append(a_score)
        team_dow[away][dow].append(h_score)
        team_dow_total[home][dow].append(total)
        team_dow_total[away][dow].append(total)

    # Get starter ERA by team + day if we have ESPN pitcher data
    starter_eras = defaultdict(lambda: defaultdict(list))
    starter_rows = conn.execute("""
        SELECT ps.team, ps.game_date, ps.era, ps.earned_runs, ps.innings_pitched
        FROM pitcher_stats ps
        WHERE ps.is_starter=1 AND ps.innings_pitched IS NOT NULL
    """).fetchall()

    for team, game_date, era, er, ip in starter_rows:
        try:
            dt = datetime.strptime(game_date, '%Y-%m-%d')
            dow = dt.weekday()
            if era is not None:
                starter_eras[team][dow].append(era)
        except (ValueError, AttributeError):
            continue

    # Write to DB
    inserted = 0
    for team, dows in team_dow.items():
        for dow, runs_list in dows.items():
            if len(runs_list) < 2:
                continue

            avg_ra = sum(runs_list) / len(runs_list)
            totals = team_dow_total[team][dow]
            avg_total = sum(totals) / len(totals) if totals else None

            # Starter ERA from ESPN data (may be None for most teams)
            s_eras = starter_eras.get(team, {}).get(dow, [])
            starter_era = sum(s_eras) / len(s_eras) if s_eras else None

            conn.execute("""
                INSERT OR REPLACE INTO team_pitching_quality
                    (team, day_of_week, games_count, avg_runs_allowed,
                     avg_total, starter_era, updated_at)
                VALUES (?,?,?,?,?,?,?)
            """, (team, dow, len(runs_list), round(avg_ra, 2),
                  round(avg_total, 2) if avg_total else None,
                  round(starter_era, 2) if starter_era else None,
                  datetime.now().isoformat()))
            inserted += 1

    conn.commit()

    if verbose:
        teams_tracked = len(team_dow)
        print(f"  ⚾ Pitching quality built: {teams_tracked} teams, {inserted} team-day records")
        print(f"    ESPN starter data: {len(starter_eras)} teams with named starters")

    conn.close()
    return inserted


# ═══════════════════════════════════════════════════════════════════
# MLB PROBABLE PITCHERS (from ESPN scoreboard API)
# ═══════════════════════════════════════════════════════════════════

def _ensure_mlb_probables_table(conn):
    """Create MLB probable pitchers table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mlb_probable_pitchers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date TEXT NOT NULL,
            espn_event_id TEXT NOT NULL,
            home TEXT NOT NULL,
            away TEXT NOT NULL,
            home_pitcher TEXT,
            away_pitcher TEXT,
            home_pitcher_id TEXT,
            away_pitcher_id TEXT,
            home_pitcher_record TEXT,
            away_pitcher_record TEXT,
            home_pitcher_era REAL,
            away_pitcher_era REAL,
            home_pitcher_season_era REAL,
            away_pitcher_season_era REAL,
            home_pitcher_season_k9 REAL,
            away_pitcher_season_k9 REAL,
            home_pitcher_season_whip REAL,
            away_pitcher_season_whip REAL,
            home_pitcher_season_ip REAL,
            away_pitcher_season_ip REAL,
            fetched_at TEXT,
            UNIQUE(espn_event_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_mlb_pp_date
        ON mlb_probable_pitchers(game_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_mlb_pp_teams
        ON mlb_probable_pitchers(home, away, game_date)
    """)
    conn.commit()


def _calc_pitcher_season_stats(conn, pitcher_name, team):
    """
    Calculate a pitcher's season stats from pitcher_stats table.

    Returns dict with era, k9, whip, ip or None if no data.
    Primary source: pitcher_stats table (populated from ESPN box scores).
    """
    # Try pitcher-specific data first (most accurate)
    ps_rows = conn.execute("""
        SELECT innings_pitched, earned_runs, strikeouts, hits, walks
        FROM pitcher_stats
        WHERE pitcher_name=? AND is_starter=1
        AND innings_pitched IS NOT NULL AND innings_pitched > 0
        ORDER BY game_date DESC LIMIT 15
    """, (pitcher_name,)).fetchall()

    if ps_rows and len(ps_rows) >= 2:
        total_ip = sum(r[0] for r in ps_rows if r[0])
        total_er = sum(r[1] for r in ps_rows if r[1] is not None)
        total_k = sum(r[2] for r in ps_rows if r[2] is not None)
        total_h = sum(r[3] for r in ps_rows if r[3] is not None)
        total_bb = sum(r[4] for r in ps_rows if r[4] is not None)
        if total_ip > 0:
            return {
                'era': round(total_er * 9.0 / total_ip, 2),
                'k9': round(total_k * 9.0 / total_ip, 2),
                'whip': round((total_h + total_bb) / total_ip, 2),
                'ip': round(total_ip, 1),
            }

    # Fallback: any starter data for this team
    team_rows = conn.execute("""
        SELECT innings_pitched, earned_runs, strikeouts, hits, walks
        FROM pitcher_stats
        WHERE team=? AND is_starter=1
        AND innings_pitched IS NOT NULL AND innings_pitched > 0
        ORDER BY game_date DESC LIMIT 20
    """, (team,)).fetchall()

    if team_rows and len(team_rows) >= 2:
        total_ip = sum(r[0] for r in team_rows if r[0])
        total_er = sum(r[1] for r in team_rows if r[1] is not None)
        total_k = sum(r[2] for r in team_rows if r[2] is not None)
        total_h = sum(r[3] for r in team_rows if r[3] is not None)
        total_bb = sum(r[4] for r in team_rows if r[4] is not None)
        if total_ip > 0:
            return {
                'era': round(total_er * 9.0 / total_ip, 2),
                'k9': round(total_k * 9.0 / total_ip, 2),
                'whip': round((total_h + total_bb) / total_ip, 2),
                'ip': round(total_ip, 1),
            }

    return None


def scrape_mlb_pitchers(conn=None, verbose=True):
    """
    Fetch today's MLB probable pitchers from ESPN scoreboard API.

    ESPN provides probable starting pitchers in the scoreboard response
    at: events[].competitions[].competitors[].probables[]

    Returns count of games with pitcher data stored.
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close_conn = True

    _ensure_mlb_probables_table(conn)

    today = datetime.now(_ET).strftime('%Y%m%d')
    today_iso = datetime.now(_ET).strftime('%Y-%m-%d')

    url = f"{ESPN_MLB_SCOREBOARD}?dates={today}"
    data = _fetch_json(url)
    if not data:
        if verbose:
            print("  \u26a0 MLB scoreboard fetch failed")
        if close_conn:
            conn.close()
        return 0

    events = data.get('events', [])
    if verbose:
        print(f"  \u26be MLB: {len(events)} games on scoreboard")

    games_stored = 0
    games_no_pitcher = 0

    for event in events:
        eid = event.get('id', '')
        comps = event.get('competitions', [{}])[0].get('competitors', [])

        home_team = away_team = ''
        home_pitcher = away_pitcher = None
        home_pitcher_id = away_pitcher_id = ''
        home_record = away_record = ''
        home_era = away_era = None

        for c in comps:
            team_name = c.get('team', {}).get('displayName', '')
            is_home = c.get('homeAway') == 'home'

            # Extract probable pitcher
            probables = c.get('probables', [])
            pitcher_name = None
            pitcher_id = ''
            pitcher_record = ''
            pitcher_era = None

            for prob in probables:
                if prob.get('abbreviation') == 'SP' or prob.get('name') == 'probableStartingPitcher':
                    athlete = prob.get('athlete', {})
                    pitcher_name = athlete.get('fullName') or athlete.get('displayName')
                    pitcher_id = str(athlete.get('id', ''))
                    pitcher_record = prob.get('record', '')

                    # Extract ERA from statistics array
                    for stat in prob.get('statistics', []):
                        if stat.get('abbreviation') == 'ERA' or stat.get('name') == 'ERA':
                            try:
                                pitcher_era = float(stat.get('displayValue', 0))
                            except (ValueError, TypeError):
                                pass
                    break

            if is_home:
                home_team = team_name
                home_pitcher = pitcher_name
                home_pitcher_id = pitcher_id
                home_record = pitcher_record
                home_era = pitcher_era
            else:
                away_team = team_name
                away_pitcher = pitcher_name
                away_pitcher_id = pitcher_id
                away_record = pitcher_record
                away_era = pitcher_era

        # Check if we have pitchers for both teams
        if not home_pitcher or not away_pitcher:
            games_no_pitcher += 1
            if verbose:
                missing = []
                if not home_pitcher:
                    missing.append(home_team or 'HOME')
                if not away_pitcher:
                    missing.append(away_team or 'AWAY')
                print(f"    \u26a0 {away_team} @ {home_team}: TBD pitcher ({', '.join(missing)})")
            continue

        # Look up season stats from box_scores/pitcher_stats
        home_stats = _calc_pitcher_season_stats(conn, home_pitcher, home_team)
        away_stats = _calc_pitcher_season_stats(conn, away_pitcher, away_team)

        # Store
        try:
            conn.execute("""
                INSERT OR REPLACE INTO mlb_probable_pitchers
                    (game_date, espn_event_id, home, away,
                     home_pitcher, away_pitcher,
                     home_pitcher_id, away_pitcher_id,
                     home_pitcher_record, away_pitcher_record,
                     home_pitcher_era, away_pitcher_era,
                     home_pitcher_season_era, away_pitcher_season_era,
                     home_pitcher_season_k9, away_pitcher_season_k9,
                     home_pitcher_season_whip, away_pitcher_season_whip,
                     home_pitcher_season_ip, away_pitcher_season_ip,
                     fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                today_iso, eid, home_team, away_team,
                home_pitcher, away_pitcher,
                home_pitcher_id, away_pitcher_id,
                home_record, away_record,
                home_era, away_era,
                home_stats['era'] if home_stats else None,
                away_stats['era'] if away_stats else None,
                home_stats['k9'] if home_stats else None,
                away_stats['k9'] if away_stats else None,
                home_stats['whip'] if home_stats else None,
                away_stats['whip'] if away_stats else None,
                home_stats['ip'] if home_stats else None,
                away_stats['ip'] if away_stats else None,
                datetime.now().isoformat(),
            ))
            games_stored += 1

            if verbose:
                h_era_str = f" ({home_era:.2f} ERA)" if home_era else ""
                a_era_str = f" ({away_era:.2f} ERA)" if away_era else ""
                print(f"    \u2705 {away_team} @ {home_team}: "
                      f"{away_pitcher}{a_era_str} vs {home_pitcher}{h_era_str}")
        except sqlite3.Error as e:
            if verbose:
                print(f"    \u274c DB error for {away_team} @ {home_team}: {e}")

    conn.commit()

    if verbose:
        print(f"\n  \u26be MLB pitchers: {games_stored} games with both starters, "
              f"{games_no_pitcher} games with TBD")

    if close_conn:
        conn.close()

    return games_stored


def get_mlb_probable_starters(conn, home, away, game_date=None):
    """
    Get MLB probable starters for a game.

    Returns dict with:
        - home_pitcher: name or None
        - away_pitcher: name or None
        - home_era: ESPN ERA or None
        - away_era: ESPN ERA or None
        - both_confirmed: True if both starters are known
        - summary: human-readable string

    Used by model_engine to gate MLB picks.
    """
    if game_date is None:
        game_date = datetime.now(_ET).strftime('%Y-%m-%d')

    # Check if table exists
    try:
        conn.execute("SELECT 1 FROM mlb_probable_pitchers LIMIT 1")
    except sqlite3.OperationalError:
        return {'home_pitcher': None, 'away_pitcher': None,
                'home_era': None, 'away_era': None,
                'both_confirmed': False, 'summary': 'No MLB pitcher data table'}

    row = conn.execute("""
        SELECT home_pitcher, away_pitcher, home_pitcher_era, away_pitcher_era,
               home_pitcher_season_era, away_pitcher_season_era,
               home_pitcher_season_k9, away_pitcher_season_k9,
               home_pitcher_season_whip, away_pitcher_season_whip
        FROM mlb_probable_pitchers
        WHERE home=? AND away=? AND game_date=?
        ORDER BY fetched_at DESC LIMIT 1
    """, (home, away, game_date)).fetchone()

    if not row:
        # Try fuzzy match (team names may differ slightly between APIs)
        row = conn.execute("""
            SELECT home_pitcher, away_pitcher, home_pitcher_era, away_pitcher_era,
                   home_pitcher_season_era, away_pitcher_season_era,
                   home_pitcher_season_k9, away_pitcher_season_k9,
                   home_pitcher_season_whip, away_pitcher_season_whip
            FROM mlb_probable_pitchers
            WHERE game_date=?
            AND (home LIKE ? OR home LIKE ?)
            AND (away LIKE ? OR away LIKE ?)
            ORDER BY fetched_at DESC LIMIT 1
        """, (game_date,
              f"%{home.split()[-1]}%", f"%{home}%",
              f"%{away.split()[-1]}%", f"%{away}%")).fetchone()

    if not row:
        return {'home_pitcher': None, 'away_pitcher': None,
                'home_era': None, 'away_era': None,
                'both_confirmed': False,
                'summary': f'No pitcher data for {away} @ {home}'}

    hp, ap = row[0], row[1]
    h_era = row[2] or row[4]  # ESPN ERA, fallback to season ERA
    a_era = row[3] or row[5]
    both = hp is not None and ap is not None

    parts = []
    if hp:
        era_s = f" ({h_era:.2f})" if h_era else ""
        parts.append(f"{home} SP: {hp}{era_s}")
    if ap:
        era_s = f" ({a_era:.2f})" if a_era else ""
        parts.append(f"{away} SP: {ap}{era_s}")

    return {
        'home_pitcher': hp,
        'away_pitcher': ap,
        'home_era': h_era,
        'away_era': a_era,
        'home_k9': row[6],
        'away_k9': row[7],
        'home_whip': row[8],
        'away_whip': row[9],
        'both_confirmed': both,
        'summary': ' | '.join(parts) if parts else 'No pitcher data',
    }


# ═══════════════════════════════════════════════════════════════════
# MLB HISTORICAL PITCHER SCRAPING (from completed game box scores)
# ═══════════════════════════════════════════════════════════════════

def scrape_mlb_pitcher_history(days_back=None, verbose=True):
    """
    Scrape MLB pitcher data from ESPN completed game summaries.
    Stores into pitcher_stats table (same as college baseball).
    Returns count of new pitcher records inserted.
    """
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    MLB_SEASON_START = '2026-03-26'  # Opening Day 2026

    if days_back:
        start = datetime.now() - timedelta(days=days_back)
    else:
        start = datetime.strptime(MLB_SEASON_START, '%Y-%m-%d')

    end = datetime.now() - timedelta(days=1)

    if verbose:
        total_days = (end - start).days + 1
        print(f"  \u26be MLB: Scanning {total_days} days for pitcher history...")

    total_games = 0
    games_with_data = 0
    pitchers_inserted = 0

    current = start
    while current <= end:
        date_str = current.strftime('%Y%m%d')

        # Fetch MLB scoreboard for completed games
        url = f"{ESPN_MLB_SCOREBOARD}?dates={date_str}"
        data = _fetch_json(url)
        if not data:
            current += timedelta(days=1)
            continue

        for event in data.get('events', []):
            status = event.get('status', {}).get('type', {}).get('completed', False)
            if not status:
                continue

            eid = event.get('id', '')
            total_games += 1

            # Skip if we already have pitcher data for this game
            existing = conn.execute(
                "SELECT COUNT(*) FROM pitcher_stats WHERE espn_event_id=?",
                (eid,)
            ).fetchone()[0]
            if existing > 0:
                continue

            # Fetch game summary for box score
            summary = _fetch_json(ESPN_MLB_SUMMARY.format(eid))
            if not summary:
                time.sleep(0.3)
                continue

            game_date = event.get('date', '')[:10]
            pitchers = _parse_pitchers_from_summary(summary, eid, game_date)

            if pitchers:
                games_with_data += 1
                for p in pitchers:
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO pitcher_stats
                                (game_date, espn_event_id, team, pitcher_name,
                                 espn_athlete_id, is_starter, innings_pitched,
                                 hits, runs, earned_runs, walks, strikeouts,
                                 home_runs, pitch_count, era, fetched_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            p['game_date'], p['espn_event_id'], p['team'],
                            p['pitcher_name'], p['espn_athlete_id'], p['is_starter'],
                            p['innings_pitched'], p['hits'], p['runs'],
                            p['earned_runs'], p['walks'], p['strikeouts'],
                            p['home_runs'], p['pitch_count'], p['era'],
                            datetime.now().isoformat()
                        ))
                        pitchers_inserted += 1
                    except sqlite3.IntegrityError:
                        pass

                if verbose:
                    starters = [p for p in pitchers if p['is_starter']]
                    starter_names = ', '.join(p['pitcher_name'] for p in starters)
                    comps = event.get('competitions', [{}])[0].get('competitors', [])
                    home = away = ''
                    for c in comps:
                        name = c.get('team', {}).get('displayName', '')
                        if c.get('homeAway') == 'home':
                            home = name
                        else:
                            away = name
                    print(f"    \u2705 {away} @ {home} ({game_date}) \u2014 "
                          f"{len(pitchers)} pitchers, starters: {starter_names}")

            time.sleep(0.3)

        current += timedelta(days=1)
        time.sleep(0.25)

    conn.commit()

    if verbose:
        print(f"\n  \u26be MLB pitcher history: {total_games} games scanned, "
              f"{games_with_data} with data, {pitchers_inserted} new records")

    conn.close()
    return pitchers_inserted


# ═══════════════════════════════════════════════════════════════════
# MODEL INTEGRATION — get_pitcher_context()
# ═══════════════════════════════════════════════════════════════════

# League-wide averages (from results data)
LEAGUE_AVG_RUNS_ALLOWED = 6.5  # Will be computed dynamically

def _get_league_avg(conn):
    """Get league-wide average runs per team per game."""
    row = conn.execute("""
        SELECT AVG(home_score + away_score) / 2.0
        FROM results
        WHERE sport='baseball_ncaa' AND completed=1
        AND home_score IS NOT NULL
    """).fetchone()
    return row[0] if row and row[0] else LEAGUE_AVG_RUNS_ALLOWED


def get_pitcher_context(conn, home, away, commence_time=None, sport='baseball_ncaa'):
    """
    Get pitcher-quality context for a baseball game.

    Returns dict with:
        - home_pitching_adj: runs above/below average (negative = better pitching)
        - away_pitching_adj: runs above/below average
        - spread_adj: net spread adjustment (positive = home advantage)
        - total_adj: total adjustment (positive = expect more runs)
        - confidence: 'HIGH', 'MEDIUM', 'LOW' based on data quality
        - summary: human-readable description
        - home_starter: named starter if known from ESPN data
        - away_starter: named starter if known

    Day-of-week matters hugely in college baseball:
        Friday games: aces pitch → lower scoring, tighter games
        Saturday: #2 starters → slightly higher scoring
        Sunday: #3 starters → highest scoring of weekend
        Midweek (Tue/Wed): bullpen days or #4 → unpredictable
    """
    result = {
        'home_pitching_adj': 0.0,
        'away_pitching_adj': 0.0,
        'spread_adj': 0.0,
        'total_adj': 0.0,
        'confidence': 'LOW',
        'summary': '',
        'home_starter': None,
        'away_starter': None,
        'home_starter_era': None,
        'away_starter_era': None,
        'day_type': 'unknown',
    }

    # Determine day of week
    dow = None
    if commence_time:
        try:
            dt = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
            dow = dt.astimezone(_ET).weekday()  # Convert UTC to ET before weekday
        except (ValueError, AttributeError):
            pass

    if dow is None:
        dow = datetime.now().weekday()

    # Classify game day
    if dow == 4:
        day_type = 'friday'  # Ace day
    elif dow == 5:
        day_type = 'saturday'  # #2 starter
    elif dow == 6:
        day_type = 'sunday'  # #3 starter
    else:
        day_type = 'midweek'  # #4/bullpen
    result['day_type'] = day_type

    league_avg = _get_league_avg(conn)

    # Get day-of-week pitching quality for both teams
    home_q = conn.execute("""
        SELECT avg_runs_allowed, games_count, starter_era, avg_total
        FROM team_pitching_quality
        WHERE team=? AND day_of_week=?
    """, (home, dow)).fetchone()

    away_q = conn.execute("""
        SELECT avg_runs_allowed, games_count, starter_era, avg_total
        FROM team_pitching_quality
        WHERE team=? AND day_of_week=?
    """, (away, dow)).fetchone()

    # Get overall team pitching quality as fallback
    home_overall = conn.execute("""
        SELECT AVG(avg_runs_allowed), SUM(games_count)
        FROM team_pitching_quality WHERE team=?
    """, (home,)).fetchone()

    away_overall = conn.execute("""
        SELECT AVG(avg_runs_allowed), SUM(games_count)
        FROM team_pitching_quality WHERE team=?
    """, (away,)).fetchone()

    # Check for named starters from ESPN data (most recent on this day of week)
    home_starter_row = conn.execute("""
        SELECT pitcher_name, era, innings_pitched, earned_runs
        FROM pitcher_stats
        WHERE team=? AND is_starter=1
        AND CAST(strftime('%w', game_date) AS INTEGER) = ?
        ORDER BY game_date DESC LIMIT 1
    """, (home, (dow + 1) % 7)).fetchone()  # SQLite %w: 0=Sun, Python weekday: 0=Mon

    away_starter_row = conn.execute("""
        SELECT pitcher_name, era, innings_pitched, earned_runs
        FROM pitcher_stats
        WHERE team=? AND is_starter=1
        AND CAST(strftime('%w', game_date) AS INTEGER) = ?
        ORDER BY game_date DESC LIMIT 1
    """, (away, (dow + 1) % 7)).fetchone()

    if home_starter_row:
        result['home_starter'] = home_starter_row[0]
        result['home_starter_era'] = home_starter_row[1]

    if away_starter_row:
        result['away_starter'] = away_starter_row[0]
        result['away_starter_era'] = away_starter_row[1]

    # Calculate pitching adjustments
    # Use day-specific data if available (3+ games), else overall
    home_ra = None
    away_ra = None
    data_quality = 0

    if home_q and home_q[1] >= 3:
        home_ra = home_q[0]
        data_quality += 2
    elif home_overall and home_overall[1] and home_overall[1] >= 5:
        home_ra = home_overall[0]
        data_quality += 1

    if away_q and away_q[1] >= 3:
        away_ra = away_q[0]
        data_quality += 2
    elif away_overall and away_overall[1] and away_overall[1] >= 5:
        away_ra = away_overall[0]
        data_quality += 1

    if home_ra is not None:
        result['home_pitching_adj'] = round(home_ra - league_avg, 2)
    if away_ra is not None:
        result['away_pitching_adj'] = round(away_ra - league_avg, 2)

    # Spread adjustment: better away pitching (fewer runs allowed) helps away team
    # If home allows fewer runs → home advantage → positive spread_adj
    # If away allows fewer runs → away advantage → negative spread_adj
    if home_ra is not None and away_ra is not None:
        # Pitching differential: negative means home has better pitching
        diff = away_ra - home_ra  # positive = home team allows fewer runs
        # Scale: 1 run difference in runs allowed ≈ 0.3 spread points
        # Capped at ±1.0 to avoid overweighting
        result['spread_adj'] = round(max(-1.0, min(1.0, diff * 0.3)), 2)

    # Total adjustment: both teams' pitching quality affects total
    if home_ra is not None and away_ra is not None:
        combined_adj = (home_ra - league_avg) + (away_ra - league_avg)
        # Scale: 1 combined run above average ≈ 0.5 total points
        # Capped at ±2.0
        result['total_adj'] = round(max(-2.0, min(2.0, combined_adj * 0.5)), 2)

    # Day-of-week baseline adjustments (college baseball only)
    # Friday aces suppress scoring, Sunday #3 starters allow more
    # MLB Friday scoring is actually HIGHER than average — no DOW adj for MLB
    if sport == 'baseball_mlb':
        dow_adj = 0.0
    else:
        DOW_TOTAL_ADJ = {
            'friday': -0.3,    # Aces → lower scoring (reduced from -0.5; market already prices this)
            'saturday': 0.0,   # #2 starter → average
            'sunday': 0.5,     # #3 starter → higher scoring
            'midweek': 0.3,    # Bullpen day → slightly higher
        }
        dow_adj = DOW_TOTAL_ADJ.get(day_type, 0.0)
    result['total_adj'] = round(result['total_adj'] + dow_adj, 2)

    # Confidence level
    if data_quality >= 4:
        result['confidence'] = 'HIGH'
    elif data_quality >= 2:
        result['confidence'] = 'MEDIUM'
    else:
        result['confidence'] = 'LOW'

    # Build summary
    parts = [f"{day_type.title()} game"]
    if result['home_starter']:
        era_str = f" ({result['home_starter_era']:.2f} ERA)" if result['home_starter_era'] else ""
        parts.append(f"{home} SP: {result['home_starter']}{era_str}")
    if result['away_starter']:
        era_str = f" ({result['away_starter_era']:.2f} ERA)" if result['away_starter_era'] else ""
        parts.append(f"{away} SP: {result['away_starter']}{era_str}")
    if home_ra is not None:
        parts.append(f"{home} allows {home_ra:.1f} R/{day_type}")
    if away_ra is not None:
        parts.append(f"{away} allows {away_ra:.1f} R/{day_type}")
    if result['spread_adj'] != 0:
        side = home if result['spread_adj'] > 0 else away
        parts.append(f"Pitching edge: {side} ({result['spread_adj']:+.1f} pts)")

    result['summary'] = ' | '.join(parts)

    return result


# ═══════════════════════════════════════════════════════════════════
# ANALYSIS / REPORTING
# ═══════════════════════════════════════════════════════════════════

def analyze_rotations(team_filter=None, verbose=True):
    """Show rotation analysis for teams with ESPN pitcher data."""
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    query = """
        SELECT team, pitcher_name, game_date, is_starter,
               innings_pitched, earned_runs, era, strikeouts
        FROM pitcher_stats
        WHERE is_starter=1
    """
    params = []
    if team_filter:
        query += " AND team LIKE ?"
        params.append(f"%{team_filter}%")
    query += " ORDER BY team, game_date"

    rows = conn.execute(query, params).fetchall()

    if not rows:
        print("  No starter data found in ESPN box scores.")
        print("  (ESPN provides box data for ~1-2% of college baseball games)")
        conn.close()
        return

    # Group by team
    teams = defaultdict(list)
    for team, pitcher, date, starter, ip, er, era, k in rows:
        try:
            dt = datetime.strptime(date, '%Y-%m-%d')
            dow_name = dt.strftime('%A')
        except ValueError:
            dow_name = '?'
        teams[team].append({
            'pitcher': pitcher, 'date': date, 'day': dow_name,
            'ip': ip, 'er': er, 'era': era, 'k': k
        })

    print(f"\n{'='*60}")
    print(f"  PITCHER ROTATION ANALYSIS ({len(teams)} teams with ESPN data)")
    print(f"{'='*60}")

    for team, starts in sorted(teams.items()):
        print(f"\n  {team}:")
        for s in starts:
            era_str = f"ERA {s['era']:.2f}" if s['era'] else "ERA ?"
            print(f"    {s['date']} ({s['day'][:3]}): {s['pitcher']} — "
                  f"{s['ip']:.1f} IP, {s['er'] or 0} ER, {s['k'] or 0} K, {era_str}")

    # Day-of-week summary
    print(f"\n{'='*60}")
    print(f"  DAY-OF-WEEK PITCHING QUALITY (all teams, from results)")
    print(f"{'='*60}")

    dow_stats = conn.execute("""
        SELECT day_of_week, COUNT(*) as teams, AVG(avg_runs_allowed) as avg_ra,
               AVG(avg_total) as avg_total
        FROM team_pitching_quality
        WHERE games_count >= 3
        GROUP BY day_of_week
        ORDER BY day_of_week
    """).fetchall()

    DOW_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    for dow, team_count, avg_ra, avg_total in dow_stats:
        if dow < 7:
            role = {'4': 'ACE', '5': '#2', '6': '#3'}.get(str(dow), 'MID')
            print(f"    {DOW_NAMES[dow]} ({role}): {avg_ra:.1f} runs allowed avg "
                  f"({avg_total:.1f} total), {team_count} teams")

    conn.close()


# ═══════════════════════════════════════════════════════════════════
# NHL GOALIE SCRAPING — Probable starters + historical stats
# ═══════════════════════════════════════════════════════════════════

def _ensure_nhl_goalies_table(conn):
    """Create NHL probable goalies table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nhl_probable_goalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date TEXT NOT NULL,
            espn_event_id TEXT NOT NULL,
            home TEXT NOT NULL,
            away TEXT NOT NULL,
            home_goalie TEXT,
            away_goalie TEXT,
            home_goalie_id TEXT,
            away_goalie_id TEXT,
            home_goalie_status TEXT,
            away_goalie_status TEXT,
            fetched_at TEXT,
            UNIQUE(espn_event_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_nhl_pg_date
        ON nhl_probable_goalies(game_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_nhl_pg_teams
        ON nhl_probable_goalies(home, away, game_date)
    """)
    # Goalie game stats — scraped from ESPN game summaries
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nhl_goalie_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date TEXT NOT NULL,
            espn_event_id TEXT NOT NULL,
            team TEXT NOT NULL,
            goalie_name TEXT NOT NULL,
            espn_athlete_id TEXT,
            goals_against INTEGER,
            shots_against INTEGER,
            saves INTEGER,
            save_pct REAL,
            time_on_ice TEXT,
            is_starter INTEGER DEFAULT 0,
            fetched_at TEXT,
            UNIQUE(espn_event_id, team, goalie_name)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_nhl_gs_goalie
        ON nhl_goalie_stats(goalie_name, game_date)
    """)
    conn.commit()


def scrape_nhl_goalies(conn=None, verbose=True):
    """
    Fetch today's NHL probable starting goalies from ESPN scoreboard API.

    ESPN provides probable starting goalies in the scoreboard response
    at: events[].competitions[].competitors[].probables[]
    with name='probableStartingGoalie' and status (Confirmed/Expected/Probable).

    Returns count of games with goalie data stored.
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close_conn = True

    _ensure_nhl_goalies_table(conn)

    today = datetime.now(_ET).strftime('%Y%m%d')
    today_iso = datetime.now(_ET).strftime('%Y-%m-%d')

    url = f"{ESPN_NHL_SCOREBOARD}?dates={today}"
    data = _fetch_json(url)
    if not data:
        if verbose:
            print("  \u26a0 NHL scoreboard fetch failed")
        if close_conn:
            conn.close()
        return 0

    events = data.get('events', [])
    if verbose:
        print(f"  \U0001f3d2 NHL: {len(events)} games on scoreboard")

    games_stored = 0
    games_no_goalie = 0

    for event in events:
        eid = event.get('id', '')
        comps = event.get('competitions', [{}])[0].get('competitors', [])

        home_team = away_team = ''
        home_goalie = away_goalie = None
        home_goalie_id = away_goalie_id = ''
        home_status = away_status = ''

        for c in comps:
            team_name = c.get('team', {}).get('displayName', '')
            is_home = c.get('homeAway') == 'home'

            # Extract probable starting goalie
            probables = c.get('probables', [])
            goalie_name = None
            goalie_id = ''
            goalie_status = ''

            for prob in probables:
                if prob.get('name') == 'probableStartingGoalie':
                    athlete = prob.get('athlete', {})
                    goalie_name = athlete.get('fullName') or athlete.get('displayName')
                    goalie_id = str(athlete.get('id', ''))
                    goalie_status = prob.get('status', {}).get('name', 'Unknown')
                    break

            if is_home:
                home_team = team_name
                home_goalie = goalie_name
                home_goalie_id = goalie_id
                home_status = goalie_status
            else:
                away_team = team_name
                away_goalie = goalie_name
                away_goalie_id = goalie_id
                away_status = goalie_status

        if not home_goalie or not away_goalie:
            games_no_goalie += 1
            if verbose:
                missing = []
                if not home_goalie:
                    missing.append(home_team or 'HOME')
                if not away_goalie:
                    missing.append(away_team or 'AWAY')
                print(f"    \u26a0 {away_team} @ {home_team}: TBD goalie ({', '.join(missing)})")
            continue

        # Store
        try:
            conn.execute("""
                INSERT OR REPLACE INTO nhl_probable_goalies
                    (game_date, espn_event_id, home, away,
                     home_goalie, away_goalie,
                     home_goalie_id, away_goalie_id,
                     home_goalie_status, away_goalie_status,
                     fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                today_iso, eid, home_team, away_team,
                home_goalie, away_goalie,
                home_goalie_id, away_goalie_id,
                home_status, away_status,
                datetime.now(_ET).isoformat()
            ))
            conn.commit()
            games_stored += 1
            if verbose:
                print(f"    {away_team} @ {home_team}: "
                      f"{away_goalie} ({away_status}) vs {home_goalie} ({home_status})")
        except Exception as e:
            if verbose:
                print(f"    \u26a0 DB error for {away_team} @ {home_team}: {e}")

    if verbose:
        print(f"  NHL goalies: {games_stored} games stored, "
              f"{games_no_goalie} missing goalie data")

    if close_conn:
        conn.close()
    return games_stored


def scrape_nhl_goalie_history(days_back=30, verbose=True):
    """
    Backfill NHL goalie stats from completed game summaries.

    Fetches ESPN game summaries for recent NHL games and extracts
    goalie box score data (GA, SA, SV, SV%, TOI) for each goalie.
    The starter is the goalie with the most TOI in each game.
    """
    conn = sqlite3.connect(DB_PATH)
    _ensure_nhl_goalies_table(conn)

    end_date = datetime.now(_ET)
    start_date = end_date - timedelta(days=days_back)
    games_scraped = 0
    goalies_stored = 0

    if verbose:
        print(f"  Scanning NHL games from {start_date.strftime('%Y-%m-%d')} "
              f"to {end_date.strftime('%Y-%m-%d')}...")

    current = start_date
    while current <= end_date:
        date_str = current.strftime('%Y%m%d')
        url = f"{ESPN_NHL_SCOREBOARD}?dates={date_str}"
        data = _fetch_json(url)
        if not data:
            current += timedelta(days=1)
            continue

        for event in data.get('events', []):
            eid = event.get('id', '')
            status = event.get('competitions', [{}])[0].get('status', {}).get('type', {}).get('state', '')
            if status != 'post':
                continue  # Only completed games

            # Check if we already have stats for this game
            existing = conn.execute(
                "SELECT COUNT(*) FROM nhl_goalie_stats WHERE espn_event_id=?",
                (eid,)).fetchone()[0]
            if existing > 0:
                continue

            # Fetch game summary for goalie box scores
            summary_url = ESPN_NHL_SUMMARY.format(eid)
            summary = _fetch_json(summary_url)
            if not summary:
                continue

            bs = summary.get('boxscore', {})
            for team_block in bs.get('players', []):
                team_name = team_block.get('team', {}).get('displayName', '')
                for stat_group in team_block.get('statistics', []):
                    if stat_group.get('name', '').lower() != 'goalies':
                        continue

                    labels = stat_group.get('labels', [])
                    # Map label positions
                    label_idx = {l: i for i, l in enumerate(labels)}

                    max_toi_mins = 0
                    starter_name = None
                    athletes_data = []

                    for athlete in stat_group.get('athletes', []):
                        a_info = athlete.get('athlete', {})
                        g_name = a_info.get('displayName', '')
                        g_id = str(a_info.get('id', ''))
                        stats = athlete.get('stats', [])

                        ga = int(stats[label_idx['GA']]) if 'GA' in label_idx and label_idx['GA'] < len(stats) else 0
                        sa = int(stats[label_idx['SA']]) if 'SA' in label_idx and label_idx['SA'] < len(stats) else 0
                        sv = int(stats[label_idx['SV']]) if 'SV' in label_idx and label_idx['SV'] < len(stats) else 0
                        sv_pct_str = stats[label_idx['SV%']] if 'SV%' in label_idx and label_idx['SV%'] < len(stats) else '0'
                        toi = stats[label_idx['TOI']] if 'TOI' in label_idx and label_idx['TOI'] < len(stats) else '0:00'

                        try:
                            sv_pct = float(sv_pct_str)
                        except (ValueError, TypeError):
                            sv_pct = 0.0

                        # Parse TOI to minutes for starter detection
                        toi_mins = 0
                        try:
                            parts = toi.split(':')
                            toi_mins = int(parts[0]) + int(parts[1]) / 60 if len(parts) == 2 else 0
                        except (ValueError, IndexError):
                            pass

                        athletes_data.append({
                            'name': g_name, 'id': g_id,
                            'ga': ga, 'sa': sa, 'sv': sv,
                            'sv_pct': sv_pct, 'toi': toi,
                            'toi_mins': toi_mins
                        })

                        if toi_mins > max_toi_mins:
                            max_toi_mins = toi_mins
                            starter_name = g_name

                    # Store all goalies, marking starter
                    for gd in athletes_data:
                        is_starter = 1 if gd['name'] == starter_name else 0
                        try:
                            conn.execute("""
                                INSERT OR REPLACE INTO nhl_goalie_stats
                                    (game_date, espn_event_id, team, goalie_name,
                                     espn_athlete_id, goals_against, shots_against,
                                     saves, save_pct, time_on_ice, is_starter, fetched_at)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                            """, (
                                current.strftime('%Y-%m-%d'), eid, team_name,
                                gd['name'], gd['id'],
                                gd['ga'], gd['sa'], gd['sv'],
                                gd['sv_pct'], gd['toi'], is_starter,
                                datetime.now(_ET).isoformat()
                            ))
                            goalies_stored += 1
                        except Exception:
                            pass

            conn.commit()
            games_scraped += 1
            time.sleep(0.3)  # Be polite to ESPN

        current += timedelta(days=1)

    if verbose:
        print(f"  NHL goalie history: {games_scraped} games, {goalies_stored} goalie lines stored")

    conn.close()
    return games_scraped


def get_nhl_goalie_stats(conn, goalie_name):
    """
    Get a goalie's season stats from nhl_goalie_stats table.

    Returns dict with:
        - gaa: Goals Against Average
        - sv_pct: Save Percentage
        - games: Games played (starts)
    Or None if insufficient data.

    Requires 10+ starts before returning stats (same logic as
    MLB pitcher 30 IP minimum for reliability).
    """
    MIN_GAMES = 10

    try:
        conn.execute("SELECT 1 FROM nhl_goalie_stats LIMIT 1")
    except sqlite3.OperationalError:
        return None

    # Get aggregate stats for this goalie (starter appearances only)
    row = conn.execute("""
        SELECT
            COUNT(*) as games,
            SUM(goals_against) as total_ga,
            SUM(shots_against) as total_sa,
            SUM(saves) as total_sv
        FROM nhl_goalie_stats
        WHERE goalie_name = ?
        AND is_starter = 1
    """, (goalie_name,)).fetchone()

    if not row or row[0] < MIN_GAMES:
        # Try fuzzy match on last name
        last_name = goalie_name.split()[-1] if goalie_name else ''
        if last_name:
            row = conn.execute("""
                SELECT
                    COUNT(*) as games,
                    SUM(goals_against) as total_ga,
                    SUM(shots_against) as total_sa,
                    SUM(saves) as total_sv
                FROM nhl_goalie_stats
                WHERE goalie_name LIKE ?
                AND is_starter = 1
            """, (f"%{last_name}%",)).fetchone()

    if not row or row[0] < MIN_GAMES:
        return None

    games, total_ga, total_sa, total_sv = row
    gaa = round(total_ga / games, 2) if games > 0 else 0.0
    sv_pct = round(total_sv / total_sa, 3) if total_sa and total_sa > 0 else 0.0

    return {
        'gaa': gaa,
        'sv_pct': sv_pct,
        'games': games,
    }


def get_nhl_probable_goalies(conn, home, away, game_date=None):
    """
    Get NHL probable starting goalies for a game.

    Returns dict with:
        - home_goalie: name or None
        - away_goalie: name or None
        - home_goalie_stats: dict with gaa/sv_pct/games or None
        - away_goalie_stats: dict with gaa/sv_pct/games or None
        - both_confirmed: True if both goalies are known
        - summary: human-readable string

    Used by model_engine to gate NHL picks and adjust totals.
    """
    if game_date is None:
        game_date = datetime.now(_ET).strftime('%Y-%m-%d')

    # Check if table exists
    try:
        conn.execute("SELECT 1 FROM nhl_probable_goalies LIMIT 1")
    except sqlite3.OperationalError:
        return {'home_goalie': None, 'away_goalie': None,
                'home_goalie_stats': None, 'away_goalie_stats': None,
                'both_confirmed': False, 'summary': 'No NHL goalie data table'}

    row = conn.execute("""
        SELECT home_goalie, away_goalie, home_goalie_status, away_goalie_status
        FROM nhl_probable_goalies
        WHERE home=? AND away=? AND game_date=?
        ORDER BY fetched_at DESC LIMIT 1
    """, (home, away, game_date)).fetchone()

    if not row:
        # Try fuzzy match (team names may differ slightly between APIs)
        row = conn.execute("""
            SELECT home_goalie, away_goalie, home_goalie_status, away_goalie_status
            FROM nhl_probable_goalies
            WHERE game_date=?
            AND (home LIKE ? OR home LIKE ?)
            AND (away LIKE ? OR away LIKE ?)
            ORDER BY fetched_at DESC LIMIT 1
        """, (game_date,
              f"%{home.split()[-1]}%", f"%{home}%",
              f"%{away.split()[-1]}%", f"%{away}%")).fetchone()

    if not row:
        return {'home_goalie': None, 'away_goalie': None,
                'home_goalie_stats': None, 'away_goalie_stats': None,
                'both_confirmed': False,
                'summary': f'No goalie data for {away} @ {home}'}

    hg, ag = row[0], row[1]
    h_status, a_status = row[2] or '', row[3] or ''
    both = hg is not None and ag is not None

    # Look up season stats for each goalie
    h_stats = get_nhl_goalie_stats(conn, hg) if hg else None
    a_stats = get_nhl_goalie_stats(conn, ag) if ag else None

    parts = []
    if hg:
        stat_s = f" ({h_stats['gaa']:.2f} GAA, {h_stats['sv_pct']:.3f} SV%)" if h_stats else ""
        parts.append(f"{home} G: {hg}{stat_s}")
    if ag:
        stat_s = f" ({a_stats['gaa']:.2f} GAA, {a_stats['sv_pct']:.3f} SV%)" if a_stats else ""
        parts.append(f"{away} G: {ag}{stat_s}")

    return {
        'home_goalie': hg,
        'away_goalie': ag,
        'home_goalie_stats': h_stats,
        'away_goalie_stats': a_stats,
        'home_goalie_status': h_status,
        'away_goalie_status': a_status,
        'both_confirmed': both,
        'summary': ' | '.join(parts) if parts else 'No goalie data',
    }


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    args = sys.argv[1:]

    if '--analyze' in args:
        team = None
        if '--team' in args:
            idx = args.index('--team')
            if idx + 1 < len(args):
                team = args[idx + 1]
        build_pitching_quality(verbose=True)
        analyze_rotations(team_filter=team)
        return

    days_back = None
    if '--days' in args:
        idx = args.index('--days')
        if idx + 1 < len(args):
            try:
                days_back = int(args[idx + 1])
            except ValueError:
                pass

    print("="*60)
    print("  PITCHER SCRAPER — ESPN Baseball (College + MLB)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    print("="*60)

    # Step 1: Scrape ESPN pitcher data (college)
    print("\n📊 Step 1: Scraping ESPN box scores for college pitcher data...")
    scrape_pitcher_data(days_back=days_back)

    # Step 1b: Scrape MLB pitcher history from completed games
    print("\n📊 Step 1b: Scraping MLB pitcher history...")
    scrape_mlb_pitcher_history(days_back=days_back or 7)

    # Step 2: Build day-of-week pitching quality from results
    print("\n📈 Step 2: Building day-of-week pitching quality...")
    build_pitching_quality()

    # Step 3: Fetch today's MLB probable pitchers
    print("\n⚾ Step 3: Fetching MLB probable pitchers...")
    scrape_mlb_pitchers()

    print("\n✅ Done!")


if __name__ == '__main__':
    main()
