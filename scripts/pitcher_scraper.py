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
            dow = dt.weekday()  # 0=Mon through 6=Sun
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


def get_pitcher_context(conn, home, away, commence_time=None):
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
            dow = dt.weekday()
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

    # Day-of-week baseline adjustments (from college baseball patterns)
    # Friday aces suppress scoring, Sunday #3 starters allow more
    DOW_TOTAL_ADJ = {
        'friday': -0.5,    # Aces → lower scoring
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
    print("  PITCHER SCRAPER — ESPN College Baseball")
    print(f"  {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    print("="*60)

    # Step 1: Scrape ESPN pitcher data
    print("\n📊 Step 1: Scraping ESPN box scores for pitcher data...")
    scrape_pitcher_data(days_back=days_back)

    # Step 2: Build day-of-week pitching quality from results
    print("\n📈 Step 2: Building day-of-week pitching quality...")
    build_pitching_quality()

    print("\n✅ Done!")


if __name__ == '__main__':
    main()
