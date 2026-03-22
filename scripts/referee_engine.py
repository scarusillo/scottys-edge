"""
referee_engine.py — Referee/Official Tendencies for Totals Adjustment

Tracks how specific referees affect game totals by scraping official
assignments from ESPN's game summary API.

Data source: ESPN summary endpoint (gameInfo.officials)
  - NBA: 3 referees per game, available pre-game (~9am game day)
  - NHL: 2 referees + 2 linesmen, available pre-game
  - Soccer: 1 referee, available pre-game
  - NCAAB: 3 referees, availability varies

ESPN provides pre-game official assignments for NBA/NHL/soccer, so this
engine CAN be used for live predictions — not just retroactive analysis.

Usage:
    python referee_engine.py                    # Backfill last 14 days all sports
    python referee_engine.py --sport nba        # Just NBA
    python referee_engine.py --days 30          # Last 30 days
    python referee_engine.py --today            # Scrape today's assigned officials
"""
import sqlite3, json, os, sys, time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ESPN summary endpoints by sport key
ESPN_SUMMARY = {
    'basketball_nba': 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={}',
    'basketball_ncaab': 'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={}',
    'icehockey_nhl': 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event={}',
    'soccer_epl': 'https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary?event={}',
    'soccer_italy_serie_a': 'https://site.api.espn.com/apis/site/v2/sports/soccer/ita.1/summary?event={}',
    'soccer_spain_la_liga': 'https://site.api.espn.com/apis/site/v2/sports/soccer/esp.1/summary?event={}',
    'soccer_germany_bundesliga': 'https://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/summary?event={}',
    'soccer_france_ligue_one': 'https://site.api.espn.com/apis/site/v2/sports/soccer/fra.1/summary?event={}',
    'soccer_usa_mls': 'https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/summary?event={}',
}

ESPN_SCOREBOARD = {
    'basketball_nba': 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={}',
    'basketball_ncaab': 'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={}',
    'icehockey_nhl': 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={}',
    'soccer_epl': 'https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?dates={}',
    'soccer_italy_serie_a': 'https://site.api.espn.com/apis/site/v2/sports/soccer/ita.1/scoreboard?dates={}',
    'soccer_spain_la_liga': 'https://site.api.espn.com/apis/site/v2/sports/soccer/esp.1/scoreboard?dates={}',
    'soccer_germany_bundesliga': 'https://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/scoreboard?dates={}',
    'soccer_france_ligue_one': 'https://site.api.espn.com/apis/site/v2/sports/soccer/fra.1/scoreboard?dates={}',
    'soccer_usa_mls': 'https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard?dates={}',
}

# Adjustment caps per sport category
ADJ_CAPS = {
    'basketball': 2.0,
    'icehockey': 0.5,
    'soccer': 0.3,
}

# Minimum games required before we trust a ref's tendency
MIN_GAMES = 15


def _get_sport_category(sport):
    """Return broad category for cap lookup."""
    if 'basketball' in sport:
        return 'basketball'
    elif 'hockey' in sport or 'icehockey' in sport:
        return 'icehockey'
    elif 'soccer' in sport:
        return 'soccer'
    return None


def _fetch_json(url):
    """Fetch JSON from ESPN API with retries."""
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    for attempt in range(3):
        try:
            resp = urlopen(req, timeout=15)
            return json.loads(resp.read().decode())
        except (URLError, HTTPError, Exception) as e:
            if attempt < 2:
                time.sleep(1)
            else:
                return None
    return None


def init_db(conn):
    """Create the officials table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS officials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT NOT NULL,
            event_id TEXT NOT NULL,
            official_name TEXT NOT NULL,
            role TEXT DEFAULT 'Referee',
            game_date TEXT NOT NULL,
            home TEXT,
            away TEXT,
            home_score INTEGER,
            away_score INTEGER,
            actual_total INTEGER,
            total_fouls INTEGER,
            UNIQUE(sport, event_id, official_name)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_officials_name
        ON officials(official_name, sport)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_officials_sport_date
        ON officials(sport, game_date)
    """)
    # Migration: add total_cards column for soccer
    try:
        conn.execute("SELECT total_cards FROM officials LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE officials ADD COLUMN total_cards INTEGER")
    conn.commit()


def _get_event_ids_for_date(sport, date_str):
    """Get event IDs from ESPN scoreboard for a specific date."""
    url_template = ESPN_SCOREBOARD.get(sport)
    if not url_template:
        return []
    url = url_template.format(date_str)
    data = _fetch_json(url)
    if not data:
        return []

    event_ids = []
    for event in data.get('events', []):
        eid = event.get('id')
        if eid:
            event_ids.append(str(eid))
    return event_ids


def _parse_summary(data, sport, event_id, game_date):
    """Parse ESPN summary response into official records."""
    records = []
    if not data:
        return records

    game_info = data.get('gameInfo', {})
    officials = game_info.get('officials', [])
    if not officials:
        return records

    # Get scores from header
    header = data.get('header', {})
    competitions = header.get('competitions', [])
    home_team = away_team = None
    home_score = away_score = None

    for comp in competitions:
        for competitor in comp.get('competitors', []):
            team_name = competitor.get('team', {}).get('displayName', '')
            score = competitor.get('score', '')
            try:
                score = int(score)
            except (ValueError, TypeError):
                score = None
            if competitor.get('homeAway') == 'home':
                home_team = team_name
                home_score = score
            else:
                away_team = team_name
                away_score = score

    # Calculate total
    actual_total = None
    if home_score is not None and away_score is not None:
        actual_total = home_score + away_score

    # Get fouls from boxscore (NBA/NCAAB)
    total_fouls = None
    total_cards = None
    boxscore = data.get('boxscore', {})
    if 'basketball' in sport:
        # Look in team stats
        for team_data in boxscore.get('teams', []):
            stats = team_data.get('statistics', [])
            for stat_group in stats:
                # Can be a list of stat objects
                if isinstance(stat_group, dict):
                    labels = stat_group.get('labels', [])
                    totals = stat_group.get('totals', [])
                    if 'PF' in labels and totals:
                        idx = labels.index('PF')
                        if idx < len(totals):
                            try:
                                pf = int(totals[idx])
                                total_fouls = (total_fouls or 0) + pf
                            except (ValueError, TypeError):
                                pass
    elif 'soccer' in sport:
        # Soccer: extract cards and fouls from boxscore team stats or
        # from the keyEvents / header statistics.
        yellow_cards = 0
        red_cards = 0
        fouls_committed = 0

        # Method 1: boxscore.teams[].statistics[] — labels like
        # "Yellow Cards", "Red Cards", "Fouls"
        for team_data in boxscore.get('teams', []):
            stats = team_data.get('statistics', [])
            for stat_group in stats:
                if isinstance(stat_group, dict):
                    labels = stat_group.get('labels', [])
                    totals = stat_group.get('totals', [])
                    if not labels or not totals:
                        continue
                    for lbl_key, target in [
                        ('Yellow Cards', 'yellow'), ('yellowCards', 'yellow'),
                        ('Red Cards', 'red'), ('redCards', 'red'),
                        ('Fouls', 'fouls'), ('Fouls Committed', 'fouls'),
                        ('foulsCommitted', 'fouls'),
                    ]:
                        if lbl_key in labels:
                            idx = labels.index(lbl_key)
                            if idx < len(totals):
                                try:
                                    val = int(totals[idx])
                                    if target == 'yellow':
                                        yellow_cards += val
                                    elif target == 'red':
                                        red_cards += val
                                    elif target == 'fouls':
                                        fouls_committed += val
                                except (ValueError, TypeError):
                                    pass
                    # ESPN soccer sometimes uses 'displayValue' in stat objects
                    if hasattr(stat_group, 'get') and stat_group.get('name'):
                        nm = stat_group['name'].lower()
                        dv = stat_group.get('displayValue', stat_group.get('value', ''))
                        try:
                            dv = int(dv)
                        except (ValueError, TypeError):
                            dv = 0
                        if 'yellow' in nm and 'card' in nm:
                            yellow_cards += dv
                        elif 'red' in nm and 'card' in nm:
                            red_cards += dv
                        elif nm in ('fouls', 'foulscommitted', 'fouls committed'):
                            fouls_committed += dv

        # Method 2: header.competitions[].competitors[].statistics[]
        # ESPN sometimes puts team stats here with abbreviated names
        for comp in competitions:
            for competitor in comp.get('competitors', []):
                for st in competitor.get('statistics', []):
                    if isinstance(st, dict):
                        nm = st.get('name', '').lower()
                        val = st.get('value', st.get('displayValue', ''))
                        try:
                            val = int(val)
                        except (ValueError, TypeError):
                            try:
                                val = int(float(val))
                            except (ValueError, TypeError):
                                val = 0
                        if nm in ('yellowcards', 'yellow_cards'):
                            yellow_cards += val
                        elif nm in ('redcards', 'red_cards'):
                            red_cards += val
                        elif nm in ('fouls', 'foulscommitted', 'fouls_committed'):
                            fouls_committed += val

        total_cards = yellow_cards + red_cards
        if fouls_committed > 0:
            total_fouls = fouls_committed

    for official in officials:
        name = official.get('fullName') or official.get('displayName', '')
        if not name:
            continue
        role = official.get('position', {}).get('displayName', 'Referee')
        records.append({
            'sport': sport,
            'event_id': str(event_id),
            'official_name': name,
            'role': role,
            'game_date': game_date,
            'home': home_team,
            'away': away_team,
            'home_score': home_score,
            'away_score': away_score,
            'actual_total': actual_total,
            'total_fouls': total_fouls,
            'total_cards': total_cards,
        })

    return records


def scrape_officials(sport=None, days_back=14, conn=None, verbose=False):
    """
    Fetch official names from ESPN game summaries for completed games.

    Args:
        sport: Specific sport key (e.g., 'basketball_nba') or None for all
        days_back: Number of days to look back
        conn: Optional DB connection (will create one if None)

    Returns:
        int: Number of new records inserted
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close_conn = True

    init_db(conn)

    sports = [sport] if sport else list(ESPN_SUMMARY.keys())
    total_inserted = 0

    for sp in sports:
        if sp not in ESPN_SUMMARY:
            print(f"  [SKIP] No summary endpoint for {sp}")
            continue

        print(f"  [{sp}] Scraping officials for last {days_back} days...")
        new_count = 0

        for day_offset in range(days_back):
            dt = datetime.now() - timedelta(days=day_offset)
            date_str = dt.strftime('%Y%m%d')
            game_date = dt.strftime('%Y-%m-%d')

            # Check if we already have data for this date/sport
            existing = conn.execute(
                "SELECT COUNT(*) FROM officials WHERE sport=? AND game_date=?",
                (sp, game_date)
            ).fetchone()[0]
            if existing > 0 and day_offset > 1:
                # Skip dates we already have (except last 2 days — refresh those)
                continue

            event_ids = _get_event_ids_for_date(sp, date_str)
            if not event_ids:
                continue

            for eid in event_ids:
                # Check if already scraped
                already = conn.execute(
                    "SELECT 1 FROM officials WHERE sport=? AND event_id=?",
                    (sp, str(eid))
                ).fetchone()
                if already:
                    continue

                url = ESPN_SUMMARY[sp].format(eid)
                data = _fetch_json(url)
                if not data:
                    continue

                records = _parse_summary(data, sp, eid, game_date)
                for rec in records:
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO officials
                            (sport, event_id, official_name, role, game_date,
                             home, away, home_score, away_score, actual_total,
                             total_fouls, total_cards)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (rec['sport'], rec['event_id'], rec['official_name'],
                              rec['role'], rec['game_date'], rec['home'], rec['away'],
                              rec['home_score'], rec['away_score'], rec['actual_total'],
                              rec['total_fouls'], rec.get('total_cards')))
                        if conn.total_changes:
                            new_count += 1
                    except sqlite3.IntegrityError:
                        pass

                time.sleep(0.3)  # Be polite to ESPN

            conn.commit()

        print(f"    -> {new_count} new official records for {sp}")
        total_inserted += new_count

    if close_conn:
        conn.close()

    return total_inserted


def get_ref_tendency(official_name, sport, conn=None):
    """
    Returns tendency stats for a specific referee.

    Args:
        official_name: Full name of the official
        sport: Sport key (e.g., 'basketball_nba')
        conn: Optional DB connection

    Returns:
        dict with:
            - avg_total_deviation: how much this ref's games deviate from league avg
            - game_count: number of games with data
            - over_pct: % of games that went over league average total
            - avg_fouls: average total fouls in this ref's games (basketball only)
        or None if no data
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close_conn = True

    try:
        init_db(conn)

        # Get this ref's games with actual totals
        ref_games = conn.execute("""
            SELECT actual_total, total_fouls, total_cards FROM officials
            WHERE official_name = ? AND sport = ? AND actual_total IS NOT NULL
            GROUP BY event_id
        """, (official_name, sport)).fetchall()

        if not ref_games:
            return None

        ref_totals = [r[0] for r in ref_games]
        ref_fouls = [r[1] for r in ref_games if r[1] is not None]
        ref_cards = [r[2] for r in ref_games if r[2] is not None]

        # Get league average total for this sport
        league_avg = conn.execute("""
            SELECT AVG(actual_total) FROM (
                SELECT actual_total FROM officials
                WHERE sport = ? AND actual_total IS NOT NULL
                GROUP BY event_id
            )
        """, (sport,)).fetchone()[0]

        if league_avg is None:
            return None

        avg_ref_total = sum(ref_totals) / len(ref_totals)
        avg_total_deviation = avg_ref_total - league_avg
        over_count = sum(1 for t in ref_totals if t > league_avg)
        over_pct = over_count / len(ref_totals) * 100

        result = {
            'official_name': official_name,
            'avg_total_deviation': round(avg_total_deviation, 2),
            'game_count': len(ref_totals),
            'over_pct': round(over_pct, 1),
            'league_avg_total': round(league_avg, 1),
            'ref_avg_total': round(avg_ref_total, 1),
        }

        if ref_fouls:
            result['avg_fouls'] = round(sum(ref_fouls) / len(ref_fouls), 1)

        if ref_cards:
            result['avg_cards'] = round(sum(ref_cards) / len(ref_cards), 1)

        return result

    finally:
        if close_conn:
            conn.close()


def get_game_officials(home, away, sport, conn=None):
    """
    Try to find tonight's assigned officials for a game.

    ESPN's summary endpoint often has pre-game official assignments
    (NBA posts ~9am game day, NHL similar). This attempts to fetch them.

    Args:
        home: Home team name
        away: Away team name
        sport: Sport key

    Returns:
        list of dicts with official info, or None if unavailable
    """
    if sport not in ESPN_SCOREBOARD or sport not in ESPN_SUMMARY:
        return None

    today_str = datetime.now().strftime('%Y%m%d')
    event_ids = _get_event_ids_for_date(sport, today_str)

    for eid in event_ids:
        url = ESPN_SUMMARY[sport].format(eid)
        data = _fetch_json(url)
        if not data:
            continue

        # Check if this is the right game by matching teams
        header = data.get('header', {})
        competitions = header.get('competitions', [])
        game_teams = []
        for comp in competitions:
            for competitor in comp.get('competitors', []):
                tname = competitor.get('team', {}).get('displayName', '')
                game_teams.append(tname.lower())

        # Fuzzy match: check if home/away appear in the team names
        home_match = any(home.lower() in t or t in home.lower() for t in game_teams)
        away_match = any(away.lower() in t or t in away.lower() for t in game_teams)

        if not (home_match or away_match):
            # Try short names
            for comp in competitions:
                for competitor in comp.get('competitors', []):
                    short = competitor.get('team', {}).get('shortDisplayName', '')
                    game_teams.append(short.lower())
            home_match = any(home.lower() in t or t in home.lower() for t in game_teams)
            away_match = any(away.lower() in t or t in away.lower() for t in game_teams)

        if home_match or away_match:
            game_info = data.get('gameInfo', {})
            officials = game_info.get('officials', [])
            if officials:
                return [
                    {
                        'name': o.get('fullName', ''),
                        'role': o.get('position', {}).get('displayName', 'Referee'),
                    }
                    for o in officials if o.get('fullName')
                ]

    return None


def get_ref_adjustment(home, away, sport, conn):
    """
    Main function called by model_engine.py.

    Returns a float adjustment to add to model_total, or 0.0 if no data.
    Only returns non-zero if we have MIN_GAMES+ games for the official.
    Caps adjustment based on sport.

    Args:
        home: Home team name
        away: Away team name
        sport: Sport key
        conn: DB connection

    Returns:
        tuple: (adjustment_float, info_string or '')
    """
    init_db(conn)

    cat = _get_sport_category(sport)
    if cat is None:
        return 0.0, ''

    cap = ADJ_CAPS.get(cat, 1.0)

    # Try to get today's officials for this game
    officials = get_game_officials(home, away, sport, conn)
    if not officials:
        return 0.0, ''

    # Only use referees (not linesmen) for the adjustment
    refs = [o for o in officials if o['role'] == 'Referee']
    if not refs:
        refs = officials  # Fallback: use all officials if role parsing failed

    adjustments = []
    ref_infos = []

    for ref in refs:
        tendency = get_ref_tendency(ref['name'], sport, conn)
        if tendency is None:
            continue
        if tendency['game_count'] < MIN_GAMES:
            continue

        dev = tendency['avg_total_deviation']

        # Weight by confidence: more games = more weight (sqrt scaling)
        # At MIN_GAMES (15): weight = 0.5, at 60 games: weight = 1.0
        confidence_weight = min(1.0, (tendency['game_count'] / 60) ** 0.5)
        weighted_dev = dev * confidence_weight

        adjustments.append(weighted_dev)
        ref_infos.append(f"{ref['name']} ({dev:+.1f}, {tendency['game_count']}g)")

    if not adjustments:
        return 0.0, ''

    # Average across all referees
    raw_adj = sum(adjustments) / len(adjustments)

    # Cap the adjustment
    adj = max(-cap, min(cap, raw_adj))

    if abs(adj) < 0.1:
        return 0.0, ''  # Too small to matter

    info = 'Ref: ' + ', '.join(ref_infos)
    return round(adj, 2), info


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Referee/Official Tendencies Engine')
    parser.add_argument('--sport', type=str, default=None,
                        help='Sport key (e.g., basketball_nba, icehockey_nhl)')
    parser.add_argument('--days', type=int, default=14,
                        help='Days to look back (default: 14)')
    parser.add_argument('--today', action='store_true',
                        help="Show today's assigned officials")
    parser.add_argument('--ref', type=str, default=None,
                        help='Look up a specific referee tendency')
    parser.add_argument('--top', action='store_true',
                        help='Show top over/under refs')
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # Map short names
    sport_alias = {
        'nba': 'basketball_nba', 'ncaab': 'basketball_ncaab',
        'nhl': 'icehockey_nhl', 'epl': 'soccer_epl',
        'mls': 'soccer_usa_mls', 'laliga': 'soccer_spain_la_liga',
        'seriea': 'soccer_italy_serie_a', 'bundesliga': 'soccer_germany_bundesliga',
        'ligue1': 'soccer_france_ligue_one',
    }
    sport = sport_alias.get(args.sport, args.sport) if args.sport else None

    if args.ref:
        # Look up a specific ref
        sp = sport or 'basketball_nba'
        t = get_ref_tendency(args.ref, sp, conn)
        if t:
            print(f"\n  {t['official_name']} ({sp})")
            print(f"  Games: {t['game_count']}")
            print(f"  Avg Total: {t['ref_avg_total']} (league avg: {t['league_avg_total']})")
            print(f"  Deviation: {t['avg_total_deviation']:+.1f}")
            print(f"  Over %: {t['over_pct']:.0f}%")
            if 'avg_fouls' in t:
                print(f"  Avg Fouls: {t['avg_fouls']}")
            if 'avg_cards' in t:
                print(f"  Avg Cards: {t['avg_cards']}")
        else:
            print(f"  No data for {args.ref} in {sp}")
        conn.close()
        return

    if args.top:
        # Show top over/under referees
        sp = sport or 'basketball_nba'
        refs = conn.execute("""
            SELECT official_name, COUNT(DISTINCT event_id) as games
            FROM officials WHERE sport = ? AND role = 'Referee'
            GROUP BY official_name HAVING games >= ?
            ORDER BY games DESC
        """, (sp, MIN_GAMES)).fetchall()

        if not refs:
            print(f"  No referees with {MIN_GAMES}+ games in {sp}")
            conn.close()
            return

        tendencies = []
        for name, _ in refs:
            t = get_ref_tendency(name, sp, conn)
            if t:
                tendencies.append(t)

        tendencies.sort(key=lambda x: x['avg_total_deviation'], reverse=True)

        print(f"\n  {'Referee':<25} {'Games':>5} {'AvgTotal':>8} {'Dev':>6} {'Over%':>6}")
        print(f"  {'-'*25} {'-'*5} {'-'*8} {'-'*6} {'-'*6}")
        for t in tendencies:
            print(f"  {t['official_name']:<25} {t['game_count']:>5} "
                  f"{t['ref_avg_total']:>8.1f} {t['avg_total_deviation']:>+6.1f} "
                  f"{t['over_pct']:>5.0f}%")
        conn.close()
        return

    if args.today:
        # Show today's officials
        sports_to_check = [sport] if sport else list(ESPN_SUMMARY.keys())
        for sp in sports_to_check:
            today_str = datetime.now().strftime('%Y%m%d')
            event_ids = _get_event_ids_for_date(sp, today_str)
            if not event_ids:
                continue
            print(f"\n  [{sp}] Today's games:")
            for eid in event_ids:
                url = ESPN_SUMMARY[sp].format(eid)
                data = _fetch_json(url)
                if not data:
                    continue
                header = data.get('header', {})
                comps = header.get('competitions', [])
                teams = []
                for comp in comps:
                    for c in comp.get('competitors', []):
                        teams.append(c.get('team', {}).get('displayName', '?'))
                game_info = data.get('gameInfo', {})
                officials = game_info.get('officials', [])
                ref_names = [o.get('fullName', '?') for o in officials]
                matchup = ' vs '.join(teams) if teams else eid
                print(f"    {matchup}")
                if ref_names:
                    for o in officials:
                        n = o.get('fullName', '?')
                        r = o.get('position', {}).get('displayName', '?')
                        t = get_ref_tendency(n, sp, conn)
                        if t and t['game_count'] >= 5:
                            print(f"      {r}: {n} (dev={t['avg_total_deviation']:+.1f}, "
                                  f"{t['game_count']}g, over={t['over_pct']:.0f}%)")
                        else:
                            print(f"      {r}: {n}")
                else:
                    print(f"      Officials: not yet assigned")
                time.sleep(0.3)
        conn.close()
        return

    # Default: scrape officials
    print(f"\nReferee Engine — Scraping officials data...")
    inserted = scrape_officials(sport=sport, days_back=args.days, conn=conn)
    print(f"\nDone. {inserted} new records inserted.")

    # Show summary
    summary = conn.execute("""
        SELECT sport, COUNT(DISTINCT event_id) as games, COUNT(DISTINCT official_name) as refs
        FROM officials GROUP BY sport ORDER BY sport
    """).fetchall()
    if summary:
        print(f"\n  {'Sport':<30} {'Games':>6} {'Officials':>9}")
        print(f"  {'-'*30} {'-'*6} {'-'*9}")
        for sp, games, refs in summary:
            print(f"  {sp:<30} {games:>6} {refs:>9}")

    conn.close()


if __name__ == '__main__':
    main()
