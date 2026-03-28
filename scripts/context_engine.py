"""
context_engine.py — Contextual Edge Factors

Computes spread/total adjustments that the market often misprices:
  1. REST & SCHEDULE — back-to-backs, 3-in-5, midweek congestion
  2. LINE MOVEMENT  — opener→current, sharp vs public money signal
  3. HOME/AWAY SPLITS — teams that over/underperform at home vs road
  4. TRAVEL & TIMEZONE — cross-country trips, early starts for west coast
  5. ALTITUDE — Denver (5,280ft), Salt Lake, Mexico City
  6. MOTIVATION/SPOT — post-rivalry letdown, conference tournament bubble

These return spread adjustments (in points) that modify the model prediction
before it enters scottys_edge_assessment(). They also return confidence
modifiers that can scale the edge up or down.

Integration:
    from context_engine import get_context_adjustments
    adj = get_context_adjustments(conn, sport, home, away, event_id, commence)
    model_spread -= adj['spread_adj']       # Positive adj = home advantage = ms more negative
    confidence_mult = adj['confidence']     # Scale edge up/down

Each factor is independent and capped to prevent runaway adjustments.
Total spread adjustment is capped at ±3.0 points.
"""
import sqlite3
from datetime import datetime, timedelta, timezone


# ═══════════════════════════════════════════════════════════════════
# CALENDAR-AWARE CONTEXT — Skip unreliable factors during special events
# ═══════════════════════════════════════════════════════════════════════
# Certain context factors become meaningless during specific calendar windows.
# Instead of disabling all context, we surgically skip only the broken factors.

def _get_context_freeze(sport, commence=None):
    """
    Returns a set of context keys to SKIP based on sport + date.
    
    Empty set = everything applies normally.
    Non-empty = those specific adjustments are suppressed.
    """
    now = datetime.now()
    month, day = now.month, now.day
    freeze = set()
    
    # ── NCAAB: NCAA Tournament (March 17+ and April 1-7) ──
    # Neutral sites. Both teams at same venue. B2B/rest/bounce-back meaningless.
    # Before March 17: regular season + conf tournaments (rest still matters).
    # March 17+: NCAA tournament — teams play every 2 days, rest is equal.
    if 'basketball_ncaab' in sport:
        if (month == 3 and day >= 17) or (month == 4 and day <= 7):
            freeze.update(['home_b2b', 'away_b2b', 'away_bounceback', 'home_bounceback'])
    
    # ── NBA: All-Star Break window (Feb 14-23 typical) ──
    # Every team gets ~a week off. Rest calculations are meaningless
    # for the first 3 days after break since everyone is rested equally.
    if 'basketball_nba' in sport and month == 2 and 14 <= day <= 25:
        freeze.update(['home_b2b', 'away_b2b', 'home_3in5', 'away_3in5',
                       'home_4in6', 'away_4in6', 'home_extra_rest', 'away_extra_rest'])
    
    # ── NHL: All-Star Break window (Feb 1-7 typical) ──
    # Same as NBA — everyone had extended rest, B2B doesn't apply
    if 'icehockey_nhl' in sport and month == 2 and 1 <= day <= 10:
        freeze.update(['home_b2b', 'away_b2b'])
    
    # ── Soccer: International Break windows ──
    # Clubs lose key players to national teams for 10-14 days.
    # First matchweek back is unpredictable — fatigue, injuries, disrupted chemistry.
    # Disable rest/congestion factors for first weekend after break.
    # Major international windows: early Sep, mid Oct, mid Nov, late Mar
    if 'soccer' in sport:
        # March international break (typically March 20-28)
        if month == 3 and 20 <= day <= 31:
            freeze.update(['home_midweek', 'away_midweek'])
        # September break (typically Sep 1-12)
        if month == 9 and 1 <= day <= 15:
            freeze.update(['home_midweek', 'away_midweek'])
        # October break (typically Oct 7-15)
        if month == 10 and 7 <= day <= 18:
            freeze.update(['home_midweek', 'away_midweek'])
        # November break (typically Nov 11-21)
        if month == 11 and 11 <= day <= 24:
            freeze.update(['home_midweek', 'away_midweek'])
    
    # ── College Baseball: Conference Tournaments + Regionals (May-June) ──
    # Same neutral-site issue as NCAAB. B2B/rest meaningless.
    if 'baseball' in sport and month in (5, 6):
        freeze.update(['home_b2b', 'away_b2b', 'away_bounceback'])
    
    return freeze


# ═══════════════════════════════════════════════════════════════════
# 1. REST & SCHEDULE
# ═══════════════════════════════════════════════════════════════════
# Walters methodology: schedule spots are one of the most reliable
# edges because the market underweights fatigue and travel.
#
# NBA back-to-back: team playing second night is -1.5 to -2.0 pts
# NBA 3-in-5 days: additional -0.5 pts
# NBA 4-in-6 days: additional -1.0 pts
# Soccer midweek (played Wed/Thu, now playing Sat): -0.5 pts
# Soccer extra time in previous match: -0.5 pts

def _days_since_last_game(conn, team, sport, before_date):
    """Find days of rest for a team before a given date."""
    row = conn.execute("""
        SELECT commence_time FROM results
        WHERE (home = ? OR away = ?) AND sport = ? AND completed = 1
        AND commence_time < ?
        ORDER BY commence_time DESC LIMIT 1
    """, (team, team, sport, before_date)).fetchone()
    
    if not row or not row[0]:
        return None  # Unknown — can't compute
    
    try:
        last_game = datetime.fromisoformat(row[0].replace('Z', '+00:00'))
        game_date = datetime.fromisoformat(before_date.replace('Z', '+00:00'))
        diff = (game_date - last_game).total_seconds() / 86400
        return diff
    except:
        return None


def _games_in_window(conn, team, sport, before_date, days_back):
    """Count how many games a team played in the last N days."""
    try:
        game_date = datetime.fromisoformat(before_date.replace('Z', '+00:00'))
        window_start = (game_date - timedelta(days=days_back)).isoformat()
    except:
        return 0
    
    row = conn.execute("""
        SELECT COUNT(*) FROM results
        WHERE (home = ? OR away = ?) AND sport = ? AND completed = 1
        AND commence_time >= ? AND commence_time < ?
    """, (team, team, sport, window_start, before_date)).fetchone()
    
    return row[0] if row else 0


def rest_adjustment(conn, home, away, sport, commence):
    """
    Compute rest advantage/disadvantage for home team.
    Returns spread adjustment (positive = home advantage).
    
    v12.2: Calendar-aware — skips unreliable factors during tournaments,
    All-Star breaks, and international windows.
    """
    if not commence:
        return 0.0, {}
    
    freeze = _get_context_freeze(sport, commence)
    
    h_rest = _days_since_last_game(conn, home, sport, commence)
    a_rest = _days_since_last_game(conn, away, sport, commence)
    
    h_games_5 = _games_in_window(conn, home, sport, commence, 5)
    a_games_5 = _games_in_window(conn, away, sport, commence, 5)
    
    adj = 0.0
    reasons = {}
    
    if 'basketball_nba' in sport:
        if h_rest is not None and h_rest <= 1.2 and 'home_b2b' not in freeze:
            adj -= 1.5
            reasons['home_b2b'] = -1.5
        if a_rest is not None and a_rest <= 1.2 and 'away_b2b' not in freeze:
            adj += 1.5
            reasons['away_b2b'] = 1.5
        
        if h_games_5 >= 3 and 'home_3in5' not in freeze:
            adj -= 0.5
            reasons['home_3in5'] = -0.5
        if a_games_5 >= 3 and 'away_3in5' not in freeze:
            adj += 0.5
            reasons['away_3in5'] = 0.5
            
        h_games_6 = _games_in_window(conn, home, sport, commence, 6)
        a_games_6 = _games_in_window(conn, away, sport, commence, 6)
        if h_games_6 >= 4 and 'home_4in6' not in freeze:
            adj -= 1.0
            reasons['home_4in6'] = -1.0
        if a_games_6 >= 4 and 'away_4in6' not in freeze:
            adj += 1.0
            reasons['away_4in6'] = 1.0
            
        if h_rest is not None and a_rest is not None:
            if h_rest >= 3 and a_rest <= 2 and 'home_extra_rest' not in freeze:
                adj += 0.5
                reasons['home_extra_rest'] = 0.5
            elif a_rest >= 3 and h_rest <= 2 and 'away_extra_rest' not in freeze:
                adj -= 0.5
                reasons['away_extra_rest'] = -0.5

    elif 'basketball_ncaab' in sport:
        if h_rest is not None and h_rest <= 1.2 and 'home_b2b' not in freeze:
            adj -= 1.0
            reasons['home_b2b'] = -1.0
        if a_rest is not None and a_rest <= 1.2 and 'away_b2b' not in freeze:
            adj += 1.0
            reasons['away_b2b'] = 1.0

    elif 'soccer' in sport:
        # Midweek congestion (played Wed/Thu, now Sat/Sun)
        if h_rest is not None and h_rest <= 3.5 and 'home_midweek' not in freeze:
            adj -= 0.5
            reasons['home_midweek'] = -0.5
        if a_rest is not None and a_rest <= 3.5 and 'away_midweek' not in freeze:
            adj += 0.5
            reasons['away_midweek'] = 0.5

        # European competition fatigue — UCL/Europa League midweek drains teams.
        # Teams playing European competition mid-week then domestic on weekend
        # are fatigued, especially if they traveled. The market often doesn't
        # fully adjust for this, especially in Ligue 1/Bundesliga.
        if 'champs' not in sport and 'europa' not in sport:
            # Only check for domestic league games (not UCL games themselves)
            for team, sign, label in [(home, -1, 'home'), (away, 1, 'away')]:
                euro_game = conn.execute("""
                    SELECT commence_time, home, away FROM results
                    WHERE sport IN ('soccer_uefa_champs_league', 'soccer_uefa_europa_league')
                    AND (home LIKE ? OR away LIKE ?) AND completed = 1
                    AND commence_time >= datetime(?, '-5 days')
                    AND commence_time < ?
                """, (f'%{team}%', f'%{team}%', commence, commence)).fetchone()
                if euro_game:
                    adj += sign * 0.4
                    reasons[f'{label}_euro_fatigue'] = sign * 0.4

    elif 'icehockey_nhl' in sport:
        if h_rest is not None and h_rest <= 1.2 and 'home_b2b' not in freeze:
            adj -= 1.0
            reasons['home_b2b'] = -1.0
        if a_rest is not None and a_rest <= 1.2 and 'away_b2b' not in freeze:
            adj += 1.0
            reasons['away_b2b'] = 1.0

    elif 'baseball' in sport:
        # Baseball: B2B is normal (weekend series). Detect HEAVY schedules.
        h_games_6d = _games_in_window(conn, home, sport, commence, days_back=6)
        a_games_6d = _games_in_window(conn, away, sport, commence, days_back=6)

        # Heavy schedule: 4 in 5 days
        if 'home_heavy_sched' not in freeze and h_games_5 >= 4:
            adj -= 0.5
            reasons['home_heavy_sched'] = -0.5
        if 'away_heavy_sched' not in freeze and a_games_5 >= 4:
            adj += 0.5
            reasons['away_heavy_sched'] = 0.5

        # Extreme: 5+ in 6 days
        if 'home_extreme_sched' not in freeze and h_games_6d >= 5:
            adj -= 0.75
            reasons['home_extreme_sched'] = -0.75
        if 'away_extreme_sched' not in freeze and a_games_6d >= 5:
            adj += 0.75
            reasons['away_extreme_sched'] = 0.75

        # Fresh arms advantage
        if h_rest is not None and a_rest is not None:
            if h_rest >= 3 and a_rest <= 1 and 'home_fresh_arms' not in freeze:
                adj += 0.3
                reasons['home_fresh_arms'] = 0.3
            elif a_rest >= 3 and h_rest <= 1 and 'away_fresh_arms' not in freeze:
                adj -= 0.3
                reasons['away_fresh_arms'] = -0.3

    return adj, reasons


# ═══════════════════════════════════════════════════════════════════
# 2. LINE MOVEMENT (opener → current)
# ═══════════════════════════════════════════════════════════════════
# Walters p.195: "Follow the money, not the public."
#
# Line moves toward our side = sharp money agrees = confidence BOOST
# Line moves away from our side = public on our side = CAUTION
# Reverse line movement (line moves opposite to public) = STRONG signal

def line_movement_signal(conn, event_id, side, market_type='spreads'):
    """
    Compare opening line to current line for a specific side.
    
    Returns:
        movement: float (positive = line moved in our favor)
        signal: 'SHARP_AGREE' | 'PUBLIC_SIDE' | 'NEUTRAL'
        confidence_mult: float (0.85 to 1.15)
    """
    if market_type == 'TOTAL':
        market_key = 'totals'
        # For totals, side is 'Over' or 'Under'
        sel = side  
    else:
        market_key = 'spreads'
        sel = side
    
    opener = conn.execute("""
        SELECT line, odds FROM openers
        WHERE event_id = ? AND market = ? AND selection LIKE ?
        ORDER BY timestamp ASC LIMIT 1
    """, (event_id, market_key, f'%{sel}%')).fetchone()
    
    if not opener:
        return 0.0, 'NO_OPENER', 1.0
    
    current = conn.execute("""
        SELECT line, odds FROM odds
        WHERE event_id = ? AND market = ? AND selection LIKE ?
        ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1
    """, (event_id, market_key, f'%{sel}%')).fetchone()
    
    if not current:
        return 0.0, 'NO_CURRENT', 1.0
    
    open_line = opener[0]
    curr_line = current[0]
    
    if open_line is None or curr_line is None:
        return 0.0, 'NEUTRAL', 1.0
    
    if market_type == 'TOTAL':
        # For overs: line went DOWN = better for over = favorable
        # For unders: line went UP = better for under = favorable
        if 'OVER' in side.upper():
            movement = open_line - curr_line  # Positive = favorable
        else:
            movement = curr_line - open_line
    else:
        # For spreads: line moved toward our side = getting more points
        # E.g., opened +3.5, now +5.5 = +2.0 favorable movement
        movement = curr_line - open_line
    
    # Classify the movement
    if abs(movement) < 0.5:
        return movement, 'NEUTRAL', 1.0
    elif movement > 0:
        # Line moved in our favor — sharp money likely agrees
        return movement, 'SHARP_AGREE', 1.10
    else:
        # Line moved against us — we're on the public side
        # This doesn't mean we're wrong, but lower confidence
        return movement, 'PUBLIC_SIDE', 0.90


# ═══════════════════════════════════════════════════════════════════
# 3. HOME/AWAY PERFORMANCE SPLITS
# ═══════════════════════════════════════════════════════════════════
# Some teams are drastically different home vs away. If a team is 
# 15-3 at home but 5-12 on the road, the market spread should reflect
# that — but often the power rating treats them as one number.

def home_away_split_adjustment(conn, home, away, sport):
    """
    Compute whether home or away team significantly over/underperforms
    relative to their overall record at this venue type.
    
    Returns spread adjustment (positive = home stronger than ratings suggest).
    """
    # Get home team's home record
    h_home = conn.execute("""
        SELECT COUNT(*) as games,
               SUM(CASE WHEN winner = home THEN 1 ELSE 0 END) as wins,
               AVG(actual_margin) as avg_margin
        FROM results
        WHERE home = ? AND sport = ? AND completed = 1
    """, (home, sport)).fetchone()
    
    # Get home team's overall record
    h_all = conn.execute("""
        SELECT COUNT(*) as games,
               AVG(CASE 
                   WHEN home = ? THEN actual_margin
                   ELSE -actual_margin
               END) as avg_margin
        FROM results
        WHERE (home = ? OR away = ?) AND sport = ? AND completed = 1
    """, (home, home, home, sport)).fetchone()
    
    # Get away team's away record
    a_away = conn.execute("""
        SELECT COUNT(*) as games,
               SUM(CASE WHEN winner = away THEN 1 ELSE 0 END) as wins,
               AVG(-actual_margin) as avg_margin
        FROM results
        WHERE away = ? AND sport = ? AND completed = 1
    """, (away, sport)).fetchone()
    
    # Get away team's overall record
    a_all = conn.execute("""
        SELECT COUNT(*) as games,
               AVG(CASE 
                   WHEN home = ? THEN actual_margin
                   ELSE -actual_margin
               END) as avg_margin
        FROM results
        WHERE (home = ? OR away = ?) AND sport = ? AND completed = 1
    """, (away, away, away, sport)).fetchone()
    
    adj = 0.0
    reasons = {}
    
    # Need minimum sample size
    min_games = 5
    
    if h_home and h_all and h_home[0] >= min_games and h_all[0] >= min_games * 2:
        if h_home[2] is not None and h_all[1] is not None:
            home_boost = h_home[2] - h_all[1]  # How much better at home
            if abs(home_boost) >= 3.0:
                # Team is significantly different at home
                # Cap at ±1.0 adjustment (reduced from 1.5)
                capped = max(-1.0, min(1.0, home_boost * 0.2))
                adj += capped
                reasons['home_split'] = round(capped, 1)
    
    if a_away and a_all and a_away[0] >= min_games and a_all[0] >= min_games * 2:
        if a_away[2] is not None and a_all[1] is not None:
            away_drop = a_away[2] - a_all[1]  # How much worse on road (negative)
            if abs(away_drop) >= 3.0:
                capped = max(-1.0, min(1.0, -away_drop * 0.2))
                adj += capped  # Bad road team = advantage for home
                reasons['away_split'] = round(capped, 1)
    
    return adj, reasons


# ═══════════════════════════════════════════════════════════════════
# 4. TRAVEL & TIMEZONE
# ═══════════════════════════════════════════════════════════════════
# West coast team playing a noon EST game = body thinks it's 9am.
# Cross-country road trip = fatigue.
# These effects are small but consistent.

# Timezone mapping (offset from EST)
TEAM_TIMEZONE = {
    # NBA — Pacific (EST-3)
    'Los Angeles Lakers': -3, 'Los Angeles Clippers': -3,
    'Golden State Warriors': -3, 'Sacramento Kings': -3,
    'Portland Trail Blazers': -3,
    # NBA — Mountain (EST-2)
    'Denver Nuggets': -2, 'Utah Jazz': -2, 'Phoenix Suns': -2,
    # NBA — Central (EST-1)
    'Chicago Bulls': -1, 'Milwaukee Bucks': -1, 'Minnesota Timberwolves': -1,
    'Dallas Mavericks': -1, 'Houston Rockets': -1, 'San Antonio Spurs': -1,
    'Memphis Grizzlies': -1, 'New Orleans Pelicans': -1,
    'Oklahoma City Thunder': -1, 'Indiana Pacers': -1,
    # NBA — Eastern (EST+0)
    'Boston Celtics': 0, 'Brooklyn Nets': 0, 'New York Knicks': 0,
    'Philadelphia 76ers': 0, 'Toronto Raptors': 0, 'Miami Heat': 0,
    'Orlando Magic': 0, 'Atlanta Hawks': 0, 'Charlotte Hornets': 0,
    'Washington Wizards': 0, 'Cleveland Cavaliers': 0, 'Detroit Pistons': 0,
    
    # NHL — Pacific
    'Los Angeles Kings': -3, 'Anaheim Ducks': -3, 'San Jose Sharks': -3,
    'Seattle Kraken': -3, 'Vancouver Canucks': -3, 'Edmonton Oilers': -2,
    'Calgary Flames': -2, 'Vegas Golden Knights': -3,
    # NHL — Central
    'Chicago Blackhawks': -1, 'Minnesota Wild': -1, 'Dallas Stars': -1,
    'St. Louis Blues': -1, 'Nashville Predators': -1, 'Winnipeg Jets': -1,
    'Colorado Avalanche': -2, 'Arizona Coyotes': -2, 'Utah Hockey Club': -2,
    # NHL — Eastern
    'New York Rangers': 0, 'New York Islanders': 0, 'New Jersey Devils': 0,
    'Pittsburgh Penguins': 0, 'Philadelphia Flyers': 0, 'Washington Capitals': 0,
    'Carolina Hurricanes': 0, 'Columbus Blue Jackets': 0, 'Tampa Bay Lightning': 0,
    'Florida Panthers': 0, 'Boston Bruins': 0, 'Montreal Canadiens': 0,
    'Ottawa Senators': 0, 'Toronto Maple Leafs': 0, 'Detroit Red Wings': 0,
    'Buffalo Sabres': 0,

    # MLS — Pacific
    'LA Galaxy': -3, 'Los Angeles FC': -3, 'San Jose Earthquakes': -3,
    'Seattle Sounders FC': -3, 'Portland Timbers': -3, 'Vancouver Whitecaps FC': -3,
    # MLS — Mountain
    'Colorado Rapids': -2, 'Real Salt Lake': -2,
    # MLS — Central
    'FC Dallas': -1, 'Houston Dynamo FC': -1, 'Austin FC': -1,
    'Nashville SC': -1, 'St. Louis City SC': -1, 'Sporting Kansas City': -1,
    'Minnesota United FC': -1, 'Chicago Fire FC': -1,
    # MLS — Eastern
    'Atlanta United FC': 0, 'Charlotte FC': 0, 'Inter Miami CF': 0,
    'CF Montréal': 0, 'New England Revolution': 0, 'New York City FC': 0,
    'New York Red Bulls': 0, 'Orlando City SC': 0, 'Philadelphia Union': 0,
    'Toronto FC': 0, 'Columbus Crew': 0, 'D.C. United': 0, 'Cincinnati': 0,

    # College Baseball — SEC West
    'LSU Tigers': -1, 'Mississippi State Bulldogs': -1, 'Ole Miss Rebels': -1,
    'Alabama Crimson Tide': -1, 'Auburn Tigers': -1, 'Texas A&M Aggies': -1,
    'Arkansas Razorbacks': -1,
    # College Baseball — SEC East
    'Florida Gators': 0, 'Georgia Bulldogs': 0, 'Vanderbilt Commodores': -1,
    'South Carolina Gamecocks': 0, 'Tennessee Volunteers': 0, 'Kentucky Wildcats': 0,
    # College Baseball — ACC
    'Wake Forest Demon Deacons': 0, 'Clemson Tigers': 0, 'Duke Blue Devils': 0,
    'North Carolina Tar Heels': 0, 'Virginia Cavaliers': 0, 'Miami Hurricanes': 0,
    'Florida State Seminoles': 0, 'Louisville Cardinals': 0, 'NC State Wolfpack': 0,
    # College Baseball — Big 12
    'Oklahoma Sooners': -1, 'Texas Longhorns': -1, 'TCU Horned Frogs': -1,
    'Oklahoma State Cowboys': -1, 'BYU Cougars': -2, 'Arizona State Sun Devils': -3,
    'Arizona Wildcats': -3, 'West Virginia Mountaineers': 0,
    # College Baseball — West Coast
    'Stanford Cardinal': -3, 'Oregon State Beavers': -3, 'Oregon Ducks': -3,
    'UCLA Bruins': -3, 'USC Trojans': -3, 'California Golden Bears': -3,
    # College Baseball — Big Ten
    'Michigan Wolverines': 0, 'Ohio State Buckeyes': 0, 'Indiana Hoosiers': 0,
    'Nebraska Cornhuskers': -1, 'Minnesota Golden Gophers': -1,
}


def travel_timezone_adjustment(home, away, commence, sport):
    """
    Compute travel/timezone fatigue adjustment.
    
    West coast team playing before 2pm EST = fatigue (-0.5 to -1.0 pts)
    Cross-country trip = -0.5 pts for visitor
    """
    if not commence:
        return 0.0, {}
    
    # Only applies to US sports
    if 'soccer' in sport and 'usa_mls' not in sport:
        return 0.0, {}
    
    try:
        game_time = datetime.fromisoformat(commence.replace('Z', '+00:00'))
        est_hour = (game_time - timedelta(hours=5)).hour
    except:
        return 0.0, {}
    
    adj = 0.0
    reasons = {}
    
    h_tz = TEAM_TIMEZONE.get(home, 0)
    a_tz = TEAM_TIMEZONE.get(away, 0)
    
    # Timezone difference
    tz_diff = abs(h_tz - a_tz)
    
    # Early game + west coast visitor = fatigue
    # If game is before 2pm EST and visitor is from Pacific time
    if est_hour < 14 and a_tz <= -3:
        body_hour = est_hour + a_tz  # e.g., noon EST = 9am Pacific
        if body_hour < 11:
            adj += 0.75  # Significant west coast disadvantage
            reasons['away_early_west'] = 0.75
        elif body_hour < 13:
            adj += 0.5
            reasons['away_early_west'] = 0.5
    
    # Same for home team going east
    if est_hour < 14 and h_tz <= -3:
        body_hour = est_hour + h_tz
        if body_hour < 11:
            adj -= 0.75
            reasons['home_early_west'] = -0.75
    
    # Cross-country trip fatigue (3+ timezone difference)
    if tz_diff >= 3:
        adj += 0.5  # Visitor disadvantage
        reasons['cross_country'] = 0.5
    
    return adj, reasons


# ═══════════════════════════════════════════════════════════════════
# 5. ALTITUDE
# ═══════════════════════════════════════════════════════════════════
# Denver (5,280ft): affects pace, shooting, fatigue
# NBA: ~1.0 pts on totals, ~0.5 on spread (home advantage)
# MLS: Same effect. Visitors gas out.
# Slight effect for other high-altitude venues.

ALTITUDE_VENUES = {
    # NBA — v17: halved total_adj (was 1.5/1.0), altitude totals were 1W-2L
    'Denver Nuggets': {'altitude': 5280, 'spread_adj': 0.5, 'total_adj': 0.75},
    'Utah Jazz': {'altitude': 4226, 'spread_adj': 0.3, 'total_adj': 0.5},
    # NHL
    'Colorado Avalanche': {'altitude': 5280, 'spread_adj': 0.0, 'total_adj': 0.3},
    # MLS — v17: halved total_adj
    'Colorado Rapids': {'altitude': 5280, 'spread_adj': 0.5, 'total_adj': 0.5},
    'Real Salt Lake': {'altitude': 4226, 'spread_adj': 0.3, 'total_adj': 0.25},
}


def altitude_adjustment(home, market_type):
    """
    Altitude advantage for home team at high-altitude venues.
    
    Returns spread adjustment (positive = home advantage).
    For totals, returns a total adjustment (positive = higher scoring).
    """
    venue = ALTITUDE_VENUES.get(home)
    if not venue:
        return 0.0, {}
    
    if market_type in ('SPREAD', 'MONEYLINE'):
        return venue['spread_adj'], {'altitude': venue['spread_adj']}
    elif market_type == 'TOTAL':
        return venue['total_adj'], {'altitude_total': venue['total_adj']}
    
    return 0.0, {}


# ═══════════════════════════════════════════════════════════════════
# 6. MOTIVATION / SCHEDULE SPOT
# ═══════════════════════════════════════════════════════════════════
# Post-rivalry letdown, conference tournament bubble pressure,
# teams with nothing to play for at end of season.
# Hard to quantify precisely — use conservative adjustments.

def _last_game_result(conn, team, sport, before_date):
    """Get team's last game result: margin and opponent."""
    last = conn.execute("""
        SELECT home, away, actual_margin FROM results
        WHERE (home = ? OR away = ?) AND sport = ? AND completed = 1
        AND commence_time < ?
        ORDER BY commence_time DESC LIMIT 1
    """, (team, team, sport, before_date)).fetchone()
    
    if not last or last[2] is None:
        return None, None, None
    
    # Margin from team's perspective (positive = team won)
    if last[0] == team:
        team_margin = last[2]
        opponent = last[1]
    else:
        team_margin = -last[2]
        opponent = last[0]
    
    return team_margin, opponent, abs(last[2]) <= 3


def _season_h2h_revenge(conn, team, opponent, sport, before_date):
    """
    Check if team lost badly to this specific opponent earlier this season.
    Walters: revenge games have measurable value — teams remember blowout losses.
    
    Returns the worst loss margin (from team's perspective) against this opponent
    this season, or None if no prior meeting or no blowout.
    """
    # Look for prior meetings this season (last 6 months)
    from datetime import timedelta
    season_start = (datetime.fromisoformat(before_date.replace('Z', '+00:00')) - timedelta(days=180)).isoformat()
    
    meetings = conn.execute("""
        SELECT home, away, actual_margin FROM results
        WHERE sport = ? AND completed = 1
        AND ((home = ? AND away = ?) OR (home = ? AND away = ?))
        AND commence_time >= ? AND commence_time < ?
        ORDER BY commence_time DESC
    """, (sport, team, opponent, opponent, team, season_start, before_date)).fetchall()
    
    if not meetings:
        return None
    
    worst_loss = 0
    for m in meetings:
        if m[2] is None:
            continue
        # Margin from team's perspective
        margin = m[2] if m[0] == team else -m[2]
        if margin < worst_loss:
            worst_loss = margin
    
    return worst_loss if worst_loss < -10 else None  # Only flag if lost by 10+


def motivation_adjustment(conn, home, away, sport, commence):
    """
    Detect motivational edges (Walters p.240-252):
    
    v12.2: Calendar-aware — skips bounce-back during tournaments/breaks.
    """
    if not commence:
        return 0.0, {}
    
    freeze = _get_context_freeze(sport, commence)
    
    adj = 0.0
    reasons = {}
    
    # ── 1. LETDOWN SPOT ──
    h_margin, h_opp, h_tight = _last_game_result(conn, home, sport, commence)
    a_margin, a_opp, a_tight = _last_game_result(conn, away, sport, commence)
    
    if h_tight and not a_tight:
        adj -= 0.25  # Home team in letdown spot (v12: was 0.5, 1-3 -10.9u record)
        reasons['home_letdown'] = -0.25
    elif a_tight and not h_tight:
        adj += 0.25  # Away team in letdown spot
        reasons['away_letdown'] = 0.25
    
    # ── 2. BOUNCE-BACK (Walters p.252) ──
    # Walters NFL: +2 after 19pt loss, +4 after 29pt loss
    # Adapt thresholds per sport's scoring scale
    # v17: Raised guard to boost >= 1.5 — small bounce-backs (1.0) were 0W-3L
    # at the +0.8/+1.0 level. Only severe blowout bounce-backs are profitable.
    # v18: Removed sub-1.5 tiers — they were always blocked by the
    # boost >= 1.5 guard below, but their existence let the loop match
    # a margin (e.g. hockey -4) and break before any adjustment was
    # applied, creating dead-code confusion.  Now only severe blowouts
    # remain, each with boost >= 1.5 so the guard always passes.
    if 'basketball' in sport:
        bounce_thresholds = [(-29, 2.0)]                 # Basketball: 29pt loss = severe
    elif 'soccer' in sport:
        bounce_thresholds = [(-4, 1.5)]                  # Soccer: 4-goal loss = blowout
    elif 'icehockey' in sport:
        bounce_thresholds = [(-5, 1.5)]                  # Hockey: 5-goal loss = blowout
    elif 'baseball' in sport:
        bounce_thresholds = [(-10, 1.5)]                 # Baseball: 10-run loss = ugly
    else:
        bounce_thresholds = [(-29, 2.0)]

    if h_margin is not None:
        for threshold, boost in bounce_thresholds:
            if h_margin <= threshold:
                if boost >= 1.5 and 'home_bounceback' not in freeze:
                    adj += boost
                    reasons['home_bounceback'] = boost
                break

    if a_margin is not None:
        for threshold, boost in bounce_thresholds:
            if a_margin <= threshold:
                if boost >= 1.5 and 'away_bounceback' not in freeze:
                    adj -= boost
                    reasons['away_bounceback'] = -boost
                break
    
    # ── 3. REVENGE FACTOR ──
    # Team facing opponent that blew them out earlier this season
    # v17: Raised basketball threshold from -10 to -20. The -1.0 revenge adj
    # (loss by 10-19) was 2W-4L (33%). Only blowout revenge (-1.5, loss by 20+)
    # is profitable at 3W-2L. Other sports: removed small revenge entirely.
    h_revenge = _season_h2h_revenge(conn, home, away, sport, commence)
    a_revenge = _season_h2h_revenge(conn, away, home, sport, commence)

    if 'basketball' in sport:
        revenge_thresholds = [(-20, 1.5)]               # Only blowout revenge (20+ pt loss)
    elif 'soccer' in sport:
        revenge_thresholds = [(-3, 1.0)]                # Only 3+ goal blowout
    elif 'icehockey' in sport:
        revenge_thresholds = [(-4, 1.0)]                # Only 4+ goal blowout
    elif 'baseball' in sport:
        revenge_thresholds = [(-7, 1.0)]                # Only 7+ run blowout
    else:
        revenge_thresholds = [(-20, 1.5)]

    if h_revenge is not None:
        for threshold, boost in revenge_thresholds:
            if h_revenge <= threshold:
                adj += boost  # Home team has revenge motive
                reasons['home_revenge'] = boost
                break

    if a_revenge is not None:
        for threshold, boost in revenge_thresholds:
            if a_revenge <= threshold:
                adj -= boost  # Away team has revenge motive
                reasons['away_revenge'] = -boost
                break
    
    return adj, reasons


# ═══════════════════════════════════════════════════════════════════
# 7a. BASEBALL SERIES CONTEXT
# ═══════════════════════════════════════════════════════════════════
# College baseball plays 3-game weekend series (Fri-Sat-Sun) and
# occasional midweek games. Series position matters for context.

def _series_context(conn, home, away, sport, commence):
    """Detect if this game is part of a series and return context.

    College baseball plays 3-game weekend series (Fri-Sat-Sun) and
    occasional midweek games. Series position matters:
    - Game 2: loser of game 1 adjusts, creates tighter game
    - Game 3 (rubber match): highest intensity, even matchups
    - Game 3 (sweep attempt): trailing team may rest starters
    """
    # Find recent games between these exact teams (last 4 days)
    recent = conn.execute("""
        SELECT home, away, actual_margin, commence_time
        FROM results
        WHERE sport=? AND completed=1
        AND ((home=? AND away=?) OR (home=? AND away=?))
        AND commence_time >= datetime(?, '-4 days')
        AND commence_time < ?
        ORDER BY commence_time DESC
    """, (sport, home, away, away, home, commence, commence)).fetchall()

    if not recent:
        return 0, None, {}

    series_len = len(recent)  # How many games already played in this series

    # Track series score from home team's perspective
    home_wins = 0
    away_wins = 0
    last_margin = None
    for r in recent:
        margin = r[2] if r[0] == home else -r[2]
        if margin is not None:
            if margin > 0:
                home_wins += 1
            elif margin < 0:
                away_wins += 1
            if last_margin is None:
                last_margin = margin  # Most recent game

    adj = 0
    reasons = {}

    if series_len >= 1:
        # Game 2+: adjustment based on yesterday's result
        if last_margin is not None and abs(last_margin) >= 7:
            # Blowout in last game — loser may bounce back, winner may let up
            if last_margin > 0:
                # Home won big yesterday
                adj -= 0.3  # Home letdown, away bounce-back
                reasons['series_blowout_adj'] = -0.3
            else:
                # Away won big yesterday
                adj += 0.3
                reasons['series_blowout_adj'] = 0.3

    if series_len >= 2:
        # Game 3: rubber match or sweep attempt
        if home_wins == 1 and away_wins == 1:
            # Rubber match — tighter game, slight under lean
            reasons['rubber_match'] = 0  # No spread adj, but flag for totals
        elif home_wins == 2 or away_wins == 2:
            # Sweep attempt — trailing team may rest starters
            if home_wins == 0:
                adj -= 0.5  # Home down 0-2, may rest
                reasons['sweep_attempt_home'] = -0.5
            elif away_wins == 0:
                adj += 0.5  # Away down 0-2, may rest
                reasons['sweep_attempt_away'] = 0.5

    series_desc = f"Game {series_len + 1} of series"
    if series_len >= 2 and home_wins == 1 and away_wins == 1:
        series_desc = "Rubber match (1-1)"

    return adj, series_desc, reasons


# ═══════════════════════════════════════════════════════════════════
# 7b. PACE OF PLAY (affects TOTALS)
# ═══════════════════════════════════════════════════════════════════
# The totals model currently averages market totals — which is circular.
# Pace measures how fast teams actually play by looking at REAL scored
# totals from the results table. Two fast teams → total should be higher.
# Two slow teams → total should be lower.
#
# NBA: Pacers (fast) average 235+ totals. Knicks (slow) average 210-.
#      When Pacers play Knicks, the market total often splits the
#      difference, but the actual pace-adjusted expectation is different.
#
# Soccer: Teams that average 3.5+ goals are "attacking". Teams at 1.5-
#         are "defensive". This directly affects over/under value.

def pace_of_play_adjustment(conn, home, away, sport):
    """
    Compute total adjustment based on team pace (actual scoring history).
    
    Compares each team's average game total against the league average.
    If both teams are fast (high-scoring), the total should go UP.
    If both teams are slow (low-scoring), the total should go DOWN.
    If one is fast and one is slow, effects mostly cancel out.
    
    Returns: (total_adj, reasons_dict)
    """
    # League average total from actual results
    league = conn.execute("""
        SELECT AVG(actual_total), COUNT(*)
        FROM results
        WHERE sport = ? AND completed = 1 AND actual_total IS NOT NULL
    """, (sport,)).fetchone()
    
    league_avg = league[0] if league and league[0] else None
    league_count = league[1] if league else 0
    
    if not league_avg or league_count < 20:
        return 0.0, {}
    
    # Compute standard deviation manually (SQLite has no STDEV)
    variance_row = conn.execute("""
        SELECT AVG((actual_total - ?) * (actual_total - ?))
        FROM results
        WHERE sport = ? AND completed = 1 AND actual_total IS NOT NULL
    """, (league_avg, league_avg, sport)).fetchone()
    
    league_stdev = (variance_row[0] ** 0.5) if variance_row and variance_row[0] else 10.0
    
    # v12.3: Use LAST 5 GAMES only for pace context (recent form).
    # The model total already uses season-long recency-weighted data,
    # so context should capture what the team is doing RIGHT NOW.
    h_rows = conn.execute("""
        SELECT actual_total FROM results
        WHERE (home = ? OR away = ?) AND sport = ? AND completed = 1
        AND actual_total IS NOT NULL
        ORDER BY commence_time DESC LIMIT 5
    """, (home, home, sport)).fetchall()

    a_rows = conn.execute("""
        SELECT actual_total FROM results
        WHERE (home = ? OR away = ?) AND sport = ? AND completed = 1
        AND actual_total IS NOT NULL
        ORDER BY commence_time DESC LIMIT 5
    """, (away, away, sport)).fetchall()

    h_avg = sum(r[0] for r in h_rows) / len(h_rows) if len(h_rows) >= 4 else None
    a_avg = sum(r[0] for r in a_rows) / len(a_rows) if len(a_rows) >= 4 else None
    
    if not h_avg and not a_avg:
        return 0.0, {}
    
    adj = 0.0
    reasons = {}
    
    # Each team's deviation from league average
    if h_avg:
        h_pace = (h_avg - league_avg) / league_stdev  # Z-score
        if abs(h_pace) >= 0.5:  # At least half a stdev from average
            # Convert to points: each 1 stdev = ~2 pts adjustment (split between teams)
            h_adj = h_pace * 1.0  # Half of 2 pts since two teams contribute
            adj += h_adj
            reasons['home_pace'] = round(h_adj, 1)
    
    if a_avg:
        a_pace = (a_avg - league_avg) / league_stdev
        if abs(a_pace) >= 0.5:
            a_adj = a_pace * 1.0
            adj += a_adj
            reasons['away_pace'] = round(a_adj, 1)
    
    # Cap at ±3.0
    adj = max(-3.0, min(3.0, adj))
    
    return round(adj, 1), reasons


# ═══════════════════════════════════════════════════════════════════
# 8. HEAD-TO-HEAD HISTORY
# ═══════════════════════════════════════════════════════════════════
# Some matchups are consistently high or low scoring regardless of
# team-level tendencies. Duke-UNC always goes under. Certain NBA
# rivalries always go over due to competitive intensity.
#
# Also useful for spreads: if Team A has beaten Team B by 10+ in
# their last 3 meetings, the market often doesn't fully price
# the matchup-specific dominance.

def head_to_head_adjustment(conn, home, away, sport):
    """
    Look at recent head-to-head history between these specific teams.
    
    Returns: (spread_adj, total_adj, reasons_dict)
    """
    # Get H2H results (both home/away combinations)
    h2h = conn.execute("""
        SELECT home, away, actual_margin, actual_total
        FROM results
        WHERE sport = ? AND completed = 1
        AND ((home = ? AND away = ?) OR (home = ? AND away = ?))
        ORDER BY commence_time DESC
        LIMIT 6
    """, (sport, home, away, away, home)).fetchall()
    
    if len(h2h) < 2:
        return 0.0, 0.0, {}
    
    spread_adj = 0.0
    total_adj = 0.0
    reasons = {}
    
    # Compute average margin from home's perspective
    margins = []
    totals = []
    for r in h2h:
        if r[2] is not None:
            # If home team in this result == our home team, use margin as-is
            # Otherwise flip it
            m = r[2] if r[0] == home else -r[2]
            margins.append(m)
        if r[3] is not None:
            totals.append(r[3])
    
    # H2H margin: if home consistently dominates this matchup
    if len(margins) >= 2:
        avg_margin = sum(margins) / len(margins)
        if abs(avg_margin) >= 5.0:
            # Strong dominance pattern — adjust spread slightly
            adj = max(-1.0, min(1.0, avg_margin * 0.1))
            spread_adj = round(adj, 1)
            reasons['h2h_margin'] = spread_adj
    
    # H2H totals: if this matchup consistently goes over or under
    if len(totals) >= 3:
        # Compare H2H average total to league average
        league = conn.execute("""
            SELECT AVG(actual_total) FROM results
            WHERE sport = ? AND completed = 1 AND actual_total IS NOT NULL
        """, (sport,)).fetchone()
        
        if league and league[0]:
            h2h_avg_total = sum(totals) / len(totals)
            diff = h2h_avg_total - league[0]
            if abs(diff) >= 5.0:  # At least 5 pts different from league avg
                adj = max(-2.0, min(2.0, diff * 0.2))
                total_adj = round(adj, 1)
                direction = 'high-scoring' if adj > 0 else 'low-scoring'
                reasons['h2h_total'] = total_adj
    
    return spread_adj, total_adj, reasons


# ═══════════════════════════════════════════════════════════════════
# 9. NBA/NHL REFEREE TENDENCIES
# ═══════════════════════════════════════════════════════════════════
# NBA refs have documented, persistent tendencies:
#   - Some refs call 20% more fouls → more free throws → higher totals
#   - Some refs "let them play" → fewer stoppages → lower totals
#   - Some refs have consistent home/away bias
#
# Data source: ref assignments announced ~1hr before tipoff on NBA.com
# Since assignments come late, this works best with the 5:30pm run.
#
# USAGE:
#   1. Before tip, check NBA.com for ref crew
#   2. Run: python context_engine.py --refs "Tony Brothers,Scott Foster" --game "LAL@BOS"
#   3. Or set refs in the DB: INSERT INTO ref_assignments (...)
#
# The lookup table below is based on publicly tracked referee stats.
# Over/under: positive = tends to push totals higher. Negative = lower.
# Home bias: positive = home team gets more calls.

# NBA referee tendencies (total_adj, home_bias)
# Sources: NBARefStats.com, Covers.com
# Values represent average deviation from league average total
NBA_REF_TENDENCIES = {
    # Refs that push totals HIGHER (more fouls, more FTs)
    'Tony Brothers':    {'total_adj': +2.5, 'home_bias': +0.3, 'style': 'whistle-happy'},
    'Scott Foster':     {'total_adj': +1.5, 'home_bias': +0.0, 'style': 'high-foul'},
    'Kane Fitzgerald':  {'total_adj': +1.5, 'home_bias': +0.2, 'style': 'high-foul'},
    'Courtney Kirkland':{'total_adj': +1.0, 'home_bias': +0.3, 'style': 'moderate'},
    'Marc Davis':       {'total_adj': +1.0, 'home_bias': +0.0, 'style': 'moderate'},
    'Zach Zarba':       {'total_adj': +0.5, 'home_bias': +0.2, 'style': 'moderate'},
    
    # Refs that push totals LOWER (fewer fouls, let them play)
    'Ed Malloy':        {'total_adj': -1.5, 'home_bias': +0.0, 'style': 'swallow-whistle'},
    'Josh Tiven':       {'total_adj': -1.0, 'home_bias': -0.2, 'style': 'swallow-whistle'},
    'John Goble':       {'total_adj': -1.0, 'home_bias': +0.0, 'style': 'low-foul'},
    'Bennie Adams':     {'total_adj': -0.5, 'home_bias': +0.0, 'style': 'moderate'},
    'Sean Wright':      {'total_adj': -1.0, 'home_bias': +0.0, 'style': 'low-foul'},
    'Bill Kennedy':     {'total_adj': -0.5, 'home_bias': +0.0, 'style': 'moderate'},
    
    # Neutral refs
    'James Capers':     {'total_adj': +0.0, 'home_bias': +0.0, 'style': 'neutral'},
    'David Guthrie':    {'total_adj': +0.0, 'home_bias': +0.0, 'style': 'neutral'},
    'Eric Lewis':       {'total_adj': +0.0, 'home_bias': +0.0, 'style': 'neutral'},
}

# NHL referee tendencies (penalty-minutes per game deviation)
NHL_REF_TENDENCIES = {
    'Wes McCauley':     {'total_adj': +0.2, 'penalty_rate': 'high', 'style': 'showman'},
    'Chris Rooney':     {'total_adj': +0.3, 'penalty_rate': 'high', 'style': 'whistle-happy'},
    'Frederick L\'Ecuyer':{'total_adj': -0.2, 'penalty_rate': 'low', 'style': 'lenient'},
    'Dan O\'Rourke':    {'total_adj': +0.1, 'penalty_rate': 'moderate', 'style': 'neutral'},
    'Chris Lee':        {'total_adj': -0.1, 'penalty_rate': 'low', 'style': 'lenient'},
}


# ═══════════════════════════════════════════════════════════════════
# SOCCER DERBY / RIVALRY DETECTION
# ═══════════════════════════════════════════════════════════════════
# Derbies are structurally different from normal matches:
#   - Players raise intensity → more goals, more cards, more chaos
#   - Home advantage shrinks — away fans travel in numbers, both sides fired up
#   - Favorites underperform ATS — emotional leveler, underdogs fight harder
#   - Totals push slightly higher in MAJOR derbies (end-to-end tempo)
#
# frozenset keys so home/away order doesn't matter.
# spread_tighten: how much to reduce model's favorite confidence (toward 0)
# total_bump: small upward push on totals for intense derbies

SOCCER_DERBIES = {
    # ── EPL ──
    frozenset(['Manchester United', 'Manchester City']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.15, 'name': 'Manchester Derby'},
    frozenset(['Liverpool', 'Everton']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'Merseyside Derby'},
    frozenset(['Arsenal', 'Tottenham']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'North London Derby'},
    frozenset(['Chelsea', 'Arsenal']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'London Derby'},
    frozenset(['Chelsea', 'Tottenham']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'London Derby'},
    frozenset(['Liverpool', 'Manchester United']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'Northwest Derby'},
    frozenset(['Manchester City', 'Liverpool']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'Title Rivalry'},
    frozenset(['Newcastle', 'Sunderland']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.10, 'name': 'Tyne-Wear Derby'},
    frozenset(['West Ham', 'Tottenham']):
        {'intensity': 'MINOR', 'spread_tighten': 0.10, 'total_bump': 0.0, 'name': 'London Derby'},
    frozenset(['Aston Villa', 'Wolverhampton']):
        {'intensity': 'MINOR', 'spread_tighten': 0.10, 'total_bump': 0.0, 'name': 'West Midlands Derby'},

    # ── La Liga ──
    frozenset(['Real Madrid', 'Barcelona']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'El Clasico'},
    frozenset(['Atletico Madrid', 'Real Madrid']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'Madrid Derby'},
    frozenset(['Real Betis', 'Sevilla']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'Seville Derby'},
    frozenset(['Valencia', 'Villarreal']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'Comunitat Derby'},
    frozenset(['Athletic Bilbao', 'Real Sociedad']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'Basque Derby'},

    # ── Serie A ──
    frozenset(['AC Milan', 'Inter Milan']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'Derby della Madonnina'},
    frozenset(['Juventus', 'Inter Milan']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': "Derby d'Italia"},
    frozenset(['Roma', 'Lazio']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'Derby della Capitale'},
    frozenset(['Napoli', 'Juventus']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'Derby del Sole'},
    frozenset(['Fiorentina', 'Juventus']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'Historic Rivalry'},
    frozenset(['AC Milan', 'Juventus']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'Historic Rivalry'},
    frozenset(['Napoli', 'Roma']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'Derby del Sud'},

    # ── Bundesliga ──
    frozenset(['Bayern Munich', 'Borussia Dortmund']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'Der Klassiker'},
    frozenset(['Schalke 04', 'Borussia Dortmund']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'Revierderby'},
    frozenset(['Hamburg', 'Werder Bremen']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'Nordderby'},
    frozenset(['RB Leipzig', 'Bayern Munich']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'Topspiel'},
    frozenset(['Eintracht Frankfurt', 'Bayern Munich']):
        {'intensity': 'MINOR', 'spread_tighten': 0.10, 'total_bump': 0.0, 'name': 'Bundesliga Rivalry'},

    # ── Ligue 1 ──
    frozenset(['Paris Saint-Germain', 'Marseille']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'Le Classique'},
    frozenset(['Lyon', 'Saint-Etienne']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'Derby Rhone-Alpes'},
    frozenset(['Monaco', 'Nice']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'Cote d\'Azur Derby'},
    frozenset(['Lens', 'Lille']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'Derby du Nord'},
    frozenset(['Lyon', 'Marseille']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'Choc des Olympiques'},

    # ── MLS ──
    frozenset(['LA Galaxy', 'LAFC']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'El Trafico'},
    frozenset(['NY Red Bulls', 'New York City FC']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.25, 'total_bump': 0.10, 'name': 'Hudson River Derby'},
    frozenset(['Portland Timbers', 'Seattle Sounders']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.30, 'total_bump': 0.15, 'name': 'Cascadia Derby'},
    frozenset(['Atlanta United', 'Orlando City']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'Southern Derby'},
    frozenset(['Inter Miami', 'Orlando City']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'Florida Derby'},

    # ── UCL Cross-League Rivalries ──
    frozenset(['Real Madrid', 'Liverpool']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.20, 'total_bump': 0.10, 'name': 'UCL Rivalry'},
    frozenset(['Barcelona', 'Paris Saint-Germain']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'UCL Rivalry'},
    frozenset(['Bayern Munich', 'Real Madrid']):
        {'intensity': 'MAJOR', 'spread_tighten': 0.20, 'total_bump': 0.10, 'name': 'UCL Rivalry'},
    frozenset(['Manchester City', 'Real Madrid']):
        {'intensity': 'MINOR', 'spread_tighten': 0.15, 'total_bump': 0.0, 'name': 'UCL Rivalry'},
}

# Fuzzy matching keywords: map common fragments to canonical names
# ESPN and Odds API use different formats — this bridges the gap
_SOCCER_TEAM_ALIASES = {
    # EPL
    'man utd':          'Manchester United',
    'man united':       'Manchester United',
    'manchester utd':   'Manchester United',
    'manchester united':'Manchester United',
    'man city':         'Manchester City',
    'manchester city':  'Manchester City',
    'liverpool':        'Liverpool',
    'everton':          'Everton',
    'arsenal':          'Arsenal',
    'tottenham':        'Tottenham',
    'spurs':            'Tottenham',
    'chelsea':          'Chelsea',
    'newcastle':        'Newcastle',
    'newcastle united': 'Newcastle',
    'sunderland':       'Sunderland',
    'west ham':         'West Ham',
    'west ham united':  'West Ham',
    'aston villa':      'Aston Villa',
    'wolverhampton':    'Wolverhampton',
    'wolves':           'Wolverhampton',
    'wolverhampton wanderers': 'Wolverhampton',
    # La Liga
    'real madrid':      'Real Madrid',
    'barcelona':        'Barcelona',
    'barca':            'Barcelona',
    'atletico madrid':  'Atletico Madrid',
    'atletico':         'Atletico Madrid',
    'atl madrid':       'Atletico Madrid',
    'real betis':       'Real Betis',
    'betis':            'Real Betis',
    'sevilla':          'Sevilla',
    'valencia':         'Valencia',
    'villarreal':       'Villarreal',
    'athletic bilbao':  'Athletic Bilbao',
    'athletic club':    'Athletic Bilbao',
    'real sociedad':    'Real Sociedad',
    # Serie A
    'ac milan':         'AC Milan',
    'milan':            'AC Milan',
    'inter milan':      'Inter Milan',
    'inter':            'Inter Milan',
    'internazionale':   'Inter Milan',
    'juventus':         'Juventus',
    'juve':             'Juventus',
    'roma':             'Roma',
    'as roma':          'Roma',
    'lazio':            'Lazio',
    'napoli':           'Napoli',
    'fiorentina':       'Fiorentina',
    # Bundesliga
    'bayern munich':    'Bayern Munich',
    'bayern':           'Bayern Munich',
    'bayern munchen':   'Bayern Munich',
    'borussia dortmund':'Borussia Dortmund',
    'dortmund':         'Borussia Dortmund',
    'bvb':              'Borussia Dortmund',
    'schalke 04':       'Schalke 04',
    'schalke':          'Schalke 04',
    'hamburg':          'Hamburg',
    'hamburger sv':     'Hamburg',
    'werder bremen':    'Werder Bremen',
    'bremen':           'Werder Bremen',
    'rb leipzig':       'RB Leipzig',
    'leipzig':          'RB Leipzig',
    'eintracht frankfurt': 'Eintracht Frankfurt',
    'frankfurt':        'Eintracht Frankfurt',
    # Ligue 1
    'paris saint-germain': 'Paris Saint-Germain',
    'paris saint germain': 'Paris Saint-Germain',
    'psg':              'Paris Saint-Germain',
    'paris sg':         'Paris Saint-Germain',
    'marseille':        'Marseille',
    'olympique marseille': 'Marseille',
    'om':               'Marseille',
    'lyon':             'Lyon',
    'olympique lyonnais': 'Lyon',
    'olympique lyon':   'Lyon',
    'saint-etienne':    'Saint-Etienne',
    'saint etienne':    'Saint-Etienne',
    'st etienne':       'Saint-Etienne',
    'monaco':           'Monaco',
    'as monaco':        'Monaco',
    'nice':             'Nice',
    'ogc nice':         'Nice',
    'lens':             'Lens',
    'rc lens':          'Lens',
    'lille':            'Lille',
    'losc lille':       'Lille',
    'losc':             'Lille',
    # MLS
    'la galaxy':        'LA Galaxy',
    'los angeles galaxy': 'LA Galaxy',
    'lafc':             'LAFC',
    'los angeles fc':   'LAFC',
    'los angeles football club': 'LAFC',
    'ny red bulls':     'NY Red Bulls',
    'new york red bulls': 'NY Red Bulls',
    'red bulls':        'NY Red Bulls',
    'new york city fc': 'New York City FC',
    'nycfc':            'New York City FC',
    'nyc fc':           'New York City FC',
    'portland timbers': 'Portland Timbers',
    'timbers':          'Portland Timbers',
    'seattle sounders': 'Seattle Sounders',
    'sounders':         'Seattle Sounders',
    'seattle sounders fc': 'Seattle Sounders',
    'atlanta united':   'Atlanta United',
    'atlanta united fc':'Atlanta United',
    'orlando city':     'Orlando City',
    'orlando city sc':  'Orlando City',
    'inter miami':      'Inter Miami',
    'inter miami cf':   'Inter Miami',
}


def _resolve_soccer_team(name):
    """
    Resolve a team name to its canonical form using fuzzy alias matching.

    Tries exact lowercase match first, then checks if any alias key is
    contained within the team name (or vice versa) to handle cases like
    "Manchester United FC" matching "manchester united".
    """
    lower = name.strip().lower()

    # 1. Exact alias match
    if lower in _SOCCER_TEAM_ALIASES:
        return _SOCCER_TEAM_ALIASES[lower]

    # 2. Check if any alias key is a substring of the input (or vice versa)
    #    Sort by length descending so longer (more specific) aliases match first.
    #    Require alias length >= 5 to avoid false positives from short aliases
    #    like "om", "nice", "inter" matching unrelated team names.
    for alias in sorted(_SOCCER_TEAM_ALIASES, key=len, reverse=True):
        if len(alias) < 5:
            continue  # Skip very short aliases for substring matching
        if alias in lower or lower in alias:
            return _SOCCER_TEAM_ALIASES[alias]

    # 3. No match — return original name stripped
    return name.strip()


def derby_adjustment(conn, home, away, sport):
    """
    Detect if a soccer match is a known derby/rivalry.

    Derbies tighten the spread toward zero (favorites underperform ATS)
    and MAJOR derbies get a small total bump (higher intensity → more goals).

    Args:
        conn: sqlite3 connection (unused currently, reserved for future DB lookups)
        home: home team name (any format — fuzzy matched)
        away: away team name (any format — fuzzy matched)
        sport: sport key string

    Returns:
        (spread_adj, total_adj, info_dict)
        spread_adj: negative value to tighten spread toward zero
                    (applied as: model_spread -= spread_adj, so negative
                     means the spread moves toward 0 / less confident)
        total_adj:  small positive bump for MAJOR derbies
        info_dict:  details for logging/display
    """
    # Only applies to soccer
    if 'soccer' not in sport:
        return 0.0, 0.0, {}

    # Resolve team names to canonical forms
    canon_home = _resolve_soccer_team(home)
    canon_away = _resolve_soccer_team(away)

    matchup = frozenset([canon_home, canon_away])

    derby = SOCCER_DERBIES.get(matchup)
    if not derby:
        return 0.0, 0.0, {}

    intensity = derby['intensity']
    spread_tighten = derby['spread_tighten']
    total_bump = derby.get('total_bump', 0.0)
    derby_name = derby.get('name', 'Derby')

    # spread_adj is negative: it reduces the model's confidence in the favorite
    # by pushing the spread toward zero. The caller does model_spread -= spread_adj,
    # so a negative spread_adj makes model_spread less negative (closer to 0).
    spread_adj = -spread_tighten

    # total_adj is positive for MAJOR derbies (higher intensity → more goals)
    total_adj = total_bump if intensity == 'MAJOR' else 0.0

    info = {
        'derby_name': derby_name,
        'intensity': intensity,
        'spread_tighten': spread_tighten,
        'total_bump': total_adj,
        'matched_home': canon_home,
        'matched_away': canon_away,
    }

    return round(spread_adj, 2), round(total_adj, 2), info


# ═══════════════════════════════════════════════════════════════════
# SOCCER-SPECIFIC CONTEXT MODULES (not yet wired into get_context_adjustments)
# ═══════════════════════════════════════════════════════════════════

def ucl_rotation_adjustment(conn, home, away, sport, commence_time):
    """
    Detect if either team has an upcoming European (UCL/Europa) fixture
    within 5 days. Domestic league teams facing midweek European matches
    often rotate squads, weakening their domestic lineup.

    Only fires for domestic soccer leagues — NOT for UCL/Europa games themselves.

    Returns:
        (spread_adj, info_dict)
        spread_adj: negative = weakens home, positive = weakens away
    """
    try:
        if 'soccer' not in sport:
            return 0.0, {}
        # Don't apply to European competition matches themselves
        european_comps = ('soccer_uefa_champs_league', 'soccer_uefa_europa_league',
                         'soccer_uefa_europa_conference_league')
        if sport in european_comps:
            return 0.0, {}

        if isinstance(commence_time, str):
            commence_dt = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
        elif isinstance(commence_time, datetime):
            commence_dt = commence_time
        else:
            return 0.0, {}

        # Make commence_dt naive for comparison if needed
        if commence_dt.tzinfo is not None:
            commence_dt = commence_dt.replace(tzinfo=None)

        window_start = commence_dt.strftime('%Y-%m-%d')
        window_end = (commence_dt + timedelta(days=5)).strftime('%Y-%m-%d')

        spread_adj = 0.0
        info = {}

        for team, label in [(home, 'home'), (away, 'away')]:
            # Fuzzy match: first word + last word of team name
            parts = team.strip().split()
            like_pattern = f"%{parts[0]}%" if len(parts) == 1 else f"%{parts[0]}%{parts[-1]}%"

            # Check odds table for upcoming European fixtures
            has_euro = False
            days_until = None

            for comp in european_comps:
                rows = conn.execute("""
                    SELECT DISTINCT commence_time FROM odds
                    WHERE sport = ?
                      AND (home LIKE ? OR away LIKE ?)
                      AND commence_time >= ?
                      AND commence_time <= ?
                    ORDER BY commence_time ASC
                    LIMIT 1
                """, (comp, like_pattern, like_pattern,
                      window_start, window_end)).fetchall()

                if rows:
                    has_euro = True
                    try:
                        euro_dt = datetime.fromisoformat(
                            str(rows[0][0]).replace('Z', '+00:00')
                        ).replace(tzinfo=None)
                        days_until = (euro_dt - commence_dt).days
                    except Exception:
                        days_until = 3  # default to mid-range
                    break

            # Also check results table for scheduled but uncompleted
            if not has_euro:
                for comp in european_comps:
                    rows = conn.execute("""
                        SELECT commence_time FROM results
                        WHERE sport = ?
                          AND (home LIKE ? OR away LIKE ?)
                          AND completed = 0
                          AND commence_time >= ?
                          AND commence_time <= ?
                        ORDER BY commence_time ASC
                        LIMIT 1
                    """, (comp, like_pattern, like_pattern,
                          window_start, window_end)).fetchall()

                    if rows:
                        has_euro = True
                        try:
                            euro_dt = datetime.fromisoformat(
                                str(rows[0][0]).replace('Z', '+00:00')
                            ).replace(tzinfo=None)
                            days_until = (euro_dt - commence_dt).days
                        except Exception:
                            days_until = 3
                        break

            if has_euro and days_until is not None:
                if days_until <= 3:
                    penalty = 0.25
                else:
                    penalty = 0.10

                # Negative = weakens home, positive = weakens away
                if label == 'home':
                    spread_adj -= penalty
                else:
                    spread_adj += penalty

                info[f'{label}_euro_match'] = True
                info[f'{label}_days_until'] = days_until
                info[f'{label}_penalty'] = penalty

        if not info:
            return 0.0, {}

        return round(spread_adj, 2), info

    except Exception:
        return 0.0, {}


def fixture_congestion_adjustment(conn, home, away, sport):
    """
    Count games in the last 30 days for each team across ALL soccer leagues.
    Teams playing 8+ games in 30 days face fatigue / squad depth issues.

    Returns:
        (spread_adj, info_dict)
        spread_adj: net differential capped at +/-0.35
                    negative = weakens home, positive = weakens away
    """
    try:
        if 'soccer' not in sport:
            return 0.0, {}

        def _count_recent(team):
            """Count games in last 30 days using fuzzy name matching."""
            parts = team.strip().split()
            if len(parts) == 1:
                like_pattern = f"%{parts[0]}%"
            else:
                like_pattern = f"%{parts[0]}%{parts[-1]}%"

            row = conn.execute("""
                SELECT COUNT(*) FROM results
                WHERE sport LIKE 'soccer%'
                  AND (home LIKE ? OR away LIKE ?)
                  AND completed = 1
                  AND commence_time >= datetime('now', '-30 days')
            """, (like_pattern, like_pattern)).fetchone()
            return row[0] if row else 0

        home_games = _count_recent(home)
        away_games = _count_recent(away)

        def _fatigue_penalty(games):
            if games >= 10:
                return 0.30
            elif games >= 8:
                return 0.15
            return 0.0

        home_penalty = _fatigue_penalty(home_games)
        away_penalty = _fatigue_penalty(away_games)

        # Net differential: positive penalty for home = weakens home (negative adj)
        # positive penalty for away = weakens away (positive adj)
        spread_adj = away_penalty - home_penalty
        spread_adj = max(-0.35, min(0.35, spread_adj))

        if home_penalty == 0 and away_penalty == 0:
            return 0.0, {}

        info = {
            'home_games_30d': home_games,
            'away_games_30d': away_games,
            'home_fatigue_penalty': home_penalty,
            'away_fatigue_penalty': away_penalty,
        }

        return round(spread_adj, 2), info

    except Exception:
        return 0.0, {}


def soccer_home_away_adjustment(conn, home, away, sport):
    """
    Soccer-specific home/away splits wrapper. Computes goal margin
    differential between a team's home and away performances.

    If a team scores significantly more (or concedes less) at home vs away,
    the home advantage is real and should be reflected in the spread.

    Returns:
        (spread_adj, info_dict)
        spread_adj: positive = home stronger than ratings, capped at +/-0.30
    """
    try:
        if 'soccer' not in sport:
            return 0.0, {}

        def _goal_margins(team):
            """
            Calculate home and away goal margins for a team.
            Returns (home_margin, away_margin, home_games, away_games).
            home_margin = avg(home_score - away_score) when playing at home
            away_margin = avg(away_score - home_score) when playing away
            """
            rows = conn.execute("""
                SELECT home_score, away_score, home, away FROM results
                WHERE sport = ? AND completed = 1 AND (home = ? OR away = ?)
            """, (sport, team, team)).fetchall()

            home_margins = []
            away_margins = []

            for hs, as_, h, a in rows:
                if hs is None or as_ is None:
                    continue
                if h == team:
                    home_margins.append(hs - as_)
                elif a == team:
                    away_margins.append(as_ - hs)

            h_avg = sum(home_margins) / len(home_margins) if home_margins else 0.0
            a_avg = sum(away_margins) / len(away_margins) if away_margins else 0.0
            return h_avg, a_avg, len(home_margins), len(away_margins)

        h_home_margin, h_away_margin, h_hg, h_ag = _goal_margins(home)
        a_home_margin, a_away_margin, a_hg, a_ag = _goal_margins(away)

        # Need minimum sample: 3 home + 3 away games for each team
        if h_hg < 3 or h_ag < 3 or a_hg < 3 or a_ag < 3:
            return 0.0, {}

        # Home team: how much better at home vs away
        home_diff = h_home_margin - h_away_margin
        # Away team: how much worse away vs home
        away_diff = a_home_margin - a_away_margin

        # Combined: home team's home boost + away team's away deficit
        # If home_diff is large, home team overperforms at home
        # If away_diff is large, away team underperforms away from home
        combined_diff = (home_diff + away_diff) / 2.0

        if abs(combined_diff) < 0.5:
            return 0.0, {}

        spread_adj = 0.20 * combined_diff
        spread_adj = max(-0.30, min(0.30, spread_adj))

        info = {
            'home_team_home_margin': round(h_home_margin, 2),
            'home_team_away_margin': round(h_away_margin, 2),
            'home_team_split_diff': round(home_diff, 2),
            'away_team_home_margin': round(a_home_margin, 2),
            'away_team_away_margin': round(a_away_margin, 2),
            'away_team_split_diff': round(away_diff, 2),
            'combined_diff': round(combined_diff, 2),
        }

        return round(spread_adj, 2), info

    except Exception:
        return 0.0, {}


def ref_adjustment(conn, sport, event_id):
    """
    Look up referee assignment and compute total/spread adjustment.
    
    Checks ref_assignments table first (populated by user or scraper),
    then falls back to nothing if refs aren't set.
    
    Returns: (total_adj, spread_adj, reasons_dict)
    """
    # Check if ref_assignments table exists and has data
    try:
        refs_row = conn.execute("""
            SELECT ref_names FROM ref_assignments
            WHERE event_id = ?
            ORDER BY created_at DESC LIMIT 1
        """, (event_id,)).fetchone()
    except:
        # Table doesn't exist yet — that's fine
        return 0.0, 0.0, {}
    
    if not refs_row or not refs_row[0]:
        return 0.0, 0.0, {}
    
    ref_names = [r.strip() for r in refs_row[0].split(',')]
    
    # Pick the right lookup table
    if 'nba' in sport:
        tendencies = NBA_REF_TENDENCIES
    elif 'nhl' in sport:
        tendencies = NHL_REF_TENDENCIES
    else:
        return 0.0, 0.0, {}
    
    total_adj = 0.0
    spread_adj = 0.0
    matched_refs = []
    
    for ref in ref_names:
        if ref in tendencies:
            t = tendencies[ref]
            total_adj += t.get('total_adj', 0)
            spread_adj += t.get('home_bias', 0)
            matched_refs.append(f"{ref} ({t['style']})")
    
    if not matched_refs:
        return 0.0, 0.0, {}
    
    # Average across crew (typically 3 refs, lead ref matters most)
    num = len(matched_refs)
    total_adj = round(total_adj / num, 1) if num > 1 else total_adj
    spread_adj = round(spread_adj / num, 1) if num > 1 else spread_adj
    
    reasons = {
        'refs': ', '.join(matched_refs),
        'total_adj': total_adj,
        'spread_adj': spread_adj,
    }
    
    return total_adj, spread_adj, reasons


def ensure_ref_table(conn):
    """Create ref_assignments table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            sport TEXT NOT NULL,
            ref_names TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(event_id)
        )
    """)
    conn.commit()


def set_refs(event_id, sport, ref_names_csv):
    """
    Manually set refs for a game. Call before running the model.
    
    Usage:
        python context_engine.py --refs "Tony Brothers,Scott Foster,Kane Fitzgerald" --event abc123
    """
    import sqlite3 as _sq
    DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = _sq.connect(DB)
    ensure_ref_table(conn)
    conn.execute("""
        INSERT OR REPLACE INTO ref_assignments (event_id, sport, ref_names, created_at)
        VALUES (?, ?, ?, ?)
    """, (event_id, sport, ref_names_csv, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    print(f"  ✅ Set refs for {event_id}: {ref_names_csv}")


# ═══════════════════════════════════════════════════════════════════
# 10. CONFERENCE/DIVISION FAMILIARITY
# ═══════════════════════════════════════════════════════════════════
# Teams in the same conference/division play each other multiple times.
# These games tend to be closer (lower spread variance) because:
#   - Game-planning advantage from film study
#   - Emotional intensity of divisional matchups
#   - Teams adjust strategies based on previous meetings
#
# Effect: Divisional games trend toward UNDER and tighter spreads.

def _teams_played_recently(conn, home, away, sport, days=90):
    """Check how many times these teams have played in the last N days."""
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    row = conn.execute("""
        SELECT COUNT(*) FROM results
        WHERE sport = ? AND completed = 1
        AND ((home = ? AND away = ?) OR (home = ? AND away = ?))
        AND commence_time >= ?
    """, (sport, home, away, away, home, cutoff)).fetchone()
    return row[0] if row else 0


def familiarity_adjustment(conn, home, away, sport):
    """
    If teams have played each other 2+ times recently, they're
    likely in the same conference/division. Games tend to be tighter.
    
    Returns: (spread_adj, total_adj, reasons)
    """
    meetings = _teams_played_recently(conn, home, away, sport, days=120)
    
    if meetings < 2:
        return 0.0, 0.0, {}
    
    reasons = {}
    spread_adj = 0.0
    total_adj = 0.0
    
    if 'basketball' in sport:
        # Division familiarity tightens games slightly
        total_adj = -1.0  # Familiar opponents → more defensive adjustment
        reasons['familiarity'] = f"{meetings} meetings in 120 days"
    elif 'icehockey' in sport:
        total_adj = -0.3
        reasons['familiarity'] = f"{meetings} meetings"
    elif 'soccer' in sport:
        # Same league teams play 2x/season — built into ratings already
        pass
    
    return spread_adj, total_adj, reasons


# ═══════════════════════════════════════════════════════════════════
# 11. RECENT FORM — Hot/Cold Streaks
# ═══════════════════════════════════════════════════════════════════

def _recent_form_adjustment(conn, home, away, sport, commence):
    """
    Detect hot and cold streaks from recent results.
    
    A team winning 4+ of last 5 is 'hot' — momentum matters.
    A team losing 4+ of last 5 is 'cold' — market may not adjust fast enough.
    
    Adjustment: +0.5 to -0.5 per team (capped at ±1.0 combined).
    """
    adj = 0.0
    reasons = {}
    
    for team, side in [(home, 'home'), (away, 'away')]:
        recent = conn.execute("""
            SELECT home, home_score, away_score
            FROM results
            WHERE sport=? AND completed=1 AND (home=? OR away=?)
            ORDER BY commence_time DESC LIMIT 5
        """, (sport, team, team)).fetchall()
        
        if len(recent) < 4:
            continue
        
        wins = 0
        for r in recent:
            if r[0] == team:  # Home game
                if r[1] > r[2]: wins += 1
            else:  # Away game
                if r[2] > r[1]: wins += 1
        
        if wins >= 4:
            boost = 0.5 if side == 'home' else -0.5
            adj += boost
            reasons[f'{side}_hot'] = boost
        elif wins <= 1:
            boost = -0.5 if side == 'home' else 0.5
            adj += boost
            reasons[f'{side}_cold'] = boost
    
    return round(adj, 1), reasons


# ═══════════════════════════════════════════════════════════════════
# 12. SCORING TREND — Recent scoring vs season average
# ═══════════════════════════════════════════════════════════════════

def _scoring_trend_adjustment(conn, home, away, sport):
    """
    Compare each team's last 5 games scoring to their season average.
    
    If both teams are trending above their average, push total up.
    If both trending below, push total down.
    Only fires when the trend is significant (>10% deviation).
    
    Affects TOTALS only.
    """
    deviations = []
    
    for team in [home, away]:
        # Season average
        season = conn.execute("""
            SELECT AVG(CASE WHEN home=? THEN home_score + away_score
                            ELSE home_score + away_score END),
                   COUNT(*)
            FROM results
            WHERE sport=? AND completed=1 AND (home=? OR away=?)
        """, (team, sport, team, team)).fetchone()
        
        if not season or not season[0] or season[1] < 8:
            return 0.0, {}
        
        season_avg = season[0]
        
        # Last 5 games
        recent = conn.execute("""
            SELECT home_score + away_score
            FROM results
            WHERE sport=? AND completed=1 AND (home=? OR away=?)
            ORDER BY commence_time DESC LIMIT 5
        """, (sport, team, team)).fetchall()
        
        if len(recent) < 4:
            return 0.0, {}
        
        recent_avg = sum(r[0] for r in recent) / len(recent)
        pct_dev = (recent_avg - season_avg) / season_avg
        deviations.append(pct_dev)
    
    if len(deviations) != 2:
        return 0.0, {}
    
    # Both trending same direction and significantly (>10%)
    avg_dev = sum(deviations) / 2
    
    if abs(avg_dev) < 0.10:
        return 0.0, {}
    
    # Scale: 10% deviation = ±1.0 adjustment for basketball, ±0.3 for soccer/hockey
    if 'basketball' in sport:
        adj = round(avg_dev * 10.0, 1)  # 10% = 1.0 pts
        adj = max(-2.0, min(2.0, adj))
    elif 'soccer' in sport:
        adj = round(avg_dev * 3.0, 1)   # 10% = 0.3 goals
        adj = max(-0.5, min(0.5, adj))
    elif 'hockey' in sport:
        adj = round(avg_dev * 3.0, 1)   # 10% = 0.3 goals
        adj = max(-0.5, min(0.5, adj))
    else:
        adj = round(avg_dev * 5.0, 1)
        adj = max(-1.5, min(1.5, adj))
    
    return adj, {'home_dev': round(deviations[0], 3), 'away_dev': round(deviations[1], 3), 'adj': adj}


# ═══════════════════════════════════════════════════════════════════
# MASTER FUNCTION — Combine all adjustments
# ═══════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════
# TENNIS CONTEXT — surface, fatigue, H2H
# ═══════════════════════════════════════════════════════════════════

def tennis_context_adjustments(conn, player1, player2, sport, commence):
    """
    Tennis-specific context factors. Returns (spread_adj, info_dict).
    Positive adj = advantage for player1 (listed as "home").

    Factors:
    1. Fatigue — matches played in last 7 and 14 days
    2. Head-to-head record — direct meetings (very meaningful in tennis)
    3. Surface Elo gap vs overall Elo — detects surface specialists
    """
    adj = 0.0
    info = {}

    # 1. FATIGUE — recent match load
    # A player coming off a 5-set match or deep tournament run is fatigued
    if commence:
        p1_7d = _games_in_window(conn, player1, sport, commence, 7)
        p2_7d = _games_in_window(conn, player2, sport, commence, 7)
        # Also check across all tennis sports (player may have played another tournament)
        try:
            game_date = datetime.fromisoformat(commence.replace('Z', '+00:00'))
            window_7 = (game_date - timedelta(days=7)).isoformat()
            window_14 = (game_date - timedelta(days=14)).isoformat()
            p1_all_7 = conn.execute("""
                SELECT COUNT(*) FROM results
                WHERE (home = ? OR away = ?) AND sport LIKE 'tennis_%' AND completed = 1
                AND commence_time >= ? AND commence_time < ?
            """, (player1, player1, window_7, commence)).fetchone()[0]
            p2_all_7 = conn.execute("""
                SELECT COUNT(*) FROM results
                WHERE (home = ? OR away = ?) AND sport LIKE 'tennis_%' AND completed = 1
                AND commence_time >= ? AND commence_time < ?
            """, (player2, player2, window_7, commence)).fetchone()[0]
            p1_all_14 = conn.execute("""
                SELECT COUNT(*) FROM results
                WHERE (home = ? OR away = ?) AND sport LIKE 'tennis_%' AND completed = 1
                AND commence_time >= ? AND commence_time < ?
            """, (player1, player1, window_14, commence)).fetchone()[0]
            p2_all_14 = conn.execute("""
                SELECT COUNT(*) FROM results
                WHERE (home = ? OR away = ?) AND sport LIKE 'tennis_%' AND completed = 1
                AND commence_time >= ? AND commence_time < ?
            """, (player2, player2, window_14, commence)).fetchone()[0]
        except Exception:
            p1_all_7, p2_all_7 = p1_7d, p2_7d
            p1_all_14, p2_all_14 = 0, 0

        # Heavy load: 4+ matches in 7 days or 7+ in 14 days
        p1_fatigue = 0.0
        p2_fatigue = 0.0
        if p1_all_7 >= 5:
            p1_fatigue = -0.5
        elif p1_all_7 >= 4:
            p1_fatigue = -0.3
        if p1_all_14 >= 8:
            p1_fatigue -= 0.2

        if p2_all_7 >= 5:
            p2_fatigue = -0.5
        elif p2_all_7 >= 4:
            p2_fatigue = -0.3
        if p2_all_14 >= 8:
            p2_fatigue -= 0.2

        fatigue_diff = p2_fatigue - p1_fatigue  # Positive = P1 advantage (P2 more tired)
        if abs(fatigue_diff) >= 0.1:
            adj += fatigue_diff
            info['p1_fatigue'] = round(p1_fatigue, 2)
            info['p2_fatigue'] = round(p2_fatigue, 2)

    # 2. HEAD-TO-HEAD RECORD
    # Tennis H2H is very meaningful — same individuals playing repeatedly
    try:
        h2h = conn.execute("""
            SELECT
                SUM(CASE WHEN winner = ? THEN 1 ELSE 0 END),
                SUM(CASE WHEN winner = ? THEN 1 ELSE 0 END)
            FROM results
            WHERE ((home = ? AND away = ?) OR (home = ? AND away = ?))
            AND sport LIKE 'tennis_%' AND completed = 1
        """, (player1, player2, player1, player2, player2, player1)).fetchone()

        if h2h and h2h[0] is not None and h2h[1] is not None:
            p1_wins, p2_wins = int(h2h[0]), int(h2h[1])
            total_meetings = p1_wins + p2_wins
            if total_meetings >= 3:
                # Significant H2H: 5-1 = strong signal, 3-2 = weak signal
                win_rate = p1_wins / total_meetings
                if win_rate >= 0.75:
                    h2h_adj = 0.5
                elif win_rate >= 0.65:
                    h2h_adj = 0.3
                elif win_rate <= 0.25:
                    h2h_adj = -0.5
                elif win_rate <= 0.35:
                    h2h_adj = -0.3
                else:
                    h2h_adj = 0.0

                if h2h_adj != 0:
                    adj += h2h_adj
                    info['h2h'] = round(h2h_adj, 2)
                    info['h2h_record'] = f"{p1_wins}-{p2_wins}"
    except Exception:
        pass

    # 3. SURFACE SPECIALIST detection
    # Compare player's surface-specific Elo to their overall tennis Elo.
    # If surface Elo is 100+ points higher → surface specialist → boost.
    try:
        from config import TENNIS_SURFACES
        surface = TENNIS_SURFACES.get(sport, 'hard')
        tour = 'atp' if '_atp_' in sport else 'wta'
        surface_elo_key = f'tennis_{tour}_{surface}'

        # Get surface Elo for both players
        from elo_engine import get_elo_ratings
        surface_elos = get_elo_ratings(conn, surface_elo_key)

        # Get overall Elo (average across all surfaces)
        all_surface_keys = [f'tennis_{tour}_{s}' for s in ('hard', 'clay', 'grass')]
        p1_overall = []
        p2_overall = []
        for sk in all_surface_keys:
            sk_elos = get_elo_ratings(conn, sk)
            if player1 in sk_elos:
                p1_overall.append(sk_elos[player1]['elo'])
            if player2 in sk_elos:
                p2_overall.append(sk_elos[player2]['elo'])

        p1_surf = surface_elos.get(player1, {}).get('elo')
        p2_surf = surface_elos.get(player2, {}).get('elo')
        p1_avg = sum(p1_overall) / len(p1_overall) if p1_overall else None
        p2_avg = sum(p2_overall) / len(p2_overall) if p2_overall else None

        surf_adj = 0.0
        if p1_surf and p1_avg and (p1_surf - p1_avg) > 80:
            surf_adj += 0.3  # P1 is a surface specialist
        if p2_surf and p2_avg and (p2_surf - p2_avg) > 80:
            surf_adj -= 0.3  # P2 is a surface specialist

        if surf_adj != 0:
            adj += surf_adj
            info['surface_specialist'] = round(surf_adj, 2)
    except Exception:
        pass

    return round(adj, 2), info


def get_context_adjustments(conn, sport, home, away, event_id, commence,
                            market_type='SPREAD', selection=None):
    """
    Compute all contextual adjustments and return combined result.
    
    IMPORTANT: Summary only includes factors relevant to the market_type.
    Spread picks don't show totals factors, and vice versa.
    
    Returns dict:
        spread_adj: float — total points to add to model spread (positive = home advantage)
        total_adj: float — points to add to model total
        confidence: float — multiplier for edge (0.8 to 1.2)
        factors: dict — individual factor breakdowns
        summary: str — human-readable summary of RELEVANT factors only
    """
    total_spread_adj = 0.0
    total_total_adj = 0.0
    confidence_mult = 1.0
    all_factors = {}
    spread_summaries = []  # Only shown for SPREAD/ML picks
    total_summaries = []   # Only shown for TOTAL picks
    
    # 1. Rest & Schedule (affects SPREAD)
    rest_adj, rest_info = rest_adjustment(conn, home, away, sport, commence)
    if rest_adj != 0:
        total_spread_adj += rest_adj
        all_factors['rest'] = rest_info
        for k, v in rest_info.items():
            if 'b2b' in k:
                team_side = 'Home' if 'home' in k else 'Away'
                spread_summaries.append(f"{team_side} on B2B ({v:+.1f})")
            elif '3in5' in k:
                team_side = 'Home' if 'home' in k else 'Away'
                spread_summaries.append(f"{team_side} 3-in-5 ({v:+.1f})")
            elif 'extra_rest' in k or 'fresh_arms' in k:
                team_side = 'Home' if 'home' in k else 'Away'
                label = 'fresh arms' if 'fresh_arms' in k else 'extra rest'
                spread_summaries.append(f"{team_side} {label} ({v:+.1f})")
            elif 'heavy_sched' in k or 'extreme_sched' in k:
                team_side = 'Home' if 'home' in k else 'Away'
                label = 'extreme schedule' if 'extreme' in k else 'heavy schedule'
                spread_summaries.append(f"{team_side} {label} ({v:+.1f})")
    
    # 2. Line Movement (affects CONFIDENCE for any market type)
    if selection:
        lm_move, lm_signal, lm_conf = line_movement_signal(
            conn, event_id, selection, market_type)
        if lm_signal != 'NEUTRAL' and lm_signal not in ('NO_OPENER', 'NO_CURRENT'):
            confidence_mult *= lm_conf
            all_factors['line_movement'] = {
                'movement': round(lm_move, 1), 'signal': lm_signal,
                'confidence': lm_conf
            }
            label = f"Sharp money agrees ({lm_move:+.1f})" if lm_signal == 'SHARP_AGREE' \
                    else f"⚠️ Public side ({lm_move:+.1f})"
            spread_summaries.append(label)
            total_summaries.append(label)
    
    # 3. Home/Away Splits — DISABLED v12 for non-soccer
    # Data: 3W-8L, -26.5u in basketball. Re-enabled for soccer only (v16).
    # Soccer has extreme home/away splits (e.g., Serie A, Bundesliga fortress teams).
    if 'soccer' in sport:
        try:
            ha_adj, ha_info = soccer_home_away_adjustment(conn, home, away, sport)
            if ha_adj != 0:
                total_spread_adj += ha_adj
                all_factors['home_away_split'] = ha_info
                spread_summaries.append(f"H/A split ({ha_adj:+.2f})")
        except Exception:
            pass
    else:
        pass  # Disabled v12 for non-soccer — was 3-8, -26.5u
    
    # 4. Travel & Timezone (affects SPREAD)
    travel_adj, travel_info = travel_timezone_adjustment(home, away, commence, sport)
    if travel_adj != 0:
        total_spread_adj += travel_adj
        all_factors['travel'] = travel_info
        for k, v in travel_info.items():
            if 'early_west' in k:
                spread_summaries.append(f"West coast early start ({v:+.1f})")
            elif 'cross_country' in k:
                spread_summaries.append(f"Cross-country trip ({v:+.1f})")
    
    # 5. Altitude (affects SPREAD or TOTAL depending on market)
    alt_adj, alt_info = altitude_adjustment(home, market_type)
    if alt_adj != 0:
        if market_type == 'TOTAL':
            total_total_adj += alt_adj
            total_summaries.append(f"Altitude ({alt_adj:+.1f})")
        else:
            total_spread_adj += alt_adj
            spread_summaries.append(f"Altitude ({alt_adj:+.1f})")
        all_factors['altitude'] = alt_info
    
    # 6. Motivation (affects SPREAD)
    mot_adj, mot_info = motivation_adjustment(conn, home, away, sport, commence)
    if mot_adj != 0:
        total_spread_adj += mot_adj
        all_factors['motivation'] = mot_info
        for k, v in mot_info.items():
            if 'letdown' in k:
                team_side = 'Home' if 'home' in k else 'Away'
                spread_summaries.append(f"{team_side} letdown spot ({v:+.1f})")
            elif 'bounceback' in k:
                team_side = 'Home' if 'home' in k else 'Away'
                spread_summaries.append(f"{team_side} bounce-back ({v:+.1f})")
            elif 'revenge' in k:
                team_side = 'Home' if 'home' in k else 'Away'
                spread_summaries.append(f"{team_side} revenge game ({v:+.1f})")

    # 6b. Baseball Series Context (affects SPREAD)
    if 'baseball' in sport:
        series_adj, series_desc, series_reasons = _series_context(
            conn, home, away, sport, commence)
        if series_adj != 0:
            total_spread_adj += series_adj
        if series_desc:
            spread_summaries.append(series_desc)
        if series_reasons:
            all_factors['series'] = series_reasons
            for k, v in series_reasons.items():
                if 'blowout' in k:
                    spread_summaries.append(f"Series blowout adj ({v:+.1f})")
                elif 'sweep' in k:
                    team_side = 'Home' if 'home' in k else 'Away'
                    spread_summaries.append(f"{team_side} sweep attempt ({v:+.1f})")
                elif 'rubber' in k:
                    spread_summaries.append("Rubber match — tight game expected")

    # 7. Pace of Play (affects TOTAL only)
    pace_adj, pace_info = pace_of_play_adjustment(conn, home, away, sport)
    if pace_adj != 0:
        total_total_adj += pace_adj
        all_factors['pace'] = pace_info
        if pace_info.get('home_pace') and pace_info.get('away_pace'):
            desc = 'fast' if pace_adj > 0 else 'slow'
            total_summaries.append(f"Both teams {desc}-paced ({pace_adj:+.1f})")
        elif pace_info.get('home_pace'):
            desc = 'fast' if pace_info['home_pace'] > 0 else 'slow'
            total_summaries.append(f"Home {desc}-paced ({pace_info['home_pace']:+.1f})")
        elif pace_info.get('away_pace'):
            desc = 'fast' if pace_info['away_pace'] > 0 else 'slow'
            total_summaries.append(f"Away {desc}-paced ({pace_info['away_pace']:+.1f})")
    
    # 8. Head-to-Head History (can affect both SPREAD and TOTAL)
    h2h_spread, h2h_total, h2h_info = head_to_head_adjustment(conn, home, away, sport)
    has_h2h = False
    if h2h_spread != 0:
        total_spread_adj += h2h_spread
        all_factors['h2h'] = h2h_info
        spread_summaries.append(f"H2H matchup ({h2h_spread:+.1f})")
        has_h2h = True
    if h2h_total != 0:
        total_total_adj += h2h_total
        if 'h2h' not in all_factors:
            all_factors['h2h'] = h2h_info
        direction = 'high' if h2h_total > 0 else 'low'
        total_summaries.append(f"H2H {direction}-scoring ({h2h_total:+.1f})")
        has_h2h = True
    
    # 9. Referee Tendencies (TOTAL primarily, small SPREAD via home bias)
    ref_total, ref_spread, ref_info = ref_adjustment(conn, sport, event_id)
    if ref_total != 0 or ref_spread != 0:
        total_total_adj += ref_total
        total_spread_adj += ref_spread
        all_factors['refs'] = ref_info
        ref_names = ref_info.get('refs', 'Unknown')
        if ref_total > 0:
            total_summaries.append(f"Refs push over ({ref_names})")
        elif ref_total < 0:
            total_summaries.append(f"Refs push under ({ref_names})")
        if ref_spread != 0:
            spread_summaries.append(f"Ref home bias ({ref_spread:+.1f})")
    
    # 10. Conference/Division Familiarity (TOTAL only)
    # SKIP if H2H already fired — they detect the same signal (repeated matchups)
    if not has_h2h:
        fam_spread, fam_total, fam_info = familiarity_adjustment(conn, home, away, sport)
        if fam_total != 0:
            total_total_adj += fam_total
            all_factors['familiarity'] = fam_info
            total_summaries.append(f"Division familiarity ({fam_total:+.1f})")
    
    # 11. Recent Form — DISABLED
    # Added in session 14, immediately went 1W-4L (-14.4u) on home hot streaks.
    # Hot/cold streaks are already priced by the market. Using them as "context"
    # let garbage picks through the soft market filter. Removing entirely.
    # form_adj, form_info = _recent_form_adjustment(conn, home, away, sport, commence)
    
    # 12. Scoring Trend — DISABLED
    # Added in session 14, double-counts with estimate_model_total which already
    # uses team scoring averages. "Both trending low-scoring" was 1W-2L (-7.5u).
    # Even halved, it pushed totals in wrong direction and gave false context.
    # trend_adj, trend_info = _scoring_trend_adjustment(conn, home, away, sport)
    
    # ── SOCCER-SPECIFIC CONTEXT (v16 rebuild) ──
    if 'soccer' in sport:
        # 13a. Derby Detection — tighten spread, bump total for rivalry games
        try:
            derby_spread, derby_total, derby_info = derby_adjustment(conn, home, away, sport)
            if derby_spread != 0 or derby_total != 0:
                total_spread_adj += derby_spread
                total_total_adj += derby_total
                all_factors['derby'] = derby_info
                intensity = derby_info.get('intensity', 'MINOR')
                spread_summaries.append(f"Derby match ({intensity}, {derby_spread:+.2f})")
                if derby_total != 0:
                    total_summaries.append(f"Derby intensity ({derby_total:+.2f})")
        except Exception:
            pass

        # 13b. UCL Rotation — weaken team with upcoming European fixture
        try:
            ucl_adj, ucl_info = ucl_rotation_adjustment(conn, home, away, sport, commence)
            if ucl_adj != 0:
                total_spread_adj += ucl_adj
                all_factors['ucl_rotation'] = ucl_info
                spread_summaries.append(f"UCL rotation ({ucl_adj:+.2f})")
        except Exception:
            pass

        # 13c. League Position / Motivation — relegation battles, dead rubbers
        try:
            from soccer_standings import get_motivation_factor
            h_mot, a_mot = get_motivation_factor(conn, home, away, sport)
            if h_mot != 1.0 or a_mot != 1.0:
                # Convert motivation to spread adjustment
                # Relegation team (1.10) gets +0.15, dead rubber (0.85) gets -0.20
                mot_spread = (h_mot - a_mot) * 0.5  # Differential as spread points
                if abs(mot_spread) >= 0.05:
                    total_spread_adj += mot_spread
                    all_factors['league_position'] = {
                        'home_motivation': h_mot, 'away_motivation': a_mot
                    }
                    if h_mot > a_mot:
                        spread_summaries.append(f"Home more motivated ({mot_spread:+.2f})")
                    else:
                        spread_summaries.append(f"Away more motivated ({mot_spread:+.2f})")
        except Exception:
            pass

        # 13d. Fixture Congestion — cumulative fatigue over 30 days
        try:
            congestion_adj, congestion_info = fixture_congestion_adjustment(conn, home, away, sport)
            if congestion_adj != 0:
                total_spread_adj += congestion_adj
                all_factors['fixture_congestion'] = congestion_info
                spread_summaries.append(f"Fixture congestion ({congestion_adj:+.2f})")
        except Exception:
            pass

    # ── TENNIS-SPECIFIC CONTEXT ──
    if sport.startswith('tennis_'):
        try:
            t_adj, t_info = tennis_context_adjustments(conn, home, away, sport, commence)
            if t_adj != 0:
                total_spread_adj += t_adj
                all_factors['tennis'] = t_info
                for k, v in t_info.items():
                    if 'fatigue' in k:
                        who = 'P1' if 'p1' in k else 'P2'
                        spread_summaries.append(f"{who} fatigue ({v:+.2f})")
                    elif 'h2h' in k:
                        spread_summaries.append(f"H2H dominance ({v:+.2f})")
                    elif 'surface' in k:
                        spread_summaries.append(f"Surface specialist ({v:+.2f})")
        except Exception:
            pass

    # 14. Weather — Walters' outdoor sports factor
    # Wind, rain, cold push totals down. Only fires for outdoor sports.
    try:
        from weather_engine import get_weather_adjustment
        weather_adj, weather_info = get_weather_adjustment(home, sport, commence)
        if weather_adj != 0:
            total_total_adj += weather_adj
            all_factors['weather'] = weather_info
            weather_desc = weather_info.get('weather', {}).get('description', '')
            wind = weather_info.get('weather', {}).get('wind_mph', 0)
            if weather_adj <= -1.5:
                total_summaries.append(f"Weather: {weather_desc}, {wind}mph wind ({weather_adj:+.1f})")
            elif weather_adj < 0:
                total_summaries.append(f"Weather: {wind}mph wind ({weather_adj:+.1f})")
    except ImportError:
        pass  # Weather engine not installed — skip silently
    except Exception:
        pass  # Weather API error — skip silently
    
    # ── Cap total adjustment to prevent runaway ──
    # v12 FIX: Sport-specific caps. 2.5 pts in basketball is fine.
    # 2.5 goals in hockey/soccer flips the favorite entirely — insane.
    if sport.startswith('tennis_'):
        MAX_SPREAD_ADJ = 1.0   # Tennis: ~1 game handicap max
        MAX_TOTAL_ADJ = 1.5
    elif 'soccer' in sport:
        MAX_SPREAD_ADJ = 0.75  # v16: raised from 0.5 to accommodate derby/UCL/congestion modules
        MAX_TOTAL_ADJ = 0.6    # v16: raised from 0.5 to accommodate derby/referee modules
    elif 'hockey' in sport:
        MAX_SPREAD_ADJ = 0.5   # Half a goal max — hockey spreads are 1.5
        MAX_TOTAL_ADJ = 0.5
    elif 'baseball' in sport:
        MAX_SPREAD_ADJ = 1.0   # One run max
        MAX_TOTAL_ADJ = 1.5
    else:
        MAX_SPREAD_ADJ = 2.5   # Basketball: 2.5 pts is reasonable
        MAX_TOTAL_ADJ = 3.0
    
    total_spread_adj = max(-MAX_SPREAD_ADJ, min(MAX_SPREAD_ADJ, total_spread_adj))
    total_total_adj = max(-MAX_TOTAL_ADJ, min(MAX_TOTAL_ADJ, total_total_adj))
    confidence_mult = max(0.80, min(1.20, confidence_mult))
    
    # Pick the right summary based on what we're evaluating
    if market_type == 'TOTAL':
        summary_list = total_summaries
    else:
        summary_list = spread_summaries
    
    return {
        'spread_adj': round(total_spread_adj, 1),
        'total_adj': round(total_total_adj, 1),
        'confidence': round(confidence_mult, 2),
        'factors': all_factors,
        'summary': ' | '.join(summary_list) if summary_list else None,
        'factor_count': len([f for f in all_factors.values() if f]),
    }


# ═══════════════════════════════════════════════════════════════════
# CLI — Set refs manually before a run
# ═══════════════════════════════════════════════════════════════════

import os

def _cli_set_refs():
    """Interactive ref input for upcoming games."""
    import sqlite3 as _sq
    DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = _sq.connect(DB)
    ensure_ref_table(conn)
    
    # Show today's games that need refs
    from datetime import datetime, timedelta, timezone
    now_utc = datetime.now(timezone.utc)
    window_end = (now_utc + timedelta(hours=12)).isoformat()
    
    games = conn.execute("""
        SELECT DISTINCT event_id, sport, home, away, commence_time
        FROM market_consensus
        WHERE commence_time >= ? AND commence_time <= ?
        AND sport IN ('basketball_nba', 'icehockey_nhl')
        ORDER BY commence_time
    """, (now_utc.isoformat(), window_end)).fetchall()
    
    if not games:
        print("  No NBA/NHL games in the next 12 hours.")
        conn.close()
        return
    
    # Check which already have refs
    for g in games:
        eid, sport, home, away, commence = g
        existing = conn.execute(
            "SELECT ref_names FROM ref_assignments WHERE event_id=?", (eid,)
        ).fetchone()
        
        label = sport.split('_')[-1].upper()
        status = f"✅ {existing[0]}" if existing else "❌ No refs set"
        
        try:
            gt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
            est = gt - timedelta(hours=5)
            time_str = est.strftime('%I:%M %p')
        except:
            time_str = '?'
        
        print(f"  [{label}] {away} @ {home} | {time_str} EST | {status}")
    
    print(f"\n  To set refs:")
    print(f"  python context_engine.py --set-ref EVENT_ID \"Ref1,Ref2,Ref3\"")
    print(f"\n  Available NBA refs: {', '.join(list(NBA_REF_TENDENCIES.keys())[:8])}...")
    
    conn.close()


if __name__ == '__main__':
    import sys
    if '--set-ref' in sys.argv:
        idx = sys.argv.index('--set-ref')
        eid = sys.argv[idx + 1]
        refs = sys.argv[idx + 2]
        # Determine sport from event_id
        set_refs(eid, 'basketball_nba', refs)
    elif '--refs' in sys.argv:
        _cli_set_refs()
    else:
        _cli_set_refs()
