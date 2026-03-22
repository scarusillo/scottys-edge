"""
injuries.py — Injury impact tracking and line movement analysis.

Core principle: injury information moves markets.
This module helps you:
1. Log injuries and estimate their point spread impact
2. Detect line movements that suggest injury news
3. Auto-adjust power ratings based on active injuries
"""
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ══════════════════════════════════════════════════════════════════════
# INJURY IMPACT ESTIMATES (points of spread impact)
# ══════════════════════════════════════════════════════════════════════
# These are approximate. You'll refine these over time with actual data.

NBA_IMPACT = {
    # Tier 1: MVP candidates — 5-8 point impact
    'tier1': 6.5,
    # Tier 2: All-Stars / top-20 players — 3-5 points
    'tier2': 4.0,
    # Tier 3: Quality starters — 1.5-3 points
    'tier3': 2.0,
    # Tier 4: Rotation players — 0.5-1.5 points
    'tier4': 1.0,
    # Tier 5: End of bench — 0-0.5 points
    'tier5': 0.25,
}

NCAAB_IMPACT = {
    'tier1': 5.0,   # Best player on a team
    'tier2': 3.0,   # Second-best / key starter
    'tier3': 1.5,   # Starter
    'tier4': 0.5,   # Rotation
    'tier5': 0.1,
}

NHL_IMPACT = {
    'tier1': 0.35,   # Star goalie or #1C (in goals)
    'tier2': 0.20,
    'tier3': 0.10,
    'tier4': 0.05,
    'tier5': 0.02,
}


def add_injury(sport, team, player, status, injury_type=None,
               tier=3, source=None):
    """
    Log an injury and compute its point impact.

    Args:
        sport: e.g., 'basketball_nba'
        team: canonical team name
        player: player name
        status: 'OUT', 'DOUBTFUL', 'QUESTIONABLE', 'PROBABLE'
        tier: 1-5 (1=MVP level, 5=end of bench)
        source: where you got the info
    """
    conn = sqlite3.connect(DB_PATH)

    # Status multiplier (how likely they actually miss)
    status_mult = {
        'OUT': 1.0,
        'DOUBTFUL': 0.75,
        'QUESTIONABLE': 0.40,
        'PROBABLE': 0.10,
        'DAY-TO-DAY': 0.30,
    }.get(status.upper(), 0.5)

    # Get tier impact based on sport
    if 'nba' in sport:
        impact_map = NBA_IMPACT
    elif 'ncaa' in sport:
        impact_map = NCAAB_IMPACT
    elif 'nhl' in sport or 'hockey' in sport:
        impact_map = NHL_IMPACT
    else:
        impact_map = NBA_IMPACT  # default

    base_impact = impact_map.get(f'tier{tier}', 1.0)
    point_impact = round(base_impact * status_mult, 2)

    conn.execute("""
        INSERT INTO injuries (report_date, sport, team, player, status,
            injury_type, point_impact, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (datetime.now().strftime('%Y-%m-%d'), sport, team, player,
          status.upper(), injury_type, point_impact, source,
          datetime.now().isoformat()))
    conn.commit()
    conn.close()
    print(f"  ✅ Logged: {player} ({team}) — {status} — Impact: {point_impact:+.2f} pts")
    return point_impact


def get_team_injury_impact(sport, team, date=None):
    """Get total injury impact for a team on a given date."""
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT player, status, point_impact
        FROM injuries
        WHERE sport = ? AND team = ? AND report_date = ?
        AND status IN ('OUT', 'DOUBTFUL', 'QUESTIONABLE', 'DAY-TO-DAY')
    """, (sport, team, date)).fetchall()
    conn.close()

    total_impact = sum(r[2] for r in rows if r[2])
    return total_impact, rows


def apply_injuries_to_ratings(sport, date=None):
    """
    Update power ratings injury_adjust column based on today's injury reports.
    This is the key integration between injury data and the model.
    """
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')

    conn = sqlite3.connect(DB_PATH)

    # Get all teams with injuries today
    teams = conn.execute("""
        SELECT DISTINCT team FROM injuries
        WHERE sport = ? AND report_date = ?
    """, (sport, date)).fetchall()

    updated = 0
    for (team,) in teams:
        impact, injuries = get_team_injury_impact(sport, team, date)
        if impact > 0:
            # Negative because injuries hurt the team
            conn.execute("""
                UPDATE power_ratings
                SET injury_adjust = ?
                WHERE sport = ? AND team = ?
                AND run_timestamp = (
                    SELECT MAX(run_timestamp) FROM power_ratings
                    WHERE sport = ? AND team = ?
                )
            """, (-impact, sport, team, sport, team))
            updated += 1
            print(f"  {team}: injury_adjust = {-impact:+.2f} ({len(injuries)} players)")

    conn.commit()
    conn.close()
    print(f"  Updated {updated} teams' injury adjustments")


# ══════════════════════════════════════════════════════════════════════
# LINE MOVEMENT DETECTION
# ══════════════════════════════════════════════════════════════════════

def detect_line_movement(sport, min_move=1.5):
    """
    Compare opening lines to current lines to detect significant moves.
    Large moves often indicate injury news or sharp money.
    """
    conn = sqlite3.connect(DB_PATH)

    moves = conn.execute("""
        SELECT mc.event_id, mc.home, mc.away, mc.commence_time,
               mc.best_home_spread, o.line as open_spread,
               mc.best_home_spread - o.line as movement
        FROM market_consensus mc
        JOIN openers o ON mc.event_id = o.event_id
            AND o.market = 'spreads' AND o.selection = mc.home
        WHERE mc.sport = ?
            AND ABS(mc.best_home_spread - o.line) >= ?
        ORDER BY ABS(mc.best_home_spread - o.line) DESC
    """, (sport, min_move)).fetchall()
    conn.close()

    if moves:
        print(f"\n  ⚡ SIGNIFICANT LINE MOVES ({sport}):")
        for m in moves:
            direction = "→ MORE favored" if m[6] < 0 else "→ LESS favored"
            print(f"    {m[1]} vs {m[2]}: Opened {m[5]:+.1f}, Now {m[4]:+.1f} ({m[6]:+.1f} {direction})")
    else:
        print(f"  No significant line moves (>{min_move} pts) for {sport}")

    return moves


def detect_reverse_line_movement(sport):
    """
    Detect games where the line moved OPPOSITE to where the public
    would push it. This is a classic sharp money indicator.

    Logic: If Team A opened as -3 favorite and is now -2 (line moved
    toward underdog), but the moneyline moved toward Team A (more juice),
    that's reverse line movement — sharps are on Team A.
    """
    conn = sqlite3.connect(DB_PATH)

    # Get opening vs current for spreads
    games = conn.execute("""
        SELECT DISTINCT mc.event_id, mc.home, mc.away,
               mc.best_home_spread, mc.best_home_spread_odds
        FROM market_consensus mc
        WHERE mc.sport = ?
    """, (sport,)).fetchall()

    rlm_games = []
    for g in games:
        event_id, home, away, current_spread, current_odds = g

        # Get opening spread
        opener = conn.execute("""
            SELECT line, odds FROM openers
            WHERE event_id = ? AND market = 'spreads' AND selection = ?
            ORDER BY timestamp ASC LIMIT 1
        """, (event_id, home)).fetchone()

        if not opener or current_spread is None:
            continue

        open_spread, open_odds = opener
        if open_spread is None:
            continue

        move = current_spread - open_spread
        # RLM: line moved toward underdog but odds got juicier on favorite
        if abs(move) >= 0.5:
            rlm_games.append({
                'event_id': event_id,
                'home': home,
                'away': away,
                'open_spread': open_spread,
                'current_spread': current_spread,
                'move': move,
            })

    conn.close()

    if rlm_games:
        print(f"\n  🔄 REVERSE LINE MOVEMENT ({sport}):")
        for g in rlm_games:
            print(f"    {g['home']} vs {g['away']}: "
                  f"Opened {g['open_spread']:+.1f} → Now {g['current_spread']:+.1f} "
                  f"(Moved {g['move']:+.1f})")

    return rlm_games


# ══════════════════════════════════════════════════════════════════════
# QUICK INJURY INPUT (for manual daily updates)
# ══════════════════════════════════════════════════════════════════════

def quick_injuries_nba(injuries_list):
    """
    Batch add NBA injuries. Input format:
    [
        ("Lakers", "LeBron James", "OUT", 1),
        ("Lakers", "Anthony Davis", "QUESTIONABLE", 1),
        ("Celtics", "Jaylen Brown", "OUT", 2),
    ]
    Each tuple: (team_short, player, status, tier)
    """
    # Map short names to full canonical names
    NBA_SHORT = {
        'Hawks': 'Atlanta Hawks', 'Celtics': 'Boston Celtics',
        'Nets': 'Brooklyn Nets', 'Hornets': 'Charlotte Hornets',
        'Bulls': 'Chicago Bulls', 'Cavaliers': 'Cleveland Cavaliers',
        'Cavs': 'Cleveland Cavaliers', 'Mavericks': 'Dallas Mavericks',
        'Mavs': 'Dallas Mavericks', 'Nuggets': 'Denver Nuggets',
        'Pistons': 'Detroit Pistons', 'Warriors': 'Golden State Warriors',
        'Rockets': 'Houston Rockets', 'Pacers': 'Indiana Pacers',
        'Clippers': 'Los Angeles Clippers', 'Lakers': 'Los Angeles Lakers',
        'Grizzlies': 'Memphis Grizzlies', 'Heat': 'Miami Heat',
        'Bucks': 'Milwaukee Bucks', 'Timberwolves': 'Minnesota Timberwolves',
        'Wolves': 'Minnesota Timberwolves', 'Pelicans': 'New Orleans Pelicans',
        'Knicks': 'New York Knicks', 'Thunder': 'Oklahoma City Thunder',
        'OKC': 'Oklahoma City Thunder', 'Magic': 'Orlando Magic',
        '76ers': 'Philadelphia 76ers', 'Sixers': 'Philadelphia 76ers',
        'Suns': 'Phoenix Suns', 'Trail Blazers': 'Portland Trail Blazers',
        'Blazers': 'Portland Trail Blazers', 'Kings': 'Sacramento Kings',
        'Spurs': 'San Antonio Spurs', 'Raptors': 'Toronto Raptors',
        'Jazz': 'Utah Jazz', 'Wizards': 'Washington Wizards',
    }

    for team_short, player, status, tier in injuries_list:
        team = NBA_SHORT.get(team_short, team_short)
        add_injury('basketball_nba', team, player, status, tier=tier,
                   source='manual')

    apply_injuries_to_ratings('basketball_nba')


if __name__ == '__main__':
    # Example: log tonight's NBA injuries
    print("Injury system ready. Use quick_injuries_nba() to batch add.")
    print("Example:")
    print('  quick_injuries_nba([')
    print('      ("Mavs", "Luka Doncic", "OUT", 1),')
    print('      ("Bucks", "Giannis Antetokounmpo", "QUESTIONABLE", 1),')
    print('  ])')
