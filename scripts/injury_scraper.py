#!/usr/bin/env python3
"""
INJURY SCRAPER — ESPN free injury reports.

Walters used injury information BEFORE the market adjusted.
ESPN publishes injury reports that update throughout the day.
Key players being Out or Doubtful can shift a spread 2-5 points
but the market sometimes lags, especially for mid-major NCAAB
and early-morning injury news.

Usage:
    python main.py injuries           # Auto-fetch from ESPN
    python main.py injuries --manual  # Manual entry
    python injury_scraper.py          # Standalone

Sports covered: NBA, NCAAB (partial), NHL
"""
import sqlite3
import os
import json
from datetime import datetime

try:
    import urllib.request
except ImportError:
    urllib = None

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ESPN injury endpoints (free, no auth required)
ESPN_INJURY_URLS = {
    'basketball_nba': 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries',
    'icehockey_nhl': 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/injuries',
}

# Approximate point impact by position (NBA)
NBA_IMPACT = {
    'PG': 3.0, 'SG': 2.5, 'SF': 2.5, 'PF': 2.5, 'C': 2.0,
}

# Approximate goal impact (NHL)
NHL_IMPACT = {
    'G': 0.5,  # Goalie out is huge
    'C': 0.2, 'LW': 0.15, 'RW': 0.15, 'D': 0.1,
}


def ensure_injuries_table(conn):
    """Create injuries table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS injuries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT,
            sport TEXT,
            team TEXT,
            player TEXT,
            position TEXT,
            status TEXT,
            detail TEXT,
            point_impact REAL,
            fetched_at TEXT,
            UNIQUE(report_date, sport, team, player)
        )
    """)
    conn.commit()


def _fetch_espn_injuries(sport):
    """Fetch injury data from ESPN's free API."""
    url = ESPN_INJURY_URLS.get(sport)
    if not url:
        return []
    
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0'
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        return data
    except Exception as e:
        print(f"  ESPN injury fetch failed for {sport}: {e}")
        return []


def _parse_nba_injuries(data):
    """Parse ESPN NBA injury response into rows."""
    rows = []
    today = datetime.now().strftime('%Y-%m-%d')
    
    teams = data.get('injuries', []) if isinstance(data, dict) else data
    
    for team_data in teams:
        team_name = team_data.get('displayName', '')
        injuries = team_data.get('injuries', [])
        
        for inj in injuries:
            athlete = inj.get('athlete', {})
            player = athlete.get('displayName', '')
            # ESPN doesn't include position in injury endpoint
            # Use shortComment to detect status
            status = inj.get('status', '')
            detail = inj.get('shortComment', '')
            
            # Only track meaningful statuses
            if status.upper() not in ('OUT', 'DOUBTFUL', 'DAY-TO-DAY', 'QUESTIONABLE'):
                continue
            
            # Default impact — we don't have position data from this endpoint
            impact = 2.5 if status.upper() in ('OUT', 'DOUBTFUL') else 1.0
            
            rows.append({
                'date': today,
                'sport': 'basketball_nba',
                'team': team_name,
                'player': player,
                'position': '',
                'status': status,
                'detail': detail,
                'impact': impact,
            })
    
    return rows


def _parse_nhl_injuries(data):
    """Parse ESPN NHL injury response into rows."""
    rows = []
    today = datetime.now().strftime('%Y-%m-%d')
    
    teams = data.get('injuries', []) if isinstance(data, dict) else data
    
    for team_data in teams:
        team_name = team_data.get('displayName', '')
        injuries = team_data.get('injuries', [])
        
        for inj in injuries:
            athlete = inj.get('athlete', {})
            player = athlete.get('displayName', '')
            status = inj.get('status', '')
            detail = inj.get('shortComment', '')
            
            if status.upper() not in ('OUT', 'DOUBTFUL', 'DAY-TO-DAY', 'QUESTIONABLE'):
                continue
            
            # Goalie detection from comment text
            is_goalie = 'goalie' in detail.lower() or 'goaltender' in detail.lower()
            impact = 0.5 if is_goalie else 0.15
            if status.upper() in ('QUESTIONABLE', 'DAY-TO-DAY'):
                impact *= 0.5
            
            rows.append({
                'date': today,
                'sport': 'icehockey_nhl',
                'team': team_name,
                'player': player,
                'position': 'G' if is_goalie else '',
                'status': status,
                'detail': detail,
                'impact': impact,
            })
    
    return rows


def fetch_and_apply_all():
    """Fetch injuries for all supported sports and store in DB."""
    conn = sqlite3.connect(DB_PATH)
    ensure_injuries_table(conn)
    
    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.now().isoformat()
    total = 0
    
    for sport, url in ESPN_INJURY_URLS.items():
        print(f"  Fetching {sport} injuries from ESPN...")
        data = _fetch_espn_injuries(sport)
        
        if not data:
            print(f"    No data returned")
            continue
        
        if sport == 'basketball_nba':
            rows = _parse_nba_injuries(data)
        elif sport == 'icehockey_nhl':
            rows = _parse_nhl_injuries(data)
        else:
            continue
        
        # Clear today's old data for this sport, then insert fresh
        conn.execute("DELETE FROM injuries WHERE sport=? AND report_date=?",
                     (sport, today))
        
        for r in rows:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO injuries
                    (report_date, sport, team, player, position, status, injury_type, point_impact, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (r['date'], r['sport'], r['team'], r['player'],
                      r['position'], r['status'], r['detail'], r['impact'], now))
            except Exception as e:
                print(f"    Injury insert error: {e}")
        
        # Count meaningful injuries (Out + Doubtful only)
        key_injuries = [r for r in rows if r['status'].upper() in ('OUT', 'DOUBTFUL')]
        print(f"    {len(rows)} total injuries, {len(key_injuries)} Out/Doubtful")
        total += len(rows)
    
    conn.commit()
    
    # Show summary by team for today's games
    print(f"\n  Key injuries affecting today's games:")
    for sport in ESPN_INJURY_URLS:
        out_players = conn.execute("""
            SELECT team, player, position, status FROM injuries
            WHERE sport=? AND report_date=? AND status IN ('Out', 'OUT', 'Doubtful', 'DOUBTFUL')
            ORDER BY team
        """, (sport, today)).fetchall()
        
        if out_players:
            current_team = None
            for team, player, pos, status in out_players:
                if team != current_team:
                    current_team = team
                    print(f"    {team}:")
                print(f"      {status:10s} {pos:3s} {player}")
    
    conn.close()
    print(f"\n  Total injuries stored: {total}")


def manual_injury_entry():
    """Interactive manual injury input."""
    conn = sqlite3.connect(DB_PATH)
    ensure_injuries_table(conn)
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    print("\n  MANUAL INJURY ENTRY")
    print("  Type 'done' when finished.\n")
    
    while True:
        team = input("  Team (full name): ").strip()
        if team.lower() == 'done':
            break
        
        player = input("  Player: ").strip()
        sport = input("  Sport (basketball_nba/icehockey_nhl): ").strip()
        status = input("  Status (Out/Doubtful/Day-To-Day): ").strip()
        position = input("  Position (PG/SG/SF/PF/C/G/D/LW/RW): ").strip()
        
        impact = NBA_IMPACT.get(position, 2.0) if 'basketball' in sport else NHL_IMPACT.get(position, 0.1)
        
        conn.execute("""
            INSERT OR REPLACE INTO injuries
            (report_date, sport, team, player, position, status, detail, point_impact, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (today, sport, team, player, position, status, 'Manual entry', impact,
              datetime.now().isoformat()))
        
        print(f"    Added: {player} ({status}) → {team}\n")
    
    conn.commit()
    conn.close()
    print("  Done.")


if __name__ == '__main__':
    fetch_and_apply_all()
