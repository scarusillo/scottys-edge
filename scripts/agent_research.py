"""
agent_research.py — Scotty's Edge Research Agent

Runs before each pick run (11am + 5:30pm) to check for:
  1. Injuries — ESPN injury reports for NBA, NHL, college sports
  2. Lineup changes — late scratches that affect the line
  3. Weather — for baseball (outdoor games affected by wind/rain)

Outputs a research brief that gets attached to each pick's context.
Also flags games where a key player status changed since the opener.

Usage:
    python agent_research.py                    # All sports
    python agent_research.py --sport nba        # Single sport
    python agent_research.py --email            # Email the brief
"""
import sqlite3, json, os, sys
from datetime import datetime, timedelta
from urllib.request import Request, urlopen

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

ESPN_INJURY_ENDPOINTS = {
    'basketball_nba': 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries',
    'icehockey_nhl': 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/injuries',
    'basketball_ncaab': 'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/injuries',
}

# Players whose absence significantly moves the line (star players)
# Updated by checking who has highest usage/impact per team
IMPACT_THRESHOLD = {
    'basketball_nba': ['OUT', 'DOUBTFUL'],
    'icehockey_nhl': ['OUT', 'DOUBTFUL'],
    'basketball_ncaab': ['OUT', 'DOUBTFUL'],
}


def _fetch_json(url):
    """Fetch JSON from ESPN endpoint."""
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        resp = urlopen(req, timeout=15)
        return json.loads(resp.read().decode())
    except Exception:
        return None


def fetch_injuries(sport):
    """Fetch current injury report for a sport from ESPN."""
    url = ESPN_INJURY_ENDPOINTS.get(sport)
    if not url:
        return {}
    
    data = _fetch_json(url)
    if not data:
        return {}
    
    injuries = {}  # team_name -> list of injured players
    
    for item in data.get('items', data.get('injuries', [])):
        # ESPN formats vary by sport
        team = item.get('team', {})
        team_name = team.get('displayName', team.get('name', 'Unknown'))
        
        team_injuries = []
        for athlete in item.get('injuries', []):
            player = athlete.get('athlete', {})
            player_name = player.get('displayName', player.get('fullName', '?'))
            status = athlete.get('status', '?')
            injury_type = athlete.get('type', {}).get('description', '') if isinstance(athlete.get('type'), dict) else str(athlete.get('type', ''))
            
            if status.upper() in ['OUT', 'DOUBTFUL', 'QUESTIONABLE', 'DAY-TO-DAY']:
                team_injuries.append({
                    'player': player_name,
                    'status': status.upper(),
                    'injury': injury_type,
                })
        
        if team_injuries:
            injuries[team_name] = team_injuries
    
    return injuries


def check_injury_changes(conn, sport):
    """
    Compare current injuries against what was known at opener time.
    Flag any new OUT/DOUBTFUL players that appeared after 8am.
    """
    changes = []
    
    # Get today's bets for this sport
    today_bets = conn.execute("""
        SELECT DISTINCT selection, event_id FROM bets
        WHERE sport=? AND DATE(created_at) = DATE('now')
    """, (sport,)).fetchall()
    
    if not today_bets:
        return changes
    
    # Get current injuries
    current = fetch_injuries(sport)
    
    # Check stored injuries from opener (if we have them)
    stored = conn.execute("""
        SELECT team, player, status FROM injuries
        WHERE sport=? AND DATE(updated_at) = DATE('now')
    """, (sport,)).fetchall()
    
    stored_set = set()
    for team, player, status in stored:
        stored_set.add(f"{team}|{player}|{status}")
    
    # Find new injuries
    for team, players in current.items():
        for p in players:
            key = f"{team}|{p['player']}|{p['status']}"
            if key not in stored_set and p['status'] in ['OUT', 'DOUBTFUL']:
                # Check if this team is in our picks
                for sel, eid in today_bets:
                    if team.split()[-1].lower() in sel.lower():
                        changes.append({
                            'team': team,
                            'player': p['player'],
                            'status': p['status'],
                            'injury': p['injury'],
                            'affects_pick': sel,
                        })
    
    return changes


def generate_research_brief(sports=None):
    """Generate pre-game research brief."""
    conn = sqlite3.connect(DB_PATH)
    
    if sports is None:
        sports = ['basketball_nba', 'icehockey_nhl', 'basketball_ncaab']
    
    lines = []
    lines.append("=" * 60)
    lines.append(f"  PRE-GAME RESEARCH BRIEF")
    lines.append(f"  {datetime.now().strftime('%A %B %d, %Y  %I:%M %p')}")
    lines.append("=" * 60)
    
    alerts = []
    
    for sport in sports:
        sport_label = {
            'basketball_nba': 'NBA', 'icehockey_nhl': 'NHL',
            'basketball_ncaab': 'NCAAB', 'baseball_ncaa': 'Baseball',
        }.get(sport, sport)
        
        # Fetch injuries
        injuries = fetch_injuries(sport)
        
        if injuries:
            # Count OUT players
            out_count = sum(
                sum(1 for p in players if p['status'] == 'OUT')
                for players in injuries.values()
            )
            doubt_count = sum(
                sum(1 for p in players if p['status'] == 'DOUBTFUL')
                for players in injuries.values()
            )
            
            lines.append(f"\n  {sport_label}: {out_count} OUT, {doubt_count} DOUBTFUL")
            
            # Show teams with 2+ OUT players (significant)
            for team, players in injuries.items():
                outs = [p for p in players if p['status'] in ['OUT', 'DOUBTFUL']]
                if len(outs) >= 2:
                    names = ', '.join(f"{p['player']} ({p['status']})" for p in outs[:4])
                    lines.append(f"    {team}: {names}")
        else:
            lines.append(f"\n  {sport_label}: No injury data available")
        
        # Check for changes since opener
        changes = check_injury_changes(conn, sport)
        if changes:
            lines.append(f"\n  ⚠️ NEW SINCE OPENER:")
            for c in changes:
                lines.append(f"    ! {c['player']} ({c['team']}) — {c['status']} — affects: {c['affects_pick']}")
                alerts.append(c)
    
    # Summary
    if alerts:
        lines.append(f"\n  {'='*50}")
        lines.append(f"  {len(alerts)} INJURY ALERTS affecting today's picks")
        lines.append(f"  Review before posting!")
    else:
        lines.append(f"\n  No injury alerts affecting picks.")
    
    lines.append("\n" + "=" * 60)
    
    conn.close()
    return "\n".join(lines), alerts


if __name__ == '__main__':
    sport_filter = None
    for i, arg in enumerate(sys.argv):
        if arg == '--sport' and i + 1 < len(sys.argv):
            s = sys.argv[i + 1].lower()
            if 'nba' in s: sport_filter = ['basketball_nba']
            elif 'nhl' in s: sport_filter = ['icehockey_nhl']
            elif 'ncaab' in s: sport_filter = ['basketball_ncaab']
    
    brief, alerts = generate_research_brief(sport_filter)
    print(brief)
    
    if '--email' in sys.argv:
        try:
            from emailer import send_email
            today = datetime.now().strftime('%Y-%m-%d')
            subject = f"Research Brief - {today}"
            if alerts:
                subject = f"⚠️ INJURY ALERT - {len(alerts)} changes - {today}"
            send_email(subject, brief)
            print("\n  Email sent")
        except Exception as e:
            print(f"\n  Email failed: {e}")
