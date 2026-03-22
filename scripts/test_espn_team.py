"""
test_espn_team.py — Test ESPN team-specific endpoint for missing scores

ESPN's scoreboard misses games. The team-specific schedule endpoint
returns ALL games for a team, including ones the scoreboard skips.

Usage:
    python test_espn_team.py
"""
from urllib.request import Request, urlopen
import json

# Step 1: Search for Vanderbilt's team ID
print("Step 1: Finding Vanderbilt team ID...")
try:
    url = "https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams?limit=400"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    resp = urlopen(req, timeout=30)
    data = json.loads(resp.read().decode())
    
    teams = []
    for sport in data.get('sports', []):
        for league in sport.get('leagues', []):
            for team in league.get('teams', []):
                t = team.get('team', team)
                teams.append({'id': t.get('id'), 'name': t.get('displayName', t.get('name', ''))})
    
    print(f"  Found {len(teams)} teams")
    vandy = [t for t in teams if 'vanderbilt' in t['name'].lower()]
    if vandy:
        print(f"  Vanderbilt: ID={vandy[0]['id']}, Name={vandy[0]['name']}")
        vandy_id = vandy[0]['id']
    else:
        # Try search endpoint
        print("  Not in main list, trying search...")
        url2 = "https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams?search=vanderbilt"
        req2 = Request(url2, headers={'User-Agent': 'Mozilla/5.0'})
        resp2 = urlopen(req2, timeout=15)
        data2 = json.loads(resp2.read().decode())
        print(f"  Search result: {json.dumps(data2)[:500]}")
        vandy_id = None
except Exception as e:
    print(f"  Error: {e}")
    vandy_id = None

# Step 2: Try team schedule endpoint
if vandy_id:
    print(f"\nStep 2: Fetching Vanderbilt schedule (ID={vandy_id})...")
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{vandy_id}/schedule"
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urlopen(req, timeout=15)
        data = json.loads(resp.read().decode())
        
        events = data.get('events', [])
        print(f"  Found {len(events)} events")
        
        # Show most recent games
        for event in events[-5:]:
            name = event.get('name', event.get('shortName', '?'))
            date = event.get('date', '?')[:10]
            status = event.get('competitions', [{}])[0].get('status', {}).get('type', {}).get('name', '?')
            
            # Get scores
            scores = ""
            for comp in event.get('competitions', []):
                for team in comp.get('competitors', []):
                    tn = team.get('team', {}).get('displayName', '?')
                    sc = team.get('score', '?')
                    ha = team.get('homeAway', '?')
                    scores += f"  {tn}({ha})={sc}"
            
            print(f"  {date} | {status:12s} | {name} | {scores}")
    except Exception as e:
        print(f"  Schedule error: {e}")

# Step 3: Try the scoreboard with groups parameter
print(f"\nStep 3: Testing scoreboard with groups=50...")
try:
    url = "https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/scoreboard?dates=20260315&groups=50&limit=300"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    resp = urlopen(req, timeout=30)
    data = json.loads(resp.read().decode())
    events = data.get('events', [])
    print(f"  Found {len(events)} games on March 15")
    
    vandy_games = [e for e in events if 'vanderbilt' in json.dumps(e).lower()]
    if vandy_games:
        for g in vandy_games:
            name = g.get('name', '?')
            for comp in g.get('competitions', []):
                status = comp.get('status', {}).get('type', {}).get('name', '?')
                scores = ""
                for team in comp.get('competitors', []):
                    tn = team.get('team', {}).get('displayName', '?')
                    sc = team.get('score', '?')
                    scores += f"  {tn}={sc}"
                print(f"  FOUND: {name} | {status} | {scores}")
    else:
        print("  Vanderbilt NOT found in scoreboard results")
except Exception as e:
    print(f"  Scoreboard error: {e}")

# Step 4: Try date-specific team results
if vandy_id:
    print(f"\nStep 4: Trying team results endpoint...")
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{vandy_id}/schedule?season=2026"
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urlopen(req, timeout=15)
        data = json.loads(resp.read().decode())
        
        events = data.get('events', [])
        print(f"  Found {len(events)} season events")
        
        # Show March 15 games
        march15 = [e for e in events if '2026-03-15' in str(e.get('date', ''))]
        for event in march15:
            name = event.get('name', '?')
            for comp in event.get('competitions', []):
                status = comp.get('status', {}).get('type', {}).get('name', '?')
                scores = ""
                for team in comp.get('competitors', []):
                    tn = team.get('team', {}).get('displayName', '?')
                    sc = team.get('score', '?')
                    scores += f"  {tn}={sc}"
                print(f"  March 15: {name} | {status} | {scores}")
    except Exception as e:
        print(f"  Team results error: {e}")
