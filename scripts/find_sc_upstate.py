"""Find South Carolina Upstate score from ESPN"""
from urllib.request import Request, urlopen
import json

# Try searching ESPN for the team
print("Searching ESPN for South Carolina Upstate...")
try:
    url = "https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams?limit=500"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    resp = urlopen(req, timeout=30)
    data = json.loads(resp.read().decode())
    
    matches = []
    for sport in data.get('sports', []):
        for league in sport.get('leagues', []):
            for team in league.get('teams', []):
                t = team.get('team', team)
                name = t.get('displayName', t.get('name', ''))
                if 'upstate' in name.lower() or 'south carolina up' in name.lower():
                    matches.append({'id': t.get('id'), 'name': name})
                # Also check abbreviation
                abbr = t.get('abbreviation', '')
                if 'SCUP' in abbr or 'USC UP' in abbr.upper():
                    matches.append({'id': t.get('id'), 'name': name, 'abbr': abbr})
    
    if matches:
        for m in matches:
            print(f"  Found: {m}")
            tid = m['id']
            # Get schedule
            url2 = f"https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{tid}/schedule?season=2026"
            req2 = Request(url2, headers={'User-Agent': 'Mozilla/5.0'})
            resp2 = urlopen(req2, timeout=15)
            data2 = json.loads(resp2.read().decode())
            events = data2.get('events', [])
            print(f"  {len(events)} events")
            # Show March 14 games
            for e in events:
                d = str(e.get('date', ''))[:10]
                if '2026-03-14' in d or '2026-03-15' in d:
                    name = e.get('name', '?')
                    for comp in e.get('competitions', []):
                        status = comp.get('status', {}).get('type', {}).get('name', '?')
                        scores = ""
                        for t in comp.get('competitors', []):
                            tn = t.get('team', {}).get('displayName', '?')
                            sc = t.get('score', '?')
                            scores += f" {tn}={sc}"
                        print(f"  {d} | {status} | {name} |{scores}")
    else:
        print("  Not found in ESPN teams list")
        # Try alternate names
        print("\n  Trying 'Spartans' search...")
        for sport in data.get('sports', []):
            for league in sport.get('leagues', []):
                for team in league.get('teams', []):
                    t = team.get('team', team)
                    name = t.get('displayName', t.get('name', ''))
                    if 'spartan' in name.lower() and 'carolina' in name.lower():
                        print(f"  Found: ID={t.get('id')} Name={name}")
except Exception as e:
    print(f"  Error: {e}")

# Also try High Point (the opponent)
print("\nSearching for High Point Panthers...")
try:
    for sport in data.get('sports', []):
        for league in sport.get('leagues', []):
            for team in league.get('teams', []):
                t = team.get('team', team)
                name = t.get('displayName', t.get('name', ''))
                if 'high point' in name.lower():
                    print(f"  Found: ID={t.get('id')} Name={name}")
                    tid = t['id']
                    url2 = f"https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{tid}/schedule?season=2026"
                    req2 = Request(url2, headers={'User-Agent': 'Mozilla/5.0'})
                    resp2 = urlopen(req2, timeout=15)
                    data2 = json.loads(resp2.read().decode())
                    for e in data2.get('events', []):
                        d = str(e.get('date', ''))[:10]
                        if '2026-03-14' in d or '2026-03-15' in d:
                            name2 = e.get('name', '?')
                            for comp in e.get('competitions', []):
                                status = comp.get('status', {}).get('type', {}).get('name', '?')
                                scores = ""
                                for ct in comp.get('competitors', []):
                                    tn = ct.get('team', {}).get('displayName', '?')
                                    sc = ct.get('score', '?')
                                    scores += f" {tn}={sc}"
                                print(f"  {d} | {status} | {name2} |{scores}")
except Exception as e:
    print(f"  Error: {e}")
