from urllib.request import Request, urlopen
import json
url = 'https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/364/schedule?season=2026'
req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
data = json.loads(urlopen(req, timeout=15).read().decode())
events = data.get('events', [])
print(f"{len(events)} events for High Point")
for e in events:
    d = str(e.get('date', ''))[:10]
    name = e.get('name', '?')
    print(f"  {d} | {name}")
