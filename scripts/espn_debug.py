"""
espn_debug.py — Run this to diagnose why ESPN injuries aren't loading.
Just run: python espn_debug.py
"""
import sys
print(f"Python version: {sys.version}")

# Test 1: Can we import requests?
print("\n--- TEST 1: requests library ---")
try:
    import requests
    print(f"  ✅ requests installed (version {requests.__version__})")
    HAS_REQUESTS = True
except ImportError:
    print("  ❌ requests NOT installed")
    print("  Fix: python -m pip install requests")
    HAS_REQUESTS = False

# Test 2: Can we reach ESPN at all?
print("\n--- TEST 2: Basic internet connectivity ---")
if HAS_REQUESTS:
    try:
        r = requests.get("https://www.google.com", timeout=10)
        print(f"  ✅ Google reachable (status {r.status_code})")
    except Exception as e:
        print(f"  ❌ Can't reach Google: {e}")

# Test 3: ESPN JSON API
print("\n--- TEST 3: ESPN JSON API ---")
url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
if HAS_REQUESTS:
    try:
        r = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }, timeout=15, verify=False)
        print(f"  Status: {r.status_code}")
        print(f"  Content length: {len(r.text)} bytes")
        data = r.json()
        print(f"  JSON keys: {list(data.keys())}")
        if 'items' in data:
            print(f"  Teams with injuries: {len(data['items'])}")
            for team in data['items'][:3]:
                t = team.get('team', {}).get('displayName', '?')
                injs = team.get('injuries', [])
                print(f"    {t}: {len(injs)} injuries")
                for inj in injs[:2]:
                    p = inj.get('athlete', {}).get('displayName', '?')
                    s = inj.get('status', '?')
                    print(f"      {p} — {s}")
        else:
            print(f"  Response preview: {r.text[:500]}")
    except Exception as e:
        print(f"  ❌ Failed: {e}")
else:
    # Try urllib
    print("  Trying urllib instead...")
    try:
        import ssl
        from urllib.request import urlopen, Request
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        resp = urlopen(req, timeout=15, context=ctx)
        data = resp.read().decode('utf-8')
        print(f"  Status: {resp.status}")
        print(f"  Content length: {len(data)} bytes")
        import json
        j = json.loads(data)
        print(f"  JSON keys: {list(j.keys())}")
    except Exception as e:
        print(f"  ❌ Failed: {type(e).__name__}: {e}")

# Test 4: ESPN HTML page
print("\n--- TEST 4: ESPN HTML injury page ---")
html_url = "https://www.espn.com/nba/injuries"
if HAS_REQUESTS:
    try:
        r = requests.get(html_url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }, timeout=15, verify=False)
        print(f"  Status: {r.status_code}")
        print(f"  Content length: {len(r.text)} bytes")
        if 'injury' in r.text.lower() or 'Out' in r.text:
            print(f"  ✅ Contains injury data")
        else:
            print(f"  ⚠ Page loaded but may not have injury content")
        print(f"  Preview: {r.text[:300]}...")
    except Exception as e:
        print(f"  ❌ Failed: {e}")

print("\n--- DONE ---")
print("Copy all of this output and send it back to me.")
