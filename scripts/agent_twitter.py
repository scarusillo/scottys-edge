"""
agent_twitter.py — Scotty's Edge Twitter Auto-Poster

Auto-posts picks, results, and no-edge messages to Twitter/X.
Uses Twitter API v2 (free tier supports posting).

Setup:
  1. Go to developer.twitter.com
  2. Create a project + app
  3. Generate API keys (Consumer Keys + Access Token)
  4. Set environment variables:
     setx TWITTER_API_KEY "your_key"
     setx TWITTER_API_SECRET "your_secret"  
     setx TWITTER_ACCESS_TOKEN "your_token"
     setx TWITTER_ACCESS_SECRET "your_token_secret"

Usage:
    python agent_twitter.py --picks          # Post today's picks
    python agent_twitter.py --results        # Post today's results
    python agent_twitter.py --no-edge        # Post no-edge message
    python agent_twitter.py --test           # Test connection
"""
import os, sys, json, time, hashlib, hmac, base64
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.parse import quote

# Twitter API credentials from environment
API_KEY = os.environ.get('TWITTER_API_KEY', '')
API_SECRET = os.environ.get('TWITTER_API_SECRET', '')
ACCESS_TOKEN = os.environ.get('TWITTER_ACCESS_TOKEN', '')
ACCESS_SECRET = os.environ.get('TWITTER_ACCESS_SECRET', '')

TWITTER_API_URL = 'https://api.twitter.com/2/tweets'


def _generate_oauth_signature(method, url, params, consumer_secret, token_secret):
    """Generate OAuth 1.0a signature."""
    sorted_params = '&'.join(f"{quote(k, '')}={quote(v, '')}" for k, v in sorted(params.items()))
    base_string = f"{method}&{quote(url, '')}&{quote(sorted_params, '')}"
    signing_key = f"{quote(consumer_secret, '')}&{quote(token_secret, '')}"
    signature = base64.b64encode(
        hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha1).digest()
    ).decode()
    return signature


def _generate_oauth_header(method, url, body_params=None):
    """Generate full OAuth 1.0a Authorization header."""
    import uuid
    oauth_params = {
        'oauth_consumer_key': API_KEY,
        'oauth_nonce': uuid.uuid4().hex,
        'oauth_signature_method': 'HMAC-SHA1',
        'oauth_timestamp': str(int(time.time())),
        'oauth_token': ACCESS_TOKEN,
        'oauth_version': '1.0',
    }
    
    all_params = {**oauth_params}
    if body_params:
        all_params.update(body_params)
    
    signature = _generate_oauth_signature(method, url, all_params, API_SECRET, ACCESS_SECRET)
    oauth_params['oauth_signature'] = signature
    
    header = 'OAuth ' + ', '.join(
        f'{quote(k, "")}="{quote(v, "")}"' for k, v in sorted(oauth_params.items())
    )
    return header


def post_tweet(text, reply_to=None):
    """Post a tweet. Returns tweet ID or None."""
    if not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
        print("  Twitter: API keys not set. Run setup first.")
        return None
    
    payload = {"text": text}
    if reply_to:
        payload["reply"] = {"in_reply_to_tweet_id": reply_to}
    
    body = json.dumps(payload).encode()
    auth_header = _generate_oauth_header('POST', TWITTER_API_URL)
    
    req = Request(TWITTER_API_URL, data=body, headers={
        'Authorization': auth_header,
        'Content-Type': 'application/json',
        'User-Agent': 'ScottysEdge/1.0',
    })
    
    try:
        resp = urlopen(req, timeout=15)
        data = json.loads(resp.read().decode())
        tweet_id = data.get('data', {}).get('id')
        print(f"  Twitter: Posted (ID: {tweet_id})")
        return tweet_id
    except Exception as e:
        print(f"  Twitter: Failed — {e}")
        return None


def post_picks_thread(picks):
    """Post picks as a Twitter thread — header tweet + one per pick."""
    from scottys_edge import kelly_label
    
    filtered = [p for p in picks if p.get('units', 0) >= 4.5]
    
    if not filtered:
        return post_no_edge()
    
    now = datetime.now()
    total_u = sum(p['units'] for p in filtered)
    
    # Header tweet
    header = (f"\U0001f3c0\U0001f3d2 Scotty's Edge — {now.strftime('%A %B %d')}\n\n"
              f"{len(filtered)} plays | {total_u:.0f}u total\n\n"
              f"Picks below \u2b07\ufe0f\n\n#ScottysEdge #SportsBetting")
    
    tweet_id = post_tweet(header)
    if not tweet_id:
        return
    
    time.sleep(2)
    
    # Individual pick tweets
    for p in sorted(filtered, key=lambda x: x['units'], reverse=True):
        kl = kelly_label(p['units'])
        tier = '\U0001f525' if kl == 'MAX PLAY' else '\u2b50'
        odds_str = f"({p['odds']:+.0f})" if p.get('odds') else ''
        book = p.get('book', '')
        
        tweet = (f"{tier} {kl}: {p['selection']} {odds_str} | {book} | {p['units']:.1f}u\n"
                 f"{p.get('home', '')} vs {p.get('away', '')}\n"
                 f"\n#ScottysEdge")
        
        tweet_id = post_tweet(tweet, reply_to=tweet_id)
        time.sleep(2)


def post_results(record_str, daily_str, pick_lines):
    """Post results as a tweet."""
    tweet = (f"\U0001f4ca Scotty's Edge — {datetime.now().strftime('%A %B %d')} Results\n\n"
             f"{daily_str}\n\n"
             f"Season: {record_str}\n"
             f"Every pick tracked. Every loss shown.\n\n"
             f"#ScottysEdge #SportsBetting")
    
    return post_tweet(tweet)


def post_no_edge():
    """Post no-edge message."""
    tweet = (f"No plays tonight.\n\n"
             f"Discipline is the edge. We only bet when the data says to bet.\n\n"
             f"Back tomorrow. \U0001f4ca\n\n"
             f"#ScottysEdge #SportsBetting")
    
    return post_tweet(tweet)


def test_connection():
    """Test Twitter API connection."""
    if not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
        print("  Twitter API keys not configured.")
        print("  Set these environment variables:")
        print("    setx TWITTER_API_KEY \"your_key\"")
        print("    setx TWITTER_API_SECRET \"your_secret\"")
        print("    setx TWITTER_ACCESS_TOKEN \"your_token\"")
        print("    setx TWITTER_ACCESS_SECRET \"your_token_secret\"")
        return False
    
    print(f"  API Key: {API_KEY[:8]}...")
    print(f"  Access Token: {ACCESS_TOKEN[:8]}...")
    print("  Attempting test tweet...")
    
    result = post_tweet(f"Test from Scotty's Edge — {datetime.now().strftime('%I:%M %p')} \U0001f4ca #ScottysEdge")
    return result is not None


if __name__ == '__main__':
    if '--test' in sys.argv:
        test_connection()
    elif '--no-edge' in sys.argv:
        post_no_edge()
    elif '--results' in sys.argv:
        import sqlite3
        conn = sqlite3.connect('../data/betting_model.db')
        # Get today's results
        r = conn.execute("""
            SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
                   ROUND(SUM(pnl_units),1)
            FROM graded_bets WHERE DATE(created_at)>='2026-03-04'
            AND result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 4.5
        """).fetchone()
        record_str = f"{r[0]}W-{r[1]}L | {r[2]:+.1f}u"
        
        yesterday = conn.execute("""
            SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
                   ROUND(SUM(pnl_units),1)
            FROM graded_bets WHERE DATE(created_at) = (
                SELECT MAX(DATE(created_at)) FROM graded_bets 
                WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 4.5
            ) AND result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 4.5
        """).fetchone()
        daily_str = f"{yesterday[0]}W-{yesterday[1]}L | {yesterday[2]:+.1f}u"
        
        post_results(record_str, daily_str, [])
        conn.close()
    elif '--picks' in sys.argv:
        print("  Use: python main.py run --email --twitter")
    else:
        print(__doc__)
