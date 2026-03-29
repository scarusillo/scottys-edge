#!/usr/bin/env python3
"""
reddit_poster.py — Post picks and results to Reddit via PRAW.

Posts to r/sportsbetting (standalone) and r/sportsbook (daily thread comment).
Requires: pip install praw

Setup:
  1. Create app at https://www.reddit.com/prefs/apps (type: script)
  2. Set environment variables:
     REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET,
     REDDIT_USERNAME, REDDIT_PASSWORD

Usage:
    python reddit_poster.py picks        # Post today's picks
    python reddit_poster.py results      # Post yesterday's results
"""
import os, sys, sqlite3
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Target subreddits
PICKS_SUBREDDITS = ['sportsbetting']
RESULTS_SUBREDDITS = ['sportsbetting']


def get_reddit():
    """Initialize PRAW Reddit instance."""
    try:
        import praw
    except ImportError:
        print("  Reddit: praw not installed. Run: pip install praw")
        return None

    client_id = os.environ.get('REDDIT_CLIENT_ID')
    client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
    username = os.environ.get('REDDIT_USERNAME')
    password = os.environ.get('REDDIT_PASSWORD')

    if not all([client_id, client_secret, username, password]):
        print("  Reddit: Missing credentials (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD)")
        return None

    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password,
        user_agent="ScottysEdge/1.0 by u/" + username
    )


def format_picks_post(conn):
    """Format today's picks for Reddit."""
    today = datetime.now().strftime('%Y-%m-%d')
    day_name = datetime.now().strftime('%A %B %d, %Y')

    picks = conn.execute("""
        SELECT selection, sport, market_type, odds, units, edge_pct, context_factors
        FROM bets
        WHERE DATE(created_at) = ? AND units >= 3.5
        ORDER BY units DESC
    """, (today,)).fetchall()

    if not picks:
        return None, None

    # Season record
    all_bets = conn.execute("""
        SELECT result, pnl_units FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchall()
    tw = sum(1 for b in all_bets if b[0] == 'WIN')
    tl = sum(1 for b in all_bets if b[0] == 'LOSS')
    tp = sum(b[1] or 0 for b in all_bets)
    twp = tw / (tw + tl) * 100 if (tw + tl) > 0 else 0

    sport_labels = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL', 'baseball_ncaa': 'College Baseball',
        'baseball_mlb': 'MLB',
    }

    title = f"Scotty's Edge Model Picks — {day_name} ({tw}W-{tl}L season)"

    lines = [
        f"**Season Record:** {tw}W-{tl}L | {tp:+.1f}u | {twp:.1f}% Win Rate",
        f"",
        f"Every pick tracked since March 4. Every loss shown. Model-driven, no gut calls.",
        f"",
        f"---",
        f"",
        f"**Today's Picks ({len(picks)} plays):**",
        f"",
    ]

    for sel, sport, mkt, odds, units, edge, ctx in picks:
        sport_label = sport_labels.get(sport, sport.replace('soccer_', '').replace('_', ' ').title())
        odds_str = f"+{int(odds)}" if odds > 0 else str(int(odds))
        lines.append(f"- **{sel}** ({odds_str}) — {units}u | {sport_label}")
        if ctx:
            # Show first context factor only (keep it concise)
            first_ctx = ctx.split('|')[0].strip()
            if first_ctx:
                lines.append(f"  - *Context: {first_ctx}*")

    lines.extend([
        f"",
        f"---",
        f"",
        f"Model uses Elo ratings, contextual adjustments (rest, travel, pace, pitching), "
        f"and closing line value analysis across 8+ books. Only fires when edge > threshold.",
        f"",
        f"Full transparency — results posted daily with W/L and P/L.",
        f"",
        f"*Not gambling advice. 21+. Please gamble responsibly.*",
    ])

    return title, "\n".join(lines)


def format_results_post(conn):
    """Format yesterday's results for Reddit."""
    # Find most recent grading date
    game_date = conn.execute("""
        SELECT MAX(DATE(created_at)) FROM graded_bets
        WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5
    """).fetchone()[0]

    if not game_date:
        return None, None

    game_dt = datetime.strptime(game_date, '%Y-%m-%d')
    day_name = game_dt.strftime('%A %B %d')

    bets = conn.execute("""
        SELECT selection, result, pnl_units, sport, clv
        FROM graded_bets
        WHERE DATE(created_at) = ? AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
        AND units >= 3.5
        ORDER BY pnl_units DESC
    """, (game_date,)).fetchall()

    if not bets:
        return None, None

    yw = sum(1 for b in bets if b[1] == 'WIN')
    yl = sum(1 for b in bets if b[1] == 'LOSS')
    yp = sum(b[2] or 0 for b in bets)

    # Season totals
    all_bets = conn.execute("""
        SELECT result, pnl_units FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchall()
    tw = sum(1 for b in all_bets if b[0] == 'WIN')
    tl = sum(1 for b in all_bets if b[0] == 'LOSS')
    tp = sum(b[1] or 0 for b in all_bets)
    twp = tw / (tw + tl) * 100 if (tw + tl) > 0 else 0

    verdict = ""
    if yp >= 10:
        verdict = "Big day."
    elif yp >= 0:
        verdict = "Green day."
    elif yp >= -5:
        verdict = "Minor loss."
    else:
        verdict = "Tough day. Full transparency — every pick tracked."

    title = f"Scotty's Edge Results — {day_name}: {yw}W-{yl}L ({yp:+.1f}u)"

    lines = [
        f"**{day_name} Results: {yw}W-{yl}L | {yp:+.1f}u** — {verdict}",
        f"",
        f"**Season: {tw}W-{tl}L | {tp:+.1f}u | {twp:.1f}% Win Rate**",
        f"",
        f"---",
        f"",
    ]

    # Winners
    winners = [b for b in bets if b[1] == 'WIN']
    losers = [b for b in bets if b[1] == 'LOSS']
    pushes = [b for b in bets if b[1] == 'PUSH']

    if winners:
        lines.append("**Winners:**")
        lines.append("")
        for sel, res, pnl, sport, clv in winners:
            clv_str = f" (CLV: {clv:+.1f})" if clv else ""
            lines.append(f"- {sel} — **{pnl:+.1f}u**{clv_str}")
        lines.append("")

    if losers:
        lines.append("**Losses:**")
        lines.append("")
        for sel, res, pnl, sport, clv in losers:
            clv_str = f" (CLV: {clv:+.1f})" if clv else ""
            lines.append(f"- {sel} — {pnl:+.1f}u{clv_str}")
        lines.append("")

    if pushes:
        lines.append("**Pushes:**")
        lines.append("")
        for sel, res, pnl, sport, clv in pushes:
            lines.append(f"- {sel} — push")
        lines.append("")

    lines.extend([
        f"---",
        f"",
        f"Model-driven picks. Elo ratings + contextual analysis + CLV tracking. "
        f"Every pick tracked since day one.",
        f"",
        f"*Not gambling advice. 21+. Please gamble responsibly.*",
    ])

    return title, "\n".join(lines)


def post_picks(conn=None):
    """Post today's picks to Reddit."""
    reddit = get_reddit()
    if not reddit:
        return False

    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close_conn = True

    title, body = format_picks_post(conn)
    if close_conn:
        conn.close()

    if not title:
        print("  Reddit: No picks to post")
        return False

    for sub in PICKS_SUBREDDITS:
        try:
            subreddit = reddit.subreddit(sub)
            submission = subreddit.submit(title=title, selftext=body)
            print(f"  Reddit: Posted picks to r/{sub} — {submission.url}")
        except Exception as e:
            print(f"  Reddit r/{sub}: {e}")

    return True


def post_results(conn=None):
    """Post yesterday's results to Reddit."""
    reddit = get_reddit()
    if not reddit:
        return False

    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close_conn = True

    title, body = format_results_post(conn)
    if close_conn:
        conn.close()

    if not title:
        print("  Reddit: No results to post")
        return False

    for sub in RESULTS_SUBREDDITS:
        try:
            subreddit = reddit.subreddit(sub)
            submission = subreddit.submit(title=title, selftext=body)
            print(f"  Reddit: Posted results to r/{sub} — {submission.url}")
        except Exception as e:
            print(f"  Reddit r/{sub}: {e}")

    return True


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python reddit_poster.py [picks|results]")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    cmd = sys.argv[1].lower()

    if cmd == 'picks':
        post_picks(conn)
    elif cmd == 'results':
        post_results(conn)
    else:
        print(f"Unknown command: {cmd}")

    conn.close()
