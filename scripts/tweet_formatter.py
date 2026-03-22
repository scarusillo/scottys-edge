"""
tweet_formatter.py — Format picks for Twitter/X subscription service

Generates:
  1. Tweet thread (header + one tweet per pick, all <280 chars)
  2. Visual card HTML (screenshot-ready for X engagement)
  3. Results tweets (W/L tracking with running record)

Usage:
    # From main.py (automatic):
    python main.py run --twitter

    # Standalone:
    python tweet_formatter.py              # Generate from latest picks in DB
    python tweet_formatter.py --results    # Generate results thread from graded bets
"""
import sqlite3, os, sys
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
CARD_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards')

# ═══════════════════════════════════════════════════════════════════
# TWEET THREAD GENERATION
# ═══════════════════════════════════════════════════════════════════

SPORT_EMOJI = {
    'basketball_ncaab': '🏀', 'basketball_nba': '🏀',
    'icehockey_nhl': '🏒',
    'baseball_ncaa': '⚾',
    'soccer_epl': '⚽', 'soccer_italy_serie_a': '⚽',
    'soccer_spain_la_liga': '⚽', 'soccer_germany_bundesliga': '⚽',
    'soccer_france_ligue_one': '⚽', 'soccer_uefa_champs_league': '⚽',
    'soccer_usa_mls': '⚽',
}

SPORT_TAG = {
    'basketball_ncaab': '#CBB', 'basketball_nba': '#NBA',
    'icehockey_nhl': '#NHL',
    'baseball_ncaa': '#CollegeBaseball',
    'soccer_epl': '#EPL', 'soccer_italy_serie_a': '#SerieA',
    'soccer_spain_la_liga': '#LaLiga', 'soccer_germany_bundesliga': '#Bundesliga',
    'soccer_france_ligue_one': '#Ligue1', 'soccer_uefa_champs_league': '#UCL',
    'soccer_usa_mls': '#MLS',
}

TIER_EMOJI = {
    'MAX PLAY': '🔥', 'STRONG': '⭐', 'SOLID': '✅',
    'LEAN': '📊', 'SPRINKLE': '📋',
}


def _short_sport(sport):
    """Short sport label for tweets."""
    return sport.split('_')[-1].upper()


def _est_time(commence):
    """Convert ISO timestamp to EST time string."""
    if not commence:
        return ''
    try:
        gt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
        est = gt - timedelta(hours=5)
        return est.strftime('%-I:%M%p').lower()
    except:
        return ''


def _short_selection(selection):
    """Shorten selection text for tweet constraints."""
    # "Northern Illinois Huskies -1.5" → "N Illinois -1.5"
    # Keep it recognizable but compact
    sel = selection
    # Common word shortenings
    replacements = [
        ('Northern ', 'N '), ('Southern ', 'S '), ('Western ', 'W '),
        ('Eastern ', 'E '), ('Central ', 'C '),
        ('University', 'U'), ('State ', 'St '),
        (' Huskies', ''), (' Bulldogs', ''), (' Tigers', ''),
        (' Cardinals', ''), (' Bears', ''), (' Coyotes', ''),
        (' Spartans', ''), (' Badgers', ''), (' Lions', ''),
        (' Texans', ''), (' Royals', ''), (' Lancers', ''),
        (' Panthers', ''), (' Redhawks', ''), (' Dons', ''),
        (' Mustangs', ''), (' Rams', ''), (' Hawks', ''),
        (' Wolves', ''), (' Jaguars', ''), (' Jackrabbits', ''),
        (' Golden Panthers', ''), (' Roadrunners', ''),
        (' Mavericks', ''),
    ]
    for old, new in replacements:
        sel = sel.replace(old, new)
    return sel.strip()


def generate_tweet_thread(picks):
    """
    Generate a list of tweet strings for a thread.

    Returns: list of strings, each <280 chars.
    First tweet = header card, rest = individual picks.
    """
    if not picks:
        return ["No plays today. Patience IS the edge. 🎯"]

    from scottys_edge import kelly_label

    today = datetime.now().strftime('%m/%d')
    day_name = datetime.now().strftime('%A')

    # ── Header tweet ──
    total_units = sum(p['units'] for p in picks)
    sports_seen = set()
    for p in picks:
        tag = SPORT_TAG.get(p['sport'], '')
        if tag:
            sports_seen.add(tag)

    max_play = [p for p in picks if kelly_label(p['units']) == 'MAX PLAY']
    strong = [p for p in picks if kelly_label(p['units']) == 'STRONG']

    header = f"🎯 {day_name} Card — {today}\n\n"
    header += f"{len(picks)} plays | {total_units:.0f}u total"
    if max_play:
        header += f" | {len(max_play)} MAX PLAY"
    if strong:
        header += f" | {len(strong)} STRONG"
    header += f"\n\n{' '.join(sorted(sports_seen))}\n\n🧵👇"

    tweets = [header]

    # ── Individual pick tweets ──
    for i, p in enumerate(picks, 1):
        units = p['units']
        kl = kelly_label(units)
        icon = TIER_EMOJI.get(kl, '📋')
        sport_emoji = SPORT_EMOJI.get(p['sport'], '🎯')
        tag = SPORT_TAG.get(p['sport'], '')
        time_str = _est_time(p.get('commence', ''))

        # Compact selection
        sel = _short_selection(p['selection'])

        # Build tweet — must be <280 chars
        tweet = f"{icon} {sel}\n"
        tweet += f"{sport_emoji} {p['book']} | {p['odds']:+.0f}\n"
        tweet += f"📐 {units:.1f}u {kl} | Edge: {p['edge_pct']:.1f}%"
        if time_str:
            tweet += f" | {time_str}"

        # Add timing
        timing = p.get('timing', '')
        if timing == 'EARLY':
            tweet += "\n⏰ Bet early"
        elif timing == 'LATE':
            tweet += "\n⏳ Wait for best line"

        # Add context factor (only the first one, space permitting)
        ctx = p.get('context', '')
        if ctx and len(tweet) + len(ctx) + 5 < 270:
            # Grab first factor only to stay under 280
            first_factor = ctx.split(' | ')[0]
            tweet += f"\n📍 {first_factor}"

        if tag:
            tweet += f"\n{tag}"

        # Safety check
        if len(tweet) > 280:
            # Emergency trim — drop book name
            tweet = f"{icon} {sel}\n"
            tweet += f"{p['odds']:+.0f} | {units:.1f}u {kl} | Edge: {p['edge_pct']:.1f}%"
            if tag:
                tweet += f" {tag}"

        tweets.append(tweet)

    # ── Footer tweet ──
    footer = f"📋 Full card: {len(picks)} plays | {total_units:.0f}u\n\n"
    footer += "Model: Scotty's Edge v11\n"
    footer += "Kelly-sized | CLV-tracked\n\n"
    footer += "Like + RT if tailing 🤝"

    tweets.append(footer)

    return tweets


def generate_results_thread(graded_bets):
    """Generate a results thread from today's graded bets."""
    if not graded_bets:
        return []

    from scottys_edge import kelly_label

    wins = sum(1 for b in graded_bets if b['result'] == 'WIN')
    losses = sum(1 for b in graded_bets if b['result'] == 'LOSS')
    pushes = sum(1 for b in graded_bets if b['result'] == 'PUSH')
    total_pnl = sum(b['pnl'] for b in graded_bets)
    clv_vals = [b['clv'] for b in graded_bets if b.get('clv') is not None]
    avg_clv = sum(clv_vals) / len(clv_vals) if clv_vals else None

    today = datetime.now().strftime('%m/%d')
    emoji = '🟢' if total_pnl > 0 else ('🔴' if total_pnl < 0 else '⚪')

    # Header
    header = f"{emoji} Results — {today}\n\n"
    header += f"{wins}W-{losses}L"
    if pushes:
        header += f"-{pushes}P"
    header += f" | {total_pnl:+.1f}u"
    if avg_clv is not None:
        clv_emoji = '✅' if avg_clv > 0 else '⚠️'
        header += f"\nCLV: {avg_clv:+.1f} pts {clv_emoji}"
    header += "\n\n🧵👇"

    tweets = [header]

    for b in graded_bets:
        if b['result'] == 'DUPLICATE':
            continue
        icon = '✅' if b['result'] == 'WIN' else ('❌' if b['result'] == 'LOSS' else '➖')
        clv_str = f" | CLV: {b['clv']:+.1f}" if b.get('clv') is not None else ""
        tweet = f"{icon} {b['selection']}\n{b['pnl']:+.1f}u{clv_str}"
        tweets.append(tweet)

    return tweets


# ═══════════════════════════════════════════════════════════════════
# VISUAL CARD (HTML → screenshot for X)
# ═══════════════════════════════════════════════════════════════════

def generate_card_html(picks):
    """
    Generate a dark-mode visual card as HTML.
    Screenshot this at 1200x630 (Twitter card ratio) for maximum engagement.

    Save to data/cards/card_YYYY-MM-DD.html — open in browser and screenshot.
    """
    if not picks:
        return ""

    from scottys_edge import kelly_label

    today = datetime.now().strftime('%A, %B %d')
    today_short = datetime.now().strftime('%Y-%m-%d')
    total_units = sum(p['units'] for p in picks)

    # Build pick rows
    pick_rows = ""
    for p in picks:
        units = p['units']
        kl = kelly_label(units)
        tier_class = kl.lower().replace(' ', '-')
        sport_label = _short_sport(p['sport'])
        time_str = _est_time(p.get('commence', ''))
        icon = TIER_EMOJI.get(kl, '📋')

        # Unit bar (visual weight indicator)
        bar_width = min(100, int((units / 5.0) * 100))

        pick_rows += f"""
        <div class="pick-row {tier_class}">
            <div class="pick-tier">{icon}</div>
            <div class="pick-main">
                <div class="pick-sel">{p['selection']}</div>
                <div class="pick-meta">
                    <span class="sport-badge">{sport_label}</span>
                    <span>{p['book']} {p['odds']:+.0f}</span>
                    {f'<span class="time">{time_str}</span>' if time_str else ''}
                </div>
            </div>
            <div class="pick-right">
                <div class="pick-units">{units:.1f}u</div>
                <div class="pick-label">{kl}</div>
                <div class="pick-edge">Edge: {p['edge_pct']:.1f}%</div>
                <div class="unit-bar"><div class="unit-fill" style="width:{bar_width}%"></div></div>
            </div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Space+Grotesk:wght@400;600;700&display=swap');

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    background: #0a0a0f;
    color: #e8e8ed;
    font-family: 'Space Grotesk', sans-serif;
    width: 800px;
    padding: 0;
  }}

  .card {{
    background: linear-gradient(135deg, #0d1117 0%, #161b22 50%, #0d1117 100%);
    border: 1px solid #21262d;
    border-radius: 16px;
    overflow: hidden;
  }}

  .card-header {{
    padding: 28px 32px 20px;
    border-bottom: 1px solid #21262d;
    background: linear-gradient(90deg, rgba(56,139,253,0.06) 0%, transparent 100%);
  }}

  .brand {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: #388bfd;
    margin-bottom: 8px;
  }}

  .title {{
    font-size: 28px;
    font-weight: 700;
    color: #f0f3f6;
    line-height: 1.2;
  }}

  .subtitle {{
    font-size: 14px;
    color: #7d8590;
    margin-top: 6px;
  }}

  .stats-bar {{
    display: flex;
    gap: 24px;
    padding: 14px 32px;
    background: rgba(56,139,253,0.04);
    border-bottom: 1px solid #21262d;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
  }}

  .stat {{ color: #7d8590; }}
  .stat strong {{ color: #e8e8ed; font-weight: 600; }}

  .picks-list {{
    padding: 8px 0;
  }}

  .pick-row {{
    display: flex;
    align-items: center;
    padding: 14px 32px;
    border-bottom: 1px solid rgba(33,38,45,0.6);
    transition: background 0.2s;
  }}

  .pick-row:last-child {{ border-bottom: none; }}

  .pick-row.max-play {{
    background: rgba(187,128,9,0.08);
    border-left: 3px solid #d29922;
  }}

  .pick-row.strong {{
    background: rgba(56,139,253,0.04);
    border-left: 3px solid #388bfd;
  }}

  .pick-row.solid {{
    border-left: 3px solid #3fb950;
  }}

  .pick-row.lean {{
    border-left: 3px solid #7d8590;
  }}

  .pick-tier {{
    font-size: 22px;
    width: 40px;
    text-align: center;
    flex-shrink: 0;
  }}

  .pick-main {{
    flex: 1;
    min-width: 0;
  }}

  .pick-sel {{
    font-size: 16px;
    font-weight: 600;
    color: #f0f3f6;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}

  .pick-meta {{
    font-size: 12px;
    color: #7d8590;
    margin-top: 3px;
    display: flex;
    gap: 10px;
  }}

  .sport-badge {{
    background: rgba(56,139,253,0.15);
    color: #388bfd;
    padding: 1px 6px;
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.5px;
  }}

  .time {{
    color: #57606a;
  }}

  .pick-right {{
    text-align: right;
    flex-shrink: 0;
    width: 110px;
  }}

  .pick-units {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 20px;
    font-weight: 700;
    color: #f0f3f6;
  }}

  .pick-label {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: #7d8590;
  }}

  .pick-edge {{
    font-size: 11px;
    color: #3fb950;
    margin-top: 2px;
  }}

  .unit-bar {{
    height: 3px;
    background: #21262d;
    border-radius: 2px;
    margin-top: 4px;
    overflow: hidden;
  }}

  .unit-fill {{
    height: 100%;
    border-radius: 2px;
    background: linear-gradient(90deg, #388bfd, #3fb950);
  }}

  .max-play .pick-units {{ color: #d29922; }}
  .max-play .pick-edge {{ color: #d29922; }}
  .max-play .unit-fill {{ background: linear-gradient(90deg, #d29922, #e8b931); }}

  .card-footer {{
    padding: 16px 32px;
    border-top: 1px solid #21262d;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 12px;
    color: #484f58;
    font-family: 'JetBrains Mono', monospace;
  }}

  .card-footer .model {{ color: #388bfd; }}
</style>
</head>
<body>
  <div class="card">
    <div class="card-header">
      <div class="brand">Scotty's Edge</div>
      <div class="title">{today}</div>
      <div class="subtitle">Model-driven picks • Kelly-sized • CLV-tracked</div>
    </div>
    <div class="stats-bar">
      <div class="stat"><strong>{len(picks)}</strong> plays</div>
      <div class="stat"><strong>{total_units:.0f}u</strong> total</div>
      <div class="stat"><strong>1/8</strong> Kelly</div>
    </div>
    <div class="picks-list">
      {pick_rows}
    </div>
    <div class="card-footer">
      <span class="model">v11 • {today_short}</span>
      <span>@ScottysEdge</span>
    </div>
  </div>
</body>
</html>"""

    return html


def save_card(picks):
    """Save visual card HTML to data/cards/ directory."""
    os.makedirs(CARD_DIR, exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    hour = datetime.now().strftime('%H%M')
    path = os.path.join(CARD_DIR, f'card_{today}_{hour}.html')
    html = generate_card_html(picks)
    if html:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"  📸 Card saved: {path}")
        print(f"     Open in browser → screenshot at 800px width")
    return path


# ═══════════════════════════════════════════════════════════════════
# CLIPBOARD HELPER
# ═══════════════════════════════════════════════════════════════════

def copy_thread_to_clipboard(tweets):
    """Print tweets separated by dividers for easy copy-paste."""
    print(f"\n{'='*60}")
    print(f"  TWITTER THREAD — {len(tweets)} tweets")
    print(f"  Copy each block between the lines")
    print(f"{'='*60}")

    for i, tweet in enumerate(tweets):
        label = "HEADER" if i == 0 else ("FOOTER" if i == len(tweets) - 1 else f"PICK {i}")
        chars = len(tweet)
        status = '✅' if chars <= 280 else f'❌ {chars} chars!'
        print(f"\n  ┌─ {label} ({chars} chars) {status}")
        print(f"  │")
        for line in tweet.split('\n'):
            print(f"  │  {line}")
        print(f"  │")
        print(f"  └─────────────────────────")


# ═══════════════════════════════════════════════════════════════════
# INTEGRATION: Generate from DB
# ═══════════════════════════════════════════════════════════════════

def twitter_from_db():
    """Pull latest picks from bets table and generate Twitter content."""
    conn = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime('%Y-%m-%d')

    rows = conn.execute("""
        SELECT sport, event_id, market_type, selection, book, line, odds,
               edge_pct, confidence, units, created_at
        FROM bets
        WHERE DATE(created_at) = ?
        ORDER BY units DESC, edge_pct DESC
    """, (today,)).fetchall()

    if not rows:
        print("  No picks in DB for today.")
        conn.close()
        return

    # Deduplicate
    seen = set()
    picks = []
    for r in rows:
        key = f"{r[1]}|{r[3]}|{r[2]}"
        if key in seen:
            continue
        seen.add(key)

        # Get game time
        mc = conn.execute("""
            SELECT commence_time, home, away FROM market_consensus
            WHERE event_id = ? LIMIT 1
        """, (r[1],)).fetchone()

        picks.append({
            'sport': r[0], 'event_id': r[1], 'market_type': r[2],
            'selection': r[3], 'book': r[4], 'line': r[5], 'odds': r[6],
            'edge_pct': r[7], 'confidence': r[8], 'units': r[9],
            'commence': mc[0] if mc else '', 'home': mc[1] if mc else '',
            'away': mc[2] if mc else '', 'timing': '',
        })

    conn.close()

    # Generate content
    tweets = generate_tweet_thread(picks)
    copy_thread_to_clipboard(tweets)
    card_path = save_card(picks)

    return tweets, card_path


def results_from_db(days_back=1):
    """Pull graded bets and generate results thread."""
    conn = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

    rows = conn.execute("""
        SELECT sport, selection, market_type, result, pnl_units, clv, created_at
        FROM graded_bets
        WHERE DATE(created_at) >= ? AND result != 'DUPLICATE'
        ORDER BY created_at
    """, (cutoff,)).fetchall()

    conn.close()

    graded = [{'sport': r[0], 'selection': r[1], 'market_type': r[2],
               'result': r[3], 'pnl': r[4] or 0, 'clv': r[5]} for r in rows]

    if graded:
        tweets = generate_results_thread(graded)
        copy_thread_to_clipboard(tweets)
        return tweets
    else:
        print("  No graded bets to report.")
        return []


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    if '--results' in sys.argv:
        results_from_db()
    else:
        twitter_from_db()
