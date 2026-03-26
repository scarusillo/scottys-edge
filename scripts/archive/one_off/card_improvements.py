"""
card_improvements.py — Card, Email, and Social Media Patches

Changes:
  1. HTML card shows sportsbook name on each pick
  2. HTML card includes unit explanation in footer
  3. Email includes plain text picks BELOW the HTML card
  4. Generates Twitter/Instagram caption

Usage:
    python card_improvements.py              # Preview
    python card_improvements.py --apply      # Apply
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_card')

PATCHES = []

# ══════════════════════════════════════════════════════════════
# PATCH 1: Add book name to card pick blocks
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'main.py',
    """        pick_blocks.append(f\"\"\"
    <div class="pick">
      <div class="pick-icon">{icon}</div>
      <div class="pick-info">
        <div class="pick-team">{p['selection']}</div>
        <div class="pick-matchup">{p['home']} vs {p['away']} • {game_time}</div>
        {ctx_html}
      </div>
      <div class="pick-meta">
        <div class="pick-odds">{p['odds']:+.0f}</div>
        <div class="pick-units">{p['units']:.1f} units</div>
        <div class="pick-conviction {conv_class}">{kl}</div>
      </div>
    </div>\"\"\")""",
    """        pick_blocks.append(f\"\"\"
    <div class="pick">
      <div class="pick-icon">{icon}</div>
      <div class="pick-info">
        <div class="pick-team">{p['selection']}</div>
        <div class="pick-matchup">{p['home']} vs {p['away']} • {game_time}</div>
        <div class="pick-book">📖 {p['book']}</div>
        {ctx_html}
      </div>
      <div class="pick-meta">
        <div class="pick-odds">{p['odds']:+.0f}</div>
        <div class="pick-units">{p['units']:.1f} units</div>
        <div class="pick-conviction {conv_class}">{kl}</div>
      </div>
    </div>\"\"\")""",
    "Add sportsbook name to each pick on the card"
))

# ══════════════════════════════════════════════════════════════
# PATCH 2: Add book styling + unit explanation to card
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'main.py',
    """  .pick-context {{ font-size: 12px; color: #00e676; opacity: 0.7; font-weight: 500; }}""",
    """  .pick-context {{ font-size: 12px; color: #00e676; opacity: 0.7; font-weight: 500; }}
  .pick-book {{ font-size: 12px; color: rgba(255,255,255,0.35); font-weight: 500; margin-bottom: 2px; }}""",
    "Add CSS styling for sportsbook name"
))

PATCHES.append((
    'main.py',
    """    <p>For entertainment and informational purposes only. Not gambling advice. Past performance does not guarantee future results. Please gamble responsibly. If you or someone you know has a gambling problem, call 1-800-GAMBLER. Must be 21+ and located in a legal jurisdiction. Scotty's Edge does not accept or place bets on behalf of users.</p>""",
    """    <p style="margin-bottom: 8px;"><strong style="color: rgba(255,255,255,0.4);">UNIT SIZING:</strong> 1 unit = 1% of your bankroll. If your bankroll is $1,000, one unit = $10. A 5.0u MAX PLAY at $10/unit = $50 wager. Scale to your comfort level — never bet more than you can afford to lose.</p>
    <p>For entertainment and informational purposes only. Not gambling advice. Past performance does not guarantee future results. Please gamble responsibly. If you or someone you know has a gambling problem, call 1-800-GAMBLER. Must be 21+ and located in a legal jurisdiction. Scotty's Edge does not accept or place bets on behalf of users.</p>""",
    "Add unit sizing explanation to card footer"
))

# ══════════════════════════════════════════════════════════════
# PATCH 3: Email includes plain text below the HTML card
# ══════════════════════════════════════════════════════════════
# The current MIMEMultipart('alternative') makes the client CHOOSE
# between HTML and text. We need to embed the text IN the HTML so
# both the card and the detailed output appear in one email.

PATCHES.append((
    'main.py',
    """    if do_email:
        print("\\n📧 Step 9: Sending email...")
        if all_picks:
            from emailer import send_picks_email
            text = picks_to_text(all_picks, f"{run_type} Picks")
            social = _social_media_card(all_picks)
            full_text = text + "\\n\\n" + social
            send_picks_email(full_text, run_type, html_body=html_content)""",
    """    if do_email:
        print("\\n📧 Step 9: Sending email...")
        if all_picks:
            from emailer import send_picks_email
            text = picks_to_text(all_picks, f"{run_type} Picks")
            social = _social_media_card(all_picks)
            full_text = text + "\\n\\n" + social
            # Embed plain text output below the HTML card so both appear in email
            combined_html = None
            if html_content:
                text_as_html = full_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('\\n', '<br>')
                combined_html = html_content.replace('</body>', f'''
<div style="max-width:1080px;margin:40px auto;padding:32px 52px;font-family:Consolas,monospace;font-size:12px;color:rgba(255,255,255,0.6);background:#0a0a0a;line-height:1.6;">
<div style="font-family:Barlow Condensed,sans-serif;font-size:13px;color:#00e676;letter-spacing:3px;text-transform:uppercase;margin-bottom:16px;">DETAILED BREAKDOWN</div>
{text_as_html}
</div>
</body>''')
            send_picks_email(full_text, run_type, html_body=combined_html)""",
    "Embed plain text output below HTML card in email"
))

# ══════════════════════════════════════════════════════════════
# PATCH 4: Add Twitter/Instagram caption generator
# ══════════════════════════════════════════════════════════════
# Adds a function that generates a caption and prints it + copies it.
# Integrated into the social media section of cmd_run.

PATCHES.append((
    'main.py',
    """    # Step 9c: Auto-post to Discord + Twitter
    if all_picks:
        try:
            from social_media import post_picks_social
            post_picks_social(all_picks)
        except Exception as e:
            print(f"  Social media: {e}")""",
    """    # Step 9c: Auto-post to Discord + Twitter
    if all_picks:
        try:
            from social_media import post_picks_social
            post_picks_social(all_picks)
        except Exception as e:
            print(f"  Social media: {e}")

    # Step 9d: Generate Twitter/Instagram caption
    if all_picks:
        caption = _generate_social_caption(all_picks)
        print(f"\\n📱 TWITTER / INSTAGRAM CAPTION:")
        print(f"{'─'*50}")
        print(caption)
        print(f"{'─'*50}")
        # Save to file for easy copy-paste
        caption_path = os.path.join(os.path.expanduser('~'), 'Desktop', 'scottys_edge_caption.txt')
        if not os.path.exists(os.path.dirname(caption_path)):
            caption_path = os.path.join(os.path.expanduser('~'), 'OneDrive', 'Desktop', 'scottys_edge_caption.txt')
        try:
            with open(caption_path, 'w', encoding='utf-8') as f:
                f.write(caption)
            print(f"  Saved to: {caption_path}")
        except:
            pass""",
    "Add Twitter/Instagram caption generation to run pipeline"
))

# Now add the _generate_social_caption function itself.
# Place it right after _social_media_card function.

PATCHES.append((
    'main.py',
    """def _validate_picks(picks):""",
    """def _generate_social_caption(picks):
    \"\"\"
    Generate a Twitter/Instagram caption for today's picks.
    Works for both platforms — under 280 chars for Twitter,
    includes hashtags for Instagram discoverability.
    \"\"\"
    from model_engine import kelly_label
    now = datetime.now()
    day_str = now.strftime('%A')
    date_str = now.strftime('%B %d')
    
    # Count by sport
    sport_emojis = {
        'basketball_nba': '🏀', 'basketball_ncaab': '🏀',
        'icehockey_nhl': '🏒', 'baseball_ncaa': '⚾',
    }
    sport_set = set()
    for p in picks:
        sp = p.get('sport', '')
        emoji = sport_emojis.get(sp, '⚽')
        sport_set.add(emoji)
    sports_str = ''.join(sorted(sport_set))
    
    tu = sum(p['units'] for p in picks)
    max_plays = sum(1 for p in picks if kelly_label(p['units']) == 'MAX PLAY')
    
    # Build pick summary lines
    pick_lines = []
    for p in sorted(picks, key=lambda x: x['units'], reverse=True):
        kl = kelly_label(p['units'])
        tier = '🔥' if kl == 'MAX PLAY' else '⭐' if kl == 'STRONG' else '✅'
        odds_str = f"({p['odds']:+.0f})" if p['odds'] else ''
        pick_lines.append(f"{tier} {p['selection']} {odds_str}")
    
    picks_block = '\\n'.join(pick_lines)
    
    # Sport-specific hashtags
    sport_tags = set()
    tag_map = {
        'basketball_nba': '#NBA', 'basketball_ncaab': '#CBB #MarchMadness',
        'icehockey_nhl': '#NHL', 'baseball_ncaa': '#CollegeBaseball',
        'soccer_epl': '#EPL', 'soccer_italy_serie_a': '#SerieA',
    }
    for p in picks:
        tag = tag_map.get(p.get('sport', ''))
        if tag:
            sport_tags.add(tag)
    
    hashtags = ' '.join(sorted(sport_tags))
    
    caption = f\"\"\"{sports_str} Scotty's Edge — {day_str} {date_str}

{picks_block}

{len(picks)} plays • {tu:.0f}u total
Every pick tracked & graded 📊

1 unit = 1% of bankroll. Scale to your comfort.
⚠️ Not gambling advice • 21+ • 1-800-GAMBLER

#ScottysEdge #SportsBetting #SportsPicks {hashtags}\"\"\"
    
    return caption


def _validate_picks(picks):""",
    "Add _generate_social_caption function"
))


# ══════════════════════════════════════════════════════════════
# EXECUTION
# ══════════════════════════════════════════════════════════════

def preview():
    print("=" * 65)
    print("  CARD IMPROVEMENTS (PREVIEW)")
    print("=" * 65)
    pending = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  ⚠️  {filename}: not found")
            continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text in content:
            print(f"  📝 {desc}")
            pending += 1
        elif new_text[:80] in content:
            print(f"  ✅ {desc} — already applied")
        else:
            print(f"  ⚠️  {desc} — text not found")
            print(f"      Looking for: {old_text[:80]}...")
    print(f"\n  {pending} patches to apply.")
    print(f"  Run with --apply to execute.")


def apply():
    print("=" * 65)
    print("  CARD IMPROVEMENTS — Applying")
    print("=" * 65)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  ⚠️  {filename}: not found")
            continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text not in content:
            if new_text[:80] in content:
                print(f"  ✅ {desc} — already applied")
            else:
                print(f"  ⚠️  {desc} — text mismatch")
                print(f"      Looking for: {old_text[:80]}...")
            continue
        bak = os.path.join(BACKUP_DIR, f"{filename}.{ts}.bak")
        shutil.copy2(filepath, bak)
        new_content = content.replace(old_text, new_text, 1)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  ✅ {desc}")
        success += 1
    print(f"\n  Applied {success} patches. Backups at: {BACKUP_DIR}")
    print(f"  Test: python main.py run --email")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    else:
        preview()
