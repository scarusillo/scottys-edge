#!/usr/bin/env python3
"""
SOCIAL MEDIA AUTOMATION — Discord + Instagram

Auto-posts picks to Discord via webhook and Instagram via instagrapi.
Integrated into main.py run pipeline.

Discord: Free, uses webhooks (no approval needed)
Instagram: Auto-post via instagrapi (set IG_USERNAME + IG_PASSWORD)

v25.3 (April 2026): Twitter/X removed — @Scottys_Edge account
permanently suspended. All Twitter posting code deleted.

Setup:
  Discord:   Set DISCORD_WEBHOOK_URL environment variable
  Instagram: Set IG_USERNAME + IG_PASSWORD (or use ig_session.json)
"""
import os
import json
import urllib.request
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════
# DISCORD
# ═══════════════════════════════════════════════════════════════════

DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL', '')


def post_to_discord(picks):
    """Post formatted picks to Discord via webhook."""
    if not DISCORD_WEBHOOK_URL:
        print("  Discord: No webhook URL set")
        return False
    
    try:
        from model_engine import _to_eastern, _eastern_tz_label, kelly_label
    except ImportError:
        print("  Discord: Could not import model_engine")
        return False
    
    tz = _eastern_tz_label()
    now = datetime.now()
    date_str = now.strftime('%A, %B %d %Y')
    
    # Build Discord embed
    sport_icons = {
        'basketball_nba': '🏀', 'basketball_ncaab': '🏀',
        'icehockey_nhl': '🏒', 'baseball_ncaa': '⚾', 'baseball_mlb': '⚾',
        'soccer_epl': '⚽', 'soccer_germany_bundesliga': '⚽',
        'soccer_france_ligue_one': '⚽', 'soccer_italy_serie_a': '⚽',
        'soccer_spain_la_liga': '⚽', 'soccer_usa_mls': '⚽',
        'soccer_uefa_champs_league': '⚽', 'soccer_mexico_ligamx': '⚽',
    }
    
    # Build pick lines — sorted by confidence (highest first)
    picks = sorted(picks, key=lambda p: p['units'], reverse=True)
    pick_lines = []
    for p in picks:
        kl = kelly_label(p['units'])
        icon = sport_icons.get(p.get('sport', ''), '🏟️')
        odds_str = f"{p['odds']:+.0f}" if p['odds'] else ''
        tier = '🔥' if kl == 'MAX PLAY' else '⭐' if kl == 'STRONG' else '✅'
        
        game_time = ''
        if p.get('commence'):
            try:
                gt = datetime.fromisoformat(p['commence'].replace('Z', '+00:00'))
                est = _to_eastern(gt)
                game_time = est.strftime('%I:%M %p')
            except Exception:
                pass
        
        line = f"{tier} {icon} **{p['selection']}** ({odds_str}) • {p['units']:.0f}u {kl}"
        if game_time:
            line += f" • {game_time} {tz}"
        pick_lines.append(line)
        
        # Add context if available
        if p.get('context'):
            pick_lines.append(f"  └ 📍 {p['context']}")
    
    tu = sum(p['units'] for p in picks)
    
    # Discord embed (rich formatting)
    embed = {
        "embeds": [{
            "title": f"🎯 SCOTTY'S EDGE — {date_str}",
            "description": '\n'.join(pick_lines),
            "color": 0x00e676,  # Green accent
            "footer": {
                "text": f"{len(picks)} plays • {tu:.0f}u total • IG: @scottys_edge • X: @Scottys_edge • Not gambling advice • 21+"
            },
            "timestamp": now.isoformat()
        }],
        "username": "Scotty's Edge",
    }
    
    data = json.dumps(embed).encode('utf-8')
    req = urllib.request.Request(
        DISCORD_WEBHOOK_URL,
        data=data,
        headers={'Content-Type': 'application/json', 'User-Agent': 'ScottysEdge/1.0'},
        method='POST'
    )
    
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status in (200, 204):
                print(f"  Discord: ✅ Posted {len(picks)} picks")
                return True
            else:
                print(f"  Discord: ❌ Status {resp.status}")
                return False
    except Exception as e:
        print(f"  Discord: ❌ {e}")
        return False


def post_results_to_discord(report_text):
    """Post clean public-facing results to Discord."""
    if not DISCORD_WEBHOOK_URL:
        return False
    
    # Parse key stats from report
    import re
    
    record = re.search(r'Record: (\d+W-\d+L)', report_text)
    pl = re.search(r'P/L: ([+-]?\d+\.\d+)u', report_text)
    roi = re.search(r'ROI: ([+-]?\d+\.\d+)%', report_text)
    
    record_str = record.group(1) if record else 'N/A'
    pl_str = pl.group(1) if pl else 'N/A'
    roi_str = roi.group(1) if roi else 'N/A'
    
    # Extract today's picks
    today_picks = []
    in_picks = False
    for line in report_text.split('\n'):
        if 'PICKS FROM' in line:
            in_picks = True
            continue
        if in_picks and ('=====' in line or line.strip() == ''):
            if today_picks:
                break
            continue
        if in_picks and line.strip():
            today_picks.append(line.strip())
    
    # Build pick results
    pick_lines = []
    for p in today_picks:
        pick_lines.append(p)
    
    # Extract best performers by sport
    sport_lines = []
    in_sport = False
    for line in report_text.split('\n'):
        if '── BY SPORT' in line:
            in_sport = True
            continue
        if in_sport and '──' in line:
            break
        if in_sport and line.strip() and 'W-' in line:
            sport_lines.append(line.strip())
    
    # Build Discord embed
    picks_str = '\n'.join(pick_lines[:10]) if pick_lines else 'No picks graded'
    
    # Color based on P/L
    try:
        pl_val = float(pl_str)
        color = 0x00e676 if pl_val >= 0 else 0xff5252
    except Exception:
        color = 0x666666
    
    now = datetime.now()
    
    embed = {
        "embeds": [{
            "title": f"📊 SCOTTY'S EDGE — Daily Results",
            "description": f"**Overall: {record_str} | {pl_str}u | {roi_str}% ROI**\n\n**Today's Results:**\n```\n{picks_str}\n```",
            "color": color,
            "footer": {
                "text": f"IG: @scottys_edge • X: @Scottys_edge • Not gambling advice • 21+"
            },
            "timestamp": now.isoformat()
        }],
        "username": "Scotty's Edge",
    }
    
    data = json.dumps(embed).encode('utf-8')
    req = urllib.request.Request(
        DISCORD_WEBHOOK_URL,
        data=data,
        headers={'Content-Type': 'application/json', 'User-Agent': 'ScottysEdge/1.0'},
        method='POST'
    )
    
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status in (200, 204):
                print(f"  Discord: ✅ Posted results")
                return True
    except Exception as e:
        print(f"  Discord: ❌ {e}")
    return False


# ═══════════════════════════════════════════════════════════════════
# TWITTER/X — removed (account permanently suspended April 2026)


# ═══════════════════════════════════════════════════════════════════
# UNIFIED POST FUNCTION
# ═══════════════════════════════════════════════════════════════════

def post_picks_social(picks):
    """Post picks to all configured social platforms."""
    if not picks:
        return

    print("\n📱 Posting to social media...")
    post_to_discord(picks)


def post_results_social(report_text):
    """Post grading results to all platforms."""
    post_results_to_discord(report_text)


# ═══════════════════════════════════════════════════════════════════
# INSTAGRAM
# ═══════════════════════════════════════════════════════════════════

IG_USERNAME = os.environ.get('IG_USERNAME', '')
IG_PASSWORD = os.environ.get('IG_PASSWORD', '')
IG_SESSION_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'ig_session.json')


def _get_ig_client():
    """Get authenticated Instagram client, reusing session if possible."""
    try:
        from instagrapi import Client
    except ImportError:
        print("  Instagram: instagrapi not installed (pip install instagrapi)")
        return None

    if not IG_USERNAME or not IG_PASSWORD:
        print("  Instagram: No credentials (set IG_USERNAME + IG_PASSWORD env vars)")
        return None

    cl = Client()
    # Set realistic device/user-agent to avoid bot detection
    cl.delay_range = [2, 5]  # 2-5 second random delay between API calls

    # Try to reuse saved session to avoid login challenges
    if os.path.exists(IG_SESSION_PATH):
        try:
            cl.load_settings(IG_SESSION_PATH)
            cl.login(IG_USERNAME, IG_PASSWORD)
            cl.get_timeline_feed()  # Verify session is valid
            return cl
        except Exception:
            pass  # Session expired, do fresh login

    try:
        cl.login(IG_USERNAME, IG_PASSWORD)
        cl.dump_settings(IG_SESSION_PATH)
        return cl
    except Exception as e:
        print(f"  Instagram: Login failed — {e}")
        return None


def _prepare_for_ig(image_paths):
    """Convert PNGs to JPEGs (required for carousels) and return paths."""
    from PIL import Image
    prepared = []
    for p in image_paths:
        if not os.path.exists(p):
            continue
        if p.lower().endswith('.png'):
            jpg_path = p.rsplit('.', 1)[0] + '_ig.jpg'
            img = Image.open(p).convert('RGB')
            img.save(jpg_path, 'JPEG', quality=95)
            prepared.append(jpg_path)
        else:
            prepared.append(p)
    return prepared


def _make_story_image(image_path):
    """Resize a 4:5 card to 9:16 story format (1080x1920) with black bars."""
    from PIL import Image
    story_w, story_h = 1080, 1920
    img = Image.open(image_path).convert('RGB')

    # Scale image to fit within story dimensions (width-constrained)
    scale = story_w / img.width
    new_w = story_w
    new_h = int(img.height * scale)

    img_resized = img.resize((new_w, new_h), Image.LANCZOS)

    # Create black background and paste centered
    story = Image.new('RGB', (story_w, story_h), (0, 0, 0))
    y_offset = (story_h - new_h) // 2
    story.paste(img_resized, (0, y_offset))

    story_path = image_path.rsplit('.', 1)[0] + '_story.jpg'
    story.save(story_path, 'JPEG', quality=95)
    return story_path


def post_reel_to_instagram(video_path, caption):
    """Post a video as an Instagram Reel.

    Args:
        video_path: str — path to MP4 file
        caption: str — the reel caption

    Returns:
        bool — True if posted successfully
    """
    cl = _get_ig_client()
    if not cl:
        return False

    if not os.path.exists(video_path):
        print(f"  Instagram Reel: Video not found — {video_path}")
        return False

    try:
        media = cl.clip_upload(video_path, caption)
        print(f"  Instagram Reel: Posted (media pk={media.pk})")
        return True
    except Exception as e:
        print(f"  Instagram Reel: Post failed — {e}")
        return False


def post_to_instagram(image_paths, caption, also_story=True):
    """Post image(s) to Instagram feed (carousel if multiple) + story.

    This is called from post_picks_to_instagram / post_results_to_instagram
    which are only triggered when there are NEW picks (main.py dedup handles
    filtering out already-posted picks before this is ever called).

    Args:
        image_paths: str or list of str — path(s) to PNG/JPG files
        caption: str — the post caption
        also_story: bool — also post first image to story (default True)

    Returns:
        bool — True if posted successfully
    """
    cl = _get_ig_client()
    if not cl:
        return False

    if isinstance(image_paths, str):
        image_paths = [image_paths]

    # Convert PNGs to JPEGs (instagrapi carousels require .jpg)
    valid_paths = _prepare_for_ig(image_paths)
    if not valid_paths:
        print("  Instagram: No valid image files found")
        return False

    import time

    # NOTE: Photo usertags removed — looking up accounts via user_info_by_username
    # triggers Instagram bot detection. Stick to @mentions in caption text only.

    success = False
    try:
        if len(valid_paths) == 1:
            media = cl.photo_upload(valid_paths[0], caption)
        else:
            media = cl.album_upload(valid_paths, caption)

        print(f"  Instagram Feed: Posted (media pk={media.pk})")
        success = True
    except Exception as e:
        print(f"  Instagram Feed: Post failed — {e}")

    # Post to story — resize to 9:16 so it's not zoomed/cropped
    if also_story and valid_paths and success:
        time.sleep(10)  # Wait between feed post and story
        try:
            story_path = _make_story_image(valid_paths[0])
            story = cl.photo_upload_to_story(story_path)
            print(f"  Instagram Story: Posted (media pk={story.pk})")
        except Exception as e:
            print(f"  Instagram Story: Post failed — {e}")

    return success


def _get_sport_tags(picks):
    """Get relevant @tags and #hashtags based on sports in the picks."""
    sports = set()
    for p in (picks or []):
        sp = p.get('sport', '')
        if 'nba' in sp: sports.add('nba')
        elif 'ncaab' in sp: sports.add('ncaab')
        elif 'nhl' in sp or 'hockey' in sp: sports.add('nhl')
        elif 'mlb' in sp or 'baseball' in sp: sports.add('baseball')
        elif 'soccer' in sp: sports.add('soccer')
        elif 'tennis' in sp: sports.add('tennis')

    # NO @mentions — triggers Instagram bot detection. Hashtags only.
    sport_hashtags = {
        'nba': ['#NBAPicks', '#NBABetting', '#NBA'],
        'ncaab': ['#CBBPicks', '#MarchMadness', '#CollegeBasketball'],
        'nhl': ['#NHLPicks', '#NHLBetting', '#NHL'],
        'baseball': ['#CollegeBaseball', '#BaseballBetting', '#CWS'],
        'soccer': ['#SoccerPicks', '#SoccerBetting', '#EPL'],
        'tennis': ['#TennisPicks', '#TennisBetting', '#ATP'],
    }

    hashtags = set()
    for s in sports:
        hashtags.update(sport_hashtags.get(s, []))

    community_hashtags = ['#SportsBetting', '#FreePicks', '#BettingPicks',
                          '#BettingCommunity', '#ScottysEdge',
                          '#SportsAnalytics', '#BettingModel', '#DataDriven']
    all_hashtags = list(hashtags) + community_hashtags

    return [], all_hashtags  # No @mentions — only hashtags


def post_picks_to_instagram(card_paths, picks):
    """Post picks card to Instagram STORY ONLY (not feed — feed reserved for results + video)."""
    cl = _get_ig_client()
    if not cl:
        return False

    import time

    if isinstance(card_paths, str):
        card_paths = [card_paths]

    valid_paths = _prepare_for_ig(card_paths)
    if not valid_paths:
        print("  Instagram: No valid image files found")
        return False

    # Post each card as a story
    for path in valid_paths:
        try:
            story_path = _make_story_image(path)
            story = cl.photo_upload_to_story(story_path)
            print(f"  Instagram Story: Posted picks (media pk={story.pk})")
            time.sleep(5)
        except Exception as e:
            print(f"  Instagram Story: Pick post failed — {e}")

    return True


def post_results_to_instagram(card_paths, report_text=None):
    """Post results card to Instagram feed + story with results caption."""
    caption = "Results are in. Every pick tracked. Every loss shown.\n\n"
    if report_text:
        import re
        record_match = re.search(r'Record:\s*(\d+W-\d+L)', report_text)
        pnl_match = re.search(r'P/L:\s*([\+\-][\d.]+u)', report_text)
        if record_match:
            caption += f"Season: {record_match.group(1)}"
        if pnl_match:
            caption += f" | {pnl_match.group(1)}"
        caption += "\n\n"

    caption += "Swipe for full breakdown.\n\n"
    caption += "\u26a0\ufe0f Not gambling advice \u2022 21+ \u2022 1-800-GAMBLER\n\n"
    caption += "\U0001f4f1 @scottys_edge | \U0001f4ac discord.gg/JQ6rRfuN\n\n"

    # Hashtags only — NO @mentions (triggers bot detection)
    results_hashtags = ['#SportsBetting', '#BettingResults', '#FreePicks',
                        '#BettingCommunity', '#ScottysEdge', '#SportsAnalytics',
                        '#BettingModel', '#DataDriven', '#Transparency',
                        '#SportsBettingPicks', '#BettingRecord']

    caption += " ".join(results_hashtags)

    return post_to_instagram(card_paths, caption, also_story=False)  # Feed only — stories are for picks


# ═══════════════════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    # Test Discord webhook
    test_picks = [{
        'selection': 'Test Pick +5.0',
        'sport': 'basketball_nba',
        'odds': -110,
        'units': 4.0,
        'commence': '2026-03-10T23:00:00Z',
        'home': 'Team A',
        'away': 'Team B',
        'context': 'Test context',
    }]
    
    print("Testing Discord webhook...")
    post_to_discord(test_picks)
    # v25.3: removed _format_twitter_thread test — Twitter account suspended
    # April 2026, function no longer exists. Was a NameError on direct invocation.
