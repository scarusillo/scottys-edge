#!/usr/bin/env python3
"""
reel_generator.py — Auto-generate Instagram Reels from picks/results data.

Creates a 15-20 second 9:16 video (1080x1920) with animated text reveals.
Uses Pillow for frames + ffmpeg to encode. No additional dependencies.

Two reel types:
  1. RESULTS REEL — "13-7 yesterday, +19u" with each pick revealed
  2. PICKS REEL — Today's card animated pick-by-pick

Usage:
    python reel_generator.py results    # Generate results reel
    python reel_generator.py picks      # Generate picks reel
    python reel_generator.py results --post  # Generate + post to Instagram
"""
import sqlite3, os, sys, subprocess, tempfile, shutil
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
CARDS_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards')
os.makedirs(CARDS_DIR, exist_ok=True)

# ── Design constants (match card_image.py branding) ──
W, H = 1080, 1920  # 9:16 Reels format
BG = (13, 17, 23)  # #0d1117
CARD_BG = (20, 30, 42)  # #141e2a
GREEN = (0, 230, 118)  # #00e676
RED = (255, 82, 82)
WHITE = (255, 255, 255)
GRAY = (160, 170, 180)
FPS = 30
HOLD_FRAMES = int(FPS * 2.5)  # Hold each slide 2.5 seconds
FADE_FRAMES = int(FPS * 0.4)  # 0.4s fade transition


def _get_font(size, bold=False):
    """Get a font, falling back gracefully."""
    candidates = [
        'C:/Windows/Fonts/segoeui.ttf',
        'C:/Windows/Fonts/segoeuib.ttf' if bold else 'C:/Windows/Fonts/segoeui.ttf',
        'C:/Windows/Fonts/arial.ttf',
        'C:/Windows/Fonts/arialbd.ttf' if bold else 'C:/Windows/Fonts/arial.ttf',
    ]
    for f in candidates:
        if os.path.exists(f):
            return ImageFont.truetype(f, size)
    return ImageFont.load_default()


def _draw_rounded_rect(draw, xy, fill, radius=20):
    """Draw a rounded rectangle."""
    x1, y1, x2, y2 = xy
    draw.rectangle([(x1 + radius, y1), (x2 - radius, y2)], fill=fill)
    draw.rectangle([(x1, y1 + radius), (x2, y2 - radius)], fill=fill)
    draw.pieslice([(x1, y1), (x1 + 2*radius, y1 + 2*radius)], 180, 270, fill=fill)
    draw.pieslice([(x2 - 2*radius, y1), (x2, y1 + 2*radius)], 270, 360, fill=fill)
    draw.pieslice([(x1, y2 - 2*radius), (x1 + 2*radius, y2)], 90, 180, fill=fill)
    draw.pieslice([(x2 - 2*radius, y2 - 2*radius), (x2, y2)], 0, 90, fill=fill)


def _make_title_frame(title, subtitle, stat_line=None):
    """Create an opening title frame."""
    img = Image.new('RGB', (W, H), BG)
    draw = ImageDraw.Draw(img)

    # Logo
    logo_font = _get_font(72, bold=True)
    edge_font = _get_font(72, bold=True)
    logo_text = "SCOTTY'S "
    logo_w = draw.textlength(logo_text, font=logo_font)
    edge_w = draw.textlength("EDGE", font=edge_font)
    total_w = logo_w + edge_w
    x_start = (W - total_w) / 2
    draw.text((x_start, 680), logo_text, fill=WHITE, font=logo_font)
    draw.text((x_start + logo_w, 680), "EDGE", fill=GREEN, font=edge_font)

    # Title (e.g., "YESTERDAY'S RESULTS")
    title_font = _get_font(36, bold=True)
    tw = draw.textlength(title, font=title_font)
    draw.text(((W - tw) / 2, 800), title, fill=GRAY, font=title_font)

    # Subtitle (e.g., "13W-7L | +19.0u")
    sub_font = _get_font(64, bold=True)
    sw = draw.textlength(subtitle, font=sub_font)
    color = GREEN if '+' in subtitle else RED
    draw.text(((W - sw) / 2, 880), subtitle, fill=color, font=sub_font)

    # Stat line (e.g., "Season: 101W-64L | +92.8u | 61.2%")
    if stat_line:
        stat_font = _get_font(30)
        stw = draw.textlength(stat_line, font=stat_font)
        draw.text(((W - stw) / 2, 980), stat_line, fill=GRAY, font=stat_font)

    # Accent line
    line_w = 200
    draw.rectangle([(W/2 - line_w/2, 770), (W/2 + line_w/2, 773)], fill=GREEN)

    return img


def _make_pick_frame(pick_text, result, pnl, index, total, sport_label=""):
    """Create a single pick result frame."""
    img = Image.new('RGB', (W, H), BG)
    draw = ImageDraw.Draw(img)

    # Header
    header_font = _get_font(28)
    header = f"PICK {index}/{total}"
    hw = draw.textlength(header, font=header_font)
    draw.text(((W - hw) / 2, 120), header, fill=GRAY, font=header_font)

    # Sport badge
    if sport_label:
        badge_font = _get_font(24, bold=True)
        bw = draw.textlength(sport_label, font=badge_font)
        badge_colors = {
            'NBA': (255, 100, 50), 'NCAAB': (255, 140, 0), 'NHL': (50, 150, 255),
            'Baseball': (50, 200, 100), 'MLB': (200, 50, 50),
            'Soccer': (100, 200, 50), 'Tennis': (200, 200, 50),
        }
        badge_col = badge_colors.get(sport_label, GREEN)
        _draw_rounded_rect(draw, ((W - bw) / 2 - 15, 175, (W + bw) / 2 + 15, 210), fill=badge_col, radius=10)
        draw.text(((W - bw) / 2, 178), sport_label, fill=WHITE, font=badge_font)

    # Result icon — big and centered
    is_win = result == 'WIN'
    is_push = result == 'PUSH'
    icon = "W" if is_win else ("P" if is_push else "L")
    icon_color = GREEN if is_win else (GRAY if is_push else RED)
    icon_font = _get_font(200, bold=True)
    iw = draw.textlength(icon, font=icon_font)
    draw.text(((W - iw) / 2, 400), icon, fill=icon_color, font=icon_font)

    # Pick text — card style
    _draw_rounded_rect(draw, (60, 700, W - 60, 1000), fill=CARD_BG, radius=15)

    pick_font = _get_font(32, bold=True)
    # Word wrap if too long
    max_chars = 30
    lines = []
    words = pick_text.split()
    current_line = ""
    for word in words:
        if len(current_line + " " + word) > max_chars and current_line:
            lines.append(current_line)
            current_line = word
        else:
            current_line = (current_line + " " + word).strip()
    if current_line:
        lines.append(current_line)

    y_start = 740 + (200 - len(lines) * 50) / 2
    for i, line in enumerate(lines):
        lw = draw.textlength(line, font=pick_font)
        draw.text(((W - lw) / 2, y_start + i * 50), line, fill=WHITE, font=pick_font)

    # P/L
    pnl_font = _get_font(56, bold=True)
    pnl_str = f"{pnl:+.1f}u"
    pnl_color = GREEN if pnl > 0 else (GRAY if pnl == 0 else RED)
    pw = draw.textlength(pnl_str, font=pnl_font)
    draw.text(((W - pw) / 2, 1080), pnl_str, fill=pnl_color, font=pnl_font)

    # Footer
    foot_font = _get_font(24)
    draw.text((W/2 - 100, 1700), "@scottys_edge", fill=GRAY, font=foot_font)

    return img


def _make_summary_frame(record, pnl, wp, roi):
    """Create a closing summary frame."""
    img = Image.new('RGB', (W, H), BG)
    draw = ImageDraw.Draw(img)

    # Logo
    logo_font = _get_font(60, bold=True)
    logo_text = "SCOTTY'S "
    logo_w = draw.textlength(logo_text, font=logo_font)
    edge_w = draw.textlength("EDGE", font=logo_font)
    total_w = logo_w + edge_w
    x_start = (W - total_w) / 2
    draw.text((x_start, 400), logo_text, fill=WHITE, font=logo_font)
    draw.text((x_start + logo_w, 400), "EDGE", fill=GREEN, font=logo_font)

    # Season stats in cards
    stats = [
        ("RECORD", record, WHITE),
        ("PROFIT", f"+{pnl}u" if pnl > 0 else f"{pnl}u", GREEN if pnl > 0 else RED),
        ("WIN RATE", f"{wp}%", GREEN if wp >= 55 else WHITE),
        ("ROI", f"+{roi}%" if roi > 0 else f"{roi}%", GREEN if roi > 0 else RED),
    ]

    y = 600
    for label, value, color in stats:
        _draw_rounded_rect(draw, (100, y, W - 100, y + 100), fill=CARD_BG, radius=15)
        label_font = _get_font(24)
        val_font = _get_font(42, bold=True)
        draw.text((140, y + 15), label, fill=GRAY, font=label_font)
        vw = draw.textlength(value, font=val_font)
        draw.text((W - 140 - vw, y + 35), value, fill=color, font=val_font)
        y += 130

    # CTA
    cta_font = _get_font(32, bold=True)
    cta = "FOLLOW FOR DAILY PICKS"
    cw = draw.textlength(cta, font=cta_font)
    _draw_rounded_rect(draw, ((W - cw)/2 - 30, 1250, (W + cw)/2 + 30, 1310), fill=GREEN, radius=15)
    draw.text(((W - cw) / 2, 1260), cta, fill=BG, font=cta_font)

    # Social handles
    social_font = _get_font(26)
    social = "IG: @scottys_edge | X: @Scottys_edge"
    sw = draw.textlength(social, font=social_font)
    draw.text(((W - sw) / 2, 1380), social, fill=GRAY, font=social_font)

    disc = "Discord: discord.gg/JQ6rRfuN"
    dw = draw.textlength(disc, font=social_font)
    draw.text(((W - dw) / 2, 1420), disc, fill=GRAY, font=social_font)

    # Disclaimer
    disc_font = _get_font(18)
    draw.text((W/2 - 180, 1750), "Not gambling advice  |  21+  |  1-800-GAMBLER", fill=(100, 100, 100), font=disc_font)

    return img


def _frames_to_video(frames, output_path):
    """Convert PIL frames to video using ffmpeg."""
    tmpdir = tempfile.mkdtemp()
    try:
        # Save all frames
        frame_files = []
        frame_num = 0

        for frame_img in frames:
            # Hold frame
            for _ in range(HOLD_FRAMES):
                path = os.path.join(tmpdir, f'frame_{frame_num:05d}.png')
                frame_img.save(path)
                frame_files.append(path)
                frame_num += 1

            # Fade to black (transition)
            for f in range(FADE_FRAMES):
                alpha = f / FADE_FRAMES
                faded = Image.blend(frame_img, Image.new('RGB', (W, H), BG), alpha)
                path = os.path.join(tmpdir, f'frame_{frame_num:05d}.png')
                faded.save(path)
                frame_files.append(path)
                frame_num += 1

        # ffmpeg encode
        ffmpeg_path = shutil.which('ffmpeg')
        if not ffmpeg_path:
            # Try common Windows paths
            for p in [r'C:\ffmpeg\bin\ffmpeg.exe', r'C:\Program Files\ffmpeg\bin\ffmpeg.exe']:
                if os.path.exists(p):
                    ffmpeg_path = p
                    break

        if not ffmpeg_path:
            # Try the winget install location
            import glob
            for pattern in [
                r'C:\Users\*\AppData\Local\Microsoft\WinGet\Links\ffmpeg.exe',
                r'C:\Users\*\AppData\Local\Microsoft\WinGet\Packages\*\ffmpeg-*\bin\ffmpeg.exe',
            ]:
                matches = glob.glob(pattern)
                if matches:
                    ffmpeg_path = matches[0]
                    break

        if not ffmpeg_path:
            print("  Reel: ffmpeg not found in PATH")
            return False

        cmd = [
            ffmpeg_path, '-y',
            '-framerate', str(FPS),
            '-i', os.path.join(tmpdir, 'frame_%05d.png'),
            '-c:v', 'libx264',
            '-pix_fmt', 'yuv420p',
            '-preset', 'fast',
            '-crf', '23',
            output_path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"  Reel: ffmpeg error — {result.stderr[:200]}")
            return False

        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def generate_results_reel(conn):
    """Generate a results reel from yesterday's graded bets."""
    # Get yesterday's results
    game_date = conn.execute("""
        SELECT MAX(DATE(created_at)) FROM graded_bets
        WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5
    """).fetchone()[0]

    if not game_date:
        print("  Reel: No results to animate")
        return None

    bets = conn.execute("""
        SELECT selection, result, pnl_units, sport
        FROM graded_bets
        WHERE DATE(created_at) = ? AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
        AND units >= 3.5
        ORDER BY pnl_units DESC
    """, (game_date,)).fetchall()

    if not bets:
        return None

    yw = sum(1 for b in bets if b[1] == 'WIN')
    yl = sum(1 for b in bets if b[1] == 'LOSS')
    yp = sum(b[2] or 0 for b in bets)

    # Season stats
    all_bets = conn.execute("""
        SELECT result, pnl_units, units FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04' AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchall()
    tw = sum(1 for b in all_bets if b[0] == 'WIN')
    tl = sum(1 for b in all_bets if b[0] == 'LOSS')
    tp = round(sum(b[1] or 0 for b in all_bets), 1)
    twag = sum(b[2] or 0 for b in all_bets)
    twp = round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0
    troi = round(tp / twag * 100, 1) if twag else 0

    sport_labels = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL', 'baseball_ncaa': 'Baseball',
        'baseball_mlb': 'MLB',
    }

    # Build frames
    frames = []

    # Frame 1: Title
    game_dt = datetime.strptime(game_date, '%Y-%m-%d')
    day_name = game_dt.strftime('%A %B %d')
    frames.append(_make_title_frame(
        day_name.upper(),
        f"{yw}W-{yl}L | {yp:+.1f}u",
        f"Season: {tw}W-{tl}L | {tp:+.1f}u | {twp}%"
    ))

    # Frames 2-N: Each pick (show max 8 to keep reel short)
    show_bets = bets[:8]
    for i, (sel, result, pnl, sport) in enumerate(show_bets, 1):
        sp_label = sport_labels.get(sport, '')
        if 'soccer' in sport:
            sp_label = 'Soccer'
        elif 'tennis' in sport:
            sp_label = 'Tennis'
        frames.append(_make_pick_frame(sel, result, pnl or 0, i, len(show_bets), sp_label))

    # Final frame: Season summary
    frames.append(_make_summary_frame(f"{tw}W-{tl}L", tp, twp, troi))

    # Encode
    output_path = os.path.join(CARDS_DIR, 'scottys_edge_reel.mp4')
    print(f"  Reel: Generating {len(frames)} slides ({len(frames) * 2.9:.0f}s)...")
    if _frames_to_video(frames, output_path):
        print(f"  Reel: Saved to {output_path}")
        return output_path
    return None


def generate_picks_reel(conn):
    """Generate a picks reel from today's bets."""
    today = datetime.now().strftime('%Y-%m-%d')

    picks = conn.execute("""
        SELECT selection, sport, odds, units
        FROM bets WHERE DATE(created_at) = ? AND units >= 3.5
        ORDER BY units DESC
    """, (today,)).fetchall()

    if not picks:
        print("  Reel: No picks today")
        return None

    sport_labels = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL', 'baseball_ncaa': 'Baseball',
        'baseball_mlb': 'MLB',
    }

    # Season stats
    all_bets = conn.execute("""
        SELECT result, pnl_units, units FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04' AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchall()
    tw = sum(1 for b in all_bets if b[0] == 'WIN')
    tl = sum(1 for b in all_bets if b[0] == 'LOSS')
    tp = round(sum(b[1] or 0 for b in all_bets), 1)
    twag = sum(b[2] or 0 for b in all_bets)
    twp = round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0
    troi = round(tp / twag * 100, 1) if twag else 0

    frames = []

    # Title
    day_name = datetime.now().strftime('%A %B %d')
    frames.append(_make_title_frame(
        f"{day_name.upper()} PICKS",
        f"{len(picks)} PLAYS",
        f"Season: {tw}W-{tl}L | {tp:+.1f}u | {twp}%"
    ))

    # Each pick
    show_picks = picks[:8]
    for i, (sel, sport, odds, units) in enumerate(show_picks, 1):
        sp_label = sport_labels.get(sport, '')
        if 'soccer' in sport:
            sp_label = 'Soccer'
        elif 'tennis' in sport:
            sp_label = 'Tennis'

        # Use the pick frame but show odds/units instead of W/L
        img = Image.new('RGB', (W, H), BG)
        draw = ImageDraw.Draw(img)

        header_font = _get_font(28)
        draw.text(((W - draw.textlength(f"PICK {i}/{len(show_picks)}", font=header_font)) / 2, 120),
                  f"PICK {i}/{len(show_picks)}", fill=GRAY, font=header_font)

        if sp_label:
            badge_font = _get_font(24, bold=True)
            bw = draw.textlength(sp_label, font=badge_font)
            _draw_rounded_rect(draw, ((W - bw)/2 - 15, 175, (W + bw)/2 + 15, 210), fill=GREEN, radius=10)
            draw.text(((W - bw) / 2, 178), sp_label, fill=WHITE, font=badge_font)

        # Pick name
        _draw_rounded_rect(draw, (60, 500, W - 60, 850), fill=CARD_BG, radius=15)
        pick_font = _get_font(32, bold=True)
        max_chars = 30
        lines = []
        words = sel.split()
        current_line = ""
        for word in words:
            if len(current_line + " " + word) > max_chars and current_line:
                lines.append(current_line)
                current_line = word
            else:
                current_line = (current_line + " " + word).strip()
        if current_line:
            lines.append(current_line)

        y_start = 580 + (200 - len(lines) * 50) / 2
        for li, line in enumerate(lines):
            lw = draw.textlength(line, font=pick_font)
            draw.text(((W - lw) / 2, y_start + li * 50), line, fill=WHITE, font=pick_font)

        # Odds + Units
        odds_font = _get_font(56, bold=True)
        odds_str = f"{int(odds):+d}" if odds else ""
        ow = draw.textlength(odds_str, font=odds_font)
        draw.text(((W - ow) / 2, 950), odds_str, fill=GREEN, font=odds_font)

        units_font = _get_font(36)
        units_str = f"{units}u"
        uw = draw.textlength(units_str, font=units_font)
        draw.text(((W - uw) / 2, 1030), units_str, fill=GRAY, font=units_font)

        foot_font = _get_font(24)
        draw.text((W/2 - 100, 1700), "@scottys_edge", fill=GRAY, font=foot_font)

        frames.append(img)

    # Summary
    frames.append(_make_summary_frame(f"{tw}W-{tl}L", tp, twp, troi))

    output_path = os.path.join(CARDS_DIR, 'scottys_edge_picks_reel.mp4')
    print(f"  Reel: Generating {len(frames)} slides ({len(frames) * 2.9:.0f}s)...")
    if _frames_to_video(frames, output_path):
        print(f"  Reel: Saved to {output_path}")
        return output_path
    return None


def post_reel(video_path, caption):
    """Post reel to Instagram."""
    try:
        from social_media import _get_ig_client
        cl = _get_ig_client()
        if not cl:
            return False

        media = cl.clip_upload(video_path, caption)
        print(f"  Instagram Reel: Posted (media pk={media.pk})")
        return True
    except Exception as e:
        print(f"  Instagram Reel: Post failed — {e}")
        return False


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python reel_generator.py [results|picks] [--post]")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    cmd = sys.argv[1].lower()
    do_post = '--post' in sys.argv

    video_path = None
    caption = ""

    if cmd == 'results':
        video_path = generate_results_reel(conn)
        if video_path:
            # Build caption
            game_date = conn.execute("SELECT MAX(DATE(created_at)) FROM graded_bets WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5").fetchone()[0]
            bets = conn.execute("SELECT result, pnl_units FROM graded_bets WHERE DATE(created_at)=? AND result IN ('WIN','LOSS') AND units >= 3.5", (game_date,)).fetchall()
            yw = sum(1 for b in bets if b[0] == 'WIN')
            yl = sum(1 for b in bets if b[0] == 'LOSS')
            yp = sum(b[1] or 0 for b in bets)
            caption = f"{yw}W-{yl}L yesterday | {yp:+.1f}u\n\nEvery pick tracked. Every loss shown.\n\n"
            caption += "Follow for daily model-driven picks.\n\n"
            caption += "#SportsBetting #FreePicks #BettingPicks #BettingResults #ScottysEdge"

    elif cmd == 'picks':
        video_path = generate_picks_reel(conn)
        if video_path:
            caption = "Today's picks are live.\n\nModel-driven. Data-backed. Full transparency.\n\n"
            caption += "Follow for daily picks.\n\n"
            caption += "#SportsBetting #FreePicks #BettingPicks #ScottysEdge"

    conn.close()

    if video_path and do_post:
        post_reel(video_path, caption)
    elif video_path:
        print(f"\n  Video ready: {video_path}")
        print("  Run with --post to upload to Instagram")
