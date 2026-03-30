#!/usr/bin/env python3
"""
reel_generator.py — Animated Instagram Reels from picks/results data.

Creates a 20-second 1080x1920 video at 30fps (600 frames) with:
  - Hook slide (2.5s): yesterday's record + P/L in dollars ($10/bet)
  - Season stats (3s): counter-animated record + win rate + dollar profit
  - All picks by sport (5.5s): grouped list with W/L badges + dollar amounts
  - Day summary (3.5s): narrative per-sport breakdown
  - Trust (3s): transparency differentiator
  - Fade out (2.5s): logo

Two reel types:
  1. RESULTS REEL — Yesterday's record + all picks grouped by sport + day narrative
  2. PICKS REEL — Today's card with all picks grouped by sport

Usage:
    python reel_generator.py results    # Generate results reel
    python reel_generator.py picks      # Generate picks reel
    python reel_generator.py results --post  # Generate + post to Instagram
"""
import sqlite3, os, sys, subprocess, tempfile, shutil, math
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
CARDS_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards')
os.makedirs(CARDS_DIR, exist_ok=True)

FFMPEG_PATH = r'C:\Users\carus\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin\ffmpeg.exe'

# ── Design constants ──
W, H = 1080, 1920
BG = (13, 17, 23)       # #0d1117
CARD_BG = (20, 30, 42)  # #141e2a
GREEN = (0, 230, 118)   # #00e676
RED = (255, 82, 82)
WHITE = (255, 255, 255)
GRAY = (160, 170, 180)
DIM_GRAY = (80, 90, 100)
FPS = 30
TOTAL_FRAMES = 600       # 20 seconds
DOLLARS_PER_UNIT = 10    # $10/bet baseline for dollar conversion

SPORT_COLORS = {
    'NBA': (255, 100, 50),
    'NCAAB': (255, 140, 0),
    'NHL': (50, 150, 255),
    'Baseball': (50, 200, 100),
    'MLB': (200, 50, 50),
    'Soccer': (100, 200, 50),
    'Tennis': (200, 200, 50),
    'NCAA BB': (180, 100, 255),
}

SPORT_LABELS = {
    'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
    'icehockey_nhl': 'NHL', 'baseball_ncaa': 'Baseball',
    'baseball_mlb': 'MLB',
}


# ══════════════════════════════════════════════════════
# Utility
# ══════════════════════════════════════════════════════

def _get_font(size, bold=False):
    path = 'C:/Windows/Fonts/segoeuib.ttf' if bold else 'C:/Windows/Fonts/segoeui.ttf'
    if os.path.exists(path):
        return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _ease_out(t):
    """Cubic ease-out: fast start, smooth deceleration."""
    return 1 - (1 - t) ** 3


def _ease_in_out(t):
    """Cubic ease-in-out."""
    if t < 0.5:
        return 4 * t * t * t
    else:
        return 1 - (-2 * t + 2) ** 3 / 2


def _lerp(a, b, t):
    """Linear interpolation."""
    return a + (b - a) * t


def _color_lerp(c1, c2, t):
    """Lerp between two RGB colors."""
    return tuple(int(_lerp(c1[i], c2[i], t)) for i in range(3))


def _alpha_color(color, alpha):
    """Dim a color by alpha (0-1)."""
    return tuple(int(c * alpha) for c in color)


def _sport_label(sport_key):
    if not sport_key:
        return ''
    if sport_key in SPORT_LABELS:
        return SPORT_LABELS[sport_key]
    if 'soccer' in sport_key:
        return 'Soccer'
    if 'tennis' in sport_key:
        return 'Tennis'
    return ''


def _fmt_dollars(units):
    """Convert units to dollar string: 19.0u -> '+$190', -5.0u -> '-$50'."""
    dollars = int(round(units * DOLLARS_PER_UNIT))
    abs_d = abs(dollars)
    if abs_d >= 1000:
        formatted = f"${abs_d:,}"
    else:
        formatted = f"${abs_d}"
    if dollars >= 0:
        return f"+{formatted}"
    else:
        return f"-{formatted}"


# ══════════════════════════════════════════════════════
# Drawing helpers
# ══════════════════════════════════════════════════════

def _draw_gradient_bg(img):
    """Subtle radial gradient from center — slightly lighter core."""
    draw = ImageDraw.Draw(img)
    cx, cy = W // 2, H // 2
    max_r = math.sqrt(cx * cx + cy * cy)
    # Draw concentric rectangles (faster than per-pixel)
    for step in range(0, 40):
        t = step / 40
        r = int(max_r * (1 - t))
        bright = 1.0 + 0.15 * (1 - t)  # center is 15% brighter
        color = tuple(min(255, int(c * bright)) for c in BG)
        x1, y1 = cx - r, cy - r
        x2, y2 = cx + r, cy + r
        draw.rectangle([(x1, y1), (x2, y2)], fill=color)


def _draw_glow_text(draw, xy, text, font, color, intensity=3):
    """Draw text with a glow effect — multiple offset layers in dimmer color."""
    x, y = xy
    glow_color = _alpha_color(color, 0.25)
    for dx in range(-intensity, intensity + 1):
        for dy in range(-intensity, intensity + 1):
            if dx == 0 and dy == 0:
                continue
            if abs(dx) + abs(dy) <= intensity:
                draw.text((x + dx, y + dy), text, fill=glow_color, font=font)
    draw.text((x, y), text, fill=color, font=font)


def _draw_centered_text(draw, y, text, font, color):
    tw = draw.textlength(text, font=font)
    draw.text(((W - tw) / 2, y), text, fill=color, font=font)
    return tw


def _draw_centered_glow(draw, y, text, font, color, intensity=3):
    tw = draw.textlength(text, font=font)
    _draw_glow_text(draw, ((W - tw) / 2, y), text, font, color, intensity)
    return tw


def _draw_rounded_rect(draw, xy, fill, radius=20):
    x1, y1, x2, y2 = xy
    draw.rectangle([(x1 + radius, y1), (x2 - radius, y2)], fill=fill)
    draw.rectangle([(x1, y1 + radius), (x2, y2 - radius)], fill=fill)
    draw.pieslice([(x1, y1), (x1 + 2*radius, y1 + 2*radius)], 180, 270, fill=fill)
    draw.pieslice([(x2 - 2*radius, y1), (x2, y1 + 2*radius)], 270, 360, fill=fill)
    draw.pieslice([(x1, y2 - 2*radius), (x1 + 2*radius, y2)], 90, 180, fill=fill)
    draw.pieslice([(x2 - 2*radius, y2 - 2*radius), (x2, y2)], 0, 90, fill=fill)


def _draw_accent_line(draw, y, width=300, color=GREEN):
    """Horizontal accent line centered at y."""
    x1 = (W - width) // 2
    draw.rectangle([(x1, y), (x1 + width, y + 3)], fill=color)


def _new_frame():
    """Create a new frame with gradient background."""
    img = Image.new('RGB', (W, H), BG)
    _draw_gradient_bg(img)
    return img


# ══════════════════════════════════════════════════════
# Frame generators for RESULTS reel
# ══════════════════════════════════════════════════════

def _render_hook_frames(frames, wins, losses, pnl):
    """Frames 0-75 (2.5s): HOOK — big record + P/L in dollars, casual-friendly."""
    record_str = f"{wins}W-{losses}L"
    pnl_dollars = _fmt_dollars(pnl)
    pnl_str = f"{pnl_dollars} PROFIT" if pnl >= 0 else f"{pnl_dollars} LOSS"
    pnl_color = GREEN if pnl >= 0 else RED

    title_font = _get_font(48, bold=True)
    record_font = _get_font(140, bold=True)
    pnl_font = _get_font(80, bold=True)

    for f in range(76):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        # Everything fades in over 35 frames, then holds
        fade_t = min(1.0, f / 35)
        alpha = _ease_out(fade_t)

        # Logo at top
        _draw_logo(draw, 200, alpha)

        # "YESTERDAY'S RESULTS" in gray
        _draw_centered_text(draw, 650, "YESTERDAY'S RESULTS", title_font, _alpha_color(GRAY, alpha))

        # Accent line
        _draw_accent_line(draw, 710, 250, _alpha_color(GREEN, alpha))

        # Big record with green glow
        rec_color = _alpha_color(WHITE, alpha)
        tw = draw.textlength(record_str, font=record_font)
        x = (W - tw) / 2
        _draw_glow_text(draw, (x, 760), record_str, record_font, rec_color, int(4 * alpha))

        # P/L below in green or red
        if f >= 10:
            pnl_t = min(1.0, (f - 10) / 25)
            pnl_a = _ease_out(pnl_t)
            _draw_centered_glow(draw, 930, pnl_str, pnl_font, _alpha_color(pnl_color, pnl_a), int(3 * pnl_a))

        frames.append(img)


def _render_stats_frames(frames, record, pnl, wp, roi):
    """Frames 76-165 (3s): SEASON RECORD — record + win rate + dollar profit."""
    w_count = int(record.split('W')[0])
    l_count = int(record.split('-')[1].replace('L', ''))

    header_font = _get_font(42, bold=True)
    record_font = _get_font(100, bold=True)
    wp_font = _get_font(70, bold=True)
    dollar_font = _get_font(60, bold=True)
    scale_font = _get_font(28)

    pnl_dollar_str = _fmt_dollars(pnl)
    pnl_color = GREEN if pnl >= 0 else RED

    for f in range(90):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        _draw_logo(draw, 200, 1.0)

        # "SEASON RECORD" header
        _draw_centered_text(draw, 560, "SEASON RECORD", header_font, GRAY)

        # Counter animation over 40 frames
        counter_t = min(1.0, f / 40)
        ct = _ease_in_out(counter_t)

        # Big record: "108W - 71L" in white
        cur_w = int(w_count * ct)
        cur_l = int(l_count * ct)
        record_str = f"{cur_w}W - {cur_l}L"
        _draw_centered_text(draw, 650, record_str, record_font, WHITE)

        # Accent line between them
        _draw_accent_line(draw, 770, 300, GREEN)

        # Win rate in green below
        cur_wp = wp * ct
        wp_str = f"{cur_wp:.1f}% WIN RATE"
        wp_color = GREEN if wp >= 55 else WHITE
        _draw_centered_text(draw, 810, wp_str, wp_font, wp_color)

        # "ON $10 BETS" in smaller gray text
        _draw_centered_text(draw, 885, "ON $10 BETS", scale_font, _alpha_color(GRAY, 0.7))

        # Dollar profit below
        if f >= 15:
            d_t = min(1.0, (f - 15) / 30)
            d_a = _ease_out(d_t)
            cur_dollars = _fmt_dollars(pnl * ct)
            _draw_centered_glow(draw, 940, f"{cur_dollars} PROFIT", dollar_font,
                                _alpha_color(pnl_color, d_a), int(3 * d_a))

        frames.append(img)


def _render_picks_frames(frames, bets):
    """Frames 166-330 (5.5s): TOP 4 WINS + WORST 4 LOSSES as full-width rows."""
    # Split into wins and losses, pick top/worst
    wins = [(sel, result, pnl, sport) for sel, result, pnl, sport in bets if result == 'WIN']
    losses = [(sel, result, pnl, sport) for sel, result, pnl, sport in bets if result == 'LOSS']
    # Top 4 wins by pnl descending
    wins.sort(key=lambda x: x[2] or 0, reverse=True)
    top_wins = wins[:4]
    # Worst 4 losses by pnl ascending (most negative)
    losses.sort(key=lambda x: x[2] or 0)
    worst_losses = losses[:4]
    # Combine: wins first, then losses
    display_picks = top_wins + worst_losses

    title_font = _get_font(42, bold=True)
    badge_font = _get_font(36, bold=True)
    units_font = _get_font(38, bold=True)
    name_font = _get_font(32, bold=True)
    sport_font = _get_font(20, bold=True)

    row_spacing = 90
    start_y = 450
    total_frames = 165  # 5.5 seconds

    for f in range(total_frames):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        _draw_logo(draw, 200, 1.0)

        # "RESULTS" header
        _draw_centered_text(draw, 370, "RESULTS", title_font, GRAY)
        _draw_accent_line(draw, 420, 200, GREEN)

        # All picks fade in together over 30 frames
        fade_t = min(1.0, f / 30)
        fade_a = _ease_out(fade_t)

        y = start_y
        for pi, (sel, result, pnl, sport) in enumerate(display_picks):
            is_win = result == 'WIN'
            row_color = GREEN if is_win else RED

            # W/L badge — 50px circle with letter inside
            badge_r = 25
            badge_cx = 80 + badge_r
            badge_cy = y + 30
            badge_fill = _alpha_color(row_color, fade_a)
            draw.ellipse([(badge_cx - badge_r, badge_cy - badge_r),
                          (badge_cx + badge_r, badge_cy + badge_r)], fill=badge_fill)
            letter = "W" if is_win else "L"
            lw = draw.textlength(letter, font=badge_font)
            draw.text((badge_cx - lw / 2, badge_cy - 18), letter,
                      fill=_alpha_color(WHITE, fade_a), font=badge_font)

            # Dollars: "+$42" or "-$50"
            pnl_str = _fmt_dollars(pnl) if pnl else "+$0"
            units_x = 145
            draw.text((units_x, y + 12), pnl_str,
                      fill=_alpha_color(row_color, fade_a), font=units_font)

            # Pick name, truncated at 35 chars
            short_sel = sel[:35] + "..." if len(sel) > 35 else sel
            name_x = 310
            draw.text((name_x, y + 16), short_sel,
                      fill=_alpha_color(WHITE, fade_a), font=name_font)

            # Sport badge on right side
            sp = _sport_label(sport)
            if sp:
                sp_color = SPORT_COLORS.get(sp, GREEN)
                sp_tw = draw.textlength(sp, font=sport_font)
                draw.text((W - 80 - sp_tw, y + 20), sp,
                          fill=_alpha_color(sp_color, fade_a), font=sport_font)

            # Thin colored bar underneath
            bar_y = y + 62
            draw.rectangle([(80, bar_y), (W - 80, bar_y + 2)],
                           fill=_alpha_color(row_color, fade_a * 0.4))

            y += row_spacing

        frames.append(img)


def _generate_day_summary(bets):
    """Generate casual narrative summary lines from bets, grouped by sport."""
    from collections import OrderedDict
    sport_stats = OrderedDict()
    for sel, result, pnl, sport in bets:
        sp = _sport_label(sport)
        if not sp:
            sp = 'Other'
        if sp not in sport_stats:
            sport_stats[sp] = {'w': 0, 'l': 0, 'pnl': 0.0}
        if result == 'WIN':
            sport_stats[sp]['w'] += 1
        elif result == 'LOSS':
            sport_stats[sp]['l'] += 1
        sport_stats[sp]['pnl'] += (pnl or 0)

    lines = []
    for sport_name, stats in sport_stats.items():
        w, l, p = stats['w'], stats['l'], round(stats['pnl'], 1)
        sp_color = SPORT_COLORS.get(sport_name, GREEN)
        total = w + l
        if total == 0:
            continue
        # Build casual descriptor
        if l == 0 and w > 0:
            desc = "perfect day"
        elif w == 0 and l > 0:
            desc = "tough"
        elif w / total >= 0.75:
            desc = "strong"
        elif w / total >= 0.5:
            desc = "solid"
        elif w / total >= 0.4:
            desc = "grind"
        else:
            desc = "tough"
        lines.append((f"{sport_name}: {w}-{l} -- {desc}", sp_color))

    # Limit to 4 sport lines
    lines = lines[:4]

    return lines


def _render_day_summary_frames(frames, bets):
    """Frames 331-435 (3.5s): DAY SUMMARY — casual sport lines + verdict."""
    summary_lines = _generate_day_summary(bets)

    # Calculate overall verdict
    total_pnl = 0
    for sel, result, pnl, sport in bets:
        if result in ('WIN', 'LOSS'):
            total_pnl += (pnl or 0)

    if total_pnl >= 5:
        verdict = "GREEN DAY"
        verdict_color = GREEN
    elif total_pnl >= 0:
        verdict = "BREAKEVEN"
        verdict_color = GREEN
    else:
        verdict = "TOUGH DAY -- TRUST THE PROCESS"
        verdict_color = RED

    title_font = _get_font(42, bold=True)
    line_font = _get_font(40, bold=True)
    verdict_font = _get_font(52, bold=True)

    n_lines = len(summary_lines)
    line_spacing = 75
    start_y = 650

    total_frames = 105  # 3.5 seconds

    for f in range(total_frames):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        _draw_logo(draw, 200, 1.0)

        # Title
        title_a = min(1.0, f / 25)
        _draw_centered_text(draw, 550, "THE DAY", title_font, _alpha_color(GRAY, _ease_out(title_a)))
        _draw_accent_line(draw, 605, 200, _alpha_color(GREEN, _ease_out(title_a)))

        # Sport lines, staggered fade-in 12 frames apart
        y = start_y
        for li, (text, color) in enumerate(summary_lines):
            line_delay = 10 + li * 12
            if f < line_delay:
                y += line_spacing
                continue
            line_t = min(1.0, (f - line_delay) / 20)
            line_a = _ease_out(line_t)
            _draw_centered_text(draw, y, text, line_font, _alpha_color(color, line_a))
            y += line_spacing

        # Verdict at bottom with glow
        verdict_delay = 10 + n_lines * 12 + 10
        if f >= verdict_delay:
            v_t = min(1.0, (f - verdict_delay) / 20)
            v_a = _ease_out(v_t)
            verdict_y = start_y + n_lines * line_spacing + 60
            _draw_centered_glow(draw, verdict_y, verdict, verdict_font,
                                _alpha_color(verdict_color, v_a), int(3 * v_a))

        frames.append(img)


def _render_trust_frames(frames):
    """Frames 436-525 (3s): TRUST/PROOF — transparency differentiator."""
    line1_font = _get_font(52, bold=True)
    handle_font = _get_font(36)

    for f in range(90):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        # Fade in over 30 frames
        fade_t = min(1.0, f / 30)
        alpha = _ease_out(fade_t)

        # "EVERY PICK TRACKED" in white
        _draw_centered_text(draw, 780, "EVERY PICK TRACKED", line1_font, _alpha_color(WHITE, alpha))

        # "EVERY LOSS SHOWN" with green glow
        if f >= 8:
            t2 = min(1.0, (f - 8) / 25)
            a2 = _ease_out(t2)
            _draw_centered_glow(draw, 860, "EVERY LOSS SHOWN", line1_font,
                                _alpha_color(GREEN, a2), int(3 * a2))

        # Handle in gray
        if f >= 16:
            t3 = min(1.0, (f - 16) / 20)
            a3 = _ease_out(t3)
            _draw_centered_text(draw, 960, "@scottys_edge", handle_font, _alpha_color(GRAY, a3))

        frames.append(img)


def _render_fade_out(frames):
    """Frames 526-600 (2.5s): Fade in SCOTTY'S EDGE logo with green glow."""
    logo_font = _get_font(88, bold=True)

    for f in range(75):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        if f < 20:
            fade_t = f / 20
        else:
            fade_t = 1.0

        alpha = _ease_out(fade_t)

        logo_text = "SCOTTY'S "
        edge_text = "EDGE"
        lw = draw.textlength(logo_text, font=logo_font)
        ew = draw.textlength(edge_text, font=logo_font)
        total = lw + ew
        x = (W - total) / 2
        y = (H - 80) / 2

        white_a = _alpha_color(WHITE, alpha)
        green_a = _alpha_color(GREEN, alpha)
        draw.text((x, y), logo_text, fill=white_a, font=logo_font)
        _draw_glow_text(draw, (x + lw, y), edge_text, logo_font, green_a, int(4 * alpha))

        frames.append(img)


def _draw_logo(draw, y, alpha=1.0):
    """Draw SCOTTY'S EDGE logo at given y position."""
    logo_font = _get_font(58, bold=True)
    logo_text = "SCOTTY'S "
    edge_text = "EDGE"
    lw = draw.textlength(logo_text, font=logo_font)
    ew = draw.textlength(edge_text, font=logo_font)
    total = lw + ew
    x = (W - total) / 2
    white_a = _alpha_color(WHITE, alpha)
    green_a = _alpha_color(GREEN, alpha)
    draw.text((x, y), logo_text, fill=white_a, font=logo_font)
    draw.text((x + lw, y), edge_text, fill=green_a, font=logo_font)


# ══════════════════════════════════════════════════════
# Video encoding
# ══════════════════════════════════════════════════════

def _frames_to_video(frames, output_path):
    """Pipe PIL frames to ffmpeg via stdin (no temp PNGs needed)."""
    ffmpeg_path = FFMPEG_PATH
    if not os.path.exists(ffmpeg_path):
        ffmpeg_path = shutil.which('ffmpeg')
    if not ffmpeg_path:
        print("  Reel: ffmpeg not found")
        return False

    cmd = [
        ffmpeg_path, '-y',
        '-f', 'rawvideo',
        '-vcodec', 'rawvideo',
        '-s', f'{W}x{H}',
        '-pix_fmt', 'rgb24',
        '-r', str(FPS),
        '-i', '-',
        '-c:v', 'libx264',
        '-pix_fmt', 'yuv420p',
        '-preset', 'fast',
        '-crf', '23',
        '-movflags', '+faststart',
        output_path
    ]

    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for frame_img in frames:
            proc.stdin.write(frame_img.tobytes())
        proc.stdin.close()
        _, stderr = proc.communicate(timeout=120)
        if proc.returncode != 0:
            print(f"  Reel: ffmpeg error — {stderr.decode('utf-8', errors='replace')[:300]}")
            return False
        return True
    except Exception as e:
        print(f"  Reel: encoding error — {e}")
        return False


# ══════════════════════════════════════════════════════
# Data loading
# ══════════════════════════════════════════════════════

def _load_season_stats(conn):
    """Load season-wide stats."""
    rows = conn.execute("""
        SELECT result, pnl_units, units FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04' AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchall()
    tw = sum(1 for b in rows if b[0] == 'WIN')
    tl = sum(1 for b in rows if b[0] == 'LOSS')
    tp = round(sum(b[1] or 0 for b in rows), 1)
    twag = sum(b[2] or 0 for b in rows)
    twp = round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0
    troi = round(tp / twag * 100, 1) if twag else 0
    return tw, tl, tp, twp, troi


def _load_yesterday_bets(conn):
    """Load yesterday's graded bets."""
    game_date = conn.execute("""
        SELECT MAX(DATE(created_at)) FROM graded_bets
        WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5
    """).fetchone()[0]
    if not game_date:
        return None, []
    bets = conn.execute("""
        SELECT selection, result, pnl_units, sport
        FROM graded_bets
        WHERE DATE(created_at) = ? AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
        AND units >= 3.5
        ORDER BY pnl_units DESC
    """, (game_date,)).fetchall()
    return game_date, bets


# ══════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════

def generate_results_reel(conn):
    """Generate a 20-second animated results reel (600 frames at 30fps)."""
    game_date, bets = _load_yesterday_bets(conn)
    if not bets:
        print("  Reel: No results to animate")
        return None

    wl_bets = [b for b in bets if b[1] in ('WIN', 'LOSS')]
    yw = sum(1 for b in wl_bets if b[1] == 'WIN')
    yl = sum(1 for b in wl_bets if b[1] == 'LOSS')
    yp = round(sum(b[2] or 0 for b in wl_bets), 1)

    tw, tl, tp, twp, troi = _load_season_stats(conn)

    yp_dollars = _fmt_dollars(yp)
    print(f"  Reel: Building {TOTAL_FRAMES} frames — {yw}W-{yl}L, {yp_dollars}, {len(wl_bets)} picks")

    frames = []

    # Slide 1: HOOK (frames 0-75, 2.5s)
    _render_hook_frames(frames, yw, yl, yp)

    # Slide 2: SEASON RECORD (frames 76-165, 3s)
    _render_stats_frames(frames, f"{tw}W-{tl}L", tp, twp, troi)

    # Slide 3: PICKS — top wins + worst losses (frames 166-330, 5.5s)
    _render_picks_frames(frames, wl_bets)

    # Slide 4: DAY SUMMARY (frames 331-435, 3.5s)
    _render_day_summary_frames(frames, wl_bets)

    # Slide 5: TRUST/PROOF (frames 436-525, 3s)
    _render_trust_frames(frames)

    # Slide 6: FADE OUT (frames 526-600, 2.5s)
    _render_fade_out(frames)

    # Sanity: pad or trim to exactly 600
    while len(frames) < TOTAL_FRAMES:
        frames.append(frames[-1].copy())
    frames = frames[:TOTAL_FRAMES]

    output_path = os.path.join(CARDS_DIR, 'scottys_edge_reel.mp4')
    print(f"  Reel: Encoding {len(frames)} frames ({len(frames)/FPS:.1f}s) to {output_path}")
    if _frames_to_video(frames, output_path):
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"  Reel: Done — {size_mb:.1f} MB")
        return output_path
    return None


def generate_picks_reel(conn):
    """Generate a 20-second animated picks reel for today's bets."""
    today = datetime.now().strftime('%Y-%m-%d')
    picks = conn.execute("""
        SELECT selection, sport, odds, units
        FROM bets WHERE DATE(created_at) = ? AND units >= 3.5
        ORDER BY units DESC
    """, (today,)).fetchall()

    if not picks:
        print("  Reel: No picks today")
        return None

    tw, tl, tp, twp, troi = _load_season_stats(conn)
    season_record = f"Season: {tw}W-{tl}L  |  {_fmt_dollars(tp)}  |  {twp}%"

    # Convert picks to format for all_picks_frames (sel, result, pnl, sport)
    show_picks = []
    for sel, sport, odds, units in picks:
        odds_str = f"({int(odds):+d})" if odds else ""
        show_picks.append((f"{sel} {odds_str}", 'WIN', units, sport))

    print(f"  Reel: Building picks reel — {len(show_picks)} picks")

    frames = []

    # Phase 1: HOOK (frames 0-60, 2s)
    title_font = _get_font(48, bold=True)
    count_font = _get_font(136, bold=True)
    sub_font = _get_font(50, bold=True)

    for f in range(61):
        img = _new_frame()
        draw = ImageDraw.Draw(img)
        t = min(1.0, f / 30)
        alpha = _ease_out(t)
        _draw_centered_text(draw, 680, "TODAY'S PLAYS", title_font, _alpha_color(GRAY, alpha))
        _draw_accent_line(draw, 740, 250, _alpha_color(GREEN, alpha))
        if f >= 10:
            rt = min(1.0, (f - 10) / 25)
            ra = _ease_out(rt)
            _draw_centered_glow(draw, 790, f"{len(picks)} PICKS", count_font, _alpha_color(GREEN, ra), int(4 * ra))
        if f >= 25:
            pt = min(1.0, (f - 25) / 25)
            pa = _ease_out(pt)
            _draw_centered_text(draw, 930, season_record, sub_font, _alpha_color(GRAY, pa))
        _draw_logo(draw, 200, min(1.0, f / 15))
        frames.append(img)

    # Phase 2: SEASON STATS (frames 61-120, 2s)
    _render_stats_frames(frames, f"{tw}W-{tl}L", tp, twp, troi)

    # Phase 3: ALL PICKS BY SPORT (frames 121-300, 6s)
    _render_all_picks_frames(frames, show_picks)

    # Phase 4-5: CTA + FADE OUT (frames 301-360 CTA, 361-390 fade — adjusted for picks)
    _render_cta_frames(frames, season_record)

    _render_fade_out(frames)

    while len(frames) < TOTAL_FRAMES:
        frames.append(frames[-1].copy())
    frames = frames[:TOTAL_FRAMES]

    output_path = os.path.join(CARDS_DIR, 'scottys_edge_picks_reel.mp4')
    print(f"  Reel: Encoding {len(frames)} frames ({len(frames)/FPS:.1f}s)")
    if _frames_to_video(frames, output_path):
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"  Reel: Done — {size_mb:.1f} MB")
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
            game_date, bets = _load_yesterday_bets(conn)
            wl_bets = [b for b in bets if b[1] in ('WIN', 'LOSS')]
            yw = sum(1 for b in wl_bets if b[1] == 'WIN')
            yl = sum(1 for b in wl_bets if b[1] == 'LOSS')
            yp = sum(b[2] or 0 for b in wl_bets)
            yp_dollars = _fmt_dollars(yp)
            caption = f"{yw}W-{yl}L yesterday | {yp_dollars} on $10 bets\n\nEvery pick tracked. Every loss shown.\n\n"
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
