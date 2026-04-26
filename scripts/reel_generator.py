#!/usr/bin/env python3
"""
reel_generator.py — Animated Instagram Reels from picks/results data.

USAGE: Manual tool only. Not wired into main.py or the daily pipeline.
Run directly:  python reel_generator.py results
Run directly:  python reel_generator.py picks

Kept rather than archived because the Sora-generated Kling video workflow
(see reference_sora_video.md) is the current default reel — but the
animated alternative is valuable as a fallback or for special-content drops.

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

Rendering: Internal 2160x3840 (2x) downscaled to 1080x1920 for anti-aliased text.

Usage:
    python reel_generator.py results    # Generate results reel
    python reel_generator.py picks      # Generate picks reel
    python reel_generator.py results --post  # Generate + post to Instagram
"""
import sqlite3, os, sys, subprocess, shutil, math, random
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import numpy as np

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
CARDS_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards')
os.makedirs(CARDS_DIR, exist_ok=True)

FFMPEG_PATH = os.environ.get('FFMPEG_PATH', os.path.join(
    os.path.expanduser('~'), 'AppData', 'Local', 'Microsoft', 'WinGet', 'Packages',
    'Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe', 'ffmpeg-8.1-full_build', 'bin', 'ffmpeg.exe'))

# ── Design constants ──
W, H = 1080, 1920            # Output resolution
RENDER_W, RENDER_H = 2160, 3840  # Internal 2x rendering resolution
BG = (13, 17, 23)       # #0d1117
CARD_BG = (20, 30, 42)  # #141e2a
GREEN = (0, 230, 118)   # #00e676
RED = (255, 82, 82)
WHITE = (255, 255, 255)
GRAY = (160, 170, 180)
DIM_GRAY = (80, 90, 100)
SHADOW_COLOR = (5, 5, 5)  # ~40% opacity black on dark bg
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
    """Get font at given size (caller provides 2x sizes for internal rendering)."""
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
    return a + (b - a) * t


def _color_lerp(c1, c2, t):
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


def _downscale(img):
    """Downscale from 2x render resolution to output resolution with LANCZOS."""
    return img.resize((W, H), Image.LANCZOS)


# ── Pre-computed background ──
_BG_CACHE = None

def _make_bg():
    """Create gradient background once, cache it."""
    global _BG_CACHE
    if _BG_CACHE is not None:
        return _BG_CACHE.copy()
    img = Image.new('RGB', (RENDER_W, RENDER_H), BG)
    draw = ImageDraw.Draw(img)
    cx, cy = RENDER_W // 2, RENDER_H // 2
    max_r = math.sqrt(cx * cx + cy * cy)
    steps = 150
    for step in range(0, steps):
        t = step / steps
        r = int(max_r * (1 - t))
        bright = 1.0 + 0.15 * (1 - t)
        color = tuple(min(255, int(c * bright)) for c in BG)
        x1, y1 = cx - r, cy - r
        x2, y2 = cx + r, cy + r
        draw.rectangle([(x1, y1), (x2, y2)], fill=color)
    _BG_CACHE = img
    return _BG_CACHE.copy()


# ── Grain ──
_GRAIN_CACHE = []

def _add_grain(img):
    """Add subtle film grain noise (~3% blend) using pre-cached grain textures."""
    if not _GRAIN_CACHE:
        print("  Reel: Pre-generating grain textures...")
        for _ in range(5):
            gray = np.random.randint(0, 9, (RENDER_H, RENDER_W), dtype=np.uint8)
            rgb = np.stack([gray, gray, gray], axis=-1)
            _GRAIN_CACHE.append(Image.fromarray(rgb, 'RGB'))
    grain = _GRAIN_CACHE[random.randint(0, len(_GRAIN_CACHE) - 1)]
    return Image.blend(img, grain, 0.03)


# ══════════════════════════════════════════════════════
# Drawing helpers (all coordinates at 2x scale)
# ══════════════════════════════════════════════════════

def _draw_shadow_text(draw, xy, text, font, color, shadow_offset=(6, 6)):
    """Draw text with a dark drop shadow for depth, then the main text on top."""
    x, y = xy
    sx, sy = shadow_offset
    draw.text((x + sx, y + sy), text, fill=SHADOW_COLOR, font=font)
    draw.text((x, y), text, fill=color, font=font)


def _draw_glow_text(draw, xy, text, font, color, intensity=6):
    """Draw text with a smooth Gaussian blur glow effect + drop shadow."""
    x, y = xy
    # Measure text to create a tight crop for the glow (much faster than full frame)
    tw = draw.textlength(text, font=font)
    # Estimate text height from font size
    th = font.size + 20
    pad = intensity * 4  # Padding for blur spread
    gw = int(tw + pad * 2)
    gh = int(th + pad * 2)
    if gw <= 0 or gh <= 0:
        _draw_shadow_text(draw, (x, y), text, font, color)
        return

    # Draw glow text on a small RGBA image
    glow_img = Image.new('RGBA', (gw, gh), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow_img)
    glow_color = color + (100,)
    glow_draw.text((pad, pad), text, fill=glow_color, font=font)
    blur_radius = max(1, intensity * 2)
    glow_img = glow_img.filter(ImageFilter.GaussianBlur(radius=blur_radius))

    # Paste the glow onto the main image
    base_img = draw._image
    ix, iy = int(x - pad), int(y - pad)
    # Extract the region, composite, paste back
    if ix >= 0 and iy >= 0 and ix + gw <= RENDER_W and iy + gh <= RENDER_H:
        region = base_img.crop((ix, iy, ix + gw, iy + gh)).convert('RGBA')
        region = Image.alpha_composite(region, glow_img)
        base_img.paste(region.convert('RGB'), (ix, iy))

    # Draw the sharp text on top with drop shadow
    _draw_shadow_text(draw, (x, y), text, font, color)


def _draw_centered_text(draw, y, text, font, color):
    """Draw centered text with drop shadow at 2x scale."""
    tw = draw.textlength(text, font=font)
    x = (RENDER_W - tw) / 2
    _draw_shadow_text(draw, (x, y), text, font, color)
    return tw


def _draw_centered_glow(draw, y, text, font, color, intensity=6):
    tw = draw.textlength(text, font=font)
    _draw_glow_text(draw, ((RENDER_W - tw) / 2, y), text, font, color, intensity)
    return tw


def _draw_rounded_rect(draw, xy, fill, radius=40):
    x1, y1, x2, y2 = xy
    draw.rectangle([(x1 + radius, y1), (x2 - radius, y2)], fill=fill)
    draw.rectangle([(x1, y1 + radius), (x2, y2 - radius)], fill=fill)
    draw.pieslice([(x1, y1), (x1 + 2*radius, y1 + 2*radius)], 180, 270, fill=fill)
    draw.pieslice([(x2 - 2*radius, y1), (x2, y1 + 2*radius)], 270, 360, fill=fill)
    draw.pieslice([(x1, y2 - 2*radius), (x1 + 2*radius, y2)], 90, 180, fill=fill)
    draw.pieslice([(x2 - 2*radius, y2 - 2*radius), (x2, y2)], 0, 90, fill=fill)


def _draw_accent_line(draw, y, width=600, color=GREEN):
    """Horizontal accent line centered at y (2x scale)."""
    x1 = (RENDER_W - width) // 2
    draw.rectangle([(x1, y), (x1 + width, y + 6)], fill=color)


def _new_frame():
    """Create a new frame at 2x render resolution with cached gradient background."""
    return _make_bg()


def _draw_logo(draw, y, alpha=1.0):
    """Draw SCOTTY'S EDGE logo at given y position (2x scale)."""
    logo_font = _get_font(116, bold=True)  # 58*2
    logo_text = "SCOTTY'S "
    edge_text = "EDGE"
    lw = draw.textlength(logo_text, font=logo_font)
    ew = draw.textlength(edge_text, font=logo_font)
    total = lw + ew
    x = (RENDER_W - total) / 2
    white_a = _alpha_color(WHITE, alpha)
    green_a = _alpha_color(GREEN, alpha)
    _draw_shadow_text(draw, (x, y), logo_text, logo_font, white_a)
    _draw_shadow_text(draw, (x + lw, y), edge_text, logo_font, green_a)


# ══════════════════════════════════════════════════════
# Streaming video encoder
# ══════════════════════════════════════════════════════

class VideoWriter:
    """Streams frames to ffmpeg as they are generated — no memory accumulation."""

    def __init__(self, output_path):
        ffmpeg_path = FFMPEG_PATH
        if not os.path.exists(ffmpeg_path):
            ffmpeg_path = shutil.which('ffmpeg')
        if not ffmpeg_path:
            raise RuntimeError("ffmpeg not found")

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
            '-crf', '18',
            '-movflags', '+faststart',
            output_path
        ]
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.count = 0
        self.output_path = output_path

    def write_frame(self, img):
        """Downscale from 2x and write a single frame."""
        downscaled = _downscale(img)
        self.proc.stdin.write(downscaled.tobytes())
        self.count += 1

    def finish(self):
        """Close the pipe, wait for ffmpeg, then mux audio."""
        self.proc.stdin.close()
        _, stderr = self.proc.communicate(timeout=300)
        if self.proc.returncode != 0:
            print(f"  Reel: ffmpeg error — {stderr.decode('utf-8', errors='replace')[:300]}")
            return False

        # Mux audio if ambient_beat.wav exists
        audio_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards', 'ambient_beat.wav')
        if os.path.exists(audio_path):
            ffmpeg_path = FFMPEG_PATH
            if not os.path.exists(ffmpeg_path):
                ffmpeg_path = shutil.which('ffmpeg')
            final_path = self.output_path.replace('.mp4', '_final.mp4')
            cmd = [
                ffmpeg_path, '-y',
                '-i', self.output_path,
                '-i', audio_path,
                '-c:v', 'copy',
                '-c:a', 'aac',
                '-shortest',
                final_path
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=300)
            if result.returncode == 0:
                os.replace(final_path, self.output_path)
                print(f"  Reel: Audio muxed from {os.path.basename(audio_path)}")
            else:
                print(f"  Reel: Audio mux failed — {result.stderr.decode('utf-8', errors='replace')[:200]}")
        return True


# ══════════════════════════════════════════════════════
# Frame generators for RESULTS reel (all coords at 2x)
# ══════════════════════════════════════════════════════

def _render_hook_frames(writer, wins, losses, pnl):
    """Frames 0-75 (2.5s): HOOK — big record + P/L in dollars."""
    record_str = f"{wins}W-{losses}L"
    pnl_dollars = _fmt_dollars(pnl)
    pnl_str = f"{pnl_dollars} PROFIT" if pnl >= 0 else f"{pnl_dollars} LOSS"
    pnl_color = GREEN if pnl >= 0 else RED

    title_font = _get_font(96, bold=True)
    record_font = _get_font(280, bold=True)
    pnl_font = _get_font(160, bold=True)
    bets_font = _get_font(64, bold=True)  # ~40% of pnl_font size

    for f in range(75):  # 2.5s = 75 frames
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        fade_t = min(1.0, f / 35)
        alpha = _ease_out(fade_t)

        _draw_logo(draw, 400, alpha)
        _draw_centered_text(draw, 1300, "YESTERDAY'S RESULTS", title_font, _alpha_color(GRAY, alpha))
        _draw_accent_line(draw, 1420, 500, _alpha_color(GREEN, alpha))

        rec_color = _alpha_color(WHITE, alpha)
        tw = draw.textlength(record_str, font=record_font)
        x = (RENDER_W - tw) / 2
        _draw_glow_text(draw, (x, 1520), record_str, record_font, rec_color, int(8 * alpha))

        if f >= 10:
            pnl_t = min(1.0, (f - 10) / 25)
            pnl_a = _ease_out(pnl_t)
            _draw_centered_glow(draw, 1860, pnl_str, pnl_font, _alpha_color(pnl_color, pnl_a), int(6 * pnl_a))
            _draw_centered_text(draw, 2040, "ON $10 BETS", bets_font, _alpha_color(GRAY, pnl_a))

        writer.write_frame(_add_grain(img))


def _render_stats_frames(writer, record, pnl, wp, roi):
    """3s: SEASON RECORD — record + win rate + dollar profit."""
    w_count = int(record.split('W')[0])
    l_count = int(record.split('-')[1].replace('L', ''))

    header_font = _get_font(84, bold=True)
    record_font = _get_font(200, bold=True)
    wp_font = _get_font(140, bold=True)
    dollar_font = _get_font(120, bold=True)
    scale_font = _get_font(56)

    pnl_color = GREEN if pnl >= 0 else RED

    for f in range(90):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        _draw_logo(draw, 400, 1.0)
        _draw_centered_text(draw, 1120, "SEASON RECORD", header_font, GRAY)

        counter_t = min(1.0, f / 40)
        ct = _ease_in_out(counter_t)

        cur_w = int(w_count * ct)
        cur_l = int(l_count * ct)
        record_str = f"{cur_w}W - {cur_l}L"
        _draw_centered_text(draw, 1300, record_str, record_font, WHITE)

        _draw_accent_line(draw, 1540, 600, GREEN)

        cur_wp = wp * ct
        wp_str = f"{cur_wp:.1f}% WIN RATE"
        wp_color = GREEN if wp >= 55 else WHITE
        _draw_centered_text(draw, 1620, wp_str, wp_font, wp_color)

        _draw_centered_text(draw, 1770, "ON $10 BETS", scale_font, _alpha_color(GRAY, 0.7))

        if f >= 15:
            d_t = min(1.0, (f - 15) / 30)
            d_a = _ease_out(d_t)
            cur_dollars = _fmt_dollars(pnl * ct)
            _draw_centered_glow(draw, 1880, f"{cur_dollars} PROFIT", dollar_font,
                                _alpha_color(pnl_color, d_a), int(6 * d_a))

        writer.write_frame(_add_grain(img))


def _render_picks_frames(writer, bets):
    """5.5s: ALL picks as full-width rows, auto-scaled to fit."""
    # Sort: wins first (best to worst), then losses (worst to best)
    wins = [(sel, result, pnl, sport) for sel, result, pnl, sport in bets if result == 'WIN']
    losses = [(sel, result, pnl, sport) for sel, result, pnl, sport in bets if result == 'LOSS']
    wins.sort(key=lambda x: x[2] or 0, reverse=True)
    losses.sort(key=lambda x: x[2] or 0)
    display_picks = wins + losses

    n = len(display_picks)

    # Auto-scale fonts and spacing based on pick count
    if n <= 8:
        title_font = _get_font(84, bold=True)
        badge_font = _get_font(72, bold=True)
        units_font = _get_font(76, bold=True)
        name_font = _get_font(64, bold=True)
        sport_font = _get_font(40, bold=True)
        row_spacing = 180
        start_y = 900
        badge_r = 50
        max_sel = 35
    elif n <= 13:
        title_font = _get_font(76, bold=True)
        badge_font = _get_font(56, bold=True)
        units_font = _get_font(60, bold=True)
        name_font = _get_font(50, bold=True)
        sport_font = _get_font(36, bold=True)
        row_spacing = 140
        start_y = 880
        badge_r = 40
        max_sel = 32
    else:  # 14+
        title_font = _get_font(72, bold=True)
        badge_font = _get_font(44, bold=True)
        units_font = _get_font(48, bold=True)
        name_font = _get_font(42, bold=True)
        sport_font = _get_font(30, bold=True)
        row_spacing = 110
        start_y = 860
        badge_r = 32
        max_sel = 28

    total_frames = 165

    for f in range(total_frames):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        _draw_logo(draw, 400, 1.0)
        _draw_centered_text(draw, 740, "RESULTS", title_font, GRAY)
        _draw_accent_line(draw, 840, 400, GREEN)

        fade_t = min(1.0, f / 30)
        fade_a = _ease_out(fade_t)

        y = start_y
        for pi, (sel, result, pnl, sport) in enumerate(display_picks):
            is_win = result == 'WIN'
            row_color = GREEN if is_win else RED

            badge_cx = 160 + badge_r
            badge_cy = y + badge_r + 10
            badge_fill = _alpha_color(row_color, fade_a)
            draw.ellipse([(badge_cx - badge_r, badge_cy - badge_r),
                          (badge_cx + badge_r, badge_cy + badge_r)], fill=badge_fill)
            letter = "W" if is_win else "L"
            lw = draw.textlength(letter, font=badge_font)
            _draw_shadow_text(draw, (badge_cx - lw / 2, badge_cy - badge_font.size // 2),
                              letter, badge_font, _alpha_color(WHITE, fade_a))

            pnl_str = _fmt_dollars(pnl) if pnl else "+$0"
            pnl_x = 160 + badge_r * 2 + 30
            _draw_shadow_text(draw, (pnl_x, y + 10), pnl_str,
                              units_font, _alpha_color(row_color, fade_a))

            short_sel = sel[:max_sel] + "..." if len(sel) > max_sel else sel
            _draw_shadow_text(draw, (620, y + 14), short_sel,
                              name_font, _alpha_color(WHITE, fade_a))

            sp = _sport_label(sport)
            if sp:
                sp_color = SPORT_COLORS.get(sp, GREEN)
                sp_tw = draw.textlength(sp, font=sport_font)
                _draw_shadow_text(draw, (RENDER_W - 160 - sp_tw, y + 18), sp,
                                  sport_font, _alpha_color(sp_color, fade_a))

            bar_y = y + row_spacing - 16
            draw.rectangle([(160, bar_y), (RENDER_W - 160, bar_y + 4)],
                           fill=_alpha_color(row_color, fade_a * 0.4))

            y += row_spacing

        writer.write_frame(_add_grain(img))


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

    return lines[:4]


def _render_day_summary_frames(writer, bets):
    """3.5s: DAY SUMMARY — casual sport lines + verdict."""
    summary_lines = _generate_day_summary(bets)

    total_pnl = 0
    for sel, result, pnl, sport in bets:
        if result in ('WIN', 'LOSS'):
            total_pnl += (pnl or 0)

    if total_pnl >= 5:
        verdict, verdict_color = "GREEN DAY", GREEN
    elif total_pnl >= 0:
        verdict, verdict_color = "BREAKEVEN", GREEN
    else:
        verdict, verdict_color = "TOUGH DAY -- TRUST THE PROCESS", RED

    title_font = _get_font(84, bold=True)
    line_font = _get_font(80, bold=True)
    verdict_font = _get_font(104, bold=True)

    n_lines = len(summary_lines)
    line_spacing = 150
    start_y = 1300
    total_frames = 105

    for f in range(total_frames):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        _draw_logo(draw, 400, 1.0)

        title_a = min(1.0, f / 25)
        _draw_centered_text(draw, 1100, "THE DAY", title_font, _alpha_color(GRAY, _ease_out(title_a)))
        _draw_accent_line(draw, 1210, 400, _alpha_color(GREEN, _ease_out(title_a)))

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

        verdict_delay = 10 + n_lines * 12 + 10
        if f >= verdict_delay:
            v_t = min(1.0, (f - verdict_delay) / 20)
            v_a = _ease_out(v_t)
            verdict_y = start_y + n_lines * line_spacing + 120
            _draw_centered_glow(draw, verdict_y, verdict, verdict_font,
                                _alpha_color(verdict_color, v_a), int(6 * v_a))

        writer.write_frame(_add_grain(img))


def _render_trust_frames(writer):
    """3s: TRUST/PROOF — transparency differentiator."""
    line1_font = _get_font(104, bold=True)
    handle_font = _get_font(72)

    for f in range(90):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        fade_t = min(1.0, f / 30)
        alpha = _ease_out(fade_t)

        _draw_centered_text(draw, 1560, "EVERY PICK TRACKED", line1_font, _alpha_color(WHITE, alpha))

        if f >= 8:
            t2 = min(1.0, (f - 8) / 25)
            a2 = _ease_out(t2)
            _draw_centered_glow(draw, 1720, "EVERY LOSS SHOWN", line1_font,
                                _alpha_color(GREEN, a2), int(6 * a2))

        if f >= 16:
            t3 = min(1.0, (f - 16) / 20)
            a3 = _ease_out(t3)
            _draw_centered_text(draw, 1920, "@scottys_edge", handle_font, _alpha_color(GRAY, a3))

        writer.write_frame(_add_grain(img))


def _render_fade_out(writer):
    """2.5s: Fade in SCOTTY'S EDGE logo with green glow."""
    logo_font = _get_font(176, bold=True)

    for f in range(75):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        fade_t = min(1.0, f / 20)
        alpha = _ease_out(fade_t)

        logo_text = "SCOTTY'S "
        edge_text = "EDGE"
        lw = draw.textlength(logo_text, font=logo_font)
        ew = draw.textlength(edge_text, font=logo_font)
        total = lw + ew
        x = (RENDER_W - total) / 2
        y = (RENDER_H - 160) / 2

        _draw_shadow_text(draw, (x, y), logo_text, logo_font, _alpha_color(WHITE, alpha))
        _draw_glow_text(draw, (x + lw, y), edge_text, logo_font, _alpha_color(GREEN, alpha), int(8 * alpha))

        writer.write_frame(_add_grain(img))


def _render_all_picks_frames(writer, picks):
    """6s: All picks grouped by sport."""
    title_font = _get_font(84, bold=True)
    pick_font = _get_font(60, bold=True)
    sport_font = _get_font(48, bold=True)

    total_frames = 180
    row_spacing = 100
    start_y = 900

    for f in range(total_frames):
        img = _new_frame()
        draw = ImageDraw.Draw(img)
        _draw_logo(draw, 400, 1.0)
        _draw_centered_text(draw, 740, "TODAY'S CARD", title_font, GRAY)
        _draw_accent_line(draw, 840, 400, GREEN)

        fade_t = min(1.0, f / 30)
        fade_a = _ease_out(fade_t)

        y = start_y
        for pi, (sel, _, units, sport) in enumerate(picks[:10]):
            sp = _sport_label(sport)
            sp_color = SPORT_COLORS.get(sp, GREEN)
            short_sel = sel[:40] + "..." if len(sel) > 40 else sel
            _draw_shadow_text(draw, (160, y), short_sel, pick_font, _alpha_color(WHITE, fade_a))
            if sp:
                _draw_shadow_text(draw, (RENDER_W - 400, y), sp, sport_font, _alpha_color(sp_color, fade_a))
            y += row_spacing

        writer.write_frame(_add_grain(img))


def _render_cta_frames(writer, season_record):
    """2s: CTA slide."""
    cta_font = _get_font(96, bold=True)
    sub_font = _get_font(64)

    for f in range(60):
        img = _new_frame()
        draw = ImageDraw.Draw(img)

        fade_t = min(1.0, f / 25)
        alpha = _ease_out(fade_t)

        _draw_centered_glow(draw, 1500, "FOLLOW FOR DAILY PICKS", cta_font, _alpha_color(GREEN, alpha), int(6 * alpha))
        if f >= 10:
            t2 = min(1.0, (f - 10) / 20)
            a2 = _ease_out(t2)
            _draw_centered_text(draw, 1660, "@scottys_edge", sub_font, _alpha_color(GRAY, a2))

        _draw_logo(draw, 400, 1.0)
        writer.write_frame(_add_grain(img))


# ══════════════════════════════════════════════════════
# Data loading
# ══════════════════════════════════════════════════════

def _load_season_stats(conn):
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
    print(f"  Reel: Building {TOTAL_FRAMES} frames @ 2x ({RENDER_W}x{RENDER_H}) — {yw}W-{yl}L, {yp_dollars}, {len(wl_bets)} picks")

    output_path = os.path.join(CARDS_DIR, 'scottys_edge_reel.mp4')

    try:
        writer = VideoWriter(output_path)
    except RuntimeError as e:
        print(f"  Reel: {e}")
        return None

    # Slide 1: HOOK (76 frames, 2.5s)
    _render_hook_frames(writer, yw, yl, yp)
    print(f"  Reel: Hook done ({writer.count} frames)")

    # Slide 2: SEASON RECORD (90 frames, 3s)
    _render_stats_frames(writer, f"{tw}W-{tl}L", tp, twp, troi)
    print(f"  Reel: Stats done ({writer.count} frames)")

    # Slide 3: PICKS (165 frames, 5.5s)
    _render_picks_frames(writer, wl_bets)
    print(f"  Reel: Picks done ({writer.count} frames)")

    # Slide 4: DAY SUMMARY (105 frames, 3.5s)
    _render_day_summary_frames(writer, wl_bets)
    print(f"  Reel: Summary done ({writer.count} frames)")

    # Slide 5: TRUST (90 frames, 3s)
    _render_trust_frames(writer)
    print(f"  Reel: Trust done ({writer.count} frames)")

    # Slide 6: FADE OUT (75 frames, 2.5s)
    _render_fade_out(writer)
    print(f"  Reel: Fade done ({writer.count} frames)")

    # Pad to exactly 600 if needed
    while writer.count < TOTAL_FRAMES:
        img = _new_frame()
        writer.write_frame(img)

    print(f"  Reel: Encoding {writer.count} frames ({writer.count/FPS:.1f}s) — downscaling to {W}x{H}")
    if writer.finish():
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

    show_picks = []
    for sel, sport, odds, units in picks:
        odds_str = f"({int(odds):+d})" if odds else ""
        show_picks.append((f"{sel} {odds_str}", 'WIN', units, sport))

    print(f"  Reel: Building picks reel @ 2x ({RENDER_W}x{RENDER_H}) — {len(show_picks)} picks")

    output_path = os.path.join(CARDS_DIR, 'scottys_edge_picks_reel.mp4')

    try:
        writer = VideoWriter(output_path)
    except RuntimeError as e:
        print(f"  Reel: {e}")
        return None

    # Phase 1: HOOK (61 frames, 2s)
    title_font = _get_font(96, bold=True)
    count_font = _get_font(272, bold=True)
    sub_font = _get_font(100, bold=True)

    for f in range(61):
        img = _new_frame()
        draw = ImageDraw.Draw(img)
        t = min(1.0, f / 30)
        alpha = _ease_out(t)
        _draw_centered_text(draw, 1360, "TODAY'S PLAYS", title_font, _alpha_color(GRAY, alpha))
        _draw_accent_line(draw, 1480, 500, _alpha_color(GREEN, alpha))
        if f >= 10:
            rt = min(1.0, (f - 10) / 25)
            ra = _ease_out(rt)
            _draw_centered_glow(draw, 1580, f"{len(picks)} PICKS", count_font, _alpha_color(GREEN, ra), int(8 * ra))
        if f >= 25:
            pt = min(1.0, (f - 25) / 25)
            pa = _ease_out(pt)
            _draw_centered_text(draw, 1860, season_record, sub_font, _alpha_color(GRAY, pa))
        _draw_logo(draw, 400, min(1.0, f / 15))
        writer.write_frame(_add_grain(img))

    # Phase 2: SEASON STATS (90 frames, 3s)
    _render_stats_frames(writer, f"{tw}W-{tl}L", tp, twp, troi)

    # Phase 3: ALL PICKS (180 frames, 6s)
    _render_all_picks_frames(writer, show_picks)

    # Phase 4: CTA (60 frames, 2s)
    _render_cta_frames(writer, season_record)

    # Phase 5: FADE OUT (75 frames, 2.5s)
    _render_fade_out(writer)

    while writer.count < TOTAL_FRAMES:
        img = _new_frame()
        writer.write_frame(img)

    print(f"  Reel: Encoding {writer.count} frames ({writer.count/FPS:.1f}s) — downscaling to {W}x{H}")
    if writer.finish():
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
