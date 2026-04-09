"""
card_image.py — Instagram-optimized PNG cards (1080x1350, 4:5 ratio, 2x retina)

All cards render at exactly 2160x2700 (1080x1350 @2x) — Instagram's max feed size.
Content that overflows gets split into multiple slides automatically.

Generates:
  1. PICKS CARDS — MAX PLAY slide + STRONG slide
  2. RESULTS CARDS — Winners slide + Losers slide
  3. STATS CARD — Running performance data
  4. CAPTION — Copyable text for Instagram/Twitter

Usage:
    python card_image.py                    # Test picks card
    python card_image.py --stats            # Stats card
    python card_image.py --results          # Results cards
"""
import os, sys, sqlite3
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Instagram 4:5 at 2x retina
S = 2
IG_W = 1080 * S   # 2160
IG_H = 1350 * S   # 2700
CARD_WIDTH = IG_W
PADDING = 52 * S
INNER_WIDTH = CARD_WIDTH - (PADDING * 2)

BG = (13, 17, 23)
CARD_BG = (20, 30, 42)
GREEN = (0, 230, 118)
WHITE = (255, 255, 255)
WHITE_80 = (204, 204, 204)
WHITE_60 = (153, 153, 153)
WHITE_40 = (102, 102, 102)
WHITE_25 = (64, 64, 64)
RED = (255, 82, 82)
YELLOW = (255, 193, 7)
BLUE = (100, 181, 246)
ORANGE = (255, 152, 0)
TIER_COLORS = {'MAX PLAY': GREEN, 'STRONG': YELLOW, 'SOLID': BLUE, 'LEAN': WHITE_60, 'SPRINKLE': WHITE_40}

SPORT_LABELS = {
    'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB', 'icehockey_nhl': 'NHL',
    'baseball_ncaa': 'BASEBALL', 'soccer_epl': 'EPL', 'soccer_germany_bundesliga': 'BUNDESLIGA',
    'soccer_france_ligue_one': 'LIGUE 1', 'soccer_italy_serie_a': 'SERIE A',
    'soccer_spain_la_liga': 'LA LIGA', 'soccer_usa_mls': 'MLS', 'soccer_uefa_champs_league': 'UCL',
    'soccer_mexico_ligamx': 'LIGA MX',
    'baseball_mlb': 'MLB',
}
SPORT_BADGE_COLORS = {
    'basketball_nba': (255, 100, 50), 'basketball_ncaab': (255, 152, 0),
    'icehockey_nhl': (50, 150, 255), 'baseball_ncaa': (120, 180, 60),
    'soccer_epl': (130, 50, 200), 'soccer_germany_bundesliga': (130, 50, 200),
    'soccer_france_ligue_one': (130, 50, 200), 'soccer_italy_serie_a': (130, 50, 200),
    'soccer_spain_la_liga': (130, 50, 200), 'soccer_usa_mls': (130, 50, 200),
    'soccer_uefa_champs_league': (130, 50, 200),
    'soccer_mexico_ligamx': (130, 50, 200),
    'baseball_mlb': (120, 180, 60),
}
# Tennis: dynamically add labels and badge colors from config
try:
    from config import TENNIS_SPORTS, TENNIS_LABELS
    for _tk in TENNIS_SPORTS:
        SPORT_LABELS[_tk] = TENNIS_LABELS.get(_tk, _tk.split('_')[-1].upper())
        SPORT_BADGE_COLORS[_tk] = (200, 200, 50)  # Yellow-gold for tennis
except ImportError:
    pass
SOCIALS = "IG: @scottys_edge | X: @Scottys_edge | Discord: discord.gg/JQ6rRfuN"
DISCLAIMER = ("For entertainment and informational purposes only. Not gambling advice. "
              "Past performance does not guarantee future results. Please gamble responsibly. "
              "If you or someone you know has a gambling problem, call 1-800-GAMBLER. "
              "Must be 21+ and located in a legal jurisdiction. "
              "Scotty's Edge does not accept or place bets on behalf of users.")


def _load_fonts():
    paths = {
        'bold': ['C:/Windows/Fonts/arialbd.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'],
        'regular': ['C:/Windows/Fonts/arial.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'],
    }
    fonts = {}
    for style, ps in paths.items():
        fonts[style] = None
        for p in ps:
            if os.path.exists(p):
                fonts[style] = p; break
    return fonts

def _font(fonts, style, size):
    path = fonts.get(style)
    if path:
        try: return ImageFont.truetype(path, size * S)
        except Exception: pass
    return ImageFont.load_default()

def _to_eastern(utc_str):
    try:
        dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
        offset = 4 if 3 <= dt.month <= 10 else 5
        eastern = dt - timedelta(hours=offset)
        tz = 'EDT' if 3 <= dt.month <= 10 else 'EST'
        return eastern.strftime('%I:%M %p') + f' {tz}'
    except Exception: return ''

def _draw_divider(draw, y):
    mid = CARD_WIDTH // 2
    for x in range(PADDING, CARD_WIDTH - PADDING):
        alpha = max(0, 1.0 - abs(x - mid) / (INNER_WIDTH / 2))
        c = int(230 * alpha * 0.3)
        draw.point((x, y), fill=(0, c, int(c * 0.5)))

def _draw_header(draw, fonts, y, subtitle="DATA-DRIVEN SPORTS PICKS"):
    now = datetime.now()
    tf = _font(fonts, 'bold', 58)
    draw.text((PADDING, y), "SCOTTY'S ", fill=WHITE, font=tf)
    sw = draw.textlength("SCOTTY'S ", font=tf)
    draw.text((PADDING + sw, y), "EDGE", fill=GREEN, font=tf)
    sf = _font(fonts, 'regular', 17)
    draw.text((PADDING, y + 66*S), subtitle, fill=WHITE_40, font=sf)
    df = _font(fonts, 'bold', 30); df2 = _font(fonts, 'regular', 16)
    day_str = now.strftime('%A').upper()
    date_str = now.strftime('%B %d, %Y').upper()
    dw = draw.textlength(day_str, font=df)
    dtw = draw.textlength(date_str, font=df2)
    draw.text((CARD_WIDTH - PADDING - dw, y + 8*S), day_str, fill=WHITE_80, font=df)
    draw.text((CARD_WIDTH - PADDING - dtw, y + 44*S), date_str, fill=WHITE_40, font=df2)
    return y + 105*S

def _draw_disclaimer(draw, fonts, y):
    draw.line([(PADDING, y), (CARD_WIDTH - PADDING, y)], fill=WHITE_25, width=S)
    y += 14*S
    # Social handles
    social_font = _font(fonts, 'bold', 16)
    social_w = draw.textlength(SOCIALS, font=social_font)
    social_x = (CARD_WIDTH - social_w) // 2  # Center
    draw.text((social_x, y), SOCIALS, fill=(0, 200, 120), font=social_font)
    y += 30*S
    disc_font = _font(fonts, 'regular', 11)
    words = DISCLAIMER.split()
    lines, current = [], ""
    for word in words:
        test = current + " " + word if current else word
        if draw.textlength(test, font=disc_font) > INNER_WIDTH:
            lines.append(current); current = word
        else: current = test
    if current: lines.append(current)
    for line in lines:
        draw.text((PADDING, y), line, fill=WHITE_25, font=disc_font)
        y += 17*S
    return y

def _draw_sport_badge(draw, fonts, x, y, sport):
    label = SPORT_LABELS.get(sport, '?')
    color = SPORT_BADGE_COLORS.get(sport, WHITE_40)
    bf = _font(fonts, 'bold', 13)
    tw = draw.textlength(label, font=bf)
    bw = tw + 16*S
    bg = (color[0]//5, color[1]//5, color[2]//5)
    draw.rectangle([(x, y), (x+bw, y+22*S)], fill=bg)
    draw.text((x+8*S, y+3*S), label, fill=color, font=bf)
    return bw + 8*S

def _draw_timing_badge(draw, fonts, x, y, timing):
    if not timing or timing == 'STANDARD': return 0
    label, color = ("BET NOW", ORANGE) if timing == 'EARLY' else ("BET LATE", BLUE)
    bf = _font(fonts, 'bold', 12)
    tw = draw.textlength(label, font=bf)
    bw = tw + 14*S
    bg = (color[0]//6, color[1]//6, color[2]//6)
    draw.rectangle([(x, y), (x+bw, y+20*S)], fill=bg)
    draw.text((x+7*S, y+3*S), label, fill=color, font=bf)
    return bw + 8*S

def _get_desktop():
    d = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards')
    os.makedirs(d, exist_ok=True)
    return d

def _finalize(img, y):
    """Pad or crop image to exact IG_H. Content pinned to top for mobile readability."""
    final = Image.new('RGB', (IG_W, IG_H), BG)
    content_h = y + 20*S
    # Always pin to top — no vertical centering (wastes space on mobile)
    paste_h = min(content_h, IG_H)
    final.paste(img.crop((0, 0, IG_W, paste_h)), (0, 0))
    return final


# ═══════════════════════════════════════════════════════════════
# PICKS CARD
# ═══════════════════════════════════════════════════════════════

def _render_picks_slide(picks, fonts, section_label, show_units_explain=True):
    from scottys_edge import kelly_label
    n = len(picks)
    ctx_count = sum(1 for p in picks if p.get('context'))

    # Dynamic layout — calculate pick_h to fill the card exactly
    # Fixed overhead: top bar(5) + top pad(20) + header(105) + divider gap(26)
    #   + section label(38) + bottom divider gap(20) + summary(70)
    #   + unit explain(55 if shown) + disclaimer(~85)
    fixed = (5 + 20 + 105 + 26 + 38 + 20 + 70 + 85) * S
    if show_units_explain:
        fixed += 55 * S
    ctx_extra = ctx_count * 22 * S
    pick_gaps = n * 14 * S  # gap after each pick box
    avail = IG_H - fixed - ctx_extra - pick_gaps
    pick_h = avail // max(n, 1)
    pick_h = max(120*S, min(275*S, pick_h))  # clamp to sane range

    # Scale font sizes based on pick_h for balanced proportions
    # Reference: pick_h=155*S is the "standard" size
    scale = pick_h / (155*S)
    name_sz = max(22, min(32, int(28 * scale)))
    detail_sz = max(14, min(20, int(17 * scale)))
    book_sz = max(13, min(18, int(15 * scale)))
    ctx_sz = max(13, min(18, int(15 * scale)))
    odds_sz = max(28, min(44, int(38 * scale)))
    units_sz = max(14, min(20, int(17 * scale)))
    tier_sz = max(12, min(17, int(14 * scale)))

    total_h = max(IG_H, fixed + n*(pick_h+14*S) + ctx_extra)
    img = Image.new('RGB', (CARD_WIDTH, total_h), BG)
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0,0),(CARD_WIDTH,5*S)], fill=GREEN)
    y = 20*S
    y = _draw_header(draw, fonts, y)
    y += 8*S; _draw_divider(draw, y); y += 18*S
    draw.text((PADDING, y), section_label, fill=GREEN, font=_font(fonts, 'bold', 18)); y += 38*S

    pnf=_font(fonts,'bold',name_sz); pdf=_font(fonts,'regular',detail_sz)
    pbf=_font(fonts,'regular',book_sz); pcf=_font(fonts,'regular',ctx_sz)
    of=_font(fonts,'bold',odds_sz); uf=_font(fonts,'regular',units_sz)
    ttf=_font(fonts,'bold',tier_sz)

    # Vertical offsets inside pick box, scaled proportionally
    badge_y = int(12 * scale) * S
    name_y = int(40 * scale) * S
    matchup_y = int(72 * scale) * S
    book_y = int(94 * scale) * S
    ctx_y = int(116 * scale) * S
    odds_y = int(20 * scale) * S
    units_y = int(62 * scale) * S
    tier_y_top = int(88 * scale) * S
    tier_y_bot = int(112 * scale) * S
    tier_txt_y = int(92 * scale) * S

    # Reserve right column width for odds/units/tier badge
    # Measure widest possible odds string to set a fixed right column
    right_col_w = 160 * S  # Reserve fixed space for odds column
    name_max_w = CARD_WIDTH - PADDING*2 - 22*S - right_col_w - 10*S  # 10*S gap

    for p in picks:
        kl=kelly_label(p['units']); tier_color=TIER_COLORS.get(kl, WHITE_60)
        game_time=_to_eastern(p.get('commence','')); ctx=p.get('context','')
        if ctx: ctx = ctx.replace('[SHADOW] ', '').replace('?.??', 'N/A')  # v23.1: strip internal tags from public cards
        matchup=f"{p.get('home','')} vs {p.get('away','')} \u2022 {game_time}"
        box_h=pick_h+(22*S if ctx else 0)
        draw.rectangle([(PADDING,y),(CARD_WIDTH-PADDING,y+box_h)], fill=CARD_BG)
        draw.rectangle([(PADDING,y),(PADDING+5*S,y+box_h)], fill=GREEN)
        tx=PADDING+22*S
        bx=tx; bw=_draw_sport_badge(draw,fonts,bx,y+badge_y,p.get('sport','')); bx+=bw
        if p.get('timing'): _draw_timing_badge(draw,fonts,bx,y+badge_y+1*S,p['timing'])
        # Draw selection name — shrink font if it would overlap the odds column
        sel_text = p['selection']
        sel_font = pnf
        sel_w = draw.textlength(sel_text, font=sel_font)
        if sel_w > name_max_w:
            # Try progressively smaller fonts until it fits
            for shrink_sz in range(name_sz - 2, 16, -2):
                sel_font = _font(fonts, 'bold', shrink_sz)
                sel_w = draw.textlength(sel_text, font=sel_font)
                if sel_w <= name_max_w:
                    break
        draw.text((tx,y+name_y),sel_text,fill=WHITE,font=sel_font)
        draw.text((tx,y+matchup_y),matchup,fill=WHITE_40,font=pdf)
        draw.text((tx,y+book_y),p.get('book',''),fill=WHITE_40,font=pbf)
        if ctx:
            _ctx_max = CARD_WIDTH - PADDING*2 - 22*S - 10*S
            while draw.textlength(ctx, font=pcf) > _ctx_max and len(ctx) > 10:
                ctx = ctx.rsplit(' | ', 1)[0] + '...' if ' | ' in ctx else ctx[:len(ctx)-4] + '...'
            draw.text((tx,y+ctx_y),ctx,fill=GREEN,font=pcf)
        rx=CARD_WIDTH-PADDING-22*S
        os_=f"{p['odds']:+.0f}"; ow=draw.textlength(os_,font=of)
        draw.text((rx-ow,y+odds_y),os_,fill=WHITE,font=of)
        us_=f"{p['units']:.1f} units"; uw=draw.textlength(us_,font=uf)
        draw.text((rx-uw,y+units_y),us_,fill=WHITE_60,font=uf)
        tw=draw.textlength(kl,font=ttf); bwt=tw+20*S; bxt=rx-bwt
        bbg=(tier_color[0]//6,tier_color[1]//6,tier_color[2]//6)
        draw.rectangle([(bxt,y+tier_y_top),(bxt+bwt,y+tier_y_bot)],fill=bbg)
        draw.text((bxt+10*S,y+tier_txt_y),kl,fill=tier_color,font=ttf)
        y+=box_h+14*S
    _draw_divider(draw,y); y+=20*S
    tu=sum(p['units'] for p in picks)
    stf=_font(fonts,'bold',36); slf=_font(fonts,'regular',14)
    draw.text((PADDING+30*S,y),str(len(picks)),fill=WHITE,font=stf)
    draw.text((PADDING+30*S,y+40*S),"PLAYS",fill=WHITE_40,font=slf)
    draw.text((PADDING+150*S,y),f"{tu:.0f}u",fill=WHITE,font=stf)
    draw.text((PADDING+150*S,y+40*S),"TOTAL",fill=WHITE_40,font=slf)
    trf=_font(fonts,'regular',16); ts="Every pick tracked & graded"
    tsw=draw.textlength(ts,font=trf)
    draw.text((CARD_WIDTH-PADDING-tsw,y+14*S),ts,fill=WHITE_40,font=trf)
    y+=70*S
    if show_units_explain:
        ef=_font(fonts,'regular',13)
        draw.text((PADDING,y),"UNIT SIZING: 1 unit = 1% of your bankroll. If your bankroll is $1,000, one unit = $10.",fill=WHITE_40,font=ef)
        draw.text((PADDING,y+20*S),"A 5.0u MAX PLAY at $10/unit = $50 wager. Scale to your comfort level.",fill=WHITE_40,font=ef)
        y+=55*S
    y=_draw_disclaimer(draw,fonts,y)
    return _finalize(img, y)


def _render_picks_slide_grouped(items, fonts, section_label, show_units_explain=True):
    """Render a picks card with sport group headers interleaved."""
    from scottys_edge import kelly_label

    # Count actual picks (not headers)
    pick_items = [item for kind, item in items if kind == '__PICK__']
    n = len(pick_items)
    header_count = sum(1 for kind, _ in items if kind == '__HEADER__')
    ctx_count = sum(1 for p in pick_items if p.get('context'))

    # Dynamic layout
    fixed = (5 + 20 + 105 + 26 + 38 + 20 + 70 + 85) * S
    if show_units_explain:
        fixed += 55 * S
    header_space = header_count * 48 * S  # Space for sport headers
    ctx_extra = ctx_count * 22 * S
    pick_gaps = n * 14 * S
    avail = IG_H - fixed - ctx_extra - pick_gaps - header_space
    pick_h = avail // max(n, 1)
    pick_h = max(120*S, min(275*S, pick_h))

    scale = pick_h / (155*S)
    name_sz = max(22, min(32, int(28 * scale)))
    detail_sz = max(14, min(20, int(17 * scale)))
    book_sz = max(13, min(18, int(15 * scale)))
    ctx_sz = max(13, min(18, int(15 * scale)))
    odds_sz = max(28, min(44, int(38 * scale)))
    units_sz = max(14, min(20, int(17 * scale)))
    tier_sz = max(12, min(17, int(14 * scale)))

    total_h = max(IG_H, fixed + n*(pick_h+14*S) + ctx_extra + header_space)
    img = Image.new('RGB', (CARD_WIDTH, total_h), BG)
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0,0),(CARD_WIDTH,5*S)], fill=GREEN)
    y = 20*S
    y = _draw_header(draw, fonts, y)
    y += 8*S; _draw_divider(draw, y); y += 18*S
    draw.text((PADDING, y), section_label, fill=GREEN, font=_font(fonts, 'bold', 18)); y += 38*S

    pnf=_font(fonts,'bold',name_sz); pdf=_font(fonts,'regular',detail_sz)
    pbf=_font(fonts,'regular',book_sz); pcf=_font(fonts,'regular',ctx_sz)
    of=_font(fonts,'bold',odds_sz); uf=_font(fonts,'regular',units_sz)
    ttf=_font(fonts,'bold',tier_sz)

    badge_y = int(12 * scale) * S
    name_y = int(40 * scale) * S
    matchup_y = int(72 * scale) * S
    book_y = int(94 * scale) * S
    ctx_y = int(116 * scale) * S
    odds_y = int(20 * scale) * S
    units_y = int(62 * scale) * S
    tier_y_top = int(88 * scale) * S
    tier_y_bot = int(112 * scale) * S
    tier_txt_y = int(92 * scale) * S
    right_col_w = 160 * S
    name_max_w = CARD_WIDTH - PADDING*2 - 22*S - right_col_w - 10*S

    sport_hdr_font = _font(fonts, 'bold', 20)

    for kind, item in items:
        if kind == '__HEADER__':
            # Draw sport section header
            icon = SPORT_ICONS.get(item.replace(' (CONT.)', ''), '🏟️')
            hdr_text = f"{icon}  {item}"
            y += 8*S
            draw.text((PADDING, y), hdr_text, fill=WHITE_80, font=sport_hdr_font)
            y += 32*S
            # Subtle divider under header
            draw.rectangle([(PADDING, y), (CARD_WIDTH - PADDING, y + 1*S)],
                           fill=(255, 255, 255, 20))
            y += 8*S
            continue

        p = item
        kl=kelly_label(p['units']); tier_color=TIER_COLORS.get(kl, WHITE_60)
        game_time=_to_eastern(p.get('commence','')); ctx=p.get('context','')
        if ctx: ctx = ctx.replace('[SHADOW] ', '').replace('?.??', 'N/A')  # v23.1: strip internal tags from public cards
        matchup=f"{p.get('home','')} vs {p.get('away','')} \u2022 {game_time}"
        box_h=pick_h+(22*S if ctx else 0)
        draw.rectangle([(PADDING,y),(CARD_WIDTH-PADDING,y+box_h)], fill=CARD_BG)
        draw.rectangle([(PADDING,y),(PADDING+5*S,y+box_h)], fill=GREEN)
        tx=PADDING+22*S
        bx=tx; bw=_draw_sport_badge(draw,fonts,bx,y+badge_y,p.get('sport','')); bx+=bw
        if p.get('timing'): _draw_timing_badge(draw,fonts,bx,y+badge_y+1*S,p['timing'])
        sel_text = p['selection']
        sel_font = pnf
        sel_w = draw.textlength(sel_text, font=sel_font)
        if sel_w > name_max_w:
            for shrink_sz in range(name_sz - 2, 16, -2):
                sel_font = _font(fonts, 'bold', shrink_sz)
                sel_w = draw.textlength(sel_text, font=sel_font)
                if sel_w <= name_max_w:
                    break
        draw.text((tx,y+name_y),sel_text,fill=WHITE,font=sel_font)
        draw.text((tx,y+matchup_y),matchup,fill=WHITE_40,font=pdf)
        draw.text((tx,y+book_y),p.get('book',''),fill=WHITE_40,font=pbf)
        if ctx:
            _ctx_max = CARD_WIDTH - PADDING*2 - 22*S - 10*S
            while draw.textlength(ctx, font=pcf) > _ctx_max and len(ctx) > 10:
                ctx = ctx.rsplit(' | ', 1)[0] + '...' if ' | ' in ctx else ctx[:len(ctx)-4] + '...'
            draw.text((tx,y+ctx_y),ctx,fill=GREEN,font=pcf)
        rx=CARD_WIDTH-PADDING-22*S
        os_=f"{p['odds']:+.0f}"; ow=draw.textlength(os_,font=of)
        draw.text((rx-ow,y+odds_y),os_,fill=WHITE,font=of)
        us_=f"{p['units']:.1f} units"; uw=draw.textlength(us_,font=uf)
        draw.text((rx-uw,y+units_y),us_,fill=WHITE_60,font=uf)
        tw=draw.textlength(kl,font=ttf); bwt=tw+20*S; bxt=rx-bwt
        bbg=(tier_color[0]//6,tier_color[1]//6,tier_color[2]//6)
        draw.rectangle([(bxt,y+tier_y_top),(bxt+bwt,y+tier_y_bot)],fill=bbg)
        draw.text((bxt+10*S,y+tier_txt_y),kl,fill=tier_color,font=ttf)
        y+=box_h+14*S

    _draw_divider(draw,y); y+=20*S
    tu=sum(p['units'] for p in pick_items)
    stf=_font(fonts,'bold',36); slf=_font(fonts,'regular',14)
    draw.text((PADDING+30*S,y),str(n),fill=WHITE,font=stf)
    draw.text((PADDING+30*S,y+40*S),"PLAYS",fill=WHITE_40,font=slf)
    draw.text((PADDING+150*S,y),f"{tu:.0f}u",fill=WHITE,font=stf)
    draw.text((PADDING+150*S,y+40*S),"TOTAL",fill=WHITE_40,font=slf)
    trf=_font(fonts,'regular',16); ts="Every pick tracked & graded"
    tsw=draw.textlength(ts,font=trf)
    draw.text((CARD_WIDTH-PADDING-tsw,y+14*S),ts,fill=WHITE_40,font=trf)
    y+=70*S
    if show_units_explain:
        ef=_font(fonts,'regular',13)
        draw.text((PADDING,y),"UNIT SIZING: 1 unit = 1% of your bankroll. If your bankroll is $1,000, one unit = $10.",fill=WHITE_40,font=ef)
        draw.text((PADDING,y+20*S),"A 5.0u MAX PLAY at $10/unit = $50 wager. Scale to your comfort level.",fill=WHITE_40,font=ef)
        y+=55*S
    y=_draw_disclaimer(draw,fonts,y)
    return _finalize(img, y)


def _generate_no_picks_card(fonts, output_path=None):
    """Generate a card when no picks meet the threshold — vertically centered."""
    img = Image.new('RGB', (IG_W, IG_H), BG)
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0,0),(CARD_WIDTH,5*S)], fill=GREEN)

    # Header at top
    y_header = 20*S
    y_header = _draw_header(draw, fonts, y_header)
    y_header += 10*S; _draw_divider(draw, y_header)
    header_bottom = y_header + 10*S

    # Measure disclaimer height from bottom
    disclaimer_h = 130*S  # approximate disclaimer block height

    # Calculate content block height to center it in the available space
    content_h = 120*S + 55*S*3 + 30*S + 50*S + 6*S + 50*S + 70*S  # title + lines + gap + bar + tag
    available = IG_H - header_bottom - disclaimer_h
    y = header_bottom + (available - content_h) // 2

    # Centered "NO EDGE TONIGHT" message
    nef = _font(fonts, 'bold', 64)
    msg = "NO EDGE TONIGHT"
    mw = draw.textlength(msg, font=nef)
    draw.text(((CARD_WIDTH - mw) // 2, y), msg, fill=WHITE, font=nef)
    y += 120*S

    # Subtitle lines
    sf = _font(fonts, 'regular', 30)
    lines = [
        "The model didn't find enough edge",
        "to recommend a play.",
        "",
        "We only bet when the data says to bet.",
        "Sitting out is part of the strategy.",
    ]
    for line in lines:
        if line == "":
            y += 30*S
            continue
        lw = draw.textlength(line, font=sf)
        draw.text(((CARD_WIDTH - lw) // 2, y), line, fill=WHITE_60, font=sf)
        y += 55*S

    y += 50*S
    # Green accent bar
    bar_w = 200*S
    draw.rectangle([((CARD_WIDTH - bar_w) // 2, y), ((CARD_WIDTH + bar_w) // 2, y + 6*S)], fill=GREEN)
    y += 50*S

    # Bottom tagline
    tf = _font(fonts, 'bold', 26)
    tag = "DISCIPLINE IS THE EDGE"
    tw = draw.textlength(tag, font=tf)
    draw.text(((CARD_WIDTH - tw) // 2, y), tag, fill=GREEN, font=tf)

    # Disclaimer pinned to bottom
    y_disc = IG_H - disclaimer_h
    _draw_divider(draw, y_disc); y_disc += 10*S
    _draw_disclaimer(draw, fonts, y_disc)

    if output_path is None:
        output_path = os.path.join(_get_desktop(), 'scottys_edge_card.png')
    img.save(output_path, 'PNG', quality=95)
    print(f"  \U0001f4f8 No-edge card: {output_path}")
    return output_path


SPORT_ORDER = ['NBA', 'NHL', 'NCAAB', 'NCAA BASEBALL',
               'EPL', 'LA LIGA', 'SERIE A', 'BUNDESLIGA', 'LIGUE 1', 'MLS', 'LIGA MX', 'UCL',
               'AUS OPEN', 'FRENCH OPEN', 'WIMBLEDON', 'US OPEN',
               'INDIAN WELLS', 'MIAMI OPEN', 'MONTE CARLO', 'MADRID OPEN',
               'ITALIAN OPEN', 'CANADIAN OPEN', 'CINCINNATI', 'SHANGHAI',
               'PARIS MASTERS', 'DUBAI', 'QATAR OPEN', 'CHINA OPEN',
               'AUS OPEN (W)', 'FRENCH OPEN (W)', 'WIMBLEDON (W)', 'US OPEN (W)',
               'INDIAN WELLS (W)', 'MIAMI OPEN (W)', 'MADRID OPEN (W)',
               'ITALIAN OPEN (W)', 'CANADIAN OPEN (W)', 'CINCINNATI (W)',
               'DUBAI (W)', 'QATAR OPEN (W)', 'CHINA OPEN (W)', 'WUHAN OPEN']

SPORT_ICONS = {
    'NBA': '🏀', 'NCAAB': '🏀', 'NHL': '🏒', 'BASEBALL': '⚾',
    'NCAA BASEBALL': '⚾', 'EPL': '⚽', 'BUNDESLIGA': '⚽', 'LIGUE 1': '⚽',
    'SERIE A': '⚽', 'LA LIGA': '⚽', 'MLS': '⚽', 'LIGA MX': '⚽', 'UCL': '⚽',
}
# Add tennis icons for all tournament labels
try:
    from config import TENNIS_LABELS
    for _label in TENNIS_LABELS.values():
        SPORT_ICONS[_label] = '🎾'
except ImportError:
    pass


def _group_picks_by_sport(picks):
    """Group picks by sport label in display order."""
    from scottys_edge import kelly_label
    groups = {}
    for p in picks:
        sp = p.get('sport', 'other')
        label = SPORT_LABELS.get(sp, sp.upper())
        if label not in groups:
            groups[label] = []
        groups[label].append(p)

    # Sort within each group by tier then units
    tier_order = {'MAX PLAY': 0, 'STRONG': 1, 'SOLID': 2, 'LEAN': 3, 'SPRINKLE': 4}
    for label in groups:
        groups[label].sort(key=lambda p: (tier_order.get(kelly_label(p['units']), 5), -p['units']))

    # Return ordered list of (label, picks) tuples
    ordered = []
    for sl in SPORT_ORDER:
        if sl in groups:
            ordered.append((sl, groups.pop(sl)))
    for sl, sp in groups.items():
        ordered.append((sl, sp))
    return ordered


def generate_card_image(picks, output_path=None, min_units=3.5, max_per_card=5):
    from scottys_edge import kelly_label
    fonts=_load_fonts()
    picks=[p for p in picks if p.get('units',0)>=min_units]
    if not picks:
        print("  No picks above minimum units threshold — generating no-edge card.")
        return _generate_no_picks_card(fonts, output_path)

    sport_groups = _group_picks_by_sport(picks)
    desktop=_get_desktop(); cards=[]

    # Build a flat ordered list with sport header markers
    ordered_picks = []
    for sport_label, sport_picks in sport_groups:
        ordered_picks.append(('__HEADER__', sport_label))
        for p in sport_picks:
            ordered_picks.append(('__PICK__', p))

    # Split into chunks respecting max_per_card (count only picks, not headers)
    chunks = []
    current_chunk = []
    pick_count = 0
    current_sport = None
    for kind, item in ordered_picks:
        if kind == '__HEADER__':
            current_sport = item
            current_chunk.append((kind, item))
        else:
            if pick_count >= max_per_card:
                chunks.append(current_chunk)
                current_chunk = []
                pick_count = 0
                # Re-add current sport header if we're mid-sport
                if current_sport:
                    current_chunk.append(('__HEADER__', current_sport + ' (CONT.)'))
            current_chunk.append((kind, item))
            pick_count += 1
    if current_chunk:
        chunks.append(current_chunk)

    if len(chunks) == 1:
        c = _render_picks_slide_grouped(chunks[0], fonts, "TODAY'S PLAYS", True)
        p = output_path or os.path.join(desktop, 'scottys_edge_card.png')
        c.save(p, 'PNG', quality=95); print(f"  \U0001f4f8 Picks card: {p}"); cards.append(p)
    else:
        for idx, chunk in enumerate(chunks, 1):
            label = f"TODAY'S PLAYS ({idx}/{len(chunks)})"
            show_units = (idx == 1)
            c = _render_picks_slide_grouped(chunk, fonts, label, show_units)
            p = os.path.join(desktop, f'scottys_edge_card_{idx}.png')
            c.save(p, 'PNG', quality=95)
            print(f"  \U0001f4f8 Card {idx}/{len(chunks)}: {p}")
            cards.append(p)

    return cards[0] if len(cards) == 1 else cards


# ═══════════════════════════════════════════════════════════════
# RESULTS CARDS
# ═══════════════════════════════════════════════════════════════

def _render_results_slide(bets, fonts, label, accent_color, daily_record, daily_pnl,
                          latest_date, total_wins, total_losses, total_pnl, total_roi, start_date,
                          show_verdict=True):
    n = len(bets)

    # Dynamic layout — distribute space across all sections to fill the card
    # Minimum sizes for each section
    min_summary_h = 100*S   # daily record banner
    min_row_h = 55*S        # per bet row
    min_record_h = 90*S     # running record footer
    min_fixed = (5+20+105+26+20+38+20+10+85)*S  # bar+pad+header+dividers+label+disclaimer
    min_total = min_fixed + min_summary_h + n*(min_row_h+10*S) + min_record_h

    # Extra space to distribute evenly, with caps
    extra = max(0, IG_H - min_total)
    summary_h = min(min_summary_h + int(extra * 0.25), 200*S)
    row_h = min(min_row_h + int(extra * 0.35) // max(n, 1), 130*S)
    record_h = min(min_record_h + int(extra * 0.30), 250*S)
    # Any leftover from caps becomes even spacing between sections
    used = min_fixed + summary_h + n*(row_h+10*S) + record_h
    section_pad = max(0, (IG_H - used) // 3)  # distribute across 3 gaps

    # Scale fonts based on row height (reference: 75*S)
    rscale = row_h / (75*S)
    sel_sz = max(17, min(26, int(22 * rscale)))
    pnl_sz = max(20, min(32, int(26 * rscale)))
    badge_sz = max(10, min(14, int(12 * rscale)))
    clv_sz = max(11, min(16, int(13 * rscale)))

    # Vertical offsets inside row, scaled
    badge_ry = int(8 * rscale) * S
    badge_rbot = int(26 * rscale) * S
    sel_ry = int(38 * rscale) * S
    pnl_ry = int(14 * rscale) * S
    clv_ry = int(46 * rscale) * S

    # Scale summary font based on how much space it got
    sum_scale = summary_h / (100*S)
    big_sz = max(56, min(80, int(56 * sum_scale)))
    med_sz = max(36, min(50, int(36 * sum_scale)))
    verdict_sz = max(34, min(46, int(34 * sum_scale)))

    total_h = max(IG_H, min_fixed + summary_h + n*(row_h+10*S) + record_h)
    img = Image.new('RGB', (CARD_WIDTH, total_h), BG)
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0,0),(CARD_WIDTH,5*S)], fill=accent_color)
    y = 20*S
    y = _draw_header(draw, fonts, y, subtitle="DAILY RESULTS")
    y += 8*S; _draw_divider(draw, y); y += 18*S + section_pad

    # Daily summary banner (dynamically sized, content vertically centered)
    # v17: Fixed layout — date above record, units below, verdict right-aligned
    big_f=_font(fonts,'bold',big_sz); med_f=_font(fonts,'bold',med_sz); lbl_f=_font(fonts,'regular',17)
    yw,yl = daily_record
    content_block = 95*S
    sum_top = max(0, (summary_h - content_block) // 2)
    # Date first (above the record)
    draw.text((PADDING+20*S,y+sum_top),latest_date,fill=WHITE_40,font=lbl_f)
    # Record below date
    draw.text((PADDING+20*S,y+sum_top+24*S),f"{yw}W-{yl}L",fill=WHITE,font=big_f)
    # P/L next to record
    pnl_color=GREEN if daily_pnl>=0 else RED
    draw.text((PADDING+380*S,y+sum_top+34*S),f"{daily_pnl:+.1f}u",fill=pnl_color,font=med_f)
    draw.text((PADDING+380*S,y+sum_top+76*S),"DAILY P/L",fill=WHITE_40,font=lbl_f)
    if show_verdict:
        if daily_pnl>=10: verdict,vc="HUGE DAY",GREEN
        elif daily_pnl>=0: verdict,vc="GREEN DAY",GREEN
        elif daily_pnl>=-5: verdict,vc="MINOR LOSS",YELLOW
        else: verdict,vc="TOUGH DAY",RED
        draw.text((PADDING+630*S,y+sum_top+34*S),verdict,fill=vc,font=_font(fonts,'bold',verdict_sz))
    y+=summary_h; _draw_divider(draw,y); y+=20*S + section_pad

    # Section label
    draw.text((PADDING,y),label,fill=accent_color,font=_font(fonts,'bold',18)); y+=38*S

    sel_f=_font(fonts,'bold',sel_sz); pnl_f=_font(fonts,'bold',pnl_sz)
    badge_f=_font(fonts,'bold',badge_sz); clv_f=_font(fonts,'regular',clv_sz)
    for b in bets:
        sel,sport,result,pnl,units,odds,clv,side_type,conf = b
        r_color=GREEN if result=='WIN' else RED if result=='LOSS' else WHITE_40
        draw.rectangle([(PADDING,y),(CARD_WIDTH-PADDING,y+row_h)],fill=CARD_BG)
        draw.rectangle([(PADDING,y),(PADDING+5*S,y+row_h)],fill=r_color)
        tx=PADDING+22*S
        sp_label=SPORT_LABELS.get(sport,''); sp_color=SPORT_BADGE_COLORS.get(sport,WHITE_40)
        if sp_label:
            tw=draw.textlength(sp_label,font=badge_f); bw=tw+14*S
            bg=(sp_color[0]//5,sp_color[1]//5,sp_color[2]//5)
            draw.rectangle([(tx,y+badge_ry),(tx+bw,y+badge_rbot)],fill=bg)
            draw.text((tx+7*S,y+badge_ry+2*S),sp_label,fill=sp_color,font=badge_f)
        sel_display=sel[:45]+"..." if len(sel)>48 else sel
        draw.text((tx,y+sel_ry),sel_display,fill=WHITE,font=sel_f)
        rx=CARD_WIDTH-PADDING-22*S
        ps=f"{pnl:+.1f}u"; pw=draw.textlength(ps,font=pnl_f)
        draw.text((rx-pw,y+pnl_ry),ps,fill=r_color,font=pnl_f)
        if clv is not None and side_type=='SPREAD':
            cs=f"CLV {clv:+.1f}"; cw=draw.textlength(cs,font=clv_f)
            cc=GREEN if clv>0 else RED if clv<0 else WHITE_40
            draw.text((rx-cw,y+clv_ry),cs,fill=cc,font=clv_f)
        y+=row_h+10*S
    _draw_divider(draw,y); y+=20*S + section_pad

    # Running record section — center content block within allocated record_h
    rec_scale = min(record_h / (90*S), 2.0)  # cap scale at 2x
    rec_title_sz = max(18, min(28, int(18 * rec_scale)))
    rec_num_sz = max(26, min(44, int(26 * rec_scale)))
    rec_sub_sz = max(15, min(22, int(15 * rec_scale)))
    # Content block height: title(40) + numbers(40) + subtitle(30) = ~110*S
    content_block_h = 132*S
    rec_top = max(0, (record_h - content_block_h) // 2)
    ry = y + rec_top
    draw.text((PADDING,ry),"RUNNING RECORD",fill=GREEN,font=_font(fonts,'bold',rec_title_sz))
    ry+=42*S
    rf=_font(fonts,'bold',rec_num_sz)
    draw.text((PADDING+20*S,ry),f"{total_wins}W-{total_losses}L",fill=WHITE,font=rf)
    twp=total_wins/(total_wins+total_losses)*100 if (total_wins+total_losses)>0 else 0
    draw.text((PADDING+260*S,ry),f"{twp:.1f}%",fill=GREEN if twp>52 else RED,font=rf)
    draw.text((PADDING+440*S,ry),f"{total_pnl:+.1f}u",fill=GREEN if total_pnl>=0 else RED,font=rf)
    draw.text((PADDING+620*S,ry),f"ROI {total_roi:+.1f}%",fill=GREEN if total_roi>=0 else RED,font=rf)
    ry+=56*S
    days=(datetime.now()-datetime.strptime(start_date,'%Y-%m-%d')).days
    draw.text((PADDING+20*S,ry),f"Since {start_date}  \u2022  {days} days  \u2022  {total_wins+total_losses} total picks",fill=WHITE_40,font=_font(fonts,'regular',rec_sub_sz))
    y+=record_h

    # Streak & last 10 section
    try:
        import sqlite3 as _sq
        _db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
        _conn = _sq.connect(_db)
        _recent = _conn.execute("""
            SELECT result, pnl_units, DATE(created_at) FROM graded_bets
            WHERE DATE(created_at) >= ? AND result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5
            ORDER BY created_at DESC
        """, (start_date,)).fetchall()
        _conn.close()

        if _recent:
            # Last 10 results
            last10 = _recent[:10]
            l10_w = sum(1 for r in last10 if r[0] == 'WIN')
            l10_l = sum(1 for r in last10 if r[0] == 'LOSS')
            l10_pnl = sum(r[1] or 0 for r in last10)

            # v17: Show "LAST 10" with dot visualization — no confusing streak counter
            _draw_divider(draw, y); y += 24*S
            streak_f = _font(fonts, 'bold', max(16, int(18 * rec_scale)))
            l10_f = _font(fonts, 'regular', max(14, int(16 * rec_scale)))

            draw.text((PADDING, y), "LAST 10", fill=GREEN, font=streak_f)
            l10_label_w = draw.textlength("LAST 10", font=streak_f)

            l10_x = PADDING + l10_label_w + 24*S
            l10_color = GREEN if l10_pnl >= 0 else RED
            draw.text((l10_x, y), f"{l10_w}W-{l10_l}L  {l10_pnl:+.1f}u", fill=l10_color, font=streak_f)

            # Dot visualization (oldest to newest, left to right)
            y += 40*S
            dx = PADDING + 20*S
            dot_size = 14*S
            dot_gap = 10*S
            for i, r in enumerate(reversed(last10)):
                color = GREEN if r[0] == 'WIN' else RED
                draw.ellipse([(dx, y), (dx + dot_size, y + dot_size)], fill=color)
                dx += dot_size + dot_gap
            y += dot_size + 20*S
    except Exception:
        pass

    _draw_divider(draw, y); y += 10*S
    y=_draw_disclaimer(draw,fonts,y)
    return _finalize(img, y)


def generate_results_card(conn=None, output_path=None, start_date='2026-03-04'):
    close_conn=False
    if conn is None: conn=sqlite3.connect(DB_PATH); close_conn=True
    fonts=_load_fonts()
    yesterday_bets=conn.execute("""
        SELECT selection, sport, result, pnl_units, units, odds, clv, side_type, confidence
        FROM graded_bets WHERE DATE(created_at) = (
            SELECT MAX(DATE(created_at)) FROM graded_bets WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5
        ) AND result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5 ORDER BY pnl_units DESC
    """).fetchall()
    if not yesterday_bets:
        print("  No graded bets found.")
        if close_conn: conn.close()
        return None
    latest_date=conn.execute("SELECT MAX(DATE(created_at)) FROM graded_bets WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5").fetchone()[0] or 'Unknown'
    all_bets=conn.execute("SELECT result, pnl_units, units FROM graded_bets WHERE DATE(created_at) >= ? AND result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5",(start_date,)).fetchall()
    if close_conn: conn.close()
    tw=sum(1 for b in all_bets if b[0]=='WIN'); tl=sum(1 for b in all_bets if b[0]=='LOSS')
    tp=sum(b[1] or 0 for b in all_bets); twg=sum(b[2] or 0 for b in all_bets)
    tr=(tp/twg*100) if twg>0 else 0
    yw=sum(1 for b in yesterday_bets if b[2]=='WIN'); yl=sum(1 for b in yesterday_bets if b[2]=='LOSS')
    yp=sum(b[3] or 0 for b in yesterday_bets)
    winners=[b for b in yesterday_bets if b[2]=='WIN']
    losers=[b for b in yesterday_bets if b[2] in ('LOSS','PUSH')]
    desktop=_get_desktop(); cards=[]
    if winners:
        s1=_render_results_slide(winners,fonts,f"WINNERS ({len(winners)})",GREEN,(yw,yl),yp,latest_date,tw,tl,tp,tr,start_date,True)
        p1=os.path.join(desktop,'scottys_edge_results_1_wins.png')
        s1.save(p1,'PNG',quality=95); print(f"  \U0001f4ca Winners: {p1}"); cards.append(p1)
    if losers:
        s2=_render_results_slide(losers,fonts,f"LOSSES ({len(losers)})",RED,(yw,yl),yp,latest_date,tw,tl,tp,tr,start_date,False)
        p2=os.path.join(desktop,'scottys_edge_results_2_losses.png')
        s2.save(p2,'PNG',quality=95); print(f"  \U0001f4ca Losses: {p2}"); cards.append(p2)
    if not cards:
        s=_render_results_slide(yesterday_bets,fonts,"RESULTS",GREEN if yp>=0 else RED,(yw,yl),yp,latest_date,tw,tl,tp,tr,start_date,True)
        p=output_path or os.path.join(desktop,'scottys_edge_results.png')
        s.save(p,'PNG',quality=95); print(f"  \U0001f4ca Results: {p}"); cards.append(p)
    return cards


# ═══════════════════════════════════════════════════════════════
# STATS CARD
# ═══════════════════════════════════════════════════════════════

def generate_stats_card(conn=None, output_path=None, start_date='2026-03-04'):
    close_conn=False
    if conn is None: conn=sqlite3.connect(DB_PATH); close_conn=True
    fonts=_load_fonts()
    bets=conn.execute("""
        SELECT sport, result, pnl_units, units, clv, side_type, market_type, confidence, timing
        FROM graded_bets WHERE DATE(created_at) >= ? AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
        AND units >= 3.5
        ORDER BY created_at
    """,(start_date,)).fetchall()
    if close_conn: conn.close()
    if not bets: print("  No graded bets found."); return None
    wins=sum(1 for b in bets if b[1]=='WIN'); losses=sum(1 for b in bets if b[1]=='LOSS')
    total_pnl=sum(b[2] or 0 for b in bets); total_wagered=sum(b[3] or 0 for b in bets)
    roi=(total_pnl/total_wagered*100) if total_wagered>0 else 0
    wp=wins/(wins+losses)*100 if (wins+losses)>0 else 0
    # v14: Only use SPREAD CLV (points). ML CLV uses implied probability which
    # swings 30%+ on tournament games — not comparable and misleading.
    clv_vals=[b[4] for b in bets if b[4] is not None and b[6]=='SPREAD']
    avg_clv=sum(clv_vals)/len(clv_vals) if clv_vals else 0
    pos_clv=sum(1 for c in clv_vals if c>0); clv_rate=pos_clv/len(clv_vals)*100 if clv_vals else 0
    side_data={}
    for b in bets:
        side=b[5] or 'UNKNOWN'
        if side not in side_data: side_data[side]={'W':0,'L':0,'pnl':0}
        if b[1]=='WIN': side_data[side]['W']+=1
        elif b[1]=='LOSS': side_data[side]['L']+=1
        side_data[side]['pnl']+=(b[2] or 0)
    tier_data={}
    for b in bets:
        conf=b[7] or 'UNKNOWN'
        if conf=='ELITE': tier='MAX PLAY'
        elif conf=='STRONG': tier='STRONG'
        else: continue  # v17: Skip SOLID/HIGH/LEAN — no longer active tiers
        if tier not in tier_data: tier_data[tier]={'W':0,'L':0,'pnl':0,'wager':0}
        if b[1]=='WIN': tier_data[tier]['W']+=1
        elif b[1]=='LOSS': tier_data[tier]['L']+=1
        tier_data[tier]['pnl']+=(b[2] or 0); tier_data[tier]['wager']+=(b[3] or 0)
    # v17: By Sport replaces By Timing (hourly runs make timing irrelevant)
    sport_data={}
    for b in bets:
        sp=b[0] or 'UNKNOWN'
        sp_label=SPORT_LABELS.get(sp, sp.replace('_',' ').title())
        if sp_label not in sport_data: sport_data[sp_label]={'W':0,'L':0,'pnl':0}
        if b[1]=='WIN': sport_data[sp_label]['W']+=1
        elif b[1]=='LOSS': sport_data[sp_label]['L']+=1
        sport_data[sp_label]['pnl']+=(b[2] or 0)

    total_h = max(IG_H, 1000*S)
    img=Image.new('RGB',(CARD_WIDTH,total_h),BG); draw=ImageDraw.Draw(img)
    draw.rectangle([(0,0),(CARD_WIDTH,5*S)],fill=GREEN)
    y=20*S; y=_draw_header(draw,fonts,y,subtitle="PERFORMANCE ANALYTICS")
    sf=_font(fonts,'regular',16)
    days_tracked=(datetime.now()-datetime.strptime(start_date,'%Y-%m-%d')).days
    draw.text((PADDING,y-8*S),f"ALL PICKS TRACKED & GRADED  \u2022  {days_tracked} DAYS  \u2022  SINCE {start_date}",fill=WHITE_40,font=sf)
    y+=18*S; _draw_divider(draw,y); y+=25*S

    # Hero stats row
    bf=_font(fonts,'bold',60); lf=_font(fonts,'regular',15); mf=_font(fonts,'bold',36)
    draw.text((PADDING+20*S,y),f"{wins}W-{losses}L",fill=WHITE,font=bf)
    draw.text((PADDING+20*S,y+66*S),"RECORD",fill=WHITE_40,font=lf)
    draw.text((PADDING+330*S,y+10*S),f"{wp:.1f}%",fill=GREEN if wp>52 else RED,font=mf)
    draw.text((PADDING+330*S,y+52*S),"WIN RATE",fill=WHITE_40,font=lf)
    draw.text((PADDING+530*S,y+10*S),f"{total_pnl:+.1f}u",fill=GREEN if total_pnl>=0 else RED,font=mf)
    draw.text((PADDING+530*S,y+52*S),"PROFIT / LOSS",fill=WHITE_40,font=lf)
    draw.text((PADDING+760*S,y+10*S),f"{roi:+.1f}%",fill=GREEN if roi>=0 else RED,font=mf)
    draw.text((PADDING+760*S,y+52*S),"ROI",fill=WHITE_40,font=lf)
    y+=85*S
    draw.text((PADDING+20*S,y),f"{total_wagered:.0f} units wagered  \u2022  {wins+losses} total picks",fill=WHITE_40,font=_font(fonts,'regular',16))
    y+=35*S; _draw_divider(draw,y); y+=25*S

    secf=_font(fonts,'bold',18); rowf=_font(fonts,'regular',19); numf=_font(fonts,'bold',19)
    row_spacing = 40*S

    # By Conviction — v17: Only MAX PLAY + STRONG (active tiers)
    draw.text((PADDING,y),"BY CONVICTION",fill=GREEN,font=secf); y+=38*S
    for tier in ['MAX PLAY','STRONG']:
        if tier not in tier_data: continue
        d=tier_data[tier]; t=d['W']+d['L']
        if t==0: continue
        t_wp=d['W']/t*100; t_roi=d['pnl']/d['wager']*100 if d['wager']>0 else 0
        draw.text((PADDING+20*S,y),tier,fill=TIER_COLORS.get(tier,WHITE_60),font=_font(fonts,'bold',19))
        draw.text((PADDING+240*S,y),f"{d['W']}W-{d['L']}L ({t_wp:.0f}%)",fill=WHITE,font=numf)
        draw.text((PADDING+490*S,y),f"{d['pnl']:+.1f}u",fill=GREEN if d['pnl']>=0 else RED,font=numf)
        draw.text((PADDING+650*S,y),f"ROI {t_roi:+.1f}%",fill=GREEN if t_roi>=0 else RED,font=rowf)
        y+=row_spacing
    y+=12*S; _draw_divider(draw,y); y+=25*S

    # By Bet Type — v17: More granular (side + market type)
    draw.text((PADDING,y),"BY BET TYPE",fill=GREEN,font=secf); y+=38*S
    # Side types
    for sk in ['DOG','FAVORITE','OVER','UNDER','PROP OVER']:
        if sk not in side_data: continue
        d=side_data[sk]; t=d['W']+d['L']
        if t==0: continue
        swp=d['W']/t*100
        label={'DOG':'Spread Dogs','FAVORITE':'Spread Favorites','OVER':'Totals Over',
               'UNDER':'Totals Under','PROP OVER':'Prop Overs'}.get(sk,sk)
        draw.text((PADDING+20*S,y),label,fill=WHITE_80,font=rowf)
        draw.text((PADDING+280*S,y),f"{d['W']}W-{d['L']}L ({swp:.0f}%)",fill=WHITE,font=numf)
        draw.text((PADDING+530*S,y),f"{d['pnl']:+.1f}u",fill=GREEN if d['pnl']>=0 else RED,font=numf)
        y+=row_spacing
    # Moneyline aggregate (not in side_data, compute from market_type)
    ml_bets = [b for b in bets if b[6] == 'MONEYLINE']
    if ml_bets:
        ml_w = sum(1 for b in ml_bets if b[1]=='WIN')
        ml_l = sum(1 for b in ml_bets if b[1]=='LOSS')
        ml_pnl = sum(b[2] or 0 for b in ml_bets)
        ml_t = ml_w + ml_l
        if ml_t > 0:
            draw.text((PADDING+20*S,y),"Moneylines",fill=WHITE_80,font=rowf)
            draw.text((PADDING+280*S,y),f"{ml_w}W-{ml_l}L ({ml_w/ml_t*100:.0f}%)",fill=WHITE,font=numf)
            draw.text((PADDING+530*S,y),f"{ml_pnl:+.1f}u",fill=GREEN if ml_pnl>=0 else RED,font=numf)
            y+=row_spacing
    y+=12*S; _draw_divider(draw,y); y+=25*S

    # By Sport — v17: Replaces By Timing (hourly runs make timing irrelevant)
    draw.text((PADDING,y),"BY SPORT",fill=GREEN,font=secf); y+=38*S
    # Sort by P&L descending
    sorted_sports = sorted(sport_data.items(), key=lambda x: x[1]['pnl'], reverse=True)
    for sp_label, d in sorted_sports:
        t=d['W']+d['L']
        if t==0: continue
        swp=d['W']/t*100
        draw.text((PADDING+20*S,y),sp_label,fill=WHITE_80,font=rowf)
        draw.text((PADDING+280*S,y),f"{d['W']}W-{d['L']}L ({swp:.0f}%)",fill=WHITE,font=numf)
        draw.text((PADDING+530*S,y),f"{d['pnl']:+.1f}u",fill=GREEN if d['pnl']>=0 else RED,font=numf)
        y+=row_spacing
    y+=12*S; _draw_divider(draw,y); y+=25*S

    # CLV Section
    draw.text((PADDING,y),"CLOSING LINE VALUE",fill=GREEN,font=secf); y+=14*S
    draw.text((PADDING,y+2*S),"Are we beating the market? CLV measures if our picks have real edge.",fill=WHITE_40,font=_font(fonts,'regular',14))
    y+=30*S
    draw.text((PADDING+20*S,y),f"Average CLV: {avg_clv:+.02f} pts",fill=GREEN if avg_clv>0 else RED,font=_font(fonts,'bold',22))
    y+=34*S
    draw.text((PADDING+20*S,y),f"Positive CLV rate: {pos_clv}/{len(clv_vals)} ({clv_rate:.0f}%)",fill=WHITE_60,font=_font(fonts,'regular',17))
    y+=26*S
    if avg_clv>0:
        draw.text((PADDING+20*S,y),"Consistently beating closing lines = real, sustainable edge",fill=GREEN,font=_font(fonts,'regular',16))
    y+=35*S; _draw_divider(draw,y); y+=10*S
    y=_draw_disclaimer(draw,fonts,y)
    final = _finalize(img, y)
    if output_path is None:
        output_path=os.path.join(_get_desktop(),'scottys_edge_stats.png')
    final.save(output_path,'PNG',quality=95)
    print(f"  \U0001f4ca Stats card saved: {output_path}")
    return output_path


# ═══════════════════════════════════════════════════════════════
# PICK WRITE-UPS (copy-paste social posts per pick)
# ═══════════════════════════════════════════════════════════════

def generate_pick_writeups(picks, min_units=3.5):
    """Generate a ready-to-post social write-up for each pick.
    Explains WHY the model likes the bet using context data."""
    picks = [p for p in picks if p.get('units', 0) >= min_units]
    if not picks:
        return ""

    SPORT_NAMES = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB', 'icehockey_nhl': 'NHL',
        'baseball_ncaa': 'College Baseball', 'soccer_epl': 'EPL', 'soccer_italy_serie_a': 'Serie A',
        'soccer_germany_bundesliga': 'Bundesliga', 'soccer_france_ligue_one': 'Ligue 1',
        'soccer_spain_la_liga': 'La Liga', 'soccer_usa_mls': 'MLS',
    }

    writeups = []
    for p in picks:
        sel = p.get('selection', '')
        sport = SPORT_NAMES.get(p.get('sport', ''), p.get('sport', ''))
        mtype = p.get('market_type', '')
        odds = p.get('odds', 0)
        edge = p.get('edge_pct', 0)
        ctx = p.get('context', '') or ''
        odds_str = f"({odds:+.0f})" if odds else ''

        # Parse context into bullet points
        ctx_parts = [c.strip() for c in ctx.split('|') if c.strip()] if ctx else []

        # Build the "why" narrative
        lines = []
        lines.append(f"\U0001f50d {sel} {odds_str}")
        lines.append(f"{sport} | {edge:.0f}% edge")
        lines.append("")

        if mtype == 'TOTAL':
            if 'OVER' in sel.upper():
                lines.append("Why OVER?")
            else:
                lines.append("Why UNDER?")
        elif mtype == 'SPREAD':
            lines.append("Why this side?")
        elif mtype == 'MONEYLINE':
            lines.append("Why this team?")

        if ctx_parts:
            for c in ctx_parts:
                # Clean up context for readability
                c = c.replace('(+', '(+').replace('(-', '(-')
                lines.append(f"\u2022 {c}")
        else:
            lines.append("\u2022 Model spread disagrees with the market")

        # Add edge context
        if edge >= 20:
            lines.append(f"\n\U0001f4ca Our model sees {edge:.0f}% edge over the market. That's a significant disagreement.")
        else:
            lines.append(f"\n\U0001f4ca {edge:.0f}% edge — the data supports this side.")

        lines.append("")
        lines.append("I'm always looking for that edge. \U0001f4aa")
        lines.append("")
        lines.append("\U0001f4f1 @scottys_edge | \U0001f426 @Scottys_edge")
        lines.append("#SportsBetting #BettingPicks #FreePicks")

        writeups.append('\n'.join(lines))

    return '\n\n' + ('=' * 50 + '\n\n').join(writeups)


# ═══════════════════════════════════════════════════════════════
# TWITTER THREADS (multi-tweet deep analysis per pick)
# ═══════════════════════════════════════════════════════════════

def generate_thread(picks, min_units=3.5):
    """Generate ready-to-post Twitter threads for each pick.
    Each thread is 3 tweets: hook, data/context, closer with record."""
    picks = [p for p in picks if p.get('units', 0) >= min_units]
    if not picks:
        return ""

    SPORT_NAMES = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB', 'icehockey_nhl': 'NHL',
        'baseball_ncaa': 'College Baseball', 'soccer_epl': 'EPL', 'soccer_italy_serie_a': 'Serie A',
        'soccer_germany_bundesliga': 'Bundesliga', 'soccer_france_ligue_one': 'Ligue 1',
        'soccer_spain_la_liga': 'La Liga', 'soccer_usa_mls': 'MLS',
        'soccer_uefa_champs_league': 'UCL',
    }

    # Pull season record
    try:
        conn = sqlite3.connect(DB_PATH)
        all_g = conn.execute(
            "SELECT result, pnl_units FROM graded_bets "
            "WHERE DATE(created_at) >= '2026-03-04' AND result NOT IN ('DUPLICATE','PENDING','TAINTED') "
            "AND units >= 3.5"
        ).fetchall()
        tw = sum(1 for r in all_g if r[0] == 'WIN')
        tl = sum(1 for r in all_g if r[0] == 'LOSS')
        tp = sum(r[1] or 0 for r in all_g)
        conn.close()
    except Exception:
        tw, tl, tp = 0, 0, 0.0

    win_pct = tw / (tw + tl) * 100 if (tw + tl) > 0 else 0

    threads = []
    for idx, p in enumerate(picks, 1):
        sel = p.get('selection', '')
        sport = SPORT_NAMES.get(p.get('sport', ''), p.get('sport', ''))
        mtype = p.get('market_type', '')
        odds = p.get('odds', 0)
        edge = p.get('edge_pct', 0)
        ctx = p.get('context', '') or ''
        line = p.get('line', '')
        home = p.get('home', '')
        away = p.get('away', '')
        odds_str = f"({odds:+.0f})" if odds else ''

        # === TWEET 1: Hook ===
        tweet1 = (
            f"\U0001f9f5 Why our model loves {sel} {odds_str}\n\n"
            f"{sport} | {edge:.0f}% edge | Data-backed analysis below \U0001f447"
        )
        # Trim if over 280
        if len(tweet1) > 280:
            tweet1 = (
                f"\U0001f9f5 {sel} {odds_str}\n\n"
                f"{sport} | {edge:.0f}% edge | Analysis below \U0001f447"
            )

        # === TWEET 2: The data ===
        tweet2_lines = []

        # Opening line based on market type
        if mtype == 'TOTAL':
            if 'OVER' in sel.upper():
                tweet2_lines.append(
                    f"The market has this total at {line}. Our model says it should be higher."
                    if line else "Our model says this total is set too low."
                )
            else:
                tweet2_lines.append(
                    f"The market has this total at {line}. Our model says it's too high."
                    if line else "Our model says this total is set too high."
                )
        elif mtype == 'SPREAD':
            team = sel.split('+')[0].split('-')[0].strip() if sel else sel
            tweet2_lines.append(
                f"The market has {team} at {line}. Our model says they're undervalued."
                if line else f"{team} is undervalued by the market."
            )
        elif mtype == 'MONEYLINE':
            tweet2_lines.append(
                f"{sel} at {odds_str} is mispriced. Our model sees {edge:.0f}% edge."
            )
        else:
            tweet2_lines.append(f"Our model sees {edge:.0f}% edge here.")

        tweet2_lines.append("\nHere's why:")

        # Parse context factors
        ctx_parts = [c.strip() for c in ctx.split('|') if c.strip()] if ctx else []
        for c in ctx_parts:
            cl = c.lower()
            if 'sunday' in cl or 'monday' in cl or 'tuesday' in cl or 'wednesday' in cl or 'thursday' in cl or 'friday' in cl or 'saturday' in cl:
                if 'allows' in cl or 'scores' in cl or 'r/' in cl:
                    bullet = f"\U0001f525 {c}"
                else:
                    bullet = f"\U0001f4c5 {c} \u2014 we track day-of-week data"
            elif 'slow-paced' in cl or 'slow pace' in cl:
                bullet = f"\U0001f40c {c}"
            elif 'fast-paced' in cl or 'fast pace' in cl:
                bullet = f"\u26a1 {c}"
            elif 'bounce-back' in cl or 'bounce back' in cl:
                bullet = f"\U0001f4c8 {c}"
            elif 'b2b' in cl or 'back-to-back' in cl:
                bullet = f"\U0001f634 {c} \u2014 fatigue is real"
            elif 'pitching' in cl or 'pitcher' in cl:
                bullet = f"\u26be {c}"
            elif 'division' in cl or 'familiarity' in cl:
                bullet = f"\U0001f504 {c} \u2014 these games trend tighter"
            elif 'altitude' in cl or 'denver' in cl or 'salt lake' in cl:
                bullet = f"\U0001f3d4\ufe0f {c} \u2014 altitude effect"
            elif 'derby' in cl or 'rivalry' in cl:
                bullet = f"\U0001f525 {c} \u2014 rivalry games are tighter"
            else:
                bullet = f"\u2022 {c}"
            tweet2_lines.append(bullet)

        if not ctx_parts:
            tweet2_lines.append("\u2022 Model spread disagrees with the market")

        tweet2 = '\n'.join(tweet2_lines)
        # Trim if over 280 — drop lines from the end until it fits
        while len(tweet2) > 280 and len(tweet2_lines) > 2:
            tweet2_lines.pop()
            tweet2 = '\n'.join(tweet2_lines)

        # === TWEET 3: The closer ===
        tweet3 = (
            f"Season record: {tw}W-{tl}L | {tp:+.1f}u | {win_pct:.0f}%\n"
            f"Every pick tracked. Every loss shown. \U0001f4ca\n\n"
            f"I'm always looking for that edge.\n\n"
            f"\U0001f4f1 @scottys_edge | \U0001f426 @Scottys_edge"
        )
        if len(tweet3) > 280:
            tweet3 = (
                f"{tw}W-{tl}L | {tp:+.1f}u | {win_pct:.0f}%\n"
                f"Every pick tracked. Every loss shown.\n\n"
                f"I'm always looking for that edge.\n\n"
                f"@scottys_edge | @Scottys_edge"
            )

        thread = f"THREAD {idx}:\n"
        thread += f"---TWEET 1---\n{tweet1}\n\n"
        thread += f"---TWEET 2---\n{tweet2}\n\n"
        thread += f"---TWEET 3---\n{tweet3}"
        threads.append(thread)

    return '\n\n' + ('\n\n' + '=' * 50 + '\n\n').join(threads)


# ═══════════════════════════════════════════════════════════════
# CAPTION
# ═══════════════════════════════════════════════════════════════

def generate_caption(picks, min_units=3.5):
    from scottys_edge import kelly_label
    picks=[p for p in picks if p.get('units',0)>=min_units]
    now=datetime.now(); day_str=now.strftime('%A'); date_str=now.strftime('%B %d')
    if not picks:
        return f"""Scotty's Edge \u2014 {day_str} {date_str}

No plays tonight. The model didn't find enough edge.

We only bet when the data says to bet. Sitting out is part of the strategy. \U0001f4ca

\u26a0\ufe0f Not gambling advice \u2022 21+ \u2022 1-800-GAMBLER

\U0001f4f1 @scottys_edge | \U0001f426 @Scottys_edge | \U0001f4ac discord.gg/JQ6rRfuN

#SportsBetting #BettingPicks #FreePicks #BettingCommunity"""
    sport_set=set()
    for p in picks:
        sp=p.get('sport','')
        if 'basketball' in sp: sport_set.add('basketball')
        elif 'hockey' in sp: sport_set.add('hockey')
        elif 'baseball' in sp: sport_set.add('baseball')
        elif 'soccer' in sp: sport_set.add('soccer')
    emoji_map={'basketball':'\U0001f3c0','hockey':'\U0001f3d2','baseball':'\u26be','soccer':'\u26bd'}
    sports_str=''.join(emoji_map.get(s,'') for s in sorted(sport_set))
    tu=sum(p['units'] for p in picks)
    pick_lines=[]
    for p in sorted(picks,key=lambda x:x['units'],reverse=True):
        kl=kelly_label(p['units'])
        tier='\U0001f525' if kl=='MAX PLAY' else '\u2b50' if kl=='STRONG' else '\u2705'
        odds_str=f"({p['odds']:+.0f})" if p['odds'] else ''
        pick_lines.append(f"{tier} {p['selection']} {odds_str}")
    # Top 5 hashtags — rotate slot 4-5 based on sports on card
    # Core 3: always use. Slots 4-5: sport-specific rotation.
    core_tags = ['#SportsBetting', '#BettingPicks', '#FreePicks']
    sport_rotation = {
        'basketball': '#NBABets',
        'hockey': '#NHLBets',
        'baseball': '#MLBBets',
        'soccer': '#SoccerBets',
    }
    # Pick the sport-specific tag for slot 4 (most common sport on card)
    sport_counts = {}
    for p in picks:
        sp = p.get('sport', '')
        if 'basketball' in sp: sport_counts['basketball'] = sport_counts.get('basketball', 0) + 1
        elif 'hockey' in sp: sport_counts['hockey'] = sport_counts.get('hockey', 0) + 1
        elif 'baseball' in sp: sport_counts['baseball'] = sport_counts.get('baseball', 0) + 1
        elif 'soccer' in sp: sport_counts['soccer'] = sport_counts.get('soccer', 0) + 1
    top_sport = max(sport_counts, key=sport_counts.get) if sport_counts else 'basketball'
    slot4 = sport_rotation.get(top_sport, '#NBABets')
    slot5 = '#BettingCommunity'
    # March Madness override
    if any('ncaab' in p.get('sport','') for p in picks):
        from datetime import datetime as _dt
        _m = _dt.now().month
        if _m == 3 or (_m == 4 and _dt.now().day <= 7):
            slot4 = '#MarchMadness'
    hashtags = ' '.join(core_tags + [slot4, slot5])
    return f"""{sports_str} Scotty's Edge \u2014 {day_str} {date_str}

{chr(10).join(pick_lines)}

{len(picks)} plays \u2022 {tu:.0f}u total
Every pick tracked & graded \U0001f4ca

1 unit = 1% of bankroll. Scale to your comfort.
\u26a0\ufe0f Not gambling advice \u2022 21+ \u2022 1-800-GAMBLER

\U0001f4f1 @scottys_edge | \U0001f426 @Scottys_edge | \U0001f4ac discord.gg/JQ6rRfuN

{hashtags}"""


# ═══════════════════════════════════════════════════════════════
# ENGAGEMENT COMMENTS — per-platform comments for Cowork to post
# on team pages and betting pages associated with each game
# ═══════════════════════════════════════════════════════════════

import json

# Team → social accounts mapping
TEAM_SOCIALS = {
    # NBA
    'Lakers': {'twitter': ['@Lakers'], 'ig': ['lakers'], 'reddit': ['r/lakers']},
    'Celtics': {'twitter': ['@celtics'], 'ig': ['celtics'], 'reddit': ['r/bostonceltics']},
    'Warriors': {'twitter': ['@warriors'], 'ig': ['warriors'], 'reddit': ['r/warriors']},
    'Nuggets': {'twitter': ['@nuggets'], 'ig': ['nuggets'], 'reddit': ['r/denvernuggets']},
    'Bucks': {'twitter': ['@Bucks'], 'ig': ['bucks'], 'reddit': ['r/MkeBucks']},
    'Knicks': {'twitter': ['@nyknicks'], 'ig': ['nyknicks'], 'reddit': ['r/NYKnicks']},
    'Sixers': {'twitter': ['@sixers'], 'ig': ['sixers'], 'reddit': ['r/sixers']},
    'Heat': {'twitter': ['@MiamiHEAT'], 'ig': ['miamiheat'], 'reddit': ['r/heat']},
    'Cavaliers': {'twitter': ['@cavs'], 'ig': ['cavs'], 'reddit': ['r/clevelandcavs']},
    'Mavericks': {'twitter': ['@dallasmavs'], 'ig': ['dallasmavs'], 'reddit': ['r/Mavericks']},
    'Suns': {'twitter': ['@Suns'], 'ig': ['suns'], 'reddit': ['r/suns']},
    'Timberwolves': {'twitter': ['@Timberwolves'], 'ig': ['timberwolves'], 'reddit': ['r/timberwolves']},
    'Grizzlies': {'twitter': ['@memgrizz'], 'ig': ['grizzlies'], 'reddit': ['r/memphisgrizzlies']},
    'Kings': {'twitter': ['@SacramentoKings'], 'ig': ['kings'], 'reddit': ['r/kings']},
    'Spurs': {'twitter': ['@spurs'], 'ig': ['spurs'], 'reddit': ['r/NBASpurs']},
    'Trail Blazers': {'twitter': ['@trailblazers'], 'ig': ['trailblazers'], 'reddit': ['r/ripcity']},
    'Rockets': {'twitter': ['@HoustonRockets'], 'ig': ['houstonrockets'], 'reddit': ['r/rockets']},
    'Clippers': {'twitter': ['@LAClippers'], 'ig': ['laclippers'], 'reddit': ['r/LAClippers']},
    'Pacers': {'twitter': ['@Pacers'], 'ig': ['pacers'], 'reddit': ['r/Pacers']},
    'Pistons': {'twitter': ['@DetroitPistons'], 'ig': ['pistons'], 'reddit': ['r/DetroitPistons']},
    'Nets': {'twitter': ['@BrooklynNets'], 'ig': ['brooklynnets'], 'reddit': ['r/GoNets']},
    'Raptors': {'twitter': ['@Raptors'], 'ig': ['raptors'], 'reddit': ['r/torontoraptors']},
    'Bulls': {'twitter': ['@chicagobulls'], 'ig': ['chicagobulls'], 'reddit': ['r/chicagobulls']},
    'Hawks': {'twitter': ['@ATLHawks'], 'ig': ['atlhawks'], 'reddit': ['r/AtlantaHawks']},
    'Magic': {'twitter': ['@OrlandoMagic'], 'ig': ['orlandobullets'], 'reddit': ['r/OrlandoMagic']},
    'Hornets': {'twitter': ['@hornets'], 'ig': ['hornets'], 'reddit': ['r/CharlotteHornets']},
    'Wizards': {'twitter': ['@WashWizards'], 'ig': ['washwizards'], 'reddit': ['r/washingtonwizards']},
    '76ers': {'twitter': ['@sixers'], 'ig': ['sixers'], 'reddit': ['r/sixers']},

    # NHL
    'Maple Leafs': {'twitter': ['@MapleLeafs'], 'ig': ['mapleleafs'], 'reddit': ['r/leafs']},
    'Avalanche': {'twitter': ['@Avalanche'], 'ig': ['coloradoavalanche'], 'reddit': ['r/ColoradoAvalanche']},
    'Rangers': {'twitter': ['@NYRangers'], 'ig': ['nyrangers'], 'reddit': ['r/rangers']},
    'Hurricanes': {'twitter': ['@NHLcanes'], 'ig': ['nhlcanes'], 'reddit': ['r/canes']},
    'Stars': {'twitter': ['@DallasStars'], 'ig': ['dallasstars'], 'reddit': ['r/dallasstars']},
    'Capitals': {'twitter': ['@Capitals'], 'ig': ['capitals'], 'reddit': ['r/caps']},
    'Bruins': {'twitter': ['@NHLBruins'], 'ig': ['nhlbruins'], 'reddit': ['r/BostonBruins']},
    'Panthers': {'twitter': ['@FlaPanthers'], 'ig': ['flapanthers'], 'reddit': ['r/FloridaPanthers']},
    'Leafs': {'twitter': ['@MapleLeafs'], 'ig': ['mapleleafs'], 'reddit': ['r/leafs']},
    'Oilers': {'twitter': ['@EdmontonOilers'], 'ig': ['edmontonoilers'], 'reddit': ['r/EdmontonOilers']},
    'Kings': {'twitter': ['@LAKings'], 'ig': ['lakings'], 'reddit': ['r/losangeleskings']},
    'Ducks': {'twitter': ['@AnaheimDucks'], 'ig': ['anaheimducks'], 'reddit': ['r/AnaheimDucks']},
    'Blues': {'twitter': ['@StLouisBlues'], 'ig': ['stlouisblues'], 'reddit': ['r/StLouisBlues']},
    'Predators': {'twitter': ['@PredsNHL'], 'ig': ['nashvillepredators'], 'reddit': ['r/Predators']},
    'Golden Knights': {'twitter': ['@GoldenKnights'], 'ig': ['goldenknights'], 'reddit': ['r/goldenknights']},
    'Flames': {'twitter': ['@NHLFlames'], 'ig': ['nhlflames'], 'reddit': ['r/CalgaryFlames']},
    'Canucks': {'twitter': ['@Canucks'], 'ig': ['canucks'], 'reddit': ['r/canucks']},
    'Penguins': {'twitter': ['@penguins'], 'ig': ['pittsburghpenguins'], 'reddit': ['r/penguins']},
    'Jets': {'twitter': ['@NHLJets'], 'ig': ['NHLJets'], 'reddit': ['r/winnipegjets']},
    'Red Wings': {'twitter': ['@DetroitRedWings'], 'ig': ['detroitredwings'], 'reddit': ['r/DetroitRedWings']},
    'Islanders': {'twitter': ['@NYIslanders'], 'ig': ['nyislanders'], 'reddit': ['r/NewYorkIslanders']},
    'Sabres': {'twitter': ['@BuffaloSabres'], 'ig': ['buffalosabres'], 'reddit': ['r/sabres']},
    'Senators': {'twitter': ['@Senators'], 'ig': ['ottawasenators'], 'reddit': ['r/ottawaSenators']},
    'Devils': {'twitter': ['@NJDevils'], 'ig': ['njdevils'], 'reddit': ['r/devils']},
    'Flyers': {'twitter': ['@NHLFlyers'], 'ig': ['nhlflyers'], 'reddit': ['r/Flyers']},
    'Lightning': {'twitter': ['@TBLightning'], 'ig': ['tblightning'], 'reddit': ['r/TampaBayLightning']},
    'Maple Leafs': {'twitter': ['@MapleLeafs'], 'ig': ['mapleleafs'], 'reddit': ['r/leafs']},
    'Wild': {'twitter': ['@mnwild'], 'ig': ['mnwild'], 'reddit': ['r/wildhockey']},
    'Sharks': {'twitter': ['@SanJoseSharks'], 'ig': ['sjsharks'], 'reddit': ['r/SanJoseSharks']},
    'Blackhawks': {'twitter': ['@NHLBlackhawks'], 'ig': ['nhlblackhawks'], 'reddit': ['r/hawks']},

    # MLB
    'Yankees': {'twitter': ['@Yankees'], 'ig': ['yankees'], 'reddit': ['r/NYYankees']},
    'Red Sox': {'twitter': ['@RedSox'], 'ig': ['redsox'], 'reddit': ['r/redsox']},
    'Dodgers': {'twitter': ['@Dodgers'], 'ig': ['dodgers'], 'reddit': ['r/Dodgers']},
    'Giants': {'twitter': ['@SFGiants'], 'ig': ['sfgiants'], 'reddit': ['r/SFGiants']},
    'Astros': {'twitter': ['@astros'], 'ig': ['astros'], 'reddit': ['r/Astros']},
    'Nationals': {'twitter': ['@Nationals'], 'ig': ['nationals'], 'reddit': ['r/Nationals']},
    'Mets': {'twitter': ['@Mets'], 'ig': ['mets'], 'reddit': ['r/NewYorkMets']},
    'Phillies': {'twitter': ['@Phillies'], 'ig': ['phillies'], 'reddit': ['r/phillies']},
    'Orioles': {'twitter': ['@Orioles'], 'ig': ['orioles'], 'reddit': ['r/orioles']},
    'Blue Jays': {'twitter': ['@BlueJays'], 'ig': ['bluejays'], 'reddit': ['r/Torontobluejays']},
    'Rays': {'twitter': ['@RaysBaseball'], 'ig': ['raysbaseball'], 'reddit': ['r/TampaBayRays']},
    'Tigers': {'twitter': ['@tigers'], 'ig': ['tigers'], 'reddit': ['r/motorcitykitties']},
    'White Sox': {'twitter': ['@whitesox'], 'ig': ['whitesox'], 'reddit': ['r/whitesox']},
    'Indians': {'twitter': ['@CleGuardians'], 'ig': ['clevelandindians'], 'reddit': ['r/ClevelandGuardians']},
    'Guardians': {'twitter': ['@CleGuardians'], 'ig': ['clevelandguardians'], 'reddit': ['r/ClevelandGuardians']},
    'Twins': {'twitter': ['@Twins'], 'ig': ['twins'], 'reddit': ['r/minnesotatwins']},
    'Royals': {'twitter': ['@Royals'], 'ig': ['royals'], 'reddit': ['r/KCRoyals']},
    'Athletics': {'twitter': ['@Athletics'], 'ig': ['athletics'], 'reddit': ['r/OaklandAthletics']},
    'Mariners': {'twitter': ['@Mariners'], 'ig': ['mariners'], 'reddit': ['r/Mariners']},
    'Rangers': {'twitter': ['@Rangers'], 'ig': ['texasrangers'], 'reddit': ['r/TexasRangers']},
    'Angels': {'twitter': ['@Angels'], 'ig': ['angels'], 'reddit': ['r/angelsbaseball']},
    'Padres': {'twitter': ['@Padres'], 'ig': ['padres'], 'reddit': ['r/Padres']},
    'Rockies': {'twitter': ['@Rockies'], 'ig': ['rockies'], 'reddit': ['r/ColoradoRockies']},
    'Brewers': {'twitter': ['@Brewers'], 'ig': ['brewers'], 'reddit': ['r/Brewers']},
    'Cardinals': {'twitter': ['@Cardinals'], 'ig': ['stlouiscardinals'], 'reddit': ['r/cardinals']},
    'Pirates': {'twitter': ['@Pirates'], 'ig': ['pirates'], 'reddit': ['r/Buccos']},
    'Reds': {'twitter': ['@Reds'], 'ig': ['reds'], 'reddit': ['r/Reds']},
    'Cubs': {'twitter': ['@Cubs'], 'ig': ['cubs'], 'reddit': ['r/CHCubs']},
    'Braves': {'twitter': ['@Braves'], 'ig': ['braves'], 'reddit': ['r/Braves']},
    'Marlins': {'twitter': ['@Marlins'], 'ig': ['marlins'], 'reddit': ['r/letsgofish']},

    # EPL
    'Liverpool': {'twitter': ['@LFC'], 'ig': ['liverpoolfc'], 'reddit': ['r/LiverpoolFC']},
    'Manchester City': {'twitter': ['@ManCity'], 'ig': ['mancity'], 'reddit': ['r/MCFC']},
    'Manchester United': {'twitter': ['@ManUtd'], 'ig': ['manchesterunited'], 'reddit': ['r/reddevils']},
    'Chelsea': {'twitter': ['@ChelseaFC'], 'ig': ['chelseafc'], 'reddit': ['r/chelseafc']},
    'Arsenal': {'twitter': ['@Arsenal'], 'ig': ['arsenal'], 'reddit': ['r/Gunners']},
    'Tottenham': {'twitter': ['@SpursOfficial'], 'ig': ['tottenhamhotspur'], 'reddit': ['r/coys']},
    'Brighton': {'twitter': ['@OfficialBHAFC'], 'ig': ['brightonandhovealbion'], 'reddit': ['r/BrightonHoveAlbion']},
    'Aston Villa': {'twitter': ['@AVFCOfficial'], 'ig': ['astonvilla'], 'reddit': ['r/avfc']},
    'Fulham': {'twitter': ['@FulhamFC'], 'ig': ['fulhamfc'], 'reddit': ['r/Fulham']},
    'West Ham': {'twitter': ['@WestHamUtd'], 'ig': ['westhamunited'], 'reddit': ['r/Hammers']},
    'Everton': {'twitter': ['@Everton'], 'ig': ['everton'], 'reddit': ['r/Everton']},
    'Leicester': {'twitter': ['@LCFC'], 'ig': ['leicestercityofficial'], 'reddit': ['r/lcfc']},
    'Leeds': {'twitter': ['@LUFC'], 'ig': ['leedsunited'], 'reddit': ['r/LeedsUnited']},
    'Southampton': {'twitter': ['@SouthamptonFC'], 'ig': ['southamptonfc'], 'reddit': ['r/SaintsFC']},
    'Wolves': {'twitter': ['@Wolves'], 'ig': ['officiallwfc'], 'reddit': ['r/Wolves']},
    'Newcastle': {'twitter': ['@NUFC'], 'ig': ['newcastleunited'], 'reddit': ['r/NUFC']},
    'Crystal Palace': {'twitter': ['@CPFC'], 'ig': ['crystalpalaceofficial'], 'reddit': ['r/crystalpalace']},
    'Nottingham Forest': {'twitter': ['@NFFC'], 'ig': ['nottinghamforest'], 'reddit': ['r/nffc']},
    'Luton': {'twitter': ['@LutonTown'], 'ig': ['lutontown'], 'reddit': ['r/Lutontown']},
    'Brentford': {'twitter': ['@BrentfordFC'], 'ig': ['brentfordfc'], 'reddit': ['r/Brentford']},
    'Ipswich': {'twitter': ['@IpswichTown'], 'ig': ['ipswichtownfc'], 'reddit': ['r/IpswichTown']},

    # La Liga
    'Real Madrid': {'twitter': ['@realmadrid'], 'ig': ['realmadrid'], 'reddit': ['r/realmadrid']},
    'Barcelona': {'twitter': ['@FCBarcelona'], 'ig': ['fcbarcelona'], 'reddit': ['r/Barca']},
    'Atletico Madrid': {'twitter': ['@Atleti'], 'ig': ['atleticomadrid'], 'reddit': ['r/atletico']},
    'Atlético Madrid': {'twitter': ['@Atleti'], 'ig': ['atleticomadrid'], 'reddit': ['r/atletico']},
    'Valencia': {'twitter': ['@VCF'], 'ig': ['valenciacf'], 'reddit': ['r/Valencia']},
    'Sevilla': {'twitter': ['@SevillaFC'], 'ig': ['sevillafc'], 'reddit': ['r/SevillaFC']},
    'Villarreal': {'twitter': ['@VillarrealCF'], 'ig': ['villarrealcf'], 'reddit': ['r/Villarreal']},
    'Betis': {'twitter': ['@RealBetis'], 'ig': ['realbetis'], 'reddit': ['r/Betis']},
    'Sociedad': {'twitter': ['@RealSociedad'], 'ig': ['realsociedad'], 'reddit': ['r/realsociedad']},
    'Athletic Bilbao': {'twitter': ['@AthleticClub'], 'ig': ['athleticclub'], 'reddit': ['r/AthleticClub']},
    'Girona': {'twitter': ['@GironaFC'], 'ig': ['gironacf'], 'reddit': ['r/GironaFC']},

    # Serie A
    'Juventus': {'twitter': ['@juventusfc'], 'ig': ['juventus'], 'reddit': ['r/Juve']},
    'Inter': {'twitter': ['@Inter'], 'ig': ['inter'], 'reddit': ['r/InterMilan']},
    'AC Milan': {'twitter': ['@acmilan'], 'ig': ['acmilan'], 'reddit': ['r/ACMilan']},
    'AS Roma': {'twitter': ['@ASRomaEN'], 'ig': ['asromaofficial'], 'reddit': ['r/ASRoma']},
    'Napoli': {'twitter': ['@sscnapoli'], 'ig': ['sscnapoli'], 'reddit': ['r/SSCNapoli']},
    'Lazio': {'twitter': ['@OfficialSSLazio'], 'ig': ['officialsslazio'], 'reddit': ['r/Lazio']},
    'Atalanta': {'twitter': ['@Atalanta_BC'], 'ig': ['atalantabcofficial'], 'reddit': ['r/Atalanta_BC']},
    'Fiorentina': {'twitter': ['@acffiorentina'], 'ig': ['acffiorentina'], 'reddit': ['r/Fiorentina']},
    'Torino': {'twitter': ['@TorinoFC_1906'], 'ig': ['torinofc_1906'], 'reddit': ['r/Torino']},
    'Bologna': {'twitter': ['@BolognaFC1909'], 'ig': ['bolognafc1909'], 'reddit': ['r/BolognaFC']},

    # Bundesliga
    'Bayern Munich': {'twitter': ['@FCBayern'], 'ig': ['fcbayern'], 'reddit': ['r/fcbayern']},
    'Borussia Dortmund': {'twitter': ['@BlackYellow'], 'ig': ['bvb'], 'reddit': ['r/borussiadortmund']},
    'RB Leipzig': {'twitter': ['@RBLeipzig'], 'ig': ['rbleipzig'], 'reddit': ['r/RBLeipzig']},
    'Bayer Leverkusen': {'twitter': ['@bayer04'], 'ig': ['bayer04leverkusen'], 'reddit': ['r/Bayer04']},
    'Schalke 04': {'twitter': ['@S04'], 'ig': ['schalke04'], 'reddit': ['r/schalke04']},
    'Hoffenheim': {'twitter': ['@TSGHoffenheim'], 'ig': ['tsghoffenheim'], 'reddit': ['r/hoffenheim']},

    # Ligue 1
    'Paris Saint-Germain': {'twitter': ['@PSG_English'], 'ig': ['psg'], 'reddit': ['r/psg']},
    'Marseille': {'twitter': ['@OM_Officiel'], 'ig': ['olympiquemarseille'], 'reddit': ['r/olympiquemarseille']},
    'Monaco': {'twitter': ['@AS_Monaco'], 'ig': ['asmmonaco'], 'reddit': ['r/Monaco']},
    'Lyonnais': {'twitter': ['@OL'], 'ig': ['ol'], 'reddit': ['r/Lyonnais']},

    # MLS
    'LA Galaxy': {'twitter': ['@LAGalaxy'], 'ig': ['lagalaxy'], 'reddit': ['r/LAGalaxy']},
    'New York Red Bulls': {'twitter': ['@NewYorkRedBulls'], 'ig': ['newyorkredbulls'], 'reddit': ['r/NYRB']},
    'Seattle Sounders': {'twitter': ['@SoundersFC'], 'ig': ['soundersfc'], 'reddit': ['r/SoundersFC']},
    'LAFC': {'twitter': ['@LAFC'], 'ig': ['lafc'], 'reddit': ['r/LAFC']},
    'Portland Timbers': {'twitter': ['@TimbersFC'], 'ig': ['portlandtimbers'], 'reddit': ['r/timbers']},
    'San Jose Earthquakes': {'twitter': ['@SJEarthquakes'], 'ig': ['sjquakes'], 'reddit': ['r/SJEarthquakes']},
    'Vancouver Whitecaps': {'twitter': ['@WhitecapsFC'], 'ig': ['vancouverwhitecapsfc'], 'reddit': ['r/whitecapsfc']},
    'Toronto FC': {'twitter': ['@TorontoFC'], 'ig': ['torontofc'], 'reddit': ['r/TFC']},
    'NYCFC': {'twitter': ['@NYCFC'], 'ig': ['nycfc'], 'reddit': ['r/NYCFC']},
    'Inter Miami': {'twitter': ['@InterMiamiCF'], 'ig': ['intermiamiofficial'], 'reddit': ['r/InterMiami']},
    'Columbus Crew': {'twitter': ['@ColumbusCrewSC'], 'ig': ['columbuscrewsc'], 'reddit': ['r/CrewSC']},
    'Houston Dynamo': {'twitter': ['@HoustonDynamo'], 'ig': ['houstondynamo'], 'reddit': ['r/dynamo']},
    'FC Dallas': {'twitter': ['@FCDallas'], 'ig': ['fcdallas'], 'reddit': ['r/fcdallas']},
    'Minnesota United': {'twitter': ['@MNUFC'], 'ig': ['minnesotaunited'], 'reddit': ['r/Minnesota_United']},
    'Sporting KC': {'twitter': ['@SportingKC'], 'ig': ['sportingkc'], 'reddit': ['r/SportingKC']},
    'Real Salt Lake': {'twitter': ['@RealSaltLake'], 'ig': ['realsaltlake'], 'reddit': ['r/RealsaltLake']},
    'Colorado Rapids': {'twitter': ['@ColoradoRapids'], 'ig': ['coloradorapids'], 'reddit': ['r/Rapids']},
    'Chicago Fire': {'twitter': ['@ChicagoFire'], 'ig': ['chicagofire'], 'reddit': ['r/chicagofire']},

    # Champions League
    'Liverpool': {'twitter': ['@LFC'], 'ig': ['liverpoolfc'], 'reddit': ['r/LiverpoolFC']},
    'Real Madrid': {'twitter': ['@realmadrid'], 'ig': ['realmadrid'], 'reddit': ['r/realmadrid']},
    'Bayern Munich': {'twitter': ['@FCBayern'], 'ig': ['fcbayern'], 'reddit': ['r/fcbayern']},
    'Barcelona': {'twitter': ['@FCBarcelona'], 'ig': ['fcbarcelona'], 'reddit': ['r/Barca']},
    'Manchester United': {'twitter': ['@ManUtd'], 'ig': ['manchesterunited'], 'reddit': ['r/reddevils']},
    'Manchester City': {'twitter': ['@ManCity'], 'ig': ['mancity'], 'reddit': ['r/MCFC']},
    'Paris Saint-Germain': {'twitter': ['@PSG_English'], 'ig': ['psg'], 'reddit': ['r/psg']},
    'Chelsea': {'twitter': ['@ChelseaFC'], 'ig': ['chelseafc'], 'reddit': ['r/chelseafc']},
    'Ajax': {'twitter': ['@AFCAjax'], 'ig': ['afcajax'], 'reddit': ['r/Ajax']},
    'Juventus': {'twitter': ['@juventusfc'], 'ig': ['juventus'], 'reddit': ['r/Juve']},

    # NCAA Basketball
    'Duke': {'twitter': ['@DukeBlueDevils'], 'ig': ['dukebluedevils'], 'reddit': ['r/Duke']},
    'North Carolina': {'twitter': ['@UNC_Basketball'], 'ig': ['uncbasketball'], 'reddit': ['r/UNC']},
    'Kentucky': {'twitter': ['@KentuckyMBB'], 'ig': ['officialkentuckymbb'], 'reddit': ['r/BBN']},
    'Kansas': {'twitter': ['@KUHoops'], 'ig': ['kuhoops'], 'reddit': ['r/jayhawks']},
    'Indiana': {'twitter': ['@iuhoops'], 'ig': ['iuhoops'], 'reddit': ['r/IndianaBBall']},
    'Ohio State': {'twitter': ['@OhioStateHoops'], 'ig': ['ohiostatebuckeyes'], 'reddit': ['r/OSUBuckeyes']},
    'Michigan': {'twitter': ['@umichhoops'], 'ig': ['umichhoops'], 'reddit': ['r/Wolverines']},
}

def get_kelly_label(units: float) -> str:
    """Convert units to Kelly tier label."""
    if units >= 4.5:
        return 'MAX PLAY'
    elif units >= 3.5:
        return 'STRONG'
    elif units >= 2.5:
        return 'SOLID'
    elif units >= 1.5:
        return 'LEAN'
    else:
        return 'SPRINKLE'

def get_season_stats() -> dict:
    """Query season record from graded_bets table."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT
                        SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                        SUM(CASE WHEN result IN ('WIN','LOSS') THEN pnl_units ELSE 0 END) as total_profit
                     FROM graded_bets''')
        row = c.fetchone()
        conn.close()

        wins = row[0] or 0
        losses = row[1] or 0
        profit = row[2] or 0
        total = wins + losses

        win_rate = (wins / total * 100) if total > 0 else 0

        return {
            'wins': wins,
            'losses': losses,
            'win_rate': round(win_rate, 1),
            'profit': round(profit, 1)
        }
    except Exception as e:
        print(f"Error querying season stats: {e}")
        return {'wins': 0, 'losses': 0, 'win_rate': 0, 'profit': 0}

def _parse_teams_from_selection(selection: str) -> tuple:
    """Parse away/home teams and pick detail from selection string.

    Formats:
      'Washington Capitals@Toronto Maple Leafs OVER 6.5' -> ('Washington Capitals', 'Toronto Maple Leafs', 'OVER 6.5')
      'Toronto Maple Leafs -1.5' -> ('', 'Toronto Maple Leafs', '-1.5')
      'Atlético Madrid@Barcelona OVER 3.5' -> ('Atlético Madrid', 'Barcelona', 'OVER 3.5')
    """
    if not selection:
        return ('', '', '')

    # Check for OVER/UNDER totals
    for marker in [' OVER ', ' UNDER ']:
        if marker in selection:
            teams_part = selection[:selection.index(marker)]
            detail = selection[selection.index(marker)+1:]
            if '@' in teams_part:
                away, home = teams_part.split('@', 1)
                return (away.strip(), home.strip(), detail.strip())
            return ('', teams_part.strip(), detail.strip())

    # Check for spread (e.g., "Team -1.5" or "Team +3.0")
    import re
    spread_match = re.search(r'\s([+-]\d+\.?\d*)$', selection)
    if spread_match:
        teams_part = selection[:spread_match.start()]
        detail = spread_match.group(1)
        if '@' in teams_part:
            away, home = teams_part.split('@', 1)
            return (away.strip(), home.strip(), detail.strip())
        return ('', teams_part.strip(), detail.strip())

    # ML or unknown — just look for @
    if '@' in selection:
        away, home = selection.split('@', 1)
        return (away.strip(), home.strip(), 'ML')

    return ('', selection.strip(), 'ML')


def extract_game_name(pick: dict) -> str:
    """Extract game name from pick data."""
    if 'game' in pick:
        return pick['game']
    home = pick.get('home_team', '')
    away = pick.get('away_team', '')
    if home and away:
        return f"{away} vs {home}"
    # Parse from selection
    away_parsed, home_parsed, _ = _parse_teams_from_selection(pick.get('selection', ''))
    if away_parsed and home_parsed:
        return f"{away_parsed} vs {home_parsed}"
    return pick.get('selection', 'Game')

def extract_sport_label(sport: str) -> str:
    """Map sport code to label."""
    labels = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB', 'icehockey_nhl': 'NHL',
        'baseball_mlb': 'MLB', 'baseball_ncaa': 'Baseball', 'soccer_epl': 'EPL',
        'soccer_germany_bundesliga': 'Bundesliga', 'soccer_france_ligue_one': 'Ligue 1',
        'soccer_italy_serie_a': 'Serie A', 'soccer_spain_la_liga': 'La Liga',
        'soccer_usa_mls': 'MLS', 'soccer_mexico_ligamx': 'Liga MX', 'soccer_uefa_champs_league': 'UCL'
    }
    return labels.get(sport, sport)

def get_team_accounts(team_name: str, platform: str) -> list:
    """Get social accounts for a team on a given platform."""
    if team_name in TEAM_SOCIALS:
        return TEAM_SOCIALS[team_name].get(platform, [])
    return []

def generate_engagement_comments(picks: list) -> list:
    """
    Generate platform-specific engagement comments for each pick.

    Args:
        picks: List of pick dicts with keys: selection, sport, market_type, odds, units,
               edge_pct, context_factors, home_team, away_team

    Returns:
        List of comment dicts with platform, target, target_type, comment, game, pick, sport
    """
    stats = get_season_stats()
    W = stats['wins']
    L = stats['losses']
    WR = stats['win_rate']
    profit = stats['profit']

    comments = []

    # Betting media targets (Twitter removed — account suspended April 2026)
    betting_targets = {
        'ig': [
            {'handle': 'actionnetworkhq', 'name': 'Action Network'},
            {'handle': 'barstoolsports', 'name': 'Barstool Sports'}
        ],
        'reddit': [
            {'subreddit': 'r/sportsbetting', 'name': 'r/sportsbetting'},
            {'subreddit': 'r/sportsbook', 'name': 'r/sportsbook'}
        ]
    }

    # Player → team mapping for prop bets
    # Maps player last names to their team's key in TEAM_SOCIALS
    PLAYER_TEAMS = {
        # NBA
        'Alexander-Walker': 'Hawks', 'Edwards': 'Timberwolves', 'Gobert': 'Timberwolves',
        'Jokic': 'Nuggets', 'Murray': 'Nuggets', 'Tatum': 'Celtics', 'Brown': 'Celtics',
        'Curry': 'Warriors', 'Doncic': 'Mavericks', 'Irving': 'Mavericks',
        'LeBron': 'Lakers', 'James': 'Lakers', 'Davis': 'Lakers',
        'Antetokounmpo': 'Bucks', 'Embiid': '76ers', 'Maxey': '76ers',
        'Brunson': 'Knicks', 'Hart': 'Knicks', 'Towns': 'Knicks',
        'Butler': 'Heat', 'Morant': 'Grizzlies', 'Mitchell': 'Cavaliers',
        'Booker': 'Suns', 'Durant': 'Suns', 'Fox': 'Kings',
        'Haliburton': 'Pacers', 'Wembanyama': 'Spurs', 'SGA': 'Thunder',
        'Gilgeous-Alexander': 'Thunder', 'LaVine': 'Bulls', 'DeRozan': 'Kings',
        'Young': 'Hawks', 'Lillard': 'Bucks', 'George': '76ers',
        # MLB — pitchers and hitters
        'Lorenzen': 'Rockies', 'Gorman': 'Cardinals', 'Abrams': 'Nationals',
        'Ohtani': 'Dodgers', 'Judge': 'Yankees', 'Soto': 'Mets',
        'Acuna': 'Braves', 'Tatis': 'Padres', 'Betts': 'Dodgers',
        'Freeman': 'Dodgers', 'Trout': 'Angels', 'Harper': 'Phillies',
        'Lindor': 'Mets', 'Turner': 'Mariners', 'Machado': 'Padres',
        'Alvarez': 'Astros', 'Tucker': 'Astros', 'Arenado': 'Cardinals',
        'Goldschmidt': 'Cardinals', 'Devers': 'Red Sox', 'Ramirez': 'Guardians',
        'Stanton': 'Yankees', 'Cole': 'Yankees', 'deGrom': 'Rangers',
        'Verlander': 'Mets', 'Scherzer': 'Rangers', 'Wheeler': 'Phillies',
        'Bieber': 'Guardians', 'Alcantara': 'Marlins', 'Musgrove': 'Padres',
        # NHL
        'McDavid': 'Oilers', 'Draisaitl': 'Oilers', 'MacKinnon': 'Avalanche',
        'Matthews': 'Maple Leafs', 'Marner': 'Maple Leafs', 'Ovechkin': 'Capitals',
        'Kucherov': 'Lightning', 'Pastrnak': 'Bruins', 'Bedard': 'Blackhawks',
    }

    def _find_player_team(selection: str) -> str:
        """Find team for a prop bet player from selection string like 'Nickeil Alexander-Walker OVER 3.5 THREES'."""
        # Try matching player last name against PLAYER_TEAMS
        parts = selection.split()
        for i in range(len(parts)):
            if parts[i].upper() in ('OVER', 'UNDER'):
                # Everything before OVER/UNDER is the player name
                player_parts = parts[:i]
                # Try last name, then hyphenated name, then full multi-word
                for j in range(len(player_parts)):
                    candidate = ' '.join(player_parts[j:])
                    if candidate in PLAYER_TEAMS:
                        return PLAYER_TEAMS[candidate]
                # Try just the last word
                if player_parts and player_parts[-1] in PLAYER_TEAMS:
                    return PLAYER_TEAMS[player_parts[-1]]
                break
        return ''

    for pick in picks:
        market_type = pick.get('market_type', 'ML')
        selection = pick.get('selection', '')
        sport = pick.get('sport', '')
        odds = pick.get('odds', 0)
        units = pick.get('units', 0)
        edge = pick.get('edge_pct', 0)
        raw_context = pick.get('context_factors', '') or ''
        # Clean up context — strip [SHADOW] prefix and pick the first factor
        context = raw_context.replace('[SHADOW] ', '').split('|')[0].strip() if raw_context else 'Strong model edge'
        if not context:
            context = 'Strong model edge'

        is_prop = (market_type == 'PROP')
        sport_label = extract_sport_label(sport)
        kelly_tier = get_kelly_label(units)

        if is_prop:
            # Props: selection = "Player Name OVER X.X STAT"
            pick_str = selection  # Full selection is the pick string
            player_team = _find_player_team(selection)
            game_name = selection  # Use full selection as game context

            # Odds formatting
            odds_str = f"{odds:+.0f}" if odds else "EV"

            # Target the player's team accounts only (+ betting media)
            teams_to_target = []
            if player_team:
                if player_team in TEAM_SOCIALS:
                    teams_to_target.append(player_team)
                else:
                    for key in TEAM_SOCIALS:
                        if key.endswith(player_team) or player_team.endswith(key.split()[-1]):
                            teams_to_target.append(key)
                            break
        else:
            # Game picks: parse teams from selection
            away_team, home_team, pick_detail = _parse_teams_from_selection(selection)
            game_name = extract_game_name(pick)

            # Format pick string
            if market_type == 'TOTAL' and pick_detail:
                pick_str = pick_detail
            elif market_type == 'SPREAD' and pick_detail:
                team_short = home_team.split()[-1] if home_team else selection.split()[0]
                pick_str = f"{team_short} {pick_detail}"
            else:
                team_short = home_team.split()[-1] if home_team else (selection.split()[0] if selection else 'ML')
                pick_str = f"{team_short} ML"

            # Odds formatting
            odds_str = f"{odds:+.0f}" if odds else "EV"

            # Team accounts to target
            teams_to_target = []
            for team_name in [home_team, away_team]:
                if not team_name:
                    continue
                if team_name in TEAM_SOCIALS:
                    teams_to_target.append(team_name)
                    continue
                for key in TEAM_SOCIALS:
                    if team_name.endswith(key) or key.endswith(team_name.split()[-1]):
                        teams_to_target.append(key)
                        break

        # Generate comments for each platform (Twitter removed — account suspended)
        for platform in ['ig', 'reddit']:
            # ─────────────────────────────────────────────────────────────
            # TEAM ACCOUNTS
            # ─────────────────────────────────────────────────────────────
            for team in teams_to_target:
                accounts = get_team_accounts(team, platform)
                for account in accounts:
                    target = account
                    if is_prop:
                        if platform == 'ig':
                            comment = f"Model has {pick_str} ({odds_str}) as a strong edge tonight. {edge:.0f}% over the market. {W}W-{L}L ({WR}%) this season, all graded"
                        else:  # reddit
                            comment = f"I run a data model that tracks edges across the market. {pick_str} ({odds_str}) is flagging — {edge:.0f}% edge over the market line.\n\nSeason record: {W}W-{L}L ({WR}%) | {profit}u. Every pick is tracked and graded publicly."
                    else:
                        if platform == 'ig':
                            comment = f"Big spot tonight. Our model has {pick_str} ({odds_str}) as one of the best edges on the board. {context}. {W}W-{L}L ({WR}%) this season, all tracked and graded"
                        else:  # reddit
                            comment = f"I run a data model that tracks edges across the market. For tonight's game, {pick_str} ({odds_str}) is flagging as one of the stronger plays — {edge:.0f}% edge over the market line.\n\nKey factor: {context}\n\nSeason record: {W}W-{L}L ({WR}%) | {profit}u. Every pick is tracked and graded publicly. Not trying to sell anything — just sharing the model's output."

                    comments.append({
                        'platform': platform,
                        'target': target,
                        'target_type': 'team',
                        'comment': comment,
                        'game': game_name,
                        'pick': pick_str,
                        'sport': sport_label
                    })

            # ─────────────────────────────────────────────────────────────
            # BETTING MEDIA ACCOUNTS
            # ─────────────────────────────────────────────────────────────
            for media in betting_targets[platform]:
                if is_prop:
                    if platform == 'ig':
                        target = media['handle']
                        comment = f"Model flags {pick_str} ({odds_str}) — {edge:.0f}% edge. Running {W}W-{L}L ({WR}%) this season with everything tracked"
                    else:  # reddit
                        target = media['subreddit']
                        comment = f"**{pick_str}** ({odds_str}) — {units}u ({kelly_tier})\n\nModel sees {edge:.0f}% edge here.\n\nSeason: {W}W-{L}L ({WR}%) | {profit}u, all picks tracked and graded. Full card on the IG (@scottys_edge) or Discord (discord.gg/JQ6rRfuN)."
                else:
                    if platform == 'ig':
                        target = media['handle']
                        comment = f"Model flags {pick_str} ({odds_str}) — {edge:.0f}% edge. {context}. Running {W}W-{L}L ({WR}%) this season with everything tracked"
                    else:  # reddit
                        target = media['subreddit']
                        comment = f"**{pick_str}** ({odds_str}) — {units}u ({kelly_tier})\n\nModel sees {edge:.0f}% edge here. {context}.\n\nSeason: {W}W-{L}L ({WR}%) | {profit}u, all picks tracked and graded. Full card on the IG (@scottys_edge) or Discord (discord.gg/JQ6rRfuN)."

                comments.append({
                    'platform': platform,
                    'target': target,
                    'target_type': 'betting',
                    'comment': comment,
                    'game': game_name,
                    'pick': pick_str,
                    'sport': sport_label
                })

    # Write to JSON
    output_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'cowork_comments.json')
    output_data = {
        'generated_at': datetime.now().isoformat(),
        'total_comments': len(comments),
        'by_platform': {
            'ig': len([c for c in comments if c['platform'] == 'ig']),
            'reddit': len([c for c in comments if c['platform'] == 'reddit'])
        },
        'comments': comments
    }

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
    except Exception as e:
        print(f"Error writing comments to {output_path}: {e}")

    return comments