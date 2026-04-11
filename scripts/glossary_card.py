"""
glossary_card.py — Generate pinned-post glossary cards for Instagram.

Two slides matching the standard Scotty's Edge card style:
  Slide 1: BETTING TERMS — universal sports betting vocabulary
  Slide 2: SCOTTY'S EDGE — our tier system, edge math, and grading

Output:
  data/cards/glossary_slide_1.jpg
  data/cards/glossary_slide_2.jpg

Run:
  PYTHONIOENCODING=utf-8 python scripts/glossary_card.py
"""
import os
from PIL import Image, ImageDraw

# Reuse the card_image visual language so the glossary matches the rest of the feed.
from card_image import (
    S, IG_W, IG_H, CARD_WIDTH, PADDING, INNER_WIDTH,
    BG, GREEN, WHITE, WHITE_80, WHITE_60, WHITE_40, WHITE_25, YELLOW, BLUE, ORANGE, RED,
    _load_fonts, _font, _draw_divider, _draw_disclaimer,
    _get_desktop, _finalize,
)


def _draw_header_centered(draw, fonts, y, subtitle):
    """Glossary-specific header: centered SCOTTY'S EDGE, no date in corner."""
    tf = _font(fonts, 'bold', 58)
    title_left = "SCOTTY'S "
    title_right = "EDGE"
    left_w = draw.textlength(title_left, font=tf)
    right_w = draw.textlength(title_right, font=tf)
    total_w = left_w + right_w
    start_x = (CARD_WIDTH - total_w) // 2
    draw.text((start_x, y), title_left, fill=WHITE, font=tf)
    draw.text((start_x + left_w, y), title_right, fill=GREEN, font=tf)

    # Subtitle centered below
    sf = _font(fonts, 'regular', 17)
    sub_w = draw.textlength(subtitle, font=sf)
    sub_x = (CARD_WIDTH - sub_w) // 2
    draw.text((sub_x, y + 66 * S), subtitle, fill=WHITE_40, font=sf)
    return y + 105 * S


# ─────────────────────────────────────────────────────────────────
# CONTENT
# ─────────────────────────────────────────────────────────────────

SLIDE_1_TITLE = "BETTING GLOSSARY"
SLIDE_1_SUBTITLE = "THE TERMS WE USE • PART 1 OF 2"
SLIDE_1_TERMS = [
    ("UNITS",
     "Standard bet size — set by you (1u could be $10, $25, $100). "
     "We post in units so the system works at any bankroll. "
     "Picks range from 0.5u (small lean) to 5u (MAX PLAY)."),

    ("ODDS",
     "American format. Negative odds (-110): bet that amount to win $100. "
     "Positive odds (+150): bet $100 to win that amount. "
     "-110 = 52.4% breakeven win rate."),

    ("EDGE %",
     "How much we think the line is mispriced vs fair value. "
     "20% edge means our model thinks the true probability is 20% "
     "higher than the bookmaker's implied odds. We only fire 20%+."),

    ("VIG / JUICE",
     "The bookmaker's built-in commission. Standard -110/-110 = 4.55% vig. "
     "Higher juice = harder to break even. We avoid heavy juice unless "
     "the edge is big enough to overcome it."),

    ("CLV",
     "Closing Line Value. Did the line move in our favor after we bet? "
     "Positive CLV = market agreed with our read. Negative = market "
     "disagreed. Long-term CLV is the #1 predictor of profitability."),

    ("CLOSING LINE",
     "The final line right before the game starts. Considered the "
     "sharpest market consensus. Beating the closing line consistently "
     "is the gold standard for proving an edge is real."),

    ("PUSH",
     "A tie. If the total is 8 and the final score adds to 8, the bet "
     "pushes — your stake is refunded. Pushes don't count as wins or "
     "losses on our record."),

    ("ROI",
     "Return on Investment. Total profit divided by total wagered. "
     "A 5% ROI on 1,000u wagered = +50u profit. "
     "Anything above 3% long-term is considered sharp."),
]


SLIDE_2_TITLE = "OUR SYSTEM"
SLIDE_2_SUBTITLE = "HOW WE BET • PART 2 OF 2"
SLIDE_2_SECTIONS = [
    ("BET TYPES", [
        ("MONEYLINE (ML)",
         "Pick the winner outright. No spread, no margin. Heavy favorites "
         "have negative odds; underdogs have plus odds."),

        ("SPREAD",
         "Favorite must win by MORE than the spread. Underdog can lose "
         "by less than the spread (or win outright). Levels the playing field."),

        ("TOTAL (O/U)",
         "Bet on combined points scored. OVER = both teams score MORE "
         "than the line. UNDER = LESS than the line."),

        ("PROP",
         "Player prop bet. Bet on individual stats (LeBron OVER 25.5 PTS, "
         "pitcher K's, RBI, etc) instead of game outcomes."),
    ]),

    ("RECOMMENDED PLAYS", [
        ("MAX PLAY (5u)",
         "Highest conviction. 20%+ edge at favorable odds. Kelly Criterion "
         "sizes these to 4.5-5.0 units. The bets we'd never miss."),

        ("STRONG (4u)",
         "High conviction. 20%+ edge but heavier juice (-130 or worse) so "
         "Kelly sizes the bet smaller. Same edge floor as MAX PLAY, just "
         "less favorable odds. Sized 3.5-4.5 units."),
    ]),

    ("BELOW THRESHOLD (NOT BET)", [
        ("SOLID / LEAN / SPRINKLE",
         "Lower-conviction tiers in our internal model. Our edge floor is "
         "20% — these picks are below that floor and do NOT fire as live "
         "bets. They exist for backtesting only."),
    ]),

    ("OUR RECORD", [
        ("PUBLIC RECORD",
         "Post-rebuild picks (since 3/4/2026) at 3.5+ units only. "
         "We don't pad the record with leans. What you see is what we bet."),
    ]),
]


# ─────────────────────────────────────────────────────────────────
# RENDERING
# ─────────────────────────────────────────────────────────────────

def _wrap_text(draw, text, font, max_width):
    """Greedy word-wrap. Returns list of lines."""
    words = text.split()
    lines, current = [], ""
    for word in words:
        test = (current + " " + word) if current else word
        if draw.textlength(test, font=font) > max_width:
            if current:
                lines.append(current)
            current = word
        else:
            current = test
    if current:
        lines.append(current)
    return lines


def _draw_term_block(draw, fonts, x, y, term, definition, term_font, def_font, line_height_def):
    """Draw a single TERM + definition block. Returns new y."""
    # Term in green, large
    draw.text((x, y), term, fill=GREEN, font=term_font)
    y += 36 * S

    # Definition wrapped to inner width
    lines = _wrap_text(draw, definition, def_font, INNER_WIDTH)
    for line in lines:
        draw.text((x, y), line, fill=WHITE_80, font=def_font)
        y += line_height_def
    y += 18 * S  # gap after each term
    return y


def _draw_section_header(draw, fonts, y, label):
    """Draw a category header — bigger and underlined to distinguish from term names."""
    sf = _font(fonts, 'bold', 30)
    draw.text((PADDING, y), label, fill=WHITE, font=sf)
    # Measure text width and draw a green underline beneath it
    text_w = draw.textlength(label, font=sf)
    underline_y = y + 44 * S
    draw.rectangle(
        [(PADDING, underline_y), (PADDING + int(text_w), underline_y + 4 * S)],
        fill=GREEN,
    )
    y += 60 * S
    return y


def generate_slide_1(fonts):
    img = Image.new('RGB', (IG_W, IG_H), BG)
    draw = ImageDraw.Draw(img)

    # Top accent bar
    draw.rectangle([(0, 0), (CARD_WIDTH, 5 * S)], fill=GREEN)

    # Header
    y = 20 * S
    y = _draw_header_centered(draw, fonts, y, subtitle=SLIDE_1_SUBTITLE)
    y += 8 * S
    _draw_divider(draw, y)
    y += 30 * S

    # Big section title
    title_font = _font(fonts, 'bold', 44)
    draw.text((PADDING, y), SLIDE_1_TITLE, fill=WHITE, font=title_font)
    y += 70 * S

    # Term blocks
    term_font = _font(fonts, 'bold', 24)
    def_font = _font(fonts, 'regular', 18)
    line_height_def = 26 * S

    for term, definition in SLIDE_1_TERMS:
        y = _draw_term_block(draw, fonts, PADDING, y, term, definition,
                             term_font, def_font, line_height_def)

    # Disclaimer at bottom
    disclaimer_h = 130 * S
    y_disc = IG_H - disclaimer_h
    _draw_divider(draw, y_disc)
    y_disc += 10 * S
    _draw_disclaimer(draw, fonts, y_disc)

    return img


def generate_slide_2(fonts):
    img = Image.new('RGB', (IG_W, IG_H), BG)
    draw = ImageDraw.Draw(img)

    # Top accent bar
    draw.rectangle([(0, 0), (CARD_WIDTH, 5 * S)], fill=GREEN)

    # Header
    y = 20 * S
    y = _draw_header_centered(draw, fonts, y, subtitle=SLIDE_2_SUBTITLE)
    y += 8 * S
    _draw_divider(draw, y)
    y += 26 * S

    # Big section title
    title_font = _font(fonts, 'bold', 44)
    draw.text((PADDING, y), SLIDE_2_TITLE, fill=WHITE, font=title_font)
    y += 60 * S

    # Slide 2 has more terms, so use slightly tighter sizing
    term_font = _font(fonts, 'bold', 21)
    def_font = _font(fonts, 'regular', 17)
    line_height_def = 24 * S

    for section_label, terms in SLIDE_2_SECTIONS:
        y = _draw_section_header(draw, fonts, y, section_label)
        for term, definition in terms:
            y = _draw_term_block(draw, fonts, PADDING, y, term, definition,
                                 term_font, def_font, line_height_def)
        y += 8 * S  # extra gap between sections

    # Disclaimer at bottom
    disclaimer_h = 130 * S
    y_disc = IG_H - disclaimer_h
    _draw_divider(draw, y_disc)
    y_disc += 10 * S
    _draw_disclaimer(draw, fonts, y_disc)

    return img


def main():
    fonts = _load_fonts()
    out_dir = _get_desktop()

    slide_1 = generate_slide_1(fonts)
    p1 = os.path.join(out_dir, 'glossary_slide_1.jpg')
    slide_1.save(p1, 'JPEG', quality=95)
    print(f"  Saved: {p1}")

    slide_2 = generate_slide_2(fonts)
    p2 = os.path.join(out_dir, 'glossary_slide_2.jpg')
    slide_2.save(p2, 'JPEG', quality=95)
    print(f"  Saved: {p2}")

    print()
    print("Two-slide glossary post draft generated.")
    print("Review the JPGs at data/cards/ and let me know what to tweak.")


if __name__ == '__main__':
    main()
