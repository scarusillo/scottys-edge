"""
scottys_edge.py — Scotty' Edge Calculation System

Implements the complete edge framework from "Gambler" Ch. 21-22:
  1. Key number values (adapted per sport)
  2. Star system for bet sizing (0.5★ to 3★)
  3. Vig-adjusted true spread
  4. Spread vs Moneyline decision engine
  5. Stack/cluster injury multiplier
  6. Cross-zero penalty
  7. Point value summation across spread differential
  8. Bet timing guidance (favorites early, dogs late)
"""
import math

# ══════════════════════════════════════════════════════════════
# 1. KEY NUMBER VALUES BY SPORT
# ══════════════════════════════════════════════════════════════
# Walters: "3 is the single most valuable number in NFL football betting"
# We adapt this concept to each sport based on historical margin distributions.

# Values represent the % chance a game lands on exactly that margin.
# Higher % = more valuable to get on/off that number.

KEY_NUMBERS = {
    'basketball_ncaab': {
        # College basketball margins: most common are small (1-5)
        # but 3 (free throw margin) and 7+ (blowout threshold) matter
        1: 4, 2: 4, 3: 5, 4: 4, 5: 4, 6: 4, 7: 5,
        8: 3, 9: 3, 10: 3, 11: 2, 12: 2, 13: 2, 14: 2,
        15: 2, 16: 2, 17: 1, 18: 1, 19: 1, 20: 1,
    },
    'basketball_nba': {
        # NBA: tighter margins, 1-5 very common, 7 significant
        1: 4, 2: 4, 3: 5, 4: 4, 5: 5, 6: 4, 7: 5,
        8: 3, 9: 3, 10: 3, 11: 2, 12: 2, 13: 2, 14: 2,
        15: 2, 16: 1, 17: 1, 18: 1, 19: 1, 20: 1,
    },
    'icehockey_nhl': {
        # Hockey: 1-goal margin is overwhelmingly common (~45% of games)
        # Puck line is 1.5, so crossing 1 and 2 are key
        1: 15, 2: 10, 3: 5, 4: 3, 5: 2, 6: 1,
    },
    'soccer_epl': {
        # Soccer: 1-goal margin is most common (~35%)
        # Draw (0) is very common (~25%), 2-goal ~20%
        0: 8, 1: 12, 2: 7, 3: 4, 4: 2, 5: 1,
    },
    'soccer_italy_serie_a': {
        0: 8, 1: 12, 2: 7, 3: 4, 4: 2, 5: 1,
    },
    'soccer_spain_la_liga': {
        0: 8, 1: 12, 2: 7, 3: 4, 4: 2, 5: 1,
    },
    # v12 FIX: These 5 sports were MISSING key numbers and fell back to
    # basketball_nba, which deflated their edges by ~3x. A 0.5-goal soccer
    # disagreement should produce a 12% edge, not a 4% basketball edge.
    'soccer_germany_bundesliga': {
        0: 8, 1: 12, 2: 7, 3: 4, 4: 2, 5: 1,
    },
    'soccer_france_ligue_one': {
        0: 8, 1: 12, 2: 7, 3: 4, 4: 2, 5: 1,
    },
    'soccer_uefa_champs_league': {
        0: 8, 1: 12, 2: 7, 3: 4, 4: 2, 5: 1,
    },
    'soccer_usa_mls': {
        0: 9, 1: 13, 2: 7, 3: 4, 4: 2, 5: 1,  # MLS slightly higher — more variance, goals matter more
    },
    'baseball_ncaa': {
        # College baseball: run margins 1-10. 1-run games most common (~30%)
        # But BETTING VALUE per run is lower than basketball per point.
        # Run line is ±1.5, so crossing integers 1-2 matters most.
        # Calibrated conservatively — early-season Elo is thin.
        1: 5, 2: 4, 3: 3, 4: 2, 5: 2, 6: 1, 7: 1, 8: 1, 9: 1, 10: 1,
    },
    # Tennis: game handicap margins. 1-3 game margins are most common.
    # Tennis spreads are in games (e.g., -3.5 games), not sets.
    # A 1-game margin has high value because it flips the spread side.
    'tennis': {
        0: 6, 1: 10, 2: 8, 3: 6, 4: 5, 5: 4, 6: 3, 7: 2, 8: 2, 9: 1, 10: 1,
    },
}

# ══════════════════════════════════════════════════════════════
# 2. STAR SYSTEM — Bet Sizing
# ══════════════════════════════════════════════════════════════
# From p.268: Pick minus spread → play strength
# 5.5% = 0.5 star, 7% = 1.0, 9% = 1.5, 11% = 2.0, 13% = 2.5, 15% = 3.0
# Below 5.5% = NO PLAY

STAR_THRESHOLDS = [
    (20.0, 3.0),   # v12 FIX: Was 15. 38-48 MAX PLAYs were 90% of losses. Raise bar.
    (16.0, 2.5),   # Was 13
    (13.0, 2.0),   # Was 11
    (10.0, 1.5),   # Was 9
    (7.0,  1.0),
    (5.5,  0.5),
]

def get_star_rating(point_value_pct):
    """Convert point value percentage to star rating."""
    for threshold, stars in STAR_THRESHOLDS:
        if point_value_pct >= threshold:
            return stars
    return 0.0  # No play


def stars_to_units(stars, bankroll_pct=1.0):
    """Legacy wrapper — calls kelly_units with default values."""
    return kelly_units(edge_pct=stars * 5.0, odds=-110, fraction=0.125)


def kelly_units(edge_pct, odds, fraction=0.125, max_units=5.0, min_units=0.5):
    """
    Kelly Criterion bet sizing.

    Full Kelly = (bp - q) / b
      where b = decimal profit per $1 (e.g., -110 → b=0.909)
            p = model probability of winning
            q = 1 - p

    We use 1/8 Kelly (12.5%) because:
    - Full Kelly is too aggressive for uncertain edges
    - Quarter Kelly still too aggressive for unvalidated model
    - 1/8 Kelly gives steady growth with very low ruin risk
    - As CLV proves the model works, increase to quarter Kelly

    Returns: units to bet (0.5 to 5.0)
    
    At 1/8 Kelly with -110 odds:
      8% edge  → ~1.5u    10% edge → ~2.0u
      13% edge → ~3.5u    15% edge → ~4.0u
      18% edge → ~4.5u    20%+ edge → 5.0u
    """
    if edge_pct <= 0 or odds is None:
        return 0.0

    # Convert American odds to decimal profit per $1 wagered
    if odds > 0:
        b = odds / 100.0        # e.g., +150 → 1.5
    else:
        b = 100.0 / abs(odds)   # e.g., -110 → 0.909

    # Estimate win probability from edge + implied
    if odds > 0:
        implied = 100.0 / (odds + 100.0)
    else:
        implied = abs(odds) / (abs(odds) + 100.0)

    p = implied + (edge_pct / 100.0)  # model prob = implied + edge
    p = min(0.95, max(0.05, p))       # clamp
    q = 1.0 - p

    # Full Kelly fraction
    full_kelly = (b * p - q) / b
    if full_kelly <= 0:
        return 0.0

    # Apply fraction (default 12.5% = 1/8 Kelly)
    kelly = full_kelly * fraction

    # Convert to units (1 unit = 1% of bankroll by convention)
    # Kelly as % of bankroll → divide by 0.01 to get units
    units = kelly * 100.0  # e.g., 0.015 Kelly → 1.5 units

    # Clamp to half-unit increments between 0.5 and max
    units = max(min_units, min(max_units, units))
    units = round(units * 2) / 2  # Round to nearest 0.5

    return units


def kelly_label(units):
    """Human-readable Kelly label — must actually differentiate picks."""
    if units >= 4.5:
        return "MAX PLAY"
    elif units >= 3.5:
        return "STRONG"
    elif units >= 2.5:
        return "SOLID"
    elif units >= 1.5:
        return "LEAN"
    else:
        return "SPRINKLE"


# ══════════════════════════════════════════════════════════════
# 3. POINT VALUE CALCULATION
# ══════════════════════════════════════════════════════════════
# Walters p.267: Sum the % value of each key number between
# your predicted spread and the posted spread.
# If total < 5.5%, it's not a play.

def calculate_point_value(model_spread, market_spread, sport):
    """
    Calculate the point value percentage.

    For each whole number between model_spread and market_spread,
    add the key number value. If the spread lands ON a whole number,
    count half its value.

    Returns: total point value percentage
    """
    # Tennis: all tournament keys share the same key numbers
    if sport.startswith('tennis_'):
        key_nums = KEY_NUMBERS.get('tennis', {})
    else:
        key_nums = KEY_NUMBERS.get(sport, KEY_NUMBERS.get('basketball_nba', {}))

    # Determine direction: which side are we on?
    diff = abs(model_spread - market_spread)
    if diff < 0.25:
        return 0.0  # No meaningful difference

    low = min(model_spread, market_spread)
    high = max(model_spread, market_spread)

    total_value = 0.0
    crossed_zero = False

    # Check each integer in the range
    i = math.ceil(low)
    while i <= math.floor(high):
        abs_i = abs(i)

        # Check if we cross zero (Walters: deduct value of one point)
        if low < 0 < high:
            crossed_zero = True

        val = key_nums.get(abs_i, 2)  # Default 2% for unlisted numbers

        # If spread lands exactly ON this number, count half value
        if abs(market_spread - i) < 0.01 or abs(model_spread - i) < 0.01:
            total_value += val / 2.0
        else:
            total_value += val

        i += 1

    # If no integers crossed but there's still a spread difference,
    # the half-point still has value based on nearest key number
    if total_value == 0 and diff >= 0.5:
        nearest = round(abs((model_spread + market_spread) / 2))
        val = key_nums.get(nearest, 2)
        total_value = val / 2.0

    # Cross-zero penalty (Walters p.267)
    if crossed_zero:
        min_key = min(key_nums.values()) if key_nums else 2
        total_value -= min_key

    return max(0.0, round(total_value, 1))


# ══════════════════════════════════════════════════════════════
# 4. VIG-ADJUSTED TRUE SPREAD
# ══════════════════════════════════════════════════════════════
# Walters p.269: A 3-point spread at -120 is really 3.25
# The vig shifts the true spread. This matters for edge calculation.

def vig_adjusted_spread(posted_spread, odds):
    """
    Adjust the posted spread based on the actual vig/juice.

    Standard is -110. If juice is higher (e.g., -120), the true
    spread is worse for you. If lower (e.g., -105), it's better.

    Returns: adjusted spread from bettor's perspective
    """
    if odds is None:
        return posted_spread

    # At -110, no adjustment needed (that's standard)
    # At -120, you're paying more → spread is ~0.25 worse
    # At -105, you're paying less → spread is ~0.125 better
    if odds < 0:
        vig_pct = abs(odds) / (abs(odds) + 100.0)
    else:
        vig_pct = 100.0 / (odds + 100.0)

    standard_vig = 110.0 / 210.0  # ~52.38%
    vig_diff = vig_pct - standard_vig

    # Each 2.38% of extra vig ≈ 0.5 points of spread
    # (derived from Walters' table on p.269)
    spread_adjustment = vig_diff * (0.5 / 0.0238)

    return round(posted_spread + spread_adjustment, 3)


def calculate_true_vig(odds):
    """Calculate the actual vig/hold as a percentage."""
    if odds is None:
        return 4.55  # Assume standard -110/-110
    if odds < 0:
        return (abs(odds) / (abs(odds) + 100.0) - 0.5) * 100
    else:
        return (0.5 - 100.0 / (odds + 100.0)) * 100


# ══════════════════════════════════════════════════════════════
# 5. SPREAD vs MONEYLINE DECISION
# ══════════════════════════════════════════════════════════════
# Walters p.270-272: Use conversion table to determine whether
# the spread or ML offers better value.

# Walters' NFL conversion table (spread → fair ML at -110 vig)
# Format: spread → (favorite_ml, dog_ml)
SPREAD_TO_ML_NFL = {
    1:  (116, -104), 1.5: (123, 102), 2:  (130, 108), 2.5: (137, 113),
    3:  (170, 141), 3.5: (197, 163), 4:  (210, 174), 4.5: (222, 184),
    5:  (237, 196), 5.5: (252, 208), 6:  (277, 229), 6.5: (299, 247),
    7:  (335, 277), 7.5: (368, 305), 8:  (397, 328), 8.5: (427, 353),
    9:  (441, 365), 9.5: (456, 377), 10: (510, 422),
    10.5:(561,464), 11: (595,492), 11.5:(631,522), 12: (657,543),
    12.5:(681,564), 13: (730,604), 13.5:(781,646), 14: (904,748),
}

# For basketball, the relationship is more linear since scores are higher
# We'll derive it from the logistic function
def spread_to_fair_ml(spread, sport):
    """Convert a point spread to fair moneyline odds (no vig)."""
    if 'basketball' in sport:
        scale = 6.3
    elif 'hockey' in sport:
        scale = 0.49
    elif 'soccer' in sport:
        scale = 0.40
    elif sport.startswith('tennis_'):
        scale = 2.5  # Tennis: game handicap scale
    else:
        scale = 6.3

    win_prob = 1.0 / (1.0 + math.exp(spread / scale))

    if win_prob >= 0.99:
        return (-9900, 9900)
    if win_prob <= 0.01:
        return (9900, -9900)

    if win_prob >= 0.5:
        fav_ml = -round(win_prob / (1 - win_prob) * 100)
        dog_ml = round((1 - win_prob) / win_prob * 100)
    else:
        fav_ml = round(win_prob / (1 - win_prob) * 100)
        dog_ml = -round((1 - win_prob) / win_prob * 100)

    return (fav_ml, dog_ml)


def recommend_spread_or_ml(model_spread, market_spread, market_ml, sport):
    """
    Walters' decision framework: bet the spread OR the moneyline?

    Logic:
    - Convert your model spread to a fair ML
    - Compare fair ML to actual posted ML
    - Whichever gives more value (in % terms) is the better bet

    Returns: 'SPREAD', 'MONEYLINE', or 'EITHER' with explanation
    """
    if market_ml is None or market_spread is None:
        return 'SPREAD', 'No ML available'

    # Fair ML based on model spread
    fair_fav, fair_dog = spread_to_fair_ml(model_spread, sport)

    # Edge on spread (using point value system)
    spread_edge = calculate_point_value(model_spread, market_spread, sport)

    # Edge on ML
    if market_ml > 0:
        # We're betting the dog
        ml_implied = 100.0 / (market_ml + 100.0)
        model_prob = 100.0 / (abs(fair_dog) + 100.0) if fair_dog > 0 else abs(fair_dog) / (abs(fair_dog) + 100.0)
        ml_edge = (model_prob - ml_implied) * 100
    else:
        # We're betting the favorite
        ml_implied = abs(market_ml) / (abs(market_ml) + 100.0)
        model_prob = abs(fair_fav) / (abs(fair_fav) + 100.0) if fair_fav < 0 else 100.0 / (fair_fav + 100.0)
        ml_edge = (model_prob - ml_implied) * 100

    # Walters rule: for small spreads (1-3), ML can be better
    # For larger spreads, spread is usually better
    if spread_edge >= 5.5 and ml_edge >= 5.0:
        if ml_edge > spread_edge * 1.2:
            return 'MONEYLINE', f'ML edge {ml_edge:.1f}% > spread value {spread_edge:.1f}%'
        elif spread_edge > ml_edge * 1.2:
            return 'SPREAD', f'Spread value {spread_edge:.1f}% > ML edge {ml_edge:.1f}%'
        else:
            return 'EITHER', f'Similar edge: spread {spread_edge:.1f}% vs ML {ml_edge:.1f}%'
    elif spread_edge >= 5.5:
        return 'SPREAD', f'Spread value {spread_edge:.1f}% (ML edge only {ml_edge:.1f}%)'
    elif ml_edge >= 5.0:
        return 'MONEYLINE', f'ML edge {ml_edge:.1f}% (spread value only {spread_edge:.1f}%)'
    else:
        return 'NO_PLAY', f'Insufficient edge: spread {spread_edge:.1f}%, ML {ml_edge:.1f}%'


# ══════════════════════════════════════════════════════════════
# 6. STACK INJURY MULTIPLIER
# ══════════════════════════════════════════════════════════════
# Walters p.251: "Multiple injuries at key positions can have an
# exponential impact — and it varies by position and team."
# Two top receivers out: don't assume linear sum. The combination
# "may be worth 50 percent more, or 6 points"

def stack_injury_multiplier(num_injuries_same_position, num_total_injuries):
    """
    Calculate the multiplier for stacked injuries.

    Walters: cluster injuries are exponential, not linear.
    - 2 players at same position group: 1.5x their combined value
    - 3 players at same position group: 2.0x
    - Additionally, if backup is also hurt: original player's
      impact increases significantly

    Returns: multiplier to apply to raw injury sum
    """
    multiplier = 1.0

    # Position cluster bonus
    if num_injuries_same_position >= 3:
        multiplier *= 2.0
    elif num_injuries_same_position >= 2:
        multiplier *= 1.5

    # Volume penalty: many injuries compound team dysfunction
    if num_total_injuries >= 5:
        multiplier *= 1.2
    elif num_total_injuries >= 3:
        multiplier *= 1.1

    return round(multiplier, 2)


# ══════════════════════════════════════════════════════════════
# 7. BET TIMING GUIDANCE
# ══════════════════════════════════════════════════════════════
# Walters p.240: "Bet favorites early and dogs late."

def bet_timing_advice(model_spread, market_spread):
    """
    Walters' timing advice based on WHICH SIDE you're betting:
    - Favorites (laying points): bet EARLY — line moves toward the favorite
      as public money comes in, so your number gets worse over time.
    - Dogs (getting points): bet LATE — public loads up on favorites,
      pushing the line further in your favor. Wait for the best number.
    
    market_spread here is from the perspective of the SIDE WE'RE BETTING.
    Positive = getting points (dog). Negative = laying points (favorite).
    """
    if market_spread is None:
        return 'EARLY', 'No line available — bet early'
    
    if market_spread < 0:
        # We're laying points (favorite) — bet early before line moves
        return 'EARLY', 'Favorite play — bet early before line moves'
    elif market_spread > 0:
        # We're getting points (dog) — bet late for best number
        return 'LATE', 'Dog play — bet late for best number'
    else:
        # Pick'em — slight lean to early
        return 'EARLY', 'Pick\'em — slight lean to bet early'


# ══════════════════════════════════════════════════════════════
# 8. COMPLETE EDGE ASSESSMENT
# ══════════════════════════════════════════════════════════════

def scottys_edge_assessment(model_spread, market_spread, odds, sport,
                            market_ml=None, injury_count=0,
                            position_cluster_count=0):
    """
    Complete systematic edge calculation.
    
    v12 FIX: Added directional check. The model must disagree in the
    RIGHT direction for the side being evaluated:
    
    For HOME assessment (called with ms, mkt_hs):
      Value exists when model_spread < market_spread
      (model says home is MORE favored than market → fav value)
    
    For AWAY assessment (called with -ms, mkt_as):
      Value exists when model_spread < market_spread
      (model says away deserves FEWER points → market giving too many → dog value)
    
    Example: Denver +2.0, model says +2.7 (dog deserves 2.7 but gets 2.0)
      -ms=+2.7 > mkt_as=+2.0 → dog getting LESS than fair → NO dog value
      ms=-2.7 < mkt_hs=-2.0 → home more favored than market → FAVORITE value
    """
    # DIRECTIONAL CHECK — model must favor this side
    # model_spread < market_spread means value for this side
    if model_spread >= market_spread:
        return {
            'point_value_pct': 0.0, 'star_rating': 0, 'units': 0,
            'is_play': False, 'vig_adjusted_spread': market_spread,
            'raw_spread_diff': round(abs(model_spread - market_spread), 2),
            'spread_or_ml': 'NONE', 'spread_or_ml_reason': 'Wrong direction',
            'timing': 'LATE', 'timing_reason': '', 'injury_multiplier': 1.0,
            'confidence': 'NONE',
        }
    
    # 1. Vig-adjusted spread
    true_spread = vig_adjusted_spread(market_spread, odds)

    # 2. Point value calculation (using vig-adjusted spread)
    pv_pct = calculate_point_value(model_spread, true_spread, sport)

    # 3. Apply injury multiplier to point value if relevant
    inj_mult = stack_injury_multiplier(position_cluster_count, injury_count)
    # Injuries don't directly multiply point value, but they affect
    # confidence in the model spread being accurate
    if inj_mult > 1.0:
        # If our model already accounts for injuries in the spread,
        # the extra multiplier means market hasn't adjusted enough
        pv_pct *= min(inj_mult, 1.5)  # Cap at 1.5x
        pv_pct = round(pv_pct, 1)

    # 4. Star rating
    stars = get_star_rating(pv_pct)

    # 5. Is it a play?
    is_play = stars > 0  # Must be at least 0.5 stars (5.5%+)

    # 6. Spread vs ML
    rec, rec_reason = recommend_spread_or_ml(
        model_spread, market_spread, market_ml, sport)

    # 7. Timing
    timing, timing_reason = bet_timing_advice(model_spread, market_spread)

    # 8. Units — Kelly Criterion with actual odds
    actual_odds = odds if odds else -110
    units = kelly_units(edge_pct=pv_pct, odds=actual_odds)
    kl = kelly_label(units)

    return {
        'point_value_pct': pv_pct,
        'star_rating': stars,
        'units': units,
        'is_play': is_play,
        'vig_adjusted_spread': true_spread,
        'raw_spread_diff': round(abs(model_spread - market_spread), 2),
        'spread_or_ml': rec,
        'spread_or_ml_reason': rec_reason,
        'timing': timing,
        'timing_reason': timing_reason,
        'injury_multiplier': inj_mult,
        'confidence': _confidence_label(stars),
    }


def _confidence_label(stars):
    if stars >= 2.5: return 'ELITE'
    if stars >= 2.0: return 'HIGH'
    if stars >= 1.5: return 'STRONG'
    if stars >= 1.0: return 'MEDIUM'
    if stars >= 0.5: return 'LOW'
    return 'NO_PLAY'


# ══════════════════════════════════════════════════════════════
# 9. ADAPTED EDGE FOR EACH SPORT
# ══════════════════════════════════════════════════════════════

def minimum_play_threshold(sport, is_thin_data=False):
    """
    Minimum point value % required for a play in each sport.

    MARKET TIERS (v11):
      SOFT  — thin markets, fewer eyeballs, more pricing errors
              NCAAB, MLS, Bundesliga, Ligue 1, Serie A, UCL
              → Lower threshold (8-10%) because edges are real
      
      SHARP — deep markets, sharp money, razor-thin margins
              NBA, NHL, EPL, La Liga
              → Higher threshold (15%) because "edges" are usually noise
    
    The model should be AGGRESSIVE in soft markets and SELECTIVE in sharp ones.
    """
    # Tennis: soft market, individual sport — all at 8%
    if sport.startswith('tennis_'):
        threshold = 8.0
        if is_thin_data:
            threshold *= 1.5
        return threshold

    # Soft markets — where our model can genuinely disagree with the market
    soft = {
        'basketball_ncaab': 8.0,            # 363 teams, books can't price them all well
        'soccer_usa_mls': 8.0,              # Young league, high variance, weak lines
        'soccer_germany_bundesliga': 12.0,   # v12 FIX: Was 8. 2-3, -6.7u — too loose.
        'soccer_france_ligue_one': 12.0,     # v12 FIX: Was 8. 0-3, -11.0u — only strong edges.
        'soccer_italy_serie_a': 12.0,        # v12 FIX: Was 8. 0-2, -8.0u — same.
        'soccer_uefa_champs_league': 9.0,   # Slightly sharper (high profile)
        'soccer_spain_la_liga': 10.0,       # Softer than EPL, sharper than Bundesliga
        'baseball_ncaa': 8.0,               # Same as NCAAB — key numbers + 2.0 divergence cap are the real filters
    }
    
    # Sharp markets — only bet when the model SCREAMS value
    sharp = {
        'basketball_nba': 15.0,             # Sharpest market in sports
        'icehockey_nhl': 8.0,              # Reclassified soft: puck lines + ML dogs are inefficient
        'soccer_epl': 13.0,                 # Most bet soccer league globally
    }
    
    threshold = soft.get(sport) or sharp.get(sport, 10.0)
    
    if is_thin_data:
        threshold *= 1.5  # Extra caution when we have <30 games of data
    
    return threshold


# Market tier classification — single source of truth in config.py
from config import SOFT_MARKETS, SHARP_MARKETS


# Quick test
if __name__ == '__main__':
    print("=" * 60)
    print("  SCOTTY'S EDGE SYSTEM — TEST")
    print("=" * 60)

    # Test: Model says team is -7.5, market has -5.5 (basketball)
    result = scottys_edge_assessment(
        model_spread=-7.5, market_spread=-5.5, odds=-110,
        sport='basketball_ncaab', market_ml=-220)
    print(f"\n  NCAAB: Model -7.5 vs Market -5.5")
    for k, v in result.items():
        print(f"    {k}: {v}")

    # Test: Model says team is -3, market has -1 (NHL)
    result = scottys_edge_assessment(
        model_spread=-0.3, market_spread=-0.1, odds=-115,
        sport='icehockey_nhl', market_ml=-130)
    print(f"\n  NHL: Model -0.3 vs Market -0.1")
    for k, v in result.items():
        print(f"    {k}: {v}")

    # Test: Key number value for crossing 3 and 7 in football
    pv = calculate_point_value(-7.5, -4.5, 'basketball_ncaab')
    print(f"\n  NCAAB spread -7.5 vs -4.5 → point value: {pv}%")
    print(f"  Star rating: {get_star_rating(pv)}★")
