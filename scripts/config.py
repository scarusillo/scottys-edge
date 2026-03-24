"""
config.py — Scotty's Edge v12: Single Source of Truth

Every threshold, market tier, and sport parameter lives here.
All other scripts import from this file instead of defining their own.

RULE: If you change a value, change it HERE and nowhere else.
"""

VERSION = "v12"

# ═══════════════════════════════════════════════════════════════
# MARKET TIERS — Determines filter aggressiveness
# ═══════════════════════════════════════════════════════════════
# SOFT  = thin markets, pricing errors common, lower thresholds
# SHARP = deep markets, sharp money, higher thresholds
#
# v12.2 FIX: NHL and La Liga moved back to SHARP.
#   NHL was in SOFT when it had 5 bets. Now 11W-6L +12.6u — proven sharp performer.
#   La Liga top clubs are sharply priced by European books.
#   Being in SOFT required 20% edge for small dogs — killed almost all NHL/La Liga picks.

SOFT_MARKETS = {
    'basketball_ncaab',
    'soccer_usa_mls',
    'soccer_germany_bundesliga',
    'soccer_france_ligue_one',
    'soccer_italy_serie_a',
    'soccer_uefa_champs_league',
    'soccer_mexico_ligamx',
    'baseball_ncaa',
}

SHARP_MARKETS = {
    'basketball_nba',
    'icehockey_nhl',
    'soccer_epl',
    'soccer_spain_la_liga',
}


# ═══════════════════════════════════════════════════════════════
# HOME COURT / FIELD / ICE ADVANTAGE (in sport-native units)
# ═══════════════════════════════════════════════════════════════

HOME_ADVANTAGE = {
    'basketball_nba': 2.5,
    'basketball_ncaab': 3.2,
    'icehockey_nhl': 0.15,
    'soccer_epl': 0.40,
    'soccer_italy_serie_a': 0.45,
    'soccer_spain_la_liga': 0.40,
    'soccer_germany_bundesliga': 0.42,
    'soccer_france_ligue_one': 0.40,
    'soccer_uefa_champs_league': 0.30,
    'soccer_usa_mls': 0.45,
    'soccer_mexico_ligamx': 0.50,
    'baseball_ncaa': 0.4,
}


# ═══════════════════════════════════════════════════════════════
# MINIMUM PLAY THRESHOLDS (edge % required per sport)
# ═══════════════════════════════════════════════════════════════

PLAY_THRESHOLDS = {
    'basketball_ncaab': 8.0,
    'soccer_usa_mls': 8.0,
    'soccer_germany_bundesliga': 12.0,
    'soccer_france_ligue_one': 12.0,
    'soccer_italy_serie_a': 12.0,
    'soccer_uefa_champs_league': 9.0,
    'baseball_ncaa': 8.0,
    'basketball_nba': 15.0,
    'icehockey_nhl': 8.0,
    'soccer_epl': 13.0,
    'soccer_spain_la_liga': 10.0,
    'soccer_mexico_ligamx': 8.0,
}


# ═══════════════════════════════════════════════════════════════
# SPORT MODEL PARAMETERS
# ═══════════════════════════════════════════════════════════════

SPORT_CONFIG = {
    'basketball_ncaab': {
        'logistic_scale': 6.3, 'spread_std': 11.0,
        'home_court': HOME_ADVANTAGE['basketball_ncaab'],
        'max_spread_divergence': 4.5,
        'ml_scale': 7.5,
    },
    'basketball_nba': {
        'logistic_scale': 6.3, 'spread_std': 11.0,
        'home_court': HOME_ADVANTAGE['basketball_nba'],
        'max_spread_divergence': 4.0,
        'ml_scale': 7.5,
    },
    'icehockey_nhl': {
        'logistic_scale': 0.49, 'spread_std': 2.2,
        'home_court': HOME_ADVANTAGE['icehockey_nhl'],
        'max_spread_divergence': 1.5,
        'ml_scale': 2.2,
    },
    'soccer_epl': {
        'logistic_scale': 0.40, 'spread_std': 1.3,
        'home_court': HOME_ADVANTAGE['soccer_epl'],
        'max_spread_divergence': 0.75,
        'ml_scale': 1.0,
    },
    'soccer_italy_serie_a': {
        'logistic_scale': 0.40, 'spread_std': 1.3,
        'home_court': HOME_ADVANTAGE['soccer_italy_serie_a'],
        'max_spread_divergence': 0.75,
        'ml_scale': 1.0,
    },
    'soccer_spain_la_liga': {
        'logistic_scale': 0.40, 'spread_std': 1.3,
        'home_court': HOME_ADVANTAGE['soccer_spain_la_liga'],
        'max_spread_divergence': 0.75,
        'ml_scale': 1.0,
    },
    'soccer_germany_bundesliga': {
        'logistic_scale': 0.40, 'spread_std': 1.3,
        'home_court': HOME_ADVANTAGE['soccer_germany_bundesliga'],
        'max_spread_divergence': 0.75,
        'ml_scale': 1.0,
    },
    'soccer_france_ligue_one': {
        'logistic_scale': 0.40, 'spread_std': 1.3,
        'home_court': HOME_ADVANTAGE['soccer_france_ligue_one'],
        'max_spread_divergence': 0.75,
        'ml_scale': 1.0,
    },
    'soccer_uefa_champs_league': {
        'logistic_scale': 0.40, 'spread_std': 1.3,
        'home_court': HOME_ADVANTAGE['soccer_uefa_champs_league'],
        'max_spread_divergence': 0.75,
        'ml_scale': 1.0,
    },
    'soccer_usa_mls': {
        'logistic_scale': 0.40, 'spread_std': 1.3,
        'home_court': HOME_ADVANTAGE['soccer_usa_mls'],
        'max_spread_divergence': 0.75,
        'ml_scale': 1.0,
    },
    'soccer_mexico_ligamx': {
        'logistic_scale': 0.40, 'spread_std': 1.3,
        'home_court': HOME_ADVANTAGE['soccer_mexico_ligamx'],
        'max_spread_divergence': 0.75,
        'ml_scale': 1.0,
    },
    'baseball_ncaa': {
        'logistic_scale': 1.8, 'spread_std': 10.0,
        'home_court': HOME_ADVANTAGE['baseball_ncaa'],
        'max_spread_divergence': 2.0,
        'ml_scale': 3.5,
    },
}


# ═══════════════════════════════════════════════════════════════
# PROP SPORTS
# ═══════════════════════════════════════════════════════════════

PROP_SPORTS = ['basketball_nba', 'icehockey_nhl']


# ═══════════════════════════════════════════════════════════════
# EXCLUDED BOOKS
# ═══════════════════════════════════════════════════════════════

EXCLUDED_BOOKS = {'Bovada', 'BetOnline.ag', 'BetUS', 'MyBookie.ag', 'LowVig.ag'}


# ═══════════════════════════════════════════════════════════════
# KELLY PARAMETERS
# ═══════════════════════════════════════════════════════════════

KELLY_FRACTION = 0.125
MAX_UNITS = 5.0
MIN_UNITS = 0.5


# ═══════════════════════════════════════════════════════════════
# STAR THRESHOLDS (point value % -> star rating)
# ═══════════════════════════════════════════════════════════════

STAR_THRESHOLDS = [
    (20.0, 3.0),
    (16.0, 2.5),
    (13.0, 2.0),
    (10.0, 1.5),
    (7.0,  1.0),
    (5.5,  0.5),
]


# ═══════════════════════════════════════════════════════════════
# MERGE & SELECT PARAMETERS
# ═══════════════════════════════════════════════════════════════

MAX_SHARP_PICKS = 4
MAX_SOFT_PICKS = 10
MAX_PER_SPORT_SOFT = 5

MARKET_MIN_EDGE = {
    'TOTAL': 15.0,
    'SPREAD': 13.0,
    'MONEYLINE': 13.0,
}
MERGE_MIN_UNITS = 3.0

PROP_MIN_UNITS = 2.0
PROP_MIN_EDGE = 8.0
PROP_MIN_EDGE_THREES = 12.0
MAX_PROPS_PER_GAME = 3
MAX_SAME_STAT_PER_GAME = 2


# ═══════════════════════════════════════════════════════════════
# BOOTSTRAP RATING CAPS
# ═══════════════════════════════════════════════════════════════

MAX_RATING = {
    'basketball_nba': 10, 'basketball_ncaab': 12,
    'icehockey_nhl': 0.6,
    'soccer_epl': 0.5, 'soccer_italy_serie_a': 0.5,
    'soccer_spain_la_liga': 0.5, 'soccer_germany_bundesliga': 0.5,
    'soccer_france_ligue_one': 0.5, 'soccer_uefa_champs_league': 0.5,
    'soccer_usa_mls': 0.5,
    'soccer_mexico_ligamx': 0.5,
    'baseball_ncaa': 3.0,
}
