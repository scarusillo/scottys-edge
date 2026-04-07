"""
config.py — Scotty's Edge v12: Single Source of Truth

Every threshold, market tier, and sport parameter lives here.
All other scripts import from this file instead of defining their own.

RULE: If you change a value, change it HERE and nowhere else.
"""

VERSION = "v12"

# ═══════════════════════════════════════════════════════════════
# TENNIS — Tournament keys, surfaces, and metadata
# ═══════════════════════════════════════════════════════════════
# Odds API uses per-tournament keys. Only a few are active at any time.
# detect_active_tennis() in odds_api.py polls the free /sports endpoint
# to find which are live before fetching odds.

TENNIS_ATP_KEYS = [
    'tennis_atp_aus_open_singles',
    'tennis_atp_french_open',
    'tennis_atp_wimbledon',
    'tennis_atp_us_open',
    'tennis_atp_indian_wells',
    'tennis_atp_miami_open',
    'tennis_atp_monte_carlo_masters',
    'tennis_atp_madrid_open',
    'tennis_atp_italian_open',
    'tennis_atp_canadian_open',
    'tennis_atp_cincinnati_open',
    'tennis_atp_shanghai_masters',
    'tennis_atp_paris_masters',
    'tennis_atp_dubai',
    'tennis_atp_qatar_open',
    'tennis_atp_china_open',
]

TENNIS_WTA_KEYS = [
    'tennis_wta_aus_open_singles',
    'tennis_wta_french_open',
    'tennis_wta_wimbledon',
    'tennis_wta_us_open',
    'tennis_wta_indian_wells',
    'tennis_wta_miami_open',
    'tennis_wta_madrid_open',
    'tennis_wta_italian_open',
    'tennis_wta_canadian_open',
    'tennis_wta_cincinnati_open',
    'tennis_wta_dubai',
    'tennis_wta_qatar_open',
    'tennis_wta_china_open',
    'tennis_wta_wuhan_open',
]

TENNIS_SPORTS = TENNIS_ATP_KEYS + TENNIS_WTA_KEYS

# Surface mapping — the #1 contextual factor in tennis
TENNIS_SURFACES = {
    # HARD COURT
    'tennis_atp_aus_open_singles': 'hard',
    'tennis_atp_us_open': 'hard',
    'tennis_atp_indian_wells': 'hard',
    'tennis_atp_miami_open': 'hard',
    'tennis_atp_canadian_open': 'hard',
    'tennis_atp_cincinnati_open': 'hard',
    'tennis_atp_shanghai_masters': 'hard',
    'tennis_atp_paris_masters': 'hard',  # indoor hard
    'tennis_atp_dubai': 'hard',
    'tennis_atp_qatar_open': 'hard',
    'tennis_atp_china_open': 'hard',
    'tennis_wta_aus_open_singles': 'hard',
    'tennis_wta_us_open': 'hard',
    'tennis_wta_indian_wells': 'hard',
    'tennis_wta_miami_open': 'hard',
    'tennis_wta_canadian_open': 'hard',
    'tennis_wta_cincinnati_open': 'hard',
    'tennis_wta_dubai': 'hard',
    'tennis_wta_qatar_open': 'hard',
    'tennis_wta_china_open': 'hard',
    'tennis_wta_wuhan_open': 'hard',
    # CLAY
    'tennis_atp_french_open': 'clay',
    'tennis_atp_monte_carlo_masters': 'clay',
    'tennis_atp_madrid_open': 'clay',
    'tennis_atp_italian_open': 'clay',
    'tennis_wta_french_open': 'clay',
    'tennis_wta_madrid_open': 'clay',
    'tennis_wta_italian_open': 'clay',
    # GRASS
    'tennis_atp_wimbledon': 'grass',
    'tennis_wta_wimbledon': 'grass',
}

# Grand Slams are best-of-5 sets (ATP only); everything else is best-of-3
TENNIS_BEST_OF = {k: 5 for k in [
    'tennis_atp_aus_open_singles', 'tennis_atp_french_open',
    'tennis_atp_wimbledon', 'tennis_atp_us_open',
]}

# Short display labels for each tournament
TENNIS_LABELS = {
    'tennis_atp_aus_open_singles': 'AUS OPEN',
    'tennis_atp_french_open': 'FRENCH OPEN',
    'tennis_atp_wimbledon': 'WIMBLEDON',
    'tennis_atp_us_open': 'US OPEN',
    'tennis_atp_indian_wells': 'INDIAN WELLS',
    'tennis_atp_miami_open': 'MIAMI OPEN',
    'tennis_atp_monte_carlo_masters': 'MONTE CARLO',
    'tennis_atp_madrid_open': 'MADRID OPEN',
    'tennis_atp_italian_open': 'ITALIAN OPEN',
    'tennis_atp_canadian_open': 'CANADIAN OPEN',
    'tennis_atp_cincinnati_open': 'CINCINNATI',
    'tennis_atp_shanghai_masters': 'SHANGHAI',
    'tennis_atp_paris_masters': 'PARIS MASTERS',
    'tennis_atp_dubai': 'DUBAI',
    'tennis_atp_qatar_open': 'QATAR OPEN',
    'tennis_atp_china_open': 'CHINA OPEN',
    'tennis_wta_aus_open_singles': 'AUS OPEN (W)',
    'tennis_wta_french_open': 'FRENCH OPEN (W)',
    'tennis_wta_wimbledon': 'WIMBLEDON (W)',
    'tennis_wta_us_open': 'US OPEN (W)',
    'tennis_wta_indian_wells': 'INDIAN WELLS (W)',
    'tennis_wta_miami_open': 'MIAMI OPEN (W)',
    'tennis_wta_madrid_open': 'MADRID OPEN (W)',
    'tennis_wta_italian_open': 'ITALIAN OPEN (W)',
    'tennis_wta_canadian_open': 'CANADIAN OPEN (W)',
    'tennis_wta_cincinnati_open': 'CINCINNATI (W)',
    'tennis_wta_dubai': 'DUBAI (W)',
    'tennis_wta_qatar_open': 'QATAR OPEN (W)',
    'tennis_wta_china_open': 'CHINA OPEN (W)',
    'tennis_wta_wuhan_open': 'WUHAN OPEN',
}

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
} | set(TENNIS_SPORTS)  # Tennis: individual sport, thin liquidity = soft market

SHARP_MARKETS = {
    'basketball_nba',
    'icehockey_nhl',
    'soccer_epl',
    'soccer_spain_la_liga',
    'baseball_mlb',             # v17: Pro baseball — sharp, efficient market
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
    'baseball_mlb': 0.3,        # v17: MLB ~54% home win rate (weaker than college)
}
# Tennis: no home advantage (neutral tournament venues)
for _tk in TENNIS_SPORTS:
    HOME_ADVANTAGE[_tk] = 0.0


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
    'baseball_mlb': 12.0,       # v17: Sharp market, higher bar than college (8.0)
}
# Tennis: surface-aware thresholds
# Clay tournaments are sharper than model expected (1W-3L, -14.7u on clay)
# Hard court stays at 15%; clay raised to 20%
TENNIS_CLAY_TOURNAMENTS = {k for k, v in TENNIS_SURFACES.items() if v == 'clay'}
for _tk in TENNIS_SPORTS:
    if _tk in TENNIS_CLAY_TOURNAMENTS:
        PLAY_THRESHOLDS[_tk] = 20.0
    else:
        PLAY_THRESHOLDS[_tk] = 15.0

# Tennis ML cap: no picks beyond ±200 (no big favorites or long-shot dogs)
# +200 dog = 33% implied, -200 fav = 67% implied. Keeps us in competitive matches.
TENNIS_ML_CAP = 200


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
    'baseball_mlb': {
        'logistic_scale': 1.8, 'spread_std': 8.0,
        'home_court': HOME_ADVANTAGE['baseball_mlb'],
        'max_spread_divergence': 4.0,
        'ml_scale': 3.5,
    },
}
# Tennis SPORT_CONFIG — game handicap (set handicap) based.
# Tennis spreads are in games (e.g., -3.5 games), totals in total games.
# logistic_scale: steeper than soccer (1 set diff ~ big impact), gentler than basketball
# spread_std: game-based variance (best-of-3 avg ~24 games, std ~5)
# ml_scale: for win probability — tennis ML is the primary market
_TENNIS_CONFIG_HARD = {
    'logistic_scale': 2.5, 'spread_std': 5.0, 'home_court': 0.0,
    'max_spread_divergence': 4.0, 'ml_scale': 2.5,
}
_TENNIS_CONFIG_CLAY = {
    'logistic_scale': 2.5, 'spread_std': 5.5, 'home_court': 0.0,  # Clay: more upsets, wider std
    'max_spread_divergence': 2.5, 'ml_scale': 2.5,  # v24: Was 4.5 — model spread near 0 was generating phantom edges at +3.5/+4.5 lines
}
_TENNIS_CONFIG_GRASS = {
    'logistic_scale': 2.5, 'spread_std': 4.5, 'home_court': 0.0,  # Grass: serve-dominant, tighter
    'max_spread_divergence': 3.5, 'ml_scale': 2.5,
}
for _tk in TENNIS_SPORTS:
    _surf = TENNIS_SURFACES.get(_tk, 'hard')
    if _surf == 'clay':
        SPORT_CONFIG[_tk] = dict(_TENNIS_CONFIG_CLAY)
    elif _surf == 'grass':
        SPORT_CONFIG[_tk] = dict(_TENNIS_CONFIG_GRASS)
    else:
        SPORT_CONFIG[_tk] = dict(_TENNIS_CONFIG_HARD)


# ═══════════════════════════════════════════════════════════════
# PROP SPORTS
# ═══════════════════════════════════════════════════════════════

PROP_SPORTS = ['basketball_nba', 'icehockey_nhl', 'baseball_mlb']


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
MIN_ODDS = -180          # Block heavy favorites (odds worse than -180)


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
    'TOTAL': 20.0,
    'SPREAD': 20.0,
    'MONEYLINE': 20.0,
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
# Tennis: game-handicap scale (±3-5 games typical)
for _tk in TENNIS_SPORTS:
    MAX_RATING[_tk] = 3.0
