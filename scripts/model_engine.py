"""
model_engine.py v9 — Scotty's Edge

Integrates the complete edge framework inspired by from "Gambler":
  - Key number point value system for edge calculation
  - Star system for bet sizing (0.5-3.0 stars)
  - Vig-adjusted spread calculation
  - Spread vs Moneyline recommendation
  - Stack injury multiplier (exponential, not linear)
  - Cross-zero penalty
  - Bet timing guidance (favorites early, dogs late)
  - 90/10 power rating update formula
"""
import sqlite3, math, os
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
    EASTERN = ZoneInfo('America/New_York')
except ImportError:
    EASTERN = None

def _to_eastern(utc_dt):
    """Convert UTC datetime to Eastern time (handles DST automatically)."""
    if EASTERN and utc_dt.tzinfo:
        return utc_dt.astimezone(EASTERN)
    # Fallback: check if DST is active (March-November)
    month = utc_dt.month
    if 3 <= month <= 10:  # Rough DST window
        return utc_dt - timedelta(hours=4)  # EDT
    return utc_dt - timedelta(hours=5)  # EST

def _eastern_tz_label():
    """Return 'EDT' or 'EST' based on current date."""
    now = datetime.now()
    if 3 <= now.month <= 10:
        return 'EDT'
    return 'EST'

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

from scottys_edge import (
    scottys_edge_assessment, calculate_point_value, get_star_rating,
    vig_adjusted_spread, bet_timing_advice,
    minimum_play_threshold,
    kelly_units, kelly_label,
)

# Context engine — schedule, travel, line movement, altitude, splits
try:
    from context_engine import get_context_adjustments, line_movement_signal
    HAS_CONTEXT = True
except ImportError:
    HAS_CONTEXT = False

# Elo engine integration (independent ratings from game results)
try:
    from elo_engine import get_elo_ratings, blended_spread, ELO_CONFIG
    HAS_ELO = True
except ImportError:
    HAS_ELO = False

# Pitcher context — day-of-week pitching quality + named starters
try:
    from pitcher_scraper import get_pitcher_context
    HAS_PITCHER = True
except ImportError:
    HAS_PITCHER = False

# MLB probable pitchers — gate MLB picks on confirmed starters
try:
    from pitcher_scraper import get_mlb_probable_starters
    HAS_MLB_PITCHERS = True
except ImportError:
    HAS_MLB_PITCHERS = False

# NHL probable goalies — gate NHL picks on confirmed starters
try:
    from pitcher_scraper import get_nhl_probable_goalies
    HAS_NHL_GOALIES = True
except ImportError:
    HAS_NHL_GOALIES = False

# Referee tendencies — dynamic adjustment from scraped ESPN data
try:
    from referee_engine import get_ref_adjustment
    HAS_REF = True
except ImportError:
    HAS_REF = False


SPORT_CONFIG = {
    'basketball_ncaab': {
        'logistic_scale': 7.5, 'spread_std': 13.0, 'home_court': 3.2,  # Was 6.3/11.0 — same as NBA but NCAAB has higher variance. 25%+ edge bucket was 1W-6L = fake edges.
        'max_spread_divergence': 4.5,   # v12 FIX: Was 6.0 — medium dogs (4-7.5 pts) went 2-8. 4.5 allows small dogs + favorites.
        'ml_scale': 7.5,  # Separate scale for moneyline win probability
    },
    'basketball_nba': {
        'logistic_scale': 6.3, 'spread_std': 11.0, 'home_court': 2.5,
        'max_spread_divergence': 4.0,   # Tightened from 5.0
        'ml_scale': 7.5,
    },
    'icehockey_nhl': {
        'logistic_scale': 0.49, 'spread_std': 2.2, 'home_court': 0.15,
        'max_spread_divergence': 2.5,   # v25.29: raised from 1.5. Puckline is rigid ±1.5;
                                         # any model spread >±3 goals is expected NHL divergence,
                                         # not model error. Backtest of 20 previously-blocked NHL
                                         # puckline picks at div 1.5-2.5: 17W-3L (85%), +27.4u at
                                         # 3.5u sizing. Picks in the 1.5-2.5 zone tagged DIV_EXPANDED.
        'ml_scale': 2.2,
    },
    'soccer_epl': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.40,  # v12 FIX: Was 0.25 — massively undervaluing home teams, inflating away ML edges
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0 — too flat, underdogs kept ~50% win prob. Backtest: ML dogs 13W-34L (-8.0u). Steeper curve = realistic dog probs.
    },
    'soccer_italy_serie_a': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.45,  # v12 FIX: Was 0.30 — Serie A has strong home advantage historically
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_spain_la_liga': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.40,  # v12 FIX: Was 0.25
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_germany_bundesliga': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.42,  # v12 FIX: Was 0.30 — Bundesliga has strong home support
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_france_ligue_one': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.40,  # v12 FIX: Was 0.25
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_uefa_champs_league': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.30,  # v12 FIX: Was 0.20 — UCL lower but still real
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_usa_mls': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.45,  # v12 FIX: Was 0.35 — MLS travel distances = big home edge
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_mexico_ligamx': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.50,  # Liga MX has strong home advantage (altitude, travel)
        'max_spread_divergence': 1.0,
        'ml_scale': 1.5,
    },
    # ── BASEBALL ──
    # Calibrated from Elo MAE = 6.2 runs (spread_std must exceed this)
    # NBA ratio: MAE/std = 4/11 = 0.36. Baseball: 6.2/10 = 0.62 (conservative)
    # Run lines are ±1.5 (variable juice). Primary value = MONEYLINES.
    'baseball_ncaa': {
        'logistic_scale': 1.8,    # Baseball: tighter scale, fewer blowouts than basketball
        'spread_std': 10.0,       # Calibrated: Elo MAE=6.2, match NBA conservatism ratio
        'home_court': 0.4,        # v14: Actual home win rate 65.6% — Elo home_advantage handles this
        'max_spread_divergence': 5.0,  # Was 2.0 — baseball totals at high div are 21W-10L +40u. Model's best sport needs room.
        'ml_scale': 3.5,          # v12 FIX: Was 2.2 (way too steep). Real baseball: 1 run diff = ~57% win
    },
    # v17: MLB — Opening Day 2026-03-26. Same structure as NCAA baseball
    # with tighter settings (pro markets are sharper than college).
    'baseball_mlb': {
        'logistic_scale': 1.8,    # Same as NCAA — baseball scoring distribution is similar
        'spread_std': 8.0,        # Tighter than NCAA (10.0) — pro lines are sharper
        'home_court': 0.3,        # MLB home advantage ~54% (weaker than college 65%)
        'max_spread_divergence': 4.0,  # Tighter than NCAA (5.0) — pro market more efficient
        'ml_scale': 3.5,          # Same as NCAA
    },
}

# Dynamically add tennis tournament configs from config.py
try:
    from config import TENNIS_SPORTS, TENNIS_SURFACES
    _TENNIS_PARAMS = {
        'hard': {'logistic_scale': 2.5, 'spread_std': 5.0, 'home_court': 0.0,
                 'max_spread_divergence': 4.0, 'ml_scale': 2.5},
        'clay': {'logistic_scale': 2.5, 'spread_std': 5.5, 'home_court': 0.0,
                 'max_spread_divergence': 2.5, 'ml_scale': 2.5},  # v24: tightened from 4.5
        'grass': {'logistic_scale': 2.5, 'spread_std': 4.5, 'home_court': 0.0,
                  'max_spread_divergence': 3.5, 'ml_scale': 2.5},
    }
    for _tk in TENNIS_SPORTS:
        _surf = TENNIS_SURFACES.get(_tk, 'hard')
        SPORT_CONFIG[_tk] = dict(_TENNIS_PARAMS.get(_surf, _TENNIS_PARAMS['hard']))
except ImportError:
    pass

def spread_to_win_prob(spread, sport):
    """Win probability for MONEYLINE bets. Uses ml_scale (calibrated to real win rates)."""
    s = SPORT_CONFIG.get(sport, SPORT_CONFIG['basketball_nba']).get('ml_scale', 7.5)
    return 1.0 / (1.0 + math.exp(spread / s))

def spread_to_cover_prob(model_spread, market_spread, sport):
    """Cover probability for SPREAD bets. Uses spread_std."""
    std = SPORT_CONFIG.get(sport, SPORT_CONFIG['basketball_nba'])['spread_std']
    return _ncdf((market_spread - model_spread) / std)

def _ncdf(z):
    if z > 6: return 1.0
    if z < -6: return 0.0
    a1,a2,a3,a4,a5 = 0.254829592,-0.284496736,1.421413741,-1.453152027,1.061405429
    p = 0.3275911; sign = 1 if z >= 0 else -1
    t = 1.0/(1.0+p*abs(z))
    y = 1.0-(((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*math.exp(-z*z/2)
    return 0.5*(1.0+sign*y)

def american_to_implied_prob(odds):
    if odds is None: return None
    return 100.0/(odds+100.0) if odds > 0 else abs(odds)/(abs(odds)+100.0)


def devig_ml_odds(home_odds, away_odds, draw_odds=None):
    """
    Remove vig from ML odds to get fair probabilities.
    
    Raw implied probs sum to >100% (the vig). De-vigging normalizes
    them to 100% so edge comparisons are against true market probability.
    
    Without this, dogs always look like value because vig inflates
    favorites more in absolute terms.
    
    Returns: (home_fair, away_fair, draw_fair) or (home_fair, away_fair, None)
    """
    h_imp = american_to_implied_prob(home_odds)
    a_imp = american_to_implied_prob(away_odds)
    d_imp = american_to_implied_prob(draw_odds) if draw_odds else None
    
    if h_imp is None or a_imp is None:
        return None, None, None
    
    total = h_imp + a_imp + (d_imp or 0)
    if total <= 0:
        return h_imp, a_imp, d_imp
    
    h_fair = h_imp / total
    a_fair = a_imp / total
    d_fair = d_imp / total if d_imp else None
    
    return h_fair, a_fair, d_fair

def _tennis_surface_from_sport(sport_key):
    """Infer tennis surface from a sport/tournament key."""
    _sp_lower = sport_key.lower()
    _CLAY = ['french_open', 'roland_garros', 'monte_carlo', 'madrid',
             'italian_open', 'rome', 'barcelona', 'hamburg', 'rio',
             'buenos_aires', 'lyon', 'bastad', 'kitzbuhel', 'umag',
             'gstaad', 'geneva', 'marrakech', 'bucharest', 'parma',
             'palermo', 'prague', 'rabat', 'strasbourg', 'lausanne',
             'portoroz', 'bogota', 'istanbul', 'budapest',
             'chile_open', 'argentina_open', 'tiriac', 'hassan',
             'clay_court', 'open_occitanie']
    _GRASS = ['wimbledon', 'queens', 'halle', 'eastbourne', 'berlin',
              'bad_homburg', 'nottingham', 'mallorca', 's_hertogenbosch',
              'birmingham', 'libema']
    if any(kw in _sp_lower for kw in _CLAY):
        return 'clay'
    elif any(kw in _sp_lower for kw in _GRASS):
        return 'grass'
    else:
        return 'hard'


def _tennis_h2h_adjustment(conn, player1, player2, sport):
    """
    Calculate head-to-head adjustment for tennis matchups.

    Checks historical results between two players. If one player dominates
    the H2H record (65%+ win rate over 3+ matches), applies a spread
    adjustment toward the dominant player.

    Also checks surface-specific H2H when possible (e.g., clay-only record
    may differ from hard-court record).

    Args:
        conn: SQLite connection
        player1: Home player name (from odds)
        player2: Away player name (from odds)
        sport: Sport key (e.g., 'tennis_atp_miami_open')

    Returns:
        (adjustment, context_string)
        adjustment: spread adjustment in games (negative = favors player1/home)
        context_string: human-readable summary, empty if no significant H2H
    """
    if conn is None or not sport.startswith('tennis_'):
        return 0.0, ""

    # Query all completed tennis matches between these two players.
    # Use LIKE for fuzzy matching since names may have slight variations
    # across different tournament entries.
    try:
        rows = conn.execute("""
            SELECT home, away, home_score, away_score, winner, sport
            FROM results
            WHERE sport LIKE 'tennis%' AND completed = 1
            AND ((home = ? AND away = ?)
                 OR (home = ? AND away = ?))
        """, (player1, player2, player2, player1)).fetchall()
    except Exception:
        return 0.0, ""

    if len(rows) < 3:
        return 0.0, ""

    # Determine surface of the current match
    current_surface = _tennis_surface_from_sport(sport)

    # Count overall H2H and surface-specific H2H
    p1_wins_all = 0
    p2_wins_all = 0
    p1_wins_surface = 0
    p2_wins_surface = 0
    surface_matches = 0

    for home, away, h_score, a_score, winner, r_sport in rows:
        # Determine winner
        if winner == player1:
            p1_wins_all += 1
        elif winner == player2:
            p2_wins_all += 1
        elif h_score is not None and a_score is not None:
            # Fallback: use scores if winner field is missing
            if (home == player1 and h_score > a_score) or (away == player1 and a_score > h_score):
                p1_wins_all += 1
            else:
                p2_wins_all += 1
        else:
            continue

        # Check if this match was on the same surface
        match_surface = _tennis_surface_from_sport(r_sport)
        if match_surface == current_surface:
            surface_matches += 1
            if winner == player1:
                p1_wins_surface += 1
            elif winner == player2:
                p2_wins_surface += 1
            elif (home == player1 and (h_score or 0) > (a_score or 0)) or \
                 (away == player1 and (a_score or 0) > (h_score or 0)):
                p1_wins_surface += 1
            else:
                p2_wins_surface += 1

    total_all = p1_wins_all + p2_wins_all
    if total_all < 3:
        return 0.0, ""

    # Prefer surface-specific H2H if 3+ matches on same surface
    if surface_matches >= 3:
        total = p1_wins_surface + p2_wins_surface
        p1_wins = p1_wins_surface
        p2_wins = p2_wins_surface
        surface_label = f" on {current_surface}"
    else:
        total = total_all
        p1_wins = p1_wins_all
        p2_wins = p2_wins_all
        surface_label = ""

    if total < 3:
        return 0.0, ""

    # Check for dominance (65%+ win rate)
    p1_pct = p1_wins / total
    p2_pct = p2_wins / total

    if max(p1_pct, p2_pct) < 0.65:
        return 0.0, ""

    # Calculate adjustment
    # dominance_factor ranges from 0.0 (50%) to 1.0 (100%)
    # adjustment = dominance_factor * 1.5 games, capped at 2.0
    if p1_pct > p2_pct:
        dominant, dominated = player1, player2
        dom_wins, dom_losses = p1_wins, p2_wins
        dominance = (p1_pct - 0.5) * 2.0
        # Negative adjustment = favors home (player1)
        raw_adj = -dominance * 1.5
    else:
        dominant, dominated = player2, player1
        dom_wins, dom_losses = p2_wins, p1_wins
        dominance = (p2_pct - 0.5) * 2.0
        # Positive adjustment = favors away (player2)
        raw_adj = dominance * 1.5

    # Cap at +/- 2.0 games
    adj = max(-2.0, min(2.0, round(raw_adj, 2)))

    # Short names for context (last name only)
    def _short(name):
        parts = name.split()
        return parts[-1] if parts else name

    ctx = f"H2H: {_short(dominant)} leads {_short(dominated)} {dom_wins}-{dom_losses}{surface_label} ({adj:+.1f})"
    return adj, ctx


def get_latest_ratings(conn, sport):
    rows = conn.execute("""
        SELECT team, base_rating, home_court, rest_adjust, injury_adjust,
               situational_adjust, manual_override, final_rating
        FROM power_ratings WHERE sport = ? ORDER BY run_timestamp DESC
    """, (sport,)).fetchall()
    ratings, seen = {}, set()
    for r in rows:
        t = r[0]
        if t in seen: continue
        seen.add(t)
        if r[1] is None: continue
        hca = r[2] or SPORT_CONFIG.get(sport, {}).get('home_court', 2.5)
        override = r[6]
        final = override if override is not None else (r[1] + (r[3] or 0) + (r[4] or 0) + (r[5] or 0))
        ratings[t] = {'base': r[1], 'home_court': hca, 'final': final}
    return ratings

def compute_model_spread(home, away, ratings, sport):
    h, a = ratings.get(home), ratings.get(away)
    if not h or not a: return None
    hca = h.get('home_court', SPORT_CONFIG.get(sport, {}).get('home_court', 2.5))
    return round(a['final'] - h['final'] - hca, 2)

def _soccer_draw_prob(abs_spread):
    """Estimate draw probability in soccer based on model spread.
    
    v12 FIX: Old decay (0.55) made draws crash from 30% to 14% at spread 1.0.
    Real data: draws stay ~20% even in moderately lopsided matches.
    Calibrated against EPL 2020-2024:
      Even: ~26%, Sm fav: ~24%, Med fav: ~19%, Big fav: ~15%
    """
    return 0.30 * math.exp(-0.30 * abs_spread)


def soccer_ml_probs(model_spread, sport):
    """
    Calculate 3-way soccer probabilities: (home_win, draw, away_win).
    
    CRITICAL: Soccer has 3 outcomes, not 2. Without this adjustment,
    the model overestimates underdog win probability by 15-25% because
    draw probability gets assigned to the underdog instead.
    
    Example: Inter Milan vs Genoa (spread -1.5)
      Without draw fix: Home=65%, Away=35% → Genoa looks like great value at +1100
      With draw fix:    Home=71%, Draw=13%, Away=16% → Genoa correctly filtered out
    """
    raw_home = spread_to_win_prob(model_spread, sport)
    draw = _soccer_draw_prob(abs(model_spread))
    home_win = raw_home * (1.0 - draw)
    away_win = (1.0 - raw_home) * (1.0 - draw)
    return home_win, draw, away_win


def get_team_injury_context(conn, team, sport):
    """Return injury context: (injury_count, position_cluster_count, total_point_impact).

    v17 FIX: Now returns actual point_impact sum instead of discarding it.
    Previously returned (len, min(len,3)) — treating Cade Cunningham out
    the same as a 12th man. Now the impact values drive spread adjustment.

    Status weighting:
      Out/Doubtful: full impact
      Day-To-Day/Questionable: 50% impact (may play)
    """
    today = datetime.now().strftime('%Y-%m-%d')
    rows = conn.execute("""
        SELECT player, status, point_impact FROM injuries
        WHERE sport=? AND team=? AND report_date=?
        AND status IN ('Out','OUT','Doubtful','DOUBTFUL','Day-To-Day','DAY-TO-DAY',
                       'Questionable','QUESTIONABLE')
    """, (sport, team, today)).fetchall()

    total_impact = 0.0
    for player, status, impact in rows:
        pts = impact or 0.0
        if status.upper() in ('DAY-TO-DAY', 'QUESTIONABLE'):
            pts *= 0.5  # May play — discount impact
        total_impact += pts

    return len(rows), min(len(rows), 3), round(total_impact, 2)

# ── Totals model ──

LEAGUE_AVG_TOTAL = {
    'basketball_ncaab': 145.0, 'basketball_nba': 228.0,  # Fallbacks only — see dynamic calc below
    'icehockey_nhl': 6.0,
    'soccer_epl': 2.65, 'soccer_italy_serie_a': 2.50,
    'soccer_spain_la_liga': 2.55,
    'soccer_germany_bundesliga': 3.10,  # Bundesliga averages higher scoring
    'soccer_france_ligue_one': 2.60,
    'soccer_uefa_champs_league': 2.80,
    'soccer_usa_mls': 2.85,
    'soccer_mexico_ligamx': 2.70,
    'baseball_ncaa': 13.0,  # v14: Actual data avg=13.0 (was 11.5). Metal bats + college pitching depth
    'baseball_mlb': 8.5,   # v17: MLB avg ~8.5 runs/game (lower than college — wood bats, better pitching)
}

TOTAL_STD = {
    # These reflect MODEL UNCERTAINTY, not game variance.
    # The model uses crude team averages to estimate totals.
    # Higher STD = more conservative = fewer false positives.
    'basketball_ncaab': 22.0, 'basketball_nba': 20.0,   # Was 12.0 — produced 30% edges on 10pt gaps
    'icehockey_nhl': 2.2,                                 # v12 FIX: Was 1.8. 0.5 goal disagreement was producing 8.6% edge. At 2.2, need 1.0+ goal disagreement for playable edge. Prevents systematic under flooding.
    'soccer_epl': 1.8, 'soccer_italy_serie_a': 1.8,      # v13 FIX: Was 1.5 — backtest 15W-15L coinflip, MLS 1W-8L. Raise bar to require 0.5+ goal deviation.
    'soccer_spain_la_liga': 1.8,
    'soccer_germany_bundesliga': 1.8,
    'soccer_france_ligue_one': 1.8,
    'soccer_uefa_champs_league': 1.8,
    'soccer_usa_mls': 5.0,  # v13: Was 1.8 — backtest 1W-7L (-72.8% ROI). Zero signal. Effectively disabled.
    'soccer_mexico_ligamx': 1.8,
    'baseball_ncaa': 3.5,  # v14: Was 5.0 (too conservative). Backtest: 50W-35L +30.7u +15.3% ROI at 5.0. Lower to let more signal through.
    'baseball_mlb': 4.0,   # v17: Tighter than NCAA (3.5) — pro lines sharper, need larger disagreement
}


def _get_dynamic_league_avg_total(conn, sport):
    """
    Get the REAL average total from market consensus, not a hardcoded guess.
    This fixes the NCAAB under-bias: if markets average 155, we use 155, not 145.
    """
    row = conn.execute("""
        SELECT AVG(best_over_total), COUNT(*)
        FROM market_consensus
        WHERE sport=? AND best_over_total IS NOT NULL
        AND best_over_total > 0
    """, (sport,)).fetchone()

    if row and row[0] and row[1] >= 10:
        return round(row[0], 1)
    return LEAGUE_AVG_TOTAL.get(sport, 145.0)

def _weighted_team_stats(conn, team, sport, elo_ratings=None, min_games=5):
    """
    v12.3: Recency-weighted, home/away split, opponent-adjusted team stats.

    Returns dict with offense/defense averages (overall and home/away splits).
    Last 10 games get 2x weight vs earlier games.
    If elo_ratings provided, adjusts for opponent quality.
    """
    rows = conn.execute("""
        SELECT home, away, home_score, away_score, commence_time
        FROM results
        WHERE (home=? OR away=?) AND sport=? AND completed=1
        AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY commence_time DESC
    """, (team, team, sport)).fetchall()

    if len(rows) < min_games:
        return None

    # Accumulators: overall and home/away splits
    off_sum, def_sum, off_w = 0.0, 0.0, 0.0
    h_off_sum, h_def_sum, h_w = 0.0, 0.0, 0.0  # home splits
    a_off_sum, a_def_sum, a_w = 0.0, 0.0, 0.0  # away splits
    opp_elos = []

    for i, (home_tm, away_tm, hs, as_, ct) in enumerate(rows):
        weight = 2.0 if i < 10 else 1.0  # Last 10 games get 2x weight
        is_home = (home_tm == team)
        offense = hs if is_home else as_
        defense = as_ if is_home else hs
        opponent = away_tm if is_home else home_tm

        # Overall
        off_sum += offense * weight
        def_sum += defense * weight
        off_w += weight

        # Home/away splits
        if is_home:
            h_off_sum += offense * weight
            h_def_sum += defense * weight
            h_w += weight
        else:
            a_off_sum += offense * weight
            a_def_sum += defense * weight
            a_w += weight

        # Opponent quality
        if elo_ratings and opponent in elo_ratings:
            opp_elos.append(elo_ratings[opponent].get('elo', 1500))

    overall_off = off_sum / off_w if off_w > 0 else None
    overall_def = def_sum / off_w if off_w > 0 else None

    # Use splits if enough games (4+), otherwise fall back to overall
    home_off = h_off_sum / h_w if h_w >= 4 else overall_off
    home_def = h_def_sum / h_w if h_w >= 4 else overall_def
    away_off = a_off_sum / a_w if a_w >= 4 else overall_off
    away_def = a_def_sum / a_w if a_w >= 4 else overall_def

    # v12.3: Opponent quality adjustment using Elo
    # If team scored 110 avg against weak opponents (avg Elo 1420),
    # that's less impressive than 110 against strong opponents (1580).
    elo_adj = 1.0
    if opp_elos and len(opp_elos) >= 5:
        avg_opp = sum(opp_elos) / len(opp_elos)
        # Gentle adjustment: 100 Elo points of weak schedule = ~2% offense reduction
        elo_adj = 1.0 + (avg_opp - 1500) / 5000  # ~±2% per 100 Elo points

    return {
        'offense': overall_off, 'defense': overall_def,
        'home_offense': home_off, 'home_defense': home_def,
        'away_offense': away_off, 'away_defense': away_def,
        'games': len(rows), 'elo_adj': elo_adj,
    }


def _mlb_pitcher_era_adjustment(conn, mlb_pitcher_info):
    """
    Adjust MLB total based on probable starter ERA vs league average.

    Concept: MLB average ERA ~4.00. Pitchers better than average suppress
    scoring (total goes down); worse than average inflate scoring (total up).

    Formula per pitcher:
        pitcher_deviation = (ERA - 4.00) / 4.00   (% above/below average)
        run_adj = pitcher_deviation * 1.5          (scaled to runs impact)

    Total adj = home_pitcher_adj + away_pitcher_adj, capped at +/-2.0 runs.

    v25.3: ALSO returns best_era and worst_era so the pitching gate can
    catch ASYMMETRIC matchups (one elite + one bad pitcher) where the
    sum cancels to ~0 but the elite pitcher should still hard-veto OVERs.
    Brewers/Nats 4/10: Patrick 3.27 vs Irvin 5.00 → sum +0.1 → no veto under
    old logic. New logic: best_era=3.27 → veto over.

    Returns (adjustment, context_string, best_era, worst_era, both_reliable) tuple.
    best_era/worst_era are None if no data.
    both_reliable: True if both starters have a confirmed ERA source.
    """
    LEAGUE_AVG_ERA = 4.00
    SCALE_FACTOR = 1.5  # Each 1.0 ERA above avg -> ~0.375 more runs allowed
    MAX_ADJ = 2.0

    if not mlb_pitcher_info:
        return 0.0, '', None, None, False

    home_pitcher = mlb_pitcher_info.get('home_pitcher')
    away_pitcher = mlb_pitcher_info.get('away_pitcher')

    # Get ERA for each pitcher: prefer box_scores season ERA, fall back to ESPN ERA
    # v25.14: Opener detection — if avg IP/appearance < 3.0, pitcher is a
    # bulk opener or reliever (e.g. Grant Taylor 1.0 IP avg). Their low ERA
    # reflects 1-inning work, not starter quality. Fall through to ESPN gate
    # (which requires 30+ IP) or return None → league average adjustment.
    def _get_best_era(pitcher_name, espn_era):
        """Get best available ERA: box_scores first, then ESPN."""
        if pitcher_name:
            try:
                row = conn.execute("""
                    SELECT ROUND(
                        SUM(CASE WHEN stat_type='pitcher_er' THEN stat_value ELSE 0 END) * 9.0 /
                        NULLIF(SUM(CASE WHEN stat_type='pitcher_ip' THEN stat_value ELSE 0 END), 0)
                    , 2) as era,
                    SUM(CASE WHEN stat_type='pitcher_ip' THEN stat_value ELSE 0 END) as total_ip,
                    COUNT(DISTINCT game_date) as appearances
                    FROM box_scores
                    WHERE sport='baseball_mlb'
                    AND player LIKE ?
                    AND stat_type IN ('pitcher_er', 'pitcher_ip')
                """, (f"%{pitcher_name}%",)).fetchone()
                if row and row[0] is not None and row[1] and row[1] >= 10:
                    # Opener check: avg IP per appearance < 3.0 = reliever/opener
                    avg_ip = row[1] / row[2] if row[2] and row[2] > 0 else 0
                    if avg_ip < 3.0:
                        print(f"  ⚠ {pitcher_name}: avg {avg_ip:.1f} IP/app ({row[1]:.0f} IP / {row[2]} games) — opener, using league avg")
                        return None  # Fall through to league average
                    return row[0]  # Enough IP for reliable ERA
            except Exception:
                pass
        # Fall back to ESPN ERA — but only with enough sample size
        # Early season ERA is noise. Need 30+ IP (5-6 quality starts) minimum
        # to trust the number. Below that, treat as league average.
        side = 'home' if pitcher_name == home_pitcher else 'away'
        season_ip = mlb_pitcher_info.get(f"{side}_season_ip", 0)
        if espn_era is not None and season_ip and season_ip >= 30:
            return espn_era
        # Not enough data — use league average (no adjustment for this pitcher)
        return None

    home_era = _get_best_era(home_pitcher, mlb_pitcher_info.get('home_era'))
    away_era = _get_best_era(away_pitcher, mlb_pitcher_info.get('away_era'))

    # Calculate adjustments (use league avg if no ERA available = zero adj for that side)
    h_era = home_era if home_era is not None else LEAGUE_AVG_ERA
    a_era = away_era if away_era is not None else LEAGUE_AVG_ERA

    home_dev = (h_era - LEAGUE_AVG_ERA) / LEAGUE_AVG_ERA
    away_dev = (a_era - LEAGUE_AVG_ERA) / LEAGUE_AVG_ERA

    home_run_adj = home_dev * SCALE_FACTOR
    away_run_adj = away_dev * SCALE_FACTOR

    total_adj = home_run_adj + away_run_adj
    total_adj = max(-MAX_ADJ, min(MAX_ADJ, total_adj))
    total_adj = round(total_adj, 2)

    # Build context string
    ctx_parts = []
    if home_pitcher and home_era is not None:
        ctx_parts.append(f"{home_pitcher} {home_era:.2f}")
    elif home_pitcher:
        ctx_parts.append(f"{home_pitcher} ?.??")
    if away_pitcher and away_era is not None:
        ctx_parts.append(f"{away_pitcher} {away_era:.2f}")
    elif away_pitcher:
        ctx_parts.append(f"{away_pitcher} ?.??")

    if ctx_parts and total_adj != 0:
        ctx_str = f"Pitching: {' vs '.join(ctx_parts)} ({total_adj:+.1f})"
    elif ctx_parts:
        ctx_str = f"Pitching: {' vs '.join(ctx_parts)} (avg)"
    else:
        ctx_str = ''

    # v25.3: best_era / worst_era for asymmetric pitching gate
    eras = [e for e in (home_era, away_era) if e is not None]
    best_era = min(eras) if eras else None
    worst_era = max(eras) if eras else None

    # v25.14: both_reliable = True only if BOTH starters have confirmed ERA
    both_reliable = (home_era is not None and away_era is not None)

    return total_adj, ctx_str, best_era, worst_era, both_reliable


def _log_park_veto(conn, sport, event_id, selection, park_adj, park_ctx):
    """Log a pick vetoed by park gate to shadow_blocked_picks for tracking."""
    try:
        conn.execute("""
            INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                market_type, line, odds, edge_pct, units, reason)
            VALUES (?, ?, ?, ?, 'TOTAL', NULL, NULL, NULL, NULL, ?)
        """, (datetime.now().isoformat(), sport, event_id, selection,
              f"PARK_GATE ({park_ctx})"))
        conn.commit()
    except Exception:
        pass


def _log_divergence_block(conn, sport, event_id, home, away, model_spread, market_spread, reason_detail):
    """Log a pick blocked by max_spread_divergence to shadow_blocked_picks.

    Divergence blocks fire BEFORE we know which bet type would have been generated,
    so 'selection' just records the matchup. The reason_detail explains which of
    the 3 div paths fired (insufficient_elo / post_elo_rescue / ml_only_implied).
    """
    try:
        div = abs(model_spread - market_spread) if (model_spread is not None and market_spread is not None) else None
        div_str = f"{div:.1f}" if div is not None else "?"
        ms_str = f"{model_spread:+.1f}" if model_spread is not None else "?"
        msp_str = f"{market_spread:+.1f}" if market_spread is not None else "?"
        conn.execute("""
            INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                market_type, line, odds, edge_pct, units, reason)
            VALUES (?, ?, ?, ?, 'SPREAD', NULL, NULL, NULL, NULL, ?)
        """, (datetime.now().isoformat(), sport, event_id, f"{home} vs {away}",
              f"DIVERGENCE_GATE ({reason_detail}, div={div_str}, ms={ms_str}, mkt_sp={msp_str})"))
        conn.commit()
    except Exception:
        pass


# ═══ MLB PARK FACTOR MAPPING ═══
MLB_PARK_NAMES = {
    'Arizona Diamondbacks': 'Chase Field',
    'Athletics': 'Sacramento (Sutter Health Park)',
    'Atlanta Braves': 'Truist Park',
    'Baltimore Orioles': 'Camden Yards',
    'Boston Red Sox': 'Fenway Park',
    'Chicago Cubs': 'Wrigley Field',
    'Chicago White Sox': 'Guaranteed Rate Field',
    'Cincinnati Reds': 'Great American Ball Park',
    'Cleveland Guardians': 'Progressive Field',
    'Colorado Rockies': 'Coors Field',
    'Detroit Tigers': 'Comerica Park',
    'Houston Astros': 'Minute Maid Park',
    'Kansas City Royals': 'Kauffman Stadium',
    'Los Angeles Angels': 'Angel Stadium',
    'Los Angeles Dodgers': 'Dodger Stadium',
    'Miami Marlins': 'LoanDepot Park',
    'Milwaukee Brewers': 'American Family Field',
    'Minnesota Twins': 'Target Field',
    'New York Mets': 'Citi Field',
    'New York Yankees': 'Yankee Stadium',
    'Philadelphia Phillies': 'Citizens Bank Park',
    'Pittsburgh Pirates': 'PNC Park',
    'San Diego Padres': 'Petco Park',
    'San Francisco Giants': 'Oracle Park',
    'Seattle Mariners': 'T-Mobile Park',
    'St. Louis Cardinals': 'Busch Stadium',
    'Tampa Bay Rays': 'Tropicana Field',
    'Texas Rangers': 'Globe Life Field',
    'Toronto Blue Jays': 'Rogers Centre',
    'Washington Nationals': 'Nationals Park',
}


def _mlb_park_factor_adjustment(conn, home_team, away_team=None, side=None):
    """
    Adjust MLB total based on historical park scoring vs league average.

    The market already partially prices park effects (everyone knows Coors
    is a hitter's park), so we divide the raw park deviation by 2 to capture
    only the RESIDUAL edge the market may not fully account for.

    v23.2: Park factor decays by 50% for each consecutive day we've already
    bet the same matchup+direction. Prevents park from being the sole driver
    on series repeats (e.g., Coors OVER firing 3 days straight).

    Formula:
        park_avg = average actual_total for games at this home team's park
        league_avg = average actual_total across all MLB games
        adjustment = (park_avg - league_avg) / 2, capped at +/- 1.5 runs

    Requires 30+ home games for reliable park factor.

    Returns (adjustment, context_string) or (0.0, '') if insufficient data.
    """
    # v24: Park factor used as GATE only (not edge generator).
    # Data: park-as-edge was 3W-6L -16.1u (market already prices parks).
    # Now: park confirms or vetoes picks but never inflates the model total.
    # Returns the raw adjustment for gate logic, tagged as gate-only.
    MAX_ADJ = 1.0
    MIN_GAMES = 30
    MARKET_DIVISOR = 3

    try:
        # Park average for this home team
        row = conn.execute("""
            SELECT COUNT(*), AVG(actual_total)
            FROM results
            WHERE sport = 'baseball_mlb'
              AND home = ?
              AND actual_total IS NOT NULL
        """, (home_team,)).fetchone()

        if not row or row[0] < MIN_GAMES or row[1] is None:
            return 0.0, '', 0.0

        park_games = row[0]
        park_avg = row[1]

        # League average across all MLB games
        league_row = conn.execute("""
            SELECT AVG(actual_total)
            FROM results
            WHERE sport = 'baseball_mlb'
              AND actual_total IS NOT NULL
        """).fetchone()

        if not league_row or league_row[0] is None:
            return 0.0, '', 0.0

        league_avg = league_row[0]

        # Calculate adjustment: halved because market already partially prices parks
        raw_dev = park_avg - league_avg
        adj = raw_dev / MARKET_DIVISOR
        adj = max(-MAX_ADJ, min(MAX_ADJ, adj))
        adj = round(adj, 2)

        if adj == 0.0:
            return 0.0, '', 0.0

        # v23.2: Decay park factor for consecutive-day same-matchup bets.
        # If we already bet this matchup's total yesterday, the park factor
        # was already the driver — decay it so the pick needs pitching/weather
        # to stand on its own. 50% decay per consecutive day.
        decay = 1.0
        if away_team:
            from datetime import datetime, timedelta
            try:
                today = datetime.now().strftime('%Y-%m-%d')
                lookback = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
                # Count distinct prior days we bet this matchup total (either direction)
                matchup_pattern = f'%{away_team}%{home_team}%'
                prior_bets = conn.execute("""
                    SELECT DISTINCT DATE(created_at) FROM bets
                    WHERE sport = 'baseball_mlb'
                      AND market_type = 'TOTAL'
                      AND selection LIKE ?
                      AND DATE(created_at) < ? AND DATE(created_at) >= ?
                      AND (result IS NULL OR result NOT IN ('TAINTED','DUPLICATE'))
                """, (matchup_pattern, today, lookback)).fetchall()
                consec_days = len(prior_bets)
                if consec_days > 0:
                    decay = 0.5 ** consec_days
            except Exception:
                pass

        adj = round(adj * decay, 2)
        if adj == 0.0:
            return 0.0, '', 0.0

        # Build context string with park name
        park_name = MLB_PARK_NAMES.get(home_team, home_team)
        decay_note = f' decay={decay:.0%}' if decay < 1.0 else ''

        # v24: Park is gate-only — never added to model_total.
        # Return 0 for model adjustment, but include raw adj in context
        # so the gate logic downstream can use it.
        ctx = f"Park: {park_name} ({adj:+.1f}{decay_note})"
        # Return tuple: (0 for model, context string, raw adj for gate)
        return 0.0, ctx, adj

    except Exception:
        return 0.0, '', 0.0


def _mlb_bullpen_adjustment(conn, home_team, away_team):
    """
    Adjust MLB total based on aggregate bullpen ERA vs league average.

    Starters pitch ~5-6 IP; the bullpen handles the last 3-4 innings (~40%).
    A dominant bullpen (sub-3.00 ERA) suppresses scoring; a bad bullpen
    (4.50+ ERA) inflates it.

    Relievers are identified as pitchers with avg IP < 4.0 across their
    appearances (starters average 5-6 IP, relievers average 1-2 IP).

    Formula:
        combined_deviation = ((home_bp_era - 3.80) + (away_bp_era - 3.80)) / 2
        adjustment = combined_deviation * 0.4  (bullpen pitches ~40% of innings)
        Capped at +/- 0.8 runs.

    Requires 30+ total reliever IP per team for reliable data.

    Returns (adjustment, context_string) or (0.0, '') if insufficient data.
    """
    LEAGUE_AVG_BP_ERA = 3.80
    SCALE_FACTOR = 0.4   # Bullpen pitches ~40% of innings
    MAX_ADJ = 0.8
    MIN_IP = 30           # Minimum total reliever IP per team

    def _team_bullpen_era(team):
        """Calculate aggregate bullpen ERA for a team from box_scores."""
        try:
            rows = conn.execute("""
                SELECT player,
                       AVG(CASE WHEN stat_type='pitcher_ip' THEN stat_value END) as avg_ip,
                       SUM(CASE WHEN stat_type='pitcher_er' THEN stat_value END) as total_er,
                       SUM(CASE WHEN stat_type='pitcher_ip' THEN stat_value END) as total_ip
                FROM box_scores
                WHERE sport='baseball_mlb' AND team LIKE ?
                AND stat_type IN ('pitcher_er', 'pitcher_ip')
                GROUP BY player
                HAVING avg_ip < 4.0 AND total_ip >= 5
            """, (f"%{team}%",)).fetchall()

            if not rows:
                return None, 0

            total_er = sum(r[2] for r in rows if r[2] is not None)
            total_ip = sum(r[3] for r in rows if r[3] is not None)

            if total_ip < MIN_IP:
                return None, total_ip

            bp_era = round((total_er * 9.0) / total_ip, 2)
            return bp_era, total_ip
        except Exception:
            return None, 0

    try:
        home_bp_era, home_ip = _team_bullpen_era(home_team)
        away_bp_era, away_ip = _team_bullpen_era(away_team)

        if home_bp_era is None or away_bp_era is None:
            return 0.0, ''

        # Combined deviation from league average
        combined_dev = ((home_bp_era - LEAGUE_AVG_BP_ERA) + (away_bp_era - LEAGUE_AVG_BP_ERA)) / 2.0
        adj = combined_dev * SCALE_FACTOR
        adj = max(-MAX_ADJ, min(MAX_ADJ, adj))
        adj = round(adj, 2)

        if adj == 0.0:
            return 0.0, ''

        # Short team names for context string
        home_short = home_team.split()[-1] if ' ' in home_team else home_team
        away_short = away_team.split()[-1] if ' ' in away_team else away_team
        ctx = f"Bullpen: {home_short} {home_bp_era:.2f} vs {away_short} {away_bp_era:.2f} ({adj:+.1f})"

        return adj, ctx

    except Exception:
        return 0.0, ''


def _nhl_goalie_adjustment(conn, nhl_goalie_info):
    """
    Adjust NHL total based on starting goalie GAA vs league average.

    NHL average GAA is ~2.80. Elite goalies suppress scoring (total down);
    bad goalies inflate scoring (total up).

    Formula per goalie:
        goalie_deviation = (GAA - 2.80) / 2.80   (% above/below average)
        goal_adj = goalie_deviation * 1.2         (scaled to goal impact)

    Total adj = home_goalie_adj + away_goalie_adj, capped at +/-1.0 goals.
    NHL totals are tighter than MLB, so the cap is lower.

    Returns (adjustment, context_string) or (0.0, '') if no data.
    """
    LEAGUE_AVG_GAA = 2.80
    SCALE_FACTOR = 1.2   # Each 1.0 GAA above avg -> ~0.43 more goals allowed
    MAX_ADJ = 1.0

    if not nhl_goalie_info:
        return 0.0, ''

    h_stats = nhl_goalie_info.get('home_goalie_stats')
    a_stats = nhl_goalie_info.get('away_goalie_stats')
    home_goalie = nhl_goalie_info.get('home_goalie', '')
    away_goalie = nhl_goalie_info.get('away_goalie', '')

    # Calculate adjustments
    # Home goalie's GAA affects how many goals the AWAY team scores
    # Away goalie's GAA affects how many goals the HOME team scores
    # v23: Use blended GAA (80% season + 20% last 10 days) if available
    h_gaa = h_stats.get('blended_gaa', h_stats['gaa']) if h_stats else LEAGUE_AVG_GAA
    a_gaa = a_stats.get('blended_gaa', a_stats['gaa']) if a_stats else LEAGUE_AVG_GAA

    home_dev = (h_gaa - LEAGUE_AVG_GAA) / LEAGUE_AVG_GAA
    away_dev = (a_gaa - LEAGUE_AVG_GAA) / LEAGUE_AVG_GAA

    home_goal_adj = home_dev * SCALE_FACTOR
    away_goal_adj = away_dev * SCALE_FACTOR

    total_adj = home_goal_adj + away_goal_adj
    total_adj = max(-MAX_ADJ, min(MAX_ADJ, total_adj))
    total_adj = round(total_adj, 2)

    # Build context string — show recent form if it diverges from season
    ctx_parts = []
    if home_goalie and h_stats:
        _hg_str = f"{home_goalie} {h_stats['gaa']:.2f}"
        if h_stats.get('recent_gaa') is not None and abs(h_stats['recent_gaa'] - h_stats['gaa']) >= 0.3:
            _hg_str += f" (recent {h_stats['recent_gaa']:.2f})"
        ctx_parts.append(_hg_str)
    elif home_goalie:
        ctx_parts.append(f"{home_goalie} ?.??")
    if away_goalie and a_stats:
        _ag_str = f"{away_goalie} {a_stats['gaa']:.2f}"
        if a_stats.get('recent_gaa') is not None and abs(a_stats['recent_gaa'] - a_stats['gaa']) >= 0.3:
            _ag_str += f" (recent {a_stats['recent_gaa']:.2f})"
        ctx_parts.append(_ag_str)
    elif away_goalie:
        ctx_parts.append(f"{away_goalie} ?.??")

    if ctx_parts and total_adj != 0:
        ctx_str = f"Goalies: {' vs '.join(ctx_parts)} ({total_adj:+.1f})"
    elif ctx_parts:
        ctx_str = f"Goalies: {' vs '.join(ctx_parts)} (avg)"
    else:
        ctx_str = ''

    return total_adj, ctx_str


def estimate_model_total(home, away, ratings, sport, conn):
    """
    Estimate game total from team scoring history.

    v12 fix: For soccer, uses ACTUAL scoring data (goals scored) from results
    table instead of market totals. This eliminates the circular bias where
    the model averaged market lines and compared them back to the market,
    systematically finding false under "edges."

    For basketball/hockey, blends team-specific market totals with league average.
    
    Returns model_total (float) or None.
    """
    h = ratings.get(home)
    a = ratings.get(away)
    if not h or not a:
        return None

    # ──────────────────────────────────────────────────────────
    # SOCCER: Anchor on MARKET total, adjust by team deviation
    # ──────────────────────────────────────────────────────────
    # The market is SMART about soccer totals. We should only
    # disagree when team-specific data shows they deviate from
    # what the market expects. The model starts at the market
    # line and adjusts based on:
    #   - Each team's goals scored vs league average (attack rate)
    #   - Each team's goals conceded vs league average (defense rate)
    #
    # This prevents the old bugs:
    #   v11: averaged market totals → always said under (circular)
    #   v12a: averaged actual goals → always said over (blunt)
    #   v12b: starts at market, only moves for real team deviations ✅
    
    if 'soccer' in sport:
        # INDEPENDENT soccer total — does NOT anchor on market total.
        # Uses team attack/defense rates to predict scoring from scratch,
        # then compares to market. Same philosophy as Elo spreads.
        #
        # Old approach anchored on market total and barely adjusted (±0.1),
        # producing zero disagreement. New approach builds prediction
        # independently, finding edges where scoring rates diverge from
        # the market's expectation.
        #
        # Method: Expected goals = (home_atk × away_def_leak) + (away_atk × home_def_leak)
        # normalized to league average. This captures matchup-specific scoring.

        min_games = 8

        # League average actual goals per game
        league_row = conn.execute("""
            SELECT AVG(actual_total), COUNT(*)
            FROM results
            WHERE sport = ? AND completed = 1 AND actual_total IS NOT NULL
        """, (sport,)).fetchone()
        league_avg = league_row[0] if league_row and league_row[0] and league_row[1] >= 20 else None

        if not league_avg:
            return None

        league_atk = league_avg / 2  # Average goals per team per game

        # v25.3 (Fix 3): Use LAST 12 games per team instead of all-time average.
        # Old logic averaged the entire season equally — Augsburg game from 6 months
        # ago weighted the same as last week's game. Augsburg's recent under trend
        # (5/8 of last 8 games under 2.5) was diluted by older high-scoring games.
        # Hoffenheim/Augsburg 4/10: model said 73% over but team last-8 averages
        # suggested ~50%. Recent form is the strongest soccer total signal.
        h_like = f'%{home}%'
        h_rows = conn.execute("""
            SELECT CASE WHEN home LIKE ? THEN home_score ELSE away_score END as scored,
                   CASE WHEN home LIKE ? THEN away_score ELSE home_score END as conceded
            FROM results
            WHERE (home LIKE ? OR away LIKE ?) AND sport = ? AND completed = 1
              AND home_score IS NOT NULL
            ORDER BY commence_time DESC LIMIT 12
        """, (h_like, h_like, h_like, h_like, sport)).fetchall()

        a_like = f'%{away}%'
        a_rows = conn.execute("""
            SELECT CASE WHEN home LIKE ? THEN home_score ELSE away_score END as scored,
                   CASE WHEN home LIKE ? THEN away_score ELSE home_score END as conceded
            FROM results
            WHERE (home LIKE ? OR away LIKE ?) AND sport = ? AND completed = 1
              AND home_score IS NOT NULL
            ORDER BY commence_time DESC LIMIT 12
        """, (a_like, a_like, a_like, a_like, sport)).fetchall()

        if len(h_rows) < min_games or len(a_rows) < min_games:
            return None

        h_atk = sum(r[0] for r in h_rows) / len(h_rows)
        h_def = sum(r[1] for r in h_rows) / len(h_rows)
        a_atk = sum(r[0] for r in a_rows) / len(a_rows)
        a_def = sum(r[1] for r in a_rows) / len(a_rows)

        if not h_atk or not a_atk or not h_def or not a_def:
            return None

        # Matchup-based expected goals:
        # Home team expected = home_atk_rate × (away_def_rate / league_avg_def)
        # This captures: a strong attack vs a leaky defense = more goals
        h_atk_ratio = h_atk / league_atk if league_atk > 0 else 1.0  # e.g., 1.4 = scores 40% above avg
        a_atk_ratio = a_atk / league_atk if league_atk > 0 else 1.0
        h_def_ratio = h_def / league_atk if league_atk > 0 else 1.0  # e.g., 1.2 = concedes 20% above avg
        a_def_ratio = a_def / league_atk if league_atk > 0 else 1.0

        # Expected home goals = league_avg_atk × home_atk_strength × away_def_weakness
        exp_home_goals = league_atk * h_atk_ratio * a_def_ratio
        # Expected away goals = league_avg_atk × away_atk_strength × home_def_weakness
        exp_away_goals = league_atk * a_atk_ratio * h_def_ratio

        independent_total = exp_home_goals + exp_away_goals

        # Blend: 60% independent model, 40% league average
        # (pure independent can be noisy with small samples)
        model_total = independent_total * 0.6 + league_avg * 0.4

        return round(model_total, 2)

    # ──────────────────────────────────────────────────────────
    # BASKETBALL / HOCKEY: Independent scoring prediction
    # ──────────────────────────────────────────────────────────
    # v14 FIX: Old method anchored on market total and applied small
    # adjustments — circular by design, barely disagreed with the market.
    # New approach builds prediction independently from actual scoring
    # data (same philosophy as soccer totals and Elo spreads), then
    # compares to market to find real edges.
    #
    # Method: matchup-based expected scoring using attack/defense ratios
    #   Home expected = league_avg_per_team × home_atk_ratio × away_def_ratio
    #   Away expected = league_avg_per_team × away_atk_ratio × home_def_ratio
    # Blend with league average to dampen noise from small samples.

    # League average actual total per game
    league_row = conn.execute("""
        SELECT AVG(actual_total), COUNT(*)
        FROM results
        WHERE sport=? AND completed=1 AND actual_total IS NOT NULL
    """, (sport,)).fetchone()
    league_avg = league_row[0] if league_row and league_row[0] and league_row[1] >= 20 else None

    if not league_avg:
        avg = _get_dynamic_league_avg_total(conn, sport)
        return round(avg, 1) if avg else None

    league_per_team = league_avg / 2  # Average points per team per game

    # Try precomputed team ratings first (exponential decay, Elo-adjusted)
    # Falls back to inline _weighted_team_stats if not available
    _use_precomputed = False
    try:
        from team_ratings_engine import get_team_ratings
        _tr = get_team_ratings(conn, sport)
        if _tr and home in _tr and away in _tr:
            h_r = _tr[home]
            a_r = _tr[away]
            if h_r.get('confidence') != 'LOW' and a_r.get('confidence') != 'LOW':
                h_atk_ratio = h_r['home_off']
                h_def_ratio = h_r['home_def']  # Note: home team's defense at home
                a_atk_ratio = a_r['away_off']
                a_def_ratio = a_r['away_def']  # Away team's defense on the road

                # v25.17: Adjust def_ratios for confirmed starter (MLB pitcher / NHL goalie).
                # Caps at ±40%, weighted 50% for MLB, 30% for NHL. Falls back to no
                # adjustment (multiplier 1.0) if starter data unavailable.
                if sport in ('baseball_mlb', 'icehockey_nhl'):
                    try:
                        from starter_adjust import get_starter_adjustment
                        h_mult, a_mult, _ = get_starter_adjustment(conn, sport, home, away)
                        h_def_ratio = h_def_ratio * h_mult
                        a_def_ratio = a_def_ratio * a_mult
                    except Exception:
                        pass

                _use_precomputed = True
    except Exception:
        pass

    if not _use_precomputed:
        # Fallback: inline computation
        elo_data = None
        try:
            from elo_engine import get_elo_ratings
            elo_data = get_elo_ratings(conn, sport)
        except Exception:
            pass

        h_stats = _weighted_team_stats(conn, home, sport, elo_ratings=elo_data)
        a_stats = _weighted_team_stats(conn, away, sport, elo_ratings=elo_data)

        if not h_stats or not a_stats:
            return None

        # Use HOME splits for home team, AWAY splits for away team
        h_off = h_stats['home_offense'] * h_stats['elo_adj']
        h_def = h_stats['home_defense'] / h_stats['elo_adj']
        a_off = a_stats['away_offense'] * a_stats['elo_adj']
        a_def = a_stats['away_defense'] / a_stats['elo_adj']

        h_atk_ratio = h_off / league_per_team if league_per_team > 0 else 1.0
        a_atk_ratio = a_off / league_per_team if league_per_team > 0 else 1.0
        h_def_ratio = h_def / league_per_team if league_per_team > 0 else 1.0
        a_def_ratio = a_def / league_per_team if league_per_team > 0 else 1.0

    # Matchup-based expected scoring
    exp_home_pts = league_per_team * h_atk_ratio * a_def_ratio
    exp_away_pts = league_per_team * a_atk_ratio * h_def_ratio
    independent_total = exp_home_pts + exp_away_pts

    # Blend: 60% independent model, 40% league average (dampen noise)
    if 'basketball' in sport:
        blend_weight = 0.60
    else:
        blend_weight = 0.55  # Hockey: slightly more conservative

    model_total = independent_total * blend_weight + league_avg * (1 - blend_weight)

    # Blowout/close game adjustment — basketball only
    # Use predicted scoring gap as proxy for expected blowout
    spread_diff = abs(exp_home_pts - exp_away_pts)
    if 'basketball' in sport:
        if spread_diff > 8:
            model_total -= 2  # Blowouts tend to go under
        elif spread_diff < 2:
            model_total += 1  # Close games, OT possibility

    return round(model_total, 1)


def _totals_confidence(home, away, sport, conn):
    """
    Check if we have enough data to trust a totals prediction.
    Returns 'HIGH', 'MEDIUM', or 'LOW'.
    """
    for team in [home, away]:
        cnt = conn.execute("""
            SELECT COUNT(*) FROM market_consensus
            WHERE sport=? AND best_over_total IS NOT NULL AND (home=? OR away=?)
        """, (sport, team, team)).fetchone()[0]
        if cnt < 5:
            return 'LOW'
    
    # Also check results table for actual game data
    for team in [home, away]:
        results_cnt = conn.execute("""
            SELECT COUNT(*) FROM results
            WHERE sport=? AND completed=1 AND (home=? OR away=?)
        """, (sport, team, team)).fetchone()[0]
        if results_cnt >= 10:
            return 'HIGH'
    
    return 'MEDIUM'


def calculate_point_value_totals(model_total, market_total, sport):
    """
    Point value for totals — now probability-based, not linear.
    
    Uses the CDF to compute the actual probability that the total goes
    over/under the market line, then converts to edge %.
    
    This fixes the v10 issue where 8-pt and 14-pt diffs both showed ~20%.
    """
    diff = abs(model_total - market_total)
    std = TOTAL_STD.get(sport, 22.0)
    prob = _ncdf(diff / std)
    
    # Edge = how much our probability exceeds the implied 50% (at -110)
    # At -110 odds, implied prob = 52.4%. Edge = prob - 0.524
    edge_pct = (prob - 0.524) * 100.0
    
    # Cap at 20% for totals (realistic ceiling — anything above is a data issue)
    return round(max(0.0, min(edge_pct, 20.0)), 1)


def _total_prob(diff, sport):
    """Probability that actual total exceeds market by diff."""
    std = TOTAL_STD.get(sport, 22.0)
    return _ncdf(diff / std)

def _divergence_penalty(model_val, market_val, market_type='SPREAD'):
    """
    Model-vs-market divergence safety check.
    Data shows the 3-5pt gap is the danger zone (5W-8L, -17.2u) — model is
    indecisive. Big divergences (5+) are actually the model's best picks
    (46W-17L, +109.9u). Only penalize the uncertain middle.

    Spreads: 3-5pt gap → 0.80
    Totals:  3-5pt gap → 0.80
    """
    gap = abs(model_val - market_val)
    if 3.0 < gap <= 5.0:
        print(f"    ⚠ DIVERGENCE PENALTY: model-market gap={gap:.1f} → edge×0.80")
        return 0.80
    return 1.0


def generate_predictions(conn, sport=None, date=None):
    sports = [sport] if sport else list(SPORT_CONFIG.keys())
    all_picks = []

    now_utc = datetime.now(timezone.utc)
    # TODAY ONLY — games from now until midnight Eastern tonight
    # Midnight Eastern = 4:00 AM UTC (EDT) or 5:00 AM UTC (EST)
    offset_hours = 4 if 3 <= now_utc.month <= 10 else 5
    est_midnight = now_utc.replace(hour=offset_hours, minute=0, second=0, microsecond=0)
    if now_utc.hour >= 5:
        est_midnight += timedelta(days=1)
    # v16: Games must start at least 30 min from now so subscribers have time to bet.
    # Was -2 hours (allowed already-started games).
    window_start = (now_utc + timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    window_end = est_midnight.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"  Game window: TODAY ONLY — {window_start} to {window_end}")

    for sp in sports:
        ratings = get_latest_ratings(conn, sp)

        # Tennis: no bootstrap power ratings — seed from Elo so the pipeline proceeds
        if sp.startswith('tennis_') and len(ratings) < 5:
            try:
                from elo_engine import get_tennis_elo
                _t_elo, _t_key = get_tennis_elo(conn, sp)
                if _t_elo:
                    for player, data in _t_elo.items():
                        # Convert Elo to a spread-scale rating (centered at 0)
                        ratings[player] = {
                            'base': round((data['elo'] - 1500) / 120, 2),  # 120 Elo per set
                            'home_court': 0.0,
                            'final': round((data['elo'] - 1500) / 120, 2),
                        }
                    print(f"  {sp}: {len(ratings)} players seeded from Elo ({_t_key})")
            except Exception as e:
                print(f"  {sp}: Elo seed failed: {e}")

        if len(ratings) < 5:
            print(f"  {sp}: only {len(ratings)} teams — SKIP"); continue
        print(f"  {sp}: {len(ratings)} teams rated")

        # Load Elo ratings if available
        elo_data = {}
        if HAS_ELO:
            # Tennis: use surface-split Elo (e.g., tennis_atp_clay for French Open)
            if sp.startswith('tennis_'):
                try:
                    from elo_engine import get_tennis_elo
                    elo_data, _elo_key = get_tennis_elo(conn, sp)
                    if elo_data:
                        elo_count = sum(1 for v in elo_data.values() if v['confidence'] != 'LOW')
                        print(f"    + Tennis Elo ({_elo_key}): {elo_count} players with confidence")
                    else:
                        print(f"    ⚠ No tennis Elo — run historical_scores.py + elo_engine.py")
                except ImportError:
                    pass
            else:
                elo_data = get_elo_ratings(conn, sp)
                if elo_data:
                    elo_count = sum(1 for v in elo_data.values() if v['confidence'] != 'LOW')
                    print(f"    + Elo ratings: {elo_count} teams with confidence")
                else:
                    print(f"    ⚠ No Elo data — using market ratings only (run historical_scores.py + elo_engine.py)")

        game_count = conn.execute(
            "SELECT COUNT(DISTINCT event_id) FROM market_consensus WHERE sport=?",
            (sp,)).fetchone()[0]
        is_thin = game_count < 30
        if is_thin: print(f"    {game_count} games — conservative mode")
        min_pv = minimum_play_threshold(sp, is_thin)
        min_pv_totals = min_pv + 5.0  # v12 FIX: Totals 24% +CLV rate. Require 5% more edge than spreads.
        # Soccer totals: independent model produces smaller PV% (goals vs points),
        # but backtest shows 9W-2L at 5%+ edge. Lower threshold for soccer.
        if 'soccer' in sp:
            min_pv_totals = 5.0
        # Walters ML threshold: Elo probability vs de-vigged ML odds.
        # Lower than spread thresholds because ML edges are raw probability
        # comparisons — no key number inflation. Spread min_pv is calibrated
        # for PV% which can reach 15-25% from crossing multiple key numbers.
        # ML edges are naturally 4-12% for genuine disagreements.
        min_pv_ml = max(5.0, min_pv * 0.50)  # Half of spread threshold, floor 5%

        games = conn.execute("""
            SELECT event_id, commence_time, home, away,
                   best_home_spread, best_home_spread_odds, best_home_spread_book,
                   best_away_spread, best_away_spread_odds, best_away_spread_book,
                   best_over_total, best_over_odds, best_over_book,
                   best_under_total, best_under_odds, best_under_book,
                   best_home_ml, best_home_ml_book, best_away_ml, best_away_ml_book
            FROM market_consensus
            WHERE sport=? AND commence_time>=? AND commence_time<=?
            AND snapshot_date = (
                SELECT MAX(mc2.snapshot_date) FROM market_consensus mc2
                WHERE mc2.event_id = market_consensus.event_id AND mc2.sport = market_consensus.sport
            )
            ORDER BY commence_time
        """, (sp, window_start, window_end)).fetchall()
        print(f"    {len(games)} games today")

        # ── AUTO-SEED unrated teams from RESULTS + market data ──
        # Critical for NCAAB: 363 teams, many small schools won't be
        # in power_ratings from bootstrap. Step 1: try to derive a real
        # rating from game results (like a mini-bootstrap). Step 2: if
        # no results exist, derive from market spread. Step 3: only if
        # truly no data, seed at 0.0 (model = market, no false edges).
        seeded = 0
        hca_seed = SPORT_CONFIG.get(sp, {}).get('home_court', 2.5)
        
        def _derive_rating_from_results(team, sport_key, conn_local, ratings_local, hca_val):
            """Mini-bootstrap: derive a team's rating from their game results."""
            rows = conn_local.execute("""
                SELECT home, away, actual_margin
                FROM results
                WHERE (home = ? OR away = ?) AND sport = ? AND completed = 1
                AND actual_margin IS NOT NULL
                ORDER BY commence_time DESC LIMIT 10
            """, (team, team, sport_key)).fetchall()
            
            if len(rows) < 2:
                return None
            
            implied_ratings = []
            for r in rows:
                home_r, away_r, margin = r
                is_home = (home_r == team)
                opponent = away_r if is_home else home_r
                opp_rating = ratings_local.get(opponent, {}).get('final')
                
                if opp_rating is not None:
                    # TGPL: team_rating = margin + opponent_rating (adjusted for HCA)
                    if is_home:
                        implied = margin + opp_rating - hca_val
                    else:
                        implied = -margin + opp_rating + hca_val
                    implied_ratings.append(implied)
            
            if not implied_ratings:
                return None
            
            return round(sum(implied_ratings) / len(implied_ratings), 2)
        
        for g in games:
            home_t, away_t = g[2], g[3]
            mkt_spread = g[4]  # home spread (negative = home favored)
            h_rated = home_t in ratings
            a_rated = away_t in ratings

            if h_rated and a_rated:
                continue  # Both already rated

            # Step 1: Try to derive from results
            if not h_rated:
                derived = _derive_rating_from_results(home_t, sp, conn, ratings, hca_seed)
                if derived is not None:
                    ratings[home_t] = {'base': derived, 'home_court': hca_seed, 'final': derived}
                    h_rated = True
                    seeded += 1
            
            if not a_rated:
                derived = _derive_rating_from_results(away_t, sp, conn, ratings, hca_seed)
                if derived is not None:
                    ratings[away_t] = {'base': derived, 'home_court': hca_seed, 'final': derived}
                    a_rated = True
                    seeded += 1
            
            if h_rated and a_rated:
                continue
            
            # Step 2: Derive from market spread + rated opponent
            if mkt_spread is not None:
                if h_rated and not a_rated:
                    derived = ratings[home_t]['final'] + mkt_spread + hca_seed
                    ratings[away_t] = {'base': derived, 'home_court': hca_seed, 'final': round(derived, 2)}
                    seeded += 1
                elif a_rated and not h_rated:
                    derived = ratings[away_t]['final'] - mkt_spread - hca_seed
                    ratings[home_t] = {'base': derived, 'home_court': hca_seed, 'final': round(derived, 2)}
                    seeded += 1
                else:
                    # Neither rated, no results — seed from market (model = market, zero false edge)
                    ratings[home_t] = {'base': 0.0, 'home_court': hca_seed, 'final': 0.0}
                    derived = 0.0 + mkt_spread + hca_seed
                    ratings[away_t] = {'base': derived, 'home_court': hca_seed, 'final': round(derived, 2)}
                    seeded += 2
            else:
                if not h_rated:
                    ratings[home_t] = {'base': 0.0, 'home_court': hca_seed, 'final': 0.0}
                    seeded += 1
                if not a_rated:
                    ratings[away_t] = {'base': 0.0, 'home_court': hca_seed, 'final': 0.0}
                    seeded += 1

        if seeded:
            print(f"    + Auto-seeded {seeded} unrated teams (from results + market data)")

        cfg = SPORT_CONFIG.get(sp)
        if cfg is None:
            if 'tennis' in sp:
                # Infer surface from tournament name — paramount for correct Elo + model params
                _sp_lower = sp.lower()
                _CLAY_KEYWORDS = ['french_open', 'roland_garros', 'monte_carlo', 'madrid',
                                  'italian_open', 'rome', 'barcelona', 'hamburg', 'rio',
                                  'buenos_aires', 'lyon', 'bastad', 'kitzbuhel', 'umag',
                                  'gstaad', 'geneva', 'marrakech', 'bucharest', 'parma',
                                  'palermo', 'prague', 'rabat', 'strasbourg', 'lausanne',
                                  'portoroz', 'bogota', 'istanbul', 'budapest']
                _GRASS_KEYWORDS = ['wimbledon', 'queens', 'halle', 'stuttgart_grass',
                                   'eastbourne', 'berlin', 'bad_homburg', 'nottingham',
                                   'mallorca', 's_hertogenbosch', 'birmingham', 'libema']
                if any(kw in _sp_lower for kw in _CLAY_KEYWORDS):
                    _surface = 'clay'
                elif any(kw in _sp_lower for kw in _GRASS_KEYWORDS):
                    _surface = 'grass'
                else:
                    _surface = 'hard'  # Most tournaments are hard court
                cfg = dict(_TENNIS_PARAMS[_surface])
                SPORT_CONFIG[sp] = cfg
                print(f"    Auto-config: {sp} → {_surface} court")
            else:
                print(f"    ⚠ Unknown sport: {sp} — skipping")
                continue
        seen = set()
        skip_nr = skip_div = skip_w = 0

        for g in games:
            eid, commence, home, away = g[0], g[1], g[2], g[3]

            # Skip games already in progress or about to start
            # 5-minute buffer accounts for clock drift between API and real tip-off
            if commence:
                try:
                    game_time = datetime.fromisoformat(commence.replace('Z', '+00:00'))
                    if game_time < now_utc - timedelta(minutes=5):
                        continue
                except Exception:
                    pass

            ms = compute_model_spread(home, away, ratings, sp)
            if ms is None: skip_nr += 1; continue

            # Neutral-site detection for NCAA tournament games.
            # Before March 17: regular season + conference tournaments (HCA mostly applies)
            # March 17+: NCAA tournament / NIT / CBI — ALL neutral sites
            # April 1-7: Final Four + Championship — neutral
            # Old code treated ALL of March as neutral, removing 3.2 pts of real HCA
            # from regular season home games in early March.
            _neutral = False
            # Tennis: all matches are at neutral tournament venues
            if sp.startswith('tennis_'):
                _neutral = True
            elif sp == 'basketball_ncaab':
                _now = datetime.now()
                _m, _d = _now.month, _now.day
                if _m == 4 and _d <= 7:
                    _neutral = True   # Final Four / Championship
                elif _m == 3 and _d >= 17:
                    _neutral = True   # NCAA tournament (post-Selection Sunday)

            # UPGRADE: If Elo ratings available, use blended spread
            # This creates predictions INDEPENDENT of market lines
            if HAS_ELO and elo_data:
                elo_ms = blended_spread(home, away, elo_data, ratings, sp, conn, neutral_site=_neutral)
                if elo_ms is not None:
                    ms = elo_ms  # Use the blended prediction
                elif sp == 'basketball_ncaab':
                    # v12.2: NCAAB requires Elo. Without it, bootstrap is circular.
                    skip_nr += 1
                    continue

            mkt_hs, mkt_hs_odds, mkt_hs_book = g[4], g[5], g[6]
            mkt_as, mkt_as_odds, mkt_as_book = g[7], g[8], g[9]
            hml, hml_book, aml, aml_book = g[16], g[17], g[18], g[19]

            # ═══ MLB PITCHER GATE: Skip games without confirmed starters ═══
            # Pitching is THE dominant factor in MLB. Betting a total or spread
            # without knowing both starters is flying blind. College baseball
            # is exempt because ESPN rarely lists college probables.
            _mlb_pitcher_info = None
            if sp == 'baseball_mlb' and HAS_MLB_PITCHERS:
                try:
                    _game_date = None
                    if commence:
                        try:
                            _gdt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
                            _game_date = _gdt.astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')
                        except (ValueError, AttributeError):
                            pass
                    _mlb_pitcher_info = get_mlb_probable_starters(conn, home, away, _game_date)
                    if not _mlb_pitcher_info.get('both_confirmed', False):
                        print(f"    \u26a0 MLB pick skipped: no pitcher data for "
                              f"{away} @ {home} ({_mlb_pitcher_info.get('summary', 'TBD')})")
                        continue  # Skip entire game — no picks without both starters
                except Exception as _pe:
                    # If pitcher lookup fails, still skip — err on side of caution
                    print(f"    \u26a0 MLB pick skipped: pitcher lookup error for "
                          f"{away} @ {home}: {_pe}")
                    continue

            # ═══ NHL GOALIE GATE: Skip games without confirmed starters ═══
            # The starting goalie is the single biggest factor in NHL game outcomes.
            # A .920 vs .900 SV% goalie is ~0.5 goals/game difference.
            # Without knowing both starters, our total model is unreliable.
            _nhl_goalie_info = None
            if sp == 'icehockey_nhl' and HAS_NHL_GOALIES:
                try:
                    _game_date = None
                    if commence:
                        try:
                            _gdt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
                            _game_date = _gdt.astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')
                        except (ValueError, AttributeError):
                            pass
                    _nhl_goalie_info = get_nhl_probable_goalies(conn, home, away, _game_date)
                    if not _nhl_goalie_info.get('both_confirmed', False):
                        print(f"    \u26a0 NHL pick skipped: no goalie data for "
                              f"{away} @ {home} ({_nhl_goalie_info.get('summary', 'TBD')})")
                        continue  # Skip entire game — no picks without both starters
                except Exception as _ge:
                    # If goalie lookup fails, still skip — err on side of caution
                    print(f"    \u26a0 NHL pick skipped: goalie lookup error for "
                          f"{away} @ {home}: {_ge}")
                    continue

            # ═══ SOCCER ELO ML: DISABLED v13 — backtest 0W-8L (-100% ROI) ═══
            # ml_scale 1.5 + dog cap +180 still couldn't fix it. Model fundamentally
            # overestimates underdog win probability. All 8 remaining picks lost.
            # Spreads are +7.3% ROI — that's where soccer edge lives.
            if False and 'soccer' in sp and HAS_ELO and elo_data and hml is not None and aml is not None:
                from elo_engine import elo_win_probability, ELO_CONFIG
                _soccer_home_prob = elo_win_probability(home, away, elo_data, sp, neutral_site=False)
                if _soccer_home_prob is not None:
                    # Compute Elo-derived spread for 3-way probability split
                    h_elo = elo_data.get(home, {})
                    a_elo = elo_data.get(away, {})
                    _spe = ELO_CONFIG.get(sp, {}).get('spread_per_elo', 160)
                    _ha = ELO_CONFIG.get(sp, {}).get('home_advantage', 55)
                    _elo_spread = -((h_elo.get('elo', 1500) + _ha) - a_elo.get('elo', 1500)) / _spe
                    _s_home, _s_draw, _s_away = soccer_ml_probs(_elo_spread, sp)

                    # De-vig 3-way market odds
                    draw_row = conn.execute("""
                        SELECT odds FROM odds
                        WHERE event_id=? AND market='h2h' AND selection='Draw'
                        AND snapshot_date=(SELECT MAX(snapshot_date) FROM odds WHERE event_id=? AND market='h2h')
                        ORDER BY odds DESC LIMIT 1
                    """, (eid, eid)).fetchone()
                    _d_odds = draw_row[0] if draw_row else None
                    _h_fair, _a_fair, _ = devig_ml_odds(hml, aml, _d_odds)

                    if _h_fair and _a_fair:
                        # Confidence weight: scale by games played
                        _mgp = min(h_elo.get('games', 0), a_elo.get('games', 0))
                        _soccer_conf_w = min(1.0, _mgp / 15.0)

                        # Home ML edge
                        k_h = f"{eid}|M|{home}"
                        if k_h not in seen:
                            _h_edge = (_s_home - _h_fair) * 100 * _soccer_conf_w
                            _h_stars = get_star_rating(_h_edge)
                            if _h_edge >= min_pv_ml and _h_stars > 0 and hml <= 180:  # v13: Cap home dogs at +180. Backtest: dogs 72% loss rate.
                                timing, t_r = bet_timing_advice(ms, mkt_hs or 0)
                                seen.add(k_h)
                                pick = _mk_ml(sp, eid, commence, home, away,
                                    f"{home} ML", hml_book, hml, ms,
                                    round(_s_home, 4), round(_h_fair, 4),
                                    round(_h_edge, 2), _h_stars, timing, t_r)
                                if pick:
                                    pick['context'] = 'Soccer Elo ML — direct probability edge'
                                    all_picks.append(pick)

                        # Away ML edge
                        k_a = f"{eid}|M|{away}"
                        if k_a not in seen:
                            _a_edge = (_s_away - _a_fair) * 100 * _soccer_conf_w
                            _a_stars = get_star_rating(_a_edge)
                            if _a_edge >= min_pv_ml and _a_stars > 0 and aml < 180:  # v13: Was 250 — tighten dog cap. Backtest: away dogs hemorrhaging units.
                                timing, t_r = bet_timing_advice(-ms, mkt_as or 0)
                                seen.add(k_a)
                                pick = _mk_ml(sp, eid, commence, home, away,
                                    f"{away} ML", aml_book, aml, ms,
                                    round(_s_away, 4), round(_a_fair, 4),
                                    round(_a_edge, 2), _a_stars, timing, t_r)
                                if pick:
                                    pick['context'] = 'Soccer Elo ML — direct probability edge'
                                    all_picks.append(pick)

            # ═══ INJURY DATA — fetch BEFORE any pick generation ═══
            # Must happen before Elo ML, spreads, and totals so all paths
            # have access to injury context. Previously fetched after Elo ML,
            # which caused blind picks (e.g., Spurs ML with Wembanyama out).
            h_inj, h_cl, h_imp = get_team_injury_context(conn, home, sp)
            a_inj, a_cl, a_imp = get_team_injury_context(conn, away, sp)

            # ═══ REFEREE DATA — fetch BEFORE pick generation ═══
            # Scrapes today's assigned officials from ESPN and computes
            # total adjustment from historical tendencies (referee_engine).
            # Data flows into context_engine.ref_adjustment() for totals picks.
            ref_adj, ref_info = 0.0, ''
            if HAS_REF:
                try:
                    ref_adj, ref_info = get_ref_adjustment(home, away, sp, conn)
                except Exception:
                    pass  # Ref data is supplementary — don't crash

            # DIVERGENCE CHECK — run on RAW model spread (before context)
            # Context adjustments are our REASON for disagreeing with the market,
            # not a sign of model error. Only the base model should be checked.
            max_div = cfg['max_spread_divergence']
            if mkt_hs is not None and abs(ms - mkt_hs) > max_div:
                # ═══ WALTERS: Elo ML rescue for divergent games ═══
                # Spread is too divergent for spread-based picks, but Elo win
                # probability can still find ML value. This fires for ALL sports
                # with Elo data — divergent games are often the biggest mismatches
                # where favorites have genuine ML value that spreads can't capture.
                #
                # NCAAB tournament: 50% weight (Elo compresses extreme mismatches)
                # Other sports: full Elo probability (better calibrated, fewer games)
                if hml is not None and aml is not None and HAS_ELO and elo_data:
                    # Confidence-weighted Elo edge: scale by data quality.
                    # A 10% edge from 3 games is NOT the same as 10% from 25 games.
                    # Weight = min_team_games / sport_min_games, capped at 1.0.
                    # NCAAB tournament also dampened 50% for Elo compression.
                    _is_tourney = (sp == 'basketball_ncaab'
                        and (datetime.now().month == 3 or (datetime.now().month == 4 and datetime.now().day <= 7)))
                    h_data = elo_data.get(home, {})
                    a_data = elo_data.get(away, {})
                    _min_gp = min(h_data.get('games', 0), a_data.get('games', 0))
                    # Hard-block teams with <10 Elo games — SIU-E had 3 games,
                    # generated a -4.5 fav pick that flipped to +2.5 by game time.
                    # Soft weighting wasn't enough to prevent fake edges.
                    if _min_gp < 10:
                        _log_divergence_block(conn, sp, eid, home, away, ms, mkt_hs, 'insufficient_elo_games')
                        skip_div += 1; continue
                    _sport_min = cfg.get('min_games_elo', 15)  # Target: 15+ games for full weight
                    _conf_w = min(1.0, _min_gp / _sport_min)

                    # v14: SOS dampening for cross-conference matchups (esp March Madness).
                    # Elo built from conference play can't compare across conferences.
                    # A 1780 Elo in the MAC != 1780 in the Big 12.
                    # Two checks:
                    #   1. SOS gap > 60: teams played in different strength leagues
                    #   2. Weak SOS (< 1510): team's Elo is inflated by cupcakes
                    # Either condition dampens the edge. Both together = block.
                    _sos_w = 1.0
                    if sp == 'basketball_ncaab' and conn is not None:
                        try:
                            h_sos = conn.execute("""
                                SELECT AVG(e.elo) FROM results r
                                JOIN elo_ratings e ON e.team = CASE WHEN r.home=? THEN r.away ELSE r.home END
                                    AND e.sport=?
                                WHERE (r.home=? OR r.away=?) AND r.sport=? AND r.completed=1
                            """, (home, sp, home, home, sp)).fetchone()[0] or 1500
                            a_sos = conn.execute("""
                                SELECT AVG(e.elo) FROM results r
                                JOIN elo_ratings e ON e.team = CASE WHEN r.home=? THEN r.away ELSE r.home END
                                    AND e.sport=?
                                WHERE (r.home=? OR r.away=?) AND r.sport=? AND r.completed=1
                            """, (away, sp, away, away, sp)).fetchone()[0] or 1500
                            _sos_gap = abs(h_sos - a_sos)

                            # Check 1: SOS gap between teams
                            if _sos_gap > 100:
                                _sos_w = 0.0  # Block: completely different leagues
                            elif _sos_gap > 50:
                                _sos_w = max(0.20, 1.0 - (_sos_gap - 50) / 60)

                            # Check 2: Either team has weak SOS (< 1510 = mid-major)
                            # Their Elo is inflated regardless of the gap
                            _min_sos = min(h_sos, a_sos)
                            if _min_sos < 1500:
                                _sos_w = 0.0  # Hard block: cupcake schedule inflates Elo. Was 0.10 soft-block at 1480.
                            elif _min_sos < 1520:
                                _sos_w = min(_sos_w, 0.20)  # Heavy dampen: weak schedule. Was 0.30 at 1510.
                        except Exception:
                            pass

                    from elo_engine import elo_win_probability
                    home_prob = elo_win_probability(home, away, elo_data, sp, neutral_site=_neutral)
                    if home_prob is not None:
                        away_prob = 1.0 - home_prob

                        # ═══ INJURY ADJUSTMENT FOR ELO ML ═══
                        # Elo ratings reflect historical performance WITH key players.
                        # If a star is out, Elo overstates the team's strength.
                        # Convert point impact to probability shift:
                        #   5.0 pts impact ≈ 8-10% win probability swing (NBA/NHL).
                        #   Scale: 1.5% win prob per point of injury impact.
                        _inj_prob_shift = (a_imp - h_imp) * 0.015  # Positive = home advantage
                        if abs(_inj_prob_shift) >= 0.01:
                            home_prob = max(0.05, min(0.95, home_prob + _inj_prob_shift))
                            away_prob = 1.0 - home_prob

                        # Hard gate: block ML pick if the PICKED team has a star out
                        # (5.0+ point impact = MVP-caliber player missing)
                        _home_star_out = h_imp >= 5.0
                        _away_star_out = a_imp >= 5.0

                        h_fair, a_fair, _ = devig_ml_odds(hml, aml)
                        if h_fair and a_fair:
                            # Mismatch dampening: Elo compresses toward 50% and
                            # can't distinguish 95% from 99% favorites. Close games
                            # get full weight; extreme mismatches are dampened.
                            _mkt_max = max(h_fair, a_fair)
                            _mismatch_w = 1.0 if _mkt_max <= 0.75 else max(0.40, 1.0 - (_mkt_max - 0.75) * 2.4)
                            _elo_w = _conf_w * _mismatch_w * _sos_w
                            _min = min_pv if _is_tourney else min_pv_ml
                            # Home ML
                            h_edge = (home_prob - h_fair) * 100 * _elo_w
                            k_h = f"{eid}|M|{home}"
                            if k_h not in seen and h_edge >= _min and not _home_star_out:
                                stars = get_star_rating(h_edge)
                                if stars > 0:
                                    timing, t_r = bet_timing_advice(ms, mkt_hs or 0)
                                    seen.add(k_h)
                                    pick = _mk_ml(sp, eid, commence, home, away,
                                        f"{home} ML", hml_book, hml, ms,
                                        round(home_prob, 4), round(h_fair, 4),
                                        round(h_edge, 2), stars, timing, t_r)
                                    if pick:
                                        pick['context'] = 'Elo probability edge'
                                        if h_imp > 0 or a_imp > 0:
                                            pick['context'] += f' | Injuries: {home} -{h_imp:.1f}pts, {away} -{a_imp:.1f}pts'
                                        all_picks.append(pick)
                            elif _home_star_out and h_edge >= _min:
                                print(f"    ⚠ INJURY GATE: {home} ML blocked — star player out ({h_imp:.1f} pts impact)")
                            # Away ML
                            a_edge = (away_prob - a_fair) * 100 * _elo_w
                            k_a = f"{eid}|M|{away}"
                            if k_a not in seen and a_edge >= _min and not _away_star_out:
                                stars = get_star_rating(a_edge)
                                if stars > 0:
                                    timing, t_r = bet_timing_advice(-ms, mkt_as or 0)
                                    seen.add(k_a)
                                    pick = _mk_ml(sp, eid, commence, home, away,
                                        f"{away} ML", aml_book, aml, ms,
                                        round(away_prob, 4), round(a_fair, 4),
                                        round(a_edge, 2), stars, timing, t_r)
                                    if pick:
                                        pick['context'] = 'Elo probability edge'
                                        if h_imp > 0 or a_imp > 0:
                                            pick['context'] += f' | Injuries: {home} -{h_imp:.1f}pts, {away} -{a_imp:.1f}pts'
                                        all_picks.append(pick)
                            elif _away_star_out and a_edge >= _min:
                                print(f"    ⚠ INJURY GATE: {away} ML blocked — star player out ({a_imp:.1f} pts impact)")
                # v25.39: CONTEXT MODEL check (NHL + MLS + EPL). Phase 1-5
                # signals (injuries, form, rest, motivation, playoff HCA,
                # series momentum, home/away splits, H2H, extended form,
                # pace, star concentration). When Context Model brings the
                # adjusted spread WITHIN max_div of market, fire a
                # DATA_SPREAD pick on Context's preferred side. Takes
                # precedence over SPREAD_FADE_FLIP on same game.
                # Multi-sport backtest (14d, 90 blocked spreads):
                #   NHL: 14 picks, 11-3 (78.6%), +35.00u
                #   MLS: 5 picks, 5-0 (100%), +22.73u
                #   EPL: 2 picks, 2-0 (100%), +9.09u
                CONTEXT_MODEL_SPORTS = {
                    'icehockey_nhl', 'soccer_usa_mls', 'soccer_epl',
                }
                _context_fired = False
                if (sp in CONTEXT_MODEL_SPORTS
                        and mkt_hs is not None and mkt_as is not None
                        and mkt_hs_odds is not None and mkt_as_odds is not None):
                    try:
                        from context_model import compute_context_spread, format_context_summary
                        _commence_date = (commence[:10] if commence else None)
                        ms_ctx, _ctx_info = compute_context_spread(
                            conn, sp, home, away, eid, ms, _commence_date)
                        _ctx_div = abs(ms_ctx - mkt_hs)
                        if _ctx_div <= max_div:
                            # Context unblocks — pick Context's preferred side
                            # ms_ctx < mkt_hs → Context more bullish on home → bet home
                            if ms_ctx < mkt_hs:
                                _c_team, _c_line, _c_odds, _c_book = home, mkt_hs, mkt_hs_odds, mkt_hs_book
                            else:
                                _c_team, _c_line, _c_odds, _c_book = away, mkt_as, mkt_as_odds, mkt_as_book
                            from config import MIN_ODDS as _CM_MIN_ODDS
                            if (_c_odds is not None and _c_odds > _CM_MIN_ODDS
                                    and _c_odds <= 140 and _c_book):
                                _ctx_summary = format_context_summary(_ctx_info)
                                _ctx_ctx = (
                                    f'DATA_SPREAD v25.39 — {_ctx_summary} | '
                                    f'Market {mkt_hs:+.1f}, Context {ms_ctx:+.1f} '
                                    f'(ctx_div={_ctx_div:.1f} ≤ {max_div}). '
                                    f'Bet {_c_team} {_c_line:+.1f} @ {_c_book} {_c_odds:+.0f}.'
                                )
                                _ctx_pick = {
                                    'sport': sp, 'event_id': eid, 'commence': commence,
                                    'home': home, 'away': away,
                                    'market_type': 'SPREAD',
                                    'selection': f'{_c_team} {_c_line:+.1f}',
                                    'book': _c_book, 'line': _c_line, 'odds': _c_odds,
                                    'model_spread': ms,
                                    'model_prob': 0, 'implied_prob': 0,
                                    'edge_pct': 0,
                                    'star_rating': 3, 'units': 5.0,
                                    'confidence': 'DATA_SPREAD',
                                    'side_type': 'DATA_SPREAD',
                                    'spread_or_ml': 'SPREAD',
                                    'timing': 'STANDARD',
                                    'context': _ctx_ctx,
                                    'notes': _ctx_ctx,
                                }
                                print(f"  🧠 DATA_SPREAD: {sp.split('_')[-1]} {_c_team} "
                                      f"{_c_line:+.1f} @ {_c_book} {_c_odds:+.0f} "
                                      f"(ctx_div {_ctx_div:.1f}, raw {abs(ms-mkt_hs):.1f})")
                                all_picks.append(_ctx_pick)
                                _context_fired = True
                    except Exception as _ce:
                        print(f"  ⚠ DATA_SPREAD error: {_ce}")

                # v25.36: SPREAD_FADE_FLIP (NBA + NHL only).
                # Only fires if Context Model did NOT produce a pick for this event.
                # 14-day backtest on DIVERGENCE_GATE-blocked spread picks:
                #   NBA: 38-18 (67.9%), +82.73u
                #   NHL: 17-4 (81.0%), +57.27u
                # When the model diverges from market by max_div+, the model
                # is wrong ~70% of the time. Fade it — bet the OPPOSITE side
                # at the market line. 5u stake.
                if (not _context_fired
                        and sp in ('basketball_nba', 'icehockey_nhl')
                        and mkt_hs is not None and mkt_as is not None
                        and mkt_hs_odds is not None and mkt_as_odds is not None):
                    try:
                        # v25.60: Dual-model agreement check. Fade flip's thesis
                        # is "Elo alone is wrong when divergent." If Context
                        # also disagrees with market in the SAME direction as
                        # Elo, both our brains agree — fading is betting against
                        # our strongest conviction signal (65% WR edge cohort).
                        # Backtest sample (42 NBA+NHL dual-agree fade scenarios):
                        # 13-29, 31% WR, -86u. Veto when Context aligns with Elo.
                        _ff_context_vetoes = False
                        try:
                            from context_model import compute_context_spread
                            _ff_commence_date = (commence[:10] if commence else None)
                            ms_ctx_ff, _ = compute_context_spread(
                                conn, sp, home, away, eid, ms, _ff_commence_date)
                            # Elo vs market direction
                            _elo_more_bullish_home = (ms < mkt_hs)
                            # Context vs market direction
                            _ctx_more_bullish_home = (ms_ctx_ff < mkt_hs)
                            # If both agree direction from market → don't fade
                            if _elo_more_bullish_home == _ctx_more_bullish_home:
                                _ff_context_vetoes = True
                                _ctx_dir = 'home' if _ctx_more_bullish_home else 'away'
                                _elo_dir = 'home' if _elo_more_bullish_home else 'away'
                                print(f"    🧠 SPREAD_FADE_FLIP vetoed: Context ({ms_ctx_ff:+.1f}) "
                                      f"agrees with Elo ({ms:+.1f}) on favoring {_ctx_dir} vs market {mkt_hs:+.1f}")
                                try:
                                    conn.execute("""INSERT INTO shadow_blocked_picks
                                        (created_at, sport, event_id, selection, market_type, book,
                                         line, odds, edge_pct, units, reason)
                                        VALUES (?, ?, ?, ?, 'SPREAD', ?, ?, ?, ?, ?, ?)""",
                                        (datetime.now().isoformat(), sp, eid,
                                         f"{away}@{home} (fade flip blocked)", mkt_hs_book,
                                         mkt_hs, mkt_hs_odds, 0, 5.0,
                                         f'SPREAD_FADE_FLIP_DUAL_MODEL_VETO (v25.60 — Elo ms={ms:+.1f}, '
                                         f'Context ms_ctx={ms_ctx_ff:+.1f}, market={mkt_hs:+.1f}; both favor {_ctx_dir})'))
                                    conn.commit()
                                except Exception:
                                    pass
                        except Exception:
                            pass  # If Context check fails, default to firing (existing behavior)

                        if _ff_context_vetoes:
                            # Skip fade flip — too much conviction on the other side.
                            # Still log divergence block + bump skip_div so observability
                            # counters stay accurate (v25.60 veto accounting fix).
                            _log_divergence_block(conn, sp, eid, home, away, ms, mkt_hs, 'post_elo_rescue')
                            skip_div += 1
                            continue
                        # Determine which side the model WANTED:
                        #   ms < mkt_hs → model more bullish on home than market → fade to AWAY
                        #   ms > mkt_hs → model less bullish on home → fade to HOME
                        if ms > mkt_hs:
                            _f_team, _f_line, _f_odds, _f_book = home, mkt_hs, mkt_hs_odds, mkt_hs_book
                        else:
                            _f_team, _f_line, _f_odds, _f_book = away, mkt_as, mkt_as_odds, mkt_as_book
                        # Respect global odds policy: -150 floor (favorite cap),
                        # +140 ceiling (matches prop MAX_PROP_ODDS policy).
                        from config import MIN_ODDS as _FF_MIN_ODDS
                        if (_f_odds is not None and _f_odds > _FF_MIN_ODDS
                                and _f_odds <= 140 and _f_book):
                            _fade_div = abs(ms - mkt_hs)
                            _fade_ctx = (
                                f'SPREAD_FADE_FLIP v25.36 — model ms={ms:+.1f} vs market '
                                f'{mkt_hs:+.1f} (div={_fade_div:.1f}). Fading model → '
                                f'bet {_f_team} {_f_line:+.1f} at {_f_book} {_f_odds:+.0f}.'
                            )
                            _fade_pick = {
                                'sport': sp, 'event_id': eid, 'commence': commence,
                                'home': home, 'away': away,
                                'market_type': 'SPREAD',
                                'selection': f'{_f_team} {_f_line:+.1f}',
                                'book': _f_book, 'line': _f_line, 'odds': _f_odds,
                                'model_spread': ms,
                                'model_prob': 0, 'implied_prob': 0,
                                'edge_pct': 0,
                                'star_rating': 3, 'units': 5.0,
                                'confidence': 'FADE_FLIP',
                                'side_type': 'SPREAD_FADE_FLIP',
                                'spread_or_ml': 'SPREAD',
                                'timing': 'STANDARD',
                                'context': _fade_ctx,
                                'notes': _fade_ctx,
                            }
                            print(f"  🔄 SPREAD_FADE_FLIP: {sp.split('_')[-1]} {_f_team} "
                                  f"{_f_line:+.1f} @ {_f_book} {_f_odds:+.0f} (div {_fade_div:.1f})")
                            all_picks.append(_fade_pick)
                    except Exception as _e:
                        print(f"  ⚠ SPREAD_FADE_FLIP error: {_e}")

                _log_divergence_block(conn, sp, eid, home, away, ms, mkt_hs, 'post_elo_rescue')
                skip_div += 1; continue

            # ═══ CONTEXT MODEL PATH 2 — v25.44 (2026-04-21) ═══
            # Non-divergent games (Elo agrees with market within max_div).
            # Run Context Model and fire an own-pick at market line if Context
            # disagrees with market by >= sport-specific threshold. This turns
            # Context from a divergence rescuer (Path 1) into a general second
            # opinion engine (Path 2). Sport scope limited to where Phase A
            # 30-day backtest showed positive EV:
            #   NHL (thresh 0.5): 159 picks, 91-68, 57.2% WR, +73.6u
            #   NBA (thresh 2.5): 79 picks, 44-35, 55.7% WR, +25.0u
            #   Serie A (0.5):    12 picks, 6-3, 66.7% WR, +12.3u
            # MLB excluded — runline (±1.5) incompatible with additive Context
            # adjustments (49.6% WR, -70u backtest on 271 picks). MLS + other
            # soccer excluded — 30-day backtest lost at every threshold level.
            CONTEXT_PATH2_THRESHOLDS = {
                'icehockey_nhl': 0.5,
                'basketball_nba': 2.5,
                'soccer_italy_serie_a': 0.5,
            }
            _p2_th = CONTEXT_PATH2_THRESHOLDS.get(sp)
            if (_p2_th is not None
                    and mkt_hs is not None and mkt_as is not None
                    and mkt_hs_odds is not None and mkt_as_odds is not None):
                try:
                    from context_model import compute_context_spread, format_context_summary
                    _p2_commence = (commence[:10] if commence else None)
                    ms_ctx_p2, _p2_info = compute_context_spread(
                        conn, sp, home, away, eid, ms, _p2_commence)
                    _p2_disagreement = abs(ms_ctx_p2 - mkt_hs)
                    if _p2_disagreement >= _p2_th:
                        # ms_ctx < mkt_hs → Context more bullish on home → bet home
                        if ms_ctx_p2 < mkt_hs:
                            _p2_team, _p2_line, _p2_odds, _p2_book = home, mkt_hs, mkt_hs_odds, mkt_hs_book
                        else:
                            _p2_team, _p2_line, _p2_odds, _p2_book = away, mkt_as, mkt_as_odds, mkt_as_book
                        from config import MIN_ODDS as _P2_MIN_ODDS
                        if (_p2_odds is not None and _p2_odds > _P2_MIN_ODDS
                                and _p2_odds <= 140 and _p2_book):
                            _p2_summary = format_context_summary(_p2_info)

                            # v25.69: tag the dominant signal driving this fire.
                            # Live DATA_SPREAD sample is 0, so we don't know which
                            # sub-signals (form / momentum / injuries) produce the
                            # edge. Tag every fire with its dominant component so
                            # when n>=20 we can backtest per-dominance-bucket and
                            # decide which archetypes to keep vs throttle.
                            _dom_candidates = {
                                'form': abs(_p2_info.get('form_adj', 0) or 0),
                                'momentum': abs(_p2_info.get('momentum_adj', 0) or 0),
                                'injury': abs(_p2_info.get('injury_adj', 0) or 0),
                                'hca': abs(_p2_info.get('hca_adj', 0) or 0),
                                'injury_amp': abs(_p2_info.get('injury_amp_adj', 0) or 0),
                                'h2h': abs(_p2_info.get('h2h_adj', 0) or 0),
                                'rest': abs(_p2_info.get('rest_adj', 0) or 0),
                                'motivation': abs(_p2_info.get('mot_adj', 0) or 0),
                            }
                            _p2_dominance = max(_dom_candidates, key=_dom_candidates.get) \
                                if any(v > 0 for v in _dom_candidates.values()) else 'other'
                            _p2_dominance_val = _dom_candidates.get(_p2_dominance, 0)
                            _p2_dominance_share = (
                                _p2_dominance_val / sum(_dom_candidates.values())
                                if sum(_dom_candidates.values()) > 0 else 0
                            )

                            _p2_ctx = (
                                f'DATA_SPREAD v25.44 (Path 2) — {_p2_summary} | '
                                f'Market {mkt_hs:+.1f}, Context {ms_ctx_p2:+.1f} '
                                f'(ctx_disagreement={_p2_disagreement:.1f} ≥ {_p2_th}). '
                                f'Elo non-divergent (div={abs(ms-mkt_hs):.1f} ≤ {max_div}). '
                                f'Bet {_p2_team} {_p2_line:+.1f} @ {_p2_book} {_p2_odds:+.0f}. '
                                f'DOMINANCE:{_p2_dominance}({_p2_dominance_val:.1f}/{_p2_dominance_share*100:.0f}%)'
                            )
                            _p2_pick = {
                                'sport': sp, 'event_id': eid, 'commence': commence,
                                'home': home, 'away': away,
                                'market_type': 'SPREAD',
                                'selection': f'{_p2_team} {_p2_line:+.1f}',
                                'book': _p2_book, 'line': _p2_line, 'odds': _p2_odds,
                                'model_spread': ms,
                                'model_prob': 0, 'implied_prob': 0,
                                'edge_pct': 0,
                                'star_rating': 3, 'units': 5.0,
                                'confidence': 'DATA_SPREAD',
                                'side_type': 'DATA_SPREAD',
                                'spread_or_ml': 'SPREAD',
                                'timing': 'STANDARD',
                                'context': _p2_ctx,
                                'notes': _p2_ctx,
                            }
                            print(f"  🧠 DATA_SPREAD Path2: {sp.split('_')[-1]} {_p2_team} "
                                  f"{_p2_line:+.1f} @ {_p2_book} {_p2_odds:+.0f} "
                                  f"(ctx_disagreement {_p2_disagreement:.1f}, elo_div {abs(ms-mkt_hs):.1f})")
                            all_picks.append(_p2_pick)
                except Exception as _p2e:
                    print(f"  ⚠ DATA_SPREAD Path2 error: {_p2e}")

            # v12 FIX: If no spread line (common in baseball), check divergence via ML
            # Without this, baseball games with ML-only skip divergence entirely
            # and produce phantom 25-30% edges on thin data.
            if mkt_hs is None and hml is not None and aml is not None:
                # Infer market spread from ML odds
                _h_ml_imp = american_to_implied_prob(hml)
                _a_ml_imp = american_to_implied_prob(aml)
                if _h_ml_imp and _a_ml_imp:
                    # Rough ML-to-spread conversion using same ml_scale
                    import math as _m
                    ml_sc = cfg.get('ml_scale', 7.5)
                    # If _h_ml_imp > _a_ml_imp, home is favored → negative implied spread
                    if _h_ml_imp > 0.01 and _h_ml_imp < 0.99:
                        implied_spread = -ml_sc * _m.log(_h_ml_imp / (1 - _h_ml_imp))
                        if abs(ms - implied_spread) > max_div:
                            _log_divergence_block(conn, sp, eid, home, away, ms, implied_spread, 'ml_only_implied_spread')
                            skip_div += 1; continue

            # CONTEXT ADJUSTMENTS — schedule, travel, altitude, splits
            # Applied AFTER divergence check so legitimate context factors
            # don't get filtered out. These can push us further from the market
            # (confirming the edge) or closer (reducing it).
            ctx = None
            if HAS_CONTEXT:
                try:
                    ctx = get_context_adjustments(
                        conn, sp, home, away, eid, commence, 'SPREAD')
                    if ctx['spread_adj'] != 0:
                        ms -= ctx['spread_adj']  # Positive adj = home advantage = ms more negative
                except Exception as e:
                    pass  # Context is supplementary — don't crash on errors

            # ═══ TENNIS H2H ADJUSTMENT ═══
            # Some matchups are heavily lopsided regardless of Elo.
            # e.g., Djokovic owns Nadal on hard but Nadal dominates on clay.
            # Elo is a general rating — H2H captures matchup-specific edges.
            _h2h_ctx = ""
            if sp.startswith('tennis_'):
                try:
                    _h2h_adj, _h2h_ctx = _tennis_h2h_adjustment(conn, home, away, sp)
                    if _h2h_adj != 0:
                        ms += _h2h_adj  # Already signed: negative = favors home, positive = favors away
                except Exception:
                    pass

            # Injury data already fetched above (before Elo ML)
            inj_diff = a_imp - h_imp  # Positive = away more hurt = home advantage
            inj_spread_adj = round(inj_diff * 0.5, 2)  # 50% weight — market prices some
            ms_inj = ms - inj_spread_adj if abs(inj_spread_adj) >= 0.5 else ms

            # v25.29: DIV_EXPANDED tracking — picks that only pass due to a loosened
            # divergence threshold get tagged + unit-capped so we can monitor whether
            # the loosening was right. If backtest-justified WR doesn't hold live, agents
            # flag it. Currently only NHL (raised 1.5→2.5 on 2026-04-18).
            _DIV_EXPANDED_ORIG = {'icehockey_nhl': 1.5}
            _orig_div = _DIV_EXPANDED_ORIG.get(sp)
            _is_div_expanded = (
                _orig_div is not None and mkt_hs is not None
                and abs(ms - mkt_hs) > _orig_div
            )

            # HOME SPREAD
            if mkt_hs is not None and mkt_hs_odds is not None:
                k = f"{eid}|S|{home}"
                if k not in seen:
                    wa = scottys_edge_assessment(ms_inj, mkt_hs, mkt_hs_odds, sp, hml, a_inj, a_cl)
                    if wa['is_play'] and wa['point_value_pct'] >= min_pv:
                        prob = spread_to_cover_prob(ms, mkt_hs, sp)
                        # v12.2: Soccer draw adjustment for spreads
                        if 'soccer' in sp:
                            draw_p = _soccer_draw_prob(abs(ms))
                            prob = prob * (1.0 - draw_p * 0.5)
                        imp = american_to_implied_prob(mkt_hs_odds)
                        pick = _mk(sp, eid, commence, home, away, 'SPREAD',
                            f"{home} {mkt_hs:+.1f}", mkt_hs_book, mkt_hs, mkt_hs_odds,
                            ms, prob, imp, wa, 'home_spread')
                        if pick:
                            # Soccer Kelly boost: 1/3 Kelly (~2.7x standard).
                            # Soccer edges are proven (EPL +21% ROI, L1 +27%) but
                            # point values are smaller due to tight spreads (±0.25-1.5).
                            # Standard 1/8 Kelly undersizes soccer bets.
                            if 'soccer' in sp:
                                pick['units'] = kelly_units(edge_pct=wa['point_value_pct'], odds=mkt_hs_odds, fraction=0.333)
                            # Build context: base factors + per-pick line movement
                            _ctx_parts = [ctx['summary']] if ctx and ctx['summary'] else []
                            if _h2h_ctx:
                                _ctx_parts.append(_h2h_ctx)
                            if sp == 'icehockey_nhl' and _nhl_goalie_info and _nhl_goalie_info.get('summary'):
                                _ctx_parts.append(f"GOALIE: {_nhl_goalie_info['summary']}")
                            if HAS_CONTEXT:
                                try:
                                    _lm_move, _lm_sig, _lm_conf = line_movement_signal(
                                        conn, eid, f"{home} {mkt_hs:+.1f}", 'spreads')
                                    if _lm_sig == 'SHARP_AGREE':
                                        _ctx_parts.append(f"Sharp money agrees ({_lm_move:+.1f})")
                                    elif _lm_sig == 'PUBLIC_SIDE':
                                        _ctx_parts.append(f"Public side ({_lm_move:+.1f})")
                                except Exception:
                                    pass
                            if _ctx_parts:
                                pick['context'] = ' | '.join(_ctx_parts)
                                if ctx:
                                    pick['context_adj'] = ctx['spread_adj']
                            # v25.29: tag + unit-cap for DIV_EXPANDED picks
                            if _is_div_expanded:
                                _div_val = abs(ms - mkt_hs)
                                _div_tag = f'DIV EXPANDED v25.29 — div {_div_val:.1f} (orig threshold {_orig_div})'
                                pick['context'] = f"{pick.get('context','')} | {_div_tag}".strip(' |')
                                pick['side_type'] = 'DIV_EXPANDED'
                                pick['units'] = min(pick.get('units', 3.5), 3.5)
                            seen.add(k)
                            all_picks.append(pick)
                    else: skip_w += 1

            # AWAY SPREAD
            if mkt_as is not None and mkt_as_odds is not None:
                k = f"{eid}|S|{away}"
                if k not in seen:
                    wa = scottys_edge_assessment(-ms_inj, mkt_as, mkt_as_odds, sp, aml, h_inj, h_cl)
                    if wa['is_play'] and wa['point_value_pct'] >= min_pv:
                        prob = spread_to_cover_prob(-ms, mkt_as, sp)
                        # v12.2: Soccer draw adjustment for spreads
                        if 'soccer' in sp:
                            draw_p = _soccer_draw_prob(abs(ms))
                            prob = prob * (1.0 - draw_p * 0.5)
                        imp = american_to_implied_prob(mkt_as_odds)
                        pick = _mk(sp, eid, commence, home, away, 'SPREAD',
                            f"{away} {mkt_as:+.1f}", mkt_as_book, mkt_as, mkt_as_odds,
                            ms, prob, imp, wa, 'away_spread')
                        if pick:
                            # Soccer Kelly boost (same as home spread)
                            if 'soccer' in sp:
                                pick['units'] = kelly_units(edge_pct=wa['point_value_pct'], odds=mkt_as_odds, fraction=0.333)
                            # Build context: base factors + per-pick line movement
                            _ctx_parts = [ctx['summary']] if ctx and ctx['summary'] else []
                            if _h2h_ctx:
                                _ctx_parts.append(_h2h_ctx)
                            if sp == 'icehockey_nhl' and _nhl_goalie_info and _nhl_goalie_info.get('summary'):
                                _ctx_parts.append(f"GOALIE: {_nhl_goalie_info['summary']}")
                            if HAS_CONTEXT:
                                try:
                                    _lm_move, _lm_sig, _lm_conf = line_movement_signal(
                                        conn, eid, f"{away} {mkt_as:+.1f}", 'spreads')
                                    if _lm_sig == 'SHARP_AGREE':
                                        _ctx_parts.append(f"Sharp money agrees ({_lm_move:+.1f})")
                                    elif _lm_sig == 'PUBLIC_SIDE':
                                        _ctx_parts.append(f"Public side ({_lm_move:+.1f})")
                                except Exception:
                                    pass
                            if _ctx_parts:
                                pick['context'] = ' | '.join(_ctx_parts)
                                if ctx:
                                    pick['context_adj'] = ctx['spread_adj']
                            # v25.29: tag + unit-cap for DIV_EXPANDED picks (away spread)
                            if _is_div_expanded:
                                _div_val = abs(ms - mkt_hs)
                                _div_tag = f'DIV EXPANDED v25.29 — div {_div_val:.1f} (orig threshold {_orig_div})'
                                pick['context'] = f"{pick.get('context','')} | {_div_tag}".strip(' |')
                                pick['side_type'] = 'DIV_EXPANDED'
                                pick['units'] = min(pick.get('units', 3.5), 3.5)
                            seen.add(k)
                            all_picks.append(pick)
                    else: skip_w += 1

            # HOME ML
            # ═══ BASEBALL: Elo win probability + PITCHER-ADJUSTED ML ═══
            # v14: ML was 17W-25L (-15.9% ROI) without pitcher data.
            # v15: CONDITIONAL re-enable — only when pitcher quality data exists
            # for BOTH teams AND shows a significant gap (one team's starter
            # is much better). Dogs still blocked (they were the biggest losers).
            # Minimum 15% edge threshold (higher bar since ML is unproven).
            # Run lines remain DISABLED (1W-5L -60.8% ROI).
            if 'baseball' in sp and hml is not None and aml is not None:
                if HAS_ELO and elo_data:
                    from elo_engine import elo_win_probability
                    home_prob = elo_win_probability(home, away, elo_data, sp, neutral_site=_neutral)
                    if home_prob is not None:
                        away_prob = 1.0 - home_prob
                        h_fair, a_fair, _ = devig_ml_odds(hml, aml)

                        if h_fair and a_fair:
                            h_edge = (home_prob - h_fair) * 100
                            a_edge = (away_prob - a_fair) * 100

                            # ── Run line evaluation (±1.5) ──
                            # Compare run line to ML — recommend whichever has better EV.
                            # v14: Actual data shows 78.5% of college wins are by 2+ runs (was 72%)
                            # Metal bats + college pitching depth = bigger margins
                            # Lose-by-1 rate: 21.5% of decided games (was 30%)
                            WIN_BY_2_PCT = 0.785 if 'ncaa' in sp else 0.68
                            LOSE_BY_1_PCT = 0.215

                            # v14: Run lines DISABLED — backtest 1W-5L -60.8% ROI
                            if False and mkt_hs is not None and mkt_as is not None:
                                # Check both sides of run line
                                for rl_team, rl_line, rl_odds, rl_book, rl_win_prob, rl_side in [
                                    (away, mkt_as, mkt_as_odds, mkt_as_book, away_prob, 'away'),
                                    (home, mkt_hs, mkt_hs_odds, mkt_hs_book, home_prob, 'home'),
                                ]:
                                    if rl_line is None or rl_odds is None:
                                        continue

                                    k_rl = f"{eid}|S|{rl_team}"
                                    if k_rl in seen:
                                        continue

                                    # Calculate cover probability
                                    if rl_line == -1.5:
                                        # Favorite -1.5: must win by 2+
                                        cover_prob = rl_win_prob * WIN_BY_2_PCT
                                    elif rl_line == 1.5:
                                        # Underdog +1.5: win OR lose by exactly 1
                                        cover_prob = rl_win_prob + (1 - rl_win_prob) * LOSE_BY_1_PCT
                                    else:
                                        continue

                                    rl_imp = american_to_implied_prob(rl_odds)
                                    rl_edge = (cover_prob - rl_imp) * 100 if rl_imp else 0

                                    if rl_edge >= 10.0:
                                        stars = get_star_rating(rl_edge)
                                        if stars > 0:
                                            timing = 'EARLY' if rl_line < 0 else 'LATE'
                                            t_r = 'Favorite' if rl_line < 0 else 'Dog'
                                            seen.add(k_rl)
                                            pick = _mk(sp, eid, commence, home, away, 'SPREAD',
                                                f"{rl_team} {rl_line:+.1f}", rl_book, rl_line, rl_odds,
                                                ms, round(cover_prob, 4), round(rl_imp, 4),
                                                {'point_value_pct': round(rl_edge, 1), 'star_rating': stars,
                                                 'units': kelly_units(edge_pct=rl_edge, odds=rl_odds),
                                                 'is_play': True, 'timing': timing, 'timing_reason': t_r,
                                                 'spread_or_ml': 'RUN_LINE', 'confidence': _conf(stars),
                                                 'vig_adjusted_spread': rl_line, 'raw_spread_diff': 0,
                                                 'injury_multiplier': 1.0, 'spread_or_ml_reason': 'Run line'},
                                                f'{rl_side}_spread')
                                            if pick:
                                                pick['notes'] = f"RL: P(cover)={cover_prob:.0%} vs imp={rl_imp:.0%}"
                                                if ctx and ctx['summary']:
                                                    pick['context'] = ctx['summary']
                                                all_picks.append(pick)

                            # ── ML evaluation (v15: pitcher-conditional) ──
                            # Only enabled when BOTH teams have pitcher quality data
                            # AND there's a significant pitching gap. Dogs blocked.
                            # Minimum 15% edge (higher bar than other sports).

                            # Fetch pitcher context for ML adjustment
                            _bb_pitcher_ctx = None
                            _bb_ml_allowed = False
                            _bb_pitcher_adj = 0.0
                            BASEBALL_ML_MIN_EDGE = 15.0       # Higher bar — unproven market
                            BASEBALL_ML_PITCHER_GAP = 1.5     # Min runs-allowed gap between starters
                            BASEBALL_ML_MAX_PROB_ADJ = 0.08   # Cap pitcher adjustment at ±8%

                            if HAS_PITCHER:
                                try:
                                    _bb_pitcher_ctx = get_pitcher_context(conn, home, away, commence, sport=sp)
                                except Exception:
                                    _bb_pitcher_ctx = None

                            if _bb_pitcher_ctx and _bb_pitcher_ctx['confidence'] != 'LOW':
                                # Both teams need pitching data (adj != 0 means data exists)
                                h_pa = _bb_pitcher_ctx['home_pitching_adj']
                                a_pa = _bb_pitcher_ctx['away_pitching_adj']
                                _has_both = (h_pa != 0.0 or a_pa != 0.0)  # At least one non-zero

                                # Check for significant gap: one team's pitching is
                                # meaningfully better than the other's (in runs allowed)
                                pitcher_gap = abs(h_pa - a_pa)

                                if _has_both and pitcher_gap >= BASEBALL_ML_PITCHER_GAP:
                                    _bb_ml_allowed = True
                                    # Pitcher adjustment to win probability:
                                    # Better pitching (lower runs allowed) = higher win prob
                                    # home_pitching_adj negative = home allows fewer runs = good
                                    # Scale: 1 run difference ≈ 3% win probability shift
                                    raw_adj = (a_pa - h_pa) * 0.03  # positive = home pitching better
                                    _bb_pitcher_adj = max(-BASEBALL_ML_MAX_PROB_ADJ,
                                                         min(BASEBALL_ML_MAX_PROB_ADJ, raw_adj))

                            # Apply pitcher adjustment to probabilities
                            if _bb_ml_allowed and _bb_pitcher_adj != 0.0:
                                home_prob_adj = home_prob + _bb_pitcher_adj
                                away_prob_adj = 1.0 - home_prob_adj
                                # Clamp probabilities
                                home_prob_adj = max(0.05, min(0.95, home_prob_adj))
                                away_prob_adj = max(0.05, min(0.95, away_prob_adj))
                                h_edge = (home_prob_adj - h_fair) * 100
                                a_edge = (away_prob_adj - a_fair) * 100

                            # Home ML — FAVORITES ONLY when pitcher data confirms edge
                            k_h = f"{eid}|M|{home}"
                            if (_bb_ml_allowed and k_h not in seen
                                    and h_edge >= BASEBALL_ML_MIN_EDGE
                                    and hml < 0):  # Favorites only (negative ML = favorite)
                                stars = get_star_rating(h_edge)
                                if stars > 0:
                                    timing, t_r = bet_timing_advice(ms, mkt_hs or 0)
                                    seen.add(k_h)
                                    _adj_prob = home_prob_adj if _bb_pitcher_adj != 0.0 else home_prob
                                    pick = _mk_ml(sp, eid, commence, home, away,
                                        f"{home} ML", hml_book, hml, ms,
                                        round(_adj_prob, 4), round(h_fair, 4),
                                        round(h_edge, 2), stars, timing, t_r)
                                    if pick:
                                        pick['notes'] += f" | PITCHER: {_bb_pitcher_ctx['summary']}"
                                        if ctx and ctx['summary']:
                                            pick['context'] = ctx['summary']
                                        all_picks.append(pick)

                            # Away ML — FAVORITES ONLY when pitcher data confirms edge
                            k_a = f"{eid}|M|{away}"
                            if (_bb_ml_allowed and k_a not in seen
                                    and a_edge >= BASEBALL_ML_MIN_EDGE
                                    and aml < 0):  # Favorites only (negative ML = favorite)
                                stars = get_star_rating(a_edge)
                                if stars > 0:
                                    timing, t_r = bet_timing_advice(-ms, mkt_as or 0)
                                    seen.add(k_a)
                                    _adj_prob = away_prob_adj if _bb_pitcher_adj != 0.0 else away_prob
                                    pick = _mk_ml(sp, eid, commence, home, away,
                                        f"{away} ML", aml_book, aml, ms,
                                        round(_adj_prob, 4), round(a_fair, 4),
                                        round(a_edge, 2), stars, timing, t_r)
                                    if pick:
                                        pick['notes'] += f" | PITCHER: {_bb_pitcher_ctx['summary']}"
                                        if ctx and ctx['summary']:
                                            pick['context'] = ctx['summary']
                                        all_picks.append(pick)
                # Skip generic ML evaluation for baseball
                elif 'baseball' in sp:
                    pass
            
            # ═══ WALTERS ML EVALUATION (non-baseball) ═══
            # Power ratings → win probability → compare to de-vigged ML odds.
            # When Elo data available, use elo_win_probability() directly instead
            # of compressed spread_to_win_prob(). This produces BOTH favorite and
            # underdog ML picks based on genuine probability edge.
            elif hml is not None and aml is not None and 'baseball' not in sp:
                # Get win probabilities: Elo-direct when available, spread-derived fallback
                # Apply confidence weighting for Elo-backed edges
                _walters_elo_w = 1.0
                if HAS_ELO and elo_data:
                    from elo_engine import elo_win_probability
                    _elo_p = elo_win_probability(home, away, elo_data, sp, neutral_site=_neutral)
                    if _elo_p is not None:
                        h_prob_ml, a_prob_ml = _elo_p, 1.0 - _elo_p
                        # Confidence weight: scale edge by data quality
                        h_d = elo_data.get(home, {})
                        a_d = elo_data.get(away, {})
                        _mgp = min(h_d.get('games', 0), a_d.get('games', 0))
                        _walters_elo_w = min(1.0, _mgp / 15.0)
                    else:
                        h_prob_ml = spread_to_win_prob(ms_inj, sp)
                        a_prob_ml = 1.0 - h_prob_ml
                else:
                    h_prob_ml = spread_to_win_prob(ms_inj, sp)
                    a_prob_ml = 1.0 - h_prob_ml

                # ═══ INJURY ADJUSTMENT FOR WALTERS ML ═══
                _inj_prob_shift = (a_imp - h_imp) * 0.015
                if abs(_inj_prob_shift) >= 0.01:
                    h_prob_ml = max(0.05, min(0.95, h_prob_ml + _inj_prob_shift))
                    a_prob_ml = 1.0 - h_prob_ml

                # Hard gate: block ML pick if picked team has star out
                _home_star_out = h_imp >= 5.0
                _away_star_out = a_imp >= 5.0

                # De-vig market ML odds (soccer includes draw)
                if 'soccer' in sp:
                    h_prob_ml, draw_prob_ml, a_prob_ml = soccer_ml_probs(ms, sp)
                    draw_row = conn.execute("""
                        SELECT odds FROM odds
                        WHERE event_id=? AND market='h2h' AND selection='Draw'
                        AND snapshot_date=(SELECT MAX(snapshot_date) FROM odds WHERE event_id=? AND market='h2h')
                        ORDER BY odds DESC LIMIT 1
                    """, (eid, eid)).fetchone()
                    d_odds = draw_row[0] if draw_row else None
                    h_imp_ml, a_imp_ml, _ = devig_ml_odds(hml, aml, d_odds)
                else:
                    h_imp_ml, a_imp_ml, _ = devig_ml_odds(hml, aml)

                # Tennis ML cap: skip big favorites and long-shot dogs
                _tennis_ml_cap = None
                if sp.startswith('tennis_'):
                    try:
                        from config import TENNIS_ML_CAP
                        _tennis_ml_cap = TENNIS_ML_CAP
                    except ImportError:
                        _tennis_ml_cap = 200

                # HOME ML
                k = f"{eid}|M|{home}"
                if k not in seen and h_imp_ml:
                    if 'soccer' in sp:
                        pass  # v13: ALL soccer ML disabled — backtest 0W-8L. Edge lives in spreads only.
                    elif _tennis_ml_cap and abs(hml) > _tennis_ml_cap:
                        pass  # Tennis: line too wide, skip
                    elif _home_star_out:
                        print(f"    ⚠ INJURY GATE: {home} ML blocked — star player out ({h_imp:.1f} pts impact)")
                    else:
                        h_edge_ml = (h_prob_ml - h_imp_ml) * 100 * _walters_elo_w
                        stars = get_star_rating(h_edge_ml)
                        if h_edge_ml >= min_pv_ml and stars > 0:
                            timing, t_r = bet_timing_advice(ms, mkt_hs or 0)
                            seen.add(k)
                            pick = _mk_ml(sp, eid, commence, home, away,
                                f"{home} ML", hml_book, hml, ms,
                                round(h_prob_ml, 4), round(h_imp_ml, 4),
                                round(h_edge_ml, 2), stars, timing, t_r)
                            if pick:
                                _ml_ctx_parts = []
                                if ctx and ctx['summary']:
                                    _ml_ctx_parts.append(ctx['summary'])
                                if _h2h_ctx:
                                    _ml_ctx_parts.append(_h2h_ctx)
                                _ml_ctx_parts.append('Elo probability edge')
                                if h_imp > 0 or a_imp > 0:
                                    _ml_ctx_parts.append(f'Injuries: {home} -{h_imp:.1f}pts, {away} -{a_imp:.1f}pts')
                                pick['context'] = ' | '.join(_ml_ctx_parts)
                                all_picks.append(pick)

                # AWAY ML
                k = f"{eid}|M|{away}"
                if k not in seen and a_imp_ml:
                    if 'soccer' in sp:
                        pass  # v13: ALL soccer ML disabled — backtest 0W-8L. Edge lives in spreads only.
                    elif _tennis_ml_cap and abs(aml) > _tennis_ml_cap:
                        pass  # Tennis: line too wide, skip
                    elif _away_star_out:
                        print(f"    ⚠ INJURY GATE: {away} ML blocked — star player out ({a_imp:.1f} pts impact)")
                    else:
                        a_edge_ml = (a_prob_ml - a_imp_ml) * 100 * _walters_elo_w
                        stars = get_star_rating(a_edge_ml)
                        if a_edge_ml >= min_pv_ml and stars > 0:
                            timing, t_r = bet_timing_advice(-ms, mkt_as or 0)
                            seen.add(k)
                            pick = _mk_ml(sp, eid, commence, home, away,
                                f"{away} ML", aml_book, aml, ms,
                                round(a_prob_ml, 4), round(a_imp_ml, 4),
                                round(a_edge_ml, 2), stars, timing, t_r)
                            if pick:
                                _ml_ctx_parts = []
                                if ctx and ctx['summary']:
                                    _ml_ctx_parts.append(ctx['summary'])
                                if _h2h_ctx:
                                    _ml_ctx_parts.append(_h2h_ctx)
                                _ml_ctx_parts.append('Elo probability edge')
                                if h_imp > 0 or a_imp > 0:
                                    _ml_ctx_parts.append(f'Injuries: {home} -{h_imp:.1f}pts, {away} -{a_imp:.1f}pts')
                                pick['context'] = ' | '.join(_ml_ctx_parts)
                                all_picks.append(pick)

            # DRAW (soccer only — 3-way market)
            if 'soccer' in sp:
                k = f"{eid}|M|DRAW"
                if k not in seen:
                    # Get best draw odds from raw odds table
                    draw_row = conn.execute("""
                        SELECT odds, book FROM odds
                        WHERE event_id=? AND market='h2h' AND selection='Draw'
                        AND snapshot_date=(SELECT MAX(snapshot_date) FROM odds WHERE event_id=? AND market='h2h')
                        ORDER BY odds DESC LIMIT 1
                    """, (eid, eid)).fetchone()
                    if draw_row:
                        draw_odds, draw_book = draw_row
                        # Estimate draw probability from model spread
                        # Close games (small spread) have higher draw probability
                        # Soccer draw rate: ~25% on average, but varies by spread
                        draw_prob = _soccer_draw_prob(abs(ms))
                        _, _, imp = devig_ml_odds(hml, aml, draw_odds)
                        if imp is None:
                            imp = american_to_implied_prob(draw_odds)
                        edge = (draw_prob - imp) * 100 if imp else 0
                        edge = min(edge, 20.0)  # v20: cap at 20%
                        stars = get_star_rating(edge)
                        if edge >= min_pv and stars > 0:
                            seen.add(k)
                            all_picks.append({
                                'sport': sp, 'event_id': eid, 'commence': commence,
                                'home': home, 'away': away, 'market_type': 'MONEYLINE',
                                'selection': f"DRAW ({home} vs {away})",
                                'book': draw_book, 'line': None, 'odds': draw_odds,
                                'model_spread': ms, 'model_prob': round(draw_prob, 4),
                                'implied_prob': round(imp, 4) if imp else None,
                                'edge_pct': round(edge, 2), 'star_rating': stars,
                                'units': kelly_units(edge_pct=edge, odds=draw_odds),
                                'confidence': _conf(stars),
                                'spread_or_ml': 'DRAW', 'timing': 'EARLY',
                                'notes': f"DrawProb={draw_prob:.1%} Imp={imp:.1%} Edge={edge:.1f}% "
                                         f"ModelSpread={ms:+.1f} | Close game → draw value",
                            })

            # ═══ CROSS-MARKET EDGE: Spread-implied ML vs actual ML ═══
            # If spread says Team A wins 65% but ML implies 58%, that's a 7% edge on ML
            if hml is not None and mkt_hs is not None:
                if 'soccer' in sp:
                    spread_win_prob, _, away_spread_prob = soccer_ml_probs(ms, sp)
                else:
                    spread_win_prob = spread_to_win_prob(ms, sp)
                    away_spread_prob = 1.0 - spread_win_prob
                ml_implied, aml_dv, _ = devig_ml_odds(hml, aml)
                if ml_implied is None:
                    ml_implied = american_to_implied_prob(hml)
                if ml_implied and spread_win_prob and 'soccer' not in sp and 'baseball' not in sp:
                    # v21: Soccer cross-market ML disabled — 0W-8L historically.
                    # v25.5 (4/10/2026): Baseball cross-mkt disabled — OPPORTUNITY COST.
                    #   Historical fires: 5 non-tainted picks across 6 weeks, 2W-3L, -1.5u net.
                    #   Almost zero alpha. But the feature is FLOODING baseball pick generation
                    #   today (14 cross-mkt baseball_ncaa + 2 baseball_mlb per run) and eating
                    #   the per-sport-soft cap (5/run), squeezing out OVER/UNDER picks where
                    #   the model has its strongest edge (NCAA baseball OVERs: 21-10, +39.7u
                    #   season profit). The opportunity cost of keeping cross-mkt — losing
                    #   5-15u/day of OVER/UNDER alpha — vastly exceeds the historical break-even
                    #   value of the cross-mkt picks themselves.
                    #   v25.3 (earlier today) tried this and reverted because the catastrophic
                    #   CLVs that justified the kill turned out to be timezone-bug artifacts.
                    #   v25.5 ships with a DIFFERENT justification: not "feature is broken" but
                    #   "feature crowds out our best edge." Direct ML picks still fire from the
                    #   regular spread/ML pipeline (e.g., Vandy ML on 4/9 was direct, not cross-mkt).
                    # Shadow-tracked: agent monitors if this changes.
                    cross_edge = min((spread_win_prob - ml_implied) * 100, 20.0)  # v20: cap at 20%
                    if cross_edge > 8.0:
                        k = f"{eid}|X|{home}"
                        if k not in seen:
                            stars = get_star_rating(cross_edge)
                            if stars >= 2.0:
                                timing, t_r = bet_timing_advice(ms, mkt_hs or 0)
                                seen.add(k)
                                all_picks.append({
                                    'sport': sp, 'event_id': eid, 'commence': commence,
                                    'home': home, 'away': away, 'market_type': 'MONEYLINE',
                                    'selection': f"{home} ML (cross-mkt)",
                                    'book': hml_book, 'line': None, 'odds': hml,
                                    'model_spread': ms, 'model_prob': round(spread_win_prob, 4),
                                    'implied_prob': round(ml_implied, 4),
                                    'edge_pct': round(cross_edge, 2), 'star_rating': stars,
                                    'units': kelly_units(edge_pct=cross_edge, odds=hml), 'confidence': _conf(stars),
                                    'spread_or_ml': 'CROSS_MKT', 'timing': timing,
                                    'notes': f"Spread→{spread_win_prob:.1%} ML→{ml_implied:.1%} "
                                             f"Gap={cross_edge:.1f}% | Cross-market discrepancy",
                                })

                    # Check away side too
                    aml_implied = aml_dv  # Already de-vigged above
                    if aml_implied is None and aml:
                        _, aml_implied, _ = devig_ml_odds(hml, aml)
                    if aml_implied and away_spread_prob:
                        cross_edge_a = min((away_spread_prob - aml_implied) * 100, 20.0)  # v20: cap at 20%
                        if cross_edge_a > 8.0:
                            k = f"{eid}|X|{away}"
                            if k not in seen:
                                stars = get_star_rating(cross_edge_a)
                                if stars >= 2.0:
                                    seen.add(k)
                                    all_picks.append({
                                        'sport': sp, 'event_id': eid, 'commence': commence,
                                        'home': home, 'away': away, 'market_type': 'MONEYLINE',
                                        'selection': f"{away} ML (cross-mkt)",
                                        'book': aml_book, 'line': None, 'odds': aml,
                                        'model_spread': ms, 'model_prob': round(away_spread_prob, 4),
                                        'implied_prob': round(aml_implied, 4),
                                        'edge_pct': round(cross_edge_a, 2), 'star_rating': stars,
                                        'units': kelly_units(edge_pct=cross_edge_a, odds=aml), 'confidence': _conf(stars),
                                        'spread_or_ml': 'CROSS_MKT', 'timing': 'EARLY',
                                        'notes': f"Spread→{away_spread_prob:.1%} ML→{aml_implied:.1%} "
                                                 f"Gap={cross_edge_a:.1f}% | Cross-market discrepancy",
                                    })

            # ═══ TOTALS (OVER/UNDER) ═══
            over_total = g[10]
            over_odds = g[11]
            over_book = g[12]
            under_total = g[13]
            under_odds = g[14]
            under_book = g[15]

            # ═══ CONTEXT MODEL TOTALS — v25.47/v25.48 (2026-04-21) ═══
            # Runs BEFORE the MLS hard-block and BEFORE the edge-based model so
            # Path 2 own-picks fire independently. Scope + thresholds come from
            # Phase A with goalie-form, soccer-standings, and ref-tendency
            # signals layered onto form+H2H+MLB-pitcher:
            #   NBA    (0.30 pts): 173 picks, 58.7%, +97.4u
            #   NHL    (1.00 gls):  95 picks, 60.6%, +52.0u
            #   MLB    (1.50 run):  68 picks, 56.9%, +20.9u
            #   MLS    (0.30 gls):  15 picks, 66.7%, +14.7u
            #   La Liga/Bundesliga/Ligue 1 (0.30 gls): 4-5 picks each,
            #     75-80% WR, +4-12u (v25.48 — tiny samples, re-eval at n>=20)
            # Non-soccer sports: single threshold regardless of direction
            CONTEXT_TOTAL_P2_THRESHOLDS_V47 = {
                'basketball_nba': 0.30,
                'icehockey_nhl':  1.00,
                'baseball_mlb':   1.50,
            }
            # v25.65 (2026-04-22) — Soccer rules are per-sport × direction.
            # Inverse backtest (90d, n=133) confirmed Context FOLLOW wins overall
            # (+101u vs fade -5u). Soccer FOLLOW was actually the STRONGEST slice
            # at +55u / 37 picks / 64.9% WR. But 2 specific cohorts invert:
            #   EPL UNDER  (n=9, 37.5% WR, fade wins +5.30u)
            #   MLS UNDER  (n=5, 40% WR,   fade wins +2.80u)
            # Soccer OVER direction has only n=5 historical samples across all
            # 7 leagues combined — insufficient for live firing. Today's 9-OVER
            # spike that triggered the v25.63 halt was exactly this thin
            # direction firing all at once.
            # Rule values:
            #   number  → FOLLOW at that threshold (fire if |gap| >= value)
            #   'shadow' → log to shadow_blocked_picks; don't fire. Used for
            #              directions where we have no historical validation.
            #   'block'  → skip entirely (cohort known to be losing on follow).
            # Missing sport-key → skip (out of scope entirely).
            CONTEXT_TOTAL_P2_SOCCER_RULES = {
                'soccer_italy_serie_a':      {'UNDER': 0.30, 'OVER': 'shadow'},  # 7-1 +18.95u
                'soccer_france_ligue_one':   {'UNDER': 0.50, 'OVER': 'shadow'},  # 3-0 +30.37u at >=0.50
                'soccer_germany_bundesliga': {'UNDER': 'shadow', 'OVER': 'shadow'},  # n<=1
                'soccer_usa_mls':            {'UNDER': 'block', 'OVER': 'shadow'},  # UNDER fade cohort
                'soccer_epl':                {'UNDER': 'block', 'OVER': 'shadow'},  # UNDER fade cohort
                'soccer_spain_la_liga':      {'UNDER': 'shadow', 'OVER': 'shadow'},  # n<=1
                'soccer_uefa_champs_league': {'UNDER': 'shadow', 'OVER': 'shadow'},  # n<=1
            }

            def _log_context_shadow(sport_, eid_, sel_, line_, direction_, gap_, reason_tag):
                """Log Context Path 2 candidate to shadow_blocked_picks so
                we accumulate calibration data without firing live."""
                try:
                    from datetime import datetime as _dt
                    conn.execute("""INSERT INTO shadow_blocked_picks
                        (created_at, sport, event_id, selection, market_type, book,
                         line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, 'TOTAL', '', ?, ?, 0, 0, ?)""",
                        (_dt.now().isoformat(), sport_, eid_, sel_, line_, 0,
                         f'CONTEXT_TOTAL_P2_{reason_tag} (v25.65 — direction={direction_}, gap={gap_:+.2f})'))
                    conn.commit()
                except Exception:
                    pass

            # Resolve threshold & rule for this sport
            _ct_th = None
            _soccer_rules = CONTEXT_TOTAL_P2_SOCCER_RULES.get(sp)
            if _soccer_rules is None:
                _ct_th = CONTEXT_TOTAL_P2_THRESHOLDS_V47.get(sp)
            if ((_ct_th is not None or _soccer_rules is not None)
                    and over_total is not None and over_odds is not None
                    and under_total is not None and under_odds is not None):
                try:
                    from context_model import (
                        compute_context_total, format_context_total_summary,
                    )
                    _ct_commence = (commence[:10] if commence else None)
                    _market_total = over_total
                    ctx_tot, ct_info = compute_context_total(
                        conn, sp, home, away, eid, _market_total, _ct_commence)
                    _ct_disagreement = ctx_tot - _market_total
                    _ct_side = 'OVER' if _ct_disagreement > 0 else 'UNDER'

                    # v25.65: per-direction rules for soccer
                    if _soccer_rules is not None:
                        _rule = _soccer_rules.get(_ct_side)
                        _sel_label = f'{away}@{home} {_ct_side} {over_total if _ct_side == "OVER" else under_total}'
                        if _rule == 'block':
                            _log_context_shadow(sp, eid, _sel_label,
                                over_total if _ct_side == 'OVER' else under_total,
                                _ct_side, _ct_disagreement, 'BLOCKED_FADE_COHORT')
                            _ct_th = None  # signal: do not fire
                        elif _rule == 'shadow':
                            _log_context_shadow(sp, eid, _sel_label,
                                over_total if _ct_side == 'OVER' else under_total,
                                _ct_side, _ct_disagreement, 'SHADOW_INSUFFICIENT_SAMPLE')
                            _ct_th = None
                        elif isinstance(_rule, (int, float)):
                            _ct_th = _rule
                        else:
                            _ct_th = None

                    if _ct_th is not None and abs(_ct_disagreement) >= _ct_th:
                        if _ct_disagreement > 0:
                            _ct_side, _ct_line, _ct_odds, _ct_book = 'OVER', over_total, over_odds, over_book
                        else:
                            _ct_side, _ct_line, _ct_odds, _ct_book = 'UNDER', under_total, under_odds, under_book
                        from config import MIN_ODDS as _CT_MIN_ODDS
                        if (_ct_odds is not None and _ct_odds > _CT_MIN_ODDS
                                and _ct_odds <= 140 and _ct_book):
                            _ct_summary = format_context_total_summary(ct_info)
                            _ct_ctx = (
                                f'DATA_TOTAL v25.47 (Path 2) — {_ct_summary} | '
                                f'Market {_market_total}, Context {ctx_tot} '
                                f'(disagreement={_ct_disagreement:+.2f} ≥ {_ct_th}). '
                                f'Bet {_ct_side} {_ct_line} @ {_ct_book} {_ct_odds:+.0f}.'
                            )
                            _ct_pick = {
                                'sport': sp, 'event_id': eid, 'commence': commence,
                                'home': home, 'away': away,
                                'market_type': 'TOTAL',
                                'selection': f"{away}@{home} {_ct_side} {_ct_line}",
                                'book': _ct_book, 'line': _ct_line, 'odds': _ct_odds,
                                'model_spread': None, 'model_total': ctx_tot,
                                'model_prob': 0, 'implied_prob': 0,
                                'edge_pct': 0,
                                'star_rating': 3, 'units': 5.0,
                                'confidence': 'DATA_TOTAL',
                                'side_type': 'DATA_TOTAL',
                                'spread_or_ml': 'TOTAL',
                                'timing': 'STANDARD',
                                'context': _ct_ctx,
                                'notes': _ct_ctx,
                            }
                            print(f"  🧠 DATA_TOTAL Path2: {sp.split('_')[-1]} {_ct_side} "
                                  f"{_ct_line} @ {_ct_book} {_ct_odds:+.0f} "
                                  f"(disagreement {_ct_disagreement:+.2f})")
                            all_picks.append(_ct_pick)
                except Exception as _cte:
                    print(f"  ⚠ DATA_TOTAL Path2 error: {_cte}")

            # Hard block: MLS totals disabled for the EDGE-BASED model
            # (1W-7L -72.8% ROI from edge path). v25.47 Path 2 above handles
            # MLS via Context — fires own-picks at market line independently.
            if sp == 'soccer_usa_mls':
                over_total = None  # prevents edge-based totals evaluation below

            if over_total is not None and over_odds is not None and ms is not None:
                # Check confidence BEFORE estimating total
                total_conf = _totals_confidence(home, away, sp, conn)
                if total_conf == 'LOW':
                    pass  # Skip — insufficient data for totals prediction
                else:
                    # Estimate model total from team ratings + league average
                    model_total = estimate_model_total(home, away, ratings, sp, conn)
                    if model_total is not None:
                        # Save raw model total before context (for soccer direction gate)
                        _raw_model_total = model_total

                        # ═══ INJURY ADJUSTMENT FOR TOTALS ═══
                        # Missing scorers reduce expected scoring. A 20ppg NBA player
                        # out lowers the total. Use 50% of impact (market prices some).
                        # Only apply to sports where individual scoring matters.
                        if sp in ('basketball_nba', 'basketball_ncaab', 'icehockey_nhl'):
                            _inj_total_adj = (h_imp + a_imp) * 0.5  # Both teams' injuries reduce total
                            if _inj_total_adj >= 0.5:
                                model_total -= _inj_total_adj

                        # Apply context adjustments to total (pace, refs, altitude, H2H)
                        ctx_total = None
                        if HAS_CONTEXT:
                            ctx_total = get_context_adjustments(
                                conn, sp, home, away, eid, commence, 'TOTAL')
                            if ctx_total['total_adj'] != 0:
                                model_total += ctx_total['total_adj']

                        # v25.47 Path 2 for totals moved up before MLS hard-block —
                        # see CONTEXT_TOTAL_P2_THRESHOLDS_V47 block at line ~2676.

                        # Apply pitcher context for baseball (day-of-week quality + named starters)
                        pitcher_ctx = None
                        if HAS_PITCHER and 'baseball' in sp:
                            try:
                                pitcher_ctx = get_pitcher_context(conn, home, away, commence, sport=sp)
                                if pitcher_ctx['total_adj'] != 0 and pitcher_ctx['confidence'] != 'LOW':
                                    model_total += pitcher_ctx['total_adj']
                            except Exception:
                                pass

                        # ═══ MLB STARTER ERA ADJUSTMENT ═══
                        # Adjust total based on confirmed starter ERA vs league avg (4.00).
                        # Elite pitchers suppress scoring; bad pitchers inflate it.
                        # Uses box_scores season ERA (preferred) or ESPN ERA (fallback).
                        _pitcher_era_ctx = ''
                        _era_adj = 0.0
                        _best_era = None   # v25.3: lowest ERA in matchup (for asymmetric gate)
                        _worst_era = None  # v25.3: highest ERA in matchup
                        _both_era_reliable = False  # v25.14: both starters have confirmed ERA

                        # v25.17: NCAA midweek pitcher-data gate was considered and removed
                        # same-day. On reflection, NCAA baseball is best sport (+59u overall)
                        # and most picks fire without pitcher data anyway (only 6% of weekend
                        # picks had data, yet went 34-24 +25.4u). One bad-looking midweek day
                        # isn't a pattern. Keep the gate stub at False.
                        _ncaa_pitcher_data_veto = False

                        # v25.32: NCAA ERA RELIABILITY GATE — if pitcher_ctx returns named
                        # starters but EITHER has < 15 career IP (per `both_reliable` flag),
                        # skip the pick. The derived "ERA" on a 4-IP pitcher is noise-driven.
                        # Triggering case: Miami/Stanford UNDER 13.5 on 4/18 game 2 — scraper
                        # returned Marsh (4.1 career IP) and Evans (11.5 career IP) from
                        # game 1 of the doubleheader, built a 20% "edge" on unreliable data.
                        # Gate fires ONLY when pitcher_ctx exists and named starters exist but
                        # one is unreliable. Picks with no pitcher data at all still fire per
                        # historical pattern (weekend NCAA without pitcher data still +25u).
                        _ncaa_era_reliability_veto = False
                        if sp == 'baseball_ncaa' and pitcher_ctx:
                            if (pitcher_ctx.get('home_starter') and pitcher_ctx.get('away_starter')
                                and not pitcher_ctx.get('both_reliable', False)):
                                _ncaa_era_reliability_veto = True
                                try:
                                    _h_ip = pitcher_ctx.get('home_starter_ip') or 0
                                    _a_ip = pitcher_ctx.get('away_starter_ip') or 0
                                    conn.execute("""
                                        INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                                            market_type, line, odds, edge_pct, units, reason)
                                        VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                                    """, (datetime.now().isoformat(), sp, eid,
                                          f"{away}@{home}", None,
                                          f"NCAA_ERA_RELIABILITY_GATE ({pitcher_ctx['home_starter']} {_h_ip:.1f} IP, {pitcher_ctx['away_starter']} {_a_ip:.1f} IP; need >=15)"))
                                    conn.commit()
                                except Exception:
                                    pass

                        if sp == 'baseball_mlb' and _mlb_pitcher_info:
                            try:
                                _era_adj, _pitcher_era_ctx, _best_era, _worst_era, _both_era_reliable = _mlb_pitcher_era_adjustment(
                                    conn, _mlb_pitcher_info)
                                if _era_adj != 0:
                                    model_total += _era_adj
                                    # v25.3 (Fix 2): Pitcher quality is part of the RAW model,
                                    # not just context. Apply era_adj to _raw_model_total too,
                                    # so the v25.1 direction gate catches downstream context
                                    # (bullpen, H2H, pace) that contradicts pitcher quality.
                                    # Brewers/Nats 4/10: Patrick (3.27) made era_adj=+0.1, but
                                    # raw_model_total=8.5 still passed direction gate. Bullpen
                                    # +0.4 + H2H +1.1 + pace +0.4 then pushed final to 10.5.
                                    # By baking pitcher quality into raw, we keep the direction
                                    # gate honest about what the base model actually thinks.
                                    _raw_model_total += _era_adj
                            except Exception:
                                pass

                        # ═══ MLB PARK FACTOR — GATE ONLY (v24) ═══
                        # Park factor no longer adjusts model_total (was double-counting
                        # what the market already prices: 3W-6L -16.1u with park as edge).
                        # Now used as a GATE: if park contradicts the pick direction, veto.
                        # Hitter's park (adj > 0) vetoes UNDERs. Pitcher's park (adj < 0) vetoes OVERs.
                        _park_factor_ctx = ''
                        _park_gate_adj = 0.0  # Raw park adj for gate logic
                        if sp == 'baseball_mlb':
                            try:
                                _, _park_factor_ctx, _park_gate_adj = _mlb_park_factor_adjustment(
                                    conn, home, away_team=away)
                            except Exception:
                                pass

                        # ═══ MLB BULLPEN ERA ADJUSTMENT ═══
                        # Adjust total based on aggregate bullpen ERA vs league avg (3.80).
                        # Dominant bullpens suppress scoring; bad bullpens inflate it.
                        # Stacks with starter ERA + park factor + weather.
                        _bullpen_ctx = ''
                        if sp == 'baseball_mlb':
                            try:
                                _bp_adj, _bullpen_ctx = _mlb_bullpen_adjustment(
                                    conn, home, away)
                                if _bp_adj != 0:
                                    model_total += _bp_adj
                            except Exception:
                                pass

                        # ═══ NHL GOALIE GAA ADJUSTMENT ═══
                        # Adjust total based on confirmed starter GAA vs league avg (2.80).
                        # Elite goalies suppress scoring; bad goalies inflate it.
                        # Uses nhl_goalie_stats season GAA (10+ starts minimum).
                        _goalie_gaa_ctx = ''
                        if sp == 'icehockey_nhl' and _nhl_goalie_info:
                            try:
                                _gaa_adj, _goalie_gaa_ctx = _nhl_goalie_adjustment(
                                    conn, _nhl_goalie_info)
                                if _gaa_adj != 0:
                                    model_total += _gaa_adj
                            except Exception:
                                pass

                        # Weather adjustment for outdoor sports (baseball, soccer)
                        # Weather adjustment is applied in context_engine.py (not here)
                        # to avoid double-counting. Context engine adds it to total_adj
                        # which gets applied to model_total downstream.

                        # Referee adjustment is applied in context_engine.py (not here)
                        # to avoid double-counting. Context engine adds it to total_adj.
                        ref_adj = 0.0
                        ref_info = ''

                        # Reduce Kelly fraction for MEDIUM confidence totals
                        totals_kelly_frac = 0.125 if total_conf == 'HIGH' else 0.0625

                        # Skip MLB totals if model has no total projection (0 or None = no data)
                        # NCAA baseball doesn't generate model_total — it uses market line + adjustments
                        if sp == 'baseball_mlb' and (not model_total or model_total <= 0):
                            continue

                        # Baseball: skip totals with near-zero model conviction
                        # MLB uses model_total vs line; NCAA uses model_spread (no model_total)
                        # v25.4 (4/10/2026): NCAA UNDER conviction floor rolled back from
                        # 1.0 → 0.5. The v25 raise to 1.0 was over-tuned to one bad day —
                        # 14-day backtest of 257 games showed |ms|>=0.5 produces +100.8u
                        # vs |ms|>=1.0 at +45.0u. The 1.0 floor was costing ~55u/14d.
                        if sp == 'baseball_mlb':
                            _mlb_skip_total = abs(model_total - over_total) < 0.5
                            # v25.18: MLB side-conviction gate. When model can't pick a winner
                            # (|model_spread| < 0.5), total projections rely entirely on
                            # pitching/context multipliers with no underlying conviction.
                            # Historical: |MS|<0.5 went 6W-11L -28.4u; |MS|>=0.5 went 12W-4L +32.8u.
                            # MONITOR: Track via MLB_SIDE_CONVICTION_GATE in shadow_blocked_picks.
                            if ms is not None and abs(ms) < 0.5:
                                _mlb_skip_total = True
                                try:
                                    conn.execute("""
                                        INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                                            market_type, line, odds, edge_pct, units, reason)
                                        VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                                    """, (datetime.now().isoformat(), sp, eid,
                                          f"{away}@{home} TOTAL {over_total}",
                                          over_total,
                                          f"MLB_SIDE_CONVICTION_GATE (|model_spread|={abs(ms):.2f} < 0.5, 6W-11L -28.4u historically)"))
                                    conn.commit()
                                except Exception:
                                    pass
                        elif sp == 'baseball_ncaa':
                            _mlb_skip_total = abs(ms) < 0.5  # Overs: 0.5 floor
                            _ncaa_skip_under = abs(ms) < 0.5  # v25.4: was 1.0, rolled back
                        else:
                            _mlb_skip_total = False
                        if sp != 'baseball_ncaa':
                            _ncaa_skip_under = False

                        # OVER
                        k = f"{eid}|T|OVER"
                        # v24: Park gate — pitcher's park vetoes OVERs
                        _park_veto_over = (sp == 'baseball_mlb' and _park_gate_adj < -0.2)
                        if _park_veto_over and _park_factor_ctx:
                            _log_park_veto(conn, sp, eid, f"{away}@{home} OVER {over_total}",
                                           _park_gate_adj, _park_factor_ctx)
                        # v24: Pitching gate — elite pitching matchup vetoes OVERs
                        # When combined pitcher ERA adj is -0.5+ (strong suppression),
                        # the pitching context contradicts the OVER direction
                        # v25.3 (Fix 1): ALSO veto when ANY pitcher in the matchup is elite
                        # (best_era < 3.50). Old gate used the SUM of both pitchers, so an
                        # elite + bad combo (e.g., Patrick 3.27 + Irvin 5.00 → +0.1) silently
                        # passed. New gate: if the best arm in the matchup is sub-3.5 ERA,
                        # the over is at structural risk regardless of the other side.
                        _pitching_veto_over = (sp in ('baseball_mlb', 'baseball_ncaa')
                                               and (_era_adj <= -0.5
                                                    or (_best_era is not None and _best_era < 3.50)))
                        # v25.14: ERA reliability gate — MLB totals require both starters
                        # to have a confirmed ERA. If either shows ?.?? the model is guessing
                        # (e.g. Gallen 5.38 from 2025 box_scores when 2026 is 0.82 ERA).
                        _era_reliability_veto = (sp == 'baseball_mlb' and not _both_era_reliable)
                        if _era_reliability_veto:
                            try:
                                conn.execute("""
                                    INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                                        market_type, line, odds, edge_pct, units, reason)
                                    VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                                """, (datetime.now().isoformat(), sp, eid,
                                      f"{away}@{home} OVER {over_total}", over_total,
                                      f"ERA_RELIABILITY_GATE (missing reliable ERA for 1+ starters: {_pitcher_era_ctx})"))
                                conn.commit()
                            except Exception:
                                pass
                        # (NCAA midweek pitcher-data gate removed — see stub above)
                        # v25.1: Direction gate — raw model must agree with bet direction.
                        # Context factors (pace, pitching, H2H) can inflate model_total past
                        # the line even when the raw model disagrees. 9 of 14 MLB overs and
                        # 2 of 4 soccer overs fired with model_spread <= 0. Those went 3W-5L
                        # (MLB) and 0W-2L (soccer). Context should confirm, not override.
                        _direction_veto_over = (('soccer' in sp or sp == 'baseball_mlb')
                                                and _raw_model_total < over_total)
                        # v25.18: NHL pace gate for OVERs. Fast-paced NHL games go 6W-7L -11.5u
                        # on overs vs 5W-1L +13.8u without pace tag. Fast teams don't produce
                        # high-scoring games when facing good goalies — pace is misleading.
                        # MONITOR: Track via NHL_PACE_OVER_GATE in shadow_blocked_picks.
                        _nhl_pace_veto_over = False
                        if sp == 'icehockey_nhl' and ctx_total and ctx_total.get('summary', ''):
                            if 'fast-paced' in ctx_total['summary'].lower():
                                _nhl_pace_veto_over = True
                                try:
                                    conn.execute("""
                                        INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                                            market_type, line, odds, edge_pct, units, reason)
                                        VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                                    """, (datetime.now().isoformat(), sp, eid,
                                          f"{away}@{home} OVER {over_total}", over_total,
                                          f"NHL_PACE_OVER_GATE (fast-paced context, 6W-7L -11.5u historically)"))
                                    conn.commit()
                                except Exception:
                                    pass
                        if k not in seen and not _mlb_skip_total and not _park_veto_over and not _pitching_veto_over and not _direction_veto_over and not _era_reliability_veto and not _ncaa_pitcher_data_veto and not _ncaa_era_reliability_veto and not _nhl_pace_veto_over:
                            total_diff = model_total - over_total
                            if total_diff > 0:  # Model says higher scoring
                                pv = calculate_point_value_totals(model_total, over_total, sp)
                                # v17 FIX: Divergence penalty applied ONCE to final edge,
                                # not separately to PV and prob_edge (was double-penalizing)
                                _t_div = _divergence_penalty(model_total, over_total, 'TOTAL')
                                stars = get_star_rating(pv)
                                if pv >= min_pv_totals and stars > 0:
                                    prob = _total_prob(total_diff, sp)
                                    imp = american_to_implied_prob(over_odds)
                                    prob_edge = (prob - (imp or 0.524)) * 100.0
                                    # Apply divergence once to the sizing edge
                                    final_edge = max(pv, prob_edge)
                                    if _t_div < 1.0:
                                        final_edge *= _t_div
                                    seen.add(k)
                                    all_picks.append({
                                        'sport': sp, 'event_id': eid, 'commence': commence,
                                        'home': home, 'away': away, 'market_type': 'TOTAL',
                                        'selection': f"{away}@{home} OVER {over_total}",
                                        'book': over_book, 'line': over_total, 'odds': over_odds,
                                        'model_spread': ms, 'model_prob': round(prob, 4),
                                        'implied_prob': round(imp, 4) if imp else None,
                                        'edge_pct': round(pv, 2), 'star_rating': stars,
                                        'units': kelly_units(edge_pct=final_edge, odds=over_odds, fraction=totals_kelly_frac),
                                        'confidence': _conf(stars),
                                        'spread_or_ml': 'TOTAL', 'timing': 'EARLY',
                                        'notes': f"ModelTotal={model_total:.1f} Mkt={over_total} "
                                                 f"Diff={total_diff:+.1f} PV={pv}% {stars}★ data={total_conf}",
                                    })
                                    # Soccer totals Kelly boost (1/3 Kelly, same as spreads)
                                    if 'soccer' in sp:
                                        all_picks[-1]['units'] = kelly_units(
                                            edge_pct=final_edge, odds=over_odds, fraction=0.333)
                                    # Attach context
                                    ctx_parts = []
                                    if ctx_total and ctx_total['summary']:
                                        ctx_parts.append(ctx_total['summary'])
                                    if pitcher_ctx and pitcher_ctx['summary']:
                                        ctx_parts.append(pitcher_ctx['summary'])
                                    if _pitcher_era_ctx:
                                        ctx_parts.append(_pitcher_era_ctx)
                                    if _park_factor_ctx:
                                        ctx_parts.append(_park_factor_ctx)
                                    if _bullpen_ctx:
                                        ctx_parts.append(_bullpen_ctx)
                                    if _goalie_gaa_ctx:
                                        ctx_parts.append(_goalie_gaa_ctx)
                                    # Weather context is already in ctx_total['summary'] from context_engine
                                    if ref_adj != 0 and ref_info:
                                        ctx_parts.append(ref_info)
                                    if ctx_parts:
                                        all_picks[-1]['context'] = ' | '.join(ctx_parts)

                        # UNDER
                        # v22: NCAA baseball UNDER filters — surgical fix for -10.7u bleed
                        # Friday UNDERs: 2W-5L, -17.7u. Lines 12.5+: 8W-10L, -15.4u.
                        # Saturday UNDERs (8W-3L, +17.9u) and lines 10.5-12 (9W-5L, +11.4u) stay.
                        # v25.4 (4/10/2026): BOTH blocks REMOVED. 14-day 257-game backtest:
                        #   - Friday UNDERs: 38-20 (66% win rate), +51.9u  ← was BLOCKED, leaving $$ on table
                        #   - line > 12.0 UNDERs: 79-57 (58% win rate), +54.4u  ← was BLOCKED, ditto
                        # The v22 fixes were over-tuned to a single bad week. The broader
                        # data shows both filters removing winners. NCAA UNDERs are the
                        # model's strongest edge and the filters were eating it.
                        _block_ncaa_under = False  # v25.4: kept variable to avoid renaming downstream
                        # v24: Pace/altitude gate — fast-paced or altitude vetoes NBA UNDERs
                        # Data: NBA unders with pace/altitude 1W-4L -15.7u, without 3W-1L +7.8u
                        _pace_veto_under = False
                        if sp == 'basketball_nba' and ctx_total and ctx_total.get('summary', ''):
                            _ctx_summary = ctx_total['summary'].lower()
                            if 'fast-paced' in _ctx_summary or 'altitude' in _ctx_summary:
                                _pace_veto_under = True
                                try:
                                    conn.execute("""
                                        INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                                            market_type, line, odds, edge_pct, units, reason)
                                        VALUES (?, ?, ?, ?, 'TOTAL', ?, ?, NULL, NULL, ?)
                                    """, (datetime.now().isoformat(), sp, eid,
                                          f"{away}@{home} UNDER {under_total}",
                                          under_total, under_odds,
                                          f"PACE_GATE ({ctx_total['summary'][:80]})"))
                                    conn.commit()
                                except Exception:
                                    pass

                        # v24: Park gate — hitter's park vetoes UNDERs
                        _park_veto_under = (sp == 'baseball_mlb' and _park_gate_adj > 0.2)
                        if _park_veto_under and _park_factor_ctx:
                            _log_park_veto(conn, sp, eid, f"{away}@{home} UNDER {under_total}",
                                           _park_gate_adj, _park_factor_ctx)
                        # v24: Pitching gate — bad pitching matchup vetoes UNDERs
                        # When combined ERA adj is +0.5+ (both starters above avg),
                        # pitching context says "expect more runs" = contradicts UNDER
                        # v25.3 (Fix 1): ALSO veto when ANY pitcher in the matchup is bad
                        # (worst_era > 5.50). Mirror of the OVER gate — a single bad arm
                        # creates structural over risk regardless of the other side.
                        _pitching_veto_under = (sp in ('baseball_mlb', 'baseball_ncaa')
                                                and (_era_adj >= 0.5
                                                     or (_worst_era is not None and _worst_era > 5.50)))
                        # v25.1: Direction gate — raw model must agree with UNDER direction
                        _direction_veto_under = (('soccer' in sp or sp == 'baseball_mlb')
                                                 and _raw_model_total > (under_total or 0))
                        # v25.14: ERA reliability gate for UNDERs too
                        if _era_reliability_veto and under_total is not None:
                            try:
                                conn.execute("""
                                    INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                                        market_type, line, odds, edge_pct, units, reason)
                                    VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                                """, (datetime.now().isoformat(), sp, eid,
                                      f"{away}@{home} UNDER {under_total}", under_total,
                                      f"ERA_RELIABILITY_GATE (missing reliable ERA for 1+ starters: {_pitcher_era_ctx})"))
                                conn.commit()
                            except Exception:
                                pass
                        if under_total is not None and under_odds is not None and not _mlb_skip_total and not _ncaa_skip_under and not _block_ncaa_under and not _park_veto_under and not _pace_veto_under and not _pitching_veto_under and not _direction_veto_under and not _era_reliability_veto and not _ncaa_pitcher_data_veto and not _ncaa_era_reliability_veto:
                            k = f"{eid}|T|UNDER"
                            if k not in seen:
                                total_diff_u = under_total - model_total
                                if total_diff_u > 0:  # Model says lower scoring
                                    pv = calculate_point_value_totals(model_total, under_total, sp)
                                    # v17 FIX: Divergence penalty applied ONCE (same as OVER fix)
                                    _t_div_u = _divergence_penalty(model_total, under_total, 'TOTAL')
                                    stars = get_star_rating(pv)
                                    if pv >= min_pv_totals and stars > 0:
                                        prob = _total_prob(total_diff_u, sp)
                                        imp = american_to_implied_prob(under_odds)
                                        prob_edge = (prob - (imp or 0.524)) * 100.0
                                        final_edge_u = max(pv, prob_edge)
                                        if _t_div_u < 1.0:
                                            final_edge_u *= _t_div_u
                                        seen.add(k)
                                        all_picks.append({
                                            'sport': sp, 'event_id': eid, 'commence': commence,
                                            'home': home, 'away': away, 'market_type': 'TOTAL',
                                            'selection': f"{away}@{home} UNDER {under_total}",
                                            'book': under_book, 'line': under_total, 'odds': under_odds,
                                            'model_spread': ms, 'model_prob': round(prob, 4),
                                            'implied_prob': round(imp, 4) if imp else None,
                                            'edge_pct': round(pv, 2), 'star_rating': stars,
                                            'units': kelly_units(edge_pct=final_edge_u, odds=under_odds, fraction=totals_kelly_frac),
                                            'confidence': _conf(stars),
                                            'spread_or_ml': 'TOTAL', 'timing': 'EARLY',
                                            'notes': f"ModelTotal={model_total:.1f} Mkt={under_total} "
                                                     f"Diff={total_diff_u:+.1f} PV={pv}% {stars}★ data={total_conf}",
                                        })
                                        # Soccer totals Kelly boost (1/3 Kelly, same as spreads)
                                        if 'soccer' in sp:
                                            all_picks[-1]['units'] = kelly_units(
                                                edge_pct=final_edge_u, odds=under_odds, fraction=0.333)
                                        # Attach context
                                        ctx_parts = []
                                        if ctx_total and ctx_total['summary']:
                                            ctx_parts.append(ctx_total['summary'])
                                        if pitcher_ctx and pitcher_ctx['summary']:
                                            ctx_parts.append(pitcher_ctx['summary'])
                                        if _pitcher_era_ctx:
                                            ctx_parts.append(_pitcher_era_ctx)
                                        if _park_factor_ctx:
                                            ctx_parts.append(_park_factor_ctx)
                                        if _bullpen_ctx:
                                            ctx_parts.append(_bullpen_ctx)
                                        if _goalie_gaa_ctx:
                                            ctx_parts.append(_goalie_gaa_ctx)
                                        # Weather context is already in ctx_total['summary'] from context_engine
                                        if ref_adj != 0 and ref_info:
                                            ctx_parts.append(ref_info)
                                        if ctx_parts:
                                            all_picks[-1]['context'] = ' | '.join(ctx_parts)

        if skip_nr or skip_div or skip_w:
            print(f"    Filtered: {skip_nr} no rating, {skip_div} divergence, {skip_w} below threshold")

    # ═══════════════════════════════════════════════════════════════
    # CONTEXT-CONFIRMED CONVICTION
    # ═══════════════════════════════════════════════════════════════
    # Context factors (rest, splits, travel, pace, H2H, refs) provide
    # a REASON for the edge beyond just the numbers. Picks confirmed
    # by context deserve higher conviction than pure model disagreement.
    #
    # Rules:
    # 1. NO context → cap at STRONG (can't be MAX PLAY)
    # 2. Totals with NO context → 15% Kelly haircut (weakest model)
    # 3. Totals WITH context → full Kelly (pace/refs confirm the edge)
    
    for p in all_picks:
        has_context = bool(p.get('context'))

        if not has_context:
            if p['market_type'] == 'TOTAL':
                # Totals without context backing — apply 15% haircut
                # The totals model is the weakest part; without pace/ref/H2H
                # confirmation, we should be less aggressive
                p['units'] = round(p['units'] * 0.85, 1)
                if p['units'] < 2.0:
                    p['units'] = 2.0  # Floor at minimum

            # Cap at STRONG tier regardless of market type
            # MAX PLAY requires the model to have a situational REASON
            max_units_no_context = 4.0  # STRONG ceiling
            if p['units'] > max_units_no_context:
                p['units'] = max_units_no_context

        # v12.3: NCAAB favorite haircut — data shows NCAAB favorites are 3W-6L
        # (-16.1u) while dogs are 13W-5L (+32.4u). The model overvalues NCAAB
        # favorites, especially without context confirmation.
        # Apply 20% haircut to NCAAB favorite spread picks.
        if 'basketball_ncaab' in p.get('sport', ''):
            line = p.get('line')
            if line is not None and line < 0 and p.get('market_type') == 'SPREAD':
                p['units'] = round(p['units'] * 0.80, 1)
                if p['units'] < 2.0:
                    p['units'] = 2.0

    # v17: CLV enforcement gate — block picks where sharp money strongly disagrees
    # v12.3 only added a warning note. Now we actually remove picks with adverse
    # line movement. If the line moved 2.0+ pts AGAINST our side since opener,
    # sharp money is on the other side and we should not bet.
    # Threshold: 2.0 pts for spreads, 1.5 for totals (totals move less).
    try:
        blocked_indices = []
        for i, p in enumerate(all_picks):
            if p.get('market_type') not in ('SPREAD', 'TOTAL') or not p.get('event_id'):
                continue
            mkt = 'spreads' if p['market_type'] == 'SPREAD' else 'totals'
            sel = p.get('selection', '')
            # Determine which outcome to look up
            if p['market_type'] == 'TOTAL':
                outcome = 'Over' if 'OVER' in sel else 'Under'
            else:
                outcome = sel.split()[0]  # Team name
            snaps = conn.execute("""
                SELECT point, snapshot_time FROM line_snapshots
                WHERE event_id = ? AND market = ? AND outcome = ?
                ORDER BY snapshot_time ASC
            """, (p['event_id'], mkt, outcome)).fetchall()
            if len(snaps) >= 2:
                opener_pt = snaps[0][0]
                current_pt = snaps[-1][0]
                if opener_pt is not None and current_pt is not None:
                    shift = current_pt - opener_pt
                    # For spreads: positive shift = line moving against dog (getting fewer pts)
                    # For totals OVER: negative shift = total dropped (harder to go over)
                    # For totals UNDER: positive shift = total rose (harder to stay under)
                    clv_threshold = 1.5 if p['market_type'] == 'TOTAL' else 2.0
                    is_adverse = False
                    if p['market_type'] == 'SPREAD' and shift < -clv_threshold:
                        is_adverse = True  # Line moved against us
                    elif p['market_type'] == 'SPREAD' and shift > clv_threshold:
                        is_adverse = True  # Line moved against us
                    elif 'OVER' in sel and shift < -clv_threshold:
                        is_adverse = True  # Total dropped, harder for over
                    elif 'UNDER' in sel and shift > clv_threshold:
                        is_adverse = True  # Total rose, harder for under

                    if is_adverse:
                        print(f"  CLV BLOCK: {sel} — line moved {shift:+.1f}pts against us "
                              f"(opener={opener_pt}, now={current_pt})")
                        blocked_indices.append(i)
                    elif abs(shift) >= 1.0:
                        p['line_move'] = round(shift, 1)
                        existing = p.get('notes', '')
                        p['notes'] = existing + f" | LINE MOVE: {shift:+.1f}pts since open"
        # Remove blocked picks (reverse order to preserve indices)
        for i in sorted(blocked_indices, reverse=True):
            all_picks.pop(i)
    except Exception:
        pass  # line_snapshots table may not exist

    # Deduplicate: don't bet both sides of same event+market
    # ═══ FINAL FILTER ═══
    # Spread/Total picks: require 2.0★ (13%+ PV) — key number inflation means
    # lower PV picks are noise.
    # ML picks: require 1.0★ (7%+) — these use raw probability edge from Elo,
    # not inflated PV%. A 7%+ Elo probability edge on a favorite is real value.
    # One pick per event per market type (best edge wins).
    final_picks = []
    seen_event_market = {}
    all_picks.sort(key=lambda x: x['star_rating']*100 + x['edge_pct'], reverse=True)
    for p in all_picks:
        # v18: Block heavy favorites — odds worse than -180 are un-bettable
        from config import MIN_ODDS
        p_odds = p.get('odds')
        if p_odds is not None and p_odds <= MIN_ODDS:
            continue
        is_ml = p.get('market_type') == 'MONEYLINE'
        min_stars = 1.0 if is_ml else 2.0
        if p['star_rating'] < min_stars:
            continue
        key = f"{p['event_id']}|{p['market_type']}"
        if key not in seen_event_market:
            seen_event_market[key] = True
            final_picks.append(p)
    return final_picks


def _mk(sp, eid, commence, home, away, mtype, sel, book, line, odds, ms, prob, imp, wa, side):
    # Walters methodology: key number value (PV%) IS the edge.
    # Probability edge is a sanity check — if cover prob strongly disagrees
    # with the market, the PV% is phantom. But small probability edges with
    # real PV% are legitimate (common for favorites where the market is
    # efficient but your number crosses key numbers).
    prob_edge = (prob - imp) * 100 if imp else 0
    pv_edge = wa['point_value_pct'] * 0.40  # Dampened PV% for Kelly sizing

    # Sanity check: if probability STRONGLY disagrees, this is phantom value.
    # PV% can look good when spread crosses key numbers but the probability
    # math says there's no real edge. Allow small negative prob edges (favorites)
    # but reject deeply negative ones.
    if prob_edge < -2.0:
        return None

    # Walters: use the HIGHER of probability edge or dampened PV% for sizing.
    # This lets favorites with real key number value get meaningful units
    # instead of being killed by tiny probability edges.
    actual_edge = max(prob_edge, pv_edge, 0)

    # v23: Cap at 30% — edges above 30% are noise (2W-4L -9.3u).
    # 25-30% is the best bucket: 33W-20L +44u (62%). Let Kelly size naturally.
    actual_edge = min(actual_edge, 30.0)

    # v17: Model-vs-market divergence penalty for spreads
    # When model heavily disagrees with market, reduce edge (market is likely right)
    if line is not None:
        div_mult = _divergence_penalty(ms, line, 'SPREAD')
        if div_mult < 1.0:
            actual_edge *= div_mult

    if actual_edge < 1.0:
        return None

    units = kelly_units(edge_pct=actual_edge, odds=odds)
    if units <= 0:
        return None

    kl = kelly_label(units)
    return {
        'sport': sp, 'event_id': eid, 'commence': commence,
        'home': home, 'away': away, 'market_type': mtype,
        'selection': sel, 'book': book, 'line': line, 'odds': odds,
        'model_spread': ms, 'model_prob': round(prob,4),
        'implied_prob': round(imp,4) if imp else None,
        'edge_pct': round(actual_edge, 2),
        'star_rating': wa['star_rating'], 'units': units,
        'confidence': _conf(wa['star_rating']),
        'spread_or_ml': wa['spread_or_ml'], 'timing': wa['timing'],
        'notes': f"Model={ms:+.1f} PV={wa['point_value_pct']}% {wa['star_rating']}★ "
                 f"VigAdj={wa['vig_adjusted_spread']:+.2f} | "
                 f"Prob={prob:.1%} Imp={imp:.1%} RealEdge={actual_edge:.1f}% "
                 f"Units={units:.1f} ({kl}) | {wa['timing']}",
    }

def _mk_ml(sp, eid, commence, home, away, sel, book, odds, ms, prob, imp, edge, stars, timing, t_r):
    # v23: Cap at 30% — edges above 30% are noise (2W-4L -9.3u).
    edge = min(edge, 30.0)
    units = kelly_units(edge_pct=edge, odds=odds)
    kl = kelly_label(units)
    return {
        'sport': sp, 'event_id': eid, 'commence': commence,
        'home': home, 'away': away, 'market_type': 'MONEYLINE',
        'selection': sel, 'book': book, 'line': None, 'odds': odds,
        'model_spread': ms, 'model_prob': round(prob,4),
        'implied_prob': round(imp,4) if imp else None,
        'edge_pct': round(edge, 2), 'star_rating': stars,
        'units': units, 'confidence': _conf(stars),
        'spread_or_ml': 'MONEYLINE', 'timing': timing,
        'notes': f"Win={prob:.1%} Imp={imp:.1%} Edge={edge:.1f}% {stars}★ Kelly={kl} | {t_r}",
    }

def _conf(s):
    # 3/25: HIGH tier eliminated — 3W-7L -22.5u (-46.9% ROI).
    # Only ELITE (2.5+ stars) passes the card filter. Everything below
    # is sub-ELITE and gets blocked. No reason for a "HIGH" label that
    # could leak through filter edge cases.
    if s >= 2.5: return 'ELITE'
    if s >= 1.5: return 'STRONG'
    if s >= 1.0: return 'MEDIUM'
    return 'LOW' if s >= 0.5 else 'NO_PLAY'

def save_picks_to_db(conn, picks):
    """Save picks to bets table with full analytical metadata.
    
    Captures every dimension needed for professional performance tracking:
    side_type, spread_bucket, timing, context factors, market tier, etc.
    
    Prevents duplicate entries when model is run multiple times per day.
    """
    # Ensure new columns exist (safe migration for existing DBs)
    _ensure_bet_columns(conn)
    
    now = datetime.now().isoformat()
    today = datetime.now().strftime('%Y-%m-%d')
    day_of_week = datetime.now().strftime('%A')  # Monday, Tuesday, etc.
    saved = 0
    dupes = 0
    skipped_no_eid = 0
    saved_picks = []  # Track which picks actually made it to the DB
    for p in picks:
        # Reject picks without event_id — these can't be graded or tracked
        if not p.get('event_id'):
            skipped_no_eid += 1
            print(f"  ⚠ Skipped (no event_id): {p.get('selection', 'unknown')[:50]}")
            continue
        # v12 FIX: Dedup by SIDE, not by full selection string.
        # Old logic: "Nebraska +1.5" != "Nebraska +0.0" → saved twice.
        # New logic: extract the team/side and match on that.
        # Spreads: "Nebraska Cornhuskers +1.5" → "Nebraska Cornhuskers"
        # Totals:  "UNDER 179.5" → "UNDER"
        # ML:      "Iowa State Cyclones ML" → "Iowa State Cyclones"
        import re
        sel = p['selection']
        mtype = p['market_type']
        
        if mtype == 'SPREAD':
            dedup_side = re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
        elif mtype == 'TOTAL':
            dedup_side = 'OVER' if 'OVER' in sel.upper() else 'UNDER'
        elif mtype == 'MONEYLINE':
            dedup_side = sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
        else:
            dedup_side = sel  # Props: use full selection (player-specific)
        
        # Check if we already bet this side of this game today
        existing_bets = conn.execute("""
            SELECT id, selection FROM bets
            WHERE event_id=? AND market_type=?
            AND DATE(created_at)=?
        """, (p['event_id'], mtype, today)).fetchall()
        
        is_dupe = False
        for (existing_id, existing_sel) in existing_bets:
            if mtype == 'SPREAD':
                existing_side = re.sub(r'\s*[+-]?\d+\.?\d*$', '', existing_sel).strip()
            elif mtype == 'TOTAL':
                existing_side = 'OVER' if 'OVER' in existing_sel.upper() else 'UNDER'
            elif mtype == 'MONEYLINE':
                existing_side = existing_sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
            else:
                existing_side = existing_sel
            
            if dedup_side == existing_side:
                is_dupe = True
                break
        
        if is_dupe:
            dupes += 1
            continue
        
        # ── Derive analytical dimensions ──
        # v25.22+: if caller set p['side_type'] explicitly (e.g. 'FADE_FLIP' from
        # Option C NCAA DK gate), preserve it. Otherwise infer from pick data.
        side_type = p.get('side_type') or _classify_side(p)
        spread_bucket = _classify_spread_bucket(p)
        edge_bucket = _classify_edge_bucket(p.get('edge_pct', 0))
        timing = p.get('timing', 'UNKNOWN')
        context_factors = p.get('context', '')
        context_confirmed = 1 if context_factors else 0
        context_adj = p.get('context_adj', 0.0)
        market_tier = _classify_market_tier(p.get('sport', ''))
        model_spread = p.get('model_spread', None)

        # v25.17: Log steam signal as context (no stake/selection change yet).
        # Informational only — review at April 20 checkpoint for NBA signal.
        try:
            from steam_engine import get_steam_signal, format_steam_context
            side_hint = p.get('side_type', '') or side_type
            # Map SIDE types to what steam_engine expects
            if p['market_type'] == 'TOTAL':
                steam_side = 'OVER' if 'OVER' in side_hint.upper() or 'over' in (p.get('selection','').lower()) else 'UNDER'
            elif p['market_type'] == 'SPREAD':
                steam_side = 'FAVORITE' if side_hint == 'FAVORITE' else 'DOG'
            else:
                steam_side = None
            if steam_side and p.get('event_id') and p.get('line') is not None:
                _sig, _info = get_steam_signal(conn, p['sport'], p['event_id'],
                                                p['market_type'], steam_side,
                                                p['line'], p.get('odds'))
                _steam_ctx = format_steam_context(_sig, _info)
                if _steam_ctx:
                    context_factors = (context_factors + ' | ' + _steam_ctx) if context_factors else _steam_ctx
        except Exception:
            pass
        
        conn.execute("""
            INSERT INTO bets (created_at, sport, event_id, market_type, selection,
                book, line, odds, model_prob, implied_prob, edge_pct, confidence, units,
                side_type, spread_bucket, edge_bucket, timing, context_factors,
                context_confirmed, context_adj, market_tier, model_spread, day_of_week)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (now, p['sport'], p['event_id'], p['market_type'], p['selection'],
              p['book'], p['line'], p['odds'], p['model_prob'], p['implied_prob'],
              p['edge_pct'], p['confidence'], p['units'],
              side_type, spread_bucket, edge_bucket, timing, context_factors,
              context_confirmed, context_adj, market_tier, model_spread, day_of_week))
        saved += 1
        saved_picks.append(p)
    conn.commit()
    if dupes:
        print(f"  💾 Saved {saved} picks ({dupes} duplicates skipped)")
    else:
        print(f"  💾 Saved {saved} picks")
    return saved_picks


def _ensure_bet_columns(conn):
    """Add analytical columns to bets table if they don't exist yet."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(bets)").fetchall()}
    new_cols = {
        'side_type': 'TEXT',         # FAVORITE, DOG, OVER, UNDER, PROP_OVER, PROP_UNDER
        'spread_bucket': 'TEXT',     # SMALL_DOG, MED_DOG, BIG_DOG, SMALL_FAV, MED_FAV, BIG_FAV, PK
        'edge_bucket': 'TEXT',       # EDGE_8_12, EDGE_12_16, EDGE_16_20, EDGE_20_PLUS
        'timing': 'TEXT',            # EARLY, LATE
        'context_factors': 'TEXT',   # Pipe-separated factor summary
        'context_confirmed': 'INT',  # 1 = has context, 0 = no context
        'context_adj': 'REAL',       # Total context adjustment in points
        'market_tier': 'TEXT',       # SOFT, SHARP
        'model_spread': 'REAL',      # The model's predicted spread
        'day_of_week': 'TEXT',       # Monday, Tuesday, etc.
    }
    for col, dtype in new_cols.items():
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE bets ADD COLUMN {col} {dtype}")
            except Exception:
                pass
    conn.commit()


def _classify_side(pick):
    """Classify pick as FAVORITE, DOG, OVER, UNDER, or PROP."""
    mtype = pick.get('market_type', '')
    sel = pick.get('selection', '')
    line = pick.get('line', 0)
    
    if mtype == 'TOTAL':
        return 'OVER' if 'OVER' in sel else 'UNDER'
    elif mtype == 'PROP':
        return 'PROP_OVER' if 'OVER' in sel else 'PROP_UNDER'
    elif mtype == 'MONEYLINE':
        odds = pick.get('odds', 0)
        if odds and odds > 0:
            return 'DOG'
        return 'FAVORITE'
    elif mtype == 'SPREAD':
        if line is not None and line > 0:
            return 'DOG'
        elif line is not None and line < 0:
            return 'FAVORITE'
        return 'PK'
    return 'UNKNOWN'


def _classify_spread_bucket(pick):
    """Classify spread magnitude into buckets."""
    mtype = pick.get('market_type', '')
    line = pick.get('line', 0)
    
    if mtype in ('TOTAL', 'PROP'):
        return 'N/A'
    
    if line is None:
        return 'UNKNOWN'
    
    abs_line = abs(line)
    if abs_line <= 0.5:
        side = 'PK'
    elif line > 0:  # Dog
        if abs_line <= 3.5:
            side = 'SMALL_DOG'
        elif abs_line <= 7.5:
            side = 'MED_DOG'
        else:
            side = 'BIG_DOG'
    else:  # Favorite
        if abs_line <= 3.5:
            side = 'SMALL_FAV'
        elif abs_line <= 7.5:
            side = 'MED_FAV'
        else:
            side = 'BIG_FAV'
    return side


def _classify_edge_bucket(edge_pct):
    """Classify projected edge into buckets."""
    if edge_pct >= 20:
        return 'EDGE_20_PLUS'
    elif edge_pct >= 16:
        return 'EDGE_16_20'
    elif edge_pct >= 12:
        return 'EDGE_12_16'
    return 'EDGE_8_12'


def _classify_market_tier(sport):
    """Classify sport into SOFT or SHARP market tier."""
    soft = {'basketball_ncaab', 'soccer_usa_mls', 'soccer_germany_bundesliga',
            'soccer_france_ligue_one', 'soccer_italy_serie_a', 'soccer_uefa_champs_league',
            'soccer_mexico_ligamx', 'baseball_ncaa'}
    return 'SOFT' if sport in soft else 'SHARP'

def print_picks(picks, title="TODAY'S PICKS"):
    if not picks:
        print(f"\n{'='*70}\n  {title}: No qualifying plays\n  Target: 5-10/week — patience IS the edge.\n{'='*70}")
        return picks
    print(f"\n{'='*70}")
    print(f"  {title} — {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    print(f"  {len(picks)} plays | Scotty's Edge v11")
    print(f"{'='*70}")
    by_sport = {}
    for p in picks: by_sport.setdefault(p['sport'], []).append(p)
    for sport, spicks in by_sport.items():
        SPORT_LABELS = {
            'basketball_ncaab': 'NCAAB', 'basketball_nba': 'NBA',
            'icehockey_nhl': 'NHL', 'baseball_ncaa': 'NCAA_BASEBALL',
            'soccer_epl': 'EPL', 'soccer_italy_serie_a': 'ITALY_SERIE_A',
            'soccer_spain_la_liga': 'SPAIN_LA_LIGA',
            'soccer_germany_bundesliga': 'GERMANY_BUNDESLIGA',
            'soccer_france_ligue_one': 'FRANCE_LIGUE_ONE',
            'soccer_uefa_champs_league': 'UEFA_CHAMPIONS_LEAGUE',
            'soccer_usa_mls': 'MLS',
            'soccer_mexico_ligamx': 'LIGA_MX',
        }
        label = SPORT_LABELS.get(sport, sport.upper())
        print(f"\n  ── {label} {'─'*(50-len(label))}")
        for p in spicks:
            units = p['units']
            kl = kelly_label(units)
            # Size indicator: visual bar (scale 0-5u → 0-10 blocks)
            filled = min(10, int(units * 2))
            bar = '█' * filled + '░' * (10 - filled)
            # Icon by conviction tier
            tier_icon = {
                'MAX PLAY': '🔥', 'STRONG': '⭐', 'SOLID': '✅',
                'LEAN': '📊', 'SPRINKLE': '📋'
            }.get(kl, '📋')
            # Convert UTC to Eastern (DST-aware)
            day_label, est_time = '', ''
            tz_label = _eastern_tz_label()
            if p['commence']:
                try:
                    gt = datetime.fromisoformat(p['commence'].replace('Z','+00:00'))
                    est = _to_eastern(gt)
                    est_time = est.strftime('%I:%M %p')
                    today = datetime.now()
                    if est.date() == today.date():
                        day_label = 'TODAY'
                    elif est.date() == (today + timedelta(days=1)).date():
                        day_label = 'TOMORROW'
                    else:
                        day_label = est.strftime('%a %m/%d')
                except Exception:
                    est_time = p['commence'][:16].replace('T',' ')
            print(f"\n  {tier_icon} {p['selection']}")
            print(f"     {p['home']} vs {p['away']} | {day_label} {est_time} {tz_label}")
            print(f"     {p['book']} | {p['odds']:+.0f} | {p['market_type']}")
            print(f"     Edge: {p['edge_pct']:.1f}%  |  {bar} {units:.1f}u {kl}")
            timing = p.get('timing', '')
            if timing:
                timing_icon = {'EARLY': '⏰ BET EARLY', 'LATE': '⏳ BET LATE', 'HOLD': '🕐 HOLD FOR BEST LINE'}.get(timing, timing)
                print(f"     {timing_icon}")
            # Context factors (if any active)
            ctx_summary = p.get('context')
            if ctx_summary:
                print(f"     📍 {ctx_summary}")
    print(f"\n{'='*70}")
    tu = sum(p['units'] for p in picks)
    sizes = {}
    for p in picks:
        kl = kelly_label(p['units'])
        sizes[kl] = sizes.get(kl, 0) + 1
    size_str = ' | '.join(f"{v} {k}" for k, v in sizes.items() if v > 0)
    print(f"  {len(picks)} plays | {tu:.1f} total units | {size_str}")
    print(f"{'='*70}")
    return picks

def picks_to_text(picks, title="TODAY'S PICKS"):
    """Clean subscriber-ready text format for email/Telegram, grouped by sport."""
    lines = []
    if not picks:
        return f"{title}: No qualifying plays today. Patience is the edge."

    sport_labels = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL', 'baseball_ncaa': 'NCAA BASEBALL',
        'soccer_epl': 'EPL', 'soccer_germany_bundesliga': 'BUNDESLIGA',
        'soccer_france_ligue_one': 'LIGUE 1', 'soccer_italy_serie_a': 'SERIE A',
        'soccer_spain_la_liga': 'LA LIGA', 'soccer_usa_mls': 'MLS',
        'soccer_uefa_champs_league': 'UCL', 'soccer_mexico_ligamx': 'LIGA MX',
    }
    sport_icons = {
        'NBA': '🏀', 'NCAAB': '🏀', 'NHL': '🏒', 'NCAA BASEBALL': '⚾', 'LIGA MX': '⚽',
        'EPL': '⚽', 'BUNDESLIGA': '⚽', 'LIGUE 1': '⚽', 'SERIE A': '⚽',
        'LA LIGA': '⚽', 'MLS': '⚽', 'UCL': '⚽',
    }
    sport_order = ['NBA', 'NHL', 'NCAAB', 'NCAA BASEBALL',
                   'EPL', 'LA LIGA', 'SERIE A', 'BUNDESLIGA', 'LIGUE 1', 'MLS', 'UCL']

    # Group picks by sport
    groups = {}
    for p in picks:
        sp = p.get('sport', 'other')
        label = sport_labels.get(sp, sp.upper())
        if label not in groups:
            groups[label] = []
        groups[label].append(p)

    # Sort within groups by units descending
    for label in groups:
        groups[label].sort(key=lambda p: p['units'], reverse=True)

    lines.append(f"{'━'*50}")
    lines.append(f"  {title}")
    lines.append(f"  {datetime.now().strftime('%A, %B %d %Y • %I:%M %p')} {_eastern_tz_label()}")
    lines.append(f"  {len(picks)} plays")
    lines.append(f"{'━'*50}")

    # Render in sport order
    rendered = set()
    for sl in sport_order:
        if sl in groups:
            rendered.add(sl)
            _render_sport_group(lines, sl, sport_icons.get(sl, '🏟️'), groups[sl])
    # Any remaining sports
    for sl, gp in groups.items():
        if sl not in rendered:
            _render_sport_group(lines, sl, sport_icons.get(sl, '🏟️'), gp)

    lines.append(f"{'━'*50}")
    tu = sum(p['units'] for p in picks)
    sizes = {}
    for p in picks:
        kl = kelly_label(p['units'])
        sizes[kl] = sizes.get(kl, 0) + 1
    size_str = ' | '.join(f"{v} {k}" for k, v in sizes.items() if v > 0)
    lines.append(f"  {len(picks)} plays • {tu:.1f} total units")
    lines.append(f"  {size_str}")
    lines.append(f"{'━'*50}")
    return '\n'.join(lines)


def _render_sport_group(lines, sport_label, icon, sport_picks):
    """Render a sport group section for picks_to_text."""
    lines.append(f"")
    lines.append(f"  {icon} {sport_label}")
    lines.append(f"  {'─'*40}")
    for p in sport_picks:
        units = p['units']
        kl = kelly_label(units)
        tier_icon = {
            'MAX PLAY': '🔥', 'STRONG': '⭐', 'SOLID': '✅',
            'LEAN': '📊', 'SPRINKLE': '📋'
        }.get(kl, '📋')
        game_time = ''
        tz_label = _eastern_tz_label()
        if p['commence']:
            try:
                gt = datetime.fromisoformat(p['commence'].replace('Z','+00:00'))
                est = _to_eastern(gt)
                game_time = est.strftime('%I:%M %p') + f' {tz_label}'
            except Exception:
                game_time = ''
        lines.append(f"")
        lines.append(f"  {tier_icon} {p['selection']}")
        lines.append(f"    {p['home']} vs {p['away']} • {game_time}")
        lines.append(f"    {p['book']}  {p['odds']:+.0f}  {p['market_type']}")
        lines.append(f"    {units:.1f}u {kl}  •  Edge: {p['edge_pct']:.1f}%")
        timing = p.get('timing', '')
        if timing and timing != 'STANDARD':
            timing_label = {'EARLY': '⏰ BET EARLY', 'LATE': '⏳ BET LATE', 'HOLD': '🕐 HOLD FOR BEST LINE'}.get(timing, '')
            if timing_label:
                lines.append(f"    {timing_label}")
        ctx_summary = p.get('context')
        if ctx_summary:
            lines.append(f"    📍 {ctx_summary}")
    lines.append(f"")

def update_ratings_post_game(conn, sport, home, away, home_score, away_score,
                             home_inj=0, away_inj=0, hfa=None):
    """90/10 update: New = 90% old + 10% TGPL. Auto-seeds new teams."""
    ratings = get_latest_ratings(conn, sport)
    h, a = ratings.get(home), ratings.get(away)
    if hfa is None: hfa = SPORT_CONFIG.get(sport, {}).get('home_court', 2.5)
    
    # Auto-seed unrated teams at 0.0 (neutral) so they enter the update cycle
    if not h:
        h = {'base': 0.0, 'home_court': hfa, 'final': 0.0}
    if not a:
        a = {'base': 0.0, 'home_court': hfa, 'final': 0.0}
    
    inj_d = home_inj - away_inj
    tgpl_h = (home_score - away_score) + a['final'] + inj_d - hfa
    tgpl_a = (away_score - home_score) + h['final'] - inj_d + hfa
    new_h = round(0.9 * h['final'] + 0.1 * tgpl_h, 3)
    new_a = round(0.9 * a['final'] + 0.1 * tgpl_a, 3)
    now = datetime.now().isoformat()
    for team, nr in [(home, new_h), (away, new_a)]:
        conn.execute("""INSERT INTO power_ratings (run_timestamp, sport, team,
            base_rating, home_court, final_rating, games_used, iterations,
            learning_rate, regularization)
            VALUES (?,?,?,?,?,?,1,1,0,0)""", (now, sport, team, nr, hfa, nr))
    conn.commit()
    return new_h, new_a

if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    picks = generate_predictions(conn)
    print_picks(picks)
    conn.close()
