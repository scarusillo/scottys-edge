"""
elo_engine.py — Proper Elo ratings from game results

This replaces market-bootstrapped ratings with ratings derived from
ACTUAL GAME OUTCOMES. The key insight: ratings bootstrapped from market
spreads will always agree with the market (MAE ~0.6 pts). Elo ratings
from results create INDEPENDENT predictions that can genuinely disagree
with the market — which is where edges come from.

Elo system features:
  - Margin of Victory adjustment (bigger wins = more info)
  - Home court/ice advantage modeling
  - Recency weighting (recent games matter more)
  - Sport-specific K-factors and parameters
  - Auto-calibrated to minimize prediction error
  - Blended with market ratings (not pure replacement)

Usage:
    python elo_engine.py                # Build Elo for all sports
    python elo_engine.py --sport nba    # Just NBA
    python elo_engine.py --analyze      # Show model accuracy analysis
"""
import sqlite3, math, os, sys
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ── Sport-specific Elo parameters ──

ELO_CONFIG = {
    'basketball_nba': {
        'k_factor': 12,           # Reduced from 20 — less volatile ratings
        'home_advantage': 80,     # ~2.8 pts spread (was 100 → 3.5, too high)
        'mov_multiplier': True,   # Use margin of victory
        'mov_cap': 20,            # Cap MOV at 20 (was 30 — blowouts are noisy)
        'spread_per_elo': 28.5,   # Elo points per 1 point of spread
        'initial_elo': 1500,
        'season_revert': 0.25,    # Revert 25% to mean between seasons
        'min_games': 20,          # Min games before trusting Elo over market
    },
    'basketball_ncaab': {
        'k_factor': 20,           # Reduced from 32 — college still needs higher K
        'home_advantage': 100,    # ~3.5 pts (was 120 → 4.2, too high)
        'mov_multiplier': True,
        'mov_cap': 20,            # v12 FIX: Was 15 (session 10 overcorrection from 25). 15 capped model spreads so it could NEVER pick favorites. 20 = same as NBA, allows -15 to -20 spreads for elite teams.
        'spread_per_elo': 28.5,
        'initial_elo': 1500,
        'season_revert': 0.40,    # More revert (roster turnover)
        'min_games': 6,           # v12.2: Lowered from 8. ESPN coverage is thin for small schools
                                  # (65 teams stuck at 3 games). 6 → MEDIUM at 3 games (20% Elo blend)
                                  # instead of 4. Some Elo signal is better than pure bootstrap fallback.
        'autocorrelation': 0.006, # Stronger than default 0.004 — cupcake schedules are NCAAB's
                                  # biggest distortion. At Elo diff 200: 40% reduction (was 29%).
                                  # At Elo diff 400: 56% reduction (was 47%).
    },
    'icehockey_nhl': {
        'k_factor': 6,            # Reduced from 8 — hockey is very random
        'home_advantage': 25,     # ~0.12 goals home ice (was 30)
        'mov_multiplier': True,
        'mov_cap': 5,             # Cap at 5 goals (was 6)
        'spread_per_elo': 200,    # Elo points per 1 goal of spread
        'initial_elo': 1500,
        'season_revert': 0.25,
        'min_games': 20,
    },
    'soccer_epl': {
        'k_factor': 16,           # Reduced from 20
        'home_advantage': 55,     # ~0.35 goals (was 65)
        'mov_multiplier': True,
        'mov_cap': 4,             # Was 5
        'spread_per_elo': 160,    # Was 260 — compressed diffs to <0.1 goal, killing all picks
        'initial_elo': 1500,
        'season_revert': 0.20,
        'min_games': 8,
    },
    'soccer_italy_serie_a': {
        'k_factor': 16,
        'home_advantage': 65,     # Serie A has stronger home advantage
        'mov_multiplier': True,
        'mov_cap': 4,
        'spread_per_elo': 160,
        'initial_elo': 1500,
        'season_revert': 0.20,
        'min_games': 8,
    },
    'soccer_spain_la_liga': {
        'k_factor': 16,
        'home_advantage': 55,
        'mov_multiplier': True,
        'mov_cap': 4,
        'spread_per_elo': 160,
        'initial_elo': 1500,
        'season_revert': 0.20,
        'min_games': 8,
    },
    'soccer_germany_bundesliga': {
        'k_factor': 20,
        'home_advantage': 65,     # Bundesliga has strong home advantage
        'mov_multiplier': True,
        'mov_cap': 5,
        'spread_per_elo': 160,
        'initial_elo': 1500,
        'season_revert': 0.20,
        'min_games': 8,
    },
    'soccer_france_ligue_one': {
        'k_factor': 20,
        'home_advantage': 60,
        'mov_multiplier': True,
        'mov_cap': 5,
        'spread_per_elo': 160,
        'initial_elo': 1500,
        'season_revert': 0.20,
        'min_games': 8,
    },
    'soccer_uefa_champs_league': {
        'k_factor': 24,           # Higher K — fewer games per team, each matters more
        'home_advantage': 50,     # Smaller home edge in UCL
        'mov_multiplier': True,
        'mov_cap': 5,
        'spread_per_elo': 160,
        'initial_elo': 1500,
        'season_revert': 0.15,    # Less revert — UCL teams are well-established
        'min_games': 4,           # Only 6-8 group stage games
    },
    'soccer_usa_mls': {
        'k_factor': 20,
        'home_advantage': 75,     # MLS has massive home advantage
        'mov_multiplier': True,
        'mov_cap': 5,
        'spread_per_elo': 160,
        'initial_elo': 1500,
        'season_revert': 0.30,    # Higher roster turnover
        'min_games': 8,
    },
    'soccer_mexico_ligamx': {
        'k_factor': 20,
        'home_advantage': 80,     # Liga MX: altitude + travel = huge home edge
        'mov_multiplier': True,
        'mov_cap': 5,
        'spread_per_elo': 160,
        'initial_elo': 1500,
        'season_revert': 0.25,
        'min_games': 8,
    },
    'baseball_ncaa': {
        'k_factor': 24,           # Higher K — short season, each game matters more
        'home_advantage': 65,     # v14: Was 40. Actual home win rate 65.6% — needs stronger HA.
        'mov_multiplier': True,
        'mov_cap': 10,            # College baseball: metal bats, big blowouts happen
        'spread_per_elo': 120,    # Elo points per 1 run of spread
        'initial_elo': 1500,
        'season_revert': 0.50,    # Heavy revert — massive roster turnover in college
        'min_games': 8,           # Need at least 8 games before trusting Elo
    },
    'baseball_mlb': {
        'k_factor': 6,            # Low K — 162 game season, each game is small
        'home_advantage': 35,     # MLB home field ~53-54% win rate
        'mov_multiplier': True,
        'mov_cap': 8,             # Cap at 8 runs — blowouts are noise
        'spread_per_elo': 150,    # Elo points per 1 run of spread
        'initial_elo': 1500,
        'season_revert': 0.33,    # Moderate revert — some roster continuity
        'min_games': 15,          # ~2 weeks of games before trusting Elo
    },
    # ── TENNIS ──
    # Surface-split Elo: stored as tennis_atp_hard, tennis_atp_clay, tennis_atp_grass
    # Individual players, not teams. Each match has clear W/L (no draws).
    # Higher K-factor than team sports — individual performance is more predictable
    # and each match provides strong signal about relative skill.
    'tennis_atp_hard': {
        'k_factor': 24,           # Individual sport — each match is more informative
        'home_advantage': 0,      # Neutral tournament venues
        'mov_multiplier': True,   # Set margin: 2-0 vs 2-1 (bo3) or 3-0 vs 3-2 (bo5)
        'mov_cap': 3,             # Max 3-set margin in a Grand Slam
        'spread_per_elo': 120,    # Elo points per 1 set of spread
        'initial_elo': 1500,
        'season_revert': 0.10,    # Minimal — tennis is year-round, skills carry over
        'min_games': 8,           # Need 8+ matches on this surface
        'elo_scale': 150,         # Tennis: less random than team sports → sharper probs
    },
    'tennis_atp_clay': {
        'k_factor': 28,           # Higher K on clay — more upsets, faster signal needed
        'home_advantage': 0,
        'mov_multiplier': True,
        'mov_cap': 3,
        'spread_per_elo': 120,
        'initial_elo': 1500,
        'season_revert': 0.10,
        'min_games': 6,           # Fewer clay events per year
        'elo_scale': 150,
    },
    'tennis_atp_grass': {
        'k_factor': 32,           # Highest K — very few grass events, each one matters a lot
        'home_advantage': 0,
        'mov_multiplier': True,
        'mov_cap': 3,
        'spread_per_elo': 120,
        'initial_elo': 1500,
        'season_revert': 0.10,
        'min_games': 4,           # Only ~3 grass events per year
        'elo_scale': 150,
    },
    'tennis_wta_hard': {
        'k_factor': 28,           # WTA slightly more volatile than ATP
        'home_advantage': 0,
        'mov_multiplier': True,
        'mov_cap': 2,             # WTA is always best-of-3 (max 2-set margin)
        'spread_per_elo': 120,
        'initial_elo': 1500,
        'season_revert': 0.10,
        'min_games': 8,
        'elo_scale': 150,
    },
    'tennis_wta_clay': {
        'k_factor': 32,
        'home_advantage': 0,
        'mov_multiplier': True,
        'mov_cap': 2,
        'spread_per_elo': 120,
        'initial_elo': 1500,
        'season_revert': 0.10,
        'min_games': 6,
        'elo_scale': 150,
    },
    'tennis_wta_grass': {
        'k_factor': 36,
        'home_advantage': 0,
        'mov_multiplier': True,
        'mov_cap': 2,
        'spread_per_elo': 120,
        'initial_elo': 1500,
        'season_revert': 0.10,
        'min_games': 4,
        'elo_scale': 150,
    },
}


def _expected_score(rating_a, rating_b, scale=400):
    """Standard Elo expected score: probability A beats B.

    scale: divisor for Elo diff. Standard=400 for team sports.
    Tennis uses 150 (less randomness → sharper predictions).
    """
    return 1.0 / (1.0 + 10 ** ((rating_b - rating_a) / scale))


def _mov_multiplier(margin, elo_diff, sport_cfg):
    """
    Margin of Victory multiplier.
    
    Bigger wins provide more information, but with strong diminishing returns.
    Uses square root scaling (gentler than log) with autocorrelation correction.
    
    A 20-pt NBA win should NOT move ratings 4x more than a 5-pt win.
    sqrt(20)/sqrt(5) = 2.0x — much more reasonable than log's 3.4x.
    """
    if not sport_cfg.get('mov_multiplier', False):
        return 1.0
    
    cap = sport_cfg.get('mov_cap', 20)
    margin = min(abs(margin), cap)
    
    # Square root diminishing returns (gentler than log)
    # Normalize so a 1-point margin = multiplier of 1.0
    mov = math.sqrt(margin)
    
    # Autocorrelation correction: reduce multiplier when favorite wins big.
    # This prevents rating inflation for dominant teams.
    # NCAAB uses stronger correction (0.006) because cupcake schedules are
    # the #1 source of Elo distortion — beating weak MAC/SWAC teams by 15+
    # should barely move a power conference team's rating.
    # Other sports: 0.004 (original v12 calibration).
    ac_coeff = sport_cfg.get('autocorrelation', 0.004)
    correction = 2.2 / (2.2 + ac_coeff * abs(elo_diff))
    
    return mov * correction


def build_elo_ratings(sport, verbose=True):
    """
    Build Elo ratings for a sport from historical game results.
    
    Returns dict of {team_name: elo_rating} and metadata.
    """
    cfg = ELO_CONFIG.get(sport)
    if not cfg:
        if verbose:
            print(f"  ⚠ No Elo config for {sport}")
        return {}, {}
    
    conn = sqlite3.connect(DB_PATH)
    
    # Get all completed games, ordered by date.
    # Deduplicate: same matchup + same score within 24 hours = duplicate API entry.
    # Legit rematches (e.g., conference tournament after regular season) will have
    # different dates (weeks apart) and are kept.
    all_games = conn.execute("""
        SELECT home, away, home_score, away_score, actual_margin, commence_time
        FROM results
        WHERE sport=? AND completed=1
        AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY commence_time ASC
    """, (sport,)).fetchall()
    games = []
    seen_games = set()
    for g in all_games:
        # Key: teams + score + date (truncated to day)
        date_key = g[5][:10] if g[5] else ''
        dedup_key = (g[0], g[1], g[2], g[3], date_key)
        if dedup_key not in seen_games:
            seen_games.add(dedup_key)
            games.append(g)
    
    if not games:
        if verbose:
            print(f"  ⚠ No results for {sport} — run historical_scores.py first")
        conn.close()
        return {}, {}
    
    if verbose:
        print(f"  📊 {sport}: {len(games)} games to process")
    
    # Initialize ratings
    elos = defaultdict(lambda: cfg['initial_elo'])
    game_counts = defaultdict(int)
    recent_results = defaultdict(list)  # Track last N results per team
    opponents = defaultdict(list)  # Track opponent Elo at time of game for SOS

    # Season boundary detection — revert ratings toward 1500 between seasons.
    # Each sport has a month where the new season starts. When we cross that
    # boundary, apply season_revert to all existing ratings.
    season_start_month = {
        'basketball_nba': 10, 'basketball_ncaab': 11,
        'icehockey_nhl': 10, 'baseball_ncaa': 2, 'baseball_mlb': 3,
        'soccer_epl': 8, 'soccer_italy_serie_a': 8, 'soccer_spain_la_liga': 8,
        'soccer_germany_bundesliga': 8, 'soccer_france_ligue_one': 8,
        'soccer_uefa_champs_league': 9, 'soccer_usa_mls': 2, 'soccer_mexico_ligamx': 1,
        # Tennis: year-round, Australian Open starts in January
        'tennis_atp_hard': 1, 'tennis_atp_clay': 1, 'tennis_atp_grass': 1,
        'tennis_wta_hard': 1, 'tennis_wta_clay': 1, 'tennis_wta_grass': 1,
    }
    revert_pct = cfg.get('season_revert', 0.25)
    start_month = season_start_month.get(sport, 0)
    last_season_year = None  # Track which "season year" we're in

    # Process each game chronologically
    predictions = []  # For accuracy tracking

    for home, away, h_score, a_score, margin, commence in games:
        if margin is None:
            margin = h_score - a_score

        # Season boundary reversion: when we cross into a new season,
        # revert all ratings toward 1500 to account for roster turnover.
        if start_month > 0 and commence:
            try:
                game_month = int(commence[5:7])
                game_year = int(commence[:4])
                # Determine "season year" (e.g., Oct 2025 NBA = 2025-26 season → season_year=2025)
                season_year = game_year if game_month >= start_month else game_year - 1
                if last_season_year is not None and season_year > last_season_year:
                    # New season detected — revert all ratings toward 1500
                    for team in list(elos.keys()):
                        elos[team] = 1500 + (elos[team] - 1500) * (1 - revert_pct)
                    if verbose:
                        print(f"  Season boundary: {last_season_year}->{season_year}, reverted {revert_pct:.0%} toward 1500")
                last_season_year = season_year
            except (ValueError, IndexError):
                pass

        home_elo = elos[home] + cfg['home_advantage']
        away_elo = elos[away]
        
        # Prediction (before updating)
        expected_home = _expected_score(home_elo, away_elo)
        predicted_spread = (away_elo - home_elo) / cfg['spread_per_elo']
        
        predictions.append({
            'home': home, 'away': away,
            'predicted_spread': predicted_spread,
            'actual_margin': margin,
            'predicted_home_win': expected_home,
            'home_won': margin > 0,
            'date': commence,
        })
        
        # Actual result
        if margin > 0:
            actual_home = 1.0
        elif margin < 0:
            actual_home = 0.0
        else:
            actual_home = 0.5  # Draw
        
        # MOV multiplier
        elo_diff = home_elo - away_elo
        mov_mult = _mov_multiplier(margin, elo_diff, cfg)
        
        # K-factor: use base K. The old 1.5x boost for early games amplified
        # noise for thin-data teams (exactly where we trust ratings least).
        # Better to be conservative early and let confidence weighting handle
        # the uncertainty downstream.
        k_adjust = cfg['k_factor']

        # SOS-weighted update: scale delta by opponent quality.
        # Beating a 1300 cupcake should move your rating less than beating
        # a 1600 powerhouse. Without this, mid-majors accumulate inflated
        # ratings from weak schedules that only get corrected post-hoc.
        #
        # Scale factor: opponent Elo deviation from 1500, normalized gently.
        # Range: ~0.7x (very weak opp) to ~1.3x (very strong opp).
        # Home team's opponent is away, and vice versa.
        sos_scale = 0.15 if 'ncaab' in sport else 0.10
        home_opp_factor = 1.0 + (elos[away] - 1500) / 1500 * sos_scale / 0.10
        away_opp_factor = 1.0 + (elos[home] - 1500) / 1500 * sos_scale / 0.10

        # Update ratings with opponent-quality weighting
        delta = k_adjust * mov_mult * (actual_home - expected_home)
        elos[home] += delta * home_opp_factor
        elos[away] -= delta * away_opp_factor
        
        game_counts[home] += 1
        game_counts[away] += 1
        
        # Track recent performance
        recent_results[home].append(('home', margin, commence))
        recent_results[away].append(('away', -margin, commence))

        # Track opponents for SOS (use opponent Elo at time of game, not final)
        opponents[home].append(elos[away])
        opponents[away].append(elos[home])
    
    # Normalize: center ratings around 1500
    avg_elo = sum(elos.values()) / len(elos) if elos else 1500
    for team in elos:
        elos[team] = round(elos[team] - avg_elo + 1500, 1)

    # v12.3: Compute Strength of Schedule — average opponent Elo at time of game
    sos = {}
    for team in elos:
        opp_elos = opponents.get(team, [])
        sos[team] = round(sum(opp_elos) / len(opp_elos), 1) if opp_elos else 1500.0

    # v14: Lighter post-hoc SOS regression — most correction now happens in-loop
    # via opponent-quality weighted updates. This is a cleanup pass for residual
    # schedule bias that the in-loop weighting doesn't fully eliminate.
    sos_regression = 0.08 if 'ncaab' in sport else 0.05
    for team in elos:
        team_sos = sos.get(team, 1500)
        sos_dev = (team_sos - 1500) / 500  # Normalized: -1 to +1 range for ±500 Elo
        # Easy schedule (sos_dev < 0): pull inflated ratings down
        # Hard schedule (sos_dev > 0): boost compressed ratings up
        adjustment = (elos[team] - 1500) * sos_regression * (-sos_dev)
        elos[team] = round(elos[team] - adjustment, 1)

    if verbose:
        # Show SOS extremes
        sorted_sos = sorted(sos.items(), key=lambda x: x[1])
        if sorted_sos:
            easiest = sorted_sos[:3]
            hardest = sorted_sos[-3:]
            print(f"  SOS range: {sorted_sos[0][1]:.0f} (easiest) to {sorted_sos[-1][1]:.0f} (hardest)")

    # Calculate accuracy metrics
    meta = _calculate_accuracy(predictions, cfg, verbose)
    meta['total_games'] = len(games)
    meta['total_teams'] = len(elos)
    meta['game_counts'] = dict(game_counts)
    
    # Save to database
    _save_elo_ratings(conn, sport, dict(elos), game_counts, cfg, meta, sos)
    
    conn.close()
    return dict(elos), meta


def _calculate_accuracy(predictions, cfg, verbose=True):
    """Calculate how well Elo predicts outcomes."""
    if not predictions:
        return {}
    
    # Only evaluate predictions where both teams have 5+ games
    # (early predictions are expected to be bad)
    n = len(predictions)
    mid = max(n // 3, 10)  # Skip first third (cold start)
    eval_preds = predictions[mid:]
    
    if not eval_preds:
        return {}
    
    # Win prediction accuracy
    correct = sum(1 for p in eval_preds
                  if (p['predicted_home_win'] > 0.5) == p['home_won']
                  and p['actual_margin'] != 0)
    total_decided = sum(1 for p in eval_preds if p['actual_margin'] != 0)
    accuracy = correct / total_decided if total_decided else 0
    
    # Spread prediction MAE
    # predicted_spread: negative = home favored. actual_margin: positive = home won.
    # Negate predicted_spread to match actual_margin convention.
    spread_errors = [abs(-p['predicted_spread'] - p['actual_margin']) for p in eval_preds]
    mae = sum(spread_errors) / len(spread_errors)
    
    # Calibration: when we predict 60% home win, does home win ~60%?
    # Bucket predictions
    buckets = defaultdict(list)
    for p in eval_preds:
        bucket = round(p['predicted_home_win'] * 10) / 10  # Round to nearest 0.1
        buckets[bucket].append(1 if p['home_won'] else 0)
    
    if verbose:
        print(f"  📈 Elo accuracy (after warmup):")
        print(f"     Win prediction: {accuracy:.1%} ({correct}/{total_decided})")
        print(f"     Spread MAE: {mae:.1f} pts")
        print(f"     Predictions evaluated: {len(eval_preds)} (skipped first {mid})")
    
    return {
        'win_accuracy': accuracy,
        'spread_mae': mae,
        'predictions_evaluated': len(eval_preds),
    }


def _save_elo_ratings(conn, sport, elos, game_counts, cfg, meta, sos=None):
    """Save Elo ratings to the elo_ratings table."""

    # Create elo_ratings table if needed
    conn.execute("""
        CREATE TABLE IF NOT EXISTS elo_ratings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sport           TEXT NOT NULL,
            team            TEXT NOT NULL,
            elo             REAL NOT NULL,
            games_played    INTEGER DEFAULT 0,
            confidence      TEXT DEFAULT 'LOW',
            sos             REAL DEFAULT 1500,
            last_updated    TEXT,
            UNIQUE(sport, team)
        )
    """)
    # Migration: add SOS column if table already exists without it
    try:
        conn.execute("ALTER TABLE elo_ratings ADD COLUMN sos REAL DEFAULT 1500")
    except Exception:
        pass  # Column already exists

    now = datetime.now().isoformat()
    min_games = cfg.get('min_games', 10)
    if sos is None:
        sos = {}

    for team, elo in elos.items():
        gp = game_counts.get(team, 0)
        conf = 'HIGH' if gp >= min_games else ('MEDIUM' if gp >= min_games // 2 else 'LOW')
        team_sos = sos.get(team, 1500.0)

        conn.execute("""
            INSERT OR REPLACE INTO elo_ratings (sport, team, elo, games_played, confidence, sos, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (sport, team, elo, gp, conf, team_sos, now))

    conn.commit()


def get_elo_ratings(conn, sport):
    """Retrieve Elo ratings for a sport."""
    try:
        rows = conn.execute("""
            SELECT team, elo, games_played, confidence, COALESCE(sos, 1500)
            FROM elo_ratings WHERE sport=?
        """, (sport,)).fetchall()
        return {r[0]: {'elo': r[1], 'games': r[2], 'confidence': r[3], 'sos': r[4]} for r in rows}
    except:
        return {}


def elo_predicted_spread(home, away, elo_ratings, sport, neutral_site=False):
    """
    Predict spread from Elo ratings.

    Returns spread from home team perspective (negative = home favored).
    Returns None if either team has LOW confidence ratings.
    """
    cfg = ELO_CONFIG.get(sport, {})
    h = elo_ratings.get(home)
    a = elo_ratings.get(away)

    if not h or not a:
        return None

    # Require minimum confidence
    if h['confidence'] == 'LOW' or a['confidence'] == 'LOW':
        return None

    ha = 0 if neutral_site else cfg.get('home_advantage', 50)
    home_elo = h['elo'] + ha
    away_elo = a['elo']

    # Convert Elo difference to spread
    elo_diff = away_elo - home_elo  # Positive = home favored
    spread = elo_diff / cfg.get('spread_per_elo', 30)

    return round(spread, 2)


def elo_win_probability(home, away, elo_ratings, sport, neutral_site=False):
    """Predict home win probability from Elo."""
    cfg = ELO_CONFIG.get(sport, {})
    h = elo_ratings.get(home)
    a = elo_ratings.get(away)

    if not h or not a:
        return None

    ha = 0 if neutral_site else cfg.get('home_advantage', 50)
    home_elo = h['elo'] + ha
    away_elo = a['elo']

    scale = cfg.get('elo_scale', 400)
    return _expected_score(home_elo, away_elo, scale=scale)


def blended_spread(home, away, elo_ratings, market_ratings, sport, conn, neutral_site=False):
    """
    Blend Elo prediction with market-derived spread.
    
    The blend weights depend on how much Elo data we have:
    - <10 games per team: 20% Elo, 80% market (Elo is unreliable)
    - 10-20 games: 40% Elo, 60% market
    - 20+ games: 60% Elo, 40% market (Elo has proven itself)
    
    Elo inflation from cupcake schedules is controlled structurally via
    MOV cap (15) and autocorrelation correction (0.004 coefficient).
    
    This is the KEY function — it creates a spread prediction that's
    partly independent of the market, which is where edges come from.
    """
    from model_engine import compute_model_spread, SPORT_CONFIG
    
    elo_spread = elo_predicted_spread(home, away, elo_ratings, sport, neutral_site=neutral_site)
    market_spread = compute_model_spread(home, away, market_ratings, sport)
    
    if elo_spread is None and market_spread is None:
        return None
    if elo_spread is None:
        return market_spread
    if market_spread is None:
        return elo_spread
    
    # Determine blend weight based on data confidence
    h_data = elo_ratings.get(home, {})
    a_data = elo_ratings.get(away, {})
    min_games = min(h_data.get('games', 0), a_data.get('games', 0))
    
    cfg = ELO_CONFIG.get(sport, {})
    min_threshold = cfg.get('min_games', 15)
    
    if min_games >= min_threshold:
        elo_weight = 0.60  # Trust Elo more
    elif min_games >= min_threshold // 2:
        elo_weight = 0.40  # Balanced
    else:
        elo_weight = 0.20  # Still mostly market

    # v12.3: SOS confidence modifier — weaker schedules = less trust in Elo
    # v17: Skip for tennis — individual players don't have team SOS.
    # Tennis SOS defaults to 1500 (neutral) so this is currently a no-op,
    # but if SOS computation is ever added for tennis it would incorrectly
    # penalize players who faced weaker draws.
    if not sport.startswith('tennis_'):
        h_sos = h_data.get('sos', 1500)
        a_sos = a_data.get('sos', 1500)
        avg_sos = (h_sos + a_sos) / 2
        if avg_sos < 1450:
            sos_penalty = (1450 - avg_sos) / 200  # Max ~0.15 reduction
            elo_weight = max(0.15, elo_weight - sos_penalty)
        elif avg_sos > 1550:
            sos_bonus = (avg_sos - 1550) / 400  # Max ~0.05 boost
            elo_weight = min(0.70, elo_weight + sos_bonus)
    
    blended = elo_weight * elo_spread + (1 - elo_weight) * market_spread
    
    # v12 FIX: Spread expansion to correct for systematic compression.
    # MOV cap + conservative Elo + blending with compressed market model
    # = model spreads are always pulled toward zero.
    # This makes it impossible to bet favorites (model never thinks -15.5 should be -18).
    # Expansion stretches the spread away from zero to match market magnitude.
    #
    # v15 RECALIBRATION (200-game sample):
    # Actual BLENDED spread / market ratios (not raw Elo):
    #   NCAAB: 0.38 pre-expansion — blended spread is 38% of market magnitude
    #   NBA: 0.45 — model produces 45% of market spread
    #   NHL: 0.98 — already calibrated
    # Previous 1.20 NCAAB expansion gave 0.38*1.20 = 0.46 effective — still
    # way too compressed, zero favorite picks generated all season.
    # New target: ~0.67 effective — enough to unlock favorite-side picks
    # while keeping enough disagreement with market for genuine edge detection.
    # 0.38 * 1.75 = 0.67 effective.
    SPREAD_EXPANSION = {
        'basketball_ncaab': 1.75,  # 0.38 * 1.75 = 0.67 effective (was 1.20→0.46, data: 200 games)
        'basketball_nba': 1.40,    # 0.45 * 1.40 = 0.63 effective
        'icehockey_nhl': 1.0,      # 0.98 ratio — already calibrated
        'baseball_ncaa': 1.0,      # Run line is fixed ±1.5, uses Elo win prob instead
        'baseball_mlb': 1.0,       # Same — ML sport, uses Elo win prob
    }
    # Soccer: moderate expansion. No graded spread data yet to calibrate precisely,
    # but same Elo compression blocks nearly all soccer picks. 1.20 is conservative
    # estimate — will recalibrate once we have 20+ graded soccer spreads.
    for s in ['soccer_epl', 'soccer_spain_la_liga', 'soccer_italy_serie_a',
              'soccer_germany_bundesliga', 'soccer_france_ligue_one',
              'soccer_uefa_champs_league', 'soccer_usa_mls', 'soccer_mexico_ligamx']:
        SPREAD_EXPANSION[s] = 1.40

    # Tennis: start at 1.0 (no expansion) — calibrate after backtest data
    if sport.startswith('tennis_'):
        SPREAD_EXPANSION[sport] = 1.0
    
    expansion = SPREAD_EXPANSION.get(sport, 1.0)
    blended = blended * expansion
    
    return round(blended, 2)


def build_tennis_elo(verbose=True):
    """
    Build surface-split Elo ratings for tennis players.

    Tennis results are stored under per-tournament sport keys (e.g., tennis_atp_french_open).
    We aggregate all ATP results by surface into tennis_atp_hard, tennis_atp_clay,
    tennis_atp_grass (and same for WTA). This gives each player separate ratings
    per surface — critical because a player's skill varies hugely by surface.
    """
    from config import TENNIS_SURFACES

    conn = sqlite3.connect(DB_PATH)

    # Gather all tennis results grouped by tour + surface
    all_tennis = conn.execute("""
        SELECT sport, home, away, home_score, away_score, actual_margin, commence_time
        FROM results
        WHERE sport LIKE 'tennis_%' AND completed=1
        AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY commence_time ASC
    """).fetchall()

    if not all_tennis:
        if verbose:
            print("  ⚠ No tennis results — run historical_scores.py with tennis first")
        conn.close()
        return {}

    # Group by surface-tour key
    surface_games = defaultdict(list)
    for sport, home, away, h_score, a_score, margin, commence in all_tennis:
        surface = TENNIS_SURFACES.get(sport, 'hard')
        tour = 'atp' if '_atp_' in sport else 'wta'
        surface_key = f'tennis_{tour}_{surface}'
        if margin is None:
            margin = h_score - a_score
        surface_games[surface_key].append((home, away, h_score, a_score, margin, commence))

    if verbose:
        print(f"\n  🎾 TENNIS ELO — Surface-split ratings")
        for sk, games in sorted(surface_games.items()):
            print(f"    {sk}: {len(games)} matches")

    all_ratings = {}
    for surface_key, games in surface_games.items():
        if verbose:
            print(f"\n  ── {surface_key} {'─' * (45 - len(surface_key))}")

        cfg = ELO_CONFIG.get(surface_key)
        if not cfg:
            if verbose:
                print(f"  ⚠ No Elo config for {surface_key}")
            continue

        if not games:
            continue

        if verbose:
            print(f"  📊 {surface_key}: {len(games)} matches to process")

        # Build Elo inline (can't use build_elo_ratings because results are
        # stored under per-tournament keys, not surface keys)
        elos = defaultdict(lambda: cfg['initial_elo'])
        game_counts = defaultdict(int)
        predictions = []

        for home, away, h_score, a_score, margin, commence in games:
            home_elo = elos[home]  # No home advantage in tennis
            away_elo = elos[away]

            expected_home = _expected_score(home_elo, away_elo)
            predicted_spread = (away_elo - home_elo) / cfg['spread_per_elo']

            predictions.append({
                'home': home, 'away': away,
                'predicted_spread': predicted_spread,
                'actual_margin': margin,
                'predicted_home_win': expected_home,
                'home_won': margin > 0,
                'date': commence,
            })

            actual_home = 1.0 if margin > 0 else (0.0 if margin < 0 else 0.5)
            elo_diff = home_elo - away_elo
            mov_mult = _mov_multiplier(margin, elo_diff, cfg)
            k_adjust = cfg['k_factor']

            delta = k_adjust * mov_mult * (actual_home - expected_home)
            elos[home] += delta
            elos[away] -= delta

            game_counts[home] += 1
            game_counts[away] += 1

        # Normalize
        avg_elo = sum(elos.values()) / len(elos) if elos else 1500
        for player in elos:
            elos[player] = round(elos[player] - avg_elo + 1500, 1)

        # Calculate accuracy
        meta = _calculate_accuracy(predictions, cfg, verbose)
        meta['total_games'] = len(games)
        meta['total_teams'] = len(elos)
        meta['game_counts'] = dict(game_counts)

        # Save to elo_ratings table
        _save_elo_ratings(conn, surface_key, dict(elos), game_counts, cfg, meta)

        all_ratings[surface_key] = dict(elos)

        if elos and verbose:
            ranked = sorted(elos.items(), key=lambda x: x[1], reverse=True)
            print(f"  Top 5:")
            for t, r in ranked[:5]:
                print(f"    🎾 {t:30s} {r:.0f}")

    conn.close()
    return all_ratings


def get_tennis_elo(conn, tournament_key):
    """
    Get the correct surface-split Elo ratings for a tennis tournament.

    Maps a tournament key (e.g., tennis_atp_french_open) to the surface-specific
    Elo key (tennis_atp_clay) and returns those ratings.
    """
    from config import TENNIS_SURFACES

    surface = TENNIS_SURFACES.get(tournament_key)
    if surface is None:
        # Infer surface from tournament name for dynamically detected tournaments
        _tk = tournament_key.lower()
        _CLAY = ['french_open', 'roland_garros', 'monte_carlo', 'madrid',
                 'italian_open', 'rome', 'barcelona', 'hamburg', 'rio',
                 'buenos_aires', 'lyon', 'bastad', 'kitzbuhel', 'umag',
                 'gstaad', 'geneva', 'marrakech', 'bucharest', 'parma',
                 'palermo', 'prague', 'rabat', 'strasbourg', 'lausanne',
                 'portoroz', 'bogota', 'istanbul', 'budapest']
        _GRASS = ['wimbledon', 'queens', 'halle', 'eastbourne', 'berlin',
                  'bad_homburg', 'nottingham', 'mallorca', 's_hertogenbosch',
                  'birmingham', 'libema']
        if any(kw in _tk for kw in _CLAY):
            surface = 'clay'
        elif any(kw in _tk for kw in _GRASS):
            surface = 'grass'
        else:
            surface = 'hard'
    tour = 'atp' if '_atp_' in tournament_key else 'wta'
    elo_key = f'tennis_{tour}_{surface}'
    return get_elo_ratings(conn, elo_key), elo_key


def build_all_elo(sports=None, verbose=True):
    """Build Elo ratings for all sports."""
    if sports is None:
        sports = list(ELO_CONFIG.keys())
    
    print("=" * 60)
    print("  ELO RATINGS — Built from actual game results")
    print("=" * 60)
    
    all_ratings = {}
    for sport in sports:
        # Skip tennis surface keys here — handled by build_tennis_elo()
        if sport.startswith('tennis_'):
            continue
        print(f"\n  ── {sport} {'─' * (45 - len(sport))}")
        elos, meta = build_elo_ratings(sport, verbose=verbose)
        all_ratings[sport] = elos

        if elos:
            ranked = sorted(elos.items(), key=lambda x: x[1], reverse=True)
            print(f"  Top 5:")
            for t, r in ranked[:5]:
                print(f"    🏆 {t:30s} {r:.0f}")
            print(f"  Bottom 3:")
            for t, r in ranked[-3:]:
                print(f"    📉 {t:30s} {r:.0f}")

    # Tennis: surface-split Elo (aggregates per-tournament results by surface)
    tennis_ratings = build_tennis_elo(verbose=verbose)
    all_ratings.update(tennis_ratings)

    return all_ratings


def analyze_model(sport, verbose=True):
    """
    Detailed analysis: how does Elo compare to market spreads?
    
    This shows WHERE Elo disagrees with the market — those disagreements
    are potential edges.
    """
    conn = sqlite3.connect(DB_PATH)
    elo_data = get_elo_ratings(conn, sport)
    
    if not elo_data:
        print(f"  No Elo ratings for {sport}. Run build first.")
        conn.close()
        return
    
    from model_engine import get_latest_ratings, compute_model_spread
    market_ratings = get_latest_ratings(conn, sport)
    
    # Get recent games with market spreads
    games = conn.execute("""
        SELECT mc.home, mc.away, mc.best_home_spread, r.actual_margin
        FROM market_consensus mc
        LEFT JOIN results r ON mc.sport = r.sport AND mc.home = r.home AND mc.away = r.away
        WHERE mc.sport=? AND mc.best_home_spread IS NOT NULL
        ORDER BY mc.commence_time DESC LIMIT 30
    """, (sport,)).fetchall()
    
    print(f"\n  {'Team Matchup':50s} {'Mkt':>6s} {'Elo':>6s} {'Blend':>6s} {'Diff':>6s} {'Result':>7s}")
    print(f"  {'─' * 85}")
    
    elo_better = 0
    mkt_better = 0
    
    for home, away, mkt_spread, actual in games:
        elo_spread = elo_predicted_spread(home, away, elo_data, sport)
        blend = blended_spread(home, away, elo_data, market_ratings, sport, conn)
        
        if elo_spread is None:
            continue
        
        diff = elo_spread - mkt_spread
        result_str = f"{actual:+d}" if actual is not None else "?"
        
        # Who was more accurate?
        if actual is not None:
            elo_err = abs(elo_spread - actual)
            mkt_err = abs(mkt_spread - actual)
            if elo_err < mkt_err:
                elo_better += 1
                marker = " ✓"
            else:
                mkt_better += 1
                marker = ""
        else:
            marker = ""
        
        flag = " ← EDGE" if abs(diff) > 2 else ""
        print(f"  {away:24s}@ {home:24s} {mkt_spread:+6.1f} {elo_spread:+6.1f} {blend:+6.1f} {diff:+6.1f} {result_str:>7s}{flag}{marker}")
    
    if elo_better + mkt_better > 0:
        print(f"\n  Elo more accurate: {elo_better}/{elo_better+mkt_better} ({elo_better/(elo_better+mkt_better):.0%})")
    
    conn.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sport', type=str, help='Specific sport')
    parser.add_argument('--analyze', action='store_true', help='Show accuracy analysis')
    args = parser.parse_args()
    
    sports = [args.sport] if args.sport else None
    
    build_all_elo(sports=sports)
    
    if args.analyze:
        target = [args.sport] if args.sport else list(ELO_CONFIG.keys())
        for s in target:
            analyze_model(s)
