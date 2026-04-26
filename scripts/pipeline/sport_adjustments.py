"""
Sport-specific scoring adjustments + totals math helpers.

Extracted from model_engine.py in v26.0 Phase 7. Behavior is byte-equivalent
to the pre-refactor inline versions; verified by tests/shadow_predict.py.

Public surface (callers can `from pipeline.sport_adjustments import X`):
    Tennis:
        _tennis_surface_from_sport(sport_key) -> 'hard'|'clay'|'grass'
        _tennis_h2h_adjustment(conn, p1, p2, sport) -> (adj, ctx_string)

    Team-stat aggregation (used by estimate_model_total):
        _get_dynamic_league_avg_total(conn, sport) -> float
        _weighted_team_stats(conn, team, sport, elo_ratings, min_games) -> dict

    MLB pitcher / park / bullpen:
        _mlb_pitcher_era_adjustment(conn, mlb_pitcher_info) -> (adj, ctx, best_era, worst_era, both_reliable)
        _mlb_park_factor_adjustment(conn, home, away, side) -> (delta, ctx, raw_adj)
        _mlb_bullpen_adjustment(conn, home, away) -> (adj, ctx)

    NHL goalie:
        _nhl_goalie_adjustment(conn, nhl_goalie_info) -> (adj, ctx)

    Totals math:
        estimate_model_total(home, away, ratings, sport, conn) -> float|None
        _totals_confidence(home, away, sport, conn) -> 'HIGH'|'MEDIUM'|'LOW'
        calculate_point_value_totals(model_total, market_total, sport) -> float
        _total_prob(diff, sport) -> float
        _divergence_penalty(model_val, market_val, market_type) -> float

For backwards compatibility, all symbols above are also re-exported from
`model_engine` so existing `from model_engine import X` callers keep working.
"""
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# Module-level constants used by the totals/park functions below.
# Moved here from model_engine.py in v26.0 Phase 7.

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


# Local copy of `_ncdf` (also lives in model_engine.py for spread_to_cover_prob).
# Kept here to avoid a circular import — model_engine re-exports this module's
# names back into its own namespace, so we can't import from model_engine at
# load time.
def _ncdf(z):
    """Standard-normal CDF approximation (Abramowitz-Stegun 26.2.17)."""
    import math
    if z > 6: return 1.0
    if z < -6: return 0.0
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if z >= 0 else -1
    t = 1.0 / (1.0 + p * abs(z))
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-z * z / 2)
    return 0.5 * (1.0 + sign * y)


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


