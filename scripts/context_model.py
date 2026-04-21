"""context_model.py v1 — Secondary spread model using real-world context.

Produces `ms_context` = raw Elo spread + context adjustments. Used alongside
the raw Elo model (which feeds SPREAD_FADE_FLIP via its divergence signal).

Phase 1 adjustments:
  - Injury (100% weight of point_impact delta between teams)
  - Recent form (last 5 games margin vs season average)

Phase 2+ (not implemented yet): rest days / B2B, motivation (tanking,
seeding locked), confirmed starting lineup, home/away splits.

Usage:
    from context_model import compute_context_spread
    ms_context, info = compute_context_spread(conn, sport, home, away,
                                               event_id, ms_elo)

Returns:
    ms_context: float — injury- and form-adjusted home spread (same sign
                        convention as raw ms: negative = home favored)
    info: dict — breakdown of adjustments for logging/debug
"""
import sqlite3
from datetime import datetime, timedelta


INJURY_WEIGHT = 1.0  # 100% — market prices full injury; we match to reveal
                     # when our model agrees with market after injury adj
FORM_WEIGHT = 0.5    # 50% — recent form is real but overweighting risks
                     # chasing small streaks
RECENT_GAMES = 5     # Last 5 games for form snapshot

# --- Phase 2 weights ---
B2B_PENALTY = -2.5   # 2nd night of back-to-back = 2.5 pt spread penalty
THREE_IN_FOUR_PENALTY = -1.5
LONG_REST_BONUS = 1.0  # 4+ days rest
LONG_REST_THRESHOLD = 4

# Playoff regime detection — NBA + NHL playoffs start ~4/19 2026
PLAYOFF_START = {
    'basketball_nba': '2026-04-19',
    'icehockey_nhl':  '2026-04-19',
}

# Tanking: teams still "playing hard" vs teams coasting/tanking
# During regular season end (last 2 weeks), teams below .500 with nothing
# to play for get a motivation penalty.
TANKING_PENALTY = -3.0
# Playoff-lock motivation — teams with seed locked may rest starters;
# we already capture via injuries table when players confirmed out, but
# a mild "coasting" adjustment applies too
COASTING_PENALTY = -1.5

# --- Phase 5 weights (historical + concentration) ---
# H2H regular-season margin — how did these two teams do against each other?
# If home swept away 3-0 by avg 10 pts, that's a strong signal for tonight.
H2H_WEIGHT = 0.4
H2H_MIN_MEETINGS = 2
# Extended recency — last 20 games weighted to smooth last-5 variance
EXT_FORM_GAMES = 20
EXT_FORM_WEIGHT = 0.3
# Star concentration — if team has only 1-2 scorers averaging 20+ PPG and
# one is out, impact is amplified beyond raw point_impact.
STAR_CONCENTRATION_MULT = 1.5
STAR_CONCENTRATION_MIN_PPG = 18.0
# Pace / efficiency — team's avg points scored minus allowed, last 10.
# Normalized to league median so mismatches show up as spread adjustment.
PACE_WEIGHT = 0.15

# --- Phase 4 weights (situational) ---
# Team-specific home/away split advantage. Some teams are +8 at home, -4
# on road (big net HCA). Others are +3 / -1 (mild HCA). Our base Elo uses
# a single HCA for the whole league. Adjust by team-specific delta.
HOME_SPLIT_WEIGHT = 0.5
# Extended recency weighting — last 20 games get double weight in form calc.
# Bridges the gap between season-long Elo and recent-form Phase 1 signal.
EXT_RECENCY_GAMES = 20
EXT_RECENCY_WEIGHT = 0.3

# --- Phase 3 weights (playoff-specific) ---
# Playoffs amplify home-court advantage — crowd, adjusted game-plan, referee
# pressure. Standard NBA regular season HCA is ~2.5 pts; playoffs closer to 3.5.
PLAYOFF_HCA_BONUS = 1.0   # extra home edge in playoffs
# Playoff rotations shrink — benches get cut, stars play more. Losing a
# star in playoffs is worse than regular season because the replacement
# is a role player getting more minutes.
PLAYOFF_INJURY_AMPLIFICATION = 1.3  # 30% boost to injury_adj in playoffs
# Series momentum: if teams played recently (same series), heavily weight
# the last game's margin direction. e.g. Game 1 blowout predicts Game 2.
SERIES_MOMENTUM_WEIGHT = 0.25  # 25% of last-meeting margin carries forward
# Games-into-series adjustment: later games (4+) have different dynamics
# than Game 1. Adjustments are experimental and applied only if we've seen
# 3+ meetings recently.


def _get_injury_impact(conn, sport, team):
    """Sum of point_impact for Out/Doubtful players on team today."""
    today = datetime.now().strftime('%Y-%m-%d')
    r = conn.execute("""
        SELECT COALESCE(SUM(point_impact), 0) FROM injuries
        WHERE sport = ? AND team = ?
          AND status IN ('Out', 'Doubtful', 'Day-To-Day')
          AND DATE(report_date) = ?
    """, (sport, team, today)).fetchone()
    return float(r[0] if r else 0)


def _get_recent_form_margin(conn, sport, team, before_date):
    """Average game margin for team over last N games ended before given date.
    Positive = team wins by average N pts; negative = loses by N.
    """
    r = conn.execute("""
        SELECT home, away, home_score, away_score FROM results
        WHERE sport = ?
          AND (home = ? OR away = ?)
          AND home_score IS NOT NULL
          AND DATE(commence_time) < ?
        ORDER BY commence_time DESC LIMIT ?
    """, (sport, team, team, before_date, RECENT_GAMES)).fetchall()

    if len(r) < 3:
        return None, len(r)  # insufficient data

    margins = []
    for home, away, hs, as_ in r:
        if team == home:
            margins.append(hs - as_)
        else:
            margins.append(as_ - hs)
    avg = sum(margins) / len(margins)
    return avg, len(margins)


def _get_rest_days(conn, sport, team, before_date):
    """Days since team's last game before `before_date`. Returns None if no prior game."""
    r = conn.execute("""
        SELECT MAX(commence_time) FROM results
        WHERE sport = ?
          AND (home = ? OR away = ?)
          AND home_score IS NOT NULL
          AND DATE(commence_time) < ?
    """, (sport, team, team, before_date)).fetchone()
    if not r or not r[0]:
        return None
    from datetime import datetime
    try:
        last = datetime.fromisoformat(r[0].replace('Z', '+00:00')).date()
        curr = datetime.fromisoformat(before_date).date()
        return (curr - last).days
    except Exception:
        return None


def _count_games_in_window(conn, sport, team, before_date, days):
    """Count games team played in the N-day window ending before `before_date`."""
    from datetime import datetime, timedelta
    curr = datetime.fromisoformat(before_date).date()
    start = (curr - timedelta(days=days)).isoformat()
    r = conn.execute("""
        SELECT COUNT(*) FROM results
        WHERE sport = ?
          AND (home = ? OR away = ?)
          AND home_score IS NOT NULL
          AND DATE(commence_time) >= ?
          AND DATE(commence_time) < ?
    """, (sport, team, team, start, before_date)).fetchone()
    return r[0] if r else 0


def _get_rest_adjustment(conn, sport, team, commence_date):
    """Returns (adjustment_pts, label) for schedule/rest factors."""
    rest = _get_rest_days(conn, sport, team, commence_date)
    if rest is None:
        return 0.0, None
    if rest <= 1:
        return B2B_PENALTY, 'B2B'
    gw = _count_games_in_window(conn, sport, team, commence_date, 4)
    if gw >= 3:
        return THREE_IN_FOUR_PENALTY, '3_in_4'
    if rest >= LONG_REST_THRESHOLD:
        return LONG_REST_BONUS, 'long_rest'
    return 0.0, None


def _is_playoff_game(sport, commence_date):
    """Check if game date is in playoff window."""
    start = PLAYOFF_START.get(sport)
    if not start: return False
    return commence_date >= start


def _get_h2h_margin(conn, sport, home, away, before_date):
    """Average HOME margin from all past regular-season meetings between
    the two teams this season. Returns (avg_home_margin, count)."""
    r = conn.execute("""
        SELECT home, away, home_score, away_score
        FROM results
        WHERE sport = ? AND home_score IS NOT NULL
          AND DATE(commence_time) >= '2026-03-01'
          AND DATE(commence_time) < ?
          AND ((home = ? AND away = ?) OR (home = ? AND away = ?))
    """, (sport, before_date, home, away, away, home)).fetchall()
    if len(r) < H2H_MIN_MEETINGS:
        return None, len(r)
    margins_from_home = []
    for h, a, hs, as_ in r:
        if h == home:
            margins_from_home.append(hs - as_)
        else:
            margins_from_home.append(as_ - hs)
    avg = sum(margins_from_home)/len(margins_from_home)
    return avg, len(r)


def _get_h2h_adjustment(conn, sport, home, away, before_date):
    """Apply H2H margin as spread adjustment."""
    margin, n = _get_h2h_margin(conn, sport, home, away, before_date)
    if margin is None:
        return 0.0, None
    # Negative ms = home favored. H2H margin positive = home has won by X avg.
    # Add -margin * H2H_WEIGHT (home wins → ms more negative).
    adj = -margin * H2H_WEIGHT
    return adj, f'h2h_n={n}_avg={margin:+.1f}'


def _get_extended_form_adjustment(conn, sport, home, away, before_date):
    """Last-20 weighted form differential. Returns (adj, label)."""
    def last_n_avg(team):
        r = conn.execute("""
            SELECT home, away, home_score, away_score FROM results
            WHERE sport = ? AND (home = ? OR away = ?)
              AND home_score IS NOT NULL AND DATE(commence_time) < ?
            ORDER BY commence_time DESC LIMIT ?
        """, (sport, team, team, before_date, EXT_FORM_GAMES)).fetchall()
        if len(r) < 5: return None
        margins = [(hs - as_) if team == h else (as_ - hs) for h, a, hs, as_ in r]
        return sum(margins) / len(margins)
    h_avg = last_n_avg(home)
    a_avg = last_n_avg(away)
    if h_avg is None or a_avg is None:
        return 0.0, None
    diff = h_avg - a_avg  # positive = home better recently
    adj = -diff * EXT_FORM_WEIGHT
    return adj, f'ext_form h{h_avg:+.1f}/a{a_avg:+.1f}'


def _get_player_avg_pts(conn, player, before_date):
    """Player's avg points over last 30 days (from box_scores)."""
    r = conn.execute("""
        SELECT AVG(stat_value) FROM box_scores
        WHERE player = ? AND stat_type = 'pts'
          AND DATE(game_date) >= DATE(?, '-30 days')
          AND DATE(game_date) < ?
    """, (player, before_date, before_date)).fetchone()
    return float(r[0]) if r and r[0] else None


def _get_star_concentration_amplifier(conn, sport, team, before_date):
    """If team has only 1-2 players averaging 18+ PPG and a star is out,
    amplify injury impact. Returns multiplier (1.0 = no amplification)."""
    # Get current players on team with 18+ PPG
    r = conn.execute("""
        SELECT DISTINCT player FROM box_scores
        WHERE sport = ? AND team = ? AND stat_type = 'pts'
          AND DATE(game_date) >= DATE(?, '-30 days')
          AND DATE(game_date) < ?
        GROUP BY player HAVING AVG(stat_value) >= ?
    """, (sport, team, before_date, before_date, STAR_CONCENTRATION_MIN_PPG)).fetchall()
    num_stars = len(r)
    star_names = {row[0] for row in r}
    if num_stars == 0 or num_stars >= 4:
        return 1.0  # Either no stars or deep team — no amplification
    # Check if any of these stars are injured today
    inj = conn.execute("""
        SELECT player FROM injuries
        WHERE sport = ? AND team = ? AND status IN ('Out','Doubtful','Day-To-Day')
          AND DATE(report_date) = ?
    """, (sport, team, before_date)).fetchall()
    injured_players = {row[0] for row in inj}
    stars_out = len(star_names & injured_players)
    if stars_out == 0:
        return 1.0
    # 1 of 2 stars out = 1.5x; 1 of 3 = 1.25x
    if num_stars <= 2 and stars_out >= 1:
        return STAR_CONCENTRATION_MULT
    return 1.0 + (stars_out / num_stars) * 0.5


def _get_pace_adjustment(conn, sport, home, away, before_date):
    """Team scoring differential last 10 games as pace signal."""
    def recent_scoring(team):
        r = conn.execute("""
            SELECT home, away, home_score, away_score FROM results
            WHERE sport = ? AND (home = ? OR away = ?)
              AND home_score IS NOT NULL AND DATE(commence_time) < ?
            ORDER BY commence_time DESC LIMIT 10
        """, (sport, team, team, before_date)).fetchall()
        if len(r) < 5: return None
        scored, allowed = [], []
        for h, a, hs, as_ in r:
            if team == h:
                scored.append(hs); allowed.append(as_)
            else:
                scored.append(as_); allowed.append(hs)
        return sum(scored)/len(scored) - sum(allowed)/len(allowed)
    h_eff = recent_scoring(home)
    a_eff = recent_scoring(away)
    if h_eff is None or a_eff is None:
        return 0.0, None
    diff = h_eff - a_eff
    adj = -diff * PACE_WEIGHT
    return adj, f'pace h{h_eff:+.1f}/a{a_eff:+.1f}'


def _get_home_away_split(conn, sport, team, before_date):
    """Return (home_avg_margin, away_avg_margin) for team over full season.
    Captures team-specific home-court tendency beyond generic HCA.
    """
    r = conn.execute("""
        SELECT home, away, home_score, away_score FROM results
        WHERE sport = ? AND (home = ? OR away = ?)
          AND home_score IS NOT NULL
          AND DATE(commence_time) >= '2026-03-01'
          AND DATE(commence_time) < ?
    """, (sport, team, team, before_date)).fetchall()
    home_margins, away_margins = [], []
    for h, a, hs, as_ in r:
        if team == h:
            home_margins.append(hs - as_)
        else:
            away_margins.append(as_ - hs)
    h_avg = sum(home_margins)/len(home_margins) if len(home_margins) >= 3 else None
    a_avg = sum(away_margins)/len(away_margins) if len(away_margins) >= 3 else None
    return h_avg, a_avg


def _get_home_away_adjustment(conn, sport, home, away, commence_date):
    """Compare team-specific home/away tendencies. If home team has a bigger
    home edge than average AND away team struggles on road → amplify. Returns
    adjustment to ms (negative = home more favored)."""
    h_home, h_road = _get_home_away_split(conn, sport, home, commence_date)
    a_home, a_road = _get_home_away_split(conn, sport, away, commence_date)
    adj = 0.0
    if h_home is not None and h_road is not None:
        # Home team's home vs road delta. If huge (+10 at home, -5 on road),
        # delta is 15. A 10-pt delta means team is playing tonight at home
        # so expect +2.5 pts bonus vs baseline.
        h_delta = h_home - h_road
        adj -= h_delta * HOME_SPLIT_WEIGHT * 0.25  # 25% of the split to stay conservative
    if a_home is not None and a_road is not None:
        a_delta = a_home - a_road
        adj += a_delta * HOME_SPLIT_WEIGHT * 0.25
    return adj, {'h_home': h_home, 'h_road': h_road, 'a_home': a_home, 'a_road': a_road}


def _get_series_context(conn, sport, home, away, commence_date):
    """For playoff games: find recent meetings between these two teams.
    Returns (meetings_count, last_meeting_home_margin, last_meeting_date).
    """
    from datetime import datetime, timedelta
    curr = datetime.fromisoformat(commence_date).date()
    # Look back 21 days for series meetings
    start = (curr - timedelta(days=21)).isoformat()
    r = conn.execute("""
        SELECT home, away, home_score, away_score, DATE(commence_time) dt
        FROM results
        WHERE sport = ? AND home_score IS NOT NULL
          AND DATE(commence_time) >= ? AND DATE(commence_time) < ?
          AND ((home = ? AND away = ?) OR (home = ? AND away = ?))
        ORDER BY commence_time DESC
    """, (sport, start, commence_date, home, away, away, home)).fetchall()
    if not r:
        return 0, None, None
    # Convert to home_margin as if today's home is the one being compared
    # If previous game had same home, home_margin = hs - as_
    # If previous game had reversed home, invert
    last_game = r[0]
    prev_home, prev_away, prev_hs, prev_as, prev_dt = last_game
    if prev_home == home:
        home_margin_prev = prev_hs - prev_as
    else:
        home_margin_prev = prev_as - prev_hs  # flip perspective
    return len(r), home_margin_prev, prev_dt


def _get_playoff_adjustments(conn, sport, home, away, commence_date,
                              injury_adj_base):
    """Returns playoff-specific adjustments:
       (hca_adj, injury_amp_adj, momentum_adj, info_labels)
    """
    if not _is_playoff_game(sport, commence_date):
        return 0.0, 0.0, 0.0, {}

    labels = {'playoff': True}

    # 1) Home-court amplification
    hca_adj = -PLAYOFF_HCA_BONUS  # ms more negative = home more favored
    labels['hca_adj'] = hca_adj

    # 2) Injury amplification in playoffs (tighter rotations → bigger star impact)
    #    injury_adj_base is already computed; amplify by (amplification-1)
    injury_amp_adj = injury_adj_base * (PLAYOFF_INJURY_AMPLIFICATION - 1.0)
    labels['injury_amp_adj'] = injury_amp_adj

    # 3) Series momentum — if same teams met recently, carry forward margin
    n_meetings, last_margin, last_date = _get_series_context(
        conn, sport, home, away, commence_date)
    momentum_adj = 0.0
    if n_meetings >= 1 and last_margin is not None:
        # If home won last meeting by X, home probably covers again
        # ms more negative if home won last time
        momentum_adj = -last_margin * SERIES_MOMENTUM_WEIGHT
        labels['series_meetings'] = n_meetings
        labels['last_margin'] = last_margin
        labels['momentum_adj'] = momentum_adj

    return hca_adj, injury_amp_adj, momentum_adj, labels


def _get_motivation_adjustment(conn, sport, team, commence_date):
    """Tanking / coasting detection. Returns (adjustment, label).

    Late-season regular-season games where team is below .500 with losing
    streak gets TANKING_PENALTY. Playoff-locked teams get COASTING_PENALTY.
    Playoff teams (bubble, contenders) get no adjustment — already trying.
    """
    if _is_playoff_game(sport, commence_date):
        # Playoffs: all teams trying hard; no tanking penalty
        return 0.0, None

    # Check recent record — below .500 last 10 games = tanking candidate
    from datetime import datetime, timedelta
    curr = datetime.fromisoformat(commence_date).date()
    cutoff = (curr - timedelta(days=30)).isoformat()
    r = conn.execute("""
        SELECT home, away, home_score, away_score FROM results
        WHERE sport = ? AND (home = ? OR away = ?)
          AND home_score IS NOT NULL AND DATE(commence_time) >= ?
          AND DATE(commence_time) < ?
        ORDER BY commence_time DESC LIMIT 10
    """, (sport, team, team, cutoff, commence_date)).fetchall()
    if len(r) < 5:
        return 0.0, None
    wins = 0
    for home, away, hs, as_ in r:
        team_won = (team == home and hs > as_) or (team == away and as_ > hs)
        if team_won: wins += 1
    win_rate = wins / len(r)
    if win_rate < 0.3:
        return TANKING_PENALTY, f'tanking ({wins}-{len(r)-wins} L10)'
    return 0.0, None


def _get_season_avg_margin(conn, sport, team):
    """Season-to-date average margin for team (used as form baseline)."""
    r = conn.execute("""
        SELECT home, away, home_score, away_score FROM results
        WHERE sport = ?
          AND (home = ? OR away = ?)
          AND home_score IS NOT NULL
          AND DATE(commence_time) >= '2026-03-01'
    """, (sport, team, team)).fetchall()

    if len(r) < 10:
        return None

    margins = []
    for home, away, hs, as_ in r:
        if team == home:
            margins.append(hs - as_)
        else:
            margins.append(as_ - hs)
    return sum(margins) / len(margins)


def compute_context_spread(conn, sport, home, away, event_id, ms_elo,
                            commence_date=None):
    """Compute injury + form adjusted home spread.

    Args:
        conn: sqlite3 connection
        sport: sport key (e.g. 'basketball_nba')
        home, away: team names (must match names in `injuries` and `results`)
        event_id: game event_id
        ms_elo: raw Elo-based home spread (negative = home favored)
        commence_date: YYYY-MM-DD of the game (defaults to today)

    Returns:
        (ms_context, info)
            ms_context: adjusted home spread
            info: {'injury_adj', 'form_adj', 'h_inj', 'a_inj', 'h_form',
                   'a_form', 'raw_ms', 'adjusted_ms'}
    """
    info = {'raw_ms': ms_elo}

    if commence_date is None:
        commence_date = datetime.now().strftime('%Y-%m-%d')

    # --- INJURY ADJUSTMENT ---
    # Convention: h_inj, a_inj are sum of point_impact for OUT players.
    # Positive h_inj = home MORE hurt. Home spread worsens by h_inj (ms goes up = toward away).
    # Net effect on ms: +h_inj (worsen for home) and -a_inj (worsen for away).
    h_inj = _get_injury_impact(conn, sport, home)
    a_inj = _get_injury_impact(conn, sport, away)
    # Phase 5: apply star-concentration amplifier per side
    h_star_mult = _get_star_concentration_amplifier(conn, sport, home, commence_date)
    a_star_mult = _get_star_concentration_amplifier(conn, sport, away, commence_date)
    h_inj_amp = h_inj * h_star_mult
    a_inj_amp = a_inj * a_star_mult
    injury_delta = h_inj_amp - a_inj_amp  # positive = home more hurt
    injury_adj = injury_delta * INJURY_WEIGHT
    info['h_inj'] = h_inj
    info['a_inj'] = a_inj
    info['h_star_mult'] = h_star_mult
    info['a_star_mult'] = a_star_mult
    info['injury_adj'] = injury_adj

    # --- RECENT FORM ADJUSTMENT ---
    # h_form = avg margin for home team last 5 games
    # a_form = avg margin for away team last 5 games
    # If home's recent form is +10 but season is +5, they're playing better lately.
    # Form delta vs season reflects momentum.
    h_form, h_form_n = _get_recent_form_margin(conn, sport, home, commence_date)
    a_form, a_form_n = _get_recent_form_margin(conn, sport, away, commence_date)
    h_season = _get_season_avg_margin(conn, sport, home) if h_form else None
    a_season = _get_season_avg_margin(conn, sport, away) if a_form else None

    form_adj = 0.0
    if h_form is not None and h_season is not None:
        h_form_delta = h_form - h_season  # positive = home hotter than season
        form_adj -= h_form_delta * FORM_WEIGHT  # home hotter → ms more negative (home favored)
    if a_form is not None and a_season is not None:
        a_form_delta = a_form - a_season
        form_adj += a_form_delta * FORM_WEIGHT  # away hotter → ms more positive
    info['h_form'] = h_form
    info['a_form'] = a_form
    info['h_season'] = h_season
    info['a_season'] = a_season
    info['form_adj'] = form_adj
    info['h_form_n'] = h_form_n
    info['a_form_n'] = a_form_n

    # --- Phase 2: REST / B2B / schedule ---
    # Convention: home tired → ms goes UP (home less favored)
    #             away tired → ms goes DOWN (home more favored)
    h_rest_adj, h_rest_lbl = _get_rest_adjustment(conn, sport, home, commence_date)
    a_rest_adj, a_rest_lbl = _get_rest_adjustment(conn, sport, away, commence_date)
    # h_rest_adj is negative for tired home → home spread should worsen (ms up)
    # So we ADD -h_rest_adj to make ms more positive. Similarly subtract -a_rest_adj.
    rest_adj = -h_rest_adj + a_rest_adj
    info['h_rest_adj'] = h_rest_adj
    info['a_rest_adj'] = a_rest_adj
    info['h_rest_lbl'] = h_rest_lbl
    info['a_rest_lbl'] = a_rest_lbl
    info['rest_adj'] = rest_adj

    # --- Phase 2: MOTIVATION (tanking / coasting) ---
    h_mot_adj, h_mot_lbl = _get_motivation_adjustment(conn, sport, home, commence_date)
    a_mot_adj, a_mot_lbl = _get_motivation_adjustment(conn, sport, away, commence_date)
    # h_mot_adj < 0 for tanking home → home worse → ms UP (more positive)
    # a_mot_adj < 0 for tanking away → away worse → ms DOWN (more negative)
    mot_adj = -h_mot_adj + a_mot_adj
    info['h_mot_adj'] = h_mot_adj
    info['a_mot_adj'] = a_mot_adj
    info['h_mot_lbl'] = h_mot_lbl
    info['a_mot_lbl'] = a_mot_lbl
    info['mot_adj'] = mot_adj

    # --- Phase 3: PLAYOFF-SPECIFIC ADJUSTMENTS ---
    # Home-court amplification, injury amplification (tighter rotations),
    # series momentum (carry forward last meeting margin).
    hca_adj, injury_amp_adj, momentum_adj, playoff_info = _get_playoff_adjustments(
        conn, sport, home, away, commence_date, injury_adj)
    info['hca_adj'] = hca_adj
    info['injury_amp_adj'] = injury_amp_adj
    info['momentum_adj'] = momentum_adj
    info['playoff'] = playoff_info.get('playoff', False)
    if 'series_meetings' in playoff_info:
        info['series_meetings'] = playoff_info['series_meetings']
        info['last_margin'] = playoff_info['last_margin']

    # --- Phase 4: HOME/AWAY SPLITS ---
    split_adj, split_info = _get_home_away_adjustment(conn, sport, home, away, commence_date)
    info['split_adj'] = split_adj
    info['split_info'] = split_info

    # --- Phase 5: H2H regular-season margin ---
    h2h_adj, h2h_lbl = _get_h2h_adjustment(conn, sport, home, away, commence_date)
    info['h2h_adj'] = h2h_adj
    info['h2h_lbl'] = h2h_lbl

    # --- Phase 5: extended recent form (last 20 games) ---
    ext_adj, ext_lbl = _get_extended_form_adjustment(conn, sport, home, away, commence_date)
    info['ext_form_adj'] = ext_adj
    info['ext_form_lbl'] = ext_lbl

    # --- Phase 5: pace / scoring differential ---
    pace_adj, pace_lbl = _get_pace_adjustment(conn, sport, home, away, commence_date)
    info['pace_adj'] = pace_adj
    info['pace_lbl'] = pace_lbl

    # --- APPLY ALL ---
    ms_context = (ms_elo + injury_adj + form_adj + rest_adj + mot_adj
                   + hca_adj + injury_amp_adj + momentum_adj + split_adj
                   + h2h_adj + ext_adj + pace_adj)
    info['adjusted_ms'] = ms_context
    return ms_context, info


def format_context_summary(info):
    """Short summary string for logs / context_factors."""
    parts = []
    if info.get('injury_adj', 0) != 0:
        parts.append(f"inj={info['injury_adj']:+.1f} "
                     f"(h{info['h_inj']:.1f}/a{info['a_inj']:.1f})")
    if info.get('form_adj', 0) != 0:
        parts.append(f"form={info['form_adj']:+.1f}")
    if info.get('rest_adj', 0) != 0:
        lbls = []
        if info.get('h_rest_lbl'): lbls.append(f"h:{info['h_rest_lbl']}")
        if info.get('a_rest_lbl'): lbls.append(f"a:{info['a_rest_lbl']}")
        parts.append(f"rest={info['rest_adj']:+.1f} ({','.join(lbls)})")
    if info.get('mot_adj', 0) != 0:
        lbls = []
        if info.get('h_mot_lbl'): lbls.append(f"h:{info['h_mot_lbl']}")
        if info.get('a_mot_lbl'): lbls.append(f"a:{info['a_mot_lbl']}")
        parts.append(f"mot={info['mot_adj']:+.1f} ({','.join(lbls)})")
    if info.get('playoff'):
        pf_parts = []
        if info.get('hca_adj', 0) != 0: pf_parts.append(f"hca={info['hca_adj']:+.1f}")
        if info.get('injury_amp_adj', 0) != 0: pf_parts.append(f"inj_amp={info['injury_amp_adj']:+.1f}")
        if info.get('momentum_adj', 0) != 0:
            pf_parts.append(f"momentum={info['momentum_adj']:+.1f} (last={info.get('last_margin','?'):+d})")
        if pf_parts: parts.append(f"playoff[{','.join(pf_parts)}]")
    if parts:
        return f"CONTEXT: raw={info['raw_ms']:+.1f} → adj={info['adjusted_ms']:+.1f} | " + " | ".join(parts)
    return f"CONTEXT: raw={info['raw_ms']:+.1f} (no adjustments)"


# ═══════════════════════════════════════════════════════════════════
# v25.46 — CONTEXT TOTAL (Path 2 for totals)
# ═══════════════════════════════════════════════════════════════════
# Anchored on market_total (no Elo-totals baseline exists in market_consensus).
# Adds walk-forward-safe signals: team scoring form, H2H recent totals,
# and MLB pitcher matchup. Path 2 fires OVER/UNDER own-picks when the
# context-adjusted total disagrees with market by a sport-specific run/point
# threshold. 30-day Phase A backtest:
#   NBA: 173 picks, 101-71, 58.7% WR, +97.4u @ threshold 0.30 pts
#   MLB:  68 picks,  37-28, 56.9% WR, +20.9u @ threshold 1.50 runs
# Other sports (NHL, NCAAB, soccer) too thin or losing on this signal set;
# they need goalie-form / weather / additional signals in a follow-up.

_LEAGUE_TOTAL = {
    'icehockey_nhl': 6.2, 'basketball_nba': 228.0, 'basketball_ncaab': 150.0,
    'baseball_mlb': 8.8, 'baseball_ncaa': 11.5,
}

_TOTAL_CAP = {
    'icehockey_nhl': 1.0, 'basketball_nba': 15.0, 'basketball_ncaab': 10.0,
    'baseball_mlb': 3.5, 'baseball_ncaa': 2.5,
}


def _team_form_total_delta(conn, sport, team, before_date, last_n=10):
    """Team's avg full-game total in last N games minus league avg. Walk-forward safe."""
    rows = conn.execute("""
        SELECT home_score, away_score FROM results
        WHERE sport=? AND (home=? OR away=?) AND home_score IS NOT NULL
          AND DATE(commence_time) < ?
        ORDER BY commence_time DESC LIMIT ?
    """, (sport, team, team, before_date, last_n)).fetchall()
    if len(rows) < 3:
        return 0.0, len(rows)
    totals = [hs + as_ for hs, as_ in rows]
    la = _LEAGUE_TOTAL.get(sport, 0.0)
    return sum(totals) / len(totals) - la, len(totals)


def _h2h_total_delta(conn, sport, home, away, before_date):
    """Recent H2H meetings' avg total minus league avg."""
    rows = conn.execute("""
        SELECT home_score, away_score FROM results
        WHERE sport=? AND home_score IS NOT NULL
          AND ((home=? AND away=?) OR (home=? AND away=?))
          AND DATE(commence_time) < ?
          AND DATE(commence_time) >= DATE(?, '-300 days')
        ORDER BY commence_time DESC LIMIT 5
    """, (sport, home, away, away, home, before_date, before_date)).fetchall()
    if len(rows) < 2:
        return 0.0, len(rows)
    totals = [hs + as_ for hs, as_ in rows]
    la = _LEAGUE_TOTAL.get(sport, 0.0)
    return sum(totals) / len(totals) - la, len(totals)


def _nhl_goalie_form_delta(conn, home, away, before_date):
    """NHL starter goalie save% (last 5 starts) vs league avg 0.900.
    Each 0.010 above league avg = ~0.6 goals suppressed per matchup.
    """
    def _team_signal(team):
        r = conn.execute("""
            SELECT goalie_name FROM nhl_goalie_stats
            WHERE team=? AND is_starter=1 AND game_date<?
            ORDER BY game_date DESC LIMIT 1
        """, (team, before_date)).fetchone()
        if not r: return None, None
        g = r[0]
        rows = conn.execute("""
            SELECT save_pct FROM nhl_goalie_stats
            WHERE goalie_name=? AND team=? AND is_starter=1 AND game_date<?
              AND save_pct IS NOT NULL AND shots_against >= 15
            ORDER BY game_date DESC LIMIT 5
        """, (g, team, before_date)).fetchall()
        if len(rows) < 3: return g, None
        return g, sum(r[0] for r in rows) / len(rows)

    h_g, h_sp = _team_signal(home)
    a_g, a_sp = _team_signal(away)
    if h_sp is None and a_sp is None:
        return 0.0, {}
    LEAGUE_SP = 0.900
    h_sp = h_sp if h_sp is not None else LEAGUE_SP
    a_sp = a_sp if a_sp is not None else LEAGUE_SP
    avg_sp = (h_sp + a_sp) / 2
    delta = -(avg_sp - LEAGUE_SP) * 60  # see comment above
    delta = max(-1.5, min(1.5, delta))
    return delta, {'h_goalie': h_g, 'a_goalie': a_g, 'avg_sp': round(avg_sp, 3), 'delta': round(delta, 2)}


def _soccer_standings_delta(conn, sport, home, away, before_date):
    """Soccer: team scoring+conceding rates per game (from soccer_standings)
    vs league avg total. Positive = both teams score/concede a lot → push UP.
    """
    def _team_row(t):
        return conn.execute("""
            SELECT goals_for, goals_against, games_played FROM soccer_standings
            WHERE sport=? AND team=? ORDER BY updated_at DESC LIMIT 1
        """, (sport, t)).fetchone()
    h = _team_row(home); a = _team_row(away)
    if not h or not a: return 0.0, {}
    if not h[2] or not a[2] or h[2] < 5 or a[2] < 5: return 0.0, {'reason': 'small_sample'}
    h_rate = (h[0] + h[1]) / h[2]   # goals per game (scored + conceded) in home's games
    a_rate = (a[0] + a[1]) / a[2]
    expected = (h_rate + a_rate) / 2
    la = _LEAGUE_TOTAL.get(sport, 2.6)
    delta = (expected - la) * 0.4
    delta = max(-1.0, min(1.0, delta))
    return delta, {'h_rate': round(h_rate, 2), 'a_rate': round(a_rate, 2), 'delta': round(delta, 2)}


def _ref_total_delta(conn, sport, event_id, before_date):
    """Referee tendency — avg game total in this ref's past games vs league avg."""
    ref_row = conn.execute("""
        SELECT official_name FROM officials
        WHERE event_id=? AND sport=? AND role IN ('referee','Referee','umpire','Umpire') LIMIT 1
    """, (event_id, sport)).fetchone()
    if not ref_row: return 0.0, {}
    ref = ref_row[0]
    rows = conn.execute("""
        SELECT actual_total FROM officials
        WHERE official_name=? AND sport=? AND actual_total IS NOT NULL
          AND game_date < ?
    """, (ref, sport, before_date)).fetchall()
    if len(rows) < 5: return 0.0, {'ref': ref, 'n': len(rows)}
    avg = sum(r[0] for r in rows) / len(rows)
    la = _LEAGUE_TOTAL.get(sport, 0.0)
    delta = (avg - la) * 0.3
    cap = {'basketball_nba': 3.0, 'icehockey_nhl': 0.5, 'basketball_ncaab': 2.0}.get(sport, 0.5)
    delta = max(-cap, min(cap, delta))
    return delta, {'ref': ref, 'ref_avg': round(avg, 1), 'n': len(rows), 'delta': round(delta, 2)}


def _mlb_pitcher_matchup_delta(conn, home, away, before_date):
    """Combined starter ERA vs league avg (4.0). 0.7 runs per ERA point. Cap ±2.5."""
    row = conn.execute("""
        SELECT home_pitcher_season_era, away_pitcher_season_era,
               home_pitcher_season_ip, away_pitcher_season_ip
        FROM mlb_probable_pitchers
        WHERE game_date = ? AND home = ? AND away = ?
        ORDER BY fetched_at DESC LIMIT 1
    """, (before_date, home, away)).fetchone()
    if not row:
        return 0.0, {}
    h_era, a_era, h_ip, a_ip = row
    if h_era is None or a_era is None:
        return 0.0, {}
    if (h_ip or 0) < 10 or (a_ip or 0) < 10:
        return 0.0, {'reason': 'low_ip'}
    avg_era = (h_era + a_era) / 2
    delta = max(-2.5, min(2.5, (avg_era - 4.0) * 0.7))
    return delta, {'h_era': h_era, 'a_era': a_era, 'avg_era': round(avg_era, 2)}


def compute_context_total(conn, sport, home, away, event_id, market_total, commence_date):
    """Context total anchored on market_total with walk-forward adjustments.

    Returns (context_total, info_dict). Context disagrees with market when
    |context_total - market_total| > sport-specific threshold.
    """
    fh, fh_n = _team_form_total_delta(conn, sport, home, commence_date)
    fa, fa_n = _team_form_total_delta(conn, sport, away, commence_date)
    form_signal = (fh + fa) / 2  # average of two independent estimates
    form_adj = form_signal * 0.3

    h2h, h2h_n = _h2h_total_delta(conn, sport, home, away, commence_date)
    h2h_adj = h2h * 0.2

    pitcher_adj = 0.0
    pitcher_info = {}
    if sport == 'baseball_mlb':
        pitcher_adj, pitcher_info = _mlb_pitcher_matchup_delta(conn, home, away, commence_date)

    # v25.47: goalie + soccer standings + ref tendency
    goalie_adj = 0.0
    goalie_info = {}
    if sport == 'icehockey_nhl':
        goalie_adj, goalie_info = _nhl_goalie_form_delta(conn, home, away, commence_date)
    standings_adj = 0.0
    standings_info = {}
    if 'soccer' in sport:
        standings_adj, standings_info = _soccer_standings_delta(conn, sport, home, away, commence_date)
    ref_adj = 0.0
    ref_info = {}
    if sport in ('basketball_nba', 'icehockey_nhl', 'basketball_ncaab'):
        ref_adj, ref_info = _ref_total_delta(conn, sport, event_id, commence_date)

    total_adj = form_adj + h2h_adj + pitcher_adj + goalie_adj + standings_adj + ref_adj
    cap = _TOTAL_CAP.get(sport, 1.0)
    total_adj = max(-cap, min(cap, total_adj))

    info = {
        'form_h': round(fh, 2), 'form_a': round(fa, 2),
        'form_n': fh_n + fa_n, 'form_adj': round(form_adj, 2),
        'h2h': round(h2h, 2), 'h2h_n': h2h_n, 'h2h_adj': round(h2h_adj, 2),
        'pitcher_adj': round(pitcher_adj, 2), 'pitcher_info': pitcher_info,
        'goalie_adj': round(goalie_adj, 2), 'goalie_info': goalie_info,
        'standings_adj': round(standings_adj, 2), 'standings_info': standings_info,
        'ref_adj': round(ref_adj, 2), 'ref_info': ref_info,
        'total_adj': round(total_adj, 2),
        'market_total': market_total,
        'context_total': round(market_total + total_adj, 2),
    }
    return market_total + total_adj, info


def format_context_total_summary(info):
    parts = []
    if info.get('form_adj', 0) != 0:
        parts.append(f"form={info['form_adj']:+.1f} (h:{info['form_h']:+.1f}, a:{info['form_a']:+.1f})")
    if info.get('h2h_adj', 0) != 0:
        parts.append(f"h2h={info['h2h_adj']:+.1f} (n={info['h2h_n']})")
    if info.get('pitcher_adj', 0) != 0:
        avg = info.get('pitcher_info', {}).get('avg_era', '?')
        parts.append(f"pitcher={info['pitcher_adj']:+.1f} (avg_era={avg})")
    if info.get('goalie_adj', 0) != 0:
        gi = info.get('goalie_info', {})
        parts.append(f"goalie={info['goalie_adj']:+.1f} (avg_sp={gi.get('avg_sp','?')})")
    if info.get('standings_adj', 0) != 0:
        si = info.get('standings_info', {})
        parts.append(f"standings={info['standings_adj']:+.1f} (h:{si.get('h_rate','?')}, a:{si.get('a_rate','?')})")
    if info.get('ref_adj', 0) != 0:
        ri = info.get('ref_info', {})
        parts.append(f"ref={info['ref_adj']:+.1f} ({ri.get('ref','?')}, avg={ri.get('ref_avg','?')})")
    if parts:
        return f"CONTEXT_TOTAL: mkt={info['market_total']} → ctx={info['context_total']} | " + " | ".join(parts)
    return f"CONTEXT_TOTAL: mkt={info['market_total']} (no adj)"
