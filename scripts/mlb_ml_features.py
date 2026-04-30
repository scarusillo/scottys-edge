"""v26.1 MLB Context ML — feature extraction.

Walk-forward feature builder. At fire time (or for a historical game),
takes (home, away, game_date) and returns a dict of features used by
the logistic regression model. All features computed from data
strictly BEFORE `game_date` to keep training honest.

Feature list (10):
  1. starter_era_diff       — (away_starter_ERA - home_starter_ERA), 30+IP filter
  2. starter_k9_diff        — (home_K9 - away_K9), 30+IP filter
  3. bullpen_era_diff       — (away_bullpen_ERA - home_bullpen_ERA), 30+IP rolling
  4. recent_rd_diff         — home_L10_run_diff - away_L10_run_diff
  5. batting_form_diff      — home_runs_pg_L14 - away_runs_pg_L14
  6. park_factor            — runs adj from existing PARK_GATE logic
  7. home_advantage         — constant +0.4 runs
  8. injury_impact_diff     — home_OUT_bat_pts - away_OUT_bat_pts
  9. rest_days_home         — days since home team's last game
 10. rest_days_away         — days since away team's last game

NULLs: returned as None per feature. Caller decides how to impute
(typically league-mean = 0 for diff features, computed from training).
"""
import sqlite3
import os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

LEAGUE_AVG_ERA = 4.00
LEAGUE_AVG_K9 = 8.50
HOME_ADVANTAGE_RUNS = 0.4
STARTER_MIN_IP = 30
BULLPEN_MIN_IP = 30
FORM_WINDOW_GAMES = 10
BATTING_WINDOW_DAYS = 14


# ──────────────────────────────────────────────────────────────────────────
# Pitcher features (1-2)
# ──────────────────────────────────────────────────────────────────────────

def _get_starter_features(conn, home, away, game_date):
    """Pull season ERA + K9 + IP for tonight's probable starters from
    mlb_probable_pitchers. Returns (h_era, a_era, h_k9, a_k9) or Nones if not
    found / IP below threshold.
    """
    row = conn.execute("""
        SELECT home_pitcher_season_era, away_pitcher_season_era,
               home_pitcher_season_k9, away_pitcher_season_k9,
               home_pitcher_season_ip, away_pitcher_season_ip
        FROM mlb_probable_pitchers
        WHERE game_date = ? AND home = ? AND away = ?
        ORDER BY fetched_at DESC LIMIT 1
    """, (game_date, home, away)).fetchone()
    if not row:
        return None, None, None, None
    h_era, a_era, h_k9, a_k9, h_ip, a_ip = row
    h_ip = h_ip or 0
    a_ip = a_ip or 0
    if h_ip < STARTER_MIN_IP:
        h_era = h_k9 = None
    if a_ip < STARTER_MIN_IP:
        a_era = a_k9 = None
    return h_era, a_era, h_k9, a_k9


def _starter_era_diff(h_era, a_era):
    """Higher home ERA = more home runs allowed = lower home win prob.
    Returned as (away - home) so positive = home is better pitcher.
    """
    if h_era is None or a_era is None:
        return None
    return round(a_era - h_era, 3)


def _starter_k9_diff(h_k9, a_k9):
    """Higher home K9 = better strikeout pitcher = lower home runs allowed.
    Returned as (home - away) so positive = home is better strikeout pitcher.
    """
    if h_k9 is None or a_k9 is None:
        return None
    return round(h_k9 - a_k9, 3)


# ──────────────────────────────────────────────────────────────────────────
# Bullpen feature (3)
# ──────────────────────────────────────────────────────────────────────────

def _team_bullpen_era(conn, team, before_date, lookback_days=30):
    """Aggregate bullpen ERA from pitcher_stats (is_starter=0) over a rolling
    window before `before_date`. Returns ERA float or None if IP < threshold.
    """
    row = conn.execute("""
        SELECT
          ROUND(SUM(earned_runs) * 9.0 / NULLIF(SUM(innings_pitched), 0), 2) era,
          ROUND(SUM(innings_pitched), 1) ip
        FROM pitcher_stats
        WHERE team = ? AND is_starter = 0
          AND DATE(game_date) < ?
          AND DATE(game_date) >= DATE(?, ?)
          AND innings_pitched IS NOT NULL AND innings_pitched > 0
    """, (team, before_date, before_date, f'-{lookback_days} day')).fetchone()
    if not row or row[0] is None:
        return None
    era, ip = row
    if (ip or 0) < BULLPEN_MIN_IP:
        return None
    return era


def _bullpen_era_diff(conn, home, away, game_date):
    """Higher home bullpen ERA = more home runs allowed = lower home win prob.
    Returned as (away - home) so positive = home has better bullpen.
    """
    h_pen = _team_bullpen_era(conn, home, game_date)
    a_pen = _team_bullpen_era(conn, away, game_date)
    if h_pen is None or a_pen is None:
        return None
    return round(a_pen - h_pen, 3)


# ──────────────────────────────────────────────────────────────────────────
# Recent run differential (4)
# ──────────────────────────────────────────────────────────────────────────

def _team_l10_rd(conn, team, before_date):
    """Avg run differential over last N games before `before_date`."""
    rows = conn.execute("""
        SELECT (CASE WHEN home = ? THEN home_score - away_score
                     ELSE away_score - home_score END) AS rd
        FROM results
        WHERE sport = 'baseball_mlb'
          AND (home = ? OR away = ?)
          AND home_score IS NOT NULL
          AND DATE(commence_time) < ?
        ORDER BY commence_time DESC LIMIT ?
    """, (team, team, team, before_date, FORM_WINDOW_GAMES)).fetchall()
    if not rows or len(rows) < 3:
        return None
    return round(sum(r[0] for r in rows) / len(rows), 2)


def _recent_rd_diff(conn, home, away, game_date):
    h_rd = _team_l10_rd(conn, home, game_date)
    a_rd = _team_l10_rd(conn, away, game_date)
    if h_rd is None or a_rd is None:
        return None
    return round(h_rd - a_rd, 2)


# ──────────────────────────────────────────────────────────────────────────
# Batting recent (5)
# ──────────────────────────────────────────────────────────────────────────

def _team_runs_per_game(conn, team, before_date, window_days=BATTING_WINDOW_DAYS):
    """Runs scored per game from box_scores over rolling window."""
    row = conn.execute("""
        SELECT COUNT(DISTINCT game_date) games,
               ROUND(SUM(stat_value) * 1.0 / NULLIF(COUNT(DISTINCT game_date), 0), 2) rpg
        FROM box_scores
        WHERE sport = 'baseball_mlb'
          AND team = ? AND stat_type = 'runs'
          AND DATE(game_date) < ?
          AND DATE(game_date) >= DATE(?, ?)
    """, (team, before_date, before_date, f'-{window_days} day')).fetchone()
    if not row or row[0] is None or row[0] < 3:
        return None
    return row[1]


def _batting_form_diff(conn, home, away, game_date):
    h_rpg = _team_runs_per_game(conn, home, game_date)
    a_rpg = _team_runs_per_game(conn, away, game_date)
    if h_rpg is None or a_rpg is None:
        return None
    return round(h_rpg - a_rpg, 2)


# ──────────────────────────────────────────────────────────────────────────
# Park factor (6) — reuse existing logic
# ──────────────────────────────────────────────────────────────────────────

def _park_factor_runs(home):
    """Reuse the same constants as PARK_GATE / _mlb_park_factor_adjustment.
    Positive = run inflator (Coors), negative = suppressor (Petco).
    """
    PARK = {
        'Colorado Rockies': +0.6,
        'Cincinnati Reds': +0.3,
        'Boston Red Sox': +0.2,
        'New York Yankees': +0.2,
        'Chicago Cubs': +0.15,
        'Arizona Diamondbacks': +0.1,
        'Texas Rangers': +0.1,
        'Toronto Blue Jays': +0.1,
        'San Diego Padres': -0.2,
        'Seattle Mariners': -0.15,
        'Oakland Athletics': -0.1,
        'Athletics': -0.1,
        'Detroit Tigers': -0.1,
        'Miami Marlins': -0.1,
    }
    return PARK.get(home, 0.0)


# ──────────────────────────────────────────────────────────────────────────
# Injuries (8)
# ──────────────────────────────────────────────────────────────────────────

def _team_injury_impact(conn, team, game_date):
    """Sum of point_impact for OUT bats on this team. Positive number = larger
    negative impact on the team. We RETURN the magnitude; caller subtracts.
    """
    row = conn.execute("""
        SELECT COALESCE(SUM(point_impact), 0) total
        FROM injuries
        WHERE sport = 'baseball_mlb'
          AND team = ? AND status = 'OUT'
          AND DATE(report_date) <= ?
          AND DATE(report_date) >= DATE(?, '-7 day')
          AND position NOT IN ('P', 'SP', 'RP', 'CP')
    """, (team, game_date, game_date)).fetchone()
    return float(row[0] or 0)


def _injury_impact_diff(conn, home, away, game_date):
    """Returned as (home_injury - away_injury). NEGATIVE when home is more
    injured (home loses more from injuries) → expect lower home win prob.
    """
    h = _team_injury_impact(conn, home, game_date)
    a = _team_injury_impact(conn, away, game_date)
    return round(a - h, 2)


# ──────────────────────────────────────────────────────────────────────────
# Rest days (9-10)
# ──────────────────────────────────────────────────────────────────────────

def _team_rest_days(conn, team, game_date):
    """Days since this team's last game. Returns None if no prior game."""
    row = conn.execute("""
        SELECT MAX(commence_time) FROM results
        WHERE sport = 'baseball_mlb'
          AND (home = ? OR away = ?)
          AND home_score IS NOT NULL
          AND DATE(commence_time) < ?
    """, (team, team, game_date)).fetchone()
    if not row or not row[0]:
        return None
    last_dt = row[0][:10]
    try:
        d1 = datetime.strptime(last_dt, '%Y-%m-%d')
        d2 = datetime.strptime(game_date, '%Y-%m-%d')
        delta = (d2 - d1).days
        return min(delta, 7)  # cap at 7 days for stability
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────────

FEATURE_NAMES = [
    'starter_era_diff', 'starter_k9_diff', 'bullpen_era_diff',
    'recent_rd_diff', 'batting_form_diff', 'park_factor',
    'home_advantage', 'injury_impact_diff', 'rest_days_home',
    'rest_days_away',
]

# Sensible defaults for NULL handling (used at training imputation if a team
# has missing data — the diff features default to 0 since league-average
# minus league-average = 0 in expectation).
DEFAULT_FILL = {
    'starter_era_diff': 0.0,
    'starter_k9_diff': 0.0,
    'bullpen_era_diff': 0.0,
    'recent_rd_diff': 0.0,
    'batting_form_diff': 0.0,
    'park_factor': 0.0,
    'home_advantage': HOME_ADVANTAGE_RUNS,
    'injury_impact_diff': 0.0,
    'rest_days_home': 1.0,
    'rest_days_away': 1.0,
}


def build_features(conn, home, away, game_date):
    """Return a dict {feature_name: value} for the (home, away) matchup on
    `game_date`. All values computed strictly from data BEFORE game_date.

    NULL values are preserved (caller imputes — see DEFAULT_FILL).
    """
    h_era, a_era, h_k9, a_k9 = _get_starter_features(conn, home, away, game_date)
    return {
        'starter_era_diff': _starter_era_diff(h_era, a_era),
        'starter_k9_diff': _starter_k9_diff(h_k9, a_k9),
        'bullpen_era_diff': _bullpen_era_diff(conn, home, away, game_date),
        'recent_rd_diff': _recent_rd_diff(conn, home, away, game_date),
        'batting_form_diff': _batting_form_diff(conn, home, away, game_date),
        'park_factor': _park_factor_runs(home),
        'home_advantage': HOME_ADVANTAGE_RUNS,
        'injury_impact_diff': _injury_impact_diff(conn, home, away, game_date),
        'rest_days_home': _team_rest_days(conn, home, game_date),
        'rest_days_away': _team_rest_days(conn, away, game_date),
    }


def features_to_vector(features, fill_missing=True):
    """Convert a feature dict to a list in FEATURE_NAMES order.
    If fill_missing, replace None with DEFAULT_FILL value.
    """
    out = []
    for name in FEATURE_NAMES:
        v = features.get(name)
        if v is None and fill_missing:
            v = DEFAULT_FILL[name]
        out.append(v if v is not None else 0.0)
    return out


if __name__ == '__main__':
    # Smoke test on tonight's slate
    conn = sqlite3.connect(DB_PATH)
    print('=== Feature extraction smoke test on tonight\'s MLB slate ===\n')
    rows = conn.execute("""
        SELECT DISTINCT home, away, DATE(commence_time) AS gd
        FROM results
        WHERE sport='baseball_mlb' AND DATE(commence_time)='2026-04-29'
        ORDER BY home
    """).fetchall()
    if not rows:
        rows = conn.execute("""
            SELECT DISTINCT home, away, '2026-04-29' AS gd
            FROM market_consensus
            WHERE sport='baseball_mlb' AND DATE(commence_time)='2026-04-29'
            ORDER BY home
        """).fetchall()
    for home, away, gd in rows[:6]:
        feat = build_features(conn, home, away, gd)
        print(f'{away} @ {home} ({gd}):')
        for k, v in feat.items():
            print(f'  {k:22}: {v}')
        print()
