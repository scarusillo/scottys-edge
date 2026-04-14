"""
starter_adjust.py — Adjust team defensive ratings based on confirmed starter

The team_ratings_engine gives each team an off_rating and def_rating based
on their overall historical scoring. But the def_rating is pitcher-agnostic
(MLB) or goalie-agnostic (NHL) — it doesn't know if their ace or their #5
is starting.

This module computes a per-game adjustment to def_rating based on the
confirmed starter's recent form vs league average.

Usage:
    adj_def = adjust_def_rating_for_starter(conn, sport, team, base_def_rating, event_id)
"""
import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# League average ERAs (approximate, used as baseline for adjustment)
MLB_LEAGUE_ERA = 4.20
NHL_LEAGUE_GAA = 3.00

# Weight of starter adjustment (0-1). 0.5 = starter explains half of scoring
# variance on defense side. Tuned based on conventional wisdom: pitcher
# is ~50% of MLB total, goalie ~30% of NHL total.
MLB_STARTER_WEIGHT = 0.50
NHL_GOALIE_WEIGHT = 0.30

# Min IP / games to trust the stats
MIN_PITCHER_IP = 15
MIN_GOALIE_GAMES = 5


def _get_pitcher_era(conn, pitcher_name, game_date=None):
    """Get pitcher's recent ERA from pitcher_stats. Returns (era, reliability)."""
    if not pitcher_name:
        return None, 0.0

    # Get last 5 starts worth of stats
    query = """
        SELECT earned_runs, innings_pitched
        FROM pitcher_stats
        WHERE pitcher_name = ? AND is_starter = 1
        AND innings_pitched IS NOT NULL AND innings_pitched > 0
    """
    params = [pitcher_name]
    if game_date:
        query += " AND game_date < ?"
        params.append(game_date)
    query += " ORDER BY game_date DESC LIMIT 8"

    rows = conn.execute(query, params).fetchall()
    if not rows:
        return None, 0.0

    total_er = sum(r[0] or 0 for r in rows)
    total_ip = sum(r[1] or 0 for r in rows)

    if total_ip < MIN_PITCHER_IP:
        return None, 0.0

    era = (total_er * 9.0) / total_ip
    # Reliability: 1.0 at 30+ IP, scales down below
    reliability = min(1.0, total_ip / 30.0)
    return era, reliability


def _get_goalie_gaa(conn, goalie_name, game_date=None):
    """Get goalie's recent GAA from nhl_goalie_stats."""
    if not goalie_name:
        return None, 0.0

    query = """
        SELECT goals_against, time_on_ice
        FROM nhl_goalie_stats
        WHERE goalie_name = ? AND is_starter = 1
        AND goals_against IS NOT NULL AND time_on_ice IS NOT NULL
    """
    params = [goalie_name]
    if game_date:
        query += " AND game_date < ?"
        params.append(game_date)
    query += " ORDER BY game_date DESC LIMIT 10"

    rows = conn.execute(query, params).fetchall()
    if not rows:
        return None, 0.0

    if len(rows) < MIN_GOALIE_GAMES:
        return None, 0.0

    # time_on_ice is seconds or MM:SS format; assume seconds for simplicity
    total_ga = sum(r[0] or 0 for r in rows)
    # Estimate games played (each start ~60 minutes full game)
    gp = len(rows)
    gaa = total_ga / gp  # goals per game
    reliability = min(1.0, gp / 10.0)
    return gaa, reliability


def get_starter_adjustment(conn, sport, home_team, away_team, event_id=None, game_date=None):
    """
    Returns (home_def_mult, away_def_mult, info_dict) where the multipliers
    adjust the base def_rating to account for starter quality.

    A multiplier of 0.85 means this starter is 15% better than avg (allows fewer runs/goals).
    A multiplier of 1.20 means 20% worse than avg.
    """
    info = {'home_starter': None, 'away_starter': None,
            'home_era': None, 'away_era': None,
            'home_mult': 1.0, 'away_mult': 1.0}

    if sport == 'baseball_mlb':
        # Look up probable pitchers
        row = None
        if event_id:
            row = conn.execute("""
                SELECT home_pitcher, away_pitcher, home_pitcher_era, away_pitcher_era,
                       home_pitcher_season_era, away_pitcher_season_era
                FROM mlb_probable_pitchers
                WHERE espn_event_id = ? OR (home = ? AND away = ?)
                ORDER BY fetched_at DESC LIMIT 1
            """, (str(event_id), home_team, away_team)).fetchone()
        if not row:
            row = conn.execute("""
                SELECT home_pitcher, away_pitcher, home_pitcher_era, away_pitcher_era,
                       home_pitcher_season_era, away_pitcher_season_era
                FROM mlb_probable_pitchers
                WHERE home = ? AND away = ?
                ORDER BY fetched_at DESC LIMIT 1
            """, (home_team, away_team)).fetchone()

        if not row:
            return 1.0, 1.0, info

        h_p, a_p, h_era, a_era, h_season, a_season = row
        info['home_starter'] = h_p
        info['away_starter'] = a_p

        # Prefer recent-form ERA from pitcher_stats, fall back to provided
        h_recent, h_rel = _get_pitcher_era(conn, h_p, game_date)
        a_recent, a_rel = _get_pitcher_era(conn, a_p, game_date)

        # Use recent if reliable, else season
        h_eff = h_recent if h_recent and h_rel >= 0.5 else (h_season or h_era)
        a_eff = a_recent if a_recent and a_rel >= 0.5 else (a_season or a_era)

        info['home_era'] = h_eff
        info['away_era'] = a_eff

        # Compute multipliers: ratio of pitcher ERA to league average
        # If pitcher ERA is 3.00 and league is 4.20, ratio = 0.71 → pitcher allows 29% fewer runs
        # Blend with weight: mult = 1.0 + weight * (ratio - 1.0)
        if h_eff and h_eff > 0:
            h_ratio = h_eff / MLB_LEAGUE_ERA
            # Cap at ±40% to avoid extreme adjustments
            h_ratio = max(0.5, min(1.5, h_ratio))
            info['home_mult'] = 1.0 + MLB_STARTER_WEIGHT * (h_ratio - 1.0)

        if a_eff and a_eff > 0:
            a_ratio = a_eff / MLB_LEAGUE_ERA
            a_ratio = max(0.5, min(1.5, a_ratio))
            info['away_mult'] = 1.0 + MLB_STARTER_WEIGHT * (a_ratio - 1.0)

        return info['home_mult'], info['away_mult'], info

    elif sport == 'icehockey_nhl':
        row = conn.execute("""
            SELECT home_goalie, away_goalie
            FROM nhl_probable_goalies
            WHERE (espn_event_id = ? OR (home = ? AND away = ?))
            ORDER BY fetched_at DESC LIMIT 1
        """, (str(event_id or ''), home_team, away_team)).fetchone()

        if not row:
            return 1.0, 1.0, info

        h_g, a_g = row
        info['home_starter'] = h_g
        info['away_starter'] = a_g

        h_gaa, h_rel = _get_goalie_gaa(conn, h_g, game_date)
        a_gaa, a_rel = _get_goalie_gaa(conn, a_g, game_date)

        if h_gaa and h_rel >= 0.5:
            h_ratio = h_gaa / NHL_LEAGUE_GAA
            h_ratio = max(0.6, min(1.4, h_ratio))
            info['home_mult'] = 1.0 + NHL_GOALIE_WEIGHT * (h_ratio - 1.0)
            info['home_era'] = h_gaa

        if a_gaa and a_rel >= 0.5:
            a_ratio = a_gaa / NHL_LEAGUE_GAA
            a_ratio = max(0.6, min(1.4, a_ratio))
            info['away_mult'] = 1.0 + NHL_GOALIE_WEIGHT * (a_ratio - 1.0)
            info['away_era'] = a_gaa

        return info['home_mult'], info['away_mult'], info

    return 1.0, 1.0, info


if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)

    # Sample MLB game
    h, a, info = get_starter_adjustment(conn, 'baseball_mlb',
        'Baltimore Orioles', 'Texas Rangers')
    print(f"MLB: BAL vs TEX")
    print(f"  Home starter: {info['home_starter']} (ERA {info['home_era']})")
    print(f"  Away starter: {info['away_starter']} (ERA {info['away_era']})")
    print(f"  Multipliers: home_def x {h:.3f}, away_def x {a:.3f}")

    h, a, info = get_starter_adjustment(conn, 'icehockey_nhl',
        'Boston Bruins', 'Dallas Stars')
    print(f"\nNHL: BOS vs DAL")
    print(f"  Home goalie: {info['home_starter']}")
    print(f"  Away goalie: {info['away_starter']}")
    print(f"  Multipliers: home x {h:.3f}, away x {a:.3f}")

    conn.close()
