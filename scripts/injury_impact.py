"""
injury_impact.py — Player Impact on Team Performance

v25.18: Quantifies how much a player's absence affects their team's
scoring (spread) and total. Instead of guessing "Tatum is worth X points,"
we measure actual team performance WITH vs WITHOUT each player.

Flow:
  1. For a given team + sport, find all completed games in results
  2. Cross-reference box_scores to determine which games each player appeared in
  3. Compute team offensive/defensive averages with player IN vs OUT
  4. The delta is the player's measured impact

Usage in model:
  - When a key player is OUT (from injuries table), adjust the model's
    spread and total projections by their measured impact
  - Only apply when sample size is meaningful (5+ games in each bucket)

Data sources:
  - results: game scores (home_score, away_score, actual_total)
  - box_scores: player appearances per game (if player has a row, they played)
  - injuries: current injury status
"""

import sqlite3, os
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Minimum games in each bucket (with/without) to trust the impact
MIN_GAMES_WITH = 10
MIN_GAMES_WITHOUT = 3  # Players don't miss many games, so lower threshold

# Cache impacts for the day (recalculated each grade cycle)
_impact_cache = {}
_cache_date = None


def _get_team_games(conn, team, sport, lookback_days=120):
    """Get all completed games for a team with scores."""
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    rows = conn.execute("""
        SELECT commence_time, home, away, home_score, away_score, actual_total
        FROM results
        WHERE sport = ? AND completed = 1
        AND home_score IS NOT NULL AND away_score IS NOT NULL
        AND commence_time >= ?
        AND (home = ? OR away = ?)
        ORDER BY commence_time DESC
    """, (sport, cutoff, team, team)).fetchall()
    return rows


def _game_has_box_scores(conn, game_date, sport, team):
    """Check if we have ANY box scores for this team on this date.

    Critical: if we don't have box scores for the game at all, we can't
    determine whether a player was in or out. These games must be SKIPPED,
    not counted as 'without'. Otherwise every game we didn't fetch looks
    like the player was absent, massively inflating the 'without' bucket.
    """
    row = conn.execute("""
        SELECT 1 FROM box_scores
        WHERE game_date = ? AND sport = ? AND team = ?
        LIMIT 1
    """, (game_date, sport, team)).fetchone()
    return row is not None


def _player_played(conn, player, game_date, sport):
    """Check if player appeared in box_scores for a given game date."""
    row = conn.execute("""
        SELECT 1 FROM box_scores
        WHERE player = ? AND game_date = ? AND sport = ?
        LIMIT 1
    """, (player, game_date, sport)).fetchone()
    return row is not None


def compute_player_impact(conn, player, team, sport):
    """
    Compute a player's impact on their team's scoring.

    Returns dict with:
      - spread_impact: points the team scores more/less WITH this player
      - total_impact: how the game total changes WITH vs WITHOUT
      - games_with: number of games player appeared
      - games_without: number of games player was absent
      - confidence: 'HIGH' (5+ without), 'MEDIUM' (3-4), 'LOW' (< 3)
    Returns None if insufficient data.
    """
    games = _get_team_games(conn, team, sport)
    if not games:
        return None

    with_player = []  # (team_score, opp_score, total)
    without_player = []

    for g in games:
        game_date = g[0][:10] if g[0] else None
        if not game_date:
            continue

        is_home = (g[1] == team)
        team_score = g[3] if is_home else g[4]
        opp_score = g[4] if is_home else g[3]
        total = g[5] or (team_score + opp_score)

        # CRITICAL: Only count games where we have box scores for this team.
        # If we don't have box scores, we can't tell if the player was in or out.
        # Counting those as "without" would massively inflate the absence bucket.
        if not _game_has_box_scores(conn, game_date, sport, team):
            continue

        played = _player_played(conn, player, game_date, sport)
        if played:
            with_player.append((team_score, opp_score, total))
        else:
            without_player.append((team_score, opp_score, total))

    if len(with_player) < MIN_GAMES_WITH:
        return None  # Not enough data

    if len(without_player) < MIN_GAMES_WITHOUT:
        return None  # Player rarely misses — can't measure impact

    # Compute averages
    avg_team_with = sum(g[0] for g in with_player) / len(with_player)
    avg_team_without = sum(g[0] for g in without_player) / len(without_player)
    avg_opp_with = sum(g[1] for g in with_player) / len(with_player)
    avg_opp_without = sum(g[1] for g in without_player) / len(without_player)
    avg_total_with = sum(g[2] for g in with_player) / len(with_player)
    avg_total_without = sum(g[2] for g in without_player) / len(without_player)

    # Spread impact: how much better/worse the team performs with this player
    # Positive = team is better with this player (they should be)
    spread_impact = (avg_team_with - avg_opp_with) - (avg_team_without - avg_opp_without)

    # Total impact: how the game total changes
    total_impact = avg_total_with - avg_total_without

    # Confidence based on without-sample size
    if len(without_player) >= 5:
        confidence = 'HIGH'
    elif len(without_player) >= MIN_GAMES_WITHOUT:
        confidence = 'MEDIUM'
    else:
        confidence = 'LOW'

    return {
        'player': player,
        'team': team,
        'sport': sport,
        'spread_impact': round(spread_impact, 1),
        'total_impact': round(total_impact, 1),
        'team_pts_with': round(avg_team_with, 1),
        'team_pts_without': round(avg_team_without, 1),
        'opp_pts_with': round(avg_opp_with, 1),
        'opp_pts_without': round(avg_opp_without, 1),
        'total_with': round(avg_total_with, 1),
        'total_without': round(avg_total_without, 1),
        'games_with': len(with_player),
        'games_without': len(without_player),
        'confidence': confidence,
    }


def get_out_players(conn, team, sport):
    """Get currently OUT players for a team from injuries table.

    Deduplicates by player name — the injuries table may have multiple
    entries per player from daily ESPN scrapes.
    """
    rows = conn.execute("""
        SELECT player, status, injury_type FROM injuries
        WHERE team = ? AND sport = ? AND status = 'Out'
        GROUP BY player
    """, (team, sport)).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def get_team_injury_adjustment(conn, team, sport):
    """
    Compute total spread/total adjustment for a team based on current injuries.

    Sums the impact of all OUT players. Only includes players with measured impact.

    Returns:
      (spread_adj, total_adj, details_list)
      spread_adj: points to add to spread (positive = team weaker without these players)
      total_adj: points to add to total
      details_list: list of dicts with per-player breakdown
    """
    global _impact_cache, _cache_date
    today = datetime.now().strftime('%Y-%m-%d')
    if _cache_date != today:
        _impact_cache = {}
        _cache_date = today

    out_players = get_out_players(conn, team, sport)
    if not out_players:
        return 0.0, 0.0, []

    spread_adj = 0.0
    total_adj = 0.0
    details = []

    for player, status, injury in out_players:
        cache_key = f"{player}|{team}|{sport}"
        if cache_key in _impact_cache:
            impact = _impact_cache[cache_key]
        else:
            impact = compute_player_impact(conn, player, team, sport)
            _impact_cache[cache_key] = impact

        if impact is None:
            continue

        # When a player is OUT, the team loses their positive impact
        # spread_impact > 0 means team is better WITH player
        # So adjustment is negative (team gets worse)
        spread_adj -= impact['spread_impact']
        total_adj += impact['total_impact']  # Total change when player is out

        details.append({
            'player': player,
            'injury': injury,
            'spread_impact': impact['spread_impact'],
            'total_impact': impact['total_impact'],
            'games_without': impact['games_without'],
            'confidence': impact['confidence'],
        })

    # Cap the total adjustment — don't let injuries swing more than 5 points
    spread_adj = max(-5.0, min(5.0, spread_adj))
    total_adj = max(-5.0, min(5.0, total_adj))

    return round(spread_adj, 1), round(total_adj, 1), details


if __name__ == '__main__':
    """Test injury impact for key players."""
    conn = sqlite3.connect(DB_PATH)

    test_cases = [
        ('Jayson Tatum', 'Boston Celtics', 'basketball_nba'),
        ('Nikola Jokic', 'Denver Nuggets', 'basketball_nba'),
        ('Connor McDavid', 'Edmonton Oilers', 'icehockey_nhl'),
        ('Shohei Ohtani', 'Los Angeles Dodgers', 'baseball_mlb'),
    ]

    for player, team, sport in test_cases:
        impact = compute_player_impact(conn, player, team, sport)
        if impact:
            print(f'{player} ({team}):')
            print(f'  Games WITH: {impact["games_with"]} | WITHOUT: {impact["games_without"]}')
            print(f'  Team scores {impact["team_pts_with"]:.1f} with, {impact["team_pts_without"]:.1f} without')
            print(f'  Spread impact: {impact["spread_impact"]:+.1f} pts | Total impact: {impact["total_impact"]:+.1f} pts')
            print(f'  Confidence: {impact["confidence"]}')
        else:
            print(f'{player}: insufficient data')
        print()

    # Test full team injury adjustment
    print('=== TEAM INJURY ADJUSTMENTS ===')
    for team, sport in [('Boston Celtics', 'basketball_nba'),
                        ('New York Yankees', 'baseball_mlb'),
                        ('Edmonton Oilers', 'icehockey_nhl')]:
        spread, total, details = get_team_injury_adjustment(conn, team, sport)
        out = get_out_players(conn, team, sport)
        print(f'{team}: {len(out)} OUT players')
        if details:
            print(f'  Spread adj: {spread:+.1f} | Total adj: {total:+.1f}')
            for d in details:
                print(f'    {d["player"]}: spread {d["spread_impact"]:+.1f}, total {d["total_impact"]:+.1f} ({d["confidence"]}, {d["games_without"]}g without)')
        else:
            print(f'  No measurable impact from OUT players')
        print()

    conn.close()
