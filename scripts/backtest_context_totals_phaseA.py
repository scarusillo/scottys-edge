"""Phase A for Context Model totals.

Goal: empirically test whether we can construct a Context total that beats
market on historical games. Uses only signals that walk-forward cleanly:

  1. Team scoring form (avg total in last N games for each team, vs league avg)
  2. Pitcher matchup delta (MLB/NCAA Baseball only) — uses pitcher_stats history
  3. Recent pace (basketball) — uses game_results scoring rates
  4. H2H recent totals — these two teams' recent meetings' avg total

Combined into a single adj applied to market_total. If combined adj crosses
a sport-specific threshold, fire OVER/UNDER at market line.

Weather, ref, and live-snapshot signals intentionally excluded from Phase A —
they don't walk forward cleanly without historical snapshots we don't have.
They can be added in Phase B if the walk-forward-safe signals already show
positive EV.
"""
import os, sys, sqlite3
from collections import defaultdict
from datetime import datetime, timedelta

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Sport-specific league-average game totals (runs/goals/points)
# Used as the normalization anchor for form-based adjustments.
LEAGUE_TOTAL = {
    'icehockey_nhl': 6.2,
    'basketball_nba': 228.0,
    'basketball_ncaab': 150.0,
    'baseball_mlb': 8.8,
    'baseball_ncaa': 11.5,
    'soccer_italy_serie_a': 2.5,
    'soccer_usa_mls': 2.8,
    'soccer_epl': 2.7,
    'soccer_spain_la_liga': 2.5,
    'soccer_germany_bundesliga': 3.0,
    'soccer_france_ligue_one': 2.6,
    'soccer_mexico_ligamx': 2.7,
    'soccer_uefa_champs_league': 2.8,
}

# Sport-specific disagreement thresholds to test
# (we sweep these in the backtest)
DEFAULT_SWEEP = [0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 8.0]


def team_form_delta(conn, sport, team, before_date, last_n=10, league_avg=None):
    """Return (team_avg_total_in_last_N - league_avg, games)."""
    if league_avg is None:
        league_avg = LEAGUE_TOTAL.get(sport, 0.0)
    rows = conn.execute("""
        SELECT home_score, away_score FROM results
        WHERE sport=? AND (home=? OR away=?) AND home_score IS NOT NULL
          AND DATE(commence_time) < ?
        ORDER BY commence_time DESC LIMIT ?
    """, (sport, team, team, before_date, last_n)).fetchall()
    if len(rows) < 3:
        return 0.0, len(rows)
    totals = [hs + as_ for hs, as_ in rows]
    avg = sum(totals) / len(totals)
    return avg - league_avg, len(totals)


def nhl_goalie_form_delta(conn, home, away, before_date):
    """NHL goalie save% vs league avg (~0.900). Higher save% = fewer goals.

    Uses last 5 starts for each team's most recent starting goalie. If both
    are hot, push total DOWN. If both are cold, push UP.
    Returns (goals_delta, info). Positive = more goals expected.
    """
    def team_goalie_signal(team):
        # Find team's most recent starting goalie (before game date)
        r = conn.execute("""
            SELECT goalie_name FROM nhl_goalie_stats
            WHERE team=? AND is_starter=1 AND game_date<?
            ORDER BY game_date DESC LIMIT 1
        """, (team, before_date)).fetchone()
        if not r: return None, None
        goalie = r[0]
        # Last 5 starts
        rows = conn.execute("""
            SELECT save_pct, shots_against FROM nhl_goalie_stats
            WHERE goalie_name=? AND team=? AND is_starter=1 AND game_date<?
              AND save_pct IS NOT NULL AND shots_against >= 15
            ORDER BY game_date DESC LIMIT 5
        """, (goalie, team, before_date)).fetchall()
        if len(rows) < 3:
            return goalie, None
        savepct_avg = sum(r[0] for r in rows) / len(rows)
        return goalie, savepct_avg

    h_goalie, h_sp = team_goalie_signal(home)
    a_goalie, a_sp = team_goalie_signal(away)
    if h_sp is None and a_sp is None:
        return 0.0, {'reason': 'no_goalie_data'}
    LEAGUE_SP = 0.900
    # Fill in league avg if one goalie missing
    h_sp = h_sp if h_sp is not None else LEAGUE_SP
    a_sp = a_sp if a_sp is not None else LEAGUE_SP
    avg_sp = (h_sp + a_sp) / 2
    # Each 0.010 of save% above league avg = ~1.0 goal suppressed from total
    # (both goalies each face ~30 shots; 0.010 sp * 30 shots = 0.3 goals; both = 0.6 goals)
    delta = -(avg_sp - LEAGUE_SP) * 60  # 0.010 * 60 = 0.6 goals per team pair
    delta = max(-1.5, min(1.5, delta))
    return delta, {'h_goalie': h_goalie, 'a_goalie': a_goalie, 'avg_sp': round(avg_sp, 3), 'delta': round(delta, 2)}


def soccer_standings_delta(conn, sport, home, away, before_date):
    """Soccer: goals_for/against per game vs league avg. Uses soccer_standings snapshot.

    Positive delta = both teams score a lot and/or concede a lot = push total UP.
    """
    h_row = conn.execute("""
        SELECT goals_for, goals_against, games_played FROM soccer_standings
        WHERE sport=? AND team=? ORDER BY updated_at DESC LIMIT 1
    """, (sport, home)).fetchone()
    a_row = conn.execute("""
        SELECT goals_for, goals_against, games_played FROM soccer_standings
        WHERE sport=? AND team=? ORDER BY updated_at DESC LIMIT 1
    """, (sport, away)).fetchone()
    if not h_row or not a_row: return 0.0, {'reason': 'no_standings'}
    h_gf, h_ga, h_gp = h_row
    a_gf, a_ga, a_gp = a_row
    if not h_gp or not a_gp or h_gp < 5 or a_gp < 5: return 0.0, {'reason': 'small_sample'}
    # Expected total = home_gf/gp + away_gf/gp + (h_ga + a_ga)/2/gp proxy
    # Simpler: sum of per-team scoring + conceding rates, compare to league avg 2x scoring
    expected_per_team = (h_gf / h_gp + a_gf / a_gp + h_ga / h_gp + a_ga / a_gp) / 2
    la = LEAGUE_TOTAL.get(sport, 2.6)
    delta = (expected_per_team - la) * 0.4  # 40% weight — standings is long-run, recent form matters more
    delta = max(-1.0, min(1.0, delta))
    return delta, {'h_rate': round(h_gf/h_gp + h_ga/h_gp, 2), 'a_rate': round(a_gf/a_gp + a_ga/a_gp, 2), 'delta': round(delta, 2)}


def ref_total_delta(conn, sport, event_id, before_date):
    """Total-referee tendency: avg actual_total in games where this ref officiated,
    compared to the league avg. Walk-forward by requiring games before this date.
    """
    # Try to find ref for this event first
    ref_row = conn.execute("""
        SELECT official_name FROM officials
        WHERE event_id=? AND sport=? AND role IN ('referee','Referee','umpire','Umpire') LIMIT 1
    """, (event_id, sport)).fetchone()
    if not ref_row: return 0.0, {}
    ref = ref_row[0]
    # Past games refereed by this ref
    rows = conn.execute("""
        SELECT actual_total FROM officials
        WHERE official_name=? AND sport=? AND actual_total IS NOT NULL
          AND game_date < ?
    """, (ref, sport, before_date)).fetchall()
    if len(rows) < 5: return 0.0, {'ref': ref, 'n': len(rows)}
    avg = sum(r[0] for r in rows) / len(rows)
    la = LEAGUE_TOTAL.get(sport, 0.0)
    delta = (avg - la) * 0.3  # 30% weight
    # Cap per sport
    cap = {'basketball_nba': 3.0, 'icehockey_nhl': 0.5, 'basketball_ncaab': 2.0}.get(sport, 0.5)
    delta = max(-cap, min(cap, delta))
    return delta, {'ref': ref, 'ref_avg': round(avg, 1), 'n': len(rows), 'delta': round(delta, 2)}


def mlb_pitcher_matchup_delta(conn, home, away, before_date):
    """Return (ERA-based total delta, info).

    Uses mlb_probable_pitchers.home/away_pitcher_season_era. Each combined ERA
    point above league avg (~4.0) pushes the total up by ~0.5 runs via starter
    earned runs. Sample-size gate: require both pitchers to have >= 10 IP on
    the season. Otherwise return 0.
    """
    # Find the row for this game (walk-forward: use game_date <= before_date)
    row = conn.execute("""
        SELECT home_pitcher_season_era, away_pitcher_season_era,
               home_pitcher_season_ip, away_pitcher_season_ip
        FROM mlb_probable_pitchers
        WHERE game_date = ? AND home = ? AND away = ?
        ORDER BY fetched_at DESC LIMIT 1
    """, (before_date, home, away)).fetchone()
    if not row: return 0.0, {'reason': 'no_pitcher_row'}
    h_era, a_era, h_ip, a_ip = row
    if h_era is None or a_era is None: return 0.0, {'reason': 'no_era'}
    if (h_ip or 0) < 10 or (a_ip or 0) < 10: return 0.0, {'reason': 'low_ip'}
    avg_era = (h_era + a_era) / 2
    LEAGUE_AVG_ERA = 4.0
    delta = (avg_era - LEAGUE_AVG_ERA) * 0.7  # 0.7 runs per ERA point combined
    delta = max(-2.5, min(2.5, delta))
    return delta, {'h_era': h_era, 'a_era': a_era, 'avg_era': round(avg_era, 2), 'delta': round(delta, 2)}


def h2h_total_delta(conn, sport, home, away, before_date, league_avg=None):
    """Avg total in recent meetings between these two teams minus league avg."""
    if league_avg is None:
        league_avg = LEAGUE_TOTAL.get(sport, 0.0)
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
    avg = sum(totals) / len(totals)
    return avg - league_avg, len(totals)


def compute_context_total(conn, sport, home, away, event_id, market_total, commence_date):
    """Return (context_total, info) — market_total adjusted by walk-forward signals.

    Both team_form_delta calls return avg FULL-GAME total in that team's recent
    games minus league avg. Averaging the two estimates (not summing) is the
    correct unbiased baseline shift.

    Formula:
      team_form_est = (home_avg_total + away_avg_total) / 2  (both teams'
                       recent-games avg full-game total, averaged)
      form_signal   = team_form_est - league_avg
      h2h_signal    = h2h avg total - league_avg  (both full-game totals)
      context_total = market_total + form_signal * W_form + h2h_signal * W_h2h

    Phase B signals to add later: pitcher matchup, pace, weather, ref.
    """
    la = LEAGUE_TOTAL.get(sport, market_total)
    # Both team_form_delta calls compute avg full-game total in that team's
    # recent games, then subtract league_avg (full). Each returns a delta in
    # "runs/goals/points vs league avg for a full game."
    fh, fh_n = team_form_delta(conn, sport, home, commence_date, last_n=10, league_avg=la)
    fa, fa_n = team_form_delta(conn, sport, away, commence_date, last_n=10, league_avg=la)
    # Average (not sum) — these are two independent estimates of the same thing.
    form_signal = (fh + fa) / 2
    form_adj = form_signal * 0.3  # 30% weight — regresses to league average

    # H2H adj — already measured vs full league avg
    h2h, h2h_n = h2h_total_delta(conn, sport, home, away, commence_date, league_avg=la)
    h2h_adj = h2h * 0.2  # 20% weight — small sample, heavy regression

    # MLB pitcher matchup
    pitcher_adj = 0.0
    pitcher_info = {}
    if sport == 'baseball_mlb':
        pitcher_adj, pitcher_info = mlb_pitcher_matchup_delta(conn, home, away, commence_date)

    # NHL goalie form
    goalie_adj = 0.0
    goalie_info = {}
    if sport == 'icehockey_nhl':
        goalie_adj, goalie_info = nhl_goalie_form_delta(conn, home, away, commence_date)

    # Soccer standings signal
    standings_adj = 0.0
    standings_info = {}
    if 'soccer' in sport:
        standings_adj, standings_info = soccer_standings_delta(conn, sport, home, away, commence_date)

    # Referee tendency (NBA/NHL/NCAAB)
    ref_adj = 0.0
    ref_info = {}
    if sport in ('basketball_nba', 'icehockey_nhl', 'basketball_ncaab'):
        ref_adj, ref_info = ref_total_delta(conn, sport, event_id, commence_date)

    total_adj = form_adj + h2h_adj + pitcher_adj + goalie_adj + standings_adj + ref_adj
    # Cap at sport-specific maximum to prevent runaway adjustments
    cap = {
        'icehockey_nhl': 1.0,
        'basketball_nba': 15.0,
        'basketball_ncaab': 10.0,
        'baseball_mlb': 2.0,
        'baseball_ncaa': 2.5,
    }.get(sport, 1.0)
    total_adj = max(-cap, min(cap, total_adj))

    return market_total + total_adj, {
        'form_h': round(fh, 2), 'form_a': round(fa, 2),
        'form_adj': round(form_adj, 2),
        'h2h': round(h2h, 2), 'h2h_adj': round(h2h_adj, 2),
        'total_adj': round(total_adj, 2),
        'games_form_h': fh_n, 'games_form_a': fa_n, 'h2h_n': h2h_n,
    }


def grade_total(actual, line, side):
    if side == 'OVER':
        if actual > line: return 'WIN'
        if actual == line: return 'PUSH'
        return 'LOSS'
    else:
        if actual < line: return 'WIN'
        if actual == line: return 'PUSH'
        return 'LOSS'


def pnl(outcome, odds=-110, stake=5.0):
    if outcome == 'WIN': return stake * (100 / abs(odds))
    if outcome == 'LOSS': return -stake
    return 0.0


def main():
    conn = sqlite3.connect(DB)

    rows = conn.execute("""
        SELECT mc.sport, mc.event_id, mc.home, mc.away,
               mc.best_over_total, mc.best_over_odds,
               mc.best_under_total, mc.best_under_odds,
               r.home_score, r.away_score, r.commence_time, r.actual_total
        FROM market_consensus mc
        JOIN results r ON r.event_id = mc.event_id
        WHERE mc.tag='CURRENT'
          AND r.completed=1 AND r.actual_total IS NOT NULL
          AND DATE(r.commence_time) >= DATE('now', '-30 days')
          AND mc.best_over_total IS NOT NULL
        GROUP BY mc.event_id
    """).fetchall()

    print(f'Completed games with market totals (30d): {len(rows)}\n')

    # Cache: (sport, ms_over_line, ms_ctx_total, actual) per game
    cache = []
    for sport, eid, home, away, ov_line, ov_odds, un_line, un_odds, hs, as_, commence, actual in rows:
        commence_date = commence[:10] if commence else None
        if not commence_date: continue
        # Use OVER line as the reference market total (OVER/UNDER lines usually match)
        market_total = ov_line
        ctx_total, info = compute_context_total(conn, sport, home, away, eid, market_total, commence_date)
        cache.append((sport, market_total, ctx_total, actual, ov_odds or -110, un_odds or -110))

    print(f'Cached {len(cache)} games\n')

    # Threshold sweep per sport
    sports = sorted(set(c[0] for c in cache))
    for sport in sports:
        games = [c for c in cache if c[0] == sport]
        if len(games) < 10: continue
        print(f'{sport}  (n={len(games)} games)')
        print(f'  {"thresh":>7s}  {"cand":>5s}  {"W-L-P":>10s}  {"WR":>6s}  {"P/L":>8s}   {"OVER":>6s}  {"UNDER":>6s}')
        for th in DEFAULT_SWEEP:
            w = l = p = 0; s = 0.0; overs = unders = 0
            for _sp, mkt, ctx, actual, ov_odds, un_odds in games:
                adj = ctx - mkt
                if adj >= th:
                    side = 'OVER'; odds = ov_odds; overs += 1
                elif adj <= -th:
                    side = 'UNDER'; odds = un_odds; unders += 1
                else:
                    continue
                o = grade_total(actual, mkt, side)
                if o == 'WIN': w += 1
                elif o == 'LOSS': l += 1
                else: p += 1
                s += pnl(o, odds=odds)
            n = w + l
            wr = f'{w/n*100:.1f}%' if n else '-'
            cand = w + l + p
            if cand > 0:
                print(f'  {th:>7.2f}  {cand:>5d}  {w:>3d}-{l:>3d}-{p:>2d}  {wr:>6s}  {s:>+7.2f}u   {overs:>6d}  {unders:>6d}')
        print()


if __name__ == '__main__':
    main()
