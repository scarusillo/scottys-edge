"""Event matcher — reconcile ESPN event_ids (results) with API event_ids (odds).

Problem: `results` table uses ESPN event IDs, `odds`/`market_consensus` use
Odds API hash IDs. These don't join on event_id, breaking NCAA backtests
entirely (zero overlap). Same problem affects tennis.

Solution: match by (sport, home, away, date). Not perfect — same teams
playing multiple days in same series can create ambiguity — but unblocks
~95% of cases.

Usage (import):
    from event_matcher import match_odds_to_results, match_results_to_odds
    rows = match_odds_to_results(conn, sport='baseball_ncaa', days_back=30)
    # returns rows joining results + odds by home/away/date

CLI self-test:
    python scripts/event_matcher.py

This is a shared utility for backtests. Replaces per-script join logic.
"""
import sqlite3
import os


def _normalize_team(name):
    """Normalize team names for fuzzy matching (lowercase, strip punctuation)."""
    if not name:
        return ''
    return ''.join(c.lower() for c in name if c.isalnum() or c == ' ').strip()


def match_odds_to_results(conn, sport=None, days_back=30, same_day_only=True):
    """Return rows from `odds` joined to `results` via home/away/date.

    Args:
        sport: optional sport filter (e.g. 'baseball_ncaa'). None = all sports.
        days_back: how many days of history to include.
        same_day_only: if True, require same-day match. If False, allow +/- 1 day
          (helpful for games with timezone-shifted commence times).

    Returns: list of dicts with keys:
        sport, home, away, commence_time,
        odds_event_id, results_event_id,
        home_score, away_score, actual_total, actual_margin
    """
    sport_filter = "AND o.sport = ?" if sport else ""
    params = [days_back]
    if sport:
        params.insert(0, sport)

    if same_day_only:
        date_match = "DATE(o.commence_time) = DATE(r.commence_time)"
    else:
        # Within 1 day (handles timezone shifts)
        date_match = "ABS(julianday(o.commence_time) - julianday(r.commence_time)) <= 1"

    q = f"""
        SELECT DISTINCT
            o.sport, o.home, o.away, o.commence_time,
            o.event_id as odds_event_id,
            r.event_id as results_event_id,
            r.home_score, r.away_score
        FROM odds o
        JOIN results r ON
            o.sport = r.sport
            AND o.home = r.home
            AND o.away = r.away
            AND {date_match}
        WHERE r.completed = 1
          AND r.home_score IS NOT NULL
          AND r.away_score IS NOT NULL
          AND o.commence_time >= date('now', ?)
          {sport_filter}
    """
    params_final = [f'-{days_back} days'] + ([sport] if sport else [])
    rows = conn.execute(q, params_final).fetchall()
    result = []
    for r in rows:
        home_score = r[6]
        away_score = r[7]
        result.append({
            'sport': r[0],
            'home': r[1],
            'away': r[2],
            'commence_time': r[3],
            'odds_event_id': r[4],
            'results_event_id': r[5],
            'home_score': home_score,
            'away_score': away_score,
            'actual_total': home_score + away_score,
            'actual_margin': home_score - away_score,
        })
    return result


def match_openers_to_results(conn, sport=None, days_back=30):
    """Like match_odds_to_results but from openers table.

    `openers` doesn't carry home/away directly — we join through `odds`
    on event_id to recover them, then match to results.
    """
    sport_filter = "AND o.sport = ?" if sport else ""
    q = f"""
        SELECT DISTINCT
            op.sport, o.home, o.away, o.commence_time,
            op.event_id as openers_event_id,
            r.event_id as results_event_id,
            r.home_score, r.away_score
        FROM openers op
        JOIN odds o ON op.event_id = o.event_id
        JOIN results r ON
            op.sport = r.sport
            AND o.home = r.home
            AND o.away = r.away
            AND DATE(o.commence_time) = DATE(r.commence_time)
        WHERE r.completed = 1
          AND r.home_score IS NOT NULL
          AND r.away_score IS NOT NULL
          AND op.snapshot_date >= date('now', ?)
          {sport_filter.replace('o.sport', 'op.sport')}
    """
    params = [f'-{days_back} days'] + ([sport] if sport else [])
    rows = conn.execute(q, params).fetchall()
    return [{
        'sport': r[0], 'home': r[1], 'away': r[2], 'commence_time': r[3],
        'openers_event_id': r[4], 'results_event_id': r[5],
        'home_score': r[6], 'away_score': r[7],
        'actual_total': r[6] + r[7], 'actual_margin': r[6] - r[7],
    } for r in rows]


if __name__ == '__main__':
    # CLI self-test — report coverage per sport
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db)
    print('=== Event matcher coverage by sport (30 days) ===')
    for sport in ['baseball_ncaa', 'baseball_mlb', 'basketball_nba', 'basketball_ncaab',
                   'icehockey_nhl', 'soccer_usa_mls', 'soccer_italy_serie_a',
                   'soccer_epl', 'tennis_atp_madrid_open', 'tennis_wta_madrid_open']:
        matched = match_odds_to_results(conn, sport=sport, days_back=30)
        # Count total completed results for this sport
        total = conn.execute("""
            SELECT COUNT(*) FROM results WHERE sport = ? AND completed = 1
              AND home_score IS NOT NULL AND commence_time >= date('now', '-30 days')
        """, (sport,)).fetchone()[0]
        match_pct = len(matched) / total * 100 if total else 0
        print(f'  {sport:<30} matched={len(matched):>4} / total_results={total:>4} ({match_pct:.0f}%)')

    # Openers coverage
    print()
    print('=== Openers-to-results coverage (30 days) ===')
    for sport in ['baseball_ncaa', 'baseball_mlb', 'icehockey_nhl',
                   'basketball_nba', 'basketball_ncaab']:
        matched = match_openers_to_results(conn, sport=sport, days_back=30)
        print(f'  {sport:<30} matched={len(matched):>4}')

    conn.close()
