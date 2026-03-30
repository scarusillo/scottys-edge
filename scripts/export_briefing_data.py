#!/usr/bin/env python3
"""
export_briefing_data.py — Export graded data for cloud agent analysis.

Runs after local grading. Exports a small JSON file with everything the
cloud agent needs to produce the morning briefing, so it doesn't need
to download the 2.6GB database or call any APIs.

The cloud agent reads data/briefing_data.json and writes the analysis.
"""
import sqlite3, json, os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'briefing_data.json')


def export_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    now = datetime.now()

    # Yesterday's graded bets
    yesterday = conn.execute("""
        SELECT selection, sport, market_type, side_type, result, pnl_units,
               clv, edge_pct, odds, units, context_factors, context_confirmed,
               model_spread, closing_line, event_id, DATE(created_at) as dt
        FROM graded_bets
        WHERE DATE(created_at) = (
            SELECT MAX(DATE(created_at)) FROM graded_bets
            WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5
        )
        AND result IN ('WIN','LOSS','PUSH')
        ORDER BY pnl_units DESC
    """).fetchall()

    yesterday_bets = [dict(r) for r in yesterday]
    game_date = yesterday_bets[0]['dt'] if yesterday_bets else None

    # Get actual scores for losses
    for bet in yesterday_bets:
        if bet['result'] == 'LOSS' and bet['event_id']:
            score = conn.execute("""
                SELECT home, away, home_score, away_score, actual_total, actual_margin
                FROM results WHERE event_id = ? ORDER BY fetched_at DESC LIMIT 1
            """, (bet['event_id'],)).fetchone()
            if score:
                bet['actual_score'] = dict(score)

    # Season totals
    season = conn.execute("""
        SELECT result,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as W,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as L,
               ROUND(SUM(pnl_units), 1) as pnl,
               ROUND(SUM(units), 1) as wagered
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchone()
    season_stats = dict(season) if season else {}

    # By sport
    sport_stats = [dict(r) for r in conn.execute("""
        SELECT sport,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as W,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as L,
               ROUND(SUM(pnl_units), 1) as pnl
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
        GROUP BY sport
    """).fetchall()]

    # Context factor performance (all time)
    ctx_rows = conn.execute("""
        SELECT context_factors, result, pnl_units, DATE(created_at) as dt
        FROM graded_bets
        WHERE result IN ('WIN','LOSS') AND units >= 3.5
        AND DATE(created_at) >= '2026-03-04'
        AND context_factors IS NOT NULL AND context_factors != ''
        ORDER BY created_at
    """).fetchall()

    factor_perf = {}
    for ctx_str, result, pnl, dt in ctx_rows:
        factors = [f.strip().split('(')[0].strip() for f in ctx_str.split('|') if f.strip()]
        for f in factors:
            if f not in factor_perf:
                factor_perf[f] = {'W': 0, 'L': 0, 'pnl': 0, 'bets': []}
            if result == 'WIN':
                factor_perf[f]['W'] += 1
            elif result == 'LOSS':
                factor_perf[f]['L'] += 1
            factor_perf[f]['pnl'] += (pnl or 0)
            factor_perf[f]['bets'].append({'result': result, 'pnl': pnl, 'dt': dt})

    # Split each factor into first half / second half for trend detection
    context_health = []
    for f, d in factor_perf.items():
        total = d['W'] + d['L']
        if total < 3:
            continue
        mid = total // 2
        first_half = d['bets'][:mid]
        second_half = d['bets'][mid:]
        fh_pnl = round(sum(b['pnl'] or 0 for b in first_half), 1)
        sh_pnl = round(sum(b['pnl'] or 0 for b in second_half), 1)
        context_health.append({
            'factor': f,
            'W': d['W'], 'L': d['L'],
            'pnl': round(d['pnl'], 1),
            'first_half_pnl': fh_pnl,
            'second_half_pnl': sh_pnl,
            'trending': 'WORSE' if sh_pnl < fh_pnl - 3 else ('BETTER' if sh_pnl > fh_pnl + 3 else 'STABLE'),
        })
    context_health.sort(key=lambda x: x['pnl'])

    # Day of week performance by sport
    dow_perf = [dict(r) for r in conn.execute("""
        SELECT day_of_week, sport,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as W,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as L,
               ROUND(SUM(pnl_units), 1) as pnl
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
        GROUP BY day_of_week, sport
        HAVING (W + L) >= 2
        ORDER BY pnl
    """).fetchall()]

    # Edge cap analysis
    edge_cap = [dict(r) for r in conn.execute("""
        SELECT CASE WHEN edge_pct >= 20.0 THEN 'AT_CAP' ELSE 'BELOW_CAP' END as bucket,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as W,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as L,
               ROUND(SUM(pnl_units), 1) as pnl,
               ROUND(AVG(clv), 1) as avg_clv
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
        GROUP BY bucket
    """).fetchall()]

    # Concentration risk — days with 5+ same direction
    concentration = [dict(r) for r in conn.execute("""
        SELECT DATE(created_at) as dt, sport, side_type,
               COUNT(*) as cnt, SUM(units) as total_units
        FROM bets
        WHERE units >= 3.5 AND DATE(created_at) >= '2026-03-04'
        GROUP BY dt, sport, side_type
        HAVING cnt >= 5
        ORDER BY cnt DESC
    """).fetchall()]

    # Over vs Under by sport
    over_under = [dict(r) for r in conn.execute("""
        SELECT sport, side_type,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as W,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as L,
               ROUND(SUM(pnl_units), 1) as pnl
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
        AND side_type IN ('OVER','UNDER')
        GROUP BY sport, side_type
    """).fetchall()]

    # Ungraded bets
    ungraded = [dict(r) for r in conn.execute("""
        SELECT selection, sport, DATE(created_at) as dt
        FROM bets
        WHERE DATE(created_at) >= DATE('now', '-5 days')
        AND units >= 3.5
        AND event_id NOT IN (
            SELECT DISTINCT event_id FROM graded_bets WHERE event_id IS NOT NULL
        )
    """).fetchall()]

    # Streaks
    recent = conn.execute("""
        SELECT result FROM graded_bets
        WHERE result IN ('WIN','LOSS') AND units >= 3.5
        AND DATE(created_at) >= '2026-03-04'
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()
    last_10 = [r[0] for r in recent]

    streak = 0
    if last_10:
        streak_type = last_10[0]
        for r in last_10:
            if r == streak_type:
                streak += 1
            else:
                break

    conn.close()

    data = {
        'generated_at': now.strftime('%Y-%m-%d %H:%M'),
        'game_date': game_date,
        'yesterday': yesterday_bets,
        'season': season_stats,
        'by_sport': sport_stats,
        'context_health': context_health,
        'day_of_week': dow_perf,
        'edge_cap': edge_cap,
        'concentration_risk': concentration,
        'over_under': over_under,
        'ungraded': ungraded,
        'last_10': last_10,
        'streak': streak,
        'streak_type': last_10[0] if last_10 else 'N/A',
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)

    print(f"  Briefing data exported: {os.path.getsize(OUTPUT_PATH) / 1024:.0f} KB")
    return OUTPUT_PATH


if __name__ == '__main__':
    export_data()
