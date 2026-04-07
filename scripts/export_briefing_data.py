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

    # Yesterday's graded bets — key off graded_at (when results came in),
    # not created_at (when bet was placed), so we always get the latest batch
    yesterday = conn.execute("""
        SELECT selection, sport, market_type, side_type, result, pnl_units,
               clv, edge_pct, odds, units, context_factors, context_confirmed,
               model_spread, closing_line, event_id, DATE(created_at) as dt
        FROM graded_bets
        WHERE DATE(graded_at) = (
            SELECT MAX(DATE(graded_at)) FROM graded_bets
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

    # Shadow blocked picks — concentration cap performance tracking
    shadow_blocked = []
    try:
        _sb_rows = conn.execute("""
            SELECT created_at, sport, event_id, selection, market_type, line, odds, edge_pct, units, reason
            FROM shadow_blocked_picks
            ORDER BY created_at DESC LIMIT 100
        """).fetchall()
        shadow_blocked = [dict(r) for r in _sb_rows]
    except Exception:
        pass  # Table may not exist yet

    # Book performance — running tally by sportsbook
    book_performance = []
    try:
        _bp_rows = conn.execute("""
            SELECT book,
                   COUNT(*) as bets,
                   SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                   ROUND(SUM(profit), 1) as pnl,
                   ROUND(SUM(units), 1) as wagered,
                   ROUND(AVG(CASE WHEN clv IS NOT NULL THEN clv END), 2) as avg_clv
            FROM bets
            WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.0
            GROUP BY book ORDER BY SUM(profit) DESC
        """).fetchall()
        book_performance = [dict(r) for r in _bp_rows]
    except Exception:
        pass

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
        'shadow_blocked_picks': shadow_blocked,
        'book_performance': book_performance,
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)

    print(f"  Briefing data exported: {os.path.getsize(OUTPUT_PATH) / 1024:.0f} KB")
    return OUTPUT_PATH


BRIEFING_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'morning_briefing.md')


def generate_local_briefing(conn=None):
    """Generate a professional markdown morning briefing from graded bet data.

    Writes to data/morning_briefing.md with full analysis:
    yesterday's picks, season stats, loss analysis, context health,
    edge cap, concentration risk, over/under splits, and action items.
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        close_conn = True

    now = datetime.now()
    lines = []

    def add(text=''):
        lines.append(text)

    # ── Yesterday's bets — key off graded_at to always get latest batch ──
    yesterday = conn.execute("""
        SELECT selection, sport, market_type, side_type, result, pnl_units,
               clv, edge_pct, odds, units, context_factors, context_confirmed,
               model_spread, closing_line, event_id, DATE(created_at) as dt
        FROM graded_bets
        WHERE DATE(graded_at) = (
            SELECT MAX(DATE(graded_at)) FROM graded_bets
            WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5
        )
        AND result IN ('WIN','LOSS','PUSH')
        ORDER BY pnl_units DESC
    """).fetchall()
    yesterday_bets = [dict(r) for r in yesterday]
    game_date = yesterday_bets[0]['dt'] if yesterday_bets else now.strftime('%Y-%m-%d')

    # Attach scores for losses
    for bet in yesterday_bets:
        if bet['result'] == 'LOSS' and bet.get('event_id'):
            score = conn.execute("""
                SELECT home, away, home_score, away_score, actual_total, actual_margin
                FROM results WHERE event_id = ? ORDER BY fetched_at DESC LIMIT 1
            """, (bet['event_id'],)).fetchone()
            if score:
                bet['actual_score'] = dict(score)

    # ── Season stats ──
    season = conn.execute("""
        SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as W,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as L,
               ROUND(SUM(pnl_units), 1) as pnl,
               ROUND(SUM(units), 1) as wagered
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchone()
    season_stats = dict(season) if season else {'W': 0, 'L': 0, 'pnl': 0, 'wagered': 0}
    s_w = season_stats.get('W') or 0
    s_l = season_stats.get('L') or 0
    s_pnl = season_stats.get('pnl') or 0
    s_wag = season_stats.get('wagered') or 0
    s_wp = round(s_w / (s_w + s_l) * 100, 1) if (s_w + s_l) > 0 else 0
    s_roi = round(s_pnl / s_wag * 100, 1) if s_wag > 0 else 0

    # ── By sport ──
    sport_stats = [dict(r) for r in conn.execute("""
        SELECT sport,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as W,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as L,
               ROUND(SUM(pnl_units), 1) as pnl
        FROM graded_bets
        WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
        GROUP BY sport ORDER BY pnl DESC
    """).fetchall()]

    # ── Context factor performance ──
    ctx_rows = conn.execute("""
        SELECT context_factors, result, pnl_units
        FROM graded_bets
        WHERE result IN ('WIN','LOSS') AND units >= 3.5
        AND DATE(created_at) >= '2026-03-04'
        AND context_factors IS NOT NULL AND context_factors != ''
    """).fetchall()
    factor_perf = {}
    for ctx_str, result, pnl in ctx_rows:
        factors = [f.strip().split('(')[0].strip() for f in (ctx_str or '').split('|') if f.strip()]
        for f in factors:
            if f not in factor_perf:
                factor_perf[f] = {'W': 0, 'L': 0, 'pnl': 0}
            if result == 'WIN':
                factor_perf[f]['W'] += 1
            elif result == 'LOSS':
                factor_perf[f]['L'] += 1
            factor_perf[f]['pnl'] += (pnl or 0)

    # ── Shadow picks ──
    shadow_picks = [dict(r) for r in conn.execute("""
        SELECT selection, sport, side_type, edge_pct, odds, units, event_id,
               DATE(created_at) as dt
        FROM bets
        WHERE selection LIKE '%[SHADOW]%' OR context_factors LIKE '%SHADOW%'
        AND DATE(created_at) >= '2026-03-04'
    """).fetchall()]

    # ── Edge cap analysis ──
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

    # ── Concentration risk ──
    concentration = [dict(r) for r in conn.execute("""
        SELECT DATE(created_at) as dt, sport, side_type,
               COUNT(*) as cnt, SUM(units) as total_units
        FROM bets
        WHERE units >= 3.5 AND DATE(created_at) >= '2026-03-04'
        GROUP BY dt, sport, side_type
        HAVING cnt >= 5
        ORDER BY cnt DESC
    """).fetchall()]

    # ── Over/Under by sport ──
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

    # ── Streaks ──
    recent = conn.execute("""
        SELECT result FROM graded_bets
        WHERE result IN ('WIN','LOSS') AND units >= 3.5
        AND DATE(created_at) >= '2026-03-04'
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()
    last_10 = [r[0] for r in recent]
    streak = 0
    streak_type = last_10[0] if last_10 else 'N/A'
    if last_10:
        for r in last_10:
            if r == streak_type:
                streak += 1
            else:
                break

    if close_conn:
        conn.close()

    # ═══════════════════════════════════════════════════════════
    # BUILD THE MARKDOWN
    # ═══════════════════════════════════════════════════════════

    add(f"# Scotty's Edge Morning Briefing")
    add(f"**Date:** {now.strftime('%A, %B %d, %Y')} | **Generated:** {now.strftime('%I:%M %p ET')}")
    add(f"**Game Date Graded:** {game_date}")
    add()

    # ── Yesterday's Results ──
    add("---")
    add("## Yesterday's Results")
    add()
    y_wins = sum(1 for b in yesterday_bets if b['result'] == 'WIN')
    y_losses = sum(1 for b in yesterday_bets if b['result'] == 'LOSS')
    y_pushes = sum(1 for b in yesterday_bets if b['result'] == 'PUSH')
    y_pnl = sum(b['pnl_units'] or 0 for b in yesterday_bets)
    emoji_result = 'GREEN DAY' if y_pnl > 0 else ('RED DAY' if y_pnl < 0 else 'BREAK EVEN')
    add(f"**{y_wins}W-{y_losses}L-{y_pushes}P | {'+' if y_pnl >= 0 else ''}{y_pnl:.1f}u | {emoji_result}**")
    add()

    if yesterday_bets:
        add("| Pick | Sport | Type | Odds | Units | Result | P/L | CLV |")
        add("|------|-------|------|------|-------|--------|-----|-----|")
        for b in yesterday_bets:
            clv_str = f"{b['clv']:.1f}%" if b.get('clv') is not None else '--'
            odds_str = f"{int(b['odds']):+d}" if b.get('odds') else '--'
            result_marker = {'WIN': 'W', 'LOSS': 'L', 'PUSH': 'P'}.get(b['result'], b['result'])
            pnl_str = f"{b['pnl_units']:+.1f}" if b.get('pnl_units') is not None else '--'
            add(f"| {b['selection'][:45]} | {b['sport']} | {b.get('side_type', '--')} | {odds_str} | {b.get('units', '--')} | **{result_marker}** | {pnl_str} | {clv_str} |")
        add()

    # ── Loss Analysis ──
    losses = [b for b in yesterday_bets if b['result'] == 'LOSS']
    if losses:
        add("---")
        add("## Loss Analysis")
        add()
        for b in losses:
            clv = b.get('clv')
            if clv is not None and clv > 0:
                category = "VARIANCE"
                explanation = f"Positive CLV ({clv:+.1f}%) -- the line moved our way. Right side, wrong result."
            elif clv is not None and clv < -3:
                category = "MODEL ERROR"
                explanation = f"Negative CLV ({clv:+.1f}%) -- line moved against us. Model may have been wrong."
            elif clv is not None:
                category = "MARGINAL"
                explanation = f"Small CLV ({clv:+.1f}%) -- borderline call."
            else:
                category = "NO CLV DATA"
                explanation = "No closing line available for comparison."

            add(f"### {b['selection'][:50]}")
            add(f"- **Category:** {category}")
            add(f"- **Analysis:** {explanation}")
            if b.get('actual_score'):
                sc = b['actual_score']
                add(f"- **Final Score:** {sc.get('away', '?')} {sc.get('away_score', '?')} @ {sc.get('home', '?')} {sc.get('home_score', '?')}")
                if sc.get('actual_margin') is not None and b.get('model_spread') is not None:
                    add(f"- **Model Spread:** {b['model_spread']:+.1f} | Actual Margin: {sc['actual_margin']:+.0f}")
            if b.get('context_factors'):
                add(f"- **Context:** {b['context_factors']}")
            add()

    # ── Season Stats ──
    add("---")
    add("## Season Overview (since 3/4)")
    add()
    add(f"| Metric | Value |")
    add(f"|--------|-------|")
    add(f"| Record | **{s_w}W-{s_l}L** |")
    add(f"| Win Rate | {s_wp}% |")
    add(f"| P/L | **{'+' if s_pnl >= 0 else ''}{s_pnl:.1f}u** |")
    add(f"| ROI | {'+' if s_roi >= 0 else ''}{s_roi:.1f}% |")
    add(f"| Wagered | {s_wag:.1f}u |")
    add()

    if sport_stats:
        add("### By Sport")
        add("| Sport | W | L | P/L |")
        add("|-------|---|---|-----|")
        for s in sport_stats:
            add(f"| {s['sport']} | {s['W']} | {s['L']} | {'+' if (s['pnl'] or 0) >= 0 else ''}{s['pnl']:.1f}u |")
        add()

    # ── Context Factor Health ──
    bad_factors = [f for f, d in factor_perf.items() if d['W'] + d['L'] >= 3 and d['pnl'] < 0]
    if bad_factors:
        add("---")
        add("## Context Factor Health (Negative P/L, 3+ bets)")
        add()
        add("| Factor | Record | P/L |")
        add("|--------|--------|-----|")
        for f in sorted(bad_factors, key=lambda x: factor_perf[x]['pnl']):
            d = factor_perf[f]
            add(f"| {f} | {d['W']}W-{d['L']}L | {d['pnl']:+.1f}u |")
        add()
        add("> Factors with consistent negative P/L may need weight adjustments or removal.")
        add()

    # ── Shadow Factor Report ──
    if shadow_picks:
        add("---")
        add("## Shadow Pick Report")
        add()
        add(f"Found {len(shadow_picks)} shadow-tagged picks.")
        add("| Pick | Sport | Edge | Date |")
        add("|------|-------|------|------|")
        for sp in shadow_picks[:10]:
            add(f"| {sp['selection'][:40]} | {sp['sport']} | {sp.get('edge_pct', '--')}% | {sp['dt']} |")
        add()

    # ── Edge Cap Analysis ──
    if edge_cap:
        add("---")
        add("## Edge Cap Analysis")
        add()
        add("| Bucket | Record | P/L | Avg CLV |")
        add("|--------|--------|-----|---------|")
        for ec in edge_cap:
            total = (ec['W'] or 0) + (ec['L'] or 0)
            wp = round((ec['W'] or 0) / total * 100, 1) if total > 0 else 0
            add(f"| {ec['bucket']} | {ec['W']}W-{ec['L']}L ({wp}%) | {ec['pnl']:+.1f}u | {ec.get('avg_clv', '--')} |")
        add()
        # Insight
        at_cap = next((e for e in edge_cap if e['bucket'] == 'AT_CAP'), None)
        below_cap = next((e for e in edge_cap if e['bucket'] == 'BELOW_CAP'), None)
        if at_cap and below_cap:
            at_total = (at_cap['W'] or 0) + (at_cap['L'] or 0)
            bl_total = (below_cap['W'] or 0) + (below_cap['L'] or 0)
            at_wp = (at_cap['W'] or 0) / at_total * 100 if at_total > 0 else 0
            bl_wp = (below_cap['W'] or 0) / bl_total * 100 if bl_total > 0 else 0
            if at_wp > bl_wp + 5:
                add("> AT_CAP picks are outperforming -- the cap may be leaving value on the table.")
            elif bl_wp > at_wp + 5:
                add("> BELOW_CAP picks are more reliable -- cap is working as intended.")
            else:
                add("> Performance is similar across edge tiers -- cap is neutral.")
        add()

    # ── Concentration Risk ──
    if concentration:
        add("---")
        add("## Concentration Risk (5+ same direction in a day)")
        add()
        add("| Date | Sport | Direction | Count | Units |")
        add("|------|-------|-----------|-------|-------|")
        for c in concentration:
            add(f"| {c['dt']} | {c['sport']} | {c['side_type']} | {c['cnt']} | {c['total_units']:.1f}u |")
        add()
        add("> High concentration days increase variance. Monitor for correlated losses.")
        add()

    # ── Over/Under by Sport ──
    if over_under:
        add("---")
        add("## Over/Under by Sport")
        add()
        add("| Sport | Side | W | L | P/L |")
        add("|-------|------|---|---|-----|")
        for ou in over_under:
            add(f"| {ou['sport']} | {ou['side_type']} | {ou['W']} | {ou['L']} | {ou['pnl']:+.1f}u |")
        add()

    # ── Streak ──
    add("---")
    add("## Current Streak")
    add()
    l10_str = ' '.join(last_10) if last_10 else 'No data'
    add(f"- **Last 10:** {l10_str}")
    add(f"- **Current Streak:** {streak} {streak_type}")
    add()

    # ── Action Items ──
    add("---")
    add("## Action Items")
    add()
    actions = []

    # Red day
    if y_pnl < -5:
        actions.append(f"Big red day ({y_pnl:+.1f}u). Review if any model errors need parameter changes.")

    # Losing streak
    if streak >= 3 and streak_type == 'LOSS':
        actions.append(f"{streak}-bet losing streak. Stay disciplined -- check if edges are still valid.")

    # Bad factors
    for f in bad_factors:
        d = factor_perf[f]
        if d['pnl'] < -5:
            actions.append(f"Factor '{f}' has {d['pnl']:+.1f}u P/L over {d['W']+d['L']} bets. Consider reducing weight.")

    # Concentration
    if concentration:
        actions.append("Concentration risk detected. Review directional exposure limits.")

    # AT_CAP underperforming
    at_cap = next((e for e in edge_cap if e['bucket'] == 'AT_CAP'), None)
    if at_cap and (at_cap['pnl'] or 0) < -3:
        actions.append(f"AT_CAP picks are at {at_cap['pnl']:+.1f}u. High-edge picks may be inflated by stale lines.")

    # Over/Under imbalance
    for ou in over_under:
        total = (ou['W'] or 0) + (ou['L'] or 0)
        if total >= 5 and (ou['pnl'] or 0) < -3:
            actions.append(f"{ou['sport']} {ou['side_type']}s: {ou['W']}W-{ou['L']}L ({ou['pnl']:+.1f}u). Review totals model for this sport.")

    # Model errors in yesterday's losses
    model_errors = [b for b in losses if b.get('clv') is not None and b['clv'] < -3]
    if model_errors:
        actions.append(f"{len(model_errors)} loss(es) yesterday with CLV < -3% (MODEL ERROR). Review those matchups.")

    if not actions:
        actions.append("No urgent issues. Model is operating within expected parameters.")

    for i, a in enumerate(actions, 1):
        add(f"{i}. {a}")
    add()

    add("---")
    add(f"*Generated locally by export_briefing_data.py | {now.strftime('%Y-%m-%d %H:%M ET')}*")

    # Write the file
    md_content = '\n'.join(lines)
    with open(BRIEFING_PATH, 'w', encoding='utf-8') as f:
        f.write(md_content)

    size_kb = os.path.getsize(BRIEFING_PATH) / 1024
    print(f"  Morning briefing written: {BRIEFING_PATH} ({size_kb:.0f} KB)")
    return BRIEFING_PATH


if __name__ == '__main__':
    export_data()
    generate_local_briefing()
