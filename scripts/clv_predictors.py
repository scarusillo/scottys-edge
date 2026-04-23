"""CLV predictor mining — find fire-time features that predict CLV direction.

Goal (user ask 2026-04-23): screen every graded pick against every available
fire-time feature; rank features by how much they shift avg CLV; propose
channel concepts based on the strongest predictors.

Key idea: the CLV→Win signal is the strongest leading indicator we have.
If we can predict which fire-time features will land a pick in the
positive-CLV bucket, we can tilt stake, add channels, or add gates.

Outputs:
  - scripts stdout  (full report)
  - data/clv_predictors_report.md  (same content, saved)

Honest sample warning: we have ~378 non-null CLV picks post-rebuild.
Anything with n<15 is noise. Everything here is a HYPOTHESIS for backtest,
not a gate-ready recommendation.

Usage:
    python scripts/clv_predictors.py
    python scripts/clv_predictors.py --days 30
    python scripts/clv_predictors.py --out -    # stdout only
"""
import argparse
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
DEFAULT_OUT = os.path.join(os.path.dirname(__file__), '..', 'data', 'clv_predictors_report.md')
REBUILD_DATE = '2026-03-04'

# Significance thresholds
MIN_COHORT = 15        # below this = noise
STRONG_CLV_GAP = 0.40  # CLV gap (present - absent) at or above this = signal candidate
STRONG_N = 25          # cohorts at or above this get bolder flags

# Context factor patterns we scan for in the free-text context_factors column
CONTEXT_PATTERNS = [
    'Steam: sharp confirms', 'Steam: sharp opposes', 'Steam: no movement',
    'Home letdown', 'Away letdown', 'Home bounce-back', 'Away bounce-back',
    'Division familiarity', 'Division game',
    'Home slow-paced', 'Away slow-paced', 'Home fast-paced', 'Away fast-paced',
    'Midweek game', 'Saturday game', 'Sunday game',
    'Weather:', 'Wind',
    'Park:',
    'Pitching edge:', 'Goalies:',
    'H2H high-scoring', 'H2H low-scoring',
    'DATA_TOTAL v25', 'DATA_SPREAD v25',
    'PROP_FADE_FLIP', 'PROP_BOOK_ARB', 'BOOK_ARB',
    'Home 3-in-5', 'Away 3-in-5',
    'Home hot streak', 'Away hot streak',
    '[SHADOW]',
]


def fmt_row(cols, widths):
    return ' | '.join(str(c).ljust(w) for c, w in zip(cols, widths))


def present_vs_absent(conn, since_date, pattern):
    """Returns (n_present, avg_clv_present, pnl_present, wins_present,
                n_absent,  avg_clv_absent)."""
    q_present = """
        SELECT COUNT(*), ROUND(AVG(clv),2), ROUND(SUM(pnl_units),1),
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END)
        FROM graded_bets
        WHERE created_at >= ? AND clv IS NOT NULL
          AND result IN ('WIN','LOSS','PUSH')
          AND context_factors LIKE ?
    """
    r1 = conn.execute(q_present, (since_date, f'%{pattern}%')).fetchone()
    q_absent = """
        SELECT COUNT(*), ROUND(AVG(clv),2)
        FROM graded_bets
        WHERE created_at >= ? AND clv IS NOT NULL
          AND result IN ('WIN','LOSS','PUSH')
          AND (context_factors IS NULL OR context_factors NOT LIKE ?)
    """
    r2 = conn.execute(q_absent, (since_date, f'%{pattern}%')).fetchone()
    return (r1[0], r1[1], r1[2], r1[3], r2[0], r2[1])


def col_vs_overall(conn, since_date, col):
    """For each value of col, compare its avg CLV vs the overall avg CLV.
    Returns list of (value, n, avg_clv, pnl, wins, diff_from_overall)."""
    overall = conn.execute(
        "SELECT AVG(clv) FROM graded_bets WHERE created_at >= ? AND clv IS NOT NULL",
        (since_date,)).fetchone()[0] or 0.0
    rows = conn.execute(f"""
        SELECT COALESCE({col}, 'UNSET') AS v, COUNT(*) AS n,
               ROUND(AVG(clv),2) AS avg_clv, ROUND(SUM(pnl_units),1) AS pnl,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) AS wins
        FROM graded_bets
        WHERE created_at >= ? AND clv IS NOT NULL AND result IN ('WIN','LOSS','PUSH')
        GROUP BY v
    """, (since_date,)).fetchall()
    out = []
    for v, n, avg, pnl, w in rows:
        diff = round((avg or 0) - overall, 2)
        out.append((v, n, avg, pnl, w, diff))
    return sorted(out, key=lambda r: -r[5])  # sort by diff DESC


def two_way(conn, since_date, col_a, col_b):
    """Cross-tab. Returns rows where cohort diff from overall is strongest."""
    overall = conn.execute(
        "SELECT AVG(clv) FROM graded_bets WHERE created_at >= ? AND clv IS NOT NULL",
        (since_date,)).fetchone()[0] or 0.0
    rows = conn.execute(f"""
        SELECT COALESCE({col_a}, 'UNSET'), COALESCE({col_b}, 'UNSET'),
               COUNT(*), ROUND(AVG(clv),2), ROUND(SUM(pnl_units),1),
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END)
        FROM graded_bets
        WHERE created_at >= ? AND clv IS NOT NULL AND result IN ('WIN','LOSS','PUSH')
        GROUP BY COALESCE({col_a}, 'UNSET'), COALESCE({col_b}, 'UNSET')
    """, (since_date,)).fetchall()
    out = []
    for a, b, n, avg, pnl, w in rows:
        if n < MIN_COHORT:
            continue
        diff = round((avg or 0) - overall, 2)
        out.append((a, b, n, avg, pnl, w, diff))
    return sorted(out, key=lambda r: -r[6])


def line_movement_analysis(conn, since_date):
    """Join graded_bets with openers to compute opener→fire line movement
    and check if it predicts CLV direction."""
    # For each graded bet, compute avg opener line across books for the same
    # event+market. Then compare to bet.line (fire line). Direction-adjust
    # based on bet side (OVER/DOG = we want line UP; UNDER/FAV = line DOWN).
    q = """
        WITH opener_avg AS (
            SELECT event_id, market, AVG(line) AS avg_opener_line
            FROM openers WHERE line IS NOT NULL
            GROUP BY event_id, market
        )
        SELECT g.sport, g.market_type, g.side_type, g.line AS fire_line,
               o.avg_opener_line, g.closing_line, g.clv, g.pnl_units, g.result,
               g.selection
        FROM graded_bets g
        LEFT JOIN opener_avg o
          ON g.event_id = o.event_id
         AND ((g.market_type='SPREAD' AND o.market='spreads')
           OR (g.market_type='TOTAL' AND o.market='totals')
           OR (g.market_type='MONEYLINE' AND o.market='h2h'))
        WHERE g.created_at >= ? AND g.clv IS NOT NULL
          AND g.result IN ('WIN','LOSS','PUSH')
          AND g.market_type IN ('SPREAD','TOTAL')
    """
    rows = conn.execute(q, (since_date,)).fetchall()

    buckets = defaultdict(list)  # bucket_label -> list of (clv, pnl, result)
    matched = 0
    for sport, mtype, stype, fire, opener, close, clv, pnl, result, sel in rows:
        if opener is None:
            continue
        matched += 1
        # Direction-adjust raw move. Positive = moved toward our side.
        raw_move = fire - opener
        if mtype == 'TOTAL':
            sel_upper = (sel or '').upper()
            our_move = raw_move if 'OVER' in sel_upper else -raw_move
        else:  # SPREAD
            # DOG bets the + side (we want line more positive)
            # FAV bets the - side (we want line less negative → increase)
            # Stype may be 'DOG' or 'FAVORITE' (v25 model) or None
            if stype == 'DOG':
                our_move = raw_move
            elif stype == 'FAVORITE':
                our_move = -raw_move
            else:
                continue  # unknown side
        # Bucket: strong toward us / mild toward us / flat / mild against / strong against
        if our_move >= 1.0:
            label = 'A. opener→fire >= +1.0 (strong toward us pre-bet)'
        elif our_move >= 0.25:
            label = 'B. opener→fire +0.25..+1.0 (mild toward us)'
        elif our_move > -0.25:
            label = 'C. opener→fire flat (|<0.25|)'
        elif our_move > -1.0:
            label = 'D. opener→fire -0.25..-1.0 (mild against us)'
        else:
            label = 'E. opener→fire <= -1.0 (strong against us pre-bet)'
        buckets[label].append((clv, pnl, result))
    return matched, buckets


def fmt_table(headers, rows):
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows) or [0])
              for i, h in enumerate(headers)]
    header = '| ' + ' | '.join(str(h).ljust(w) for h, w in zip(headers, widths)) + ' |'
    sep    = '|-' + '-|-'.join('-' * w for w in widths) + '-|'
    body   = '\n'.join('| ' + ' | '.join(str(r[i]).ljust(w) for i, w in enumerate(widths)) + ' |'
                       for r in rows)
    return '\n'.join([header, sep, body])


def build_report(conn, since_date):
    lines = []
    lines.append(f"# CLV Predictor Analysis — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"\n**Window:** since {since_date}  |  **Min cohort:** {MIN_COHORT}")
    lines.append(f"**Signal threshold:** CLV gap ≥ {STRONG_CLV_GAP}, n ≥ {STRONG_N}\n")

    # Baseline
    total = conn.execute("""
        SELECT COUNT(*), ROUND(AVG(clv),2)
        FROM graded_bets WHERE created_at >= ? AND clv IS NOT NULL
          AND result IN ('WIN','LOSS','PUSH')
    """, (since_date,)).fetchone()
    lines.append(f"**Baseline:** {total[0]} graded bets with CLV, overall avg CLV = {total[1]:+.2f}\n")

    # ─── Section 1: Context-factor text patterns ──────────────
    lines.append("## 1. Context-factor signals (text patterns)\n")
    lines.append("For each recurring context tag: how does the pick perform WITH vs WITHOUT the tag?\n")
    sig_rows = []
    all_rows = []
    for pat in CONTEXT_PATTERNS:
        np, avg_p, pnl_p, wins_p, na, avg_a = present_vs_absent(conn, since_date, pat)
        if np < 1:
            continue
        gap = round((avg_p or 0) - (avg_a or 0), 2)
        wp = (wins_p / np * 100) if np else 0
        all_rows.append((pat, np, avg_p, avg_a, gap, wp, pnl_p))
        if np >= MIN_COHORT and abs(gap) >= STRONG_CLV_GAP:
            sig_rows.append((pat, np, avg_p, avg_a, gap, wp, pnl_p))
    all_rows.sort(key=lambda r: -r[4])
    lines.append(fmt_table(
        ['Pattern', 'n_with', 'CLV_with', 'CLV_without', 'gap', 'Win%', 'P/L'],
        [(r[0], r[1], f'{r[2]:+.2f}' if r[2] is not None else '—',
          f'{r[3]:+.2f}' if r[3] is not None else '—',
          f'{r[4]:+.2f}', f'{r[5]:.0f}%', f'{r[6]:+.1f}u' if r[6] is not None else '—')
         for r in all_rows]))
    lines.append("")
    if sig_rows:
        lines.append("**Strong signals (|gap| ≥ 0.40, n ≥ 15):**")
        for pat, np_, avg_p, avg_a, gap, wp, pnl_p in sig_rows:
            arrow = '↑' if gap > 0 else '↓'
            lines.append(f"- {arrow} `{pat}` — n={np_}, CLV gap {gap:+.2f}, Win% {wp:.0f}%, P/L {pnl_p:+.1f}u")
    lines.append("")

    # ─── Section 2: Single dimensions — CLV diff from overall ─
    lines.append("## 2. Dimension ranking by CLV deviation\n")
    lines.append("For each single dimension, cohorts ranked by CLV diff from the overall baseline.\n")
    for col, label in [
        ('sport', 'Sport'),
        ('side_type', 'Side type'),
        ('market_type', 'Market type'),
        ('book', 'Book'),
        ('edge_bucket', 'Edge bucket'),
        ('spread_bucket', 'Spread bucket'),
        ('timing', 'Timing'),
        ('market_tier', 'Market tier'),
        ('day_of_week', 'Day of week'),
        ('confidence', 'Confidence'),
        ('context_confirmed', 'Context confirmed'),
    ]:
        rows = [r for r in col_vs_overall(conn, since_date, col) if r[1] >= MIN_COHORT]
        if not rows:
            continue
        lines.append(f"### {label}\n")
        lines.append(fmt_table(
            ['Value', 'n', 'avg CLV', 'diff', 'Win%', 'P/L'],
            [(v, n, f'{avg:+.2f}' if avg is not None else '—',
              f'{diff:+.2f}', f'{(w/n*100):.0f}%', f'{pnl:+.1f}u' if pnl is not None else '—')
             for v, n, avg, pnl, w, diff in rows]))
        lines.append("")

    # ─── Section 3: Cross-tabs (strongest differentiators) ────
    lines.append("## 3. Interaction effects (2-way cohorts with largest CLV gaps)\n")
    lines.append("Cross-tabs showing rows where avg CLV deviates most from baseline (n ≥ 15).\n")
    for a, b, label in [
        ('book', 'market_type', 'Book × Market'),
        ('sport', 'book', 'Sport × Book'),
        ('sport', 'side_type', 'Sport × Side type'),
        ('edge_bucket', 'book', 'Edge × Book'),
        ('timing', 'book', 'Timing × Book'),
    ]:
        rows = two_way(conn, since_date, a, b)
        if not rows:
            continue
        lines.append(f"### {label}\n")
        lines.append(fmt_table(
            ['A', 'B', 'n', 'avg CLV', 'diff', 'Win%', 'P/L'],
            [(a_v, b_v, n, f'{avg:+.2f}', f'{diff:+.2f}', f'{(w/n*100):.0f}%', f'{pnl:+.1f}u')
             for a_v, b_v, n, avg, pnl, w, diff in rows[:8]]))
        lines.append("")

    # ─── Section 4: Line-movement analysis ────────────────────
    lines.append("## 4. Line movement — does opener→fire predict fire→close?\n")
    lines.append("If a line has already moved toward us BEFORE we bet, does it keep going? "
                 "This is the big question for a 'follow the move' channel.\n")
    matched, buckets = line_movement_analysis(conn, since_date)
    lines.append(f"**Matched** {matched} game-line graded bets with opener history.\n")
    bucket_rows = []
    for label in sorted(buckets.keys()):
        data = buckets[label]
        n = len(data)
        if n == 0:
            continue
        avg_clv = sum(c for c, p, r in data) / n
        pnl = sum(p for c, p, r in data if p is not None)
        wins = sum(1 for c, p, r in data if r == 'WIN')
        bucket_rows.append((label, n, avg_clv, pnl, wins))
    lines.append(fmt_table(
        ['Bucket (opener→fire movement)', 'n', 'avg CLV', 'Win%', 'P/L'],
        [(l, n, f'{a:+.2f}', f'{(w/n*100):.0f}%', f'{p:+.1f}u')
         for l, n, a, p, w in bucket_rows]))
    lines.append("")
    lines.append("**Interpretation:**")
    lines.append("- If A (strong toward us) also has highest avg CLV post-fire → momentum signal (line keeps moving our way after our bet too).")
    lines.append("- If A has negative post-fire CLV → we're buying the top (line has already priced in the move).")
    lines.append("- If E (strong against us pre-bet) has positive CLV → market overshoots; we fade the early move.")
    lines.append("")

    # ─── Section 5: Channel concepts ──────────────────────────
    lines.append("## 5. Channel concepts (proposals — not shipped)\n")
    lines.append("Based on the signals above, two or three concrete channel designs worth backtesting:\n")
    lines.append("1. **CLV_MOMENTUM_FOLLOW** — If the line has already moved ≥ +0.5 toward our")
    lines.append("   side between opener and fire time, and we have an existing edge pick, ")
    lines.append("   boost stake. Validate with Section 4 data: do bucket A/B pre-bet moves")
    lines.append("   correlate with positive post-bet CLV?")
    lines.append("")
    lines.append("2. **CLV_CONTEXT_STACK** — Fire at 5u (same default) but only when the context_factors")
    lines.append("   string contains one of the Strong Signal tags from Section 1. Initially shadow-mode")
    lines.append("   as CLV_CONTEXT_SHADOW to verify signal holds on a larger sample.")
    lines.append("")
    lines.append("3. **CLV_BOOK_ROUTE** — Prefer routing to books with consistently high avg CLV")
    lines.append("   (per Section 2's book ranking) when multiple books offer the same edge. This is a")
    lines.append("   routing change, not a new channel — execution tweak.")
    lines.append("")
    lines.append("## 6. Next steps\n")
    lines.append("- Pick the top 1-2 strongest signals from Section 1 + Section 4")
    lines.append("- Backtest them in isolation: apply the rule retroactively, measure delta P/L")
    lines.append("- If backtest clears +10u over 30d on n≥20, ship in shadow mode for 2-4 weeks")
    lines.append("- Promote to live only after shadow-mode sample confirms the backtest")
    lines.append("")
    lines.append(f"**Note on sample size:** {total[0]} graded CLV picks is enough for directional")
    lines.append("signals but NOT enough for a production classifier. Single-feature signals with")
    lines.append(f"|gap| ≥ {STRONG_CLV_GAP} at n ≥ {STRONG_N} are the realistic actionable bar today.\n")

    return '\n'.join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--days', type=int, default=None)
    p.add_argument('--out', default=DEFAULT_OUT)
    args = p.parse_args()

    if args.days:
        since = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    else:
        since = REBUILD_DATE

    conn = sqlite3.connect(DB)
    report = build_report(conn, since)
    conn.close()

    print(report)
    if args.out and args.out != '-':
        with open(args.out, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f'\n[written to {args.out}]')


if __name__ == '__main__':
    main()
