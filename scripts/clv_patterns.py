"""CLV pattern recognition — historical analysis of graded picks.

Goal (user ask, 2026-04-23): find fire-time features that correlate with
positive CLV (wins) vs negative CLV (losses), so future picks can be
screened or boosted accordingly.

Baseline finding (post-rebuild, n=442):
  POS CLV  (>0)  → 56.3% WR, +106.0u on 126 picks
  ZERO CLV (=0)  → 47.1% WR,   +5.6u on 255 picks
  NEG CLV  (<0)  → 34.4% WR,  -44.5u on  61 picks
  NULL CLV       → 16.7% WR,  -36.8u on  60 picks  (props w/ missing close)

This script groups graded_bets by fire-time dimensions and reports:
  - Cohorts with consistent positive CLV → stake-boost candidates
  - Cohorts with consistent negative CLV → gate candidates
  - Full CLV distribution within each cohort

Usage:
    python scripts/clv_patterns.py                   # full analysis
    python scripts/clv_patterns.py --days 30         # last 30 days only
    python scripts/clv_patterns.py --min-n 15        # raise significance floor
    python scripts/clv_patterns.py --out path.md     # write report to file
"""
import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
DEFAULT_OUT = os.path.join(os.path.dirname(__file__), '..', 'data', 'clv_patterns_report.md')
REBUILD_DATE = '2026-03-04'  # post-rebuild baseline per feedback_record_filter memory

# Action thresholds
BOOST_CLV = 0.5       # avg CLV >= this → boost candidate
GATE_CLV = -0.3       # avg CLV <= this → gate candidate
MIN_COHORT = 10       # min picks to report any cohort
STRONG_N = 20         # cohorts at or above this get louder flags


def query(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()


def cohort_stats(conn, group_col, label, since_date, min_n):
    """Aggregate CLV stats by one dimension. Returns rows sorted by avg CLV DESC."""
    sql = f"""
        SELECT
            COALESCE({group_col}, 'UNSET') AS val,
            COUNT(*) AS n,
            SUM(CASE WHEN clv > 0 THEN 1 ELSE 0 END) AS pos,
            SUM(CASE WHEN clv < 0 THEN 1 ELSE 0 END) AS neg,
            ROUND(AVG(clv), 2) AS avg_clv,
            ROUND(100.0 * SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_pct,
            ROUND(SUM(pnl_units), 1) AS pnl
        FROM graded_bets
        WHERE created_at >= ?
          AND clv IS NOT NULL
          AND result IN ('WIN','LOSS','PUSH')
        GROUP BY val
        HAVING n >= ?
        ORDER BY avg_clv DESC
    """
    return query(conn, sql, (since_date, min_n))


def two_dim_stats(conn, col_a, col_b, since_date, min_n):
    sql = f"""
        SELECT
            COALESCE({col_a}, 'UNSET') AS a,
            COALESCE({col_b}, 'UNSET') AS b,
            COUNT(*) AS n,
            ROUND(AVG(clv), 2) AS avg_clv,
            ROUND(100.0 * SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_pct,
            ROUND(SUM(pnl_units), 1) AS pnl
        FROM graded_bets
        WHERE created_at >= ?
          AND clv IS NOT NULL
          AND result IN ('WIN','LOSS','PUSH')
        GROUP BY a, b
        HAVING n >= ?
        ORDER BY avg_clv DESC
    """
    return query(conn, sql, (since_date, min_n))


def baseline_table(conn, since_date):
    sql = """
        SELECT
            CASE WHEN clv > 0 THEN 'POS'
                 WHEN clv < 0 THEN 'NEG'
                 WHEN clv = 0 THEN 'ZERO'
                 ELSE 'NULL' END AS bucket,
            COUNT(*) AS n,
            ROUND(100.0 * SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_pct,
            ROUND(SUM(pnl_units), 1) AS pnl
        FROM graded_bets
        WHERE created_at >= ?
        GROUP BY bucket
        ORDER BY CASE bucket WHEN 'POS' THEN 1 WHEN 'ZERO' THEN 2 WHEN 'NEG' THEN 3 ELSE 4 END
    """
    return query(conn, sql, (since_date,))


def fmt_rows(rows, headers):
    widths = [max(len(h), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    header = ' | '.join(h.ljust(w) for h, w in zip(headers, widths))
    sep = '-|-'.join('-' * w for w in widths)
    body = '\n'.join(' | '.join(str(r[i]).ljust(w) for i, w in enumerate(widths))
                     for r in rows)
    return f'{header}\n{sep}\n{body}'


def write_report(conn, since_date, min_n, out_path):
    lines = []
    lines.append(f"# CLV Pattern Analysis — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"\n**Window:** since {since_date}  |  **Min cohort size:** {min_n}\n")

    # ─── Baseline ─────────────────────────────────────────────
    lines.append("## 1. Baseline — CLV predicts wins\n")
    base = baseline_table(conn, since_date)
    lines.append('| Bucket | n | Win% | P/L |')
    lines.append('|---|---|---|---|')
    for b, n, wp, pl in base:
        lines.append(f'| {b} | {n} | {wp}% | {pl:+.1f}u |')
    lines.append("")
    lines.append("> Positive CLV picks win at materially higher rates than the rest.")
    lines.append("> NULL CLV ≈ props with missing closing-line capture; treat as unmeasured.\n")

    # ─── Positive / negative by single dimension ──────────────
    dimensions = [
        ('sport',             'Sport'),
        ('side_type',         'Side type'),
        ('market_type',       'Market type'),
        ('book',              'Book'),
        ('edge_bucket',       'Edge bucket'),
        ('spread_bucket',     'Spread bucket'),
        ('confidence',        'Confidence'),
        ('timing',            'Timing'),
        ('market_tier',       'Market tier'),
        ('day_of_week',       'Day of week'),
        ('context_confirmed', 'Context confirmed'),
    ]

    lines.append("## 2. Single-dimension cohorts (ranked by avg CLV)\n")
    for col, label in dimensions:
        rows = cohort_stats(conn, col, label, since_date, min_n)
        if not rows:
            continue
        lines.append(f"### {label}\n")
        lines.append('| Value | n | Pos | Neg | avg CLV | Win% | P/L |')
        lines.append('|---|---|---|---|---|---|---|')
        for val, n, pos, neg, avg, wp, pl in rows:
            flag = ''
            if n >= STRONG_N and avg >= BOOST_CLV:
                flag = ' ⭐'
            elif n >= STRONG_N and avg <= GATE_CLV:
                flag = ' 🚩'
            lines.append(f'| {val} | {n} | {pos} | {neg} | {avg:+.2f} | {wp}% | {pl:+.1f}u |{flag}')
        lines.append("")

    # ─── Two-dim cross-tabs for highest-value pairs ───────────
    crosstabs = [
        ('sport', 'book',        'Sport × Book'),
        ('sport', 'side_type',   'Sport × Side type'),
        ('book',  'market_type', 'Book × Market type'),
        ('edge_bucket', 'book',  'Edge bucket × Book'),
    ]
    lines.append("## 3. Cross-tabs (n >= min cohort)\n")
    for a, b, label in crosstabs:
        rows = two_dim_stats(conn, a, b, since_date, min_n)
        if not rows:
            continue
        lines.append(f"### {label}\n")
        lines.append('| A | B | n | avg CLV | Win% | P/L |')
        lines.append('|---|---|---|---|---|---|')
        for va, vb, n, avg, wp, pl in rows:
            flag = ''
            if n >= STRONG_N and avg >= BOOST_CLV:
                flag = ' ⭐'
            elif n >= STRONG_N and avg <= GATE_CLV:
                flag = ' 🚩'
            lines.append(f'| {va} | {vb} | {n} | {avg:+.2f} | {wp}% | {pl:+.1f}u |{flag}')
        lines.append("")

    # ─── Action items ─────────────────────────────────────────
    lines.append("## 4. Action candidates\n")
    lines.append("**⭐ Boost candidates** — avg CLV ≥ +0.5, n ≥ 20:\n")
    any_boost = False
    for col, label in dimensions:
        for val, n, pos, neg, avg, wp, pl in cohort_stats(conn, col, label, since_date, min_n):
            if n >= STRONG_N and avg >= BOOST_CLV:
                lines.append(f'- `{label} = {val}`  n={n}, avg CLV {avg:+.2f}, Win% {wp}%, P/L {pl:+.1f}u')
                any_boost = True
    if not any_boost:
        lines.append('- (none at current thresholds)')
    lines.append("")
    lines.append("**🚩 Gate candidates** — avg CLV ≤ -0.3, n ≥ 20:\n")
    any_gate = False
    for col, label in dimensions:
        for val, n, pos, neg, avg, wp, pl in cohort_stats(conn, col, label, since_date, min_n):
            if n >= STRONG_N and avg <= GATE_CLV:
                lines.append(f'- `{label} = {val}`  n={n}, avg CLV {avg:+.2f}, Win% {wp}%, P/L {pl:+.1f}u')
                any_gate = True
    if not any_gate:
        lines.append('- (none at current thresholds)')
    lines.append("")
    lines.append("> Candidates are **proposals only**. Backtest before shipping any gate or size change.\n")

    report = '\n'.join(lines)
    if out_path:
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(report)
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--days', type=int, default=None,
                   help='Restrict to last N days (default: since rebuild {})'.format(REBUILD_DATE))
    p.add_argument('--min-n', type=int, default=MIN_COHORT,
                   help=f'Minimum cohort size (default {MIN_COHORT})')
    p.add_argument('--out', default=DEFAULT_OUT,
                   help='Output markdown path (default data/clv_patterns_report.md). Use "-" for stdout only.')
    args = p.parse_args()

    if args.days:
        since = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    else:
        since = REBUILD_DATE

    out_path = None if args.out == '-' else args.out
    conn = sqlite3.connect(DB)
    report = write_report(conn, since, args.min_n, out_path)
    conn.close()
    print(report)
    if out_path:
        print(f'\n[report written to {out_path}]')


if __name__ == '__main__':
    main()
