"""
Gate observability — per-gate fired/blocked counters per day.

Closes the "didn't-fire" blind spot: we know which picks were blocked
(`shadow_blocked_picks`) and which fired (`bets`), but didn't have an aggregate
view of gate activity. This module produces:

  - gate_block_summary(): per-gate block volume by day, last N days
  - daily_health_card(): single-day rollup of fires + blocks

Both functions return formatted text suitable for stdout, logs, or briefing
inclusion. Uses the typed `reason_category` column populated by v25.71.

Shipped as part of v25.89 alongside CLV split columns.
"""
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict


def gate_block_summary(conn, days=7):
    """Per-gate block volume by day for the last N days.

    Returns:
        str — formatted markdown-style table.
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    # Total blocks by gate (sorted desc)
    totals = conn.execute("""
        SELECT reason_category, COUNT(*) n, COUNT(DISTINCT event_id) distinct_events
        FROM shadow_blocked_picks
        WHERE DATE(created_at) >= ?
          AND reason_category IS NOT NULL
        GROUP BY reason_category
        ORDER BY n DESC
    """, (cutoff,)).fetchall()

    if not totals:
        return f"No gate blocks logged in last {days} days."

    # Day-by-day per gate (top 10 gates by volume only — keeps output readable)
    top_gates = [r[0] for r in totals[:10]]
    daily = defaultdict(lambda: defaultdict(int))
    for gate in top_gates:
        rows = conn.execute("""
            SELECT DATE(created_at) d, COUNT(*) n
            FROM shadow_blocked_picks
            WHERE DATE(created_at) >= ?
              AND reason_category = ?
            GROUP BY d
        """, (cutoff, gate)).fetchall()
        for d, n in rows:
            daily[gate][d] = n

    # Get day list
    days_list = sorted({d for gate_days in daily.values() for d in gate_days})[-days:]

    out = []
    out.append(f"### Gate Block Summary (last {days} days)\n")
    out.append(f"**Total blocks logged: {sum(r[1] for r in totals)}** across {len(totals)} gate types\n")
    out.append("")

    # Top-line totals table
    out.append("| Gate | Total | Distinct events | Avg/day |")
    out.append("|---|---|---|---|")
    for gate, n, distinct in totals[:15]:
        avg = n / max(days, 1)
        out.append(f"| `{gate}` | {n} | {distinct} | {avg:.1f} |")
    out.append("")

    # Daily breakdown for top 10 (transposed: rows=gate, cols=days)
    if len(days_list) > 1:
        out.append(f"### Daily breakdown — top 10 gates")
        out.append("")
        header = "| Gate | " + " | ".join(d[5:] for d in days_list) + " |"
        out.append(header)
        out.append("|" + "|".join("---" for _ in range(len(days_list) + 1)) + "|")
        for gate in top_gates:
            cells = [str(daily[gate].get(d, 0)) if daily[gate].get(d, 0) > 0 else "·" for d in days_list]
            out.append(f"| `{gate[:30]}` | " + " | ".join(cells) + " |")
        out.append("")

    return "\n".join(out)


def daily_health_card(conn, date=None):
    """Integrated single-day view: fires by sport + blocks by gate.

    Args:
        date: 'YYYY-MM-DD' string. Defaults to yesterday-ET (when grading runs).
    Returns:
        str — formatted markdown.
    """
    if date is None:
        date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Fires by sport (units >= 3.5)
    fires = conn.execute("""
        SELECT sport, COUNT(*) n, ROUND(SUM(units), 1) units, ROUND(SUM(pnl_units), 2) pnl
        FROM graded_bets
        WHERE DATE(created_at) = ?
          AND units >= 3.5
          AND result IN ('WIN','LOSS','PUSH')
        GROUP BY sport ORDER BY n DESC
    """, (date,)).fetchall()

    # Pre-graded fires (today's pending)
    pending = conn.execute("""
        SELECT sport, COUNT(*) n, ROUND(SUM(units), 1) units
        FROM bets
        WHERE DATE(created_at) = ?
          AND units >= 3.5
          AND (result IS NULL OR result NOT IN ('TAINTED','DUPLICATE'))
          AND id NOT IN (SELECT bet_id FROM graded_bets WHERE bet_id IS NOT NULL)
        GROUP BY sport ORDER BY n DESC
    """, (date,)).fetchall()

    # Blocks by gate
    blocks = conn.execute("""
        SELECT reason_category, COUNT(*) total, COUNT(DISTINCT event_id) distinct_events
        FROM shadow_blocked_picks
        WHERE DATE(created_at) = ?
          AND reason_category IS NOT NULL
        GROUP BY reason_category ORDER BY total DESC
    """, (date,)).fetchall()

    # Blocks by sport
    blocks_by_sport = conn.execute("""
        SELECT sport, COUNT(DISTINCT event_id) distinct_events, COUNT(*) total
        FROM shadow_blocked_picks
        WHERE DATE(created_at) = ?
        GROUP BY sport ORDER BY distinct_events DESC
    """, (date,)).fetchall()

    out = []
    out.append(f"## Gate Health Card — {date}\n")

    if fires:
        out.append("### Fires (graded)")
        out.append("| Sport | n | Units | P/L |")
        out.append("|---|---|---|---|")
        for sport, n, units, pnl in fires:
            out.append(f"| {sport} | {n} | {units}u | {pnl:+.2f}u |")
        total_n = sum(r[1] for r in fires)
        total_units = sum(r[2] for r in fires)
        total_pnl = sum(r[3] for r in fires)
        out.append(f"| **TOTAL** | **{total_n}** | **{total_units}u** | **{total_pnl:+.2f}u** |")
        out.append("")
    else:
        out.append("_No fires graded for this date._\n")

    if pending:
        out.append("### Fires (pending grade)")
        out.append("| Sport | n | Units |")
        out.append("|---|---|---|")
        for sport, n, units in pending:
            out.append(f"| {sport} | {n} | {units}u |")
        out.append("")

    if blocks_by_sport:
        out.append("### Block volume by sport")
        out.append("| Sport | Distinct events | Total log entries |")
        out.append("|---|---|---|")
        for sport, distinct, total in blocks_by_sport[:12]:
            out.append(f"| {sport} | {distinct} | {total} |")
        out.append("")

    if blocks:
        out.append("### Blocks by gate")
        out.append("| Gate | Total log entries | Distinct events |")
        out.append("|---|---|---|")
        for gate, total, distinct in blocks[:15]:
            out.append(f"| `{gate}` | {total} | {distinct} |")
        if len(blocks) > 15:
            out.append(f"| _+{len(blocks)-15} more gates with smaller volume_ | | |")
        out.append("")

    # Pass-rate summary — what fraction of evaluated events fired vs blocked?
    # Approximation: distinct events fired + distinct events blocked = approx eval universe
    total_fires_n = sum(r[1] for r in fires) + sum(r[1] for r in pending)
    distinct_blocked_events = conn.execute("""
        SELECT COUNT(DISTINCT event_id) FROM shadow_blocked_picks WHERE DATE(created_at) = ?
    """, (date,)).fetchone()[0] or 0
    eval_universe = total_fires_n + distinct_blocked_events
    if eval_universe > 0:
        pass_rate = 100 * total_fires_n / eval_universe
        out.append(f"**Approx pass rate:** {total_fires_n} fires / ({total_fires_n} fires + {distinct_blocked_events} distinct-blocked events) = **{pass_rate:.1f}%**")
        out.append("")
        out.append("_Note: pass-rate is approximate — same event can be evaluated multiple times across the hourly pipeline; this counts distinct event_id only._")

    return "\n".join(out)


def write_daily_card(conn, date=None, out_path=None):
    """Write the daily health card to a markdown file. Default path: data/gate_health.md."""
    import os
    if out_path is None:
        out_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'gate_health.md')
    text = daily_health_card(conn, date)
    text += "\n\n---\n\n"
    text += gate_block_summary(conn, days=7)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(text)
    return out_path


if __name__ == '__main__':
    import sys
    db_path = 'data/betting_model.db'
    conn = sqlite3.connect(db_path)
    if len(sys.argv) > 1 and sys.argv[1] == '--write':
        path = write_daily_card(conn)
        print(f"Wrote: {path}")
    else:
        print(daily_health_card(conn))
        print()
        print(gate_block_summary(conn, days=7))
