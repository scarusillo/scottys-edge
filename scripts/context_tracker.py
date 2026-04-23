"""Live-vs-backtest tracker for Context Model channels.

Reports daily live performance by sport + channel against the Phase A
30-day backtest expectations shipped today (2026-04-21). Used by
agent_analyst.py to flag channels drifting from backtest pace.

Usage:
    from context_tracker import report
    lines = report(conn)
    for line in lines: print(line)

Or CLI:
    python scripts/context_tracker.py
"""
import os, sqlite3
from datetime import datetime

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
CONTEXT_GO_LIVE = '2026-04-21'  # date v25.44-v25.49 shipped

# Backtest baselines: (channel, sport, side_type, backtest_wr, backtest_picks, backtest_pnl)
# From 30-day Phase A results documented in v25.44/46/47/48/49 commits.
BACKTEST_BASELINES = [
    # Context CONTEXT_STANDALONE SPREADS
    ('Context Spread',  'icehockey_nhl',          'DATA_SPREAD',  0.572, 159,  73.6),
    ('Context Spread',  'basketball_nba',         'DATA_SPREAD',  0.557,  79,  25.0),
    ('Context Spread',  'soccer_italy_serie_a',   'DATA_SPREAD',  0.667,  12,  12.3),
    # Context CONTEXT_STANDALONE TOTALS
    ('Context Total',   'basketball_nba',         'DATA_TOTAL',   0.587, 173,  97.4),
    ('Context Total',   'icehockey_nhl',          'DATA_TOTAL',   0.606,  95,  52.0),
    ('Context Total',   'baseball_mlb',           'DATA_TOTAL',   0.568,  77,  24.6),
    ('Context Total',   'soccer_usa_mls',         'DATA_TOTAL',   0.667,  15,  14.7),
    ('Context Total',   'soccer_spain_la_liga',   'DATA_TOTAL',   0.800,   5,  12.0),
    ('Context Total',   'soccer_germany_bundesliga','DATA_TOTAL', 0.750,   4,   4.7),
    ('Context Total',   'soccer_france_ligue_one', 'DATA_TOTAL',  0.750,   4,   5.6),
]


def fetch_live(conn, side_type, sport):
    """Return (w, l, p, pnl, pending_count) for a channel since go-live."""
    r = conn.execute("""
        SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
               SUM(CASE WHEN result='PUSH' THEN 1 ELSE 0 END),
               COALESCE(SUM(pnl_units), 0)
        FROM graded_bets
        WHERE side_type=? AND sport=?
          AND DATE(created_at) >= ?
          AND result IN ('WIN','LOSS','PUSH')
    """, (side_type, sport, CONTEXT_GO_LIVE)).fetchone()
    w, l, p, pnl = r if r else (0, 0, 0, 0)
    # Pending
    pend = conn.execute("""
        SELECT COUNT(*) FROM bets WHERE side_type=? AND sport=?
          AND DATE(created_at) >= ? AND result IS NULL
    """, (side_type, sport, CONTEXT_GO_LIVE)).fetchone()
    return (w or 0, l or 0, p or 0, float(pnl or 0), pend[0] if pend else 0)


def report(conn):
    """Generate per-channel live vs backtest report lines."""
    lines = []
    lines.append('═' * 72)
    lines.append(f'CONTEXT MODEL — LIVE vs BACKTEST (since {CONTEXT_GO_LIVE})')
    lines.append('═' * 72)
    lines.append(f"  {'Channel':<14s} {'Sport':<22s}  {'LIVE':>10s}  {'LIVE WR':>8s}  {'BT WR':>7s}  {'LIVE P/L':>8s}  STATUS")
    lines.append('─' * 72)

    days_live = max(1, (datetime.now().date() - datetime.fromisoformat(CONTEXT_GO_LIVE).date()).days)

    for (channel, sport, side_type, bt_wr, bt_n, bt_pnl) in BACKTEST_BASELINES:
        w, l, p, pnl, pending = fetch_live(conn, side_type, sport)
        n = w + l
        live_wr = w / n if n else 0
        bt_pace_per_day = bt_pnl / 30  # backtest pace per day
        expected_pnl = bt_pace_per_day * days_live

        # Status logic
        if n == 0 and pending == 0:
            status = 'no fires yet'
        elif n + pending < 5:
            status = f'early (n={n}, {pending} pend)'
        elif n < 15:
            status = f'building (n={n})'
        else:
            # Compare WR — flag if >5% below backtest
            wr_gap = live_wr - bt_wr
            if wr_gap < -0.05:
                status = f'⚠ BELOW backtest by {abs(wr_gap)*100:.1f}%'
            elif wr_gap > 0.05:
                status = f'↑ above backtest by {wr_gap*100:.1f}%'
            else:
                status = 'on pace'

        live_str = f'{w}-{l}-{p}' if (w + l + p) > 0 else '—'
        wr_str = f'{live_wr*100:.1f}%' if n else '—'
        pnl_str = f'{pnl:+.2f}u' if (w + l) else '—'
        sport_short = sport.replace('basketball_', '').replace('icehockey_', '').replace('baseball_', '').replace('soccer_', '')
        lines.append(f"  {channel:<14s} {sport_short:<22s}  {live_str:>10s}  {wr_str:>8s}  {bt_wr*100:>6.1f}%  {pnl_str:>8s}  {status}")

    lines.append('─' * 72)
    lines.append(f'  Backtest baseline total: ~+360u/30d  ·  Days live: {days_live}')
    lines.append(f'  Expected P/L to-date if backtest holds: ~+{360/30*days_live:.0f}u')

    # Actual live total
    total_live_pnl = 0
    total_live_n = 0
    for (_, sport, st, _, _, _) in BACKTEST_BASELINES:
        w, l, p, pnl, _ = fetch_live(conn, st, sport)
        total_live_pnl += pnl
        total_live_n += w + l
    lines.append(f'  Actual live P/L to-date:              {total_live_pnl:+.2f}u on {total_live_n} graded')
    lines.append('═' * 72)
    return lines


if __name__ == '__main__':
    conn = sqlite3.connect(DB)
    for line in report(conn):
        print(line)
