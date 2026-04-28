"""Backtest v25.93 PROP_CAREER_FADE recency veto.

For every historical CAREER_FADE pick (including TAINTED), reconstruct the
last-10-game pts/reb/ast average that was available at pick time (games
strictly before the pick's commence date), and compare to the market line.
The v25.93 veto blocks the fade when recent_avg > market_median.
"""
import sqlite3, re, sys
from pathlib import Path

DB = Path(__file__).parent.parent / 'data' / 'betting_model.db'
conn = sqlite3.connect(DB)
cur = conn.cursor()

cur.execute("""
SELECT bet_id, created_at, sport, event_id, selection, line, odds, units,
       result, pnl_units, context_factors
FROM graded_bets
WHERE context_factors LIKE '%PROP_CAREER_FADE%'
   OR side_type = 'PROP_CAREER_FADE'
ORDER BY created_at
""")
rows = cur.fetchall()

print(f"{'bet_id':>7} {'date':10} {'player':22} {'stat':6} {'line':>5} {'L10':>6} {'mkt':>5} "
      f"{'veto?':6} {'result':8} {'pnl':>6}")
print('-' * 95)

veto_count = 0
saved_pnl = 0.0
keep_pnl = 0.0

for r in rows:
    bet_id, created_at, sport, event_id, selection, line, odds, units, result, pnl, ctx = r
    m = re.match(r'(.+?) UNDER ([\d.]+) (\w+)', selection)
    if not m:
        continue
    player, line_s, stat = m.group(1), float(m.group(2)), m.group(3)

    # Pull market_median from context_factors string (logged at fire time)
    mm = re.search(r'market median ([\d.]+)', ctx)
    market_med = float(mm.group(1)) if mm else line

    # Last-10 box score values strictly before the pick date
    pick_date = created_at[:10]
    cur.execute("""
        SELECT stat_value FROM box_scores
        WHERE player=? AND sport=? AND stat_type=?
          AND game_date < ?
        ORDER BY game_date DESC LIMIT 10
    """, (player, sport, stat, pick_date))
    vals = [v[0] for v in cur.fetchall()]
    recent_avg = sum(vals) / len(vals) if len(vals) >= 8 else None

    veto = recent_avg is not None and recent_avg > market_med
    veto_str = 'VETO' if veto else '-'
    recent_str = f'{recent_avg:.2f}' if recent_avg is not None else 'n/a'

    print(f"{bet_id:>7} {pick_date} {player[:22]:22} {stat:6} {line:>5} "
          f"{recent_str:>6} {market_med:>5} {veto_str:>6} {result:8} {pnl:>6.2f}")

    # If veto would have blocked: pnl saved when result was LOSS, lost when WIN.
    # TAINTED rows had 0 pnl regardless, so they don't move totals either way.
    if veto:
        veto_count += 1
        saved_pnl += -pnl  # blocking a loss saves +5; blocking a win loses 4.17
    else:
        keep_pnl += pnl

print('-' * 95)
print(f'Total picks:       {len(rows)}')
print(f'Veto blocks:       {veto_count}')
print(f'Net effect of veto on graded P/L: {saved_pnl:+.2f} u (positive = veto improved record)')
print(f'P/L of picks veto would KEEP:     {keep_pnl:+.2f} u')
