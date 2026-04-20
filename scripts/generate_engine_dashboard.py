"""Generate a lightweight engine-performance dashboard as static HTML.

Shows live W/L + P/L per engine (SPREAD_FADE_FLIP, DATA_SPREAD, BOOK_ARB,
PROP_FADE_FLIP, SHARP_OPPOSES_BLOCK) and overall by-sport totals so
regressions are easy to spot at a glance.

Writes to docs/engine_dashboard.html — can be linked from index or
navigated directly.
"""
import os, sqlite3
from datetime import datetime

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
OUT = os.path.join(os.path.dirname(__file__), '..', 'docs', 'engine_dashboard.html')


def fetch_engine_stats(conn):
    # Engines defined by side_type with fallback to context_factors match
    engines = {
        'SPREAD_FADE_FLIP': "side_type='SPREAD_FADE_FLIP'",
        'DATA_SPREAD (Context Model)': "side_type='DATA_SPREAD'",
        'BOOK_ARB (game lines)': "side_type='BOOK_ARB'",
        'PROP_BOOK_ARB': "side_type='PROP_BOOK_ARB'",
        'PROP_FADE_FLIP': "side_type='PROP_FADE_FLIP'",
    }
    rows = []
    for name, pred in engines.items():
        q = f"""
          SELECT
            SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) w,
            SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) l,
            SUM(CASE WHEN result='PUSH' THEN 1 ELSE 0 END) p,
            SUM(pnl_units) pnl,
            COUNT(*) n_all,
            SUM(CASE WHEN result IN ('WIN','LOSS','PUSH') THEN 1 ELSE 0 END) n_graded,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) n_pending
          FROM graded_bets WHERE {pred}
        """
        r = conn.execute(q).fetchone()
        w, l, p, pnl, n_all, n_graded, n_pending = r
        rows.append({
            'name': name, 'w': w or 0, 'l': l or 0, 'p': p or 0,
            'pnl': float(pnl or 0), 'n_graded': n_graded or 0,
            'n_pending': n_pending or 0,
        })
    # Pending count per engine (unsaved in graded_bets)
    engine_codes = {
        'SPREAD_FADE_FLIP': 'SPREAD_FADE_FLIP',
        'DATA_SPREAD (Context Model)': 'DATA_SPREAD',
        'BOOK_ARB (game lines)': 'BOOK_ARB',
        'PROP_BOOK_ARB': 'PROP_BOOK_ARB',
        'PROP_FADE_FLIP': 'PROP_FADE_FLIP',
    }
    for row in rows:
        code = engine_codes.get(row['name'], row['name'])
        pend = conn.execute(
            "SELECT COUNT(*) FROM bets WHERE side_type = ? AND result IS NULL",
            (code,)
        ).fetchone()
        row['n_pending_bets'] = pend[0] if pend else 0
    return rows


def fetch_shadow_gate_stats(conn):
    # SHARP_OPPOSES_BLOCK counterfactual would require live grading work;
    # show live block volume and note pending counterfactual grading.
    rows = []
    gates = {
        'SHARP_OPPOSES_BLOCK': "reason LIKE 'SHARP_OPPOSES_BLOCK%'",
        'PRA book-arb shadow': "reason LIKE 'PROP_BOOK_ARB_SHADOW%'",
        'NCAA_DK_TIGHT_SKIP': "reason LIKE 'NCAA_DK_TIGHT_SKIP%'",
        'NCAA_DK_FADE_FLIP': "reason LIKE 'NCAA_DK_FADE_FLIP%'",
    }
    for name, pred in gates.items():
        r = conn.execute(f"SELECT COUNT(*) FROM shadow_blocked_picks WHERE {pred}").fetchone()
        rows.append({'name': name, 'blocks': r[0] if r else 0})
    return rows


def fetch_by_sport(conn):
    q = """
      SELECT sport,
             SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) w,
             SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) l,
             SUM(pnl_units) pnl,
             COUNT(*) n
      FROM graded_bets
      WHERE DATE(created_at) >= '2026-04-01'
        AND result IN ('WIN','LOSS','PUSH')
      GROUP BY sport
      ORDER BY pnl DESC
    """
    return [{'sport': r[0], 'w': r[1] or 0, 'l': r[2] or 0, 'pnl': float(r[3] or 0), 'n': r[4]} for r in conn.execute(q)]


def build_html(engines, gates, by_sport):
    now = datetime.now().strftime('%Y-%m-%d %I:%M %p ET')
    # Top-of-file engine block
    engine_rows = []
    for e in engines:
        n = e['w'] + e['l']
        wr = (e['w'] / n * 100) if n else 0
        pnl_cls = 'pos' if e['pnl'] > 0 else ('neg' if e['pnl'] < 0 else '')
        engine_rows.append(
            f"<tr><td><strong>{e['name']}</strong></td>"
            f"<td>{e['n_graded']} graded</td>"
            f"<td>{e['w']}-{e['l']}-{e['p']}</td>"
            f"<td>{wr:.1f}%</td>"
            f"<td class='{pnl_cls}'>{e['pnl']:+.2f}u</td></tr>"
        )
    engine_table = '\n'.join(engine_rows)

    # Gates block
    gate_rows = []
    for g in gates:
        gate_rows.append(f"<tr><td><strong>{g['name']}</strong></td><td>{g['blocks']} blocks</td></tr>")
    gate_table = '\n'.join(gate_rows)

    # By sport
    sport_rows = []
    for s in by_sport:
        pnl_cls = 'pos' if s['pnl'] > 0 else ('neg' if s['pnl'] < 0 else '')
        sport_rows.append(
            f"<tr><td>{s['sport']}</td><td>{s['n']}</td>"
            f"<td>{s['w']}-{s['l']}</td>"
            f"<td class='{pnl_cls}'>{s['pnl']:+.2f}u</td></tr>"
        )
    sport_table = '\n'.join(sport_rows)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Scotty's Edge | Engine Dashboard</title>
<meta name="theme-color" content="#0d1117">
<style>
*, *::before, *::after {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: -apple-system, Segoe UI, Arial, sans-serif; background:#0d1117; color:#fff; padding:32px 16px; line-height:1.6; max-width:960px; margin:0 auto; }}
h1 {{ font-size:2rem; margin-bottom:4px; color:#00e676; }}
.ts {{ color:#999; font-size:0.9rem; margin-bottom:32px; }}
h2 {{ font-size:1.3rem; margin:28px 0 12px; color:#5b8fd0; border-bottom:1px solid #333; padding-bottom:6px; }}
table {{ width:100%; border-collapse:collapse; font-size:0.95rem; background:#141e2a; border-radius:8px; overflow:hidden; margin-bottom:16px; }}
th, td {{ padding:10px 14px; text-align:left; border-bottom:1px solid #222; }}
th {{ background:#1a2636; color:#999; font-weight:600; text-transform:uppercase; font-size:0.8rem; letter-spacing:1px; }}
tr:last-child td {{ border-bottom:none; }}
.pos {{ color:#00e676; font-weight:600; }}
.neg {{ color:#ff5252; font-weight:600; }}
.note {{ color:#999; font-size:0.85rem; font-style:italic; margin:8px 0; padding-left:12px; border-left:2px solid #333; }}
.nav {{ margin-bottom:20px; font-size:0.9rem; }}
.nav a {{ color:#00e676; text-decoration:none; margin-right:16px; }}
</style></head><body>
<div class="nav"><a href="index.html">← Home</a><a href="dashboard.html">Dashboard</a><a href="methodology.html">Methodology</a><a href="changelog.html">Changelog</a></div>
<h1>Engine Dashboard</h1>
<div class="ts">Generated {now} · Live performance per pick-engine</div>

<h2>Engines — live W/L + P/L</h2>
<table><tr><th>Engine</th><th>Sample</th><th>W-L-P</th><th>Win%</th><th>P/L</th></tr>
{engine_table}
</table>
<p class="note">Pull triggers: SPREAD_FADE_FLIP &lt;52% after 15 picks · DATA_SPREAD &lt;55% after 15 picks · PROP_BOOK_ARB still thin (3 live picks ever)</p>

<h2>Gates — shadow + live block counts</h2>
<table><tr><th>Gate</th><th>Lifetime blocks</th></tr>
{gate_table}
</table>
<p class="note">SHARP_OPPOSES_BLOCK: counterfactual grading runs in the morning briefing. PRA book-arb shadow: promotes to live at n≥15 with ≥55% counterfactual W/L.</p>

<h2>By sport (post-Apr-1)</h2>
<table><tr><th>Sport</th><th>N</th><th>W-L</th><th>P/L</th></tr>
{sport_table}
</table>

</body></html>"""


def main():
    conn = sqlite3.connect(DB)
    engines = fetch_engine_stats(conn)
    gates = fetch_shadow_gate_stats(conn)
    by_sport = fetch_by_sport(conn)
    html = build_html(engines, gates, by_sport)
    with open(OUT, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'Wrote {OUT}')
    print(f'  {len(engines)} engines, {len(gates)} gates, {len(by_sport)} sports')


if __name__ == '__main__':
    main()
