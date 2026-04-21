"""Generate comprehensive engine-performance dashboard as static HTML.

v25.53 expansion — covers every pick engine and every gate/veto, with
per-engine recent-pick drill-downs so regressions are easy to spot.

Writes to docs/engine_dashboard.html — linked from index.html nav.
"""
import os, sqlite3
from datetime import datetime

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
OUT = os.path.join(os.path.dirname(__file__), '..', 'docs', 'engine_dashboard.html')


# Engine definitions: (display_name, side_type_filter, context_filter_fallback)
# Edge-based channels are identified by a lack of side_type matching our known
# non-edge side_types (everything that ISN'T fade/Context/arb is "Elo edge").
ENGINES = [
    # ── Edge / Elo-based (the original model) ──
    ('Elo Edge — SPREAD',            "market_type='SPREAD' AND side_type IN ('FAVORITE','DOG','SPREAD') OR (market_type='SPREAD' AND side_type IS NULL)"),
    ('Elo Edge — TOTAL',             "market_type='TOTAL' AND side_type IN ('OVER','UNDER')"),
    ('Elo Edge — MONEYLINE',         "market_type='MONEYLINE' OR market_type='ML'"),
    # ── Context Model ──
    ('Context — DATA_SPREAD (P1+P2)', "side_type='DATA_SPREAD'"),
    ('Context — DATA_TOTAL',          "side_type='DATA_TOTAL'"),
    # ── Fade flips ──
    ('SPREAD_FADE_FLIP',             "side_type='SPREAD_FADE_FLIP' OR side_type='FADE_FLIP'"),
    ('PROP_FADE_FLIP',               "side_type='PROP_FADE_FLIP'"),
    # ── Book arbitrage ──
    ('BOOK_ARB — game lines',        "side_type='BOOK_ARB'"),
    ('PROP_BOOK_ARB',                "side_type='PROP_BOOK_ARB'"),
    # ── Props (edge-based) ──
    ('Prop — PROP_OVER (edge)',      "side_type='PROP_OVER'"),
    ('Prop — PROP_UNDER (edge)',     "side_type='PROP_UNDER'"),
    # ── Catch-all for anything not tagged ──
    ('Prop — other (untagged)',      "market_type='PROP' AND side_type NOT IN ('PROP_OVER','PROP_UNDER','PROP_FADE_FLIP','PROP_BOOK_ARB') AND side_type IS NOT NULL"),
]

# Gate definitions: (display_name, shadow_blocked_picks reason pattern)
GATES = [
    # Context Model gates
    ('CONTEXT_DIRECTION_VETO (v25.52)',  "reason LIKE 'CONTEXT_DIRECTION_VETO%'"),
    # BOOK_ARB gates
    ('BOOK_ARB_LINE_UNSETTLED (v25.42)', "reason LIKE 'BOOK_ARB_LINE_UNSETTLED%'"),
    ('BOOK_ARB_SHARP_OPPOSES_VETO',      "reason LIKE 'BOOK_ARB_SHARP_OPPOSES_VETO%'"),
    # Direction / CLV
    ('SHARP_OPPOSES_BLOCK (v25.35)',     "reason LIKE 'SHARP_OPPOSES_BLOCK%'"),
    ('CLV_BLOCK',                         "reason LIKE 'CLV_BLOCK%' OR reason LIKE 'CLV BLOCK%'"),
    # NCAA DK gates
    ('NCAA_DK_TIGHT_SKIP (v25.22)',      "reason LIKE 'NCAA_DK_TIGHT_SKIP%'"),
    ('NCAA_DK_FADE_FLIP (v25.23)',       "reason LIKE 'NCAA_DK_FADE_FLIP%'"),
    ('NCAA_NO_SHARP_SKIP (v25.24)',      "reason LIKE 'NCAA_NO_SHARP_SKIP%'"),
    ('NCAA_DK_SHARP_VETO',               "reason LIKE 'NCAA_DK_SHARP_VETO%'"),
    ('NCAA_ERA_RELIABILITY_GATE (v25.32)',"reason LIKE 'NCAA_ERA_RELIABILITY_GATE%'"),
    # MLB/NHL gates
    ('MLB_SIDE_CONVICTION_GATE',         "reason LIKE 'MLB_SIDE_CONVICTION_GATE%'"),
    ('NHL_PACE_OVER_GATE',               "reason LIKE 'NHL_PACE_OVER_GATE%'"),
    ('PACE_GATE',                         "reason LIKE 'PACE_GATE%'"),
    ('PITCHING_GATE',                     "reason LIKE 'PITCHING_GATE%'"),
    ('PARK_GATE',                         "reason LIKE 'PARK_GATE%'"),
    # Prop gates
    ('PROP_DIVERGENCE_GATE (v25.30)',    "reason LIKE 'PROP_DIVERGENCE_GATE%'"),
    ('PROP_BOOK_ARB_SHADOW (v25.37)',    "reason LIKE 'PROP_BOOK_ARB_SHADOW%'"),
    ('PROP_BOOK_ARB_VOLUME_CAP',         "reason LIKE 'PROP_BOOK_ARB_VOLUME_CAP%'"),
    ('BLOWOUT_GATE',                      "reason LIKE 'BLOWOUT_GATE%'"),
    # Concentration / Caps
    ('DIRECTION_CAP',                     "reason LIKE 'DIRECTION_CAP%'"),
    ('GAME_CAP (concentration)',          "reason LIKE 'GAME_CAP%'"),
    ('SHARP_CAP',                         "reason LIKE 'SHARP_CAP%'"),
    ('TOTAL_SOFT_CAP',                    "reason LIKE 'TOTAL_SOFT_CAP%'"),
    ('CROSS_RUN_CAP',                     "reason LIKE 'CROSS_RUN_CAP%'"),
    # Other
    ('NCAA_MIDWEEK_SHADOW (v25.43)',     "reason LIKE 'NCAA_MIDWEEK%'"),
    ('DIVERGENCE_GATE (general)',         "reason LIKE 'DIVERGENCE_GATE%'"),
]


def fetch_engine_stats(conn):
    rows = []
    for name, pred in ENGINES:
        q = f"""
          SELECT
            SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) w,
            SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) l,
            SUM(CASE WHEN result='PUSH' THEN 1 ELSE 0 END) p,
            SUM(pnl_units) pnl,
            SUM(CASE WHEN result IN ('WIN','LOSS','PUSH') THEN 1 ELSE 0 END) n_graded,
            SUM(CASE WHEN result='TAINTED' THEN 1 ELSE 0 END) n_tainted
          FROM graded_bets WHERE ({pred})
        """
        try:
            r = conn.execute(q).fetchone()
            w, l, p, pnl, n_graded, n_tainted = r
        except Exception:
            w = l = p = pnl = n_graded = n_tainted = 0
        # Pending (in bets, not graded yet)
        try:
            pend = conn.execute(
                f"SELECT COUNT(*) FROM bets WHERE ({pred}) AND result IS NULL"
            ).fetchone()
            n_pending = pend[0] if pend else 0
        except Exception:
            n_pending = 0
        rows.append({
            'name': name, 'w': w or 0, 'l': l or 0, 'p': p or 0,
            'pnl': float(pnl or 0), 'n_graded': n_graded or 0,
            'n_pending': n_pending, 'n_tainted': n_tainted or 0,
            'pred': pred,
        })
    return rows


def fetch_recent_picks_per_engine(conn, pred, limit=10):
    """Return a list of recent picks for an engine."""
    try:
        rows = conn.execute(f"""
            SELECT DATE(created_at) as d, sport, selection, line, odds, units, result, pnl_units
            FROM graded_bets
            WHERE ({pred})
            ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return rows
    except Exception:
        return []


def fetch_gate_stats(conn):
    rows = []
    for name, pred in GATES:
        try:
            r = conn.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT DATE(created_at)) FROM shadow_blocked_picks WHERE {pred}"
            ).fetchone()
            total, days = r if r else (0, 0)
        except Exception:
            total = days = 0
        # Recent block sample
        try:
            recent = conn.execute(
                f"SELECT DATE(created_at), sport, selection, reason FROM shadow_blocked_picks WHERE {pred} ORDER BY created_at DESC LIMIT 3"
            ).fetchall()
        except Exception:
            recent = []
        rows.append({
            'name': name, 'total': total or 0, 'days': days or 0,
            'recent': recent,
        })
    return rows


def fetch_by_sport(conn):
    q = """
      SELECT sport,
             SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) w,
             SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) l,
             SUM(CASE WHEN result='PUSH' THEN 1 ELSE 0 END) p,
             SUM(pnl_units) pnl,
             COUNT(*) n
      FROM graded_bets
      WHERE DATE(created_at) >= '2026-04-01'
        AND result IN ('WIN','LOSS','PUSH')
      GROUP BY sport
      ORDER BY pnl DESC
    """
    return [{'sport': r[0], 'w': r[1] or 0, 'l': r[2] or 0, 'p': r[3] or 0,
             'pnl': float(r[4] or 0), 'n': r[5]} for r in conn.execute(q)]


def _esc(s):
    """Minimal HTML escape."""
    if s is None: return ''
    return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')


def build_html(engines, gates, by_sport, recent_by_engine):
    now = datetime.now().strftime('%Y-%m-%d %I:%M %p ET')

    # Engines table
    engine_rows = []
    for e in engines:
        n = e['w'] + e['l']
        wr = (e['w'] / n * 100) if n else 0
        pnl_cls = 'pos' if e['pnl'] > 0 else ('neg' if e['pnl'] < 0 else '')
        pend_tag = f" <span class='pend'>({e['n_pending']} live)</span>" if e['n_pending'] else ''
        tainted_tag = f" <span class='taint'>({e['n_tainted']} scrubbed)</span>" if e['n_tainted'] else ''
        engine_rows.append(
            f"<tr><td><strong>{_esc(e['name'])}</strong>{pend_tag}{tainted_tag}</td>"
            f"<td>{e['n_graded']} graded</td>"
            f"<td>{e['w']}-{e['l']}-{e['p']}</td>"
            f"<td>{wr:.1f}%</td>"
            f"<td class='{pnl_cls}'>{e['pnl']:+.2f}u</td></tr>"
        )
    engine_table = '\n'.join(engine_rows)

    # Per-engine recent picks drill-down
    drill_sections = []
    for e in engines:
        recent = recent_by_engine.get(e['name'], [])
        if not recent:
            continue
        pick_rows = []
        for r in recent:
            d, sport, sel, line, odds, units, result, pnl = r
            rcls = 'pos' if result == 'WIN' else ('neg' if result == 'LOSS' else ('taint' if result == 'TAINTED' else ''))
            pick_rows.append(
                f"<tr><td>{_esc(d)}</td>"
                f"<td>{_esc(sport)}</td>"
                f"<td>{_esc((sel or '')[:55])}</td>"
                f"<td>{_esc(line)}</td>"
                f"<td>{_esc(odds)}</td>"
                f"<td>{_esc(units)}u</td>"
                f"<td class='{rcls}'>{_esc(result or 'PENDING')}</td>"
                f"<td class='{rcls}'>{(pnl or 0):+.2f}u</td></tr>"
            )
        drill_sections.append(
            f"<details><summary><strong>{_esc(e['name'])}</strong> — last {len(recent)} picks</summary>"
            f"<table class='mini'><tr><th>Date</th><th>Sport</th><th>Selection</th><th>Line</th><th>Odds</th><th>Units</th><th>Result</th><th>P/L</th></tr>"
            + '\n'.join(pick_rows) + "</table></details>"
        )
    drill_down_html = '\n'.join(drill_sections) if drill_sections else '<p class="note">No picks yet.</p>'

    # Gates table
    gate_rows = []
    for g in gates:
        if g['total'] == 0: continue  # skip unused
        sample = ''
        if g['recent']:
            first = g['recent'][0]
            sample = f"<br><span class='subtle'>e.g. {_esc((first[2] or '')[:40])}</span>"
        gate_rows.append(
            f"<tr><td><strong>{_esc(g['name'])}</strong>{sample}</td>"
            f"<td>{g['total']} blocks</td>"
            f"<td>{g['days']} days</td></tr>"
        )
    gate_table = '\n'.join(gate_rows) if gate_rows else "<tr><td colspan='3' class='subtle'>No blocks yet</td></tr>"

    # By sport
    sport_rows = []
    for s in by_sport:
        pnl_cls = 'pos' if s['pnl'] > 0 else ('neg' if s['pnl'] < 0 else '')
        sport_rows.append(
            f"<tr><td>{_esc(s['sport'])}</td><td>{s['n']}</td>"
            f"<td>{s['w']}-{s['l']}-{s['p']}</td>"
            f"<td class='{pnl_cls}'>{s['pnl']:+.2f}u</td></tr>"
        )
    sport_table = '\n'.join(sport_rows)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Scotty's Edge | Engine Dashboard</title>
<meta name="theme-color" content="#0d1117">
<style>
*, *::before, *::after {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: -apple-system, Segoe UI, Arial, sans-serif; background:#0d1117; color:#fff; padding:32px 16px; line-height:1.6; max-width:1100px; margin:0 auto; }}
h1 {{ font-size:2rem; margin-bottom:4px; color:#00e676; }}
.ts {{ color:#999; font-size:0.9rem; margin-bottom:32px; }}
h2 {{ font-size:1.3rem; margin:28px 0 12px; color:#5b8fd0; border-bottom:1px solid #333; padding-bottom:6px; }}
h3 {{ font-size:1.0rem; margin:16px 0 8px; color:#9ac; }}
table {{ width:100%; border-collapse:collapse; font-size:0.95rem; background:#141e2a; border-radius:8px; overflow:hidden; margin-bottom:16px; }}
table.mini {{ font-size:0.85rem; }}
th, td {{ padding:10px 14px; text-align:left; border-bottom:1px solid #222; }}
table.mini th, table.mini td {{ padding:6px 10px; }}
th {{ background:#1a2636; color:#999; font-weight:600; text-transform:uppercase; font-size:0.8rem; letter-spacing:1px; }}
tr:last-child td {{ border-bottom:none; }}
.pos {{ color:#00e676; font-weight:600; }}
.neg {{ color:#ff5252; font-weight:600; }}
.taint {{ color:#f39c12; }}
.pend {{ color:#9ac; font-size:0.85em; }}
.subtle {{ color:#888; font-size:0.8em; }}
.note {{ color:#999; font-size:0.85rem; font-style:italic; margin:8px 0; padding-left:12px; border-left:2px solid #333; }}
.nav {{ margin-bottom:20px; font-size:0.9rem; }}
.nav a {{ color:#00e676; text-decoration:none; margin-right:16px; }}
details {{ background:#141e2a; padding:10px 14px; margin-bottom:8px; border-radius:6px; border:1px solid #222; }}
details summary {{ cursor:pointer; padding:6px 0; color:#ddd; }}
details[open] summary {{ margin-bottom:10px; border-bottom:1px solid #222; }}
</style></head><body>
<div class="nav"><a href="index.html">← Home</a><a href="dashboard.html">Dashboard</a><a href="methodology.html">Methodology</a><a href="changelog.html">Changelog</a></div>
<h1>Engine Dashboard</h1>
<div class="ts">Generated {now} · Per-channel performance + per-pick drill-down</div>

<h2>Pick Engines — live W/L + P/L</h2>
<table><tr><th>Engine</th><th>Sample</th><th>W-L-P</th><th>Win%</th><th>P/L</th></tr>
{engine_table}
</table>
<p class="note">Monitoring thresholds: Context channels pull at &lt;52-55% after 15-20 picks. Edge-based channels reviewed monthly. Scrubbed counts = TAINTED bets (manually removed).</p>

<h2>Recent picks per engine</h2>
{drill_down_html}

<h2>Gates — blocks logged to shadow_blocked_picks</h2>
<table><tr><th>Gate</th><th>Lifetime</th><th>Active days</th></tr>
{gate_table}
</table>
<p class="note">Blocks = picks the model would have fired but a gate suppressed. Counterfactual grading lives in the morning briefing.</p>

<h2>By sport (post-Apr-1)</h2>
<table><tr><th>Sport</th><th>N</th><th>W-L-P</th><th>P/L</th></tr>
{sport_table}
</table>

</body></html>"""


def main():
    conn = sqlite3.connect(DB)
    engines = fetch_engine_stats(conn)
    gates = fetch_gate_stats(conn)
    by_sport = fetch_by_sport(conn)
    # Per-engine recent picks
    recent_by_engine = {}
    for e in engines:
        recent_by_engine[e['name']] = fetch_recent_picks_per_engine(conn, e['pred'], limit=10)
    html = build_html(engines, gates, by_sport, recent_by_engine)
    with open(OUT, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'Wrote {OUT}')
    print(f'  {len(engines)} engines, {len(gates)} gates, {len(by_sport)} sports')


if __name__ == '__main__':
    main()
