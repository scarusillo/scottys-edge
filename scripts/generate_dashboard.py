"""
generate_dashboard.py — Build static P&L dashboard for GitHub Pages.

Queries betting_model.db and generates docs/dashboard.html with Chart.js charts.
Called automatically by cmd_grade pipeline. No external dependencies beyond sqlite3.
"""
import sqlite3
import os
import json
from datetime import datetime, timedelta

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
OUT = os.path.join(os.path.dirname(__file__), '..', 'docs', 'dashboard.html')


def generate():
    db = sqlite3.connect(DB)

    # ── Equity curve: cumulative P&L by date ──
    rows = db.execute("""
        SELECT DATE(created_at) as dt,
               SUM(pnl_units) as day_pnl,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l
        FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5
        GROUP BY dt ORDER BY dt
    """).fetchall()

    dates, daily_pnl, cum_pnl, daily_w, daily_l = [], [], [], [], []
    running = 0.0
    for dt, pnl, w, l in rows:
        dates.append(dt)
        daily_pnl.append(round(pnl, 1))
        running += pnl
        cum_pnl.append(round(running, 1))
        daily_w.append(w)
        daily_l.append(l)

    # ── Drawdown ──
    peak = 0.0
    drawdown = []
    for c in cum_pnl:
        if c > peak:
            peak = c
        drawdown.append(round(c - peak, 1))

    # ── Weekly P&L ──
    week_rows = db.execute("""
        SELECT strftime('%Y-W%W', created_at) as wk,
               SUM(pnl_units) as pnl,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l
        FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5
        GROUP BY wk ORDER BY wk
    """).fetchall()
    week_labels = [r[0] for r in week_rows]
    week_pnl = [round(r[1], 1) for r in week_rows]
    week_colors = ['rgba(0,230,118,0.8)' if p >= 0 else 'rgba(255,82,82,0.8)' for p in week_pnl]

    # ── Monthly breakdown ──
    month_rows = db.execute("""
        SELECT strftime('%Y-%m', created_at) as mo,
               COUNT(*) as bets,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l,
               ROUND(SUM(pnl_units), 1) as pnl,
               ROUND(AVG(CASE WHEN clv IS NOT NULL THEN clv END), 2) as clv
        FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5
        GROUP BY mo ORDER BY mo
    """).fetchall()

    # ── By sport ──
    sport_rows = db.execute("""
        SELECT sport,
               COUNT(*) as bets,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l,
               ROUND(SUM(pnl_units), 1) as pnl
        FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5
        GROUP BY sport ORDER BY SUM(pnl_units) DESC
    """).fetchall()

    sport_labels = [r[0].replace('baseball_', '').replace('icehockey_', '').replace('basketball_', '').replace('soccer_', '').upper() for r in sport_rows]
    sport_pnl = [r[4] for r in sport_rows]
    sport_colors = ['rgba(0,230,118,0.8)' if p >= 0 else 'rgba(255,82,82,0.8)' for p in sport_pnl]

    # ── By book ──
    book_rows = db.execute("""
        SELECT book,
               COUNT(*) as bets,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as l,
               ROUND(SUM(pnl_units), 1) as pnl
        FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5
        GROUP BY book ORDER BY SUM(pnl_units) DESC
    """).fetchall()

    # ── Season totals ──
    total = db.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
               ROUND(SUM(pnl_units), 1)
        FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5
    """).fetchone()
    total_bets, total_w, total_l, total_pnl = total
    win_pct = round(100 * total_w / (total_w + total_l), 1) if (total_w + total_l) > 0 else 0
    max_dd = min(drawdown) if drawdown else 0

    # ── Bankroll ──
    try:
        import sys
        sys.path.insert(0, os.path.dirname(__file__))
        from config import BANKROLL_START, UNIT_VALUE
    except Exception:
        BANKROLL_START, UNIT_VALUE = 5000, 50
    bankroll = BANKROLL_START + (total_pnl * UNIT_VALUE)

    updated_at = datetime.now().strftime('%Y-%m-%d %I:%M %p ET')

    db.close()

    # ── Build HTML ──
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scotty's Edge | P&L Dashboard</title>
<meta name="theme-color" content="#0d1117">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600;700&family=Bebas+Neue&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*, *::before, *::after {{ margin: 0; padding: 0; box-sizing: border-box; }}
:root {{
  --bg: #0d1117; --bg-card: #141e2a; --green: #00e676; --red: #ff5252;
  --white: #ffffff; --white-80: #cccccc; --white-60: #999999;
}}
body {{ font-family: 'Barlow', sans-serif; background: var(--bg); color: var(--white); padding: 20px; max-width: 1200px; margin: 0 auto; }}
h1 {{ font-family: 'Bebas Neue', sans-serif; font-size: 2.5rem; color: var(--green); margin-bottom: 5px; }}
.subtitle {{ color: var(--white-60); margin-bottom: 30px; font-size: 0.9rem; }}
.stats-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 30px; }}
.stat-card {{ background: var(--bg-card); border-radius: 12px; padding: 20px; text-align: center; }}
.stat-value {{ font-family: 'Bebas Neue', sans-serif; font-size: 2rem; }}
.stat-value.green {{ color: var(--green); }}
.stat-value.red {{ color: var(--red); }}
.stat-label {{ color: var(--white-60); font-size: 0.8rem; text-transform: uppercase; margin-top: 5px; }}
.chart-container {{ background: var(--bg-card); border-radius: 12px; padding: 20px; margin-bottom: 20px; }}
.chart-title {{ font-family: 'Bebas Neue', sans-serif; font-size: 1.3rem; color: var(--white-80); margin-bottom: 15px; }}
canvas {{ max-height: 300px; }}
table {{ width: 100%; border-collapse: collapse; margin-bottom: 30px; }}
th, td {{ padding: 10px 15px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.1); }}
th {{ color: var(--white-60); font-size: 0.8rem; text-transform: uppercase; }}
td {{ font-size: 0.95rem; }}
.positive {{ color: var(--green); }}
.negative {{ color: var(--red); }}
a {{ color: var(--green); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.back-link {{ margin-bottom: 20px; display: inline-block; }}
@media (max-width: 600px) {{
  .stats-row {{ grid-template-columns: repeat(2, 1fr); }}
  h1 {{ font-size: 1.8rem; }}
}}
</style>
</head>
<body>
<a href="index.html" class="back-link">&larr; Back to Home</a>
<h1>P&L Dashboard</h1>
<p class="subtitle">Updated {updated_at} | Public record (3.5u+ picks)</p>

<div class="stats-row">
  <div class="stat-card">
    <div class="stat-value green">{total_w}W-{total_l}L</div>
    <div class="stat-label">Season Record</div>
  </div>
  <div class="stat-card">
    <div class="stat-value {'green' if total_pnl >= 0 else 'red'}">{total_pnl:+.1f}u</div>
    <div class="stat-label">Season P&L</div>
  </div>
  <div class="stat-card">
    <div class="stat-value green">{win_pct}%</div>
    <div class="stat-label">Win Rate</div>
  </div>
  <div class="stat-card">
    <div class="stat-value green">${bankroll:,.0f}</div>
    <div class="stat-label">Bankroll</div>
  </div>
  <div class="stat-card">
    <div class="stat-value red">{max_dd:+.1f}u</div>
    <div class="stat-label">Max Drawdown</div>
  </div>
  <div class="stat-card">
    <div class="stat-value">{total_bets}</div>
    <div class="stat-label">Total Bets</div>
  </div>
</div>

<div class="chart-container">
  <div class="chart-title">Equity Curve (Cumulative P&L)</div>
  <canvas id="equityChart"></canvas>
</div>

<div class="chart-container">
  <div class="chart-title">Weekly P&L</div>
  <canvas id="weeklyChart"></canvas>
</div>

<div class="chart-container">
  <div class="chart-title">Drawdown</div>
  <canvas id="drawdownChart"></canvas>
</div>

<div class="chart-container">
  <div class="chart-title">P&L by Sport</div>
  <canvas id="sportChart"></canvas>
</div>

<div class="chart-container">
  <div class="chart-title">Monthly Breakdown</div>
  <table>
    <thead><tr><th>Month</th><th>Bets</th><th>W-L</th><th>Win%</th><th>P&L</th><th>CLV</th></tr></thead>
    <tbody>
"""
    for mo, bets, w, l, pnl, clv in month_rows:
        wp = round(100 * w / (w + l), 1) if (w + l) > 0 else 0
        pnl_class = 'positive' if pnl >= 0 else 'negative'
        clv_str = f"{clv:+.2f}" if clv is not None else "—"
        html += f'      <tr><td>{mo}</td><td>{bets}</td><td>{w}W-{l}L</td><td>{wp}%</td><td class="{pnl_class}">{pnl:+.1f}u</td><td>{clv_str}</td></tr>\n'

    html += """    </tbody>
  </table>
</div>

<div class="chart-container">
  <div class="chart-title">P&L by Book</div>
  <table>
    <thead><tr><th>Book</th><th>Bets</th><th>W-L</th><th>Win%</th><th>P&L</th></tr></thead>
    <tbody>
"""
    for book, bets, w, l, pnl in book_rows:
        wp = round(100 * w / (w + l), 1) if (w + l) > 0 else 0
        pnl_class = 'positive' if pnl >= 0 else 'negative'
        html += f'      <tr><td>{book}</td><td>{bets}</td><td>{w}W-{l}L</td><td>{wp}%</td><td class="{pnl_class}">{pnl:+.1f}u</td></tr>\n'

    html += f"""    </tbody>
  </table>
</div>

<p style="color: var(--white-60); font-size: 0.8rem; text-align: center; margin-top: 30px;">
  Not gambling advice &bull; 21+ &bull; 1-800-GAMBLER<br>
  <a href="https://instagram.com/scottys_edge">@scottys_edge</a> &bull;
  <a href="https://discord.gg/JQ6rRfuN">Discord</a>
</p>

<script>
const chartDefaults = {{
  responsive: true,
  plugins: {{ legend: {{ display: false }} }},
}};
Chart.defaults.color = '#999';
Chart.defaults.borderColor = 'rgba(255,255,255,0.05)';

// Equity Curve
new Chart(document.getElementById('equityChart'), {{
  type: 'line',
  data: {{
    labels: {json.dumps(dates)},
    datasets: [{{
      data: {json.dumps(cum_pnl)},
      borderColor: '#00e676',
      backgroundColor: 'rgba(0,230,118,0.1)',
      fill: true,
      tension: 0.3,
      pointRadius: 0,
      borderWidth: 2,
    }}]
  }},
  options: {{ ...chartDefaults, scales: {{ y: {{ title: {{ display: true, text: 'Units' }} }} }} }}
}});

// Weekly P&L
new Chart(document.getElementById('weeklyChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(week_labels)},
    datasets: [{{
      data: {json.dumps(week_pnl)},
      backgroundColor: {json.dumps(week_colors)},
      borderRadius: 4,
    }}]
  }},
  options: {{ ...chartDefaults, scales: {{ y: {{ title: {{ display: true, text: 'Units' }} }} }} }}
}});

// Drawdown
new Chart(document.getElementById('drawdownChart'), {{
  type: 'line',
  data: {{
    labels: {json.dumps(dates)},
    datasets: [{{
      data: {json.dumps(drawdown)},
      borderColor: '#ff5252',
      backgroundColor: 'rgba(255,82,82,0.1)',
      fill: true,
      tension: 0.3,
      pointRadius: 0,
      borderWidth: 2,
    }}]
  }},
  options: {{ ...chartDefaults, scales: {{ y: {{ title: {{ display: true, text: 'Units from peak' }} }} }} }}
}});

// Sport P&L
new Chart(document.getElementById('sportChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(sport_labels)},
    datasets: [{{
      data: {json.dumps(sport_pnl)},
      backgroundColor: {json.dumps(sport_colors)},
      borderRadius: 4,
    }}]
  }},
  options: {{ ...chartDefaults, indexAxis: 'y', scales: {{ x: {{ title: {{ display: true, text: 'Units' }} }} }} }}
}});
</script>
</body>
</html>"""

    with open(OUT, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"  Dashboard: generated {OUT}")
    return OUT


if __name__ == '__main__':
    generate()
