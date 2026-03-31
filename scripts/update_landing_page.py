#!/usr/bin/env python3
"""
update_landing_page.py — Auto-update docs/index.html with live stats from DB.

Run after grading to keep the landing page current.
Updates: hero stats, meta tags, track record table, subscriber count.

Usage:
    python update_landing_page.py              # Update and print summary
    python update_landing_page.py --push       # Update, commit, and push
"""
import sqlite3, os, sys, re, subprocess
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
HTML_PATH = os.path.join(os.path.dirname(__file__), '..', 'docs', 'index.html')


def get_stats(conn):
    """Pull current stats from graded_bets."""
    base = """
        SELECT sport,
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as W,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as L,
               ROUND(SUM(pnl_units), 1) as pnl,
               ROUND(SUM(units), 1) as wagered
        FROM graded_bets
        WHERE result IN ('WIN','LOSS')
        AND DATE(created_at) >= '2026-03-04'
        AND units >= 3.5
    """

    # Overall
    row = conn.execute(base).fetchone()
    overall = {
        'W': row[1], 'L': row[2], 'pnl': row[3], 'wagered': row[4],
        'wp': round(row[1] / (row[1] + row[2]) * 100, 1) if (row[1] + row[2]) > 0 else 0,
        'roi': round(row[3] / row[4] * 100, 1) if row[4] else 0,
    }

    # By sport
    rows = conn.execute(base + " GROUP BY sport").fetchall()

    sport_map = {
        'basketball_nba': 'NBA',
        'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL',
        'baseball_ncaa': 'College Baseball',
        'baseball_mlb': 'MLB',
    }
    soccer_sports = [
        'soccer_epl', 'soccer_italy_serie_a', 'soccer_spain_la_liga',
        'soccer_germany_bundesliga', 'soccer_france_ligue_one',
        'soccer_uefa_champs_league', 'soccer_usa_mls', 'soccer_mexico_ligamx',
    ]
    tennis_sports = [s for s in [r[0] for r in rows] if 'tennis' in s]

    sports = {}
    soccer_w, soccer_l, soccer_pnl, soccer_wag = 0, 0, 0, 0
    tennis_w, tennis_l, tennis_pnl, tennis_wag = 0, 0, 0, 0

    for sport, w, l, pnl, wag in rows:
        if sport in soccer_sports:
            soccer_w += w; soccer_l += l; soccer_pnl += pnl; soccer_wag += wag
        elif sport in tennis_sports or 'tennis' in sport:
            tennis_w += w; tennis_l += l; tennis_pnl += pnl; tennis_wag += wag
        elif sport in sport_map:
            total = w + l
            sports[sport_map[sport]] = {
                'W': w, 'L': l, 'pnl': round(pnl, 1), 'wagered': round(wag, 1),
                'wp': round(w / total * 100, 1) if total > 0 else 0,
                'roi': round(pnl / wag * 100, 1) if wag else 0,
            }

    if soccer_w + soccer_l > 0:
        sports['Soccer'] = {
            'W': soccer_w, 'L': soccer_l, 'pnl': round(soccer_pnl, 1),
            'wagered': round(soccer_wag, 1),
            'wp': round(soccer_w / (soccer_w + soccer_l) * 100, 1),
            'roi': round(soccer_pnl / soccer_wag * 100, 1) if soccer_wag else 0,
        }
    if tennis_w + tennis_l > 0:
        sports['Tennis'] = {
            'W': tennis_w, 'L': tennis_l, 'pnl': round(tennis_pnl, 1),
            'wagered': round(tennis_wag, 1),
            'wp': round(tennis_w / (tennis_w + tennis_l) * 100, 1),
            'roi': round(tennis_pnl / tennis_wag * 100, 1) if tennis_wag else 0,
        }

    return overall, sports


def get_recent_results(conn, days=3):
    """Pull the last N days of graded results for the landing page."""
    rows = conn.execute("""
        SELECT DATE(created_at) as dt,
               selection, result, ROUND(pnl_units, 1) as pnl, market_type
        FROM graded_bets
        WHERE result IN ('WIN','LOSS','PUSH')
        ORDER BY created_at DESC
    """).fetchall()

    from collections import OrderedDict
    days_data = OrderedDict()
    for dt, sel, result, pnl, mtype in rows:
        if dt not in days_data:
            if len(days_data) >= days:
                break
            days_data[dt] = {'picks': [], 'w': 0, 'l': 0, 'p': 0, 'pnl': 0.0}
        d = days_data[dt]
        d['picks'].append({'sel': sel, 'result': result, 'pnl': pnl})
        if result == 'WIN':
            d['w'] += 1
        elif result == 'LOSS':
            d['l'] += 1
        else:
            d['p'] += 1
        d['pnl'] += pnl

    return days_data


def _truncate_selection(sel, max_len=42):
    """Shorten long selection names for display."""
    # Strip common suffixes to save space
    sel = sel.replace(' (cross-mkt)', '')
    if len(sel) <= max_len:
        return sel
    return sel[:max_len - 1].rstrip() + '\u2026'


def build_results_html(days_data):
    """Generate HTML cards for recent results."""
    from datetime import datetime as _dt
    cards = []
    for date_str, d in days_data.items():
        dt = _dt.strptime(date_str, '%Y-%m-%d')
        date_label = dt.strftime('%b %d, %Y')  # e.g. "Mar 28, 2026"

        record = f"{d['w']}W-{d['l']}L"
        if d['p'] > 0:
            record += f"-{d['p']}P"

        pnl = round(d['pnl'], 1)
        pnl_str = f"+{pnl}u" if pnl > 0 else f"{pnl}u"
        pnl_cls = 'positive' if pnl >= 0 else 'negative'

        # Build pick list items
        pick_items = []
        for pick in d['picks']:
            r = pick['result']
            icon_cls = 'win' if r == 'WIN' else ('loss' if r == 'LOSS' else 'push')
            icon_letter = 'W' if r == 'WIN' else ('L' if r == 'LOSS' else 'P')
            name = _truncate_selection(pick['sel'])
            # HTML-escape ampersands in names
            name = name.replace('&', '&amp;')
            pick_items.append(
                f'          <li><span class="pick-icon {icon_cls}">{icon_letter}</span>{name}</li>'
            )

        picks_html = '\n'.join(pick_items)

        card = f"""      <div class="result-card">
        <div class="result-card-header">
          <span class="result-card-date">{date_label}</span>
          <span class="result-card-record">{record}</span>
        </div>
        <div class="result-card-pnl {pnl_cls}">{pnl_str}</div>
        <ul class="result-card-picks">
{picks_html}
        </ul>
      </div>"""
        cards.append(card)

    return '\n'.join(cards)


def update_html(overall, sports, results_html=''):
    """Replace hardcoded stats in index.html with live data."""
    with open(HTML_PATH, 'r', encoding='utf-8') as f:
        html = f.read()

    W, L, pnl, wp, roi = overall['W'], overall['L'], overall['pnl'], overall['wp'], overall['roi']
    pnl_str = f"+{pnl}" if pnl > 0 else str(pnl)
    roi_str = f"+{roi}%" if roi > 0 else f"{roi}%"

    # ── Hero stats ──
    html = re.sub(
        r'(<div class="stat-number record">)[\d\-]+(<\/div>\s*<div class="stat-label">Record)',
        rf'\g<1>{W}-{L}\2', html)
    html = re.sub(
        r'(<div class="stat-number positive">)[+\-\d.]+u(<\/div>\s*<div class="stat-label">Profit)',
        rf'\g<1>{pnl_str}u\2', html)
    html = re.sub(
        r'(<div class="stat-number positive">)[\d.]+%(<\/div>\s*<div class="stat-label">Win Rate)',
        rf'\g<1>{wp}%\2', html)
    html = re.sub(
        r'(<div class="stat-number positive">)[+\-\d.]+%(<\/div>\s*<div class="stat-label">ROI)',
        rf'\g<1>{roi_str}\2', html)

    # ── Meta OG description ──
    html = re.sub(
        r'(<meta property="og:description" content=").*?(")',
        rf'\g<1>{W}W-{L}L | {pnl_str}u | {wp}% Win Rate. Every pick tracked since day one.\2',
        html)

    # ── Track record table ──
    def _row(label, s, is_overall=False):
        cls = ' class="overall-row"' if is_overall else ''
        name_cls = ' class="sport-name"' if True else ''
        pnl_cls = 'positive' if s['pnl'] > 0 else 'negative'
        wp_cls = 'positive' if s['wp'] >= 55 else ('negative' if s['wp'] < 50 else '')
        roi_cls = 'positive' if s['roi'] > 0 else 'negative'
        p = f"+{s['pnl']}" if s['pnl'] > 0 else str(s['pnl'])
        r = f"+{s['roi']}%" if s['roi'] > 0 else f"{s['roi']}%"
        return (f'          <tr{cls}>\n'
                f'            <td class="sport-name">{label}</td>\n'
                f'            <td>{s["W"]}W - {s["L"]}L</td>\n'
                f'            <td class="{pnl_cls}">{p}u</td>\n'
                f'            <td class="{pnl_cls}">{s["wp"]}%</td>\n'
                f'            <td class="{roi_cls}">{r}</td>\n'
                f'          </tr>')

    # Build new tbody
    sport_order = ['NBA', 'NHL', 'NCAAB', 'College Baseball', 'MLB', 'Soccer', 'Tennis']
    rows = [_row('Overall', overall, is_overall=True)]
    for s in sport_order:
        if s in sports:
            rows.append(_row(s, sports[s]))

    new_tbody = '\n'.join(rows)

    # Replace tbody content
    html = re.sub(
        r'<tbody>\s*<tr class="overall-row">.*?</tbody>',
        f'<tbody>\n{new_tbody}\n        </tbody>',
        html, flags=re.DOTALL)

    # ── Results cards ──
    if results_html:
        html = re.sub(
            r'<!-- RESULTS-START -->\s*<div class="results-grid[^"]*"[^>]*>.*?</div>\s*<!-- RESULTS-END -->',
            f'<!-- RESULTS-START -->\n    <div class="results-grid fade-in" id="results-cards">\n{results_html}\n    </div>\n    <!-- RESULTS-END -->',
            html, flags=re.DOTALL)

    with open(HTML_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    return W, L, pnl, wp, roi


if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    overall, sports = get_stats(conn)
    days_data = get_recent_results(conn, days=3)
    conn.close()

    results_html = build_results_html(days_data)
    W, L, pnl, wp, roi = update_html(overall, sports, results_html=results_html)
    print(f"  Results cards: {len(days_data)} days")
    for dt, d in days_data.items():
        p = f"+{round(d['pnl'],1)}" if d['pnl'] > 0 else str(round(d['pnl'],1))
        print(f"    {dt}: {d['w']}W-{d['l']}L | {p}u | {len(d['picks'])} picks")
    print(f"  Landing page updated: {W}W-{L}L | +{pnl}u | {wp}% | ROI +{roi}%")
    for s, d in sports.items():
        p = f"+{d['pnl']}" if d['pnl'] > 0 else str(d['pnl'])
        print(f"    {s}: {d['W']}W-{d['L']}L | {p}u | {d['wp']}% | ROI {d['roi']}%")

    if '--push' in sys.argv:
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))
        subprocess.run(['git', 'add', 'docs/index.html'], check=True)
        msg = f"Update landing page stats: {W}W-{L}L +{pnl}u"
        subprocess.run(['git', 'commit', '-m', msg], check=True)
        subprocess.run(['git', 'push'], check=True)
        print("  Committed and pushed to GitHub Pages")
