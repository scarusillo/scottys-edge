"""
Email-friendly HTML card generators.

Three rendering targets:
  _generate_html_card     — daily picks card (subscribers email body).
  _generate_results_html  — yesterday's grade card (P/L breakdown by sport).
  _social_media_card      — short HTML block for Discord/IG caption email.

Extracted from main.py in v26.0 Phase 8.

Re-exported from main for back-compat.
"""
import re
from datetime import datetime


def _generate_html_card(picks):
    """Generate a screenshot-ready HTML pick card and save to desktop."""
    from datetime import datetime, timedelta
    from pipeline.dates import _to_eastern, _eastern_tz_label
    from scottys_edge import kelly_label
    
    tz = _eastern_tz_label()
    now = datetime.now()
    date_str = now.strftime('%B %d, %Y')
    day_str = now.strftime('%A').upper()
    
    # Group picks by sport
    sport_groups = {}
    sport_icons = {
        'basketball_nba': '🏀', 'basketball_ncaab': '🏀',
        'icehockey_nhl': '🏒',
        'baseball_ncaa': '⚾',
        'soccer_epl': '⚽', 'soccer_germany_bundesliga': '⚽',
        'soccer_france_ligue_one': '⚽', 'soccer_italy_serie_a': '⚽',
        'soccer_spain_la_liga': '⚽', 'soccer_usa_mls': '⚽',
        'soccer_uefa_champs_league': '⚽', 'soccer_mexico_ligamx': '⚽',
    }
    sport_labels = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL', 'baseball_ncaa': 'NCAA BASEBALL',
        'soccer_epl': 'EPL', 'soccer_germany_bundesliga': 'BUNDESLIGA',
        'soccer_france_ligue_one': 'LIGUE 1', 'soccer_italy_serie_a': 'SERIE A',
        'soccer_spain_la_liga': 'LA LIGA', 'soccer_usa_mls': 'MLS',
        'soccer_uefa_champs_league': 'UCL', 'soccer_mexico_ligamx': 'LIGA MX',
    }
    # Tennis: dynamically add icons and labels from config
    try:
        from config import TENNIS_SPORTS, TENNIS_LABELS
        for _tk in TENNIS_SPORTS:
            sport_icons[_tk] = '🎾'
            sport_labels[_tk] = TENNIS_LABELS.get(_tk, _tk.split('_')[-1].upper())
    except ImportError:
        pass
    
    for p in picks:
        sp = p.get('sport', 'other')
        label = sport_labels.get(sp, sp.upper())
        if label not in sport_groups:
            sport_groups[label] = {'icon': sport_icons.get(sp, '🏟️'), 'picks': []}
        sport_groups[label]['picks'].append(p)
    
    # Build pick HTML blocks — grouped by sport, sorted within each group
    pick_blocks = []

    # Render sport sections in a consistent order
    sport_order = ['NBA', 'NHL', 'NCAAB', 'NCAA BASEBALL',
                   'EPL', 'LA LIGA', 'SERIE A', 'BUNDESLIGA', 'LIGUE 1', 'MLS', 'LIGA MX', 'UCL',
                   # Tennis tournaments (added dynamically but need ordering)
                   'AUS OPEN', 'FRENCH OPEN', 'WIMBLEDON', 'US OPEN',
                   'INDIAN WELLS', 'MIAMI OPEN', 'MONTE CARLO', 'MADRID OPEN',
                   'ITALIAN OPEN', 'CANADIAN OPEN', 'CINCINNATI', 'SHANGHAI',
                   'PARIS MASTERS', 'DUBAI', 'QATAR OPEN', 'CHINA OPEN',
                   'AUS OPEN (W)', 'FRENCH OPEN (W)', 'WIMBLEDON (W)', 'US OPEN (W)',
                   'INDIAN WELLS (W)', 'MIAMI OPEN (W)', 'MADRID OPEN (W)',
                   'ITALIAN OPEN (W)', 'CANADIAN OPEN (W)', 'CINCINNATI (W)',
                   'DUBAI (W)', 'QATAR OPEN (W)', 'CHINA OPEN (W)', 'WUHAN OPEN']

    for sport_label in sport_order:
        if sport_label not in sport_groups:
            continue
        sg = sport_groups[sport_label]
        sport_picks = sorted(sg['picks'], key=lambda p: p['units'], reverse=True)

        pick_blocks.append(f"""
    <div class="sport-header">{sport_label}</div>""")

        for p in sport_picks:
            _st = p.get('side_type')
            is_book_arb = _st in ('BOOK_ARB', 'PROP_BOOK_ARB')
            is_div_exp = _st == 'DIV_EXPANDED'
            is_prop_flip = _st == 'PROP_FADE_FLIP'
            if is_book_arb: kl = 'BOOK ARB'
            elif is_prop_flip: kl = 'PROP FLIP'
            elif is_div_exp: kl = 'NHL DIV'
            else: kl = kelly_label(p['units'])
            sp = p.get('sport', 'other')
            icon = sport_icons.get(sp, '🏟️')
            game_time = ''
            if p.get('commence'):
                try:
                    gt = datetime.fromisoformat(p['commence'].replace('Z', '+00:00'))
                    est = _to_eastern(gt)
                    game_time = est.strftime('%I:%M %p') + f' {tz}'
                except Exception:
                    pass

            if is_book_arb:
                conv_class = 'conviction-bookarb'
            elif is_prop_flip:
                conv_class = 'conviction-bookarb'
            elif is_div_exp:
                conv_class = 'conviction-divexp'
            else:
                conv_class = 'conviction-max' if kl == 'MAX PLAY' else 'conviction-strong' if kl == 'STRONG' else 'conviction-solid'
            _ctx = p.get('context', '') or p.get('notes', '')
            if is_book_arb:
                ctx_html = f'<div class="pick-context" style="background:#1a2b42;border-left:3px solid #5b8fd0;padding:6px 8px;margin-top:4px;">🔗 <b>WHY:</b> {_ctx}</div>'
            elif is_prop_flip:
                ctx_html = f'<div class="pick-context" style="background:#2b1a2a;border-left:3px solid #e07878;padding:6px 8px;margin-top:4px;">🔄 <b>PROP FLIP:</b> {_ctx}</div>'
            elif is_div_exp:
                ctx_html = f'<div class="pick-context" style="background:#2a1f42;border-left:3px solid #b99cff;padding:6px 8px;margin-top:4px;">⚖️ <b>NHL DIV v25.29:</b> {_ctx}</div>'
            elif _ctx:
                ctx_html = f'<div class="pick-context">📍 {_ctx}</div>'
            else:
                ctx_html = ''

            pick_blocks.append(f"""
    <div class="pick">
      <div class="pick-icon">{icon}</div>
      <div class="pick-info">
        <div class="pick-team">{p['selection']}</div>
        <div class="pick-matchup">{p['home']} vs {p['away']} • {game_time}</div>
        {ctx_html}
      </div>
      <div class="pick-meta">
        <div class="pick-odds">{p['odds']:+.0f}</div>
        <div class="pick-units">{p['units']:.1f} units</div>
        <div class="pick-conviction {conv_class}">{kl}</div>
      </div>
    </div>""")

    # Add any ungrouped sports
    grouped_labels = set(sport_order)
    for sport_label, sg in sport_groups.items():
        if sport_label not in grouped_labels:
            sport_picks = sorted(sg['picks'], key=lambda p: p['units'], reverse=True)
            pick_blocks.append(f"""
    <div class="sport-header">{sport_label}</div>""")
            for p in sport_picks:
                is_book_arb = p.get('side_type') == 'BOOK_ARB'
                is_div_exp = p.get('side_type') == 'DIV_EXPANDED'
                kl = 'BOOK ARB' if is_book_arb else ('NHL DIV' if is_div_exp else kelly_label(p['units']))
                sp = p.get('sport', 'other')
                icon = sport_icons.get(sp, '🏟️')
                game_time = ''
                if p.get('commence'):
                    try:
                        gt = datetime.fromisoformat(p['commence'].replace('Z', '+00:00'))
                        est = _to_eastern(gt)
                        game_time = est.strftime('%I:%M %p') + f' {tz}'
                    except Exception:
                        pass
                if is_book_arb:
                    conv_class = 'conviction-bookarb'
                elif is_div_exp:
                    conv_class = 'conviction-divexp'
                else:
                    conv_class = 'conviction-max' if kl == 'MAX PLAY' else 'conviction-strong' if kl == 'STRONG' else 'conviction-solid'
                _ctx = p.get('context', '')
                if is_book_arb:
                    ctx_html = f'<div class="pick-context" style="background:#1a2b42;border-left:3px solid #5b8fd0;padding:6px 8px;margin-top:4px;">🔗 <b>WHY:</b> {_ctx}</div>'
                elif is_div_exp:
                    ctx_html = f'<div class="pick-context" style="background:#2a1f42;border-left:3px solid #b99cff;padding:6px 8px;margin-top:4px;">⚖️ <b>NHL DIV v25.29:</b> {_ctx}</div>'
                elif _ctx:
                    ctx_html = f'<div class="pick-context">📍 {_ctx}</div>'
                else:
                    ctx_html = ''
                pick_blocks.append(f"""
    <div class="pick">
      <div class="pick-icon">{icon}</div>
      <div class="pick-info">
        <div class="pick-team">{p['selection']}</div>
        <div class="pick-matchup">{p['home']} vs {p['away']} • {game_time}</div>
        {ctx_html}
      </div>
      <div class="pick-meta">
        <div class="pick-odds">{p['odds']:+.0f}</div>
        <div class="pick-units">{p['units']:.1f} units</div>
        <div class="pick-conviction {conv_class}">{kl}</div>
      </div>
    </div>""")

    picks_html = '\n'.join(pick_blocks)
    tu = sum(p['units'] for p in picks)
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scotty's Edge — {date_str}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Barlow:wght@400;500;600;700&family=Barlow+Condensed:wght@400;600;700&display=swap');
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    background: #0a0a0a;
    display: flex; justify-content: center; align-items: center;
    min-height: 100vh; padding: 40px;
    font-family: 'Barlow', sans-serif;
  }}
  .card {{
    width: 1080px;
    background: linear-gradient(165deg, #0d1117 0%, #0f1923 40%, #111d2b 100%);
    border-radius: 24px; overflow: hidden; position: relative;
    border: 1px solid rgba(255,255,255,0.06);
  }}
  .card::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 4px;
    background: linear-gradient(90deg, #00e676, #00c853, #69f0ae);
  }}
  .card::after {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0;
    background-image: linear-gradient(rgba(255,255,255,0.02) 1px, transparent 1px),
      linear-gradient(90deg, rgba(255,255,255,0.02) 1px, transparent 1px);
    background-size: 40px 40px; pointer-events: none;
  }}
  .header {{
    padding: 48px 52px 32px; position: relative; z-index: 1;
    display: flex; justify-content: space-between; align-items: flex-start;
  }}
  .brand {{ display: flex; flex-direction: column; gap: 4px; }}
  .logo {{
    font-family: 'Bebas Neue', sans-serif; font-size: 52px;
    color: #ffffff; letter-spacing: 3px; line-height: 1;
  }}
  .logo span {{ color: #00e676; }}
  .subtitle {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 15px;
    color: rgba(255,255,255,0.35); letter-spacing: 4px;
    text-transform: uppercase; font-weight: 600;
  }}
  .date-block {{ text-align: right; }}
  .date-day {{
    font-family: 'Bebas Neue', sans-serif; font-size: 28px;
    color: rgba(255,255,255,0.8); letter-spacing: 2px;
  }}
  .date-full {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 14px;
    color: rgba(255,255,255,0.3); letter-spacing: 2px; text-transform: uppercase;
  }}
  .divider {{
    height: 1px; background: linear-gradient(90deg, transparent, rgba(0,230,118,0.3), transparent);
    margin: 0 52px; position: relative; z-index: 1;
  }}
  .picks-section {{ padding: 36px 52px 20px; position: relative; z-index: 1; }}
  .section-label {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 13px;
    color: #00e676; letter-spacing: 4px; text-transform: uppercase;
    font-weight: 700; margin-bottom: 24px;
  }}
  .pick {{
    display: flex; align-items: center; padding: 24px 28px;
    background: rgba(255,255,255,0.03); border-radius: 16px;
    margin-bottom: 16px; border: 1px solid rgba(255,255,255,0.04);
    position: relative; overflow: hidden;
  }}
  .pick::before {{
    content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 4px;
    background: #00e676; border-radius: 0 4px 4px 0;
  }}
  .pick-icon {{ font-size: 32px; margin-right: 20px; min-width: 40px; text-align: center; }}
  .pick-info {{ flex: 1; }}
  .pick-team {{
    font-family: 'Barlow', sans-serif; font-size: 22px; font-weight: 700;
    color: #ffffff; margin-bottom: 4px;
  }}
  .pick-matchup {{
    font-family: 'Barlow', sans-serif; font-size: 14px;
    color: rgba(255,255,255,0.4); margin-bottom: 4px;
  }}
  .pick-context {{ font-size: 12px; color: #00e676; opacity: 0.7; font-weight: 500; }}
  .sport-header {{
    font-family: 'Bebas Neue', sans-serif; font-size: 24px;
    color: rgba(255,255,255,0.7); letter-spacing: 3px;
    margin-top: 20px; margin-bottom: 12px; padding-bottom: 8px;
    border-bottom: 1px solid rgba(255,255,255,0.08);
    position: relative; z-index: 1;
  }}
  .pick-meta {{ text-align: right; min-width: 140px; }}
  .pick-odds {{ font-family: 'Bebas Neue', sans-serif; font-size: 32px; color: #ffffff; line-height: 1; }}
  .pick-units {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 14px;
    color: rgba(255,255,255,0.5); letter-spacing: 1px; margin-top: 4px;
  }}
  .pick-conviction {{
    display: inline-block; font-family: 'Barlow Condensed', sans-serif;
    font-size: 11px; font-weight: 700; letter-spacing: 2px;
    text-transform: uppercase; padding: 3px 10px; border-radius: 4px; margin-top: 6px;
  }}
  .conviction-max {{ background: rgba(0,230,118,0.15); color: #00e676; }}
  .conviction-strong {{ background: rgba(255,193,7,0.15); color: #ffc107; }}
  .conviction-solid {{ background: rgba(100,181,246,0.15); color: #64b5f6; }}
  .conviction-bookarb {{ background: rgba(91,143,208,0.22); color: #8bb4f0; }}
  .conviction-divexp {{ background: rgba(185,156,255,0.18); color: #b99cff; }}
  .footer {{
    padding: 28px 52px 20px; position: relative; z-index: 1;
    display: flex; justify-content: space-between; align-items: center;
  }}
  .footer-left {{ display: flex; gap: 32px; }}
  .stat {{ text-align: center; }}
  .stat-value {{ font-family: 'Bebas Neue', sans-serif; font-size: 28px; color: #ffffff; }}
  .stat-label {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 11px;
    color: rgba(255,255,255,0.3); letter-spacing: 2px; text-transform: uppercase;
  }}
  .footer-right {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 13px;
    color: rgba(255,255,255,0.25); letter-spacing: 2px; text-transform: uppercase;
  }}
  .disclaimer {{
    padding: 20px 52px 32px; position: relative; z-index: 1;
    border-top: 1px solid rgba(255,255,255,0.04);
  }}
  .disclaimer p {{
    font-family: 'Barlow', sans-serif; font-size: 9px;
    color: rgba(255,255,255,0.2); line-height: 1.5; text-align: center;
  }}
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div class="brand">
      <div class="logo">SCOTTY'S <span>EDGE</span></div>
      <div class="subtitle">Data-Driven Sports Picks</div>
    </div>
    <div class="date-block">
      <div class="date-day">{day_str}</div>
      <div class="date-full">{date_str}</div>
    </div>
  </div>
  <div class="divider"></div>
  <div class="picks-section">
    <div class="section-label">Today's Plays</div>
    {picks_html}
  </div>
  <div class="divider"></div>
  <div class="footer">
    <div class="footer-left">
      <div class="stat">
        <div class="stat-value">{len(picks)}</div>
        <div class="stat-label">Plays</div>
      </div>
      <div class="stat">
        <div class="stat-value">{tu:.0f}u</div>
        <div class="stat-label">Total</div>
      </div>
    </div>
    <div class="footer-right">Every pick tracked & graded</div>
  </div>
  <div class="disclaimer">
    <p>For entertainment and informational purposes only. Not gambling advice. Past performance does not guarantee future results. Please gamble responsibly. If you or someone you know has a gambling problem, call 1-800-GAMBLER. Must be 21+ and located in a legal jurisdiction. Scotty's Edge does not accept or place bets on behalf of users.</p>
  </div>
</div>
</body>
</html>"""
    
    # Save to desktop
    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')
    if not os.path.exists(desktop):
        desktop = os.path.join(os.path.expanduser('~'), 'OneDrive', 'Desktop')
    
    filepath = os.path.join(desktop, 'scottys_edge_picks.html')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  📱 Social media card saved: {filepath}")
    return filepath, html




def _generate_results_html(report_text):
    """Generate a public-facing HTML results card for email/Instagram."""
    import re
    from datetime import datetime
    
    now = datetime.now()
    date_str = now.strftime('%B %d, %Y')
    day_str = now.strftime('%A').upper()
    
    # Parse key stats from report
    record = re.search(r'Record: (\d+)W-(\d+)L', report_text)
    pl = re.search(r'P/L: ([+-]?\d+\.\d+)u', report_text)
    roi = re.search(r'ROI: ([+-]?\d+\.\d+)%', report_text)
    
    wins = int(record.group(1)) if record else 0
    losses = int(record.group(2)) if record else 0
    total = wins + losses
    pct = f"{wins/(total)*100:.1f}" if total > 0 else "0.0"
    pl_str = pl.group(1) if pl else "0.00"
    roi_str = roi.group(1) if roi else "0.0"
    pl_val = float(pl_str)
    
    # Parse today's picks
    today_picks_html = ""
    in_picks = False
    pick_date = ""
    for line in report_text.split('\n'):
        if 'PICKS FROM' in line:
            in_picks = True
            m = re.search(r'PICKS FROM (\S+)', line)
            if m: pick_date = m.group(1)
            continue
        if in_picks and ('=====' in line):
            break
        if in_picks and line.strip():
            # Parse pick line: emoji + selection + pnl + CLV
            clean = line.strip()
            if not clean:
                continue
            # Determine color based on win/loss
            is_win = '✅' in clean or '✓' in clean
            is_loss = '❌' in clean or '✗' in clean
            color = '#00e676' if is_win else '#ff5252' if is_loss else '#888'
            
            today_picks_html += f'    <div class="result-row" style="color: {color};">{clean}</div>\n'
    
    # Parse best sports
    sport_html = ""
    in_sport = False
    for line in report_text.split('\n'):
        if '── BY SPORT' in line:
            in_sport = True
            continue
        if in_sport and '──' in line:
            break
        if in_sport and line.strip() and 'W-' in line:
            clean = line.strip()
            sport_html += f'    <div class="sport-row">{clean}</div>\n'
    
    # Color accent based on P/L
    accent = '#00e676' if pl_val >= 0 else '#ff5252'
    pl_prefix = '+' if pl_val >= 0 else ''
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scotty's Edge — Results {date_str}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Barlow:wght@400;500;600;700&family=Barlow+Condensed:wght@400;600;700&display=swap');
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    background: #0a0a0a;
    display: flex; justify-content: center; align-items: center;
    min-height: 100vh; padding: 40px;
    font-family: 'Barlow', sans-serif;
  }}
  .card {{
    width: 1080px;
    background: linear-gradient(165deg, #0d1117 0%, #0f1923 40%, #111d2b 100%);
    border-radius: 24px; overflow: hidden; position: relative;
    border: 1px solid rgba(255,255,255,0.06);
  }}
  .card::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 4px;
    background: linear-gradient(90deg, {accent}, {accent}88);
  }}
  .header {{
    padding: 48px 52px 32px; position: relative; z-index: 1;
    display: flex; justify-content: space-between; align-items: flex-start;
  }}
  .brand {{ display: flex; flex-direction: column; gap: 4px; }}
  .logo {{
    font-family: 'Bebas Neue', sans-serif; font-size: 48px;
    color: #ffffff; letter-spacing: 3px; line-height: 1;
  }}
  .logo span {{ color: {accent}; }}
  .subtitle {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 15px;
    color: rgba(255,255,255,0.35); letter-spacing: 4px;
    text-transform: uppercase; font-weight: 600;
  }}
  .date-block {{ text-align: right; }}
  .date-day {{
    font-family: 'Bebas Neue', sans-serif; font-size: 24px;
    color: rgba(255,255,255,0.8); letter-spacing: 2px;
  }}
  .date-full {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 13px;
    color: rgba(255,255,255,0.3); letter-spacing: 2px;
  }}
  .divider {{
    height: 1px; background: linear-gradient(90deg, transparent, {accent}44, transparent);
    margin: 0 52px;
  }}
  .stats-row {{
    display: flex; justify-content: center; gap: 60px;
    padding: 36px 52px;
  }}
  .stat {{ text-align: center; }}
  .stat-value {{
    font-family: 'Bebas Neue', sans-serif; font-size: 48px;
    color: #ffffff; line-height: 1;
  }}
  .stat-value.positive {{ color: #00e676; }}
  .stat-value.negative {{ color: #ff5252; }}
  .stat-label {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 12px;
    color: rgba(255,255,255,0.3); letter-spacing: 3px;
    text-transform: uppercase; margin-top: 4px;
  }}
  .section {{
    padding: 24px 52px;
  }}
  .section-label {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 12px;
    color: {accent}; letter-spacing: 4px; text-transform: uppercase;
    font-weight: 700; margin-bottom: 16px;
  }}
  .result-row {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 16px;
    padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.04);
  }}
  .sport-row {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 15px;
    color: rgba(255,255,255,0.6); padding: 4px 0;
  }}
  .footer {{
    padding: 20px 52px 32px;
    border-top: 1px solid rgba(255,255,255,0.04);
  }}
  .footer p {{
    font-family: 'Barlow', sans-serif; font-size: 9px;
    color: rgba(255,255,255,0.2); line-height: 1.5; text-align: center;
  }}
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div class="brand">
      <div class="logo">SCOTTY'S <span>EDGE</span></div>
      <div class="subtitle">Daily Results</div>
    </div>
    <div class="date-block">
      <div class="date-day">{day_str}</div>
      <div class="date-full">{date_str}</div>
    </div>
  </div>
  
  <div class="divider"></div>
  
  <div class="stats-row">
    <div class="stat">
      <div class="stat-value">{wins}W-{losses}L</div>
      <div class="stat-label">Record</div>
    </div>
    <div class="stat">
      <div class="stat-value {('positive' if pl_val >= 0 else 'negative')}">{pl_prefix}{pl_str}u</div>
      <div class="stat-label">Profit / Loss</div>
    </div>
    <div class="stat">
      <div class="stat-value {('positive' if pl_val >= 0 else 'negative')}">{roi_str}%</div>
      <div class="stat-label">ROI</div>
    </div>
    <div class="stat">
      <div class="stat-value">{pct}%</div>
      <div class="stat-label">Win Rate</div>
    </div>
  </div>
  
  <div class="divider"></div>
  
  <div class="section">
    <div class="section-label">Latest Results</div>
{today_picks_html}
  </div>
  
  <div class="divider"></div>
  
  <div class="section">
    <div class="section-label">By Sport</div>
{sport_html}
  </div>
  
  <div class="footer">
    <p>For entertainment and informational purposes only. Not gambling advice. Past performance does not guarantee future results. Please gamble responsibly. If you or someone you know has a gambling problem, call 1-800-GAMBLER. Must be 21+ and located in a legal jurisdiction.</p>
  </div>
</div>
</body>
</html>"""
    
    cards_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards')
    os.makedirs(cards_dir, exist_ok=True)

    filepath = os.path.join(cards_dir, 'scottys_edge_results.html')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  📊 Results card saved: {filepath}")
    return filepath, html




def _social_media_card(picks):
    """
    Generate copy-paste ready social media content for Instagram + Discord.
    v25.3: Twitter format removed — @Scottys_Edge suspended April 2026.
    """
    from datetime import datetime, timedelta
    from pipeline.dates import _to_eastern, _eastern_tz_label
    from scottys_edge import kelly_label
    
    tz = _eastern_tz_label()
    now = datetime.now()
    date_str = now.strftime('%B %d')
    day_str = now.strftime('%A')
    
    # Sort by confidence — highest first
    picks = sorted(picks, key=lambda p: p['units'], reverse=True)
    
    lines = []
    lines.append("=" * 50)
    lines.append("📱 SOCIAL MEDIA — COPY & PASTE BELOW")
    lines.append("=" * 50)

    # v25.3: Twitter/X format removed — @Scottys_Edge suspended April 2026.

    # ── INSTAGRAM / DISCORD FORMAT (visual) ──
    lines.append("")
    lines.append("── INSTAGRAM / DISCORD ──")
    lines.append("")
    lines.append(f"🏀⚽🏒 SCOTTY'S EDGE")
    lines.append(f"📅 {now.strftime('%B %d, %Y')}")
    lines.append(f"━━━━━━━━━━━━━━━━━━━━")
    
    # Group by sport
    by_sport = {}
    for p in picks:
        sport_label = p.get('sport', '').replace('basketball_', '').replace('icehockey_', '').replace('soccer_', '').replace('baseball_', '').upper()
        sport_nice = {
            'NBA': '🏀 NBA', 'NCAAB': '🏀 NCAAB', 'NHL': '🏒 NHL',
            'NCAA': '⚾ NCAA Baseball', 'EPL': '⚽ EPL',
            'GERMANY_BUNDESLIGA': '⚽ Bundesliga',
            'FRANCE_LIGUE_ONE': '⚽ Ligue 1',
            'ITALY_SERIE_A': '⚽ Serie A',
            'SPAIN_LA_LIGA': '⚽ La Liga',
            'USA_MLS': '⚽ MLS',
        }.get(sport_label, f'🏟️ {sport_label}')
        if sport_nice not in by_sport:
            by_sport[sport_nice] = []
        by_sport[sport_nice].append(p)
    
    for sport_name, sport_picks in by_sport.items():
        lines.append(f"")
        lines.append(f"{sport_name}")
        for p in sport_picks:
            kl = kelly_label(p['units'])
            icon = '🔥' if kl == 'MAX PLAY' else '⭐' if kl == 'STRONG' else '✅'
            odds_str = f"{p['odds']:+.0f}" if p['odds'] else ''
            
            game_time = ''
            if p.get('commence'):
                try:
                    gt = datetime.fromisoformat(p['commence'].replace('Z', '+00:00'))
                    est = _to_eastern(gt)
                    game_time = est.strftime('%I:%M %p')
                except Exception:
                    pass
            
            lines.append(f"  {icon} {p['selection']}")
            lines.append(f"     {odds_str} • {p['units']:.0f}u {kl} • {game_time} {tz}")
    
    lines.append(f"")
    lines.append(f"━━━━━━━━━━━━━━━━━━━━")
    tu = sum(p.get('units', 0) or 0 for p in picks)
    lines.append(f"📊 {len(picks)} plays • {tu:.0f}u total")
    lines.append(f"Every pick tracked & graded")
    lines.append(f"")
    lines.append(f"⚠️ For entertainment & informational purposes only.")
    lines.append(f"Not gambling advice. Must be 21+. Gamble responsibly.")
    lines.append(f"1-800-GAMBLER | scottysedge.com")
    lines.append(f"")
    lines.append("=" * 50)
    
    return '\n'.join(lines)


