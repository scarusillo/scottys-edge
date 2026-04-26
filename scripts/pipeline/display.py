"""
Pick display helpers — terminal printing and email-friendly text formatting.

`print_picks` writes a styled card to stdout. `picks_to_text` returns the
same content as a plaintext string for embedding in emails. Both call
`_render_sport_group` to build per-sport sub-blocks.

Extracted from model_engine.py in v26.0 Phase 7.

Re-exported from model_engine for back-compat.
"""
from datetime import datetime, timedelta
from scottys_edge import kelly_label
from pipeline.dates import _to_eastern, _eastern_tz_label


def print_picks(picks, title="TODAY'S PICKS"):
    if not picks:
        print(f"\n{'='*70}\n  {title}: No qualifying plays\n  Target: 5-10/week — patience IS the edge.\n{'='*70}")
        return picks
    print(f"\n{'='*70}")
    print(f"  {title} — {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    print(f"  {len(picks)} plays | Scotty's Edge v11")
    print(f"{'='*70}")
    by_sport = {}
    for p in picks: by_sport.setdefault(p['sport'], []).append(p)
    for sport, spicks in by_sport.items():
        SPORT_LABELS = {
            'basketball_ncaab': 'NCAAB', 'basketball_nba': 'NBA',
            'icehockey_nhl': 'NHL', 'baseball_ncaa': 'NCAA_BASEBALL',
            'soccer_epl': 'EPL', 'soccer_italy_serie_a': 'ITALY_SERIE_A',
            'soccer_spain_la_liga': 'SPAIN_LA_LIGA',
            'soccer_germany_bundesliga': 'GERMANY_BUNDESLIGA',
            'soccer_france_ligue_one': 'FRANCE_LIGUE_ONE',
            'soccer_uefa_champs_league': 'UEFA_CHAMPIONS_LEAGUE',
            'soccer_usa_mls': 'MLS',
            'soccer_mexico_ligamx': 'LIGA_MX',
        }
        label = SPORT_LABELS.get(sport, sport.upper())
        print(f"\n  ── {label} {'─'*(50-len(label))}")
        for p in spicks:
            units = p['units']
            kl = kelly_label(units)
            # Size indicator: visual bar (scale 0-5u → 0-10 blocks)
            filled = min(10, int(units * 2))
            bar = '█' * filled + '░' * (10 - filled)
            # Icon by conviction tier
            tier_icon = {
                'MAX PLAY': '🔥', 'STRONG': '⭐', 'SOLID': '✅',
                'LEAN': '📊', 'SPRINKLE': '📋'
            }.get(kl, '📋')
            # Convert UTC to Eastern (DST-aware)
            day_label, est_time = '', ''
            tz_label = _eastern_tz_label()
            if p['commence']:
                try:
                    gt = datetime.fromisoformat(p['commence'].replace('Z','+00:00'))
                    est = _to_eastern(gt)
                    est_time = est.strftime('%I:%M %p')
                    today = datetime.now()
                    if est.date() == today.date():
                        day_label = 'TODAY'
                    elif est.date() == (today + timedelta(days=1)).date():
                        day_label = 'TOMORROW'
                    else:
                        day_label = est.strftime('%a %m/%d')
                except Exception:
                    est_time = p['commence'][:16].replace('T',' ')
            print(f"\n  {tier_icon} {p['selection']}")
            print(f"     {p['home']} vs {p['away']} | {day_label} {est_time} {tz_label}")
            print(f"     {p['book']} | {p['odds']:+.0f} | {p['market_type']}")
            print(f"     Edge: {p['edge_pct']:.1f}%  |  {bar} {units:.1f}u {kl}")
            timing = p.get('timing', '')
            if timing:
                timing_icon = {'EARLY': '⏰ BET EARLY', 'LATE': '⏳ BET LATE', 'HOLD': '🕐 HOLD FOR BEST LINE'}.get(timing, timing)
                print(f"     {timing_icon}")
            # Context factors (if any active)
            ctx_summary = p.get('context')
            if ctx_summary:
                print(f"     📍 {ctx_summary}")
    print(f"\n{'='*70}")
    tu = sum(p['units'] for p in picks)
    sizes = {}
    for p in picks:
        kl = kelly_label(p['units'])
        sizes[kl] = sizes.get(kl, 0) + 1
    size_str = ' | '.join(f"{v} {k}" for k, v in sizes.items() if v > 0)
    print(f"  {len(picks)} plays | {tu:.1f} total units | {size_str}")
    print(f"{'='*70}")
    return picks



def picks_to_text(picks, title="TODAY'S PICKS"):
    """Clean subscriber-ready text format for email/Telegram, grouped by sport."""
    lines = []
    if not picks:
        return f"{title}: No qualifying plays today. Patience is the edge."

    sport_labels = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL', 'baseball_ncaa': 'NCAA BASEBALL',
        'soccer_epl': 'EPL', 'soccer_germany_bundesliga': 'BUNDESLIGA',
        'soccer_france_ligue_one': 'LIGUE 1', 'soccer_italy_serie_a': 'SERIE A',
        'soccer_spain_la_liga': 'LA LIGA', 'soccer_usa_mls': 'MLS',
        'soccer_uefa_champs_league': 'UCL', 'soccer_mexico_ligamx': 'LIGA MX',
    }
    sport_icons = {
        'NBA': '🏀', 'NCAAB': '🏀', 'NHL': '🏒', 'NCAA BASEBALL': '⚾', 'LIGA MX': '⚽',
        'EPL': '⚽', 'BUNDESLIGA': '⚽', 'LIGUE 1': '⚽', 'SERIE A': '⚽',
        'LA LIGA': '⚽', 'MLS': '⚽', 'UCL': '⚽',
    }
    sport_order = ['NBA', 'NHL', 'NCAAB', 'NCAA BASEBALL',
                   'EPL', 'LA LIGA', 'SERIE A', 'BUNDESLIGA', 'LIGUE 1', 'MLS', 'UCL']

    # Group picks by sport
    groups = {}
    for p in picks:
        sp = p.get('sport', 'other')
        label = sport_labels.get(sp, sp.upper())
        if label not in groups:
            groups[label] = []
        groups[label].append(p)

    # Sort within groups by units descending
    for label in groups:
        groups[label].sort(key=lambda p: p['units'], reverse=True)

    lines.append(f"{'━'*50}")
    lines.append(f"  {title}")
    lines.append(f"  {datetime.now().strftime('%A, %B %d %Y • %I:%M %p')} {_eastern_tz_label()}")
    lines.append(f"  {len(picks)} plays")
    lines.append(f"{'━'*50}")

    # Render in sport order
    rendered = set()
    for sl in sport_order:
        if sl in groups:
            rendered.add(sl)
            _render_sport_group(lines, sl, sport_icons.get(sl, '🏟️'), groups[sl])
    # Any remaining sports
    for sl, gp in groups.items():
        if sl not in rendered:
            _render_sport_group(lines, sl, sport_icons.get(sl, '🏟️'), gp)

    lines.append(f"{'━'*50}")
    tu = sum(p['units'] for p in picks)
    sizes = {}
    for p in picks:
        kl = kelly_label(p['units'])
        sizes[kl] = sizes.get(kl, 0) + 1
    size_str = ' | '.join(f"{v} {k}" for k, v in sizes.items() if v > 0)
    lines.append(f"  {len(picks)} plays • {tu:.1f} total units")
    lines.append(f"  {size_str}")
    lines.append(f"{'━'*50}")
    return '\n'.join(lines)




def _render_sport_group(lines, sport_label, icon, sport_picks):
    """Render a sport group section for picks_to_text."""
    lines.append(f"")
    lines.append(f"  {icon} {sport_label}")
    lines.append(f"  {'─'*40}")
    for p in sport_picks:
        units = p['units']
        kl = kelly_label(units)
        tier_icon = {
            'MAX PLAY': '🔥', 'STRONG': '⭐', 'SOLID': '✅',
            'LEAN': '📊', 'SPRINKLE': '📋'
        }.get(kl, '📋')
        game_time = ''
        tz_label = _eastern_tz_label()
        if p['commence']:
            try:
                gt = datetime.fromisoformat(p['commence'].replace('Z','+00:00'))
                est = _to_eastern(gt)
                game_time = est.strftime('%I:%M %p') + f' {tz_label}'
            except Exception:
                game_time = ''
        lines.append(f"")
        lines.append(f"  {tier_icon} {p['selection']}")
        lines.append(f"    {p['home']} vs {p['away']} • {game_time}")
        lines.append(f"    {p['book']}  {p['odds']:+.0f}  {p['market_type']}")
        lines.append(f"    {units:.1f}u {kl}  •  Edge: {p['edge_pct']:.1f}%")
        timing = p.get('timing', '')
        if timing and timing != 'STANDARD':
            timing_label = {'EARLY': '⏰ BET EARLY', 'LATE': '⏳ BET LATE', 'HOLD': '🕐 HOLD FOR BEST LINE'}.get(timing, '')
            if timing_label:
                lines.append(f"    {timing_label}")
        ctx_summary = p.get('context')
        if ctx_summary:
            lines.append(f"    📍 {ctx_summary}")
    lines.append(f"")

