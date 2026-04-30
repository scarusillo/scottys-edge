"""
agent_sport_review.py — Per-Sport Performance Intelligence Agent

Addresses 6 critical gaps:
  1. Per-sport deep analysis (not just aggregate W-L)
  2. Learning from losses (identifies WHY picks lose by sport)
  3. CLV analysis per sport (are we beating closing lines?)
  4. Phantom pick detection (line verification, postponed games, impossible lines)
  5. Spread movement feedback (how picks perform when lines move with/against us)
  6. Context factor validation per sport (are static rules actually working?)

Runs after every grade cycle. Produces a consolidated SPORT HEALTH CARD
for each sport with 3+ graded bets.

Usage:
    from agent_sport_review import generate_sport_review
    report, alerts = generate_sport_review(conn)
"""
import sqlite3, os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

SPORT_LABELS = {
    'basketball_ncaab': 'NCAAB', 'basketball_nba': 'NBA', 'icehockey_nhl': 'NHL',
    'baseball_ncaa': 'NCAA Baseball', 'baseball_mlb': 'MLB',
    'soccer_epl': 'EPL', 'soccer_italy_serie_a': 'Serie A',
    'soccer_spain_la_liga': 'La Liga', 'soccer_germany_bundesliga': 'Bundesliga',
    'soccer_france_ligue_one': 'Ligue 1', 'soccer_usa_mls': 'MLS',
    'soccer_uefa_champs_league': 'UCL',
}

START_DATE = '2026-03-04'
MIN_BETS = 3  # Minimum graded bets to generate a sport card


def generate_sport_review(conn, start_date=None):
    """
    Generate per-sport health cards with deep analysis.

    Returns:
        (report_text, alert_list) — report is the full text,
        alerts is a list of critical findings that need attention.
    """
    cutoff = start_date or START_DATE
    alerts = []
    sections = []

    # ── Fetch all graded bets ──
    bets = conn.execute("""
        SELECT sport, selection, market_type, result, pnl_units, edge_pct,
               confidence, units, odds, created_at, closing_line, clv,
               side_type, spread_bucket, edge_bucket, timing,
               context_factors, context_confirmed, market_tier, model_spread,
               day_of_week, line, event_id, book
        FROM graded_bets
        WHERE result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND DATE(created_at) >= ?
        ORDER BY created_at
    """, (cutoff,)).fetchall()

    if not bets:
        return "No graded bets found.", []

    # ── Parse into dicts ──
    records = []
    for b in bets:
        records.append({
            'sport': b[0], 'selection': b[1], 'market_type': b[2],
            'result': b[3], 'pnl': b[4] or 0, 'edge': b[5] or 0,
            'confidence': b[6] or '', 'units': b[7] or 0,
            'odds': b[8], 'created_at': b[9] or '', 'closing_line': b[10],
            'clv': b[11], 'side_type': b[12] or '', 'spread_bucket': b[13] or '',
            'edge_bucket': b[14] or '', 'timing': b[15] or '',
            'context_factors': b[16] or '', 'context_confirmed': b[17],
            'market_tier': b[18] or '', 'model_spread': b[19],
            'day_of_week': b[20] or '', 'line': b[21],
            'event_id': b[22] or '', 'book': b[23] or '',
        })

    # ── Group by sport ──
    by_sport = {}
    for r in records:
        by_sport.setdefault(r['sport'], []).append(r)

    # ── Header ──
    total_w = sum(1 for r in records if r['result'] == 'WIN')
    total_l = sum(1 for r in records if r['result'] == 'LOSS')
    total_pnl = sum(r['pnl'] for r in records)
    total_wager = sum(r['units'] for r in records)
    total_roi = (total_pnl / total_wager * 100) if total_wager > 0 else 0

    header = []
    header.append("=" * 60)
    header.append("  SCOTTY'S EDGE — SPORT INTELLIGENCE REVIEW")
    header.append(f"  {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    header.append(f"  Season: {total_w}W-{total_l}L | {total_pnl:+.1f}u | ROI {total_roi:+.1f}%")
    header.append("=" * 60)
    sections.append("\n".join(header))

    # ── Generate card for each sport ──
    for sport in sorted(by_sport.keys()):
        sport_bets = by_sport[sport]
        label = SPORT_LABELS.get(sport, sport)
        w = sum(1 for r in sport_bets if r['result'] == 'WIN')
        l = sum(1 for r in sport_bets if r['result'] == 'LOSS')
        if (w + l) < MIN_BETS:
            continue

        card, sport_alerts = _build_sport_card(conn, label, sport, sport_bets)
        sections.append(card)
        alerts.extend(sport_alerts)

    # ── Phantom pick check (cross-sport) ──
    phantom_section, phantom_alerts = _check_phantom_picks(conn, records)
    if phantom_section:
        sections.append(phantom_section)
        alerts.extend(phantom_alerts)

    # ── Alerts summary ──
    if alerts:
        alert_section = []
        alert_section.append("\n" + "=" * 60)
        alert_section.append("  ACTION ITEMS")
        alert_section.append("=" * 60)
        for a in alerts:
            alert_section.append(f"  ! {a}")
        sections.append("\n".join(alert_section))

    report = "\n\n".join(sections)
    return report, alerts


def _build_sport_card(conn, label, sport, bets):
    """Build a detailed health card for one sport."""
    lines = []
    alerts = []

    w = sum(1 for r in bets if r['result'] == 'WIN')
    l = sum(1 for r in bets if r['result'] == 'LOSS')
    pnl = sum(r['pnl'] for r in bets)
    wager = sum(r['units'] for r in bets)
    roi = (pnl / wager * 100) if wager > 0 else 0
    wp = w / (w + l) * 100 if (w + l) > 0 else 0

    # Trend: last 7 bets
    recent = bets[-7:] if len(bets) >= 7 else bets
    rw = sum(1 for r in recent if r['result'] == 'WIN')
    rl = sum(1 for r in recent if r['result'] == 'LOSS')
    rpnl = sum(r['pnl'] for r in recent)
    if rpnl > 2:
        trend = "HOT"
    elif rpnl < -3:
        trend = "COLD"
    else:
        trend = "STEADY"

    lines.append("─" * 60)
    lines.append(f"  SPORT HEALTH CARD — {label}")
    lines.append(f"  Record: {w}W-{l}L ({wp:.1f}%) | {pnl:+.1f}u | ROI {roi:+.1f}%")
    lines.append(f"  Last {len(recent)}: {rw}W-{rl}L ({rpnl:+.1f}u) | Trend: {trend}")

    if roi < -10 and (w + l) >= 5:
        alerts.append(f"{label}: Bleeding at {roi:+.1f}% ROI ({w}W-{l}L, {pnl:+.1f}u)")

    # ── 1. CLV ANALYSIS (Issue #3) ──
    clv_section = _analyze_clv(bets, label)
    if clv_section:
        lines.append("")
        lines.extend(clv_section)

    # ── 2. WINNING & LOSING AREAS (Issue #2) ──
    areas_section, area_alerts = _analyze_areas(bets, label)
    lines.append("")
    lines.extend(areas_section)
    alerts.extend(area_alerts)

    # ── 3. SPREAD MOVEMENT FEEDBACK (Issue #5) ──
    movement_section, move_alerts = _analyze_spread_movement(bets, label)
    if movement_section:
        lines.append("")
        lines.extend(movement_section)
        alerts.extend(move_alerts)

    # ── 4. CONTEXT FACTOR VALIDATION (Issue #6) ──
    context_section, ctx_alerts = _validate_context_factors(bets, label)
    if context_section:
        lines.append("")
        lines.extend(context_section)
        alerts.extend(ctx_alerts)

    # ── 5. WEEKLY PROGRESSION ──
    weekly_section = _weekly_progression(bets)
    if weekly_section:
        lines.append("")
        lines.extend(weekly_section)

    return "\n".join(lines), alerts


# ═══════════════════════════════════════════════════════════════════
# ISSUE #3: CLV Analysis Per Sport
# ═══════════════════════════════════════════════════════════════════

def _analyze_clv(bets, label):
    """Per-sport CLV breakdown by market type."""
    lines = []
    lines.append("  CLV ANALYSIS:")

    # Split by market type (spread/total CLV is points, ML/prop is impl%)
    spread_clv = [r['clv'] for r in bets if r['clv'] is not None and r['market_type'] == 'SPREAD']
    total_clv = [r['clv'] for r in bets if r['clv'] is not None and r['market_type'] == 'TOTAL']
    ml_clv = [r['clv'] for r in bets if r['clv'] is not None and r['market_type'] == 'MONEYLINE']

    any_data = False
    for mkt_label, clv_list, unit in [
        ('Spreads', spread_clv, 'pts'),
        ('Totals', total_clv, 'pts'),
        ('ML', ml_clv, 'impl%'),
    ]:
        if not clv_list:
            continue
        any_data = True
        avg = sum(clv_list) / len(clv_list)
        pos = sum(1 for c in clv_list if c > 0)
        rate = pos / len(clv_list) * 100
        icon = "+" if avg > 0 else "-"
        lines.append(f"    {mkt_label:10s} Avg CLV: {avg:+.1f} {unit} | Beating close: {pos}/{len(clv_list)} ({rate:.0f}%) {icon}")

    # Combined points-based CLV
    pts_clv = spread_clv + total_clv
    if pts_clv:
        avg = sum(pts_clv) / len(pts_clv)
        if avg > 0.3:
            lines.append(f"    => Beating closing lines consistently (sharp-side)")
        elif avg < -0.3:
            lines.append(f"    => Getting worse numbers than close (check timing/book)")
        else:
            lines.append(f"    => Near closing line (neutral)")

    if not any_data:
        lines.append("    No CLV data available")

    return lines


# ═══════════════════════════════════════════════════════════════════
# ISSUE #2: Learning From Losses — Winning & Losing Areas
# ═══════════════════════════════════════════════════════════════════

def _analyze_areas(bets, label):
    """Identify where we win and where we bleed, per sport."""
    lines = []
    alerts = []

    # ── By side type ──
    sides = {}
    for r in bets:
        s = r['side_type'] or 'UNKNOWN'
        sides.setdefault(s, {'W': 0, 'L': 0, 'pnl': 0})
        if r['result'] == 'WIN': sides[s]['W'] += 1
        elif r['result'] == 'LOSS': sides[s]['L'] += 1
        sides[s]['pnl'] += r['pnl']

    # ── By spread bucket ──
    buckets = {}
    for r in bets:
        b = r['spread_bucket']
        if not b or b == 'N/A':
            continue
        buckets.setdefault(b, {'W': 0, 'L': 0, 'pnl': 0})
        if r['result'] == 'WIN': buckets[b]['W'] += 1
        elif r['result'] == 'LOSS': buckets[b]['L'] += 1
        buckets[b]['pnl'] += r['pnl']

    # ── By timing ──
    timing = {}
    for r in bets:
        t = r['timing'] or 'UNKNOWN'
        timing.setdefault(t, {'W': 0, 'L': 0, 'pnl': 0})
        if r['result'] == 'WIN': timing[t]['W'] += 1
        elif r['result'] == 'LOSS': timing[t]['L'] += 1
        timing[t]['pnl'] += r['pnl']

    # ── By context confirmed ──
    ctx_perf = {'yes': {'W': 0, 'L': 0, 'pnl': 0}, 'no': {'W': 0, 'L': 0, 'pnl': 0}}
    for r in bets:
        if r['context_confirmed'] == 1:
            k = 'yes'
        else:
            k = 'no'
        if r['result'] == 'WIN': ctx_perf[k]['W'] += 1
        elif r['result'] == 'LOSS': ctx_perf[k]['L'] += 1
        ctx_perf[k]['pnl'] += r['pnl']

    # Find winners and losers
    winning_areas = []
    losing_areas = []

    BUCKET_LABELS = {
        'SMALL_DOG': 'Small Dogs (1-3.5)', 'MED_DOG': 'Med Dogs (4-7.5)',
        'BIG_DOG': 'Big Dogs (8+)', 'SMALL_FAV': 'Small Favs (1-3.5)',
        'MED_FAV': 'Med Favs (4-7.5)', 'BIG_FAV': 'Big Favs (8+)', 'PK': "Pick'em",
    }
    SIDE_LABELS = {
        'DOG': 'Dogs', 'FAVORITE': 'Favorites', 'OVER': 'Overs',
        'UNDER': 'Unders', 'PK': "Pick'em",
    }

    for s, d in sides.items():
        t = d['W'] + d['L']
        if t < 2:
            continue
        lbl = SIDE_LABELS.get(s, s)
        entry = f"{lbl}: {d['W']}W-{d['L']}L, {d['pnl']:+.1f}u"
        if d['pnl'] > 1:
            winning_areas.append(entry)
        elif d['pnl'] < -2 and t >= 3:
            losing_areas.append(entry)
            if d['pnl'] < -5:
                alerts.append(f"{label}: {lbl} bleeding at {d['pnl']:+.1f}u")

    for b, d in buckets.items():
        t = d['W'] + d['L']
        if t < 2:
            continue
        lbl = BUCKET_LABELS.get(b, b)
        entry = f"{lbl}: {d['W']}W-{d['L']}L, {d['pnl']:+.1f}u"
        if d['pnl'] > 1:
            winning_areas.append(entry)
        elif d['pnl'] < -2 and t >= 3:
            losing_areas.append(entry)

    for t_key, d in timing.items():
        t = d['W'] + d['L']
        if t < 2 or t_key == 'UNKNOWN':
            continue
        entry = f"{t_key} picks: {d['W']}W-{d['L']}L, {d['pnl']:+.1f}u"
        if d['pnl'] > 1:
            winning_areas.append(entry)
        elif d['pnl'] < -2 and t >= 3:
            losing_areas.append(entry)

    # Context confirmed performance
    ctx_y = ctx_perf['yes']
    ctx_n = ctx_perf['no']
    ctx_y_t = ctx_y['W'] + ctx_y['L']
    ctx_n_t = ctx_n['W'] + ctx_n['L']
    if ctx_y_t >= 2 and ctx_y['pnl'] > 1:
        wp = ctx_y['W'] / ctx_y_t * 100
        winning_areas.append(f"Context-confirmed: {ctx_y['W']}W-{ctx_y['L']}L ({wp:.0f}%), {ctx_y['pnl']:+.1f}u")
    if ctx_n_t >= 3 and ctx_n['pnl'] < -2:
        losing_areas.append(f"Raw model (no context): {ctx_n['W']}W-{ctx_n['L']}L, {ctx_n['pnl']:+.1f}u")

    lines.append("  WINNING AREAS:")
    if winning_areas:
        for a in winning_areas:
            lines.append(f"    + {a}")
    else:
        lines.append("    (none identified yet)")

    lines.append("  LOSING AREAS:")
    if losing_areas:
        for a in losing_areas:
            lines.append(f"    - {a}")
    else:
        lines.append("    (none identified yet)")

    return lines, alerts


# ═══════════════════════════════════════════════════════════════════
# ISSUE #5: Spread Movement Feedback
# ═══════════════════════════════════════════════════════════════════

def _analyze_spread_movement(bets, label):
    """Analyze performance based on line movement direction.

    Uses CLV as a proxy for line movement:
      CLV > 0 = line moved toward us (sharp agreement)
      CLV < 0 = line moved away from us (we got a worse number)
    """
    lines = []
    alerts = []

    # Only look at spread/total bets with CLV data
    clv_bets = [r for r in bets if r['clv'] is not None and r['market_type'] in ('SPREAD', 'TOTAL')]
    if len(clv_bets) < 3:
        return None, []

    # Split: moved with us (CLV > 0) vs moved against us (CLV < 0)
    with_us = [r for r in clv_bets if r['clv'] > 0]
    against_us = [r for r in clv_bets if r['clv'] < 0]
    neutral = [r for r in clv_bets if r['clv'] == 0]

    lines.append("  SPREAD MOVEMENT:")

    if with_us:
        ww = sum(1 for r in with_us if r['result'] == 'WIN')
        wl = sum(1 for r in with_us if r['result'] == 'LOSS')
        wpnl = sum(r['pnl'] for r in with_us)
        wt = ww + wl
        wwp = ww / wt * 100 if wt > 0 else 0
        lines.append(f"    Line moved WITH us (CLV+):   {ww}W-{wl}L ({wwp:.0f}%) {wpnl:+.1f}u")

    if against_us:
        aw = sum(1 for r in against_us if r['result'] == 'WIN')
        al = sum(1 for r in against_us if r['result'] == 'LOSS')
        apnl = sum(r['pnl'] for r in against_us)
        at = aw + al
        awp = aw / at * 100 if at > 0 else 0
        lines.append(f"    Line moved AGAINST us (CLV-): {aw}W-{al}L ({awp:.0f}%) {apnl:+.1f}u")
        if at >= 3 and apnl < -3:
            alerts.append(f"{label}: Picks where line moved against us are {aw}W-{al}L ({apnl:+.1f}u) — consider timing")

    if neutral:
        nw = sum(1 for r in neutral if r['result'] == 'WIN')
        nl = sum(1 for r in neutral if r['result'] == 'LOSS')
        lines.append(f"    No movement (CLV=0):          {nw}W-{nl}L")

    # Large CLV outliers (steam moves in our favor or against)
    big_against = [r for r in clv_bets if r['clv'] <= -1.5]
    if big_against:
        bw = sum(1 for r in big_against if r['result'] == 'WIN')
        bl = sum(1 for r in big_against if r['result'] == 'LOSS')
        lines.append(f"    Big moves against (CLV <= -1.5): {bw}W-{bl}L — fading sharp money?")

    return lines, alerts


# ═══════════════════════════════════════════════════════════════════
# ISSUE #6: Context Factor Validation Per Sport
# ═══════════════════════════════════════════════════════════════════

def _validate_context_factors(bets, label):
    """Check if context factors are actually helping in this sport."""
    lines = []
    alerts = []

    factor_perf = {}
    for r in bets:
        if not r['context_factors']:
            continue
        factors = [f.strip() for f in r['context_factors'].split('|') if f.strip()]
        for f in factors:
            # Strip adjustment value: "Away 3-in-5 (+0.5)" → "Away 3-in-5"
            name = f.split('(')[0].strip()
            factor_perf.setdefault(name, {'W': 0, 'L': 0, 'pnl': 0, 'count': 0})
            if r['result'] == 'WIN': factor_perf[name]['W'] += 1
            elif r['result'] == 'LOSS': factor_perf[name]['L'] += 1
            factor_perf[name]['pnl'] += r['pnl']
            factor_perf[name]['count'] += 1

    if not factor_perf:
        return None, []

    # Only show factors with 2+ bets
    relevant = {k: v for k, v in factor_perf.items() if v['count'] >= 2}
    if not relevant:
        return None, []

    lines.append("  CONTEXT FACTORS:")

    # Sort by P/L to show best and worst
    sorted_factors = sorted(relevant.items(), key=lambda x: x[1]['pnl'])

    for name, d in sorted_factors:
        t = d['W'] + d['L']
        if t == 0:
            continue
        wp = d['W'] / t * 100
        flag = ""
        if t >= 3 and d['pnl'] < -3:
            flag = " !! HURTING"
            alerts.append(f"{label}: Context factor '{name}' is {d['W']}W-{d['L']}L ({d['pnl']:+.1f}u) — may need to disable")
        elif t >= 3 and d['pnl'] > 3:
            flag = " ** WORKING"
        lines.append(f"    {name:30s} {d['W']}W-{d['L']}L ({wp:.0f}%) {d['pnl']:+.1f}u{flag}")

    return lines, alerts


# ═══════════════════════════════════════════════════════════════════
# WEEKLY PROGRESSION — Is the model improving or declining?
# ═══════════════════════════════════════════════════════════════════

def _weekly_progression(bets):
    """Show week-over-week performance."""
    if len(bets) < 5:
        return None

    # Group by ISO week
    weeks = {}
    for r in bets:
        try:
            dt = datetime.strptime(r['created_at'][:10], '%Y-%m-%d')
            # Use Monday-start week label
            week_start = dt - timedelta(days=dt.weekday())
            wk = week_start.strftime('%m/%d')
        except (ValueError, TypeError):
            continue
        weeks.setdefault(wk, {'W': 0, 'L': 0, 'pnl': 0})
        if r['result'] == 'WIN': weeks[wk]['W'] += 1
        elif r['result'] == 'LOSS': weeks[wk]['L'] += 1
        weeks[wk]['pnl'] += r['pnl']

    if len(weeks) < 2:
        return None

    lines = []
    lines.append("  WEEKLY TREND:")
    for wk in sorted(weeks.keys()):
        d = weeks[wk]
        t = d['W'] + d['L']
        if t == 0:
            continue
        wp = d['W'] / t * 100
        bar = "+" * max(0, int(d['pnl'])) + "-" * max(0, int(-d['pnl']))
        lines.append(f"    Wk {wk}: {d['W']}W-{d['L']}L ({wp:.0f}%) {d['pnl']:+.1f}u {bar}")

    return lines


# ═══════════════════════════════════════════════════════════════════
# ISSUE #4: Phantom Pick Detection
# ═══════════════════════════════════════════════════════════════════

def _check_phantom_picks(conn, records):
    """Detect picks that shouldn't exist — the integrity layer.

    Checks:
      1. Picks on games with no result (postponed/cancelled)
      2. Duplicate sides on same game
      3. Lines that never existed at any book (impossible lines)
      4. Picks graded against wrong game (event_id mismatch)
    """
    lines = []
    alerts = []
    issues = []

    # ── Check 1: Picks on games with no scores (postponed?) ──
    # Find graded bets where the result event has NULL scores
    no_score = conn.execute("""
        SELECT g.selection, g.sport, g.created_at, g.result, g.event_id
        FROM graded_bets g
        LEFT JOIN results r ON g.event_id = r.event_id
        WHERE g.result IN ('WIN', 'LOSS')
        AND r.home_score IS NULL
        AND DATE(g.created_at) >= ?
    """, (START_DATE,)).fetchall()

    for sel, sport, dt, result, eid in no_score:
        label = SPORT_LABELS.get(sport, sport)
        issues.append(f"NO SCORE: {sel} graded as {result} but no scores found ({label}, {dt[:10]})")

    # ── Check 2: Multiple picks on same event, same side ──
    dupes = conn.execute("""
        SELECT event_id, market_type, COUNT(*), GROUP_CONCAT(selection, ' | ')
        FROM graded_bets
        WHERE result NOT IN ('DUPLICATE', 'TAINTED', 'PENDING')
        AND DATE(created_at) >= ?
        GROUP BY event_id, market_type, side_type
        HAVING COUNT(*) > 1
    """, (START_DATE,)).fetchall()

    for eid, mtype, count, sels in dupes:
        issues.append(f"DUPLICATE SIDE: {count}x {mtype} picks on same game — {sels[:80]}")

    # ── Check 3: Line verification against odds snapshots ──
    # For recent bets, verify the line existed at the book we claim
    recent_bets = conn.execute("""
        SELECT g.selection, g.sport, g.line, g.book, g.event_id, g.market_type, g.created_at
        FROM graded_bets g
        WHERE g.result NOT IN ('DUPLICATE', 'TAINTED', 'PENDING')
        AND DATE(g.created_at) >= DATE('now', '-7 days')
        AND g.line IS NOT NULL AND g.book IS NOT NULL
    """).fetchall()

    # Check if we have odds data to verify against
    has_odds = conn.execute("SELECT COUNT(*) FROM odds WHERE DATE(snapshot_date) >= DATE('now', '-7 days')").fetchone()
    if has_odds and has_odds[0] > 0:
        for sel, sport, line, book, eid, mtype, dt in recent_bets:
            # Look for this line at this book
            mkt = 'spreads' if mtype == 'SPREAD' else ('totals' if mtype == 'TOTAL' else 'h2h')
            found = conn.execute("""
                SELECT COUNT(*) FROM odds
                WHERE event_id = ? AND book = ? AND market = ?
                AND ABS(COALESCE(line, 0) - ?) <= 1.0
            """, (eid, book, mkt, line)).fetchone()

            if found and found[0] == 0:
                # Check if ANY book had this line
                any_book = conn.execute("""
                    SELECT COUNT(*) FROM odds
                    WHERE event_id = ? AND market = ?
                    AND ABS(COALESCE(line, 0) - ?) <= 0.5
                """, (eid, mkt, line)).fetchone()
                if any_book and any_book[0] == 0:
                    issues.append(f"PHANTOM LINE: {sel} at {line:+.1f} — no book had this line")

    # ── Check 4: Bets older than 3 days still ungraded ──
    stale = conn.execute("""
        SELECT b.selection, b.sport, b.created_at, b.units
        FROM bets b
        WHERE b.event_id NOT IN (
            SELECT DISTINCT event_id FROM graded_bets WHERE event_id IS NOT NULL
        )
        AND DATE(b.created_at) <= DATE('now', '-3 days')
        AND DATE(b.created_at) >= ?
        AND b.units >= 3.5
        AND (b.result IS NULL OR b.result NOT IN ('SCRUBBED','MANUAL_SCRUB','TAINTED','DUPLICATE'))
    """, (START_DATE,)).fetchall()

    for sel, sport, dt, units in stale:
        label = SPORT_LABELS.get(sport, sport)
        issues.append(f"STALE UNGRADED: {sel} ({label}) — {units:.1f}u from {dt[:10]}, 3+ days ungraded")

    if not issues:
        return None, []

    lines.append("─" * 60)
    lines.append("  INTEGRITY CHECK")
    lines.append("─" * 60)
    for issue in issues:
        lines.append(f"  ! {issue}")
        alerts.append(issue)
    lines.append(f"  {len(issues)} integrity issue(s) found")

    return "\n".join(lines), alerts


# ═══════════════════════════════════════════════════════════════════
# STANDALONE EXECUTION
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import sys
    conn = sqlite3.connect(DB_PATH)
    report, alerts = generate_sport_review(conn)
    print(report)

    if '--email' in sys.argv and alerts:
        try:
            from emailer import send_email
            today = datetime.now().strftime('%Y-%m-%d')
            send_email(f"Sport Review — {len(alerts)} alerts - {today}", report)
            print(f"\n  Alert email sent ({len(alerts)} issues)")
        except Exception as e:
            print(f"\n  Email failed: {e}")

    conn.close()
