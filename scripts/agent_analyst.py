"""
agent_analyst.py — Scotty's Edge Analyst Agent

Runs after every grade and produces a plain English briefing.
No AI API needed — uses rule-based data interpretation.

What it does:
  1. Reads diagnostic data across all sports
  2. Identifies trends (improving, declining, stable)
  3. Flags actionable warnings with specific recommendations
  4. Generates a morning briefing email
  5. Tracks week-over-week performance changes

Usage:
    python agent_analyst.py                    # Print briefing
    python agent_analyst.py --email            # Email briefing
"""
import sqlite3, sys, os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def get_performance(conn, days_back=None, start_date='2026-03-04'):
    """Get performance summary for a period."""
    if days_back:
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    else:
        cutoff = start_date
    
    bets = conn.execute("""
        SELECT sport, result, pnl_units, units, side_type, spread_bucket,
               timing, context_factors, market_type, created_at
        FROM graded_bets
        WHERE DATE(created_at) >= ? AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND units >= 3.5
    """, (cutoff,)).fetchall()
    
    wins = sum(1 for b in bets if b[1] == 'WIN')
    losses = sum(1 for b in bets if b[1] == 'LOSS')
    pnl = sum(b[2] or 0 for b in bets)
    wagered = sum(b[3] or 0 for b in bets)
    
    return {
        'bets': bets, 'wins': wins, 'losses': losses, 'pnl': pnl,
        'wagered': wagered, 'total': wins + losses,
        'wp': wins / (wins + losses) * 100 if (wins + losses) > 0 else 0,
        'roi': pnl / wagered * 100 if wagered > 0 else 0,
    }


def analyze_trends(conn):
    """Compare last 3 days vs last 7 days vs all-time."""
    all_time = get_performance(conn)
    last_7 = get_performance(conn, days_back=7)
    last_3 = get_performance(conn, days_back=3)
    
    return all_time, last_7, last_3


def analyze_sport_health(conn):
    """Check each sport's current trajectory."""
    issues = []
    strengths = []
    
    sports = conn.execute("""
        SELECT DISTINCT sport FROM graded_bets
        WHERE result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND DATE(created_at) >= '2026-03-04' AND units >= 3.5
    """).fetchall()
    
    for (sport,) in sports:
        bets = conn.execute("""
            SELECT result, pnl_units, created_at FROM graded_bets
            WHERE sport=? AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
            AND DATE(created_at) >= '2026-03-04' AND units >= 3.5
            ORDER BY created_at
        """, (sport,)).fetchall()
        
        if len(bets) < 3:
            continue
        
        wins = sum(1 for b in bets if b[0] == 'WIN')
        losses = sum(1 for b in bets if b[0] == 'LOSS')
        pnl = sum(b[1] or 0 for b in bets)
        wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        
        # Check recent trend (last 5 bets)
        recent = bets[-5:] if len(bets) >= 5 else bets
        recent_w = sum(1 for b in recent if b[0] == 'WIN')
        recent_pnl = sum(b[1] or 0 for b in recent)
        
        sport_label = {
            'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
            'icehockey_nhl': 'NHL', 'baseball_ncaa': 'Baseball',
        }.get(sport, sport)
        
        if pnl > 10:
            strengths.append(f"{sport_label}: {wins}W-{losses}L, +{pnl:.0f}u — strong performer")
        elif pnl < -5 and (wins + losses) >= 5:
            issues.append(f"{sport_label}: {wins}W-{losses}L, {pnl:+.0f}u — needs attention")
        
        if recent_pnl < -10:
            issues.append(f"{sport_label}: Last {len(recent)} picks: {recent_w}W-{len(recent)-recent_w}L, {recent_pnl:+.0f}u — cold streak")
    
    return strengths, issues


def analyze_context_health(conn):
    """Find context factors that are hurting performance."""
    warnings = []
    
    bets = conn.execute("""
        SELECT context_factors, result, pnl_units FROM graded_bets
        WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED')
        AND DATE(created_at) >= '2026-03-04' AND units >= 3.5
        AND context_factors IS NOT NULL AND context_factors != ''
    """).fetchall()
    
    factor_perf = {}
    for ctx_str, result, pnl in bets:
        factors = [f.strip().split('(')[0].strip() for f in ctx_str.split('|') if f.strip()]
        for f in factors:
            if f not in factor_perf:
                factor_perf[f] = {'W': 0, 'L': 0, 'pnl': 0}
            if result == 'WIN':
                factor_perf[f]['W'] += 1
            elif result == 'LOSS':
                factor_perf[f]['L'] += 1
            factor_perf[f]['pnl'] += (pnl or 0)
    
    for f, d in factor_perf.items():
        total = d['W'] + d['L']
        if total >= 3 and d['pnl'] < -5:
            warnings.append(f"'{f}' is {d['W']}W-{d['L']}L ({d['pnl']:+.1f}u) — investigate root cause")
    
    return warnings


def analyze_volume(conn):
    """Check if pick volume is normal."""
    notes = []
    
    # Daily volume for last 5 days
    daily = conn.execute("""
        SELECT DATE(created_at), COUNT(*) FROM bets
        WHERE DATE(created_at) >= DATE('now', '-5 days')
        AND units >= 3.5
        GROUP BY DATE(created_at)
        ORDER BY DATE(created_at)
    """).fetchall()
    
    if daily:
        avg_vol = sum(d[1] for d in daily) / len(daily)
        latest = daily[-1] if daily else None
        
        if avg_vol < 2:
            notes.append(f"Low volume: averaging {avg_vol:.1f} picks/day over last {len(daily)} days")
        
        # Check for 0-pick days
        zero_days = sum(1 for d in daily if d[1] == 0)
        if zero_days >= 2:
            notes.append(f"{zero_days} no-pick days in last 5 — check if model is too restrictive")
    
    return notes


def analyze_ungraded(conn):
    """Find bets that should have been graded but weren't."""
    ungraded = conn.execute("""
        SELECT b.selection, b.sport, b.created_at FROM bets b
        WHERE b.event_id NOT IN (
            SELECT DISTINCT event_id FROM graded_bets WHERE event_id IS NOT NULL
        )
        AND DATE(b.created_at) <= DATE('now', '-1 day')
        AND DATE(b.created_at) >= DATE('now', '-5 days')
        AND b.units >= 3.5
    """).fetchall()
    
    return ungraded


def analyze_gate_health(conn):
    """Monitor new v25.18 gates by checking shadow_blocked_picks and hypothetical results.

    Gates to track:
      - MLB_SIDE_CONVICTION_GATE: blocks MLB totals with |model_spread| < 0.5
      - NHL_PACE_OVER_GATE: blocks NHL overs with fast-paced context
      - Prop separation gate: logged implicitly (props that don't fire)
      - NCAA UNDER CLV drift: high-line UNDERs with negative CLV
    """
    notes = []

    # MLB Side Conviction Gate
    mlb_blocks = conn.execute("""
        SELECT COUNT(*) FROM shadow_blocked_picks
        WHERE reason LIKE 'MLB_SIDE_CONVICTION_GATE%'
    """).fetchone()[0]
    if mlb_blocks > 0:
        notes.append(f"MLB_SIDE_CONVICTION_GATE: {mlb_blocks} blocks (review at 25)")

    # NHL Pace Over Gate
    nhl_blocks = conn.execute("""
        SELECT COUNT(*) FROM shadow_blocked_picks
        WHERE reason LIKE 'NHL_PACE_OVER_GATE%'
    """).fetchone()[0]
    if nhl_blocks > 0:
        notes.append(f"NHL_PACE_OVER_GATE: {nhl_blocks} blocks (review at 15)")

    # Prop UNDER performance (v25.18 enabled, monitor through Apr 20)
    prop_unders = conn.execute("""
        SELECT result, COUNT(*), SUM(pnl_units) FROM graded_bets
        WHERE side_type = 'PROP_UNDER' AND result IN ('WIN','LOSS')
        GROUP BY result
    """).fetchall()
    if prop_unders:
        pw = sum(r[1] for r in prop_unders if r[0] == 'WIN')
        pl = sum(r[1] for r in prop_unders if r[0] == 'LOSS')
        pp = sum(r[2] for r in prop_unders if r[2])
        notes.append(f"PROP UNDER: {pw}W-{pl}L {pp:+.1f}u (monitoring through Apr 20)")

    # NCAA UNDER CLV drift on high totals (lines > 14.0)
    ncaa_high = conn.execute("""
        SELECT COUNT(*), SUM(CASE WHEN clv < 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END)
        FROM graded_bets
        WHERE sport = 'baseball_ncaa' AND side_type = 'UNDER'
        AND line > 14.0 AND result IN ('WIN','LOSS')
    """).fetchone()
    if ncaa_high and ncaa_high[0] >= 5:
        total, neg_clv, nw, nl = ncaa_high
        notes.append(f"NCAA UNDER >14.0: {nw}W-{nl}L, {neg_clv} neg-CLV of {total} (watch for drift)")

    # v25.23: NCAA DK tight-consensus skips
    tight_skips = conn.execute("""
        SELECT COUNT(*) FROM shadow_blocked_picks
        WHERE reason LIKE 'NCAA_DK_TIGHT_SKIP%'
    """).fetchone()[0]
    if tight_skips > 0:
        notes.append(f"NCAA_DK_TIGHT_SKIP: {tight_skips} skips (market efficient, should avoid -16u)")

    # v25.35: SHARP_OPPOSES_BLOCK — NHL + NCAA Baseball only, backtest +24.91u
    so_blocks = conn.execute("""
        SELECT sport, COUNT(*) FROM shadow_blocked_picks
        WHERE reason LIKE 'SHARP_OPPOSES_BLOCK%'
        GROUP BY sport
    """).fetchall()
    if so_blocks:
        parts = [f"{s.split('_',1)[-1].upper()}:{n}" for s, n in so_blocks]
        total = sum(n for _, n in so_blocks)
        notes.append(f"SHARP_OPPOSES_BLOCK: {total} blocks ({', '.join(parts)}) — cloud agent grades counterfactual")

    # v25.36: SPREAD_FADE_FLIP — NBA + NHL, backtest +140u over 14d
    ff_picks = conn.execute("""
        SELECT sport, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) l,
               SUM(pnl_units) pnl, COUNT(*) n
        FROM graded_bets
        WHERE side_type='SPREAD_FADE_FLIP' AND result IN ('WIN','LOSS','PUSH')
        GROUP BY sport
    """).fetchall()
    if ff_picks:
        for sp, w, l, pnl, n in ff_picks:
            wr = w/(w+l)*100 if (w+l) else 0
            notes.append(f"SPREAD_FADE_FLIP {sp}: {w}W-{l}L ({wr:.0f}%) {pnl:+.1f}u — pull if <52% after 15+ picks")

    # v25.37: NBA combo prop book-arb — SHADOW MODE (logged only, no live bet).
    # Count candidates; promote to live when ≥15 shadow candidates + counterfactual
    # W/L >= 55%.
    pra_shadow = conn.execute("""
        SELECT COUNT(*) FROM shadow_blocked_picks
        WHERE reason LIKE 'PROP_BOOK_ARB_SHADOW (player_points_rebounds_assists%'
    """).fetchone()[0]
    if pra_shadow > 0:
        notes.append(f"PRA_ARB_SHADOW: {pra_shadow} candidates logged (v25.37 shadow mode). "
                     f"Grade counterfactual outcomes; promote to live at n≥15 + W/L ≥ 55%.")

    # v25.39: DATA_SPREAD (Context Model) — live for NHL + MLS + EPL
    # Backtest: NHL 14 picks 78.6% +35u, MLS 5-0 +22.73u, EPL 2-0 +9.09u
    ds_picks = conn.execute("""
        SELECT sport, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) w,
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) l,
               SUM(pnl_units) pnl, COUNT(*) n
        FROM graded_bets
        WHERE side_type='DATA_SPREAD' AND result IN ('WIN','LOSS','PUSH')
        GROUP BY sport
    """).fetchall()
    if ds_picks:
        for sp, w, l, pnl, n in ds_picks:
            wr = w/(w+l)*100 if (w+l) else 0
            notes.append(f"DATA_SPREAD {sp}: {w}W-{l}L ({wr:.0f}%) {pnl:+.1f}u — pull if <55% after 15+ picks")

    return notes


def analyze_fade_flip_strategy(conn):
    """CRITICAL: Track Option C fade-flip picks for NCAA DK.

    When our model said one direction but sharps disagreed, we FLIP to bet the opposite
    side. This is a new, small-sample experimental strategy (based on 7-pick backtest).
    We must flag any sign it's not working so we can disable before losing money.

    Returns (summary_lines, critical_alerts) — critical_alerts bubble to top of briefing.
    """
    summary = []
    alerts = []

    # Total fade-flip fires logged
    total_flips = conn.execute("""
        SELECT COUNT(*) FROM shadow_blocked_picks
        WHERE reason LIKE 'NCAA_DK_FADE_FLIP%'
    """).fetchone()[0]

    # Graded outcomes of actual fade-flipped bets — tagged either in side_type OR
    # context_factors ('FADE_FLIP: ...'). Match on either for backward compatibility.
    graded = conn.execute("""
        SELECT result, pnl_units, clv, selection, DATE(created_at) dt
        FROM graded_bets
        WHERE (side_type = 'FADE_FLIP' OR context_factors LIKE '%FADE_FLIP%')
        AND result IN ('WIN','LOSS','PUSH')
        ORDER BY created_at DESC
    """).fetchall()

    if total_flips == 0 and not graded:
        return summary, alerts

    summary.append(f"FADE_FLIP: {total_flips} picks flipped since enablement")

    if graded:
        wins = sum(1 for r in graded if r[0] == 'WIN')
        losses = sum(1 for r in graded if r[0] == 'LOSS')
        pnl = sum((r[1] or 0) for r in graded)
        avg_clv = sum((r[2] or 0) for r in graded if r[2] is not None) / max(1, sum(1 for r in graded if r[2] is not None))
        summary.append(f"FADE_FLIP graded: {wins}W-{losses}L | {pnl:+.1f}u | avg CLV {avg_clv:+.2f}")

        # CRITICAL ALERT triggers — flag any sign strategy is breaking down
        n_graded = len(graded)
        if n_graded >= 5 and pnl <= -10:
            alerts.append(f"FADE_FLIP FAILING: {wins}W-{losses}L, {pnl:+.1f}u on {n_graded} picks — consider disabling")
        if n_graded >= 3 and wins == 0:
            alerts.append(f"FADE_FLIP: 0 wins in {n_graded} graded picks — strategy may be broken")
        # Consecutive losses check
        consec = 0
        for r in graded:  # already ordered DESC
            if r[0] == 'LOSS':
                consec += 1
            else:
                break
        if consec >= 4:
            alerts.append(f"FADE_FLIP: {consec} consecutive losses — review before next fire")
        # Negative CLV on flipped picks means even this strategy isn't catching the sharps correctly
        if n_graded >= 5 and avg_clv < -0.3:
            alerts.append(f"FADE_FLIP CLV degrading: avg {avg_clv:+.2f} — sharps may no longer be predictive")

    # Pending (ungraded) fade-flip picks from today — match on side_type or context
    pending = conn.execute("""
        SELECT selection, line, odds, DATE(created_at) dt
        FROM bets
        WHERE (side_type = 'FADE_FLIP' OR context_factors LIKE '%FADE_FLIP%')
        AND (result IS NULL OR result = 'PENDING')
        AND DATE(created_at) >= DATE('now', '-2 days')
        ORDER BY created_at DESC
    """).fetchall()

    # Also track BOOK_ARB picks — aggregate + per-gate breakdown.
    # Four gates live: v25.25 (NCAA baseball totals), v25.28 (NBA totals, NHL spreads,
    # MLB spreads). Per-gate breakdown prevents one failing gate from being masked
    # by others. Each (sport, market_type) pair is its own gate.
    ba_total = conn.execute("""
        SELECT COUNT(*) FROM bets
        WHERE (side_type = 'BOOK_ARB' OR context_factors LIKE '%BOOK_ARB%')
    """).fetchone()[0]
    ba_graded = conn.execute("""
        SELECT result, pnl_units, clv, sport, market_type FROM graded_bets
        WHERE (side_type = 'BOOK_ARB' OR context_factors LIKE '%BOOK_ARB%')
        AND result IN ('WIN','LOSS','PUSH')
        ORDER BY created_at DESC
    """).fetchall()
    if ba_total > 0 or ba_graded:
        summary.append(f"BOOK_ARB: {ba_total} picks fired across all gates")
        if ba_graded:
            # Overall aggregate
            baw = sum(1 for r in ba_graded if r[0] == 'WIN')
            bal = sum(1 for r in ba_graded if r[0] == 'LOSS')
            bap = sum((r[1] or 0) for r in ba_graded)
            bac = [r[2] for r in ba_graded if r[2] is not None]
            ba_clv = sum(bac)/len(bac) if bac else 0
            summary.append(f"BOOK_ARB overall: {baw}W-{bal}L | {bap:+.1f}u | avg CLV {ba_clv:+.2f}")

            # Per-gate breakdown — group by (sport, market_type)
            SPORT_LABEL = {
                'baseball_ncaa': 'NCAA-BB', 'baseball_mlb': 'MLB',
                'basketball_nba': 'NBA', 'icehockey_nhl': 'NHL',
                'basketball_ncaab': 'NCAAB',
            }
            gates = {}
            for result, pnl, clv, sp, mt in ba_graded:
                key = (sp or '?', mt or '?')
                if key not in gates:
                    gates[key] = {'w':0, 'l':0, 'p':0, 'pnl':0.0, 'clv':[]}
                g = gates[key]
                if result == 'WIN':  g['w'] += 1
                elif result == 'LOSS': g['l'] += 1
                else: g['p'] += 1
                g['pnl'] += (pnl or 0)
                if clv is not None: g['clv'].append(clv)
            for (sp, mt), g in sorted(gates.items()):
                n = g['w'] + g['l'] + g['p']
                wr = g['w']/(g['w']+g['l'])*100 if (g['w']+g['l']) else 0
                clv_str = f"{sum(g['clv'])/len(g['clv']):+.2f}" if g['clv'] else 'n/a'
                label = f"{SPORT_LABEL.get(sp, sp)} {mt}"
                summary.append(f"  • {label}: {g['w']}W-{g['l']}L | {g['pnl']:+.1f}u | {wr:.0f}% WR | CLV {clv_str}")

            # Alerts — aggregate level
            n = len(ba_graded)
            if n >= 5 and bap <= -10:
                alerts.append(f"BOOK_ARB FAILING (aggregate): {baw}W-{bal}L, {bap:+.1f}u on {n}")
            if n >= 4 and baw == 0:
                alerts.append(f"BOOK_ARB: 0 wins in {n} graded picks")
            if n >= 5 and ba_clv < -0.3:
                alerts.append(f"BOOK_ARB CLV degrading (aggregate): {ba_clv:+.2f}")

            # Alerts — per-gate level, fire earlier so one bad gate can't hide
            # behind winning gates
            for (sp, mt), g in gates.items():
                gn = g['w'] + g['l'] + g['p']
                label = f"{SPORT_LABEL.get(sp, sp)} {mt}"
                if gn >= 5 and g['pnl'] <= -10:
                    alerts.append(f"BOOK_ARB {label} FAILING: {g['w']}W-{g['l']}L, {g['pnl']:+.1f}u on {gn} — consider disabling this gate")
                if gn >= 4 and g['w'] == 0:
                    alerts.append(f"BOOK_ARB {label}: 0 wins in {gn} graded picks — urgent review")
                if gn >= 8 and g['w']/(g['w']+g['l']) < 0.55 and (g['w']+g['l']) > 0:
                    wr = g['w']/(g['w']+g['l'])*100
                    alerts.append(f"BOOK_ARB {label} WR low: {wr:.1f}% on {gn} (backtest 65-85%)")
    if pending:
        summary.append(f"FADE_FLIP pending grade: {len(pending)} pick(s) from last 2 days")
        for sel, line, odds, dt in pending[:3]:
            summary.append(f"  • {dt} {sel} @ {odds}")

    # Also track DIV_EXPANDED picks (v25.29 — NHL divergence threshold 1.5→2.5)
    dx_total = conn.execute("""
        SELECT COUNT(*) FROM bets
        WHERE (side_type = 'DIV_EXPANDED' OR context_factors LIKE '%DIV EXPANDED%')
    """).fetchone()[0]
    dx_graded = conn.execute("""
        SELECT result, pnl_units, clv FROM graded_bets
        WHERE (side_type = 'DIV_EXPANDED' OR context_factors LIKE '%DIV EXPANDED%')
        AND result IN ('WIN','LOSS','PUSH')
        ORDER BY created_at DESC
    """).fetchall()
    if dx_total > 0 or dx_graded:
        summary.append(f"DIV_EXPANDED (NHL v25.29): {dx_total} picks fired since enablement")
        if dx_graded:
            dxw = sum(1 for r in dx_graded if r[0] == 'WIN')
            dxl = sum(1 for r in dx_graded if r[0] == 'LOSS')
            dxp = sum((r[1] or 0) for r in dx_graded)
            dxc = [r[2] for r in dx_graded if r[2] is not None]
            dx_clv = sum(dxc)/len(dxc) if dxc else 0
            wr = dxw/(dxw+dxl)*100 if (dxw+dxl) else 0
            summary.append(f"DIV_EXPANDED graded: {dxw}W-{dxl}L | {dxp:+.1f}u | {wr:.1f}% WR | avg CLV {dx_clv:+.2f}")
            n = len(dx_graded)
            # Backtest basis: 17-3 = 85% WR, +27u at 3.5u sizing.
            # Alert thresholds reflect that baseline.
            if n >= 5 and dxp <= -10:
                alerts.append(f"DIV_EXPANDED FAILING: {dxw}W-{dxl}L, {dxp:+.1f}u on {n} — consider reverting NHL threshold 2.5→1.5")
            if n >= 8 and wr < 65:
                alerts.append(f"DIV_EXPANDED WR drop: {wr:.1f}% WR on {n} picks (backtest was 85%) — monitor carefully")
            if n >= 4 and dxw == 0:
                alerts.append(f"DIV_EXPANDED: 0 wins in {n} graded picks — urgent review")
            if n >= 5 and dx_clv < -0.3:
                alerts.append(f"DIV_EXPANDED CLV degrading: {dx_clv:+.2f} — edge may be closing")

    return summary, alerts


def generate_briefing(conn):
    """Generate the full morning briefing."""
    lines = []
    now = datetime.now()
    
    lines.append("=" * 60)
    lines.append(f"  SCOTTY'S EDGE — MORNING BRIEFING")
    lines.append(f"  {now.strftime('%A, %B %d, %Y  %I:%M %p')}")
    lines.append("=" * 60)
    
    # Overall performance
    all_time, last_7, last_3 = analyze_trends(conn)
    
    lines.append(f"\n  RECORD: {all_time['wins']}W-{all_time['losses']}L | "
                 f"{all_time['pnl']:+.1f}u | {all_time['wp']:.1f}% | ROI {all_time['roi']:+.1f}%")
    
    if last_7['total'] > 0:
        lines.append(f"  Last 7 days: {last_7['wins']}W-{last_7['losses']}L | {last_7['pnl']:+.1f}u")
    if last_3['total'] > 0:
        lines.append(f"  Last 3 days: {last_3['wins']}W-{last_3['losses']}L | {last_3['pnl']:+.1f}u")
    
    # Trend assessment
    if last_3['total'] >= 2:
        if last_3['pnl'] > 5:
            lines.append(f"\n  TREND: Hot streak — model is clicking")
        elif last_3['pnl'] < -5:
            lines.append(f"\n  TREND: Cold stretch — stay disciplined, review diagnostics")
        else:
            lines.append(f"\n  TREND: Steady — model performing as expected")
    
    # Sport health
    strengths, issues = analyze_sport_health(conn)
    if strengths or issues:
        lines.append(f"\n  SPORT HEALTH:")
        for s in strengths:
            lines.append(f"    + {s}")
        for i in issues:
            lines.append(f"    ! {i}")
    
    # Context warnings
    ctx_warnings = analyze_context_health(conn)
    if ctx_warnings:
        lines.append(f"\n  CONTEXT WARNINGS:")
        for w in ctx_warnings:
            lines.append(f"    ! {w}")
    
    # Volume check
    vol_notes = analyze_volume(conn)
    if vol_notes:
        lines.append(f"\n  VOLUME:")
        for n in vol_notes:
            lines.append(f"    ! {n}")
    
    # Ungraded bets
    ungraded = analyze_ungraded(conn)
    if ungraded:
        lines.append(f"\n  UNGRADED BETS ({len(ungraded)}):")
        for sel, sport, dt in ungraded[:5]:
            lines.append(f"    ? {sel} ({sport}) — {dt[:10]}")
        if len(ungraded) > 5:
            lines.append(f"    ... and {len(ungraded) - 5} more")
    
    # v25.18 Gate monitoring
    gate_notes = analyze_gate_health(conn)
    if gate_notes:
        lines.append(f"\n  GATE MONITORING (v25.18):")
        for g in gate_notes:
            lines.append(f"    > {g}")

    # v25.23 / Option C — CRITICAL fade-flip monitoring
    fade_summary, fade_alerts = analyze_fade_flip_strategy(conn)
    if fade_alerts:
        lines.append(f"\n  🚨 CRITICAL — OPTION C (FADE_FLIP) ALERTS:")
        for a in fade_alerts:
            lines.append(f"    !! {a}")
    if fade_summary:
        lines.append(f"\n  OPTION C / BOOK_ARB MONITOR (NCAA experimental strategies):")
        for s in fade_summary:
            lines.append(f"    > {s}")

    # Today's outlook
    day_name = now.strftime('%A')
    if day_name == 'Monday':
        lines.append(f"\n  TODAY'S OUTLOOK: Monday — lightest slate of the week. 0-2 picks expected.")
    elif day_name in ('Tuesday', 'Wednesday', 'Thursday'):
        lines.append(f"\n  TODAY'S OUTLOOK: {day_name} — moderate slate. NCAA tournament games if in season.")
    elif day_name == 'Friday':
        lines.append(f"\n  TODAY'S OUTLOOK: Friday — full slate across NBA/NHL/Baseball.")
    elif day_name == 'Saturday':
        lines.append(f"\n  TODAY'S OUTLOOK: Saturday — biggest slate of the week. All sports active.")
    else:
        lines.append(f"\n  TODAY'S OUTLOOK: Sunday — moderate slate, fewer college games.")
    
    # Action items
    action_items = []
    if ctx_warnings:
        action_items.append("Review context warnings in next session")
    if ungraded:
        action_items.append(f"Grade {len(ungraded)} missing bets (ESPN may not have scores)")
    if vol_notes:
        action_items.append("Investigate low volume if it continues 2+ more days")
    if fade_alerts:
        action_items.append("URGENT: Review Option C (FADE_FLIP) — alerts above suggest strategy may be breaking")
    
    if action_items:
        lines.append(f"\n  ACTION ITEMS:")
        for a in action_items:
            lines.append(f"    > {a}")
    else:
        lines.append(f"\n  No action items. System is running clean.")
    
    lines.append("\n" + "=" * 60)
    
    return "\n".join(lines)


if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    briefing = generate_briefing(conn)
    print(briefing)
    
    if '--email' in sys.argv:
        try:
            from emailer import send_email
            today = datetime.now().strftime('%Y-%m-%d')
            send_email(f"Morning Briefing - {today}", briefing)
            print("\n  Email sent")
        except Exception as e:
            print(f"\n  Email failed: {e}")
    
    conn.close()
