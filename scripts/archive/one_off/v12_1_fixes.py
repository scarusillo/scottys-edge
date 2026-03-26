"""
v12_1_fixes.py — Scotty's Edge v12.1: Four Targeted Fixes

Based on performance data analysis (55.2% win rate, +5.6% ROI, 7-day window):

FIX 1: Totals — Use probability edge for Kelly sizing, not PV
  Problem: kelly_units uses max(pv, prob_edge) for totals. PV is almost always
  higher, inflating unit sizes. Spreads already use actual_edge via _mk().
  Data: Unders 3W-6L (-14.9u). Totals overall 7W-8L.
  Fix: Use prob_edge for edge_pct AND Kelly. Add +3% under threshold.

FIX 2: Star rating — Calculate from actual probability edge, not PV
  Problem: _mk() stores actual_edge as edge_pct but PV-based star_rating.
  The 2.0★ filter uses stars, so picks with PV=18% / real edge=8% pass.
  Data: Edge 16-20% bucket is 8W-10L (-11.7u) — inflated PV, thin real edge.
  Fix: Recalculate star_rating from actual_edge in _mk(). Lower floor to 1.5★.

FIX 3: Remove spread_diff totals adjustment
  Problem: model_total -= 2 for blowouts creates systematic under lean.
  Data: Overs 4W-2L (+5.4u), Unders 3W-6L (-14.9u). Heuristic hurts.
  Fix: Remove the spread_diff adjustment entirely. Let scoring data speak.

FIX 4: Remove favorite exemption from early bet penalty
  Problem: Favorites exempted from +5% early penalty per Walters rule.
  Data: Favorites 3W-4L (-7.3u). The exemption lets losing picks through.
  Fix: Apply +5% penalty to ALL early bets (except soccer).

Usage:
    python v12_1_fixes.py              # Preview changes
    python v12_1_fixes.py --apply      # Apply with backups
    python v12_1_fixes.py --rollback   # Restore from backups
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_v12_1')

PATCHES = []

# ══════════════════════════════════════════════════════════════
# FIX 1a: Totals OVER — use prob_edge for edge_pct and Kelly
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'model_engine.py',
    """                                    seen.add(k)
                                    all_picks.append({
                                        'sport': sp, 'event_id': eid, 'commence': commence,
                                        'home': home, 'away': away, 'market_type': 'TOTAL',
                                        'selection': f"{away}@{home} OVER {over_total}",
                                        'book': over_book, 'line': over_total, 'odds': over_odds,
                                        'model_spread': ms, 'model_prob': round(prob, 4),
                                        'implied_prob': round(imp, 4) if imp else None,
                                        'edge_pct': round(pv, 2), 'star_rating': stars,
                                        'units': kelly_units(edge_pct=max(pv, prob_edge), odds=over_odds, fraction=totals_kelly_frac),
                                        'confidence': _conf(stars),
                                        'spread_or_ml': 'TOTAL', 'timing': 'EARLY',
                                        'notes': f"ModelTotal={model_total:.1f} Mkt={over_total} "
                                                 f"Diff={total_diff:+.1f} PV={pv}% {stars}★ data={total_conf}",
                                    })""",
    """                                    # v12.1 FIX: Use prob_edge for edge_pct and Kelly, not PV.
                                    # PV uses assumed 52.4% implied; prob_edge uses actual book odds.
                                    # Spreads already do this via _mk(). Totals were inconsistent.
                                    over_stars = get_star_rating(max(0, prob_edge))
                                    seen.add(k)
                                    all_picks.append({
                                        'sport': sp, 'event_id': eid, 'commence': commence,
                                        'home': home, 'away': away, 'market_type': 'TOTAL',
                                        'selection': f"{away}@{home} OVER {over_total}",
                                        'book': over_book, 'line': over_total, 'odds': over_odds,
                                        'model_spread': ms, 'model_prob': round(prob, 4),
                                        'implied_prob': round(imp, 4) if imp else None,
                                        'edge_pct': round(max(0, prob_edge), 2), 'star_rating': over_stars,
                                        'units': kelly_units(edge_pct=max(0, prob_edge), odds=over_odds, fraction=totals_kelly_frac),
                                        'confidence': _conf(over_stars),
                                        'spread_or_ml': 'TOTAL', 'timing': 'EARLY',
                                        'notes': f"ModelTotal={model_total:.1f} Mkt={over_total} "
                                                 f"Diff={total_diff:+.1f} ProbEdge={prob_edge:.1f}% PV={pv}% data={total_conf}",
                                    })""",
    "FIX 1a: Totals OVER — use prob_edge for edge_pct, star_rating, and Kelly"
))

# ══════════════════════════════════════════════════════════════
# FIX 1b: Totals UNDER — use prob_edge + asymmetric threshold
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'model_engine.py',
    """                                        # Use actual probability edge for Kelly (not PV)
                                        prob_edge = (prob - (imp or 0.524)) * 100.0
                                        seen.add(k)
                                        all_picks.append({
                                            'sport': sp, 'event_id': eid, 'commence': commence,
                                            'home': home, 'away': away, 'market_type': 'TOTAL',
                                            'selection': f"{away}@{home} UNDER {under_total}",
                                            'book': under_book, 'line': under_total, 'odds': under_odds,
                                            'model_spread': ms, 'model_prob': round(prob, 4),
                                            'implied_prob': round(imp, 4) if imp else None,
                                            'edge_pct': round(pv, 2), 'star_rating': stars,
                                            'units': kelly_units(edge_pct=max(pv, prob_edge), odds=under_odds, fraction=totals_kelly_frac),
                                            'confidence': _conf(stars),
                                            'spread_or_ml': 'TOTAL', 'timing': 'EARLY',
                                            'notes': f"ModelTotal={model_total:.1f} Mkt={under_total} "
                                                     f"Diff={total_diff_u:+.1f} PV={pv}% {stars}★ data={total_conf}",
                                        })""",
    """                                        # v12.1 FIX: Use prob_edge for sizing (same as overs fix).
                                        # Also: unders need +3% more edge than overs.
                                        # Data: Overs 4W-2L (+5.4u), Unders 3W-6L (-14.9u).
                                        # The model's under predictions are systematically weaker.
                                        prob_edge = (prob - (imp or 0.524)) * 100.0
                                        under_edge = max(0, prob_edge)
                                        under_stars = get_star_rating(under_edge)
                                        # Asymmetric threshold: unders need 3% more than overs
                                        if under_edge < (min_pv_totals + 3.0):
                                            pass  # Skip — not enough edge for under
                                        else:
                                            seen.add(k)
                                            all_picks.append({
                                                'sport': sp, 'event_id': eid, 'commence': commence,
                                                'home': home, 'away': away, 'market_type': 'TOTAL',
                                                'selection': f"{away}@{home} UNDER {under_total}",
                                                'book': under_book, 'line': under_total, 'odds': under_odds,
                                                'model_spread': ms, 'model_prob': round(prob, 4),
                                                'implied_prob': round(imp, 4) if imp else None,
                                                'edge_pct': round(under_edge, 2), 'star_rating': under_stars,
                                                'units': kelly_units(edge_pct=under_edge, odds=under_odds, fraction=totals_kelly_frac),
                                                'confidence': _conf(under_stars),
                                                'spread_or_ml': 'TOTAL', 'timing': 'EARLY',
                                                'notes': f"ModelTotal={model_total:.1f} Mkt={under_total} "
                                                         f"Diff={total_diff_u:+.1f} ProbEdge={under_edge:.1f}% PV={pv}% data={total_conf}",
                                            })""",
    "FIX 1b: Totals UNDER — use prob_edge for sizing + require 3% more edge than overs"
))

# ══════════════════════════════════════════════════════════════
# FIX 2: Star rating in _mk() — use actual_edge, not PV
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'model_engine.py',
    """    units = kelly_units(edge_pct=actual_edge, odds=odds)
    if units <= 0:
        return None

    kl = kelly_label(units)
    return {
        'sport': sp, 'event_id': eid, 'commence': commence,
        'home': home, 'away': away, 'market_type': mtype,
        'selection': sel, 'book': book, 'line': line, 'odds': odds,
        'model_spread': ms, 'model_prob': round(prob,4),
        'implied_prob': round(imp,4) if imp else None,
        'edge_pct': round(actual_edge, 2),
        'star_rating': wa['star_rating'], 'units': units,
        'confidence': _conf(wa['star_rating']),
        'spread_or_ml': wa['spread_or_ml'], 'timing': wa['timing'],
        'notes': f"Model={ms:+.1f} PV={wa['point_value_pct']}% {wa['star_rating']}★ "
                 f"VigAdj={wa['vig_adjusted_spread']:+.2f} | "
                 f"Prob={prob:.1%} Imp={imp:.1%} RealEdge={actual_edge:.1f}% "
                 f"Units={units:.1f} ({kl}) | {wa['timing']}",
    }""",
    """    units = kelly_units(edge_pct=actual_edge, odds=odds)
    if units <= 0:
        return None

    # v12.1 FIX: Star rating from ACTUAL probability edge, not PV.
    # PV inflates edges via key number summation (PV=18% when real edge=8%).
    # This caused the 16-20% edge bucket to go 8W-10L — inflated PV, thin real edge.
    # Now stars, edge_pct, and Kelly all use the same probability-based number.
    actual_stars = get_star_rating(actual_edge)
    kl = kelly_label(units)
    return {
        'sport': sp, 'event_id': eid, 'commence': commence,
        'home': home, 'away': away, 'market_type': mtype,
        'selection': sel, 'book': book, 'line': line, 'odds': odds,
        'model_spread': ms, 'model_prob': round(prob,4),
        'implied_prob': round(imp,4) if imp else None,
        'edge_pct': round(actual_edge, 2),
        'star_rating': actual_stars, 'units': units,
        'confidence': _conf(actual_stars),
        'spread_or_ml': wa['spread_or_ml'], 'timing': wa['timing'],
        'notes': f"Model={ms:+.1f} PV={wa['point_value_pct']}% RealEdge={actual_edge:.1f}% {actual_stars}★ "
                 f"VigAdj={wa['vig_adjusted_spread']:+.2f} | "
                 f"Prob={prob:.1%} Imp={imp:.1%} "
                 f"Units={units:.1f} ({kl}) | {wa['timing']}",
    }""",
    "FIX 2: Star rating in _mk() uses actual probability edge, not PV"
))

# ══════════════════════════════════════════════════════════════
# FIX 2b: Lower star floor from 2.0 to 1.5
# ══════════════════════════════════════════════════════════════
# With star_rating now based on actual_edge (lower than PV), the 2.0★ floor
# would filter out picks that previously passed. 1.5★ = 10%+ actual edge,
# which is a reasonable pre-filter. _merge_and_select handles real quality control.

PATCHES.append((
    'model_engine.py',
    """    # Deduplicate: don't bet both sides of same event+market
    # ═══ FINAL FILTER: HIGH (2.0★) and ELITE (2.5★+) ONLY ═══
    # One pick per event per market type (best edge wins)
    final_picks = []
    seen_event_market = {}
    all_picks.sort(key=lambda x: x['star_rating']*100 + x['edge_pct'], reverse=True)
    for p in all_picks:
        # Skip LOW, MEDIUM, STRONG
        if p['star_rating'] < 2.0:
            continue""",
    """    # Deduplicate: don't bet both sides of same event+market
    # ═══ FINAL FILTER: 1.5★+ (10%+ real edge) ═══
    # v12.1: Lowered from 2.0★ because star_rating now uses actual probability
    # edge instead of PV. Old 2.0★ = PV ≥ 13%. New 1.5★ = real edge ≥ 10%.
    # _merge_and_select applies the real quality thresholds (13% min edge).
    final_picks = []
    seen_event_market = {}
    all_picks.sort(key=lambda x: x['star_rating']*100 + x['edge_pct'], reverse=True)
    for p in all_picks:
        if p['star_rating'] < 1.5:
            continue""",
    "FIX 2b: Lower star floor from 2.0 to 1.5 (compensates for probability-based stars)"
))

# ══════════════════════════════════════════════════════════════
# FIX 3: Remove spread_diff totals adjustment
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'model_engine.py',
    """    # Adjust based on strength differential — basketball only
    # v12 FIX: Removed hockey adjustment. spread_diff > 0.3 fires on nearly
    # every NHL game, creating a systematic under bias (-0.15 on every total).
    # This caused 3+ NHL unders per card. Hockey totals should come from
    # team scoring data only, not spread differential.
    spread_diff = abs(h['final'] - a['final'])
    if 'basketball' in sport:
        if spread_diff > 8:
            model_total -= 2  # Blowouts tend to go under
        elif spread_diff < 2:
            model_total += 1  # Close games, OT possibility

    return round(model_total, 1)""",
    """    # v12.1 FIX: Removed spread_diff adjustment entirely.
    # v12 removed it for hockey (systematic under bias). The same problem
    # exists for basketball: model_total -= 2 for blowouts creates false unders.
    # Data: Overs 4W-2L (+5.4u), Unders 3W-6L (-14.9u).
    # Let team scoring deviations be the signal, not spread heuristics.

    return round(model_total, 1)""",
    "FIX 3: Remove spread_diff totals adjustment (was creating systematic under lean)"
))

# ══════════════════════════════════════════════════════════════
# FIX 4: Remove favorite exemption from early bet penalty
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'main.py',
    """        # v12 FIX: Early bets are 14W-20L (-18.4% ROI), late bets 22W-13L (+24.9% ROI).
        # Require 5% more edge for early bets to compensate for worse line timing.
        # EXCEPTIONS:
        # 1. Soccer lines are set by European books before US markets open
        # 2. Favorites should be bet EARLY (Walters rule — public pushes line later)
        timing = p.get('timing', 'EARLY')
        sport = p.get('sport', '')
        line = p.get('line')
        is_favorite = line is not None and line < 0
        if timing == 'EARLY' and 'soccer' not in sport and not is_favorite:
            min_edge += 5.0""",
    """        # v12.1 FIX: Early bets are 11W-13L (-11.1u) even with the penalty.
        # Removed favorite exemption — favorites are 3W-4L (-7.3u), the
        # Walters "bet favorites early" rule doesn't match this model's edge profile.
        # Soccer still exempted (lines set by European books before US open).
        timing = p.get('timing', 'EARLY')
        sport = p.get('sport', '')
        if timing == 'EARLY' and 'soccer' not in sport:
            min_edge += 5.0""",
    "FIX 4: Remove favorite exemption from early bet penalty (favorites are 3W-4L)"
))


# ══════════════════════════════════════════════════════════════
# EXECUTION (same pattern as migrate_to_config.py)
# ══════════════════════════════════════════════════════════════

def preview():
    print("=" * 65)
    print("  SCOTTY'S EDGE v12.1 — Performance Fixes (PREVIEW)")
    print("=" * 65)

    pending = 0
    applied = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  ⚠️  {filename}: not found")
            continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text in content:
            print(f"  📝 {filename}: {desc}")
            pending += 1
        elif new_text[:80] in content:
            print(f"  ✅ {filename}: already applied")
            applied += 1
        else:
            print(f"  ⚠️  {filename}: text not found — may need manual review")
            print(f"      Looking for: {old_text[:90]}...")

    print(f"\n  Summary: {pending} patches to apply, {applied} already done")
    print(f"  Run with --apply to execute.")


def apply():
    print("=" * 65)
    print("  SCOTTY'S EDGE v12.1 — Applying Fixes")
    print("=" * 65)

    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0

    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  ⚠️  {filename}: not found")
            continue

        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        if old_text not in content:
            if new_text[:80] in content:
                print(f"  ✅ {filename}: already applied — {desc}")
            else:
                print(f"  ⚠️  {filename}: text mismatch — {desc}")
            continue

        # Backup (first time only)
        bak = os.path.join(BACKUP_DIR, f"{filename}.bak")
        if not os.path.exists(bak):
            shutil.copy2(filepath, bak)
        shutil.copy2(filepath, os.path.join(BACKUP_DIR, f"{filename}.{timestamp}.bak"))

        new_content = content.replace(old_text, new_text, 1)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  ✅ {filename}: {desc}")
        success += 1

    print(f"\n  Applied {success} patches. Backups at: {BACKUP_DIR}")
    print(f"  Test: python main.py predict")
    print(f"  Rollback: python v12_1_fixes.py --rollback")


def rollback():
    if not os.path.exists(BACKUP_DIR):
        print("  No backups found.")
        return
    print("  Restoring from backups...")
    for f in os.listdir(BACKUP_DIR):
        if f.endswith('.bak') and not any(c.isdigit() for c in f.split('.')[-2]):
            src = os.path.join(BACKUP_DIR, f)
            dst = os.path.join(SCRIPTS_DIR, f.replace('.bak', ''))
            shutil.copy2(src, dst)
            print(f"  ✅ Restored: {f.replace('.bak', '')}")
    print("  Rollback complete.")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    elif '--rollback' in sys.argv:
        rollback()
    else:
        preview()
