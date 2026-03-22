"""
reapply_v12_1.py — Re-apply config migration + structural fixes only.

After the rollback wiped everything, this re-applies:
  ✅ Config migration patches (version strings, _classify_market_tier)
  ✅ Fix 1a: Totals OVER — probability edge for sizing
  ✅ Fix 1b: Totals UNDER — probability edge + asymmetric threshold
  ✅ Fix 2:  Star rating uses actual probability edge
  ✅ Fix 2b: Lower star floor from 2.0 to 1.5

Does NOT apply (kept as-is per Scott's decision):
  ❌ Fix 3: spread_diff totals adjustment stays
  ❌ Fix 4: favorite early exemption stays

Usage:
    python reapply_v12_1.py              # Preview
    python reapply_v12_1.py --apply      # Apply
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_reapply')

PATCHES = []

# ══════════════════════════════════════════════════════════════
# CONFIG MIGRATION — re-apply to model_engine.py and main.py
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'model_engine.py',
    """def _classify_market_tier(sport):
    \"\"\"Classify sport into SOFT or SHARP market tier.\"\"\"
    soft = {'basketball_ncaab', 'soccer_usa_mls', 'soccer_germany_bundesliga',
            'soccer_france_ligue_one', 'soccer_italy_serie_a', 'soccer_uefa_champs_league',
            'baseball_ncaa'}
    return 'SOFT' if sport in soft else 'SHARP'""",
    """def _classify_market_tier(sport):
    \"\"\"Classify sport into SOFT or SHARP market tier. Uses config.py.\"\"\"
    from config import SOFT_MARKETS
    return 'SOFT' if sport in SOFT_MARKETS else 'SHARP'""",
    "Config: _classify_market_tier imports from config.py"
))

PATCHES.append((
    'model_engine.py',
    'model_engine.py v9 — Scotty\'s Edge',
    'model_engine.py v12 — Scotty\'s Edge',
    "Config: version header v9 → v12"
))

PATCHES.append((
    'model_engine.py',
    "Scotty's Edge v11",
    "Scotty's Edge v12",
    "Config: picks banner v11 → v12"
))

PATCHES.append((
    'main.py',
    'main.py v11 — Scotty\'s Edge Command Center',
    'main.py v12 — Scotty\'s Edge Command Center',
    "Config: main.py header v11 → v12"
))

PATCHES.append((
    'main.py',
    "  SCOTTY'S EDGE v11 — {run_type} Run",
    "  SCOTTY'S EDGE v12 — {run_type} Run",
    "Config: run banner v11 → v12"
))

PATCHES.append((
    'main.py',
    '"✅ Scotty\'s Edge v11 — Email Test"',
    '"✅ Scotty\'s Edge v12 — Email Test"',
    "Config: email test v11 → v12"
))

# ══════════════════════════════════════════════════════════════
# FIX 1a: Totals OVER — probability edge for sizing
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
    "Fix 1a: Totals OVER — probability edge for edge_pct, stars, and Kelly"
))

# ══════════════════════════════════════════════════════════════
# FIX 1b: Totals UNDER — probability edge + asymmetric threshold
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
    "Fix 1b: Totals UNDER — probability edge + 3% asymmetric threshold"
))

# ══════════════════════════════════════════════════════════════
# FIX 2: Star rating in _mk() — actual probability edge
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
    "Fix 2: Star rating uses actual probability edge, not PV"
))

# ══════════════════════════════════════════════════════════════
# FIX 2b: Lower star floor from 2.0 to 1.5
# ══════════════════════════════════════════════════════════════

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
    "Fix 2b: Star floor 2.0 → 1.5 (compensates for probability-based stars)"
))


# ══════════════════════════════════════════════════════════════
# EXECUTION
# ══════════════════════════════════════════════════════════════

def preview():
    print("=" * 65)
    print("  RE-APPLY: Config + Structural Fixes (PREVIEW)")
    print("  Fixes 3 and 4 intentionally excluded.")
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
            print(f"  📝 {desc}")
            pending += 1
        elif new_text[:80] in content:
            print(f"  ✅ {desc} — already applied")
            applied += 1
        else:
            print(f"  ⚠️  {desc} — text not found")
            print(f"      Looking for: {old_text[:80]}...")
    print(f"\n  {pending} to apply, {applied} already done.")
    print(f"  Run with --apply to execute.")


def apply():
    print("=" * 65)
    print("  RE-APPLY: Config + Structural Fixes")
    print("=" * 65)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
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
                print(f"  ✅ {desc} — already applied")
            else:
                print(f"  ⚠️  {desc} — text mismatch")
            continue
        bak = os.path.join(BACKUP_DIR, f"{filename}.{ts}.bak")
        shutil.copy2(filepath, bak)
        new_content = content.replace(old_text, new_text, 1)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  ✅ {desc}")
        success += 1
    print(f"\n  Applied {success} patches.")
    print(f"  Test: python main.py predict")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    else:
        preview()
