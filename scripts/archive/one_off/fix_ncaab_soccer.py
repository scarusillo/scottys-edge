"""
fix_ncaab_soccer.py — Two structural model fixes

Fix 1: NCAAB requires Elo confidence for BOTH teams.
  Without Elo, the model uses bootstrap ratings which are derived FROM market lines.
  Comparing market-derived ratings to market lines finds noise, not edges.
  167 of 365 NCAAB teams have Elo. The other 198 are generating phantom edges.

Fix 2: Soccer spreads get draw probability adjustment.
  Soccer MLs already use soccer_ml_probs() which accounts for draws.
  Soccer spreads still use the two-outcome spread_to_cover_prob().
  This adds a draw-based edge haircut to soccer spread evaluations.

Usage:
    python fix_ncaab_soccer.py              # Preview
    python fix_ncaab_soccer.py --apply      # Apply
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_structural')

PATCHES = []

# ══════════════════════════════════════════════════════════════
# FIX 1: NCAAB — Require Elo confidence, skip games without it
# ══════════════════════════════════════════════════════════════

PATCHES.append(('model_engine.py',
    """            # UPGRADE: If Elo ratings available, use blended spread
            # This creates predictions INDEPENDENT of market lines
            if HAS_ELO and elo_data:
                elo_ms = blended_spread(home, away, elo_data, ratings, sp, conn)
                if elo_ms is not None:
                    ms = elo_ms  # Use the blended prediction""",
    """            # UPGRADE: If Elo ratings available, use blended spread
            # This creates predictions INDEPENDENT of market lines
            if HAS_ELO and elo_data:
                elo_ms = blended_spread(home, away, elo_data, ratings, sp, conn)
                if elo_ms is not None:
                    ms = elo_ms  # Use the blended prediction
                elif sp == 'basketball_ncaab':
                    # v12.2 FIX: NCAAB has 365 teams but only ~167 have confident Elo.
                    # Without Elo, the model uses bootstrap ratings derived FROM market lines.
                    # Comparing market-derived ratings to market lines is circular — any
                    # "edge" found is just noise from the bootstrap process + HCA differences.
                    # NBA/NHL have all teams rated so this only affects NCAAB.
                    skip_nr += 1
                    continue""",
    "NCAAB: require Elo confidence — skip games without it"))

# ══════════════════════════════════════════════════════════════
# FIX 2: Soccer spreads — draw probability adjustment
# The model calculates cover probability using spread_to_cover_prob()
# which is a two-outcome function. In soccer, draws add uncertainty
# that this function doesn't capture. We reduce the calculated edge
# by the draw probability to be conservative.
# ══════════════════════════════════════════════════════════════

# Home spread — soccer draw adjustment
PATCHES.append(('model_engine.py',
    """            # HOME SPREAD
            if mkt_hs is not None and mkt_hs_odds is not None:
                k = f"{eid}|S|{home}"
                if k not in seen:
                    wa = scottys_edge_assessment(ms, mkt_hs, mkt_hs_odds, sp, hml, a_inj, a_cl)
                    if wa['is_play'] and wa['point_value_pct'] >= min_pv:
                        prob = spread_to_cover_prob(ms, mkt_hs, sp)
                        imp = american_to_implied_prob(mkt_hs_odds)""",
    """            # HOME SPREAD
            if mkt_hs is not None and mkt_hs_odds is not None:
                k = f"{eid}|S|{home}"
                if k not in seen:
                    wa = scottys_edge_assessment(ms, mkt_hs, mkt_hs_odds, sp, hml, a_inj, a_cl)
                    if wa['is_play'] and wa['point_value_pct'] >= min_pv:
                        prob = spread_to_cover_prob(ms, mkt_hs, sp)
                        # v12.2: Soccer draw adjustment for spreads.
                        # spread_to_cover_prob is two-outcome. Soccer has draws which
                        # add variance the model doesn't capture. Reduce cover prob
                        # by half the draw rate as a conservative correction.
                        if 'soccer' in sp:
                            draw_p = _soccer_draw_prob(abs(ms))
                            prob = prob * (1.0 - draw_p * 0.5)
                        imp = american_to_implied_prob(mkt_hs_odds)""",
    "Soccer home spread: draw probability adjustment"))

# Away spread — soccer draw adjustment
PATCHES.append(('model_engine.py',
    """            # AWAY SPREAD
            if mkt_as is not None and mkt_as_odds is not None:
                k = f"{eid}|S|{away}"
                if k not in seen:
                    wa = scottys_edge_assessment(-ms, mkt_as, mkt_as_odds, sp, aml, h_inj, h_cl)
                    if wa['is_play'] and wa['point_value_pct'] >= min_pv:
                        prob = spread_to_cover_prob(-ms, mkt_as, sp)
                        imp = american_to_implied_prob(mkt_as_odds)""",
    """            # AWAY SPREAD
            if mkt_as is not None and mkt_as_odds is not None:
                k = f"{eid}|S|{away}"
                if k not in seen:
                    wa = scottys_edge_assessment(-ms, mkt_as, mkt_as_odds, sp, aml, h_inj, h_cl)
                    if wa['is_play'] and wa['point_value_pct'] >= min_pv:
                        prob = spread_to_cover_prob(-ms, mkt_as, sp)
                        # v12.2: Soccer draw adjustment for spreads
                        if 'soccer' in sp:
                            draw_p = _soccer_draw_prob(abs(ms))
                            prob = prob * (1.0 - draw_p * 0.5)
                        imp = american_to_implied_prob(mkt_as_odds)""",
    "Soccer away spread: draw probability adjustment"))


def preview():
    print("=" * 65)
    print("  STRUCTURAL MODEL FIXES (PREVIEW)")
    print("=" * 65)
    pending = 0
    for filename, old, new, desc in PATCHES:
        fp = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(fp):
            print(f"  {filename}: not found"); continue
        with open(fp, 'r', encoding='utf-8') as f:
            content = f.read()
        if old in content:
            print(f"  \U0001f4dd {desc}"); pending += 1
        elif new[:80] in content:
            print(f"  \u2705 {desc} — already applied")
        else:
            print(f"  \u26a0\ufe0f  {desc} — text not found")
    print(f"\n  {pending} patches to apply. Run with --apply")


def apply():
    print("=" * 65)
    print("  STRUCTURAL MODEL FIXES — Applying")
    print("=" * 65)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0
    for filename, old, new, desc in PATCHES:
        fp = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(fp):
            print(f"  {filename}: not found"); continue
        with open(fp, 'r', encoding='utf-8') as f:
            content = f.read()
        if old not in content:
            if new[:80] in content:
                print(f"  \u2705 {desc} — already applied")
            else:
                print(f"  \u26a0\ufe0f  {desc} — text not found")
            continue
        bak = os.path.join(BACKUP_DIR, f"{filename}.{ts}.bak")
        if not os.path.exists(bak):
            shutil.copy2(fp, bak)
        content = content.replace(old, new, 1)
        with open(fp, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  \u2705 {desc}"); success += 1
    print(f"\n  Applied {success} fixes.")
    print(f"\n  IMPORTANT: Rebuild Elo after this change:")
    print(f"    python elo_engine.py --sport basketball_ncaab")
    print(f"\n  Then test:")
    print(f"    python main.py predict")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    else:
        preview()
