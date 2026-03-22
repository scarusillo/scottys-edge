"""
prop_grading_patch.py — Wire ESPN box scores into the grading pipeline

Adds:
  1. Box score fetch step to cmd_grade in main.py
  2. PROP handler in grader.py that uses actual player stats
  3. Updates accumulate_player_results to use real box score data

Usage:
    python prop_grading_patch.py              # Preview
    python prop_grading_patch.py --apply      # Apply
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_prop_grading')

PATCHES = []

# ══════════════════════════════════════════════════════════════
# PATCH 1: Add box score fetch to cmd_grade in main.py
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'main.py',
    """    # v12 FIX: Odds API doesn't return college baseball scores.
    # Fetch from ESPN scraper instead. No API cost.
    print("  Fetching ESPN baseball scores...")
    try:
        from historical_scores import fetch_season_scores
        fetch_season_scores('baseball_ncaa', days_back=5, verbose=False)
    except Exception as e:
        print(f"  ESPN baseball scores: {e}")

    report = daily_grade_and_report(conn)""",
    """    # v12 FIX: Odds API doesn't return college baseball scores.
    # Fetch from ESPN scraper instead. No API cost.
    print("  Fetching ESPN baseball scores...")
    try:
        from historical_scores import fetch_season_scores
        fetch_season_scores('baseball_ncaa', days_back=5, verbose=False)
    except Exception as e:
        print(f"  ESPN baseball scores: {e}")

    # v12.1: Fetch ESPN box scores for prop bet grading (FREE)
    print("  Fetching ESPN box scores (props)...")
    try:
        from box_scores import fetch_all_box_scores
        fetch_all_box_scores(days_back=3)
    except Exception as e:
        print(f"  ESPN box scores: {e}")

    report = daily_grade_and_report(conn)""",
    "Add box score fetch to cmd_grade (enables prop grading)"
))

# ══════════════════════════════════════════════════════════════
# PATCH 2: Add PROP handler to determine_result in grader.py
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'grader.py',
    """        h_score, a_score, home, away, _ = score

        # Determine W/L/P
        result = determine_result(sel, mtype, line, h_score, a_score, home, away, sport=sport)""",
    """        h_score, a_score, home, away, _ = score

        # Determine W/L/P
        # v12.1: Props use box score player stats, not team scores
        if mtype == 'PROP':
            try:
                from box_scores import grade_prop
                bet_date = created[:10] if created else None
                result = grade_prop(conn, sel, line, bet_date, sport=sport)
            except ImportError:
                result = 'PENDING'  # box_scores.py not installed yet
            except Exception as e:
                print(f"  ⚠ Prop grading error: {e}")
                result = 'PENDING'
        else:
            result = determine_result(sel, mtype, line, h_score, a_score, home, away, sport=sport)""",
    "Add PROP handler using box_scores.grade_prop() in grade_bets()"
))

# ══════════════════════════════════════════════════════════════
# PATCH 3: Update accumulate_player_results to use real box score data
# ══════════════════════════════════════════════════════════════
# The current accumulate_player_results estimates actual values from
# win/loss (line ± 1). With box scores, we can store the REAL value.

PATCHES.append((
    'grader.py',
    """        # Estimate actual value (best we can do without box scores)
        # WIN on OVER 25.5 → actual was at least 26 (we use line + 1)
        # LOSS on OVER 25.5 → actual was at most 25 (we use line - 1)
        if prop_result == 'OVER':
            estimated_actual = line + 1.0
        else:
            estimated_actual = line - 1.0""",
    """        # v12.1: Try to get REAL value from ESPN box scores
        estimated_actual = None
        try:
            from box_scores import lookup_player_stat, PROP_TO_STAT
            stat_type_key = PROP_TO_STAT.get(stat_type.upper(), stat_type)
            real_val = lookup_player_stat(conn, player, stat_type_key, game_date, sport=sport)
            if real_val is not None:
                estimated_actual = real_val
        except ImportError:
            pass
        except Exception:
            pass
        
        # Fallback: estimate from win/loss if no box score
        if estimated_actual is None:
            if prop_result == 'OVER':
                estimated_actual = line + 1.0
            else:
                estimated_actual = line - 1.0""",
    "Use real box score values in accumulate_player_results when available"
))


# ══════════════════════════════════════════════════════════════
# EXECUTION
# ══════════════════════════════════════════════════════════════

def preview():
    print("=" * 65)
    print("  PROP GRADING INTEGRATION (PREVIEW)")
    print("=" * 65)
    pending = 0
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
        else:
            print(f"  ⚠️  {desc} — text not found")
            print(f"      Looking for: {old_text[:80]}...")
    print(f"\n  {pending} patches to apply.")
    print(f"  Run with --apply to execute.")


def apply():
    print("=" * 65)
    print("  PROP GRADING INTEGRATION — Applying")
    print("=" * 65)

    # Check that box_scores.py exists
    bs_path = os.path.join(SCRIPTS_DIR, 'box_scores.py')
    if not os.path.exists(bs_path):
        print(f"  ❌ box_scores.py not found in {SCRIPTS_DIR}")
        print(f"     Copy box_scores.py to your scripts folder first.")
        return

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
    print(f"\n  Test the box score fetcher:")
    print(f"    python box_scores.py --sport nba --days 2")
    print(f"  Then test grading:")
    print(f"    python main.py grade")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    else:
        preview()
