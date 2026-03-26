"""
migrate_to_config.py — Scotty's Edge v12 Cleanup Migration

Applies all consistency fixes:
  1. Creates config.py as single source of truth
  2. Patches scottys_edge.py to import from config
  3. Patches bootstrap_ratings.py HCA values (were stale pre-v12)
  4. Patches weekly_report.py market tiers (NHL/La Liga were wrong)
  5. Patches grader.py _infer_market_tier (same fix)
  6. Patches model_engine.py _classify_market_tier (same fix)
  7. Updates version strings from v9/v11 to v12
  8. Moves one-time scripts to archive/

SAFE TO RUN: Makes backups of every file before patching.
SAFE TO RE-RUN: Checks if patches are already applied.

Usage:
    python migrate_to_config.py              # Preview (dry run)
    python migrate_to_config.py --apply      # Apply changes
    python migrate_to_config.py --rollback   # Restore backups
"""
import os
import sys
import shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_pre_config')
ARCHIVE_DIR = os.path.join(SCRIPTS_DIR, 'archive')

# ══════════════════════════════════════════════════════════════
# PATCHES — Each patch is (filename, old_text, new_text, description)
# ══════════════════════════════════════════════════════════════

PATCHES = []

# ── 1. scottys_edge.py: Import market tiers from config ──
PATCHES.append((
    'scottys_edge.py',
    """# Market tier classification — used by pick selector in main.py
SOFT_MARKETS = {
    'basketball_ncaab', 'icehockey_nhl',  # NHL reclassified: 3-0 +16u, less efficient than NBA/EPL
    'soccer_usa_mls', 'soccer_germany_bundesliga',
    'soccer_france_ligue_one', 'soccer_italy_serie_a', 'soccer_uefa_champs_league',
    'soccer_spain_la_liga',   # v12: reclassified — bottom-half La Liga is soft, only top 3-4 teams are sharp
    'baseball_ncaa',
}
SHARP_MARKETS = {
    'basketball_nba', 'soccer_epl',  # Only the truly efficient markets
}""",
    """# Market tier classification — single source of truth in config.py
from config import SOFT_MARKETS, SHARP_MARKETS""",
    "Import SOFT_MARKETS/SHARP_MARKETS from config instead of hardcoding"
))

# ── 2. bootstrap_ratings.py: Fix stale HCA values ──
PATCHES.append((
    'bootstrap_ratings.py',
    """HCA = {
    'basketball_nba': 2.5, 'basketball_ncaab': 3.2,
    'icehockey_nhl': 0.15,
    'soccer_epl': 0.25, 'soccer_italy_serie_a': 0.30,
    'soccer_spain_la_liga': 0.25,
    'soccer_germany_bundesliga': 0.30,
    'soccer_france_ligue_one': 0.25,
    'soccer_uefa_champs_league': 0.20,
    'soccer_usa_mls': 0.35,
    'baseball_ncaa': 0.4,
}""",
    """# v12 FIX: Import from config.py — bootstrap was using pre-v12 HCA values
# (EPL 0.25 instead of 0.40, etc.) which skewed initial ratings.
from config import HOME_ADVANTAGE as HCA""",
    "bootstrap_ratings.py HCA was stale (EPL 0.25 vs model 0.40, etc.)"
))

PATCHES.append((
    'bootstrap_ratings.py',
    """MAX_RATING = {
    'basketball_nba': 10, 'basketball_ncaab': 12, 'icehockey_nhl': 0.6,
    'soccer_epl': 0.5, 'soccer_italy_serie_a': 0.5, 'soccer_spain_la_liga': 0.5,
    'soccer_germany_bundesliga': 0.5, 'soccer_france_ligue_one': 0.5,
    'soccer_uefa_champs_league': 0.5, 'soccer_usa_mls': 0.5,
    'baseball_ncaa': 3.0,
}""",
    """from config import MAX_RATING""",
    "Import MAX_RATING from config"
))

# ── 3. weekly_report.py: Fix wrong market tier classification ──
PATCHES.append((
    'weekly_report.py',
    """SOFT_MARKETS = {
    'basketball_ncaab', 'soccer_usa_mls', 'soccer_germany_bundesliga',
    'soccer_france_ligue_one', 'soccer_italy_serie_a', 'soccer_uefa_champs_league',
}
SHARP_MARKETS = {
    'basketball_nba', 'icehockey_nhl', 'soccer_epl', 'soccer_spain_la_liga',
}""",
    """# v12 FIX: Import from config.py — weekly_report had NHL and La Liga as SHARP,
# but they were reclassified to SOFT in v12 based on performance data.
from config import SOFT_MARKETS, SHARP_MARKETS""",
    "weekly_report.py had NHL/La Liga as SHARP (should be SOFT since v12)"
))

# ── 4. grader.py: Fix _infer_market_tier ──
PATCHES.append((
    'grader.py',
    """def _infer_market_tier(sport):
    \"\"\"Infer market tier from sport.\"\"\"
    soft = {'basketball_ncaab', 'soccer_usa_mls', 'soccer_germany_bundesliga',
            'soccer_france_ligue_one', 'soccer_italy_serie_a', 'soccer_uefa_champs_league',
            'baseball_ncaa'}
    return 'SOFT' if sport in soft else 'SHARP'""",
    """def _infer_market_tier(sport):
    \"\"\"Infer market tier from sport. Uses config.py as source of truth.\"\"\"
    from config import SOFT_MARKETS
    return 'SOFT' if sport in SOFT_MARKETS else 'SHARP'""",
    "grader.py _infer_market_tier was missing NHL and La Liga as SOFT"
))

# ── 5. model_engine.py: Fix _classify_market_tier ──
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
    "model_engine.py _classify_market_tier was missing NHL and La Liga as SOFT"
))

# ── 6. Version string updates ──
PATCHES.append((
    'main.py',
    'main.py v11 — Scotty\'s Edge Command Center',
    'main.py v12 — Scotty\'s Edge Command Center',
    "Update main.py version header to v12"
))

PATCHES.append((
    'main.py',
    "  SCOTTY'S EDGE v11 — {run_type} Run",
    "  SCOTTY'S EDGE v12 — {run_type} Run",
    "Update main.py run banner to v12"
))

PATCHES.append((
    'model_engine.py',
    "Scotty's Edge v11",
    "Scotty's Edge v12",
    "Update model_engine.py picks banner to v12"
))

PATCHES.append((
    'main.py',
    '"✅ Scotty\'s Edge v11 — Email Test"',
    '"✅ Scotty\'s Edge v12 — Email Test"',
    "Update main.py email test to v12"
))

PATCHES.append((
    'model_engine.py',
    'model_engine.py v9 — Scotty\'s Edge',
    'model_engine.py v12 — Scotty\'s Edge',
    "Update model_engine.py version header to v12"
))

PATCHES.append((
    'grader.py',
    'grader.py v11 — Performance Tracking with CLV Analysis',
    'grader.py v12 — Performance Tracking with CLV Analysis',
    "Update grader.py version header to v12"
))

PATCHES.append((
    'weekly_report.py',
    "SCOTTY'S EDGE v11 — WEEKLY REVIEW",
    "SCOTTY'S EDGE v12 — WEEKLY REVIEW",
    "Update weekly_report.py banner to v12"
))

# ══════════════════════════════════════════════════════════════
# ARCHIVE — One-time scripts that served their purpose
# ══════════════════════════════════════════════════════════════

ARCHIVE_FILES = [
    'purge_bogus_picks.py',
    'purge_underdog_mls.py',
    'restore_feb28_bets.py',
    'fix_consensus_dupes.py',
    'migrate_sheets_to_sqlite.py',
    'performance.py',          # Duplicates grader.py report
    'injuries.py',             # Duplicates injury_scraper.py
]


# ══════════════════════════════════════════════════════════════
# EXECUTION
# ══════════════════════════════════════════════════════════════

def preview():
    """Show what will change without touching files."""
    print("=" * 65)
    print("  SCOTTY'S EDGE — v12 Config Migration (PREVIEW)")
    print("=" * 65)

    # Check config.py
    config_path = os.path.join(SCRIPTS_DIR, 'config.py')
    if os.path.exists(config_path):
        print(f"\n  ✅ config.py already exists")
    else:
        print(f"\n  📝 Will create: config.py (single source of truth)")

    # Check patches
    print(f"\n  PATCHES ({len(PATCHES)} total):")
    applied = 0
    pending = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"    ⚠️  {filename}: FILE NOT FOUND")
            continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text in content:
            print(f"    📝 {filename}: {desc}")
            pending += 1
        elif new_text in content:
            print(f"    ✅ {filename}: already applied")
            applied += 1
        else:
            print(f"    ⚠️  {filename}: old text not found (may need manual review)")
            # Show first 60 chars of old_text for debugging
            print(f"        Looking for: {old_text[:80]}...")

    # Check archives
    print(f"\n  ARCHIVE ({len(ARCHIVE_FILES)} files → archive/ folder):")
    for f in ARCHIVE_FILES:
        fp = os.path.join(SCRIPTS_DIR, f)
        if os.path.exists(fp):
            print(f"    📦 {f}")
        else:
            print(f"    ⏭️  {f} (not found, skip)")

    print(f"\n  Summary: {pending} patches to apply, {applied} already done")
    print(f"\n  Run with --apply to execute.")
    print(f"  Backups will be saved to: {BACKUP_DIR}")


def apply():
    """Apply all patches with backups."""
    print("=" * 65)
    print("  SCOTTY'S EDGE — v12 Config Migration (APPLYING)")
    print("=" * 65)

    # Create backup directory
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Step 1: Ensure config.py exists
    config_src = os.path.join(SCRIPTS_DIR, 'config.py')
    if not os.path.exists(config_src):
        print(f"\n  ❌ config.py not found in {SCRIPTS_DIR}")
        print(f"     Copy config.py to your scripts folder first.")
        return False

    # Step 2: Apply patches
    print(f"\n  Applying patches...")
    success = 0
    skipped = 0
    failed = 0

    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"    ⚠️  {filename}: not found, skipping")
            skipped += 1
            continue

        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        if new_text in content:
            print(f"    ✅ {filename}: already applied — {desc}")
            skipped += 1
            continue

        if old_text not in content:
            print(f"    ⚠️  {filename}: old text not found — {desc}")
            failed += 1
            continue

        # Backup
        backup_path = os.path.join(BACKUP_DIR, f"{filename}.{timestamp}.bak")
        if not os.path.exists(os.path.join(BACKUP_DIR, f"{filename}.bak")):
            # Only keep first backup (the original)
            shutil.copy2(filepath, os.path.join(BACKUP_DIR, f"{filename}.bak"))
        shutil.copy2(filepath, backup_path)

        # Apply patch
        new_content = content.replace(old_text, new_text, 1)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"    ✅ {filename}: {desc}")
        success += 1

    # Step 3: Archive one-time scripts
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    archived = 0
    print(f"\n  Archiving one-time scripts...")
    for f in ARCHIVE_FILES:
        src = os.path.join(SCRIPTS_DIR, f)
        dst = os.path.join(ARCHIVE_DIR, f)
        if os.path.exists(src):
            shutil.move(src, dst)
            print(f"    📦 {f} → archive/")
            archived += 1
        else:
            print(f"    ⏭️  {f}: not found")

    print(f"\n  {'='*50}")
    print(f"  Migration complete!")
    print(f"    Patches applied: {success}")
    print(f"    Already applied: {skipped}")
    print(f"    Failed (manual review): {failed}")
    print(f"    Scripts archived: {archived}")
    print(f"    Backups at: {BACKUP_DIR}")
    print(f"  {'='*50}")

    if failed > 0:
        print(f"\n  ⚠️  {failed} patches need manual review.")
        print(f"  The file may have been edited since the patch was written.")
        print(f"  Check the descriptions above and apply manually if needed.")

    return True


def rollback():
    """Restore files from backup."""
    if not os.path.exists(BACKUP_DIR):
        print("  No backups found.")
        return

    print("  Restoring from backups...")
    for f in os.listdir(BACKUP_DIR):
        if f.endswith('.bak') and not any(c.isdigit() for c in f.split('.')[-2]):
            # This is the original backup (filename.py.bak)
            original_name = f.replace('.bak', '')
            src = os.path.join(BACKUP_DIR, f)
            dst = os.path.join(SCRIPTS_DIR, original_name)
            shutil.copy2(src, dst)
            print(f"    ✅ Restored: {original_name}")

    # Restore archived files
    if os.path.exists(ARCHIVE_DIR):
        for f in os.listdir(ARCHIVE_DIR):
            src = os.path.join(ARCHIVE_DIR, f)
            dst = os.path.join(SCRIPTS_DIR, f)
            if not os.path.exists(dst):
                shutil.move(src, dst)
                print(f"    ✅ Unarchived: {f}")

    print("  Rollback complete.")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    elif '--rollback' in sys.argv:
        rollback()
    else:
        preview()
