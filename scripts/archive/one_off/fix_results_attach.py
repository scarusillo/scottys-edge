"""
fix_results_attach.py — Fix grade email to attach all results cards (wins + losses)

Usage:
    python fix_results_attach.py --apply
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))

OLD = """        results_png = generate_results_card(conn, start_date='2026-03-04')
        if results_png: card_paths.append(results_png)"""

NEW = """        results_pngs = generate_results_card(conn, start_date='2026-03-04')
        if results_pngs:
            if isinstance(results_pngs, list):
                card_paths.extend(results_pngs)
            else:
                card_paths.append(results_pngs)"""

filepath = os.path.join(SCRIPTS_DIR, 'main.py')

if '--apply' in sys.argv:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    if OLD in content:
        shutil.copy2(filepath, filepath + '.bak')
        content = content.replace(OLD, NEW, 1)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print("  ✅ Fixed — grade email now attaches all results cards")
    elif NEW[:40] in content:
        print("  ✅ Already applied")
    else:
        print("  ❌ Text not found in main.py")
else:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    if OLD in content:
        print("  📝 Ready to apply. Run with --apply")
    elif NEW[:40] in content:
        print("  ✅ Already applied")
    else:
        print("  ❌ Text not found")
