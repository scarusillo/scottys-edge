"""
Admin / maintenance CLI commands — bootstrap, ratings reboot, email
smoke test, budget check, scheduler setup, name normalization, etc.

Extracted from main.py in v26.0 Phase 8 (CLI modularization).

Re-exported from main for back-compat — `from main import cmd_X` keeps
working so the dispatcher in main.py + any external scripts that imported
these directly are unchanged.
"""
import os
import sys
from model_engine import DB_PATH


def cmd_bootstrap(args):
    from bootstrap_ratings import bootstrap_all
    bootstrap_all()




def cmd_reboot_ratings(args):
    import sqlite3
    db = DB_PATH
    conn = sqlite3.connect(db)
    for sp in get_sports(args):
        conn.execute("DELETE FROM power_ratings WHERE sport=?", (sp,))
        print(f"  Wiped {sp}")
    conn.commit(); conn.close()
    from bootstrap_ratings import bootstrap_all
    bootstrap_all()




def cmd_email_test(args):
    from emailer import send_email
    from datetime import datetime
    success = send_email("✅ Scotty's Edge v11 — Email Test",
               f"Email is working!\nSent at {datetime.now().strftime('%I:%M %p EST')}\n\n"
               f"Your automated schedule (4 runs/day):\n"
               f"  8:00 AM — Opening lines capture (CLV baseline)\n"
               f"  11:00 AM — Morning picks\n"
               f"  5:30 PM — Afternoon picks\n"
               f"  9:00 AM — Daily grading + CLV report\n")
    if not success:
        print("""
  ────────────────────────────────────────────────────────
  EMAIL TROUBLESHOOTING:

  1. Make sure 2FA is enabled on your Google account:
     https://myaccount.google.com/signinoptions/two-step-verification

  2. Create an App Password (NOT your regular password):
     https://myaccount.google.com/apppasswords

  3. The password is 16 characters with NO spaces:
     setx GMAIL_APP_PASSWORD "abcdefghijklmnop"

  4. Close and REOPEN CMD after setting it.
  ────────────────────────────────────────────────────────""")




def cmd_budget(args):
    print("""
  📊 API BUDGET — 20,000 credits/month
  ══════════════════════════════════════════════════════════════
  Command                              | API Calls | Cost
  -------------------------------------|-----------|------
  python main.py opener                | ~18       | PAID (odds+scores)
  python main.py run                   | ~30       | PAID (odds+scores+props)
  python main.py run --sport nba       | ~5        | PAID
  python main.py predict               | 0         | FREE
  python main.py props                 | ~10       | PAID (props only)
  python main.py grade                 | ~6        | PAID (scores only)
  python main.py injuries              | 0         | FREE
  python main.py bootstrap             | 0         | FREE
  python main.py report                | 0         | FREE

  4-RUN DAILY SCHEDULE:
    8am opener (18) + 11am run (30) + 5:30pm run (30) + 9am grade (6) = 84/day
    84/day × 30 days = 2,520/month (12.6% of budget)

  Plenty of room for manual runs and experimentation.
  ══════════════════════════════════════════════════════════════""")




def cmd_log(args):
    from pick_logger import get_log_summary
    print(get_log_summary())




def cmd_historical(args):
    """Pull full-season historical scores from ESPN (FREE) + fix names + build Elo ratings."""
    from historical_scores import fetch_all_historical
    sports = get_sports(args) if '--sport' in ' '.join(args) else None
    days = int(args[args.index('--days') + 1]) if '--days' in args else None
    fetch_all_historical(sports=sports, days_back=days)

    # Fix team name mismatches between ESPN and Odds API
    print()
    from fix_names import full_fix
    full_fix(sports=sports, diagnose_only=False)




def cmd_elo(args):
    """Build/rebuild Elo ratings from existing results."""
    from elo_engine import build_all_elo, analyze_model
    sports = get_sports(args) if '--sport' in ' '.join(args) else None
    build_all_elo(sports=sports)

    if '--analyze' in args:
        target = sports or list(ELO_CONFIG.keys()) if 'ELO_CONFIG' in dir() else get_sports(args)
        for sp in target:
            analyze_model(sp)




def cmd_setup_scheduler(args):
    from model_engine import SCRIPTS_DIR as scripts_dir
    print(f"""
  🕐 AUTO-SCHEDULER SETUP — 4 Daily Runs (Windows Task Scheduler)
  ══════════════════════════════════════════════════════════════

  STEP 1: Make sure env variables are set permanently:
    setx ODDS_API_KEY "your_odds_api_key_here"
    setx GMAIL_APP_PASSWORD "your16charpassword"

  STEP 2: Open CMD as Administrator and run these 4 commands:

    schtasks /create /tn "BettingModel_Opener" /tr "cmd /c cd /d {scripts_dir} && python main.py opener --email" /sc daily /st 08:00 /f

    schtasks /create /tn "BettingModel_Morning" /tr "cmd /c cd /d {scripts_dir} && python main.py run --email" /sc daily /st 11:00 /f

    schtasks /create /tn "BettingModel_Afternoon" /tr "cmd /c cd /d {scripts_dir} && python main.py run --email" /sc daily /st 17:30 /f

    schtasks /create /tn "BettingModel_Grade" /tr "cmd /c cd /d {scripts_dir} && python main.py grade --email" /sc daily /st 09:00 /f

  STEP 3: Verify all 4 tasks:
    schtasks /query /tn "BettingModel_Opener"
    schtasks /query /tn "BettingModel_Morning"
    schtasks /query /tn "BettingModel_Afternoon"
    schtasks /query /tn "BettingModel_Grade"

  NOTE: Computer must be ON (not sleeping) for tasks to run.

  WHY 5 RUNS:
    7am SAT/SUN — Weekend soccer picks (European games kick off 10am-3pm ET)
    8am  — Captures opening lines (CLV requires knowing where lines started)
    11am — Morning picks (NCAAB games start early afternoon)
    5:30pm — Afternoon picks (NBA/NHL evening games)
    9am  — Grades yesterday + CLV analysis (the #1 metric)

  STEP 4 (OPTIONAL): Weekend soccer early run (Saturday & Sunday only):
    schtasks /create /tn "BettingModel_Soccer_Weekend" /tr "cmd /c cd /d {scripts_dir} && python main.py run-soccer --email" /sc weekly /d SAT,SUN /st 07:00 /f
  ══════════════════════════════════════════════════════════════""")




def cmd_fix_names(args):
    """Diagnose and fix team name mismatches."""
    from fix_names import full_fix
    sports = get_sports(args) if '--sport' in ' '.join(args) else None
    diagnose_only = '--diagnose' in args
    full_fix(sports=sports, diagnose_only=diagnose_only)


