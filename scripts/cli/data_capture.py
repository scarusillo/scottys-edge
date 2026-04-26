"""
Data-capture CLI commands — opener line snapshots, score scrapes,
injury fetches, prop fetches, and the weekend soccer-only run wrapper.

Extracted from main.py in v26.0 Phase 8 (CLI modularization).

Re-exported from main for back-compat — `from main import cmd_X` keeps
working so the dispatcher in main.py + any external scripts that imported
these directly are unchanged.
"""
import os
import sys


def cmd_opener(args):
    """
    Capture opening lines for all today's games.
    This is the CLV baseline — we compare our bet lines to closing lines.
    Low API cost: just odds + scores (~18 calls).
    """
    from datetime import datetime
    do_email = has_flag(args, '--email')
    sports = get_sports(args)

    # Auto-detect active tennis tournaments
    if not any(s.startswith('tennis_') for s in sports):
        try:
            active_tennis = _detect_tennis_sports()
            if active_tennis:
                sports.extend(active_tennis)
        except Exception:
            pass

    print("="*60)
    print(f"  📌 OPENING LINE CAPTURE — {datetime.now().strftime('%Y-%m-%d %I:%M %p ') + ('EDT' if 3 <= __import__('datetime').datetime.now().month <= 10 else 'EST')}")
    print("="*60)

    # Fetch scores (grade yesterday's games)
    print("\n📊 Fetching scores...")
    try:
        from odds_api import fetch_scores
        for sp in sports:
            try: fetch_scores(sp, days_back=3)
            except Exception as e: print(f"  {sp}: {e}")
    except Exception as e: print(f"  {e}")

    # Fetch current odds (these become "openers" via auto-capture)
    print("\n📈 Capturing opening lines...")
    try:
        from odds_api import fetch_odds
        for sp in sports:
            try: fetch_odds(sp, tag='OPENER')
            except Exception as e: print(f"  {sp}: {e}")
    except Exception as e: print(f"  {e}")

    # v12 FIX: NO prop fetching at opener. Props haven't stabilized at 8am
    # and cost ~279 usage per run. Props are fetched at 11am and 5pm runs only.
    print("\n✅ Opening lines captured — CLV baseline set (props at 11am)")

    if do_email:
        try:
            from emailer import send_email
            send_email("📌 Opening Lines Captured",
                f"Opening lines captured at {datetime.now().strftime('%I:%M %p EST')}.\n"
                f"CLV baseline set for today's games.\n"
                f"Picks coming at 11 AM and 5:30 PM.")
        except Exception: pass


# ═══════════════════════════════════════════════════════════════════
# SNAPSHOT — Pre-game odds capture (6:30 PM EST)
# ═══════════════════════════════════════════════════════════════════



def cmd_snapshot(args):
    """
    Capture a pre-game odds snapshot closer to tip-off.
    
    Our CLV "closing line" is the last snapshot before gametime.
    Running at 6:30 PM gets us within 30-90 mins of tip for most games,
    much better than the 5:00 PM run's 2+ hour gap.
    
    Ultra-low API cost: ~11 calls (just odds, no props, no picks).
    No picks generated. No model runs. Just odds data for CLV accuracy.
    """
    from datetime import datetime
    sports = get_sports(args)

    print("=" * 60)
    print(f"  📸 PRE-GAME SNAPSHOT — {datetime.now().strftime('%Y-%m-%d %I:%M %p ') + ('EDT' if 3 <= __import__('datetime').datetime.now().month <= 10 else 'EST')}")
    print("=" * 60)

    print("\n📈 Capturing pre-game odds...")
    try:
        from odds_api import fetch_odds
        for sp in sports:
            try:
                fetch_odds(sp, tag='SNAPSHOT')
                print(f"  ✅ {sp}")
            except Exception as e:
                print(f"  {sp}: {e}")
    except Exception as e:
        print(f"  {e}")

    print("\n✅ Pre-game snapshot captured — CLV closing lines updated.")
    print("   These become the 'closing line' for tonight's games.")


# ═══════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════
# ARB SCANNER — Cross-book arbitrage detection
# ═══════════════════════════════════════════════════════════════════

NY_LEGAL_BOOKS_ARB = {'DraftKings', 'FanDuel', 'BetMGM', 'Caesars', 'BetRivers',
                      'Fanatics', 'ESPN BET', 'PointsBet'}



def cmd_props(args):
    """Fetch and evaluate player props."""
    import sqlite3
    print("🎯 Fetching player props...")
    try:
        from odds_api import fetch_props
        for sp in PROP_SPORTS:
            try:
                fetch_props(sp)
                print(f"  ✅ {sp}: props fetched")
            except Exception as e:
                print(f"  {sp}: {e}")
    except Exception as e:
        print(f"  {e}")

    print("\n🧠 Evaluating props...")
    from props_engine import evaluate_props, print_props
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db)
    picks = evaluate_props(conn)
    conn.close()
    print_props(picks)




def cmd_run_soccer(args):
    """Weekend early morning run — soccer totals only.
    European games kick off 10am-3pm ET on weekends.
    This 7am run catches them before kickoff."""
    print("=" * 60)
    print("  SCOTTY'S EDGE — Weekend Soccer Run")
    print(f"  {datetime.now().strftime('%Y-%m-%d %I:%M %p')} EDT")
    print("=" * 60)

    # Force soccer-only sports
    soccer_sports = [s for s in ALL_SPORTS if 'soccer' in s]
    if not soccer_sports:
        print("  No soccer leagues enabled.")
        return

    # Reuse cmd_run logic but override sports
    import sys
    # Inject --sport flags for each soccer league
    soccer_args = list(args)
    # Run the standard pipeline but only for soccer
    original_get_sports = globals().get('get_sports')

    def _soccer_only(a):
        return soccer_sports

    # Monkey-patch get_sports temporarily
    import main as _self
    _orig = _self.get_sports
    _self.get_sports = _soccer_only
    try:
        cmd_run(args)
    finally:
        _self.get_sports = _orig




def cmd_scores(args):
    from odds_api import fetch_scores
    for sp in get_sports(args):
        try: fetch_scores(sp, days_back=3)
        except Exception as e: print(f"  {sp}: {e}")




def cmd_injuries(args):
    if '--manual' in args:
        from injury_scraper import manual_injury_entry
        manual_injury_entry()
    else:
        from injury_scraper import fetch_and_apply_all
        fetch_and_apply_all()


