"""
Preview / analysis CLI commands — dry-run predict, performance report,
and the backtest harness wrapper.

Extracted from main.py in v26.0 Phase 8 (CLI modularization).

Re-exported from main for back-compat — `from main import cmd_X` keeps
working so the dispatcher in main.py + any external scripts that imported
these directly are unchanged.
"""
import os
import sys


def cmd_predict(args):
    """Preview picks WITHOUT saving to DB. Use 'run' for actual picks."""
    import sqlite3
    from model_engine import generate_predictions, print_picks
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db)
    sports = get_sports(args)
    # v25.68: auto-detect active tennis tournaments (mirrors cmd_run logic).
    # Without this, predict silently skipped tennis even when Madrid/Monte Carlo
    # etc were live — making dry-run diagnosis of tennis picks impossible.
    if not any(s.startswith('tennis_') for s in sports):
        try:
            active_tennis = _detect_tennis_sports()
            if active_tennis:
                sports = list(sports) + list(active_tennis)
        except Exception:
            pass
    all_picks = []
    for sp in sports:
        picks = generate_predictions(conn, sport=sp)
        all_picks.extend(picks)
    all_picks.sort(key=lambda x: x['star_rating']*100 + x['edge_pct'], reverse=True)
    conn.close()
    # v12 FIX: predict is PREVIEW ONLY. Does NOT save to DB.
    # Previously predict saved unfiltered picks, which dedup then prevented
    # 'run' from correcting. Use 'run' to save actual picks.
    print(f"\n  ⚠ PREVIEW ONLY — {len(all_picks)} raw picks (not saved, not filtered)")
    print(f"  Use 'python main.py run --email' to generate filtered picks.\n")
    print_picks(all_picks)




def cmd_report(args):
    import sqlite3
    from grader import performance_report
    days = 7
    if '--days' in args:
        days = int(args[args.index('--days')+1])
    sport = None
    sports = get_sports(args)
    if len(sports) == 1: sport = sports[0]
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db)
    performance_report(conn, days=days, sport=sport)
    conn.close()




def cmd_backtest(args):
    """Backtest model accuracy against historical results."""
    from backtest import run_all_backtests
    sports = None
    if '--sport' in ' '.join(args):
        sports_list = get_sports(args)
        sports = sports_list
    min_edge = 2.0
    if '--min-edge' in args:
        idx = args.index('--min-edge')
        min_edge = float(args[idx + 1])
    run_all_backtests(sports=sports, min_edge=min_edge)

