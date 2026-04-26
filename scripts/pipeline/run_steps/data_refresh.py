"""
Steps 1-5b of the cmd_run pipeline — pure side-effect data refresh.

Runs (in order):
  Step 1: Scores                — SKIPPED on hourly runs (grade handles at 4am)
  Step 2: Injuries              — ESPN injury scrape + apply (FREE)
  Step 3: Odds                  — fresh odds across all sports (parallel, paid)
  Step 4: Player props          — per-event prop fetches (parallel, paid)
  Step 4b: Pitcher data         — ESPN box scores + day-of-week pitching quality
                                  + MLB probable starters (FREE)
  Step 4b2: NHL goalie data     — ESPN scoreboard scrape (FREE)
  Step 4c: Referee data         — ESPN game summaries × NBA/NCAAB/NHL (parallel, FREE)
  Step 5: Bootstrap ratings     — fill in missing power_ratings rows (FREE)
  Step 5b: Elo from results     — build per-sport Elo from historical results (FREE)

All side effects land in the DB. The function returns `total_odds_fetched`
so cmd_run can decide whether to email a no-odds alert.

Extracted from main.py cmd_run() in v26.0 Phase 8.
"""
import os
import sqlite3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_data_refresh(sports, run_type, _log, _mark, PROP_SPORTS):
    """Run Steps 1 through 5b of cmd_run.

    Args:
        sports:    list of sport keys
        run_type:  'Morning' / 'Afternoon' / 'Evening' (used in alert email body)
        _log:      logging.Logger from cmd_run (writes pipeline.log)
        _mark:     timing closure from cmd_run — call _mark('step_N') after each step
        PROP_SPORTS: list of sport keys that support player props

    Returns: int — total CURRENT-tag odds rows captured this run (for alerting).
    """
    total_odds_fetched = 0

    # Step 1: Scores — SKIPPED on hourly runs (grade handles it at 4am).
    # Hourly picks don't need game scores — only used for grading. Skipping
    # saves ~14 API calls + ~30-60s per run. If cmd_grade runs independently
    # each morning, scores are always up-to-date by the time we need them.
    print("\n📊 Step 1: Scores SKIPPED (grade job handles this at 4am).")
    _mark('step1_scores')

    # Step 2: Injuries (FREE)
    print("\n🏥 Step 2: Injuries (FREE)...")
    try:
        from injury_scraper import fetch_and_apply_all
        fetch_and_apply_all()
    except Exception as e: print(f"  {e}")
    _mark('step2_injuries')

    # Step 3: Fetch fresh odds so predictions use CURRENT market lines.
    # Stale lines produce stale picks (e.g., Lakers +3.5 when market moved to +6.5).
    # The model should always evaluate against what subscribers can actually bet NOW.
    # v25.34: parallelized across sports. Each fetch_odds creates its own DB
    # connection so SQLite write serialization handles concurrency; the network
    # I/O (previously 14 sequential ~2s calls = ~28s) runs concurrent now.
    print("\n📈 Step 3: Fetching current odds (parallel)...")
    total_odds_fetched = 0
    try:
        from odds_api import fetch_odds
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=5) as _ex:
            _futures = {_ex.submit(fetch_odds, sp, tag='CURRENT'): sp for sp in sports}
            for _future in as_completed(_futures):
                _sp = _futures[_future]
                try:
                    _future.result()
                except Exception as e:
                    print(f"  {_sp}: {e}")
    except Exception as e:
        print(f"  Odds fetch: {e}")

    # Health check: count how many odds rows were stored this run
    try:
        _hc_db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
        _hc_conn = sqlite3.connect(_hc_db)
        _today = datetime.now().strftime('%Y-%m-%d')
        total_odds_fetched = _hc_conn.execute(
            "SELECT COUNT(*) FROM odds WHERE snapshot_date = ? AND tag = 'CURRENT'",
            (_today,)
        ).fetchone()[0]
        _hc_conn.close()
    except Exception as e:
        print(f"  Health check DB error: {e}")

    if total_odds_fetched == 0:
        print("\n" + "!" * 60)
        print("  ⚠ WARNING: ZERO odds rows fetched across ALL sports!")
        print("  Possible API outage or off-day.")
        print("!" * 60)
        try:
            from emailer import send_email as _alert_email
            _alert_email(
                "⚠ ALERT: Zero odds fetched — possible API outage",
                f"Scotty's Edge {run_type} Run at {datetime.now().strftime('%I:%M %p')}\n\n"
                f"Zero odds data was fetched across all {len(sports)} sports.\n"
                f"This could indicate an API outage or a legitimate off-day.\n\n"
                f"Sports checked: {', '.join(sports)}\n"
                f"Pipeline will continue but picks may be empty."
            )
        except Exception as e:
            print(f"  Alert email failed: {e}")

    _log.info(f"Step 3: Odds fetch complete | {total_odds_fetched} rows")
    _mark('step3_odds')

    # Step 4: Player Props
    # v25.34: parallelized across sports. fetch_props makes per-event API calls
    # internally (~8-15 events per sport), so this is the biggest network I/O
    # sink in the pipeline. Running prop-supported sports concurrently
    # lets NBA+NHL+MLB+NCAAB fetch in parallel instead of stacked.
    print("\n🎯 Step 4: Player props (parallel)...")
    try:
        from odds_api import fetch_props
        from concurrent.futures import ThreadPoolExecutor, as_completed
        _prop_sports = [sp for sp in sports if sp in PROP_SPORTS]
        for sp in sports:
            if sp not in PROP_SPORTS:
                print(f"  {sp}: props not available")
        if _prop_sports:
            with ThreadPoolExecutor(max_workers=4) as _ex:
                _futures = {_ex.submit(fetch_props, sp): sp for sp in _prop_sports}
                for _future in as_completed(_futures):
                    _sp = _futures[_future]
                    try:
                        _future.result()
                    except Exception as e:
                        print(f"  {_sp} props error: {e}")
    except Exception as e: print(f"  Props: {e}")
    _mark('step4_props')

    # Step 4b: Pitcher data (FREE — ESPN box scores + day-of-week quality)
    if any('baseball' in s for s in sports):
        print("\n⚾ Step 4b: Pitcher data (FREE)...")
        try:
            from pitcher_scraper import scrape_pitcher_data, build_pitching_quality, scrape_mlb_pitchers
            scrape_pitcher_data(days_back=3, verbose=True)
            build_pitching_quality(verbose=True)
            # MLB probable pitchers — MUST run before predictions so the
            # pitcher gate in model_engine can skip games with TBD starters
            if any(s == 'baseball_mlb' for s in sports):
                print("  Fetching MLB probable pitchers...")
                scrape_mlb_pitchers(verbose=True)
        except Exception as e:
            print(f"  Pitcher scraper: {e}")

    # Step 4b2: NHL goalie data (FREE — ESPN scoreboard)
    if any(s == 'icehockey_nhl' for s in sports):
        print("\n\U0001f3d2 Step 4b2: NHL goalie data (FREE)...")
        try:
            from pitcher_scraper import scrape_nhl_goalies
            scrape_nhl_goalies(verbose=True)
        except Exception as e:
            print(f"  NHL goalie scraper: {e}")

    _mark('step4b_pitchers_goalies')

    # Step 4c: Referee/official data (FREE — ESPN game summaries)
    # v25.34: parallelized across 3 sports.
    print("\n🏛️ Step 4c: Referee data (FREE, parallel)...")
    try:
        from referee_engine import scrape_officials
        from concurrent.futures import ThreadPoolExecutor
        _ref_sports = ['basketball_nba', 'basketball_ncaab', 'icehockey_nhl']
        with ThreadPoolExecutor(max_workers=3) as _ex:
            list(_ex.map(lambda s: scrape_officials(s, days_back=3, verbose=False), _ref_sports))
        print("  Referee data updated")
    except Exception as e:
        print(f"  Referee engine: {e}")
    _mark('step4c_refs')

    # Step 5: Bootstrap missing ratings (FREE)
    print("\n🔧 Step 5: Ratings check...")
    from bootstrap_ratings import bootstrap_all
    bootstrap_all()
    _mark('step5_ratings')

    # Step 5b: Elo ratings from game results (FREE — independent of market)
    print("\n🏆 Step 5b: Elo ratings from results...")
    try:
        from elo_engine import build_elo_ratings, get_elo_ratings
        import sqlite3 as _sq
        _conn = _sq.connect(os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db'))
        for sp in sports:
            elo_data = get_elo_ratings(_conn, sp)
            if elo_data:
                confident = sum(1 for v in elo_data.values() if v['confidence'] != 'LOW')
                print(f"  ✅ {sp}: {confident} teams with Elo confidence")
            else:
                # Try building from results
                results_count = _conn.execute(
                    "SELECT COUNT(*) FROM results WHERE sport=? AND completed=1", (sp,)
                ).fetchone()[0]
                if results_count >= 20:
                    build_elo_ratings(sp, verbose=True)
                else:
                    print(f"  ⚠ {sp}: {results_count} results — need 20+ for Elo (run: python historical_scores.py)")
        _conn.close()
    except Exception as e:
        print(f"  Elo: {e} (run historical_scores.py + elo_engine.py to enable)")

    return total_odds_fetched
