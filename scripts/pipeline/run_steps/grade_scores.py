"""
cmd_grade Step 0 — score-fetching prelude.

Pulls latest game scores from multiple free sources before grading begins:
  - Odds API /scores (paid; primary)
  - ESPN tennis scoreboard (free; tennis primary, Odds API often misses)
  - ESPN baseball scoreboard (free; college baseball primary)
  - ESPN box scores (free; for NBA/NCAAB/NHL prop grading)
  - ESPN team endpoint backup (catches doubleheaders the scoreboard misses)
  - NCAA.com baseball backup (~40% college games ESPN misses)
  - Thin-team backfill (only active teams w/ <min_games results)

Extracted from main.py cmd_grade() in v26.0 Phase 8.
"""


def fetch_all_scores(ALL_SPORTS):
    """Run all score-source fetches in sequence. Side-effect only — writes
    to the `results` table. All errors caught + printed."""
    print("  Fetching latest scores...")
    try:
        from odds_api import fetch_scores
        for sp in ALL_SPORTS:
            try: fetch_scores(sp, days_back=3)
            except Exception: pass
    except Exception: pass

    # v21 FIX: Odds API scores endpoint often returns 0 results for tennis.
    # ESPN scraper reliably returns completed match scores (FREE).
    print("  Fetching ESPN tennis scores...")
    try:
        from historical_scores import fetch_tennis_scores
        for tour in ('atp', 'wta'):
            try:
                t_new = fetch_tennis_scores(tour, days_back=5, verbose=False)
                if t_new:
                    print(f"  ESPN tennis ({tour.upper()}): {t_new} new results")
            except Exception as e:
                print(f"  ESPN tennis ({tour.upper()}): {e}")
    except Exception as e:
        print(f"  ESPN tennis scores: {e}")

    # v12 FIX: Odds API doesn't return college baseball scores.
    # Fetch from ESPN scraper instead. No API cost.
    print("  Fetching ESPN baseball scores...")
    try:
        from historical_scores import fetch_season_scores
        fetch_season_scores('baseball_ncaa', days_back=5, verbose=False)
    except Exception as e:
        print(f"  ESPN baseball scores: {e}")

    # Fetch ESPN box scores for player prop grading (FREE — NBA, NCAAB, NHL)
    print("  Fetching ESPN box scores (props)...")
    try:
        from box_scores import fetch_all_box_scores
        fetch_all_box_scores(days_back=3)
    except Exception as e:
        print(f"  ESPN box scores: {e}")

    # v12.2: ESPN team endpoint backup — scoreboard misses games.
    # The team-specific schedule endpoint has ALL games including doubleheaders.
    print("  Backfilling missing scores (ESPN team endpoint)...")
    try:
        from espn_team_scores import backfill_missing
        team_new = backfill_missing(days_back=3, verbose=True)
        if team_new:
            print(f"  ESPN team endpoint: {team_new} new results")
    except Exception as e:
        print(f"  ESPN team backup: {e}")

    # v15: NCAA.com backup — ESPN misses ~40% of college baseball games.
    print("  Fetching NCAA.com baseball scores...")
    try:
        from ncaa_scores import fetch_ncaa_scores
        ncaa_new = fetch_ncaa_scores('baseball_ncaa', days_back=5, verbose=True)
        if ncaa_new:
            print(f"  NCAA.com: {ncaa_new} new results")
    except Exception as e:
        print(f"  NCAA.com scores: {e}")

    # v14: Proactively backfill thin-data teams for better Elo accuracy.
    # ESPN scoreboard misses ~40% of college games. The team endpoint gets ALL games.
    # Only looks up active teams (appearing in today's odds) with <min_games results.
    try:
        from espn_team_scores import backfill_thin_teams
        for backfill_sport in ['basketball_ncaab', 'baseball_ncaa']:
            backfill_thin_teams(backfill_sport, min_games=8, max_lookups=30, verbose=True)
    except Exception as e:
        print(f"  ESPN thin-team backfill: {e}")
