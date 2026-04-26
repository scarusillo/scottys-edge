"""
Pipeline orchestrator — composes Stages 1-5 into a single callable.

After v26.0 Phases 0-5, the per-game scoring pipeline lives in:
  Stage 1 (fetch):  pipeline.stage_1_fetch.load_sport_setup
  Stage 2 (helpers):pipeline.score_helpers.* (pure scoring math)
  Stage 3 (gates):  pipeline.gates.* (18 gate predicates + log_divergence_block)
  Stage 4 (per-game):
                    pipeline.per_game.score_game_prelude
                    pipeline.per_game.fetch_game_adjustments
                    pipeline.per_game.handle_divergence_path
                      └→ pipeline.channels.elo_ml_rescue
                      └→ pipeline.channels.spread_fade_flip
                    pipeline.per_game.process_spread_path
                    pipeline.per_game.process_ml_and_cross_market
                    pipeline.per_game.process_totals_path
                      └→ pipeline.channels.data_total
  Stage 5 (merge):  pipeline.stage_5_merge.merge_and_select

`run(conn, prop_picks=None)` is the canonical entry point — gathers game picks
across ALL configured sports and applies the final merge with cross-sport caps.

Both pre-refactor entry points still ship as thin wrappers for backwards
compatibility:
  - `model_engine.generate_predictions(conn, sport=None)` — used by the harness
    + cmd_predict. Returns un-merged game picks. Body is now glue calling
    Stages 1-4. Pass `sport=X` to restrict (used by the harness per-sport).
  - `main._merge_and_select(game_picks, prop_picks, conn)` — used by cmd_run.
    Delegates to Stage 5.

Use `pipeline.orchestrator.run` for any new caller that wants the full
end-to-end pipeline as a single call.
"""


def compute_game_window(now_utc):
    """Build (window_start, window_end) ISO strings — TODAY-only filter.

    Window: now+30min through midnight Eastern (4am UTC EDT / 5am UTC EST).
    The +30min buffer ensures subscribers have time to bet before tip-off.
    """
    from datetime import timedelta
    offset_hours = 4 if 3 <= now_utc.month <= 10 else 5
    est_midnight = now_utc.replace(hour=offset_hours, minute=0, second=0, microsecond=0)
    if now_utc.hour >= 5:
        est_midnight += timedelta(days=1)
    window_start = (now_utc + timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    window_end = est_midnight.strftime('%Y-%m-%dT%H:%M:%SZ')
    return (window_start, window_end)


def score_one_sport(conn, sp, now_utc, window_start, window_end):
    """Score every today-only game for a single sport.

    Returns (picks, skip_summary) where:
        picks: list[dict] — all picks generated for this sport
        skip_summary: dict with keys 'skip_nr', 'skip_div', 'skip_w'

    Returns ([], {...}) when load_sport_setup returns None (sport has no
    in-window games or required ratings missing).
    """
    from pipeline.stage_1_fetch import load_sport_setup
    from pipeline.per_game import score_one_game

    setup = load_sport_setup(conn, sp, window_start, window_end)
    if setup is None:
        return ([], {'skip_nr': 0, 'skip_div': 0, 'skip_w': 0})

    seen = set()
    skip_nr = skip_div = 0
    picks = []

    for g in setup['games']:
        game_picks, dn, dd = score_one_game(conn, sp, g, now_utc, setup, seen)
        picks.extend(game_picks)
        skip_nr += dn
        skip_div += dd

    if skip_nr or skip_div:
        print(f"    Filtered: {skip_nr} no rating, {skip_div} divergence")

    return (picks, {'skip_nr': skip_nr, 'skip_div': skip_div, 'skip_w': 0})


def run(conn, prop_picks=None):
    """Run the full per-game scoring + merge pipeline across ALL sports.

    Args:
        conn: open sqlite3.Connection.
        prop_picks: optional pre-generated prop pick list to feed into the
                    merge stage (default empty).

    Returns: list[dict] of final picks ready to save / email / post.

    Note: per-sport merge is intentionally not supported — cross-sport caps
    (MAX_SHARP_PICKS, sport_dir tracking, GAME_CAP) only function correctly
    on the aggregate. Callers wanting just one sport's pre-merge picks should
    call `model_engine.generate_predictions(conn, sport=X)` directly.
    """
    from model_engine import generate_predictions
    from pipeline.stage_5_merge import merge_and_select

    game_picks = generate_predictions(conn) or []
    return merge_and_select(game_picks, prop_picks or [], conn=conn)
