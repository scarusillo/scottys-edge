"""
Per-game scoring (Stage 4 of the v26.0 pipeline refactor).

This module hosts the per-game work that was previously the body of
`for g in games:` inside `model_engine.generate_predictions`. The full body
(~1,780 lines) is being moved here in chunks; each chunk is replay-verified
against a captured baseline before the next one starts.

The first extracted slice is `score_game_prelude` — the minimal entry filters
that decide whether a game should be scored at all.
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def score_game_prelude(conn, sp, g, now_utc, setup):
    """Pre-scoring entry filters for a single game.

    Extracted from `model_engine.generate_predictions()` lines 1387-1470 in
    v26.0 Phase 4-prep (chunk A). Behavior is byte-equivalent to the original
    inline code; verified by tests/shadow_predict.py.

    Args:
        conn: open sqlite3.Connection (read + occasional write to
            `shadow_blocked_picks`; this function performs no writes).
        sp: sport key string (e.g. 'basketball_nba').
        g: raw game tuple from `setup['games']` — at minimum
            (eid, commence, home, away, ...) at indices 0-3.
        now_utc: timezone-aware datetime in UTC, used for the in-progress
            cutoff (5-minute buffer behind real game time).
        setup: dict returned by `pipeline.stage_1_fetch.load_sport_setup`
            with at least `ratings` and `elo_data`.

    Returns:
        (proceed, prelude, skip_nr_delta) where
            proceed (bool): True → caller continues scoring this game with
                            `prelude` values. False → caller should skip
                            (game in progress, missing model spread, missing
                            confirmed pitcher/goalie, etc.).
            prelude (dict | None): when proceed=True, contains the computed
                            per-game values the rest of the loop body needs:
                              {'eid', 'commence', 'home', 'away',
                               'ms', '_neutral',
                               '_mlb_pitcher_info', '_nhl_goalie_info'}
                            None when proceed=False.
            skip_nr_delta (int): amount to add to the caller's skip_nr
                            counter (0 or 1). Counter semantics preserved
                            exactly from the original loop.
    """
    # Lazy imports — the helpers live in `model_engine` and `pitcher_scraper`,
    # which already import this module's parent. Lazy resolution avoids the
    # circular-import surface entirely.
    from model_engine import (
        compute_model_spread,
        blended_spread,
        HAS_ELO,
        HAS_MLB_PITCHERS,
        HAS_NHL_GOALIES,
    )
    from pipeline.score_helpers import compute_neutral_site

    eid, commence, home, away = g[0], g[1], g[2], g[3]

    # Skip games already in progress or about to start.
    # 5-minute buffer accounts for clock drift between API and real tip-off.
    if commence:
        try:
            game_time = datetime.fromisoformat(commence.replace('Z', '+00:00'))
            if game_time < now_utc - timedelta(minutes=5):
                return (False, None, 0)
        except Exception:
            pass

    ratings = setup['ratings']
    elo_data = setup['elo_data']

    ms = compute_model_spread(home, away, ratings, sp)
    if ms is None:
        return (False, None, 1)

    _neutral = compute_neutral_site(sp)

    # If Elo ratings available, use blended spread (Elo + power ratings).
    # NCAAB requires Elo — bootstrap is circular without it.
    if HAS_ELO and elo_data:
        elo_ms = blended_spread(home, away, elo_data, ratings, sp, conn, neutral_site=_neutral)
        if elo_ms is not None:
            ms = elo_ms
        elif sp == 'basketball_ncaab':
            return (False, None, 1)

    # MLB pitcher gate — pitching is THE dominant factor; flying blind without
    # both starters confirmed. College baseball exempt (ESPN rarely lists
    # college probables).
    _mlb_pitcher_info = None
    if sp == 'baseball_mlb' and HAS_MLB_PITCHERS:
        from pitcher_scraper import get_mlb_probable_starters
        try:
            _game_date = None
            if commence:
                try:
                    _gdt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
                    _game_date = _gdt.astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')
                except (ValueError, AttributeError):
                    pass
            _mlb_pitcher_info = get_mlb_probable_starters(conn, home, away, _game_date)
            if not _mlb_pitcher_info.get('both_confirmed', False):
                print(f"    ⚠ MLB pick skipped: no pitcher data for "
                      f"{away} @ {home} ({_mlb_pitcher_info.get('summary', 'TBD')})")
                return (False, None, 0)
        except Exception as _pe:
            print(f"    ⚠ MLB pick skipped: pitcher lookup error for "
                  f"{away} @ {home}: {_pe}")
            return (False, None, 0)

    # NHL goalie gate — starting goalie is the single biggest factor; .920 vs
    # .900 SV% is ~0.5 goals/game.
    _nhl_goalie_info = None
    if sp == 'icehockey_nhl' and HAS_NHL_GOALIES:
        from pitcher_scraper import get_nhl_probable_goalies
        try:
            _game_date = None
            if commence:
                try:
                    _gdt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
                    _game_date = _gdt.astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')
                except (ValueError, AttributeError):
                    pass
            _nhl_goalie_info = get_nhl_probable_goalies(conn, home, away, _game_date)
            if not _nhl_goalie_info.get('both_confirmed', False):
                print(f"    ⚠ NHL pick skipped: no goalie data for "
                      f"{away} @ {home} ({_nhl_goalie_info.get('summary', 'TBD')})")
                return (False, None, 0)
        except Exception as _ge:
            print(f"    ⚠ NHL pick skipped: goalie lookup error for "
                  f"{away} @ {home}: {_ge}")
            return (False, None, 0)

    return (True, {
        'eid': eid,
        'commence': commence,
        'home': home,
        'away': away,
        'ms': ms,
        '_neutral': _neutral,
        '_mlb_pitcher_info': _mlb_pitcher_info,
        '_nhl_goalie_info': _nhl_goalie_info,
    }, 0)


def fetch_game_adjustments(conn, sp, home, away):
    """Fetch injury context + referee adjustment for a game.

    Pre-pick fixtures shared by every downstream channel — Elo ML, spreads,
    totals, props all consume these. Fetched once per game to avoid repeated
    DB hits.

    Returns dict with:
        h_inj, h_cl, h_imp     — home team injury list, count, impact pts
        a_inj, a_cl, a_imp     — away team injury list, count, impact pts
        ref_adj, ref_info      — referee total-adjustment + descriptor (0.0/''
                                 if referee data unavailable for this sport)

    Extracted from model_engine.generate_predictions() lines 1473-1489 in
    v26.0 Phase 4-prep (chunk B).
    """
    from model_engine import get_team_injury_context, HAS_REF

    h_inj, h_cl, h_imp = get_team_injury_context(conn, home, sp)
    a_inj, a_cl, a_imp = get_team_injury_context(conn, away, sp)

    ref_adj, ref_info = 0.0, ''
    if HAS_REF:
        from referee_engine import get_ref_adjustment
        try:
            ref_adj, ref_info = get_ref_adjustment(home, away, sp, conn)
        except Exception:
            pass  # Ref data is supplementary — don't crash

    return {
        'h_inj': h_inj, 'h_cl': h_cl, 'h_imp': h_imp,
        'a_inj': a_inj, 'a_cl': a_cl, 'a_imp': a_imp,
        'ref_adj': ref_adj, 'ref_info': ref_info,
    }


def handle_divergence_path(conn, sp, prelude, adj, mkt, setup, seen):
    """Run the model-vs-market divergence branch (Elo ML rescue + DATA_SPREAD +
    SPREAD_FADE_FLIP).

    The divergence path fires when |ms - mkt_hs| exceeds the sport's
    `max_spread_divergence`. When it fires, the game is consumed — no further
    spread/ML/totals scoring runs for that game (caller `continue`s and bumps
    skip_div by 1).

    Three sub-channels run inside this branch:
      1. Elo ML rescue — if enough Elo seasoning, fire a confidence-weighted
         ML pick on whichever side has edge. Hard-block on star injuries.
      2. DATA_SPREAD (v25.39) — for NHL/MLS/EPL, if Context Model brings the
         spread within max_div of market, fire on Context's preferred side.
      3. SPREAD_FADE_FLIP (v25.36) — for NBA/NHL, fade the model when only
         Elo (not Context) disagrees with market. Vetoed by v25.60 dual-model
         agreement check.

    Mutates `seen` (adds Elo ML keys when picks fire). Caller adds the
    returned picks to all_picks and bumps skip_div by 1.

    Returns (consumed, picks) where:
        consumed (bool): True → divergence path took this game; caller `continue`s.
                         False → ms is within max_div; caller proceeds normally.
        picks (list[dict]): new picks generated by this path (may be empty even
                            when consumed, e.g. when Elo ML rescue blocks on
                            seasoning or all sub-channels skip).

    Extracted from model_engine.generate_predictions() lines 1491-1759 in
    v26.0 Phase 4-prep (chunk B).
    """
    from datetime import datetime as _dt
    from model_engine import (
        HAS_ELO, _log_divergence_block, _mk_ml,
        get_star_rating, bet_timing_advice, devig_ml_odds,
    )

    eid = prelude['eid']
    commence = prelude['commence']
    home = prelude['home']
    away = prelude['away']
    ms = prelude['ms']
    _neutral = prelude['_neutral']

    h_imp = adj['h_imp']
    a_imp = adj['a_imp']

    mkt_hs = mkt['mkt_hs']; mkt_hs_odds = mkt['mkt_hs_odds']; mkt_hs_book = mkt['mkt_hs_book']
    mkt_as = mkt['mkt_as']; mkt_as_odds = mkt['mkt_as_odds']; mkt_as_book = mkt['mkt_as_book']
    hml = mkt['hml']; hml_book = mkt['hml_book']; aml = mkt['aml']; aml_book = mkt['aml_book']

    elo_data = setup['elo_data']
    cfg = setup['cfg']
    min_pv = setup['min_pv']
    min_pv_ml = setup['min_pv_ml']

    picks = []

    max_div = cfg['max_spread_divergence']
    if mkt_hs is None or abs(ms - mkt_hs) <= max_div:
        return (False, picks)

    # ═══ Elo ML rescue (v26.0 Phase 4 → pipeline.channels.elo_ml_rescue) ═══
    from pipeline.channels.elo_ml_rescue import try_elo_ml_rescue
    _ml_picks, _seasoning_failed = try_elo_ml_rescue(
        conn, sp, prelude, adj, mkt, setup, seen)
    picks.extend(_ml_picks)
    if _seasoning_failed:
        _log_divergence_block(conn, sp, eid, home, away, ms, mkt_hs, 'insufficient_elo_games')
        return (True, picks)

    # v25.39: CONTEXT MODEL check (NHL + MLS + EPL).
    # Kept inline — it is a divergence-rescue context check rather than a
    # standalone channel; the result (_context_fired) controls SPREAD_FADE_FLIP.
    CONTEXT_MODEL_SPORTS = {
        'icehockey_nhl', 'soccer_usa_mls', 'soccer_epl',
    }
    _context_fired = False
    if (sp in CONTEXT_MODEL_SPORTS
            and mkt_hs is not None and mkt_as is not None
            and mkt_hs_odds is not None and mkt_as_odds is not None):
        try:
            from context_spread_model import compute_context_spread, format_context_summary
            _commence_date = (commence[:10] if commence else None)
            ms_ctx, _ctx_info = compute_context_spread(
                conn, sp, home, away, eid, ms, _commence_date)
            _ctx_div = abs(ms_ctx - mkt_hs)
            if _ctx_div <= max_div:
                if ms_ctx < mkt_hs:
                    _c_team, _c_line, _c_odds, _c_book = home, mkt_hs, mkt_hs_odds, mkt_hs_book
                else:
                    _c_team, _c_line, _c_odds, _c_book = away, mkt_as, mkt_as_odds, mkt_as_book
                from config import MIN_ODDS as _CM_MIN_ODDS
                if (_c_odds is not None and _c_odds > _CM_MIN_ODDS
                        and _c_odds <= 140 and _c_book):
                    _ctx_summary = format_context_summary(_ctx_info)
                    _ctx_ctx = (
                        f'DATA_SPREAD v25.39 — {_ctx_summary} | '
                        f'Market {mkt_hs:+.1f}, Context {ms_ctx:+.1f} '
                        f'(ctx_div={_ctx_div:.1f} ≤ {max_div}). '
                        f'Bet {_c_team} {_c_line:+.1f} @ {_c_book} {_c_odds:+.0f}.'
                    )
                    _ctx_pick = {
                        'sport': sp, 'event_id': eid, 'commence': commence,
                        'home': home, 'away': away,
                        'market_type': 'SPREAD',
                        'selection': f'{_c_team} {_c_line:+.1f}',
                        'book': _c_book, 'line': _c_line, 'odds': _c_odds,
                        'model_spread': ms,
                        'model_prob': 0, 'implied_prob': 0,
                        'edge_pct': 0,
                        'star_rating': 3, 'units': 5.0,
                        'confidence': 'DATA_SPREAD',
                        'side_type': 'DATA_SPREAD',
                        'spread_or_ml': 'SPREAD',
                        'timing': 'STANDARD',
                        'context': _ctx_ctx,
                        'notes': _ctx_ctx,
                    }
                    print(f"  🧠 DATA_SPREAD: {sp.split('_')[-1]} {_c_team} "
                          f"{_c_line:+.1f} @ {_c_book} {_c_odds:+.0f} "
                          f"(ctx_div {_ctx_div:.1f}, raw {abs(ms-mkt_hs):.1f})")
                    picks.append(_ctx_pick)
                    _context_fired = True
        except Exception as _ce:
            print(f"  ⚠ DATA_SPREAD error: {_ce}")

    # ═══ SPREAD_FADE_FLIP (v26.0 Phase 4 → pipeline.channels.spread_fade_flip) ═══
    from pipeline.channels.spread_fade_flip import try_spread_fade_flip
    _ff_picks, _ff_vetoed = try_spread_fade_flip(
        conn, sp, prelude, mkt, _context_fired)
    picks.extend(_ff_picks)
    if _ff_vetoed:
        _log_divergence_block(conn, sp, eid, home, away, ms, mkt_hs, 'post_elo_rescue')
        return (True, picks)

    _log_divergence_block(conn, sp, eid, home, away, ms, mkt_hs, 'post_elo_rescue')
    return (True, picks)


def process_spread_path(conn, sp, prelude, adj, mkt, setup, seen, max_div):
    """All non-divergent spread-path work for a single game.

    Runs (in order):
      1. STEAM_CHASE (v25.72) — pattern-recognition channel. Follows
         FanDuel sharp-book spread movement when the move >= sport threshold.
      2. ML-only divergence check — for baseball games with no spread line,
         infer market spread from ML odds and check divergence vs ms.
      3. Context engine spread adjustments — applied AFTER divergence check
         so legit context factors aren't filtered out (mutates ms in-place).
      4. Tennis H2H adjustment — surface-specific matchup edge added to ms.
      5. Injury spread adjustment — net injury impact (50% weight, market
         prices the rest) → ms_inj.
      6. DIV_EXPANDED tracking (v25.29) — picks passing only because of a
         loosened divergence threshold get tagged + unit-capped.
      7. HOME spread pick — Walters edge assessment + Kelly sizing + context.
      8. AWAY spread pick — same.

    Removed in v26.0 Phase 4:
      - CONTEXT_STANDALONE_SPREAD (DATA_SPREAD Path 2) — disabled v25.70
        (NBA -5.65u, NHL -7.43u, Serie A +5u backtest); dict was empty so
        the block was guaranteed dead.

    Mutates `seen` (adds spread keys when picks fire). Caller adds returned
    picks to all_picks.

    Returns (consumed, picks, ctx_state) where:
        consumed (bool): True → ml_only divergence triggered; caller
                         `continue`s + bumps skip_div by 1.
        picks (list[dict]): new picks generated by this path.
        ctx_state (dict): {ms, ms_inj, ctx, _h2h_ctx} — modified spread,
                          injury-adjusted spread, context_engine summary,
                          tennis H2H context. Needed by downstream ML +
                          totals scoring.

    Note: the original loop also incremented `skip_w` when wa['is_play']
    was False or point_value_pct was below min_pv. That counter is
    diagnostic-only ("Filtered: N below threshold") and not extracted here;
    a future cleanup can fold it into the return tuple.

    Extracted from model_engine.generate_predictions() lines 1493-1851 in
    v26.0 Phase 4-prep (chunk C).
    """
    from model_engine import (
        HAS_CONTEXT, _log_divergence_block, _mk,
        _soccer_draw_prob, _tennis_h2h_adjustment,
        american_to_implied_prob, scottys_edge_assessment, spread_to_cover_prob,
        kelly_units,
    )

    eid = prelude['eid']
    commence = prelude['commence']
    home = prelude['home']
    away = prelude['away']
    ms = prelude['ms']
    _nhl_goalie_info = prelude['_nhl_goalie_info']

    h_inj = adj['h_inj']; h_cl = adj['h_cl']; h_imp = adj['h_imp']
    a_inj = adj['a_inj']; a_cl = adj['a_cl']; a_imp = adj['a_imp']

    mkt_hs = mkt['mkt_hs']; mkt_hs_odds = mkt['mkt_hs_odds']; mkt_hs_book = mkt['mkt_hs_book']
    mkt_as = mkt['mkt_as']; mkt_as_odds = mkt['mkt_as_odds']; mkt_as_book = mkt['mkt_as_book']
    hml = mkt['hml']; aml = mkt['aml']

    cfg = setup['cfg']
    min_pv = setup['min_pv']

    picks = []

    # ═══ STEAM_CHASE — v25.72 pattern-recognition channel ═══
    STEAM_CHASE_MIN_MOVE = {
        'basketball_nba': 0.5,
        'icehockey_nhl': 0.5,
        'baseball_mlb': 0.5,
        'baseball_ncaa': 0.5,
        'basketball_ncaab': 0.5,
    }
    _sc_threshold = STEAM_CHASE_MIN_MOVE.get(sp)
    if _sc_threshold is not None and mkt_hs is not None:
        try:
            _sc_open_row = conn.execute("""
                SELECT line FROM openers
                WHERE event_id = ? AND book = 'FanDuel' AND market = 'spreads'
                  AND selection LIKE ? AND line IS NOT NULL LIMIT 1
            """, (eid, f'%{home}%')).fetchone()
            _sc_cur_row = conn.execute("""
                SELECT AVG(line) FROM odds
                WHERE event_id = ? AND book = 'FanDuel' AND market = 'spreads'
                  AND tag = 'CURRENT' AND selection LIKE ? AND line IS NOT NULL
            """, (eid, f'%{home}%')).fetchone()
            if (_sc_open_row and _sc_cur_row
                    and _sc_open_row[0] is not None and _sc_cur_row[0] is not None):
                _sc_open = _sc_open_row[0]
                _sc_cur = _sc_cur_row[0]
                _sc_move = _sc_open - _sc_cur
                if abs(_sc_move) >= _sc_threshold:
                    _sc_side = 'HOME' if _sc_move > 0 else 'AWAY'
                    if _sc_side == 'HOME':
                        _sc_team, _sc_line, _sc_odds, _sc_book = home, mkt_hs, mkt_hs_odds, mkt_hs_book
                    else:
                        _sc_team, _sc_line, _sc_odds, _sc_book = away, mkt_as, mkt_as_odds, mkt_as_book
                    from config import MIN_ODDS as _SC_MIN_ODDS
                    if (_sc_odds is not None and _sc_odds > _SC_MIN_ODDS
                            and _sc_odds <= 140 and _sc_book):
                        _sc_ctx = (
                            f'STEAM_CHASE v25.72 — FanDuel opener={_sc_open:+.1f}, '
                            f'current={_sc_cur:+.1f} (move={_sc_move:+.1f} ≥ {_sc_threshold}). '
                            f'Sharp on {_sc_side}. Bet {_sc_team} {_sc_line:+.1f} @ {_sc_book} {_sc_odds:+.0f}. '
                            f'Best available line across NY-legal books.'
                        )
                        _sc_pick = {
                            'sport': sp, 'event_id': eid, 'commence': commence,
                            'home': home, 'away': away,
                            'market_type': 'SPREAD',
                            'selection': f'{_sc_team} {_sc_line:+.1f}',
                            'book': _sc_book, 'line': _sc_line, 'odds': _sc_odds,
                            'model_spread': None,
                            'model_prob': 0, 'implied_prob': 0,
                            'edge_pct': 0,
                            'star_rating': 4, 'units': 5.0,
                            'confidence': 'STEAM_CHASE',
                            'side_type': 'STEAM_CHASE',
                            'spread_or_ml': 'SPREAD',
                            'timing': 'STANDARD',
                            'context': _sc_ctx,
                            'notes': _sc_ctx,
                        }
                        print(f"  ⚡ STEAM_CHASE: {sp.split('_')[-1]} {_sc_team} "
                              f"{_sc_line:+.1f} @ {_sc_book} {_sc_odds:+.0f} "
                              f"(sharp moved {_sc_move:+.1f})")
                        picks.append(_sc_pick)
        except Exception as _sce:
            print(f"  ⚠ STEAM_CHASE error: {_sce}")

    # v12 FIX: ML-only divergence check (baseball without spread lines).
    if mkt_hs is None and hml is not None and aml is not None:
        _h_ml_imp = american_to_implied_prob(hml)
        _a_ml_imp = american_to_implied_prob(aml)
        if _h_ml_imp and _a_ml_imp:
            import math as _m
            ml_sc = cfg.get('ml_scale', 7.5)
            if _h_ml_imp > 0.01 and _h_ml_imp < 0.99:
                implied_spread = -ml_sc * _m.log(_h_ml_imp / (1 - _h_ml_imp))
                if abs(ms - implied_spread) > max_div:
                    _log_divergence_block(conn, sp, eid, home, away, ms, implied_spread, 'ml_only_implied_spread')
                    return (True, picks, {'ms': ms, 'ms_inj': ms, 'ctx': None, '_h2h_ctx': ''})

    # CONTEXT ADJUSTMENTS — schedule, travel, altitude, splits.
    ctx = None
    if HAS_CONTEXT:
        from context_engine import get_context_adjustments
        try:
            ctx = get_context_adjustments(
                conn, sp, home, away, eid, commence, 'SPREAD')
            if ctx['spread_adj'] != 0:
                ms -= ctx['spread_adj']  # Positive adj = home advantage = ms more negative
        except Exception:
            pass

    # ═══ TENNIS H2H ADJUSTMENT ═══
    _h2h_ctx = ""
    if sp.startswith('tennis_'):
        try:
            _h2h_adj, _h2h_ctx = _tennis_h2h_adjustment(conn, home, away, sp)
            if _h2h_adj != 0:
                ms += _h2h_adj  # Already signed
        except Exception:
            pass

    # Injury spread adjustment (50% weight — market prices the rest).
    inj_diff = a_imp - h_imp
    inj_spread_adj = round(inj_diff * 0.5, 2)
    ms_inj = ms - inj_spread_adj if abs(inj_spread_adj) >= 0.5 else ms

    # v25.29: DIV_EXPANDED tracking (NHL only currently — div 1.5→2.5 on 2026-04-18).
    _DIV_EXPANDED_ORIG = {'icehockey_nhl': 1.5}
    _orig_div = _DIV_EXPANDED_ORIG.get(sp)
    _is_div_expanded = (
        _orig_div is not None and mkt_hs is not None
        and abs(ms - mkt_hs) > _orig_div
    )

    # HOME SPREAD
    if mkt_hs is not None and mkt_hs_odds is not None:
        k = f"{eid}|S|{home}"
        if k not in seen:
            wa = scottys_edge_assessment(ms_inj, mkt_hs, mkt_hs_odds, sp, hml, a_inj, a_cl)
            if wa['is_play'] and wa['point_value_pct'] >= min_pv:
                prob = spread_to_cover_prob(ms, mkt_hs, sp)
                if 'soccer' in sp:
                    draw_p = _soccer_draw_prob(abs(ms))
                    prob = prob * (1.0 - draw_p * 0.5)
                imp = american_to_implied_prob(mkt_hs_odds)
                pick = _mk(sp, eid, commence, home, away, 'SPREAD',
                    f"{home} {mkt_hs:+.1f}", mkt_hs_book, mkt_hs, mkt_hs_odds,
                    ms, prob, imp, wa, 'home_spread')
                if pick:
                    if 'soccer' in sp:
                        pick['units'] = kelly_units(edge_pct=wa['point_value_pct'], odds=mkt_hs_odds, fraction=0.333)
                    _ctx_parts = [ctx['summary']] if ctx and ctx['summary'] else []
                    if _h2h_ctx:
                        _ctx_parts.append(_h2h_ctx)
                    if sp == 'icehockey_nhl' and _nhl_goalie_info and _nhl_goalie_info.get('summary'):
                        _ctx_parts.append(f"GOALIE: {_nhl_goalie_info['summary']}")
                    if HAS_CONTEXT:
                        from context_engine import line_movement_signal
                        try:
                            _lm_move, _lm_sig, _lm_conf = line_movement_signal(
                                conn, eid, f"{home} {mkt_hs:+.1f}", 'spreads')
                            if _lm_sig == 'SHARP_AGREE':
                                _ctx_parts.append(f"Sharp money agrees ({_lm_move:+.1f})")
                            elif _lm_sig == 'PUBLIC_SIDE':
                                _ctx_parts.append(f"Public side ({_lm_move:+.1f})")
                        except Exception:
                            pass
                    if _ctx_parts:
                        pick['context'] = ' | '.join(_ctx_parts)
                        if ctx:
                            pick['context_adj'] = ctx['spread_adj']
                    if _is_div_expanded:
                        _div_val = abs(ms - mkt_hs)
                        _div_tag = f'DIV EXPANDED v25.29 — div {_div_val:.1f} (orig threshold {_orig_div})'
                        pick['context'] = f"{pick.get('context','')} | {_div_tag}".strip(' |')
                        pick['side_type'] = 'DIV_EXPANDED'
                        pick['units'] = min(pick.get('units', 3.5), 3.5)
                    seen.add(k)
                    picks.append(pick)

    # AWAY SPREAD
    if mkt_as is not None and mkt_as_odds is not None:
        k = f"{eid}|S|{away}"
        if k not in seen:
            wa = scottys_edge_assessment(-ms_inj, mkt_as, mkt_as_odds, sp, aml, h_inj, h_cl)
            if wa['is_play'] and wa['point_value_pct'] >= min_pv:
                prob = spread_to_cover_prob(-ms, mkt_as, sp)
                if 'soccer' in sp:
                    draw_p = _soccer_draw_prob(abs(ms))
                    prob = prob * (1.0 - draw_p * 0.5)
                imp = american_to_implied_prob(mkt_as_odds)
                pick = _mk(sp, eid, commence, home, away, 'SPREAD',
                    f"{away} {mkt_as:+.1f}", mkt_as_book, mkt_as, mkt_as_odds,
                    ms, prob, imp, wa, 'away_spread')
                if pick:
                    if 'soccer' in sp:
                        pick['units'] = kelly_units(edge_pct=wa['point_value_pct'], odds=mkt_as_odds, fraction=0.333)
                    _ctx_parts = [ctx['summary']] if ctx and ctx['summary'] else []
                    if _h2h_ctx:
                        _ctx_parts.append(_h2h_ctx)
                    if sp == 'icehockey_nhl' and _nhl_goalie_info and _nhl_goalie_info.get('summary'):
                        _ctx_parts.append(f"GOALIE: {_nhl_goalie_info['summary']}")
                    if HAS_CONTEXT:
                        from context_engine import line_movement_signal
                        try:
                            _lm_move, _lm_sig, _lm_conf = line_movement_signal(
                                conn, eid, f"{away} {mkt_as:+.1f}", 'spreads')
                            if _lm_sig == 'SHARP_AGREE':
                                _ctx_parts.append(f"Sharp money agrees ({_lm_move:+.1f})")
                            elif _lm_sig == 'PUBLIC_SIDE':
                                _ctx_parts.append(f"Public side ({_lm_move:+.1f})")
                        except Exception:
                            pass
                    if _ctx_parts:
                        pick['context'] = ' | '.join(_ctx_parts)
                        if ctx:
                            pick['context_adj'] = ctx['spread_adj']
                    if _is_div_expanded:
                        _div_val = abs(ms - mkt_hs)
                        _div_tag = f'DIV EXPANDED v25.29 — div {_div_val:.1f} (orig threshold {_orig_div})'
                        pick['context'] = f"{pick.get('context','')} | {_div_tag}".strip(' |')
                        pick['side_type'] = 'DIV_EXPANDED'
                        pick['units'] = min(pick.get('units', 3.5), 3.5)
                    seen.add(k)
                    picks.append(pick)

    return (False, picks, {'ms': ms, 'ms_inj': ms_inj, 'ctx': ctx, '_h2h_ctx': _h2h_ctx})


def process_ml_and_cross_market(conn, sp, prelude, adj, mkt, setup, ctx_state, seen):
    """ML evaluation, soccer draw, and cross-market picks for a single game.

    Runs (in order):
      1. Baseball ML — Elo win prob × pitcher-conditional adjustment, FAVORITES
         only when pitcher gap >= 1.5 runs. Min 15% edge (higher bar). Run
         lines DISABLED (`if False`) but block preserved.
      2. Walters ML evaluation (non-baseball) — Elo or spread-derived win
         probability, devig market odds, fire HOME/AWAY ML picks. Soccer ML
         disabled (v13: 0W-8L). Tennis ML capped to <= TENNIS_ML_CAP. Hard
         injury gate on star players out.
      3. DRAW pick (soccer 3-way market) — fire when implied draw probability
         beats market with >= min_pv edge.
      4. Cross-market edge — spread-implied ML vs actual ML. Soccer disabled
         (v21). Baseball disabled (v25.5 — opportunity cost crowds out OVER/
         UNDER alpha). Other sports: fire if cross_edge > 8% and stars >= 2.

    Mutates `seen` (adds ML, draw, cross-market keys when picks fire).

    Returns: list[dict] of picks. Always returned; caller extends all_picks.

    Extracted from model_engine.generate_predictions() lines 1514-1919 in
    v26.0 Phase 4-prep (chunk D).
    """
    from model_engine import (
        HAS_ELO, HAS_PITCHER, _mk, _mk_ml, _conf,
        _soccer_draw_prob,
        american_to_implied_prob, devig_ml_odds, soccer_ml_probs,
        spread_to_win_prob, get_star_rating, bet_timing_advice,
        kelly_units, get_pitcher_context,
    )

    eid = prelude['eid']
    commence = prelude['commence']
    home = prelude['home']
    away = prelude['away']
    _neutral = prelude['_neutral']

    h_imp = adj['h_imp']
    a_imp = adj['a_imp']

    mkt_hs = mkt['mkt_hs']; mkt_hs_odds = mkt['mkt_hs_odds']; mkt_hs_book = mkt['mkt_hs_book']
    mkt_as = mkt['mkt_as']; mkt_as_odds = mkt['mkt_as_odds']; mkt_as_book = mkt['mkt_as_book']
    hml = mkt['hml']; hml_book = mkt['hml_book']; aml = mkt['aml']; aml_book = mkt['aml_book']

    elo_data = setup['elo_data']
    min_pv = setup['min_pv']
    min_pv_ml = setup['min_pv_ml']

    ms = ctx_state['ms']
    ms_inj = ctx_state['ms_inj']
    ctx = ctx_state['ctx']
    _h2h_ctx = ctx_state['_h2h_ctx']

    picks = []

    # ═══ BASEBALL: Elo win probability + PITCHER-ADJUSTED ML ═══
    if 'baseball' in sp and hml is not None and aml is not None:
        if HAS_ELO and elo_data:
            from elo_engine import elo_win_probability
            home_prob = elo_win_probability(home, away, elo_data, sp, neutral_site=_neutral)
            if home_prob is not None:
                away_prob = 1.0 - home_prob
                h_fair, a_fair, _ = devig_ml_odds(hml, aml)

                if h_fair and a_fair:
                    h_edge = (home_prob - h_fair) * 100
                    a_edge = (away_prob - a_fair) * 100

                    # ── Run line evaluation (±1.5) — DISABLED v14 (1W-5L -60.8%) ──
                    WIN_BY_2_PCT = 0.785 if 'ncaa' in sp else 0.68
                    LOSE_BY_1_PCT = 0.215

                    if False and mkt_hs is not None and mkt_as is not None:
                        for rl_team, rl_line, rl_odds, rl_book, rl_win_prob, rl_side in [
                            (away, mkt_as, mkt_as_odds, mkt_as_book, away_prob, 'away'),
                            (home, mkt_hs, mkt_hs_odds, mkt_hs_book, home_prob, 'home'),
                        ]:
                            if rl_line is None or rl_odds is None:
                                continue
                            k_rl = f"{eid}|S|{rl_team}"
                            if k_rl in seen:
                                continue
                            if rl_line == -1.5:
                                cover_prob = rl_win_prob * WIN_BY_2_PCT
                            elif rl_line == 1.5:
                                cover_prob = rl_win_prob + (1 - rl_win_prob) * LOSE_BY_1_PCT
                            else:
                                continue
                            rl_imp = american_to_implied_prob(rl_odds)
                            rl_edge = (cover_prob - rl_imp) * 100 if rl_imp else 0
                            if rl_edge >= 10.0:
                                stars = get_star_rating(rl_edge)
                                if stars > 0:
                                    timing = 'EARLY' if rl_line < 0 else 'LATE'
                                    t_r = 'Favorite' if rl_line < 0 else 'Dog'
                                    seen.add(k_rl)
                                    pick = _mk(sp, eid, commence, home, away, 'SPREAD',
                                        f"{rl_team} {rl_line:+.1f}", rl_book, rl_line, rl_odds,
                                        ms, round(cover_prob, 4), round(rl_imp, 4),
                                        {'point_value_pct': round(rl_edge, 1), 'star_rating': stars,
                                         'units': kelly_units(edge_pct=rl_edge, odds=rl_odds),
                                         'is_play': True, 'timing': timing, 'timing_reason': t_r,
                                         'spread_or_ml': 'RUN_LINE', 'confidence': _conf(stars),
                                         'vig_adjusted_spread': rl_line, 'raw_spread_diff': 0,
                                         'injury_multiplier': 1.0, 'spread_or_ml_reason': 'Run line'},
                                        f'{rl_side}_spread')
                                    if pick:
                                        pick['notes'] = f"RL: P(cover)={cover_prob:.0%} vs imp={rl_imp:.0%}"
                                        if ctx and ctx['summary']:
                                            pick['context'] = ctx['summary']
                                        picks.append(pick)

                    # ── ML evaluation (v15: pitcher-conditional, FAVORITES only) ──
                    _bb_pitcher_ctx = None
                    _bb_ml_allowed = False
                    _bb_pitcher_adj = 0.0
                    BASEBALL_ML_MIN_EDGE = 15.0
                    BASEBALL_ML_PITCHER_GAP = 1.5
                    BASEBALL_ML_MAX_PROB_ADJ = 0.08

                    if HAS_PITCHER:
                        try:
                            _bb_pitcher_ctx = get_pitcher_context(conn, home, away, commence, sport=sp)
                        except Exception:
                            _bb_pitcher_ctx = None

                    if _bb_pitcher_ctx and _bb_pitcher_ctx['confidence'] != 'LOW':
                        h_pa = _bb_pitcher_ctx['home_pitching_adj']
                        a_pa = _bb_pitcher_ctx['away_pitching_adj']
                        _has_both = (h_pa != 0.0 or a_pa != 0.0)
                        pitcher_gap = abs(h_pa - a_pa)
                        if _has_both and pitcher_gap >= BASEBALL_ML_PITCHER_GAP:
                            _bb_ml_allowed = True
                            raw_adj = (a_pa - h_pa) * 0.03
                            _bb_pitcher_adj = max(-BASEBALL_ML_MAX_PROB_ADJ,
                                                 min(BASEBALL_ML_MAX_PROB_ADJ, raw_adj))

                    home_prob_adj = home_prob
                    away_prob_adj = away_prob
                    if _bb_ml_allowed and _bb_pitcher_adj != 0.0:
                        home_prob_adj = home_prob + _bb_pitcher_adj
                        away_prob_adj = 1.0 - home_prob_adj
                        home_prob_adj = max(0.05, min(0.95, home_prob_adj))
                        away_prob_adj = max(0.05, min(0.95, away_prob_adj))
                        h_edge = (home_prob_adj - h_fair) * 100
                        a_edge = (away_prob_adj - a_fair) * 100

                    # Home ML — FAVORITES only with pitcher confirmation
                    k_h = f"{eid}|M|{home}"
                    if (_bb_ml_allowed and k_h not in seen
                            and h_edge >= BASEBALL_ML_MIN_EDGE
                            and hml < 0):
                        stars = get_star_rating(h_edge)
                        if stars > 0:
                            timing, t_r = bet_timing_advice(ms, mkt_hs or 0)
                            seen.add(k_h)
                            _adj_prob = home_prob_adj if _bb_pitcher_adj != 0.0 else home_prob
                            pick = _mk_ml(sp, eid, commence, home, away,
                                f"{home} ML", hml_book, hml, ms,
                                round(_adj_prob, 4), round(h_fair, 4),
                                round(h_edge, 2), stars, timing, t_r)
                            if pick:
                                pick['notes'] += f" | PITCHER: {_bb_pitcher_ctx['summary']}"
                                if ctx and ctx['summary']:
                                    pick['context'] = ctx['summary']
                                picks.append(pick)

                    # Away ML — FAVORITES only
                    k_a = f"{eid}|M|{away}"
                    if (_bb_ml_allowed and k_a not in seen
                            and a_edge >= BASEBALL_ML_MIN_EDGE
                            and aml < 0):
                        stars = get_star_rating(a_edge)
                        if stars > 0:
                            timing, t_r = bet_timing_advice(-ms, mkt_as or 0)
                            seen.add(k_a)
                            _adj_prob = away_prob_adj if _bb_pitcher_adj != 0.0 else away_prob
                            pick = _mk_ml(sp, eid, commence, home, away,
                                f"{away} ML", aml_book, aml, ms,
                                round(_adj_prob, 4), round(a_fair, 4),
                                round(a_edge, 2), stars, timing, t_r)
                            if pick:
                                pick['notes'] += f" | PITCHER: {_bb_pitcher_ctx['summary']}"
                                if ctx and ctx['summary']:
                                    pick['context'] = ctx['summary']
                                picks.append(pick)
        # else baseball without Elo: skip generic ML

    # ═══ WALTERS ML EVALUATION (non-baseball) ═══
    elif hml is not None and aml is not None and 'baseball' not in sp:
        _walters_elo_w = 1.0
        if HAS_ELO and elo_data:
            from elo_engine import elo_win_probability
            _elo_p = elo_win_probability(home, away, elo_data, sp, neutral_site=_neutral)
            if _elo_p is not None:
                h_prob_ml, a_prob_ml = _elo_p, 1.0 - _elo_p
                h_d = elo_data.get(home, {})
                a_d = elo_data.get(away, {})
                _mgp = min(h_d.get('games', 0), a_d.get('games', 0))
                _walters_elo_w = min(1.0, _mgp / 15.0)
            else:
                h_prob_ml = spread_to_win_prob(ms_inj, sp)
                a_prob_ml = 1.0 - h_prob_ml
        else:
            h_prob_ml = spread_to_win_prob(ms_inj, sp)
            a_prob_ml = 1.0 - h_prob_ml

        from pipeline.score_helpers import apply_injury_to_prob
        h_prob_ml = apply_injury_to_prob(h_prob_ml, h_imp, a_imp)
        a_prob_ml = 1.0 - h_prob_ml

        _home_star_out = h_imp >= 5.0
        _away_star_out = a_imp >= 5.0

        if 'soccer' in sp:
            h_prob_ml, draw_prob_ml, a_prob_ml = soccer_ml_probs(ms, sp)
            draw_row = conn.execute("""
                SELECT odds FROM odds
                WHERE event_id=? AND market='h2h' AND selection='Draw'
                AND snapshot_date=(SELECT MAX(snapshot_date) FROM odds WHERE event_id=? AND market='h2h')
                ORDER BY odds DESC LIMIT 1
            """, (eid, eid)).fetchone()
            d_odds = draw_row[0] if draw_row else None
            h_imp_ml, a_imp_ml, _ = devig_ml_odds(hml, aml, d_odds)
        else:
            h_imp_ml, a_imp_ml, _ = devig_ml_odds(hml, aml)

        _tennis_ml_cap = None
        if sp.startswith('tennis_'):
            try:
                from config import TENNIS_ML_CAP
                _tennis_ml_cap = TENNIS_ML_CAP
            except ImportError:
                _tennis_ml_cap = 200

        # HOME ML
        k = f"{eid}|M|{home}"
        if k not in seen and h_imp_ml:
            if 'soccer' in sp:
                pass  # v13: ALL soccer ML disabled — backtest 0W-8L
            elif _tennis_ml_cap and abs(hml) > _tennis_ml_cap:
                pass
            elif _home_star_out:
                print(f"    ⚠ INJURY GATE: {home} ML blocked — star player out ({h_imp:.1f} pts impact)")
            else:
                h_edge_ml = (h_prob_ml - h_imp_ml) * 100 * _walters_elo_w
                stars = get_star_rating(h_edge_ml)
                if h_edge_ml >= min_pv_ml and stars > 0:
                    timing, t_r = bet_timing_advice(ms, mkt_hs or 0)
                    seen.add(k)
                    pick = _mk_ml(sp, eid, commence, home, away,
                        f"{home} ML", hml_book, hml, ms,
                        round(h_prob_ml, 4), round(h_imp_ml, 4),
                        round(h_edge_ml, 2), stars, timing, t_r)
                    if pick:
                        _ml_ctx_parts = []
                        if ctx and ctx['summary']:
                            _ml_ctx_parts.append(ctx['summary'])
                        if _h2h_ctx:
                            _ml_ctx_parts.append(_h2h_ctx)
                        _ml_ctx_parts.append('Elo probability edge')
                        if h_imp > 0 or a_imp > 0:
                            _ml_ctx_parts.append(f'Injuries: {home} -{h_imp:.1f}pts, {away} -{a_imp:.1f}pts')
                        pick['context'] = ' | '.join(_ml_ctx_parts)
                        picks.append(pick)

        # AWAY ML
        k = f"{eid}|M|{away}"
        if k not in seen and a_imp_ml:
            if 'soccer' in sp:
                pass
            elif _tennis_ml_cap and abs(aml) > _tennis_ml_cap:
                pass
            elif _away_star_out:
                print(f"    ⚠ INJURY GATE: {away} ML blocked — star player out ({a_imp:.1f} pts impact)")
            else:
                a_edge_ml = (a_prob_ml - a_imp_ml) * 100 * _walters_elo_w
                stars = get_star_rating(a_edge_ml)
                if a_edge_ml >= min_pv_ml and stars > 0:
                    timing, t_r = bet_timing_advice(-ms, mkt_as or 0)
                    seen.add(k)
                    pick = _mk_ml(sp, eid, commence, home, away,
                        f"{away} ML", aml_book, aml, ms,
                        round(a_prob_ml, 4), round(a_imp_ml, 4),
                        round(a_edge_ml, 2), stars, timing, t_r)
                    if pick:
                        _ml_ctx_parts = []
                        if ctx and ctx['summary']:
                            _ml_ctx_parts.append(ctx['summary'])
                        if _h2h_ctx:
                            _ml_ctx_parts.append(_h2h_ctx)
                        _ml_ctx_parts.append('Elo probability edge')
                        if h_imp > 0 or a_imp > 0:
                            _ml_ctx_parts.append(f'Injuries: {home} -{h_imp:.1f}pts, {away} -{a_imp:.1f}pts')
                        pick['context'] = ' | '.join(_ml_ctx_parts)
                        picks.append(pick)

    # DRAW (soccer only — 3-way market)
    if 'soccer' in sp:
        k = f"{eid}|M|DRAW"
        if k not in seen:
            draw_row = conn.execute("""
                SELECT odds, book FROM odds
                WHERE event_id=? AND market='h2h' AND selection='Draw'
                AND snapshot_date=(SELECT MAX(snapshot_date) FROM odds WHERE event_id=? AND market='h2h')
                ORDER BY odds DESC LIMIT 1
            """, (eid, eid)).fetchone()
            if draw_row:
                draw_odds, draw_book = draw_row
                draw_prob = _soccer_draw_prob(abs(ms))
                _, _, imp = devig_ml_odds(hml, aml, draw_odds)
                if imp is None:
                    imp = american_to_implied_prob(draw_odds)
                edge = (draw_prob - imp) * 100 if imp else 0
                edge = min(edge, 20.0)
                stars = get_star_rating(edge)
                if edge >= min_pv and stars > 0:
                    seen.add(k)
                    picks.append({
                        'sport': sp, 'event_id': eid, 'commence': commence,
                        'home': home, 'away': away, 'market_type': 'MONEYLINE',
                        'selection': f"DRAW ({home} vs {away})",
                        'book': draw_book, 'line': None, 'odds': draw_odds,
                        'model_spread': ms, 'model_prob': round(draw_prob, 4),
                        'implied_prob': round(imp, 4) if imp else None,
                        'edge_pct': round(edge, 2), 'star_rating': stars,
                        'units': kelly_units(edge_pct=edge, odds=draw_odds),
                        'confidence': _conf(stars),
                        'spread_or_ml': 'DRAW', 'timing': 'EARLY',
                        'notes': f"DrawProb={draw_prob:.1%} Imp={imp:.1%} Edge={edge:.1f}% "
                                 f"ModelSpread={ms:+.1f} | Close game → draw value",
                    })

    # ═══ CROSS-MARKET EDGE: Spread-implied ML vs actual ML ═══
    if hml is not None and mkt_hs is not None:
        if 'soccer' in sp:
            spread_win_prob, _, away_spread_prob = soccer_ml_probs(ms, sp)
        else:
            spread_win_prob = spread_to_win_prob(ms, sp)
            away_spread_prob = 1.0 - spread_win_prob
        ml_implied, aml_dv, _ = devig_ml_odds(hml, aml)
        if ml_implied is None:
            ml_implied = american_to_implied_prob(hml)
        if ml_implied and spread_win_prob and 'soccer' not in sp and 'baseball' not in sp:
            # v21: Soccer cross-market disabled. v25.5: Baseball disabled (opportunity cost).
            cross_edge = min((spread_win_prob - ml_implied) * 100, 20.0)
            if cross_edge > 8.0:
                k = f"{eid}|X|{home}"
                if k not in seen:
                    stars = get_star_rating(cross_edge)
                    if stars >= 2.0:
                        timing, t_r = bet_timing_advice(ms, mkt_hs or 0)
                        seen.add(k)
                        picks.append({
                            'sport': sp, 'event_id': eid, 'commence': commence,
                            'home': home, 'away': away, 'market_type': 'MONEYLINE',
                            'selection': f"{home} ML (cross-mkt)",
                            'book': hml_book, 'line': None, 'odds': hml,
                            'model_spread': ms, 'model_prob': round(spread_win_prob, 4),
                            'implied_prob': round(ml_implied, 4),
                            'edge_pct': round(cross_edge, 2), 'star_rating': stars,
                            'units': kelly_units(edge_pct=cross_edge, odds=hml), 'confidence': _conf(stars),
                            'spread_or_ml': 'CROSS_MKT', 'timing': timing,
                            'notes': f"Spread→{spread_win_prob:.1%} ML→{ml_implied:.1%} "
                                     f"Gap={cross_edge:.1f}% | Cross-market discrepancy",
                        })

            aml_implied = aml_dv
            if aml_implied is None and aml:
                _, aml_implied, _ = devig_ml_odds(hml, aml)
            if aml_implied and away_spread_prob:
                cross_edge_a = min((away_spread_prob - aml_implied) * 100, 20.0)
                if cross_edge_a > 8.0:
                    k = f"{eid}|X|{away}"
                    if k not in seen:
                        stars = get_star_rating(cross_edge_a)
                        if stars >= 2.0:
                            seen.add(k)
                            picks.append({
                                'sport': sp, 'event_id': eid, 'commence': commence,
                                'home': home, 'away': away, 'market_type': 'MONEYLINE',
                                'selection': f"{away} ML (cross-mkt)",
                                'book': aml_book, 'line': None, 'odds': aml,
                                'model_spread': ms, 'model_prob': round(away_spread_prob, 4),
                                'implied_prob': round(aml_implied, 4),
                                'edge_pct': round(cross_edge_a, 2), 'star_rating': stars,
                                'units': kelly_units(edge_pct=cross_edge_a, odds=aml), 'confidence': _conf(stars),
                                'spread_or_ml': 'CROSS_MKT', 'timing': 'EARLY',
                                'notes': f"Spread→{away_spread_prob:.1%} ML→{aml_implied:.1%} "
                                         f"Gap={cross_edge_a:.1f}% | Cross-market discrepancy",
                            })

    return picks


def process_totals_path(conn, sp, prelude, adj, mkt, totals_mkt, setup, ctx_state, seen):
    """All totals-related work for a single game.

    Runs (in order):
      1. CONTEXT_TOTAL_STANDALONE (v25.47/v25.65) — Phase A own-picks for
         NBA/NHL/MLB + per-direction soccer rules. Fires DATA_TOTAL pick at
         market line if Context disagrees by >= sport-specific threshold.
      2. MLS edge-based totals hard-block (v25.47).
      3. Edge-based totals — model_total estimation, injury/context/pitcher/
         park/bullpen/goalie adjustments stack onto raw_model_total.
      4. OVER pick — gates: park/pitching/era_reliability/ncaa_pitcher_data/
         ncaa_era_reliability/direction/nhl_pace.
      5. UNDER pick — gates: pace/park/pitching/direction/era_reliability/
         ncaa_*. plus the v25.4 NCAA UNDER filters (kept variable-name only,
         logic removed).

    Mutates `seen` (adds T|OVER, T|UNDER keys when picks fire).

    Returns: list[dict] of picks. Always returned; caller extends all_picks.

    Note: the MLB-only `continue` (skip game when model_total invalid) is
    converted to an early return — semantics preserved since totals is the
    last per-game work in the original loop.

    Extracted from model_engine.generate_predictions() lines 1521-2102 in
    v26.0 Phase 4-prep (chunk E — final per-game chunk).
    """
    from datetime import datetime as _dt
    from model_engine import (
        HAS_CONTEXT, HAS_PITCHER,
        _conf, _totals_confidence, _total_prob, _divergence_penalty,
        _mlb_pitcher_era_adjustment, _mlb_park_factor_adjustment,
        _mlb_bullpen_adjustment, _nhl_goalie_adjustment, _log_park_veto,
        american_to_implied_prob, get_pitcher_context,
        get_star_rating, kelly_units,
        estimate_model_total, calculate_point_value_totals,
    )

    eid = prelude['eid']
    commence = prelude['commence']
    home = prelude['home']
    away = prelude['away']
    _mlb_pitcher_info = prelude['_mlb_pitcher_info']
    _nhl_goalie_info = prelude['_nhl_goalie_info']

    h_imp = adj['h_imp']
    a_imp = adj['a_imp']

    ratings = setup['ratings']
    min_pv_totals = setup['min_pv_totals']

    ms = ctx_state['ms']

    over_total = totals_mkt['over_total']
    over_odds = totals_mkt['over_odds']
    over_book = totals_mkt['over_book']
    under_total = totals_mkt['under_total']
    under_odds = totals_mkt['under_odds']
    under_book = totals_mkt['under_book']

    picks = []

    # ═══ DATA_TOTAL Context Standalone (v26.0 Phase 4 → pipeline.channels.data_total) ═══
    from pipeline.channels.data_total import try_data_total
    picks.extend(try_data_total(conn, sp, prelude, totals_mkt))

    # MLS edge-based totals hard block (v25.47).
    if sp == 'soccer_usa_mls':
        over_total = None

    if over_total is not None and over_odds is not None and ms is not None:
        total_conf = _totals_confidence(home, away, sp, conn)
        if total_conf == 'LOW':
            return picks  # Skip — insufficient data
        model_total = estimate_model_total(home, away, ratings, sp, conn)
        if model_total is None:
            return picks
        _raw_model_total = model_total

        # Injury adjustment (NBA/NCAAB/NHL only).
        if sp in ('basketball_nba', 'basketball_ncaab', 'icehockey_nhl'):
            _inj_total_adj = (h_imp + a_imp) * 0.5
            if _inj_total_adj >= 0.5:
                model_total -= _inj_total_adj

        ctx_total = None
        if HAS_CONTEXT:
            from context_engine import get_context_adjustments
            ctx_total = get_context_adjustments(
                conn, sp, home, away, eid, commence, 'TOTAL')
            if ctx_total['total_adj'] != 0:
                model_total += ctx_total['total_adj']

        pitcher_ctx = None
        if HAS_PITCHER and 'baseball' in sp:
            try:
                pitcher_ctx = get_pitcher_context(conn, home, away, commence, sport=sp)
                if pitcher_ctx['total_adj'] != 0 and pitcher_ctx['confidence'] != 'LOW':
                    model_total += pitcher_ctx['total_adj']
            except Exception:
                pass

        # MLB starter ERA adjustment + asymmetric (best/worst) tracking.
        _pitcher_era_ctx = ''
        _era_adj = 0.0
        _best_era = None
        _worst_era = None
        _both_era_reliable = False
        _ncaa_pitcher_data_veto = False

        # NCAA ERA reliability gate (v25.32).
        _ncaa_era_reliability_veto = False
        if sp == 'baseball_ncaa' and pitcher_ctx:
            if (pitcher_ctx.get('home_starter') and pitcher_ctx.get('away_starter')
                and not pitcher_ctx.get('both_reliable', False)):
                _ncaa_era_reliability_veto = True
                try:
                    _h_ip = pitcher_ctx.get('home_starter_ip') or 0
                    _a_ip = pitcher_ctx.get('away_starter_ip') or 0
                    conn.execute("""
                        INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                            market_type, line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                    """, (_dt.now().isoformat(), sp, eid,
                          f"{away}@{home}", None,
                          f"NCAA_ERA_RELIABILITY_GATE ({pitcher_ctx['home_starter']} {_h_ip:.1f} IP, {pitcher_ctx['away_starter']} {_a_ip:.1f} IP; need >=15)"))
                    conn.commit()
                except Exception:
                    pass

        if sp == 'baseball_mlb' and _mlb_pitcher_info:
            try:
                _era_adj, _pitcher_era_ctx, _best_era, _worst_era, _both_era_reliable = _mlb_pitcher_era_adjustment(
                    conn, _mlb_pitcher_info)
                if _era_adj != 0:
                    model_total += _era_adj
                    _raw_model_total += _era_adj
            except Exception:
                pass

        # MLB park factor (gate only — not additive to model_total).
        _park_factor_ctx = ''
        _park_gate_adj = 0.0
        if sp == 'baseball_mlb':
            try:
                _, _park_factor_ctx, _park_gate_adj = _mlb_park_factor_adjustment(
                    conn, home, away_team=away)
            except Exception:
                pass

        # MLB bullpen.
        _bullpen_ctx = ''
        if sp == 'baseball_mlb':
            try:
                _bp_adj, _bullpen_ctx = _mlb_bullpen_adjustment(conn, home, away)
                if _bp_adj != 0:
                    model_total += _bp_adj
            except Exception:
                pass

        # NHL goalie GAA.
        _goalie_gaa_ctx = ''
        if sp == 'icehockey_nhl' and _nhl_goalie_info:
            try:
                _gaa_adj, _goalie_gaa_ctx = _nhl_goalie_adjustment(conn, _nhl_goalie_info)
                if _gaa_adj != 0:
                    model_total += _gaa_adj
            except Exception:
                pass

        # Weather + ref already inside ctx_total; locals stay zero (preserved
        # to keep "if ref_adj != 0 and ref_info" condition wired below).
        ref_adj = 0.0
        ref_info = ''

        totals_kelly_frac = 0.125 if total_conf == 'HIGH' else 0.0625

        if sp == 'baseball_mlb' and (not model_total or model_total <= 0):
            return picks  # was `continue` in original loop

        if sp == 'baseball_mlb':
            _mlb_skip_total = abs(model_total - over_total) < 0.5
            if ms is not None and abs(ms) < 0.5:
                _mlb_skip_total = True
                try:
                    conn.execute("""
                        INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                            market_type, line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                    """, (_dt.now().isoformat(), sp, eid,
                          f"{away}@{home} TOTAL {over_total}",
                          over_total,
                          f"MLB_SIDE_CONVICTION_GATE (|model_spread|={abs(ms):.2f} < 0.5, 6W-11L -28.4u historically)"))
                    conn.commit()
                except Exception:
                    pass
        elif sp == 'baseball_ncaa':
            _mlb_skip_total = abs(ms) < 0.5
            _ncaa_skip_under = abs(ms) < 0.5
        else:
            _mlb_skip_total = False
        if sp != 'baseball_ncaa':
            _ncaa_skip_under = False

        # OVER
        k = f"{eid}|T|OVER"
        _park_veto_over = (sp == 'baseball_mlb' and _park_gate_adj < -0.2)
        if _park_veto_over and _park_factor_ctx:
            _log_park_veto(conn, sp, eid, f"{away}@{home} OVER {over_total}",
                           _park_gate_adj, _park_factor_ctx)
        _pitching_veto_over = (sp in ('baseball_mlb', 'baseball_ncaa')
                               and (_era_adj <= -0.5
                                    or (_best_era is not None and _best_era < 3.50)))
        _era_reliability_veto = (sp == 'baseball_mlb' and not _both_era_reliable)
        if _era_reliability_veto:
            try:
                conn.execute("""
                    INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                        market_type, line, odds, edge_pct, units, reason)
                    VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                """, (_dt.now().isoformat(), sp, eid,
                      f"{away}@{home} OVER {over_total}", over_total,
                      f"ERA_RELIABILITY_GATE (missing reliable ERA for 1+ starters: {_pitcher_era_ctx})"))
                conn.commit()
            except Exception:
                pass
        _direction_veto_over = (('soccer' in sp or sp == 'baseball_mlb')
                                and _raw_model_total < over_total)
        _nhl_pace_veto_over = False
        if sp == 'icehockey_nhl' and ctx_total and ctx_total.get('summary', ''):
            if 'fast-paced' in ctx_total['summary'].lower():
                _nhl_pace_veto_over = True
                try:
                    conn.execute("""
                        INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                            market_type, line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                    """, (_dt.now().isoformat(), sp, eid,
                          f"{away}@{home} OVER {over_total}", over_total,
                          f"NHL_PACE_OVER_GATE (fast-paced context, 6W-7L -11.5u historically)"))
                    conn.commit()
                except Exception:
                    pass
        if k not in seen and not _mlb_skip_total and not _park_veto_over and not _pitching_veto_over and not _direction_veto_over and not _era_reliability_veto and not _ncaa_pitcher_data_veto and not _ncaa_era_reliability_veto and not _nhl_pace_veto_over:
            total_diff = model_total - over_total
            if total_diff > 0:
                pv = calculate_point_value_totals(model_total, over_total, sp)
                _t_div = _divergence_penalty(model_total, over_total, 'TOTAL')
                stars = get_star_rating(pv)
                if pv >= min_pv_totals and stars > 0:
                    prob = _total_prob(total_diff, sp)
                    imp = american_to_implied_prob(over_odds)
                    prob_edge = (prob - (imp or 0.524)) * 100.0
                    final_edge = max(pv, prob_edge)
                    if _t_div < 1.0:
                        final_edge *= _t_div
                    seen.add(k)
                    pick = {
                        'sport': sp, 'event_id': eid, 'commence': commence,
                        'home': home, 'away': away, 'market_type': 'TOTAL',
                        'selection': f"{away}@{home} OVER {over_total}",
                        'book': over_book, 'line': over_total, 'odds': over_odds,
                        'model_spread': ms, 'model_prob': round(prob, 4),
                        'implied_prob': round(imp, 4) if imp else None,
                        'edge_pct': round(pv, 2), 'star_rating': stars,
                        'units': kelly_units(edge_pct=final_edge, odds=over_odds, fraction=totals_kelly_frac),
                        'confidence': _conf(stars),
                        'spread_or_ml': 'TOTAL', 'timing': 'EARLY',
                        'notes': f"ModelTotal={model_total:.1f} Mkt={over_total} "
                                 f"Diff={total_diff:+.1f} PV={pv}% {stars}★ data={total_conf}",
                    }
                    if 'soccer' in sp:
                        pick['units'] = kelly_units(
                            edge_pct=final_edge, odds=over_odds, fraction=0.333)
                    ctx_parts = []
                    if ctx_total and ctx_total['summary']:
                        ctx_parts.append(ctx_total['summary'])
                    if pitcher_ctx and pitcher_ctx['summary']:
                        ctx_parts.append(pitcher_ctx['summary'])
                    if _pitcher_era_ctx:
                        ctx_parts.append(_pitcher_era_ctx)
                    if _park_factor_ctx:
                        ctx_parts.append(_park_factor_ctx)
                    if _bullpen_ctx:
                        ctx_parts.append(_bullpen_ctx)
                    if _goalie_gaa_ctx:
                        ctx_parts.append(_goalie_gaa_ctx)
                    if ref_adj != 0 and ref_info:
                        ctx_parts.append(ref_info)
                    if ctx_parts:
                        pick['context'] = ' | '.join(ctx_parts)
                    picks.append(pick)

        # UNDER
        _block_ncaa_under = False
        _pace_veto_under = False
        if sp == 'basketball_nba' and ctx_total and ctx_total.get('summary', ''):
            _ctx_summary = ctx_total['summary'].lower()
            if 'fast-paced' in _ctx_summary or 'altitude' in _ctx_summary:
                _pace_veto_under = True
                try:
                    conn.execute("""
                        INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                            market_type, line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, 'TOTAL', ?, ?, NULL, NULL, ?)
                    """, (_dt.now().isoformat(), sp, eid,
                          f"{away}@{home} UNDER {under_total}",
                          under_total, under_odds,
                          f"PACE_GATE ({ctx_total['summary'][:80]})"))
                    conn.commit()
                except Exception:
                    pass

        _park_veto_under = (sp == 'baseball_mlb' and _park_gate_adj > 0.2)
        if _park_veto_under and _park_factor_ctx:
            _log_park_veto(conn, sp, eid, f"{away}@{home} UNDER {under_total}",
                           _park_gate_adj, _park_factor_ctx)
        _pitching_veto_under = (sp in ('baseball_mlb', 'baseball_ncaa')
                                and (_era_adj >= 0.5
                                     or (_worst_era is not None and _worst_era > 5.50)))
        _direction_veto_under = (('soccer' in sp or sp == 'baseball_mlb')
                                 and _raw_model_total > (under_total or 0))
        if _era_reliability_veto and under_total is not None:
            try:
                conn.execute("""
                    INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                        market_type, line, odds, edge_pct, units, reason)
                    VALUES (?, ?, ?, ?, 'TOTAL', ?, NULL, NULL, NULL, ?)
                """, (_dt.now().isoformat(), sp, eid,
                      f"{away}@{home} UNDER {under_total}", under_total,
                      f"ERA_RELIABILITY_GATE (missing reliable ERA for 1+ starters: {_pitcher_era_ctx})"))
                conn.commit()
            except Exception:
                pass
        if under_total is not None and under_odds is not None and not _mlb_skip_total and not _ncaa_skip_under and not _block_ncaa_under and not _park_veto_under and not _pace_veto_under and not _pitching_veto_under and not _direction_veto_under and not _era_reliability_veto and not _ncaa_pitcher_data_veto and not _ncaa_era_reliability_veto:
            k = f"{eid}|T|UNDER"
            if k not in seen:
                total_diff_u = under_total - model_total
                if total_diff_u > 0:
                    pv = calculate_point_value_totals(model_total, under_total, sp)
                    _t_div_u = _divergence_penalty(model_total, under_total, 'TOTAL')
                    stars = get_star_rating(pv)
                    if pv >= min_pv_totals and stars > 0:
                        prob = _total_prob(total_diff_u, sp)
                        imp = american_to_implied_prob(under_odds)
                        prob_edge = (prob - (imp or 0.524)) * 100.0
                        final_edge_u = max(pv, prob_edge)
                        if _t_div_u < 1.0:
                            final_edge_u *= _t_div_u
                        seen.add(k)
                        pick = {
                            'sport': sp, 'event_id': eid, 'commence': commence,
                            'home': home, 'away': away, 'market_type': 'TOTAL',
                            'selection': f"{away}@{home} UNDER {under_total}",
                            'book': under_book, 'line': under_total, 'odds': under_odds,
                            'model_spread': ms, 'model_prob': round(prob, 4),
                            'implied_prob': round(imp, 4) if imp else None,
                            'edge_pct': round(pv, 2), 'star_rating': stars,
                            'units': kelly_units(edge_pct=final_edge_u, odds=under_odds, fraction=totals_kelly_frac),
                            'confidence': _conf(stars),
                            'spread_or_ml': 'TOTAL', 'timing': 'EARLY',
                            'notes': f"ModelTotal={model_total:.1f} Mkt={under_total} "
                                     f"Diff={total_diff_u:+.1f} PV={pv}% {stars}★ data={total_conf}",
                        }
                        if 'soccer' in sp:
                            pick['units'] = kelly_units(
                                edge_pct=final_edge_u, odds=under_odds, fraction=0.333)
                        ctx_parts = []
                        if ctx_total and ctx_total['summary']:
                            ctx_parts.append(ctx_total['summary'])
                        if pitcher_ctx and pitcher_ctx['summary']:
                            ctx_parts.append(pitcher_ctx['summary'])
                        if _pitcher_era_ctx:
                            ctx_parts.append(_pitcher_era_ctx)
                        if _park_factor_ctx:
                            ctx_parts.append(_park_factor_ctx)
                        if _bullpen_ctx:
                            ctx_parts.append(_bullpen_ctx)
                        if _goalie_gaa_ctx:
                            ctx_parts.append(_goalie_gaa_ctx)
                        if ref_adj != 0 and ref_info:
                            ctx_parts.append(ref_info)
                        if ctx_parts:
                            pick['context'] = ' | '.join(ctx_parts)
                        picks.append(pick)

    return picks



def score_one_game(conn, sp, g, now_utc, setup, seen):
    """Run all 5 per-game pipeline stages for a single game tuple.

    Composes:
      score_game_prelude
      fetch_game_adjustments
      handle_divergence_path  (early-return + skip_div bump)
      process_spread_path     (early-return + skip_div bump)
      process_ml_and_cross_market
      process_totals_path

    Args:
        conn:       open sqlite3.Connection
        sp:         sport key
        g:          raw game tuple from setup['games']
        now_utc:    timezone-aware datetime in UTC
        setup:      dict from pipeline.stage_1_fetch.load_sport_setup
        seen:       mutable set of dedup keys (mutated by called functions)

    Returns: (picks, skip_nr_delta, skip_div_delta) where:
        picks: list[dict] — all picks generated for this game
        skip_nr_delta: 0 or 1 — bump for caller's skip_nr counter
        skip_div_delta: 0 or 1 — bump for caller's skip_div counter
    """
    picks = []

    # Chunk A: prelude (game-time + ms + Elo blend + MLB/NHL gates)
    proceed, prelude, skip_nr_delta = score_game_prelude(conn, sp, g, now_utc, setup)
    if not proceed:
        return (picks, skip_nr_delta, 0)

    # Pull market vars from raw game tuple
    mkt = {
        'mkt_hs': g[4], 'mkt_hs_odds': g[5], 'mkt_hs_book': g[6],
        'mkt_as': g[7], 'mkt_as_odds': g[8], 'mkt_as_book': g[9],
        'hml': g[16], 'hml_book': g[17], 'aml': g[18], 'aml_book': g[19],
    }

    # Chunk B: injury+ref fetch + divergence path (Elo ML rescue + DATA_SPREAD + SPREAD_FADE_FLIP)
    adj = fetch_game_adjustments(conn, sp, prelude['home'], prelude['away'])
    div_consumed, div_picks = handle_divergence_path(
        conn, sp, prelude, adj, mkt, setup, seen)
    if div_consumed:
        picks.extend(div_picks)
        return (picks, skip_nr_delta, 1)

    # max_div is needed by Chunk C's ml-only divergence check + DATA_SPREAD context string
    max_div = setup['cfg']['max_spread_divergence']

    # Chunk C: non-divergent spread path
    sp_consumed, sp_picks, ctx_state = process_spread_path(
        conn, sp, prelude, adj, mkt, setup, seen, max_div)
    picks.extend(sp_picks)
    if sp_consumed:
        return (picks, skip_nr_delta, 1)

    # Chunk D: ML + soccer draw + cross-market
    picks.extend(process_ml_and_cross_market(
        conn, sp, prelude, adj, mkt, setup, ctx_state, seen))

    # Chunk E: totals
    totals_mkt = {
        'over_total': g[10], 'over_odds': g[11], 'over_book': g[12],
        'under_total': g[13], 'under_odds': g[14], 'under_book': g[15],
    }
    picks.extend(process_totals_path(
        conn, sp, prelude, adj, mkt, totals_mkt, setup, ctx_state, seen))

    return (picks, skip_nr_delta, 0)
