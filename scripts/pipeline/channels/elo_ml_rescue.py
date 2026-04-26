"""
Elo ML rescue channel — divergent-spread fallback.

When the model spread diverges from market by > max_spread_divergence, the
spread market is too noisy for an edge play. But the win probability from Elo
can still find ML value. This channel:

  1. Checks Elo seasoning floor (< 7 games for tennis, < 10 otherwise) and
     reports the seasoning failure to the caller (which logs the divergence
     block and short-circuits the rest of the divergence path).
  2. Computes confidence-weighted Elo win probability with SOS dampening,
     mismatch dampening, and injury-adjusted probability shift.
  3. Fires HOME and/or AWAY ML picks above the sport's `min_pv_ml` floor
     (or `min_pv` during NCAAB tournament window when Elo compresses extreme
     mismatches).
  4. Hard-blocks ML on either side when the picked team has a star out
     (5.0+ pts injury impact = MVP-caliber player missing).

Extracted from `pipeline/per_game.handle_divergence_path` in v26.0 Phase 4.
"""
from datetime import datetime as _dt


def try_elo_ml_rescue(conn, sp, prelude, adj, mkt, setup, seen):
    """Try to fire ML picks on a divergent game using Elo win probability.

    Args:
        conn, sp: DB connection + sport key.
        prelude: dict from score_game_prelude (eid, commence, home, away, ms,
                 _neutral).
        adj: dict from fetch_game_adjustments (h_imp, a_imp, plus other
             injury fields not used here).
        mkt: dict with mkt_hs, mkt_as, hml/_book, aml/_book.
        setup: dict from load_sport_setup (elo_data, cfg, min_pv, min_pv_ml).
        seen: mutable set of f"{eid}|M|{team}" keys for ML dedup.

    Returns (picks, seasoning_failed):
        picks (list[dict]): ML picks generated. May be empty.
        seasoning_failed (bool): True → caller should log divergence block as
                                 'insufficient_elo_games' and return early.
                                 False → caller continues with DATA_SPREAD /
                                 SPREAD_FADE_FLIP channels.

    No-op (returns `([], False)`) when hml/aml/HAS_ELO/elo_data are missing.
    """
    from model_engine import (
        HAS_ELO, _mk_ml,
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

    mkt_hs = mkt['mkt_hs']; mkt_hs_book = mkt['mkt_hs_book']
    mkt_as = mkt['mkt_as']
    hml = mkt['hml']; hml_book = mkt['hml_book']; aml = mkt['aml']; aml_book = mkt['aml_book']

    elo_data = setup['elo_data']
    cfg = setup['cfg']
    min_pv = setup['min_pv']
    min_pv_ml = setup['min_pv_ml']

    picks = []

    if not (hml is not None and aml is not None and HAS_ELO and elo_data):
        return (picks, False)

    _is_tourney = (sp == 'basketball_ncaab'
        and (_dt.now().month == 3 or (_dt.now().month == 4 and _dt.now().day <= 7)))
    h_data = elo_data.get(home, {})
    a_data = elo_data.get(away, {})
    _min_gp = min(h_data.get('games', 0), a_data.get('games', 0))
    # v25.85: tennis seasoning lowered to 7 (clay backfill seeds 2023-2024
    # but 2025 debutants still sit below 10).
    _seasoning_min = 7 if sp.startswith('tennis_') else 10
    if _min_gp < _seasoning_min:
        return (picks, True)  # caller logs divergence block + returns

    _sport_min = cfg.get('min_games_elo', 15)
    from pipeline.score_helpers import (
        compute_elo_confidence_weight, compute_sos_dampening,
        apply_injury_to_prob, compute_mismatch_dampening,
    )
    _conf_w = compute_elo_confidence_weight(_min_gp, _sport_min)
    _sos_w = compute_sos_dampening(home, away, sp, conn)

    from elo_engine import elo_win_probability
    home_prob = elo_win_probability(home, away, elo_data, sp, neutral_site=_neutral)
    if home_prob is None:
        return (picks, False)

    away_prob = 1.0 - home_prob

    home_prob = apply_injury_to_prob(home_prob, h_imp, a_imp)
    away_prob = 1.0 - home_prob

    # Hard gate: block ML pick if PICKED team has star out (5.0+ pts impact)
    _home_star_out = h_imp >= 5.0
    _away_star_out = a_imp >= 5.0

    h_fair, a_fair, _ = devig_ml_odds(hml, aml)
    if not (h_fair and a_fair):
        return (picks, False)

    _mismatch_w = compute_mismatch_dampening(h_fair, a_fair)
    _elo_w = _conf_w * _mismatch_w * _sos_w
    _min = min_pv if _is_tourney else min_pv_ml

    # Home ML
    h_edge = (home_prob - h_fair) * 100 * _elo_w
    k_h = f"{eid}|M|{home}"
    if k_h not in seen and h_edge >= _min and not _home_star_out:
        stars = get_star_rating(h_edge)
        if stars > 0:
            timing, t_r = bet_timing_advice(ms, mkt_hs or 0)
            seen.add(k_h)
            pick = _mk_ml(sp, eid, commence, home, away,
                f"{home} ML", hml_book, hml, ms,
                round(home_prob, 4), round(h_fair, 4),
                round(h_edge, 2), stars, timing, t_r)
            if pick:
                pick['context'] = 'Elo probability edge'
                if h_imp > 0 or a_imp > 0:
                    pick['context'] += f' | Injuries: {home} -{h_imp:.1f}pts, {away} -{a_imp:.1f}pts'
                picks.append(pick)
    elif _home_star_out and h_edge >= _min:
        print(f"    ⚠ INJURY GATE: {home} ML blocked — star player out ({h_imp:.1f} pts impact)")

    # Away ML
    a_edge = (away_prob - a_fair) * 100 * _elo_w
    k_a = f"{eid}|M|{away}"
    if k_a not in seen and a_edge >= _min and not _away_star_out:
        stars = get_star_rating(a_edge)
        if stars > 0:
            timing, t_r = bet_timing_advice(-ms, mkt_as or 0)
            seen.add(k_a)
            pick = _mk_ml(sp, eid, commence, home, away,
                f"{away} ML", aml_book, aml, ms,
                round(away_prob, 4), round(a_fair, 4),
                round(a_edge, 2), stars, timing, t_r)
            if pick:
                pick['context'] = 'Elo probability edge'
                if h_imp > 0 or a_imp > 0:
                    pick['context'] += f' | Injuries: {home} -{h_imp:.1f}pts, {away} -{a_imp:.1f}pts'
                picks.append(pick)
    elif _away_star_out and a_edge >= _min:
        print(f"    ⚠ INJURY GATE: {away} ML blocked — star player out ({a_imp:.1f} pts impact)")

    return (picks, False)
