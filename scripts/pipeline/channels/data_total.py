"""
DATA_TOTAL Context Standalone channel — totals own-picks (v25.47/v25.65).

Runs Context Model on TOTALS for in-scope sports and fires an own-pick at the
market line when Context disagrees with market by >= sport-specific threshold.

Phase A backtest with goalie-form, soccer-standings, ref-tendency signals
layered on form + H2H + MLB-pitcher:
    NBA (>=0.30 pts): 173 picks, 58.7%, +97.4u
    NHL (>=1.00 gls):  95 picks, 60.6%, +52.0u
    MLB (>=1.50 run):  68 picks, 56.9%, +20.9u
    MLS (>=0.30 gls):  15 picks, 66.7%, +14.7u

v25.65 soccer rules (per-sport × direction): inverse backtest (90d, n=133)
showed Context FOLLOW wins overall (+101u) but two specific cohorts invert
(EPL UNDER, MLS UNDER → fade wins). Soccer OVER had only n=5 across all 7
leagues — insufficient for live firing, so OVER is shadow-only everywhere.

Extracted from `pipeline/per_game.process_totals_path` in v26.0 Phase 4.
"""
from datetime import datetime as _dt


CONTEXT_TOTAL_STANDALONE_THRESHOLDS_V47 = {
    'basketball_nba': 0.30,
    'icehockey_nhl':  1.00,
    'baseball_mlb':   1.50,
}

CONTEXT_TOTAL_STANDALONE_SOCCER_RULES = {
    'soccer_italy_serie_a':      {'UNDER': 0.30, 'OVER': 'shadow'},
    'soccer_france_ligue_one':   {'UNDER': 0.50, 'OVER': 'shadow'},
    'soccer_germany_bundesliga': {'UNDER': 'shadow', 'OVER': 'shadow'},
    'soccer_usa_mls':            {'UNDER': 'block', 'OVER': 'shadow'},
    'soccer_epl':                {'UNDER': 'block', 'OVER': 'shadow'},
    'soccer_spain_la_liga':      {'UNDER': 'shadow', 'OVER': 'shadow'},
    'soccer_uefa_champs_league': {'UNDER': 'shadow', 'OVER': 'shadow'},
}


def _log_context_shadow(conn, sport_, eid_, sel_, line_, direction_, gap_, reason_tag):
    """Log a Context candidate to shadow_blocked_picks for backtest accumulation."""
    try:
        conn.execute("""INSERT INTO shadow_blocked_picks
            (created_at, sport, event_id, selection, market_type, book,
             line, odds, edge_pct, units, reason)
            VALUES (?, ?, ?, ?, 'TOTAL', '', ?, ?, 0, 0, ?)""",
            (_dt.now().isoformat(), sport_, eid_, sel_, line_, 0,
             f'CONTEXT_TOTAL_P2_{reason_tag} (v25.65 — direction={direction_}, gap={gap_:+.2f})'))
        conn.commit()
    except Exception:
        pass


def try_data_total(conn, sp, prelude, totals_mkt):
    """Try to fire a DATA_TOTAL Context-Standalone pick.

    Args:
        conn, sp: DB connection + sport key.
        prelude: dict from score_game_prelude (eid, commence, home, away).
        totals_mkt: dict with over_total, over_odds, over_book, under_total,
                    under_odds, under_book.

    Returns: list[dict] of picks (max 1, may be empty).

    No-op (returns `[]`) when the sport is out of scope, or markets are
    missing, or Context disagreement is below threshold.
    """
    eid = prelude['eid']
    commence = prelude['commence']
    home = prelude['home']
    away = prelude['away']

    over_total = totals_mkt['over_total']
    over_odds = totals_mkt['over_odds']
    over_book = totals_mkt['over_book']
    under_total = totals_mkt['under_total']
    under_odds = totals_mkt['under_odds']
    under_book = totals_mkt['under_book']

    picks = []

    _ct_th = None
    _soccer_rules = CONTEXT_TOTAL_STANDALONE_SOCCER_RULES.get(sp)
    if _soccer_rules is None:
        _ct_th = CONTEXT_TOTAL_STANDALONE_THRESHOLDS_V47.get(sp)

    if not ((_ct_th is not None or _soccer_rules is not None)
            and over_total is not None and over_odds is not None
            and under_total is not None and under_odds is not None):
        return picks

    try:
        from context_spread_model import (
            compute_context_total, format_context_total_summary,
        )
        _ct_commence = (commence[:10] if commence else None)
        _market_total = over_total
        ctx_tot, ct_info = compute_context_total(
            conn, sp, home, away, eid, _market_total, _ct_commence)
        _ct_disagreement = ctx_tot - _market_total
        _ct_side = 'OVER' if _ct_disagreement > 0 else 'UNDER'

        # v25.65: per-direction rules for soccer
        if _soccer_rules is not None:
            _rule = _soccer_rules.get(_ct_side)
            _sel_label = f'{away}@{home} {_ct_side} {over_total if _ct_side == "OVER" else under_total}'
            if _rule == 'block':
                _log_context_shadow(conn, sp, eid, _sel_label,
                    over_total if _ct_side == 'OVER' else under_total,
                    _ct_side, _ct_disagreement, 'BLOCKED_FADE_COHORT')
                _ct_th = None
            elif _rule == 'shadow':
                _log_context_shadow(conn, sp, eid, _sel_label,
                    over_total if _ct_side == 'OVER' else under_total,
                    _ct_side, _ct_disagreement, 'SHADOW_INSUFFICIENT_SAMPLE')
                _ct_th = None
            elif isinstance(_rule, (int, float)):
                _ct_th = _rule
            else:
                _ct_th = None

        if _ct_th is not None and abs(_ct_disagreement) >= _ct_th:
            if _ct_disagreement > 0:
                _ct_side, _ct_line, _ct_odds, _ct_book = 'OVER', over_total, over_odds, over_book
            else:
                _ct_side, _ct_line, _ct_odds, _ct_book = 'UNDER', under_total, under_odds, under_book
            from config import MIN_ODDS as _CT_MIN_ODDS
            if (_ct_odds is not None and _ct_odds > _CT_MIN_ODDS
                    and _ct_odds <= 140 and _ct_book):
                _ct_summary = format_context_total_summary(ct_info)
                _ct_ctx = (
                    f'DATA_TOTAL v25.47 (Path 2) — {_ct_summary} | '
                    f'Market {_market_total}, Context {ctx_tot} '
                    f'(disagreement={_ct_disagreement:+.2f} ≥ {_ct_th}). '
                    f'Bet {_ct_side} {_ct_line} @ {_ct_book} {_ct_odds:+.0f}.'
                )
                _ct_pick = {
                    'sport': sp, 'event_id': eid, 'commence': commence,
                    'home': home, 'away': away,
                    'market_type': 'TOTAL',
                    'selection': f"{away}@{home} {_ct_side} {_ct_line}",
                    'book': _ct_book, 'line': _ct_line, 'odds': _ct_odds,
                    'model_spread': None, 'model_total': ctx_tot,
                    'model_prob': 0, 'implied_prob': 0,
                    'edge_pct': 0,
                    'star_rating': 3, 'units': 5.0,
                    'confidence': 'DATA_TOTAL',
                    'side_type': 'DATA_TOTAL',
                    'spread_or_ml': 'TOTAL',
                    'timing': 'STANDARD',
                    'context': _ct_ctx,
                    'notes': _ct_ctx,
                }
                print(f"  \U0001f9e0 DATA_TOTAL Path2: {sp.split('_')[-1]} {_ct_side} "
                      f"{_ct_line} @ {_ct_book} {_ct_odds:+.0f} "
                      f"(disagreement {_ct_disagreement:+.2f})")
                picks.append(_ct_pick)
    except Exception as _cte:
        print(f"  ⚠ DATA_TOTAL Path2 error: {_cte}")

    return picks
