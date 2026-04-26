"""
SPREAD_FADE_FLIP channel — fade the model when Elo diverges (NBA + NHL).

When the Elo-derived model spread diverges from market by > max_spread_div,
the model is wrong ~70% of the time on these games. Fade it: bet the
opposite side at the market line. 5u stake.

Backtest (14d on DIVERGENCE_GATE-blocked spread picks):
    NBA: 38-18 (67.9%), +82.73u
    NHL: 17-4 (81.0%), +57.27u

v25.60 dual-model agreement check:
    Fade-flip's thesis is "Elo alone is wrong when divergent." If Context
    Model also disagrees with market in the SAME direction as Elo, both
    brains agree — fading is betting against our strongest conviction
    signal (65% WR cohort). 42-game sample of dual-agree fades: 13-29
    (31% WR), -86u. Veto when Context aligns with Elo.

Extracted from `pipeline/per_game.handle_divergence_path` in v26.0 Phase 4.
"""
from datetime import datetime as _dt


def try_spread_fade_flip(conn, sp, prelude, mkt, context_fired):
    """Try to fire a SPREAD_FADE_FLIP pick on a divergent NBA/NHL game.

    Args:
        conn, sp: DB connection + sport key.
        prelude: dict from score_game_prelude (eid, commence, home, away, ms).
        mkt: dict with mkt_hs, mkt_hs_odds, mkt_hs_book, mkt_as, mkt_as_odds,
             mkt_as_book.
        context_fired (bool): True when DATA_SPREAD already produced a pick
                              for this event. Fade flip skips entirely so the
                              same event isn't double-played.

    Returns (picks, vetoed):
        picks (list[dict]): the fade-flip pick if it fires (max 1).
        vetoed (bool): True → caller should log divergence block as
                       'post_elo_rescue' and return early (Context agrees with
                       Elo direction; fading would bet against high-conviction
                       signal). False → continue normally.

    No-op (returns `([], False)`) outside NBA/NHL or when Context already fired.
    """
    eid = prelude['eid']
    commence = prelude['commence']
    home = prelude['home']
    away = prelude['away']
    ms = prelude['ms']

    mkt_hs = mkt['mkt_hs']; mkt_hs_odds = mkt['mkt_hs_odds']; mkt_hs_book = mkt['mkt_hs_book']
    mkt_as = mkt['mkt_as']; mkt_as_odds = mkt['mkt_as_odds']; mkt_as_book = mkt['mkt_as_book']

    picks = []

    if (context_fired
            or sp not in ('basketball_nba', 'icehockey_nhl')
            or mkt_hs is None or mkt_as is None
            or mkt_hs_odds is None or mkt_as_odds is None):
        return (picks, False)

    try:
        # v25.60 dual-model agreement check.
        _ff_context_vetoes = False
        try:
            from context_spread_model import compute_context_spread
            _ff_commence_date = (commence[:10] if commence else None)
            ms_ctx_ff, _ = compute_context_spread(
                conn, sp, home, away, eid, ms, _ff_commence_date)
            _elo_more_bullish_home = (ms < mkt_hs)
            _ctx_more_bullish_home = (ms_ctx_ff < mkt_hs)
            if _elo_more_bullish_home == _ctx_more_bullish_home:
                _ff_context_vetoes = True
                _ctx_dir = 'home' if _ctx_more_bullish_home else 'away'
                _elo_dir = 'home' if _elo_more_bullish_home else 'away'
                print(f"    \U0001f9e0 SPREAD_FADE_FLIP vetoed: Context ({ms_ctx_ff:+.1f}) "
                      f"agrees with Elo ({ms:+.1f}) on favoring {_ctx_dir} vs market {mkt_hs:+.1f}")
                try:
                    conn.execute("""INSERT INTO shadow_blocked_picks
                        (created_at, sport, event_id, selection, market_type, book,
                         line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, 'SPREAD', ?, ?, ?, ?, ?, ?)""",
                        (_dt.now().isoformat(), sp, eid,
                         f"{away}@{home} (fade flip blocked)", mkt_hs_book,
                         mkt_hs, mkt_hs_odds, 0, 5.0,
                         f'SPREAD_FADE_FLIP_DUAL_MODEL_VETO (v25.60 — Elo ms={ms:+.1f}, '
                         f'Context ms_ctx={ms_ctx_ff:+.1f}, market={mkt_hs:+.1f}; both favor {_ctx_dir})'))
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass  # Context check failure → default to firing (existing behavior)

        if _ff_context_vetoes:
            return (picks, True)  # caller logs divergence block + returns

        # ms < mkt_hs → model more bullish on home → fade to AWAY
        # ms > mkt_hs → model less bullish on home → fade to HOME
        if ms > mkt_hs:
            _f_team, _f_line, _f_odds, _f_book = home, mkt_hs, mkt_hs_odds, mkt_hs_book
        else:
            _f_team, _f_line, _f_odds, _f_book = away, mkt_as, mkt_as_odds, mkt_as_book
        from config import MIN_ODDS as _FF_MIN_ODDS
        if (_f_odds is not None and _f_odds > _FF_MIN_ODDS
                and _f_odds <= 140 and _f_book):
            _fade_div = abs(ms - mkt_hs)
            _fade_ctx = (
                f'SPREAD_FADE_FLIP v25.36 — model ms={ms:+.1f} vs market '
                f'{mkt_hs:+.1f} (div={_fade_div:.1f}). Fading model → '
                f'bet {_f_team} {_f_line:+.1f} at {_f_book} {_f_odds:+.0f}.'
            )
            _fade_pick = {
                'sport': sp, 'event_id': eid, 'commence': commence,
                'home': home, 'away': away,
                'market_type': 'SPREAD',
                'selection': f'{_f_team} {_f_line:+.1f}',
                'book': _f_book, 'line': _f_line, 'odds': _f_odds,
                'model_spread': ms,
                'model_prob': 0, 'implied_prob': 0,
                'edge_pct': 0,
                'star_rating': 3, 'units': 5.0,
                'confidence': 'FADE_FLIP',
                'side_type': 'SPREAD_FADE_FLIP',
                'spread_or_ml': 'SPREAD',
                'timing': 'STANDARD',
                'context': _fade_ctx,
                'notes': _fade_ctx,
            }
            print(f"  \U0001f504 SPREAD_FADE_FLIP: {sp.split('_')[-1]} {_f_team} "
                  f"{_f_line:+.1f} @ {_f_book} {_f_odds:+.0f} (div {_fade_div:.1f})")
            picks.append(_fade_pick)
    except Exception as _e:
        print(f"  ⚠ SPREAD_FADE_FLIP error: {_e}")

    return (picks, False)
