"""
MLB_ML_FADE_FLIP channel — fade Elo-based ML edge in [8%, 12%] bucket (MLB only).

Backtest 2026-04-01 to 2026-04-28 (366 completed MLB games):
    edge >=  5%: 49W-52L (49%)  -49.05u  ← below floor: vig dominates
    edge >=  8%: 34W-20L (63%)  +38.70u  ← floor
    edge >= 10%: 22W-16L (58%)   +5.72u
    edge >= 12%: 15W- 6L (71%)  +29.00u
    edge >= 15%:  3W- 4L (43%)   -8.93u  ← cap: model captures real signal here

The 8-12% band is the empirical sweet spot. Above 12% the model picks up
real signal at extreme conviction; below 8% vig dominates the small
miscalibration. Most fade picks route to the favorite, where the
correctly-priced market beats the model's over-confident dog edges.

Methodology: model_spread (Elo + pitcher) → home_win_prob via logistic
(scale=4). Devig market ML → fair prob. Edge = model_prob − fair_prob.
Fades to the OPPOSITE side at the best NY-legal ML in [-150, +140].

Sport scope: baseball_mlb only. NCAA baseball excluded — different
pricing regime + the existing FOLLOW MLs (NCAAB FAVORITE 2W +8.35u)
already work, no fade needed.

Companion long-term project: v25.99 MLB Context ML model — purpose-built
ML predictor with pitcher quality + bullpen + park + lineup. When live,
fade flip will become a stake-boost confluence signal rather than a
fire-time channel.
"""
import math


SCALE = 4.0           # MLB run-spread → win prob logistic scale (validated by backtest sweep)
EDGE_MIN = 0.08       # Floor: below 8% the model's miscalibration is too small to overcome vig
EDGE_MAX = 0.12       # Cap: above 12% the model captures real signal and fading reverses
OPP_ODDS_MIN = -150
OPP_ODDS_MAX = 140
STAKE_UNITS = 5.0


def _home_win_prob(ms):
    return 1.0 / (1.0 + math.exp(ms / SCALE))


def _odds_to_implied(american):
    if american > 0:
        return 100.0 / (american + 100.0)
    return abs(american) / (abs(american) + 100.0)


def try_mlb_ml_fade_flip(conn, sp, prelude, mkt, seen):
    """Try to fire one MLB_ML_FADE_FLIP pick on a baseball_mlb game.

    Args:
        conn, sp: DB + sport key.
        prelude: dict from score_game_prelude (eid, commence, home, away, ms).
        mkt: dict with hml, hml_book, aml, aml_book.
        seen: pipeline-wide dedup set (mutated on fire).

    Returns:
        list[dict]: 0 or 1 pick.
    """
    if sp != 'baseball_mlb':
        return []

    eid = prelude['eid']
    commence = prelude['commence']
    home = prelude['home']
    away = prelude['away']
    ms = prelude['ms']

    hml = mkt.get('hml'); hml_book = mkt.get('hml_book')
    aml = mkt.get('aml'); aml_book = mkt.get('aml_book')

    if hml is None or aml is None or ms is None:
        return []

    # Skip if any ML already taken on this event by an upstream channel.
    if f"{eid}|M|{home}" in seen or f"{eid}|M|{away}" in seen:
        return []

    ihp = _odds_to_implied(hml)
    iap = _odds_to_implied(aml)
    total = ihp + iap
    if total <= 0:
        return []
    h_fair = ihp / total
    a_fair = iap / total

    ph_model = _home_win_prob(ms)
    pa_model = 1.0 - ph_model

    h_edge = ph_model - h_fair
    a_edge = pa_model - a_fair

    if EDGE_MIN <= h_edge <= EDGE_MAX:
        fade_team, fade_odds, fade_book = away, aml, aml_book
        followed_side = 'HOME'
        followed_edge_pct = h_edge * 100
    elif EDGE_MIN <= a_edge <= EDGE_MAX:
        fade_team, fade_odds, fade_book = home, hml, hml_book
        followed_side = 'AWAY'
        followed_edge_pct = a_edge * 100
    else:
        return []

    if (fade_odds is None or fade_book is None
            or fade_odds < OPP_ODDS_MIN or fade_odds > OPP_ODDS_MAX):
        return []

    seen.add(f"{eid}|M|{fade_team}")

    note = (
        f'MLB_ML_FADE_FLIP — model_spread={ms:+.2f} → logistic ph={ph_model:.3f} '
        f'(market fair={h_fair:.3f}). Model edge on {followed_side}={followed_edge_pct:.1f}% '
        f'(8-12% bucket). Fading to {fade_team} ML @ {fade_book} {fade_odds:+.0f}.'
    )

    pick = {
        'sport': sp,
        'event_id': eid,
        'commence': commence,
        'home': home,
        'away': away,
        'market_type': 'MONEYLINE',
        'selection': f'{fade_team} ML',
        'book': fade_book,
        'line': None,
        'odds': fade_odds,
        'model_spread': ms,
        'model_prob': 0,
        'implied_prob': round(_odds_to_implied(fade_odds), 4),
        'edge_pct': 0,
        'star_rating': 3,
        'units': STAKE_UNITS,
        'confidence': 'FADE_FLIP',
        'side_type': 'MLB_ML_FADE_FLIP',
        'spread_or_ml': 'ML',
        'timing': 'STANDARD',
        'context': note,
        'notes': note,
    }

    print(f"  \U0001f504 MLB_ML_FADE_FLIP: {fade_team} ML @ "
          f"{fade_book} {fade_odds:+.0f} (model edge on {followed_side} "
          f"{followed_edge_pct:.1f}%, ms={ms:+.2f})")

    return [pick]
