"""v26.1 MLB Context ML — SHADOW MODE channel.

Computes the new logistic regression model's home_win_prob at fire time,
finds candidate ML picks at edge >= 8% in [-150, +140], and logs to
`shadow_blocked_picks` with reason_category='MLB_CONTEXT_ML_SHADOW'.

DOES NOT FIRE LIVE PICKS. Pure observation while we accumulate forward
data. Promotion criteria (decision after 14 days):
  - n >= 30 forward shadow fires
  - WR >= 55% on counterfactual grading
  - log-loss vs market_consensus avg < 0.69
If all three pass: promote to live at 3u stake, confluence-only with
MLB_ML_FADE_FLIP (both agree → fire; either alone → don't fire).

Validation at training (2026-04-29):
  Holdout (n=20 fires at edge>=8%): 12W-8L (60%) +24.20u
  Full sample (n=96): 52W-44L (54.2%) +47.06u
  vs spread-based FOLLOW baseline: -63.50u at same threshold (+110u swing)
But: holdout log-loss 0.733 > random 0.693 → model overfits, betting EV
comes from selective high-conviction calls only. Hence shadow first.
"""
import math
import os


EDGE_FLOOR = 0.08
ODDS_MIN = -150
ODDS_MAX = 140


def _odds_to_implied(american):
    if american is None:
        return None
    if american > 0:
        return 100.0 / (american + 100.0)
    return abs(american) / (abs(american) + 100.0)


def try_mlb_context_ml_shadow(conn, sp, prelude, mkt):
    """Compute home_win_prob from the trained model and log shadow fires.

    Returns [] always (shadow channel doesn't fire live picks). Logs to
    shadow_blocked_picks for monitoring.
    """
    if sp != 'baseball_mlb':
        return []

    eid = prelude.get('eid')
    commence = prelude.get('commence')
    home = prelude.get('home')
    away = prelude.get('away')

    hml = mkt.get('hml'); hml_book = mkt.get('hml_book')
    aml = mkt.get('aml'); aml_book = mkt.get('aml_book')

    if hml is None or aml is None or not commence:
        return []

    try:
        from mlb_ml_model import predict_home_win_prob
    except Exception:
        return []

    game_date = commence[:10]
    try:
        ph = predict_home_win_prob(conn, home, away, game_date)
    except Exception:
        return []

    if ph is None:
        return []

    pa = 1.0 - ph
    ihp = _odds_to_implied(hml)
    iap = _odds_to_implied(aml)
    total = (ihp or 0) + (iap or 0)
    if total <= 0:
        return []
    h_fair = ihp / total
    a_fair = iap / total

    # Identify candidate side at edge >= 8% in odds range
    fires = []
    for side, ml, pmod, pfair, book in [
        ('HOME', hml, ph, h_fair, hml_book),
        ('AWAY', aml, pa, a_fair, aml_book),
    ]:
        if ml is None or not (ODDS_MIN <= ml <= ODDS_MAX):
            continue
        edge = pmod - pfair
        if edge < EDGE_FLOOR:
            continue
        team = home if side == 'HOME' else away
        fires.append({
            'side': side, 'team': team, 'ml': ml, 'book': book,
            'pmod': pmod, 'pfair': pfair, 'edge': edge,
        })

    if not fires:
        return []

    # Log each shadow fire
    from datetime import datetime as _dt
    for f in fires:
        try:
            detail = (f"side={f['side']} pmod={f['pmod']:.3f} pfair={f['pfair']:.3f} "
                      f"edge={f['edge']*100:.1f}% odds={f['ml']:+.0f}")
            conn.execute(
                """INSERT INTO shadow_blocked_picks (created_at, sport, event_id,
                    selection, market_type, book, line, odds, edge_pct, units,
                    reason, reason_category, reason_detail)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (_dt.now().isoformat(), sp, eid,
                 f"{f['team']} ML (shadow)", 'MONEYLINE', f['book'],
                 None, f['ml'], round(f['edge'] * 100, 2), 3.0,
                 f"MLB_CONTEXT_ML_SHADOW ({detail})",
                 'MLB_CONTEXT_ML_SHADOW', detail))
            conn.commit()
        except Exception:
            pass
        print(f"  \U0001f441 MLB_CONTEXT_ML_SHADOW: {f['team']} ML "
              f"@ {f['ml']:+.0f} (edge={f['edge']*100:.1f}%, ph={f['pmod']:.2f})")

    return []  # Shadow only — never fires live
