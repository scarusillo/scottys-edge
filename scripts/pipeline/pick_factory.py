"""
Pick-dict factories used by the per-game scoring pipeline.

`_mk` builds a SPREAD or TOTAL pick dict from a Walters edge assessment.
`_mk_ml` builds a MONEYLINE pick dict from Elo or spread-derived probs.
`_conf` maps a star_rating into a confidence label string.

Extracted from model_engine.py in v26.0 Phase 7.

Re-exported from model_engine for back-compat: `from model_engine import
_mk, _mk_ml, _conf` keeps working.
"""
from scottys_edge import kelly_units, kelly_label
from pipeline.sport_adjustments import _divergence_penalty


def _mk(sp, eid, commence, home, away, mtype, sel, book, line, odds, ms, prob, imp, wa, side):
    # Walters methodology: key number value (PV%) IS the edge.
    # Probability edge is a sanity check — if cover prob strongly disagrees
    # with the market, the PV% is phantom. But small probability edges with
    # real PV% are legitimate (common for favorites where the market is
    # efficient but your number crosses key numbers).
    prob_edge = (prob - imp) * 100 if imp else 0
    pv_edge = wa['point_value_pct'] * 0.40  # Dampened PV% for Kelly sizing

    # Sanity check: if probability STRONGLY disagrees, this is phantom value.
    # PV% can look good when spread crosses key numbers but the probability
    # math says there's no real edge. Allow small negative prob edges (favorites)
    # but reject deeply negative ones.
    if prob_edge < -2.0:
        return None

    # Walters: use the HIGHER of probability edge or dampened PV% for sizing.
    # This lets favorites with real key number value get meaningful units
    # instead of being killed by tiny probability edges.
    actual_edge = max(prob_edge, pv_edge, 0)

    # v23: Cap at 30% — edges above 30% are noise (2W-4L -9.3u).
    # 25-30% is the best bucket: 33W-20L +44u (62%). Let Kelly size naturally.
    actual_edge = min(actual_edge, 30.0)

    # v17: Model-vs-market divergence penalty for spreads
    # When model heavily disagrees with market, reduce edge (market is likely right)
    if line is not None:
        div_mult = _divergence_penalty(ms, line, 'SPREAD')
        if div_mult < 1.0:
            actual_edge *= div_mult

    if actual_edge < 1.0:
        return None

    units = kelly_units(edge_pct=actual_edge, odds=odds)
    if units <= 0:
        return None

    kl = kelly_label(units)
    return {
        'sport': sp, 'event_id': eid, 'commence': commence,
        'home': home, 'away': away, 'market_type': mtype,
        'selection': sel, 'book': book, 'line': line, 'odds': odds,
        'model_spread': ms, 'model_prob': round(prob,4),
        'implied_prob': round(imp,4) if imp else None,
        'edge_pct': round(actual_edge, 2),
        'star_rating': wa['star_rating'], 'units': units,
        'confidence': _conf(wa['star_rating']),
        'spread_or_ml': wa['spread_or_ml'], 'timing': wa['timing'],
        'notes': f"Model={ms:+.1f} PV={wa['point_value_pct']}% {wa['star_rating']}★ "
                 f"VigAdj={wa['vig_adjusted_spread']:+.2f} | "
                 f"Prob={prob:.1%} Imp={imp:.1%} RealEdge={actual_edge:.1f}% "
                 f"Units={units:.1f} ({kl}) | {wa['timing']}",
    }



def _mk_ml(sp, eid, commence, home, away, sel, book, odds, ms, prob, imp, edge, stars, timing, t_r):
    # v23: Cap at 30% — edges above 30% are noise (2W-4L -9.3u).
    edge = min(edge, 30.0)
    units = kelly_units(edge_pct=edge, odds=odds)
    kl = kelly_label(units)
    return {
        'sport': sp, 'event_id': eid, 'commence': commence,
        'home': home, 'away': away, 'market_type': 'MONEYLINE',
        'selection': sel, 'book': book, 'line': None, 'odds': odds,
        'model_spread': ms, 'model_prob': round(prob,4),
        'implied_prob': round(imp,4) if imp else None,
        'edge_pct': round(edge, 2), 'star_rating': stars,
        'units': units, 'confidence': _conf(stars),
        'spread_or_ml': 'MONEYLINE', 'timing': timing,
        'notes': f"Win={prob:.1%} Imp={imp:.1%} Edge={edge:.1f}% {stars}★ Kelly={kl} | {t_r}",
    }



def _conf(s):
    # 3/25: HIGH tier eliminated — 3W-7L -22.5u (-46.9% ROI).
    # Only ELITE (2.5+ stars) passes the card filter. Everything below
    # is sub-ELITE and gets blocked. No reason for a "HIGH" label that
    # could leak through filter edge cases.
    if s >= 2.5: return 'ELITE'
    if s >= 1.5: return 'STRONG'
    if s >= 1.0: return 'MEDIUM'
    return 'LOW' if s >= 0.5 else 'NO_PLAY'

