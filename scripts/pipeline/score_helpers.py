"""
Pure helper functions for the scoring stage.

Extracted from model_engine.generate_predictions() in v26.0 Phase 2.
Each function is a pure computation — no DB writes, no logging, no global
state mutation. They are called from inside the per-game scoring loop.

These helpers will fold into Phase 2b's score_spreads() / score_moneylines()
extractions, but live separately first so each can be unit-tested in isolation.
"""
from datetime import datetime


def compute_neutral_site(sp):
    """True if the game should be treated as a neutral-court matchup (no HCA).

    Tennis: every tournament match is at a neutral venue.
    NCAAB: March 17+ = NCAA tournament (post-Selection Sunday). April 1-7 =
    Final Four + Championship. Earlier March = regular season + conference
    tournaments where HCA still applies.
    """
    if sp.startswith('tennis_'):
        return True
    if sp == 'basketball_ncaab':
        now = datetime.now()
        m, d = now.month, now.day
        if m == 4 and d <= 7:
            return True
        if m == 3 and d >= 17:
            return True
    return False


def compute_elo_confidence_weight(min_games, sport_min_games=15):
    """Sample-size weight: 1.0 once both teams have `sport_min_games` Elo games.

    A 10% Elo edge from 3 games is not the same as 10% from 25 games — this
    weight scales the edge by data quality. Floor 0, ceiling 1.0.
    """
    if min_games is None or min_games <= 0:
        return 0.0
    return min(1.0, min_games / max(1, sport_min_games))


def compute_mismatch_dampening(h_fair, a_fair):
    """Dampen Elo edges when the market sees a clear mismatch.

    Elo compresses toward 50% — it can't distinguish 95% from 99% favorites.
    Close games get full weight (1.0). At market max-fair > 0.75, taper toward
    0.40 floor. Used to prevent Elo from claiming false edges on extreme dogs.
    """
    if h_fair is None or a_fair is None:
        return 1.0
    mkt_max = max(h_fair, a_fair)
    if mkt_max <= 0.75:
        return 1.0
    return max(0.40, 1.0 - (mkt_max - 0.75) * 2.4)


def compute_sos_dampening(home, away, sport, conn):
    """SOS-based weight for NCAAB cross-conference matchups.

    Two checks against opponents-Elo-average ("SOS"):
      1. SOS gap > 100 between teams → block (different leagues).
         50 < gap ≤ 100 → taper.
      2. Min SOS < 1500 → block (cupcake schedule).
         1500 ≤ min < 1520 → heavy dampen to 0.20.

    Either failure can zero the weight. Both together = block. Returns 1.0
    when conditions don't apply (e.g. non-NCAAB sport).
    """
    sos_w = 1.0
    if sport != 'basketball_ncaab' or conn is None:
        return sos_w
    try:
        h_sos = conn.execute(
            """
            SELECT AVG(e.elo) FROM results r
            JOIN elo_ratings e ON e.team = CASE WHEN r.home=? THEN r.away ELSE r.home END
                AND e.sport=?
            WHERE (r.home=? OR r.away=?) AND r.sport=? AND r.completed=1
            """, (home, sport, home, home, sport)).fetchone()[0] or 1500
        a_sos = conn.execute(
            """
            SELECT AVG(e.elo) FROM results r
            JOIN elo_ratings e ON e.team = CASE WHEN r.home=? THEN r.away ELSE r.home END
                AND e.sport=?
            WHERE (r.home=? OR r.away=?) AND r.sport=? AND r.completed=1
            """, (away, sport, away, away, sport)).fetchone()[0] or 1500
        sos_gap = abs(h_sos - a_sos)
        if sos_gap > 100:
            sos_w = 0.0
        elif sos_gap > 50:
            sos_w = max(0.20, 1.0 - (sos_gap - 50) / 60)
        min_sos = min(h_sos, a_sos)
        if min_sos < 1500:
            sos_w = 0.0
        elif min_sos < 1520:
            sos_w = min(sos_w, 0.20)
    except Exception:
        pass
    return sos_w


def apply_injury_to_prob(home_prob, h_imp, a_imp, threshold=0.01):
    """Adjust home win probability by net injury impact.

    Each point of injury impact ≈ 1.5% win probability swing. Skip when net
    shift is below `threshold` (avoids noise from sub-threshold reports).
    Clamped to [0.05, 0.95] to prevent NaN-equivalent edges on extreme inputs.
    """
    if home_prob is None:
        return None
    h_imp = h_imp or 0
    a_imp = a_imp or 0
    shift = (a_imp - h_imp) * 0.015
    if abs(shift) < threshold:
        return home_prob
    return max(0.05, min(0.95, home_prob + shift))


def compute_opener_move_for_pick(conn, pick):
    """Direction-adjusted opener→fire line move for a candidate pick.

    Positive = line moved TOWARD our side between opener and fire.
    Returns None if openers data is missing or market_type unsupported.

    Used by v25.80 CLV_MICRO_EDGE_SHADOW logger + LINE_AGAINST_GATE.

    v25.89: falls back to inferring direction from bet line sign for
    non-standard side_types (DATA_SPREAD, SPREAD_FADE_FLIP, etc.).
    """
    mtype = pick.get('market_type')
    if mtype not in ('SPREAD', 'TOTAL'):
        return None
    event_id = pick.get('event_id')
    fire_line = pick.get('line')
    if not event_id or fire_line is None:
        return None
    market = 'spreads' if mtype == 'SPREAD' else 'totals'
    try:
        row = conn.execute(
            "SELECT AVG(line) FROM openers WHERE event_id=? AND market=? AND line IS NOT NULL",
            (event_id, market)).fetchone()
    except Exception:
        return None
    if not row or row[0] is None:
        return None
    opener_line = row[0]
    raw = fire_line - opener_line
    if mtype == 'TOTAL':
        sel = (pick.get('selection') or '').upper()
        if 'OVER' in sel:
            return raw
        if 'UNDER' in sel:
            return -raw
    elif mtype == 'SPREAD':
        st = pick.get('side_type') or ''
        if st == 'DOG':
            return raw
        if st == 'FAVORITE':
            return -raw
        # Fall back to inferring direction from line sign
        if fire_line < 0:
            return -raw
        if fire_line > 0:
            return raw
    return None
