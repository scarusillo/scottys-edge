"""
steam_engine.py — Sharp money / steam move detection

Compares opener lines to current lines to detect where sharp money has
pushed the market. Tags picks with a steam signal:
  - SHARP_CONFIRMS: line moved in the direction of our pick (sharps agree)
  - SHARP_OPPOSES: line moved against our pick (sharps disagree) — red flag
  - NO_MOVEMENT: line is flat (no signal)

Movement thresholds are sport-specific. A 0.5 total move in MLB is huge;
in NBA it's noise.

Usage:
    from steam_engine import get_steam_signal
    signal, info = get_steam_signal(conn, sport, event_id, market_type, side, line)
"""
import sqlite3, os
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Sport-specific movement thresholds
# A move >= threshold is "significant" sharp action
STEAM_THRESHOLDS = {
    'baseball_mlb':        {'total': 0.5,  'spread': 0.5,  'h2h_cents': 15},
    'baseball_ncaa':       {'total': 1.0,  'spread': 1.0,  'h2h_cents': 20},
    'basketball_nba':      {'total': 2.0,  'spread': 1.0,  'h2h_cents': 15},
    'basketball_ncaab':    {'total': 2.0,  'spread': 1.5,  'h2h_cents': 20},
    'icehockey_nhl':       {'total': 0.5,  'spread': 0.5,  'h2h_cents': 15},
    'soccer_epl':          {'total': 0.25, 'spread': 0.5,  'h2h_cents': 20},
    'soccer_italy_serie_a': {'total': 0.25, 'spread': 0.5, 'h2h_cents': 20},
    'soccer_spain_la_liga': {'total': 0.25, 'spread': 0.5, 'h2h_cents': 20},
    'soccer_germany_bundesliga': {'total': 0.25, 'spread': 0.5, 'h2h_cents': 20},
    'soccer_france_ligue_one': {'total': 0.25, 'spread': 0.5, 'h2h_cents': 20},
    'soccer_uefa_champs_league': {'total': 0.25, 'spread': 0.5, 'h2h_cents': 20},
    'soccer_usa_mls':      {'total': 0.25, 'spread': 0.5,  'h2h_cents': 20},
    'soccer_mexico_ligamx': {'total': 0.25, 'spread': 0.5, 'h2h_cents': 20},
}

DEFAULT_THRESHOLD = {'total': 1.0, 'spread': 1.0, 'h2h_cents': 20}


def _get_threshold(sport):
    return STEAM_THRESHOLDS.get(sport, DEFAULT_THRESHOLD)


def _median_line(rows):
    """Get median line from a set of (line,) or (price,) rows."""
    vals = sorted([r[0] for r in rows if r[0] is not None])
    if not vals:
        return None
    n = len(vals)
    if n % 2 == 0:
        return (vals[n // 2 - 1] + vals[n // 2]) / 2
    return vals[n // 2]


def _median_odds(rows):
    """Get median odds from rows of odds values."""
    vals = sorted([r[0] for r in rows if r[0] is not None])
    if not vals:
        return None
    n = len(vals)
    if n % 2 == 0:
        return (vals[n // 2 - 1] + vals[n // 2]) / 2
    return vals[n // 2]


def get_steam_signal(conn, sport, event_id, market_type, side, current_line, current_odds=None):
    """
    Compare opener to current line for an event/market.

    Args:
        conn: sqlite connection
        sport: sport key
        event_id: event id
        market_type: 'TOTAL', 'SPREAD', or 'MONEYLINE'
        side: 'OVER', 'UNDER', 'DOG', 'FAVORITE', 'home', 'away' (describes pick direction)
        current_line: the line we're betting at (for totals/spreads)
        current_odds: the odds we're betting at (for moneyline)

    Returns:
        (signal, info_dict)
        signal: 'SHARP_CONFIRMS', 'SHARP_OPPOSES', or 'NO_MOVEMENT'
        info_dict: details for display/logging
    """
    thr = _get_threshold(sport)

    # Map market_type to odds table market
    market_map = {
        'TOTAL': 'totals',
        'SPREAD': 'spreads',
        'MONEYLINE': 'h2h',
    }
    market = market_map.get(market_type)
    if not market:
        return 'NO_MOVEMENT', {}

    # Get opener lines (median across books for robustness).
    # Use `openers` table (349K rows, comprehensive) first; fall back to
    # `odds` table OPENER tag (only 10K total rows).
    opener_rows = conn.execute("""
        SELECT line, odds, selection FROM openers
        WHERE event_id=? AND market=?
    """, (event_id, market)).fetchall()

    if not opener_rows:
        opener_rows = conn.execute("""
            SELECT line, odds, selection FROM odds
            WHERE event_id=? AND market=? AND tag='OPENER'
        """, (event_id, market)).fetchall()

    if not opener_rows:
        return 'NO_MOVEMENT', {'reason': 'no_opener_data'}

    info = {'sport': sport, 'event_id': event_id, 'market': market_type}

    if market_type == 'TOTAL':
        # Median opener line across books
        opener_line = _median_line([(r[0],) for r in opener_rows if r[0] is not None])
        if opener_line is None:
            return 'NO_MOVEMENT', {'reason': 'no_opener_line'}

        movement = current_line - opener_line
        info['opener'] = opener_line
        info['current'] = current_line
        info['movement'] = round(movement, 2)

        if abs(movement) < thr['total']:
            return 'NO_MOVEMENT', info

        # Movement direction tells us sharp action
        # Line went UP = sharps hit OVER (pushed total up)
        # Line went DOWN = sharps hit UNDER (pushed total down)
        sharp_side = 'OVER' if movement > 0 else 'UNDER'
        info['sharp_side'] = sharp_side

        if side == sharp_side:
            return 'SHARP_CONFIRMS', info
        else:
            return 'SHARP_OPPOSES', info

    elif market_type == 'SPREAD':
        # For spreads, movement in the favorite's number indicates sharp action
        # Opener favorite -6.5, current -7.5 = sharps hit favorite (line moved toward fav)
        # Opener favorite -6.5, current -5.5 = sharps hit dog (line moved toward dog)
        opener_line = _median_line([(r[0],) for r in opener_rows if r[0] is not None])
        if opener_line is None:
            return 'NO_MOVEMENT', {'reason': 'no_opener_line'}

        movement = current_line - opener_line
        info['opener'] = opener_line
        info['current'] = current_line
        info['movement'] = round(movement, 2)

        if abs(movement) < thr['spread']:
            return 'NO_MOVEMENT', info

        # If our line is negative (favorite), a more-negative current = sharps on fav
        # If our line is positive (dog), a more-positive current = sharps on dog
        # The 'side' parameter tells us FAVORITE or DOG
        # Sharp side: if line moved more negative, sharps took favorite
        if movement < 0:
            sharp_side = 'FAVORITE'
        else:
            sharp_side = 'DOG'
        info['sharp_side'] = sharp_side

        if side == sharp_side:
            return 'SHARP_CONFIRMS', info
        else:
            return 'SHARP_OPPOSES', info

    elif market_type == 'MONEYLINE':
        # For moneylines, compare odds movement
        # A team's odds getting shorter (e.g. +150 -> +130) = sharps on that team
        # Need to group opener odds by selection
        opener_by_sel = defaultdict(list)
        for r in opener_rows:
            if r[1] is not None:
                opener_by_sel[r[2]].append(r[1])

        if len(opener_by_sel) < 2:
            return 'NO_MOVEMENT', {'reason': 'insufficient_opener_ml'}

        # Get current median odds for each side (from odds table CURRENT tag)
        current_rows = conn.execute("""
            SELECT odds, selection FROM odds
            WHERE event_id=? AND market=? AND tag='CURRENT'
        """, (event_id, market)).fetchall()

        current_by_sel = defaultdict(list)
        for r in current_rows:
            if r[0] is not None:
                current_by_sel[r[1]].append(r[0])

        # For each selection, compute median shift
        shifts = {}
        for sel in opener_by_sel:
            open_med = _median_odds([(v,) for v in opener_by_sel[sel]])
            curr_vals = current_by_sel.get(sel, [])
            curr_med = _median_odds([(v,) for v in curr_vals]) if curr_vals else None
            if open_med is None or curr_med is None:
                continue
            # "Shift" in cents — positive means odds got longer (worse for bettor)
            shifts[sel] = curr_med - open_med

        if not shifts:
            return 'NO_MOVEMENT', {'reason': 'no_shifts'}

        # The sharp side is the one where odds got SHORTER (price went down, more expensive)
        # i.e. the side with the most negative shift
        sharp_side_name = min(shifts, key=lambda k: shifts[k])
        max_shift = abs(shifts[sharp_side_name])

        info['shifts'] = {k: round(v, 1) for k, v in shifts.items()}
        info['sharp_side_name'] = sharp_side_name
        info['max_shift_cents'] = round(max_shift, 1)

        if max_shift < thr['h2h_cents']:
            return 'NO_MOVEMENT', info

        # Determine if our pick is on the sharp side
        # side is usually 'DOG'/'FAVORITE' for spreads, but for MONEYLINE it could be team name
        # We'd need to pass the selection name to match
        if side.lower() == sharp_side_name.lower():
            return 'SHARP_CONFIRMS', info
        else:
            return 'SHARP_OPPOSES', info

    return 'NO_MOVEMENT', info


def format_steam_context(signal, info):
    """Format a steam signal as a context factor string."""
    if signal == 'NO_MOVEMENT':
        # v25.34: log NO_MOVEMENT explicitly so the morning agent can
        # distinguish "engine ran, line held" from "engine was never called."
        # Previously returned '' which made both cases look identical in
        # context_factors.
        return 'Steam: no movement'
    if 'movement' in info:
        direction = '+' if info['movement'] > 0 else ''
        if signal == 'SHARP_CONFIRMS':
            return f"Steam: sharp confirms ({direction}{info['movement']})"
        else:
            return f"Steam: sharp opposes ({direction}{info['movement']})"
    elif 'max_shift_cents' in info:
        if signal == 'SHARP_CONFIRMS':
            return f"Steam ML: sharp confirms ({info['max_shift_cents']} cents)"
        else:
            return f"Steam ML: sharp opposes ({info['max_shift_cents']} cents)"
    return ''


if __name__ == '__main__':
    # Test with some known events
    conn = sqlite3.connect(DB_PATH)

    # Find recent events with both OPENER and CURRENT data
    rows = conn.execute("""
        SELECT DISTINCT event_id, sport, home, away
        FROM odds WHERE tag='OPENER' AND DATE(snapshot_date) >= '2026-04-12'
        LIMIT 5
    """).fetchall()

    for event_id, sport, home, away in rows:
        print(f"\n{sport}: {away} @ {home}")
        for mt, side, line in [('TOTAL', 'OVER', 7.5), ('TOTAL', 'UNDER', 7.5),
                                ('SPREAD', 'FAVORITE', -3.5)]:
            signal, info = get_steam_signal(conn, sport, event_id, mt, side, line)
            print(f"  {mt} {side} {line}: {signal} ({info.get('movement', 'N/A')})")

    conn.close()
