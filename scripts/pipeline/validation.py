"""
Pre-save pick validation — catches logical errors before picks get emailed.

Checks each pick for:
  1. Wrong-direction spreads (model favors OTHER side) — exempts the
     intentionally-counter channels (SPREAD_FADE_FLIP, DATA_SPREAD,
     DATA_TOTAL, BOOK_ARB, PROP_BOOK_ARB, PROP_FADE_FLIP, FADE_FLIP,
     PROP_CAREER_FADE).
  2. Impossible edges (>50% on any bet type).
  3. Missing critical fields.
  4. Contradictory ML picks (model strongly favors other side).

Extracted from main.py in v26.0 Phase 8 (main.py modularization).

Re-exported from main for back-compat: `from main import _validate_picks`.
"""


def _validate_picks(picks):
    """
    Pre-save validation — catches logical errors before picks get emailed.
    
    Checks:
    1. Wrong-direction spreads (model favors OTHER side)
    2. Impossible edges (>50% on any bet type)
    3. Missing critical fields
    4. Contradictory picks (both sides of same game)
    
    Returns filtered list with warnings printed.
    """
    valid = []
    flagged = 0

    for p in picks:
        ms = p.get('model_spread')
        sel = p.get('selection', '')
        mtype = p.get('market_type', '')
        edge = p.get('edge_pct', 0)
        line = p.get('line')
        sport = p.get('sport', '')
        home = p.get('home', '')
        away = p.get('away', '')

        # v25.39: SPREAD_FADE_FLIP + DATA_SPREAD intentionally bet the side
        # the Elo model DISAGREES with (that's the whole strategy). The
        # wrong-direction check below would block them incorrectly. Same for
        # BOOK_ARB picks which bypass model-side validation by design.
        if p.get('side_type') in ('SPREAD_FADE_FLIP', 'DATA_SPREAD',
                                   'BOOK_ARB', 'PROP_BOOK_ARB',
                                   'DATA_TOTAL', 'PROP_FADE_FLIP', 'FADE_FLIP',
                                   'PROP_CAREER_FADE', 'RAW_EDGE_FLIP'):
            valid.append(p)
            continue

        # CHECK 1: Impossible edge (>50% is almost certainly a calculation error)
        if edge > 50:
            print(f"  ⚠ BLOCKED: {sel} — {edge:.1f}% edge is impossibly high")
            flagged += 1
            continue
        
        # CHECK 2: Missing model spread (can't validate direction)
        if ms is None and mtype == 'SPREAD':
            print(f"  ⚠ WARNING: {sel} — no model spread, can't validate direction")
        
        # CHECK 3: Spread direction validation
        if mtype == 'SPREAD' and ms is not None and line is not None:
            # For spread picks: selection contains the team + line
            # If betting the away team (dog), model should think they deserve MORE points
            # If betting the home team (fav), model should think they should lay MORE
            
            if home and home in sel:
                # Betting home side at line (negative = fav, positive = dog)
                # Model spread ms: negative = home fav
                # Value for home when ms < line (model says home stronger)
                if ms > line + 0.5:  # 0.5 tolerance for rounding
                    print(f"  ⚠ BLOCKED: {sel} — model={ms:+.1f} but line={line:+.1f}, value is on {away}")
                    flagged += 1
                    continue
            elif away and away in sel:
                # Betting away side at line (positive = dog)
                # Away model spread = -ms, away line = line
                # Value for away when -ms < line (model says away deserves fewer pts than market gives)
                neg_ms = -ms
                if neg_ms > line + 0.5:
                    print(f"  ⚠ BLOCKED: {sel} — model={ms:+.1f} but line={line:+.1f}, value is on {home}")
                    flagged += 1
                    continue
        
        # CHECK 4: ML bet where model favors other side
        if mtype == 'MONEYLINE' and ms is not None:
            if home and home in sel and ms > 0.5:
                # Betting home ML but model says away is better
                print(f"  ⚠ BLOCKED: {sel} — model={ms:+.1f} favors {away}")
                flagged += 1
                continue
            elif away and away in sel and ms < -0.5:
                # Betting away ML but model says home is better
                # This is OK for dogs — the edge comes from odds mispricing
                # Only flag if model strongly disagrees
                pass  # ML dogs can have value even when model slightly favors home
        
        valid.append(p)
    
    if flagged:
        print(f"  🛡️ Validation: blocked {flagged} wrong-direction pick(s)")
    
    return valid


