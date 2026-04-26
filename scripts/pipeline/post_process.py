"""
Post-loop processing — applied to all_picks after every sport's per-game
scoring loop has run.

Three sequential stages, in order:

  1. apply_context_confirmation(picks)
     Picks without a `context` field signal pure model disagreement (no
     situational REASON for the edge — no rest/splits/travel/pace/H2H/refs).
     - Cap units at STRONG tier (4.0u). MAX PLAY requires context backing.
     - Totals without context: extra 15% Kelly haircut (totals model is the
       weakest signal; without pace/ref confirmation, lighter sizing).
     - NCAAB favorite spreads: 20% haircut (model overvalues NCAAB favorites
       3-6 vs dogs 13-5; haircut applied regardless of context).

  2. apply_clv_gate(picks, conn)
     Block picks where the line moved >= clv_threshold AGAINST our side
     between opener and current snapshot. Per-market thresholds:
       SPREAD: 2.0 pts adverse
       TOTAL:  1.5 pts adverse (totals move less)
     Mutates `picks` in place — adverse picks removed, marginal moves
     (>=1.0 pt either direction) annotated with `line_move` field.

  3. apply_final_filter(picks)
     - MIN_ODDS hard block (anything <= MIN_ODDS dropped — too steep)
     - Star floor: ML needs >= 1.0★ (raw Elo edge), spreads/totals need
       >= 2.0★ (key-number inflation makes lower PV noise)
     - Concentration: one pick per (event_id, market_type) — best edge wins
     - Sorts picks by star × edge before deduping so the survivor of each
       (event, market) is the highest-conviction option.

Extracted from `model_engine.generate_predictions()` lines 1475-1595 in
v26.0 Phase 7. Behavior is byte-equivalent to the inline original;
verified by tests/shadow_predict.py.
"""


def apply_context_confirmation(picks):
    """Stage 1: cap unit sizing based on context backing.

    Mutates each pick dict in place. Returns the same `picks` list for
    chaining.

    Rules:
      - No `context` field → cap units at 4.0 (STRONG ceiling). MAX PLAY
        requires the model to have a situational REASON.
      - No context AND market_type='TOTAL' → 15% Kelly haircut, floor 2.0u.
        The totals model is the weakest signal; without pace/ref/H2H
        confirmation, less aggressive sizing is warranted.
      - NCAAB favorite spread (line < 0) → 20% haircut, floor 2.0u.
        Backtest: NCAAB favorites 3W-6L (-16.1u) vs dogs 13W-5L (+32.4u).
    """
    for p in picks:
        has_context = bool(p.get('context'))

        if not has_context:
            if p['market_type'] == 'TOTAL':
                p['units'] = round(p['units'] * 0.85, 1)
                if p['units'] < 2.0:
                    p['units'] = 2.0

            max_units_no_context = 4.0
            if p['units'] > max_units_no_context:
                p['units'] = max_units_no_context

        if 'basketball_ncaab' in p.get('sport', ''):
            line = p.get('line')
            if line is not None and line < 0 and p.get('market_type') == 'SPREAD':
                p['units'] = round(p['units'] * 0.80, 1)
                if p['units'] < 2.0:
                    p['units'] = 2.0
    return picks


def apply_clv_gate(picks, conn):
    """Stage 2: drop picks with adverse line movement since opener.

    Reads `line_snapshots` table (may not exist on older DBs — silently
    no-ops in that case). Mutates `picks` in place by removing blocked
    indices. Annotates surviving picks with marginal moves (>=1.0 pt).

    Adverse thresholds:
      SPREAD: shift > 2.0 or shift < -2.0 (moved against us)
      TOTAL OVER:  shift < -1.5 (total dropped — harder to go over)
      TOTAL UNDER: shift > +1.5 (total rose — harder to stay under)

    Returns the (mutated) `picks` list for chaining.
    """
    try:
        blocked_indices = []
        for i, p in enumerate(picks):
            if p.get('market_type') not in ('SPREAD', 'TOTAL') or not p.get('event_id'):
                continue
            mkt = 'spreads' if p['market_type'] == 'SPREAD' else 'totals'
            sel = p.get('selection', '')
            if p['market_type'] == 'TOTAL':
                outcome = 'Over' if 'OVER' in sel else 'Under'
            else:
                outcome = sel.split()[0]
            snaps = conn.execute("""
                SELECT point, snapshot_time FROM line_snapshots
                WHERE event_id = ? AND market = ? AND outcome = ?
                ORDER BY snapshot_time ASC
            """, (p['event_id'], mkt, outcome)).fetchall()
            if len(snaps) >= 2:
                opener_pt = snaps[0][0]
                current_pt = snaps[-1][0]
                if opener_pt is not None and current_pt is not None:
                    shift = current_pt - opener_pt
                    clv_threshold = 1.5 if p['market_type'] == 'TOTAL' else 2.0
                    is_adverse = False
                    if p['market_type'] == 'SPREAD' and shift < -clv_threshold:
                        is_adverse = True
                    elif p['market_type'] == 'SPREAD' and shift > clv_threshold:
                        is_adverse = True
                    elif 'OVER' in sel and shift < -clv_threshold:
                        is_adverse = True
                    elif 'UNDER' in sel and shift > clv_threshold:
                        is_adverse = True

                    if is_adverse:
                        print(f"  CLV BLOCK: {sel} — line moved {shift:+.1f}pts against us "
                              f"(opener={opener_pt}, now={current_pt})")
                        blocked_indices.append(i)
                    elif abs(shift) >= 1.0:
                        p['line_move'] = round(shift, 1)
                        existing = p.get('notes', '')
                        p['notes'] = existing + f" | LINE MOVE: {shift:+.1f}pts since open"
        for i in sorted(blocked_indices, reverse=True):
            picks.pop(i)
    except Exception:
        pass  # line_snapshots table may not exist
    return picks


def apply_final_filter(picks):
    """Stage 3: MIN_ODDS gate + star floor + per-(event, market) dedup.

    - Drops picks with odds <= MIN_ODDS (heavy favorites are un-bettable).
    - Star floor: ML >= 1.0★ (raw Elo probability edge), other markets
      >= 2.0★ (PV must clear key-number inflation).
    - Sorts picks by star_rating × 100 + edge_pct, then keeps only the
      first-seen pick per (event_id, market_type) tuple.

    Returns a NEW list (does not mutate `picks`). Order matches the sort
    key — highest conviction first.
    """
    from config import MIN_ODDS
    final_picks = []
    seen_event_market = {}
    picks.sort(key=lambda x: x['star_rating'] * 100 + x['edge_pct'], reverse=True)
    for p in picks:
        p_odds = p.get('odds')
        if p_odds is not None and p_odds <= MIN_ODDS:
            continue
        is_ml = p.get('market_type') == 'MONEYLINE'
        min_stars = 1.0 if is_ml else 2.0
        if p['star_rating'] < min_stars:
            continue
        key = f"{p['event_id']}|{p['market_type']}"
        if key not in seen_event_market:
            seen_event_market[key] = True
            final_picks.append(p)
    return final_picks
