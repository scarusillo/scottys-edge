"""
Pick → DB persistence + pick classification helpers.

`save_picks_to_db` is the canonical write path used by `cmd_run`.
`_ensure_bet_columns` adds any missing optional columns to the bets table.
`_classify_*` derive analytical buckets (side, spread, edge, market tier)
from a pick dict; called by save_picks_to_db when stamping rows.

Extracted from model_engine.py in v26.0 Phase 7.

Re-exported from model_engine for back-compat.
"""
import sqlite3
from datetime import datetime


def save_picks_to_db(conn, picks):
    """Save picks to bets table with full analytical metadata.
    
    Captures every dimension needed for professional performance tracking:
    side_type, spread_bucket, timing, context factors, market tier, etc.
    
    Prevents duplicate entries when model is run multiple times per day.
    """
    # Ensure new columns exist (safe migration for existing DBs)
    _ensure_bet_columns(conn)
    
    now = datetime.now().isoformat()
    today = datetime.now().strftime('%Y-%m-%d')
    day_of_week = datetime.now().strftime('%A')  # Monday, Tuesday, etc.
    saved = 0
    dupes = 0
    skipped_no_eid = 0
    saved_picks = []  # Track which picks actually made it to the DB
    for p in picks:
        # Reject picks without event_id — these can't be graded or tracked
        if not p.get('event_id'):
            skipped_no_eid += 1
            print(f"  ⚠ Skipped (no event_id): {p.get('selection', 'unknown')[:50]}")
            continue
        # v12 FIX: Dedup by SIDE, not by full selection string.
        # Old logic: "Nebraska +1.5" != "Nebraska +0.0" → saved twice.
        # New logic: extract the team/side and match on that.
        # Spreads: "Nebraska Cornhuskers +1.5" → "Nebraska Cornhuskers"
        # Totals:  "UNDER 179.5" → "UNDER"
        # ML:      "Iowa State Cyclones ML" → "Iowa State Cyclones"
        import re
        sel = p['selection']
        mtype = p['market_type']
        
        if mtype == 'SPREAD':
            dedup_side = re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
        elif mtype == 'TOTAL':
            dedup_side = 'OVER' if 'OVER' in sel.upper() else 'UNDER'
        elif mtype == 'MONEYLINE':
            dedup_side = sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
        else:
            dedup_side = sel  # Props: use full selection (player-specific)
        
        # Check if we already bet this side of this game today
        existing_bets = conn.execute("""
            SELECT id, selection FROM bets
            WHERE event_id=? AND market_type=?
            AND DATE(created_at)=?
        """, (p['event_id'], mtype, today)).fetchall()
        
        is_dupe = False
        for (existing_id, existing_sel) in existing_bets:
            if mtype == 'SPREAD':
                existing_side = re.sub(r'\s*[+-]?\d+\.?\d*$', '', existing_sel).strip()
            elif mtype == 'TOTAL':
                existing_side = 'OVER' if 'OVER' in existing_sel.upper() else 'UNDER'
            elif mtype == 'MONEYLINE':
                existing_side = existing_sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
            else:
                existing_side = existing_sel
            
            if dedup_side == existing_side:
                is_dupe = True
                break
        
        if is_dupe:
            dupes += 1
            continue
        
        # ── Derive analytical dimensions ──
        # v25.22+: if caller set p['side_type'] explicitly (e.g. 'FADE_FLIP' from
        # Option C NCAA DK gate), preserve it. Otherwise infer from pick data.
        side_type = p.get('side_type') or _classify_side(p)
        spread_bucket = _classify_spread_bucket(p)
        edge_bucket = _classify_edge_bucket(p.get('edge_pct', 0))
        timing = p.get('timing', 'UNKNOWN')
        context_factors = p.get('context', '')
        context_confirmed = 1 if context_factors else 0
        context_adj = p.get('context_adj', 0.0)
        market_tier = _classify_market_tier(p.get('sport', ''))
        model_spread = p.get('model_spread', None)

        # v25.17: Log steam signal as context (no stake/selection change yet).
        # Informational only — review at April 20 checkpoint for NBA signal.
        try:
            from steam_engine import get_steam_signal, format_steam_context
            side_hint = p.get('side_type', '') or side_type
            # Map SIDE types to what steam_engine expects
            if p['market_type'] == 'TOTAL':
                steam_side = 'OVER' if 'OVER' in side_hint.upper() or 'over' in (p.get('selection','').lower()) else 'UNDER'
            elif p['market_type'] == 'SPREAD':
                steam_side = 'FAVORITE' if side_hint == 'FAVORITE' else 'DOG'
            else:
                steam_side = None
            if steam_side and p.get('event_id') and p.get('line') is not None:
                _sig, _info = get_steam_signal(conn, p['sport'], p['event_id'],
                                                p['market_type'], steam_side,
                                                p['line'], p.get('odds'))
                _steam_ctx = format_steam_context(_sig, _info)
                if _steam_ctx:
                    context_factors = (context_factors + ' | ' + _steam_ctx) if context_factors else _steam_ctx
        except Exception:
            pass
        
        conn.execute("""
            INSERT INTO bets (created_at, sport, event_id, market_type, selection,
                book, line, odds, model_prob, implied_prob, edge_pct, confidence, units,
                side_type, spread_bucket, edge_bucket, timing, context_factors,
                context_confirmed, context_adj, market_tier, model_spread, day_of_week)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (now, p['sport'], p['event_id'], p['market_type'], p['selection'],
              p['book'], p['line'], p['odds'], p['model_prob'], p['implied_prob'],
              p['edge_pct'], p['confidence'], p['units'],
              side_type, spread_bucket, edge_bucket, timing, context_factors,
              context_confirmed, context_adj, market_tier, model_spread, day_of_week))
        saved += 1
        saved_picks.append(p)
    conn.commit()
    if dupes:
        print(f"  💾 Saved {saved} picks ({dupes} duplicates skipped)")
    else:
        print(f"  💾 Saved {saved} picks")
    return saved_picks




def _ensure_bet_columns(conn):
    """Add analytical columns to bets table if they don't exist yet."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(bets)").fetchall()}
    new_cols = {
        'side_type': 'TEXT',         # FAVORITE, DOG, OVER, UNDER, PROP_OVER, PROP_UNDER
        'spread_bucket': 'TEXT',     # SMALL_DOG, MED_DOG, BIG_DOG, SMALL_FAV, MED_FAV, BIG_FAV, PK
        'edge_bucket': 'TEXT',       # EDGE_8_12, EDGE_12_16, EDGE_16_20, EDGE_20_PLUS
        'timing': 'TEXT',            # EARLY, LATE
        'context_factors': 'TEXT',   # Pipe-separated factor summary
        'context_confirmed': 'INT',  # 1 = has context, 0 = no context
        'context_adj': 'REAL',       # Total context adjustment in points
        'market_tier': 'TEXT',       # SOFT, SHARP
        'model_spread': 'REAL',      # The model's predicted spread
        'day_of_week': 'TEXT',       # Monday, Tuesday, etc.
    }
    for col, dtype in new_cols.items():
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE bets ADD COLUMN {col} {dtype}")
            except Exception:
                pass
    conn.commit()




def _classify_side(pick):
    """Classify pick as FAVORITE, DOG, OVER, UNDER, or PROP."""
    mtype = pick.get('market_type', '')
    sel = pick.get('selection', '')
    line = pick.get('line', 0)
    
    if mtype == 'TOTAL':
        return 'OVER' if 'OVER' in sel else 'UNDER'
    elif mtype == 'PROP':
        return 'PROP_OVER' if 'OVER' in sel else 'PROP_UNDER'
    elif mtype == 'MONEYLINE':
        odds = pick.get('odds', 0)
        if odds and odds > 0:
            return 'DOG'
        return 'FAVORITE'
    elif mtype == 'SPREAD':
        if line is not None and line > 0:
            return 'DOG'
        elif line is not None and line < 0:
            return 'FAVORITE'
        return 'PK'
    return 'UNKNOWN'




def _classify_spread_bucket(pick):
    """Classify spread magnitude into buckets."""
    mtype = pick.get('market_type', '')
    line = pick.get('line', 0)
    
    if mtype in ('TOTAL', 'PROP'):
        return 'N/A'
    
    if line is None:
        return 'UNKNOWN'
    
    abs_line = abs(line)
    if abs_line <= 0.5:
        side = 'PK'
    elif line > 0:  # Dog
        if abs_line <= 3.5:
            side = 'SMALL_DOG'
        elif abs_line <= 7.5:
            side = 'MED_DOG'
        else:
            side = 'BIG_DOG'
    else:  # Favorite
        if abs_line <= 3.5:
            side = 'SMALL_FAV'
        elif abs_line <= 7.5:
            side = 'MED_FAV'
        else:
            side = 'BIG_FAV'
    return side




def _classify_edge_bucket(edge_pct):
    """Classify projected edge into buckets."""
    if edge_pct >= 20:
        return 'EDGE_20_PLUS'
    elif edge_pct >= 16:
        return 'EDGE_16_20'
    elif edge_pct >= 12:
        return 'EDGE_12_16'
    return 'EDGE_8_12'




def _classify_market_tier(sport):
    """Classify sport into SOFT or SHARP market tier."""
    soft = {'basketball_ncaab', 'soccer_usa_mls', 'soccer_germany_bundesliga',
            'soccer_france_ligue_one', 'soccer_italy_serie_a', 'soccer_uefa_champs_league',
            'soccer_mexico_ligamx', 'baseball_ncaa'}
    return 'SOFT' if sport in soft else 'SHARP'

