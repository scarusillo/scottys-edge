"""
Stage 5 — Route + merge.

Merges game-line picks and prop picks coming out of the per-game scoring
loop into the final pick list that gets saved + emailed + posted.

Pipeline (in order):
  1. _passes_filter — per-pick gates (edge floor by book, MIN_BOOKS, NCAAB
     totals block, soccer spread floor, etc.). v25.34: bypasses for
     BOOK_ARB / DATA_SPREAD / DATA_TOTAL / FADE_FLIP channels which carry
     their own thresholds.
  2. Game-line cap: max 6 sharp-market picks per run + per-sport-soft cap.
  3. Soft markets: dedup + DIRECTION_CAP + TOTAL_SOFT_CAP + per-sport cap.
  4. Sharp markets: top 6 by star_rating × edge_pct (overflow → SHARP_CAP).
  5. Re-inject Context (DATA_SPREAD/DATA_TOTAL) — bypass sharp/soft caps,
     subject to per-sport daily cap (v25.67) and concentration cap.
  6. GAME_CAP — max 1 game-line pick per event (highest edge wins).
  7. Props: lower unit floor (2u), per-event cap (PROP_EVENT_CAP),
     game-cap, then merged in.

Side effects: writes a row per blocked pick to shadow_blocked_picks for
post-hoc tracking (the same table gates write to).

Extracted from main.py in v26.0 Phase 5. Behavior is byte-equivalent to the
pre-refactor inline version; verified by tests/shadow_predict.py.
"""

def merge_and_select(game_picks, prop_picks, conn=None):
    """
    Merge game and prop picks. Apply market-tier selection:

    SOFT markets (NCAAB, MLS, Bundesliga, Ligue 1, Serie A, UCL):
      → Priority in pick selection — fill card with these first
      → Lower edge threshold already applied in generate_predictions

    SHARP markets (NBA, NHL, EPL, La Liga):
      → Max 2 picks per run — only the absolute best edges
      → Higher edge threshold already applied in generate_predictions

    No contradictory sides (can't bet both teams in same game).
    """
    from scottys_edge import SOFT_MARKETS, SHARP_MARKETS
    from datetime import datetime

    MAX_SHARP_PICKS = 6   # v17: Was 4 — NHL can have 5+ puck lines on heavy nights (20W-9L +25.3u)
    
    # ── Filter: Only highest conviction picks make the card ──
    # v24: Unified 20% edge floor across all books.
    # Data (season): Below-cap (15-20%) at soft books was 22W-13L +22.1u historically,
    # but weekend analysis showed 5W-5L -4.8u drag vs at-cap 18W-14L +11.5u.
    # Removing below-cap entirely for cleaner signals.
    # Sharp books at 20%+ are 52W-39L +32.7u (7.2% ROI) — optimal threshold.
    SOFT_BOOKS = {'FanDuel', 'Fanatics', 'Caesars'}
    SHARP_BOOKS = {'DraftKings', 'BetRivers', 'BetMGM', 'ESPN BET', 'PointsBet'}
    SOFT_BOOK_MIN_EDGE = {
        'TOTAL': 20.0,
        'SPREAD': 20.0,
        'MONEYLINE': 20.0,
    }
    SHARP_BOOK_MIN_EDGE = {
        'TOTAL': 20.0,
        'SPREAD': 20.0,
        'MONEYLINE': 20.0,
    }
    BASEBALL_TOTAL_MIN_EDGE = 20.0  # v24: Unified with all other markets
    # Soccer spreads: backtest profitable at 5%+ edge across EPL (+21% ROI),
    # Ligue 1 (+27% ROI). Soccer point values are inherently smaller
    # (spreads ±0.25 to ±1.5 vs basketball ±3 to ±15), so 13% threshold
    # was blocking proven edges. 5% captures the signal without noise.
    SOCCER_SPREAD_MIN_EDGE = 5.0
    # MLS spreads: backtest 11W-8L +3.5u +4.4% ROI overall, but
    # 10-15% edge bucket is 3W-0L +8.6u (100%). 15%+ is 7W-7L break-even.
    # Only allow the profitable sweet spot.
    MLS_SPREAD_MIN_EDGE = 10.0
    MLS_SPREAD_MAX_EDGE = 15.0
    # v25: Soccer totals aligned to 20% like all other markets.
    # Old 5% floor let through 16-19% edge picks that went 1W-2L.
    SOCCER_TOTAL_MIN_EDGE = 20.0
    # v24: All books at 20%+ which Kelly-sizes to 4-5u naturally.
    MIN_UNITS = 3.0  # Keep 3.0u floor for Kelly-scaled picks
    REQUIRED_CONFIDENCE = ('ELITE', 'HIGH')  # ELITE + HIGH only
    MIN_BOOKS = 3  # v23: Minimum books carrying the game. Oklahoma St had only 2 — thin market noise.

    # Cache book counts per event to avoid repeated DB queries
    _book_count_cache = {}

    def _get_book_count(event_id, market):
        """Count distinct books carrying this event/market in the odds table."""
        cache_key = f"{event_id}|{market}"
        if cache_key in _book_count_cache:
            return _book_count_cache[cache_key]
        count = 0
        if conn:
            try:
                row = conn.execute("""
                    SELECT COUNT(DISTINCT book) FROM odds
                    WHERE event_id = ? AND market = ?
                """, (event_id, market)).fetchone()
                count = row[0] if row else 0
            except Exception:
                pass
        _book_count_cache[cache_key] = count
        return count

    def _passes_filter(p):
        # v25.34: BOOK_ARB + PROP_BOOK_ARB bypass the model-driven edge/confidence
        # filter. Their scanners apply their own thresholds (gap >= per-stat
        # threshold, alternate-line-pollution cap at 2× threshold, MIN_ODDS,
        # dedup). Game-line BOOK_ARB already bypasses this filter by being
        # appended to all_picks AFTER _merge_and_select runs (main.py:884, 1168).
        # PROP_BOOK_ARB was instead being fed THROUGH _merge_and_select via
        # prop_picks, where edge_pct=|gap|*5.0 (5–15%) and confidence='BOOK_ARB'
        # failed both the 20% edge floor and the ELITE/HIGH confidence check,
        # killing every prop arb pick silently. This unblocks prop arb to fire
        # on the same footing as game-line arb.
        # v25.59: DATA_TOTAL added to bypass. Same rationale as DATA_SPREAD —
        # Context CONTEXT_STANDALONE picks have their own threshold gate in model_engine
        # (sport-specific disagreement), and edge_pct=0 by construction.
        # Without this bypass, every DATA_TOTAL pick was silently blocked
        # here (line 3098 edge_pct check). Today's 148 CONTEXT_STANDALONE logs never
        # became live picks because of this.
        if p.get('side_type') in ('BOOK_ARB', 'PROP_BOOK_ARB', 'SPREAD_FADE_FLIP',
                                    'DATA_SPREAD', 'DATA_TOTAL', 'PROP_FADE_FLIP',
                                    'FADE_FLIP', 'PROP_CAREER_FADE'):
            return True
        mtype = p.get('market_type', 'SPREAD')
        sport = p.get('sport', '')
        book = p.get('book', '')

        # v26.0 Phase 3: extracted to pipeline.gates.gate_thin_market_block.
        from pipeline.gates import gate_thin_market_block
        if gate_thin_market_block(p, _get_book_count, MIN_BOOKS):
            return False

        # v24: Unified 20% edge floor for all books
        # BetMGM: 22% floor — 16-22% bucket was 10W-14L -27.5u, 22%+ is 11W-8L +11.7u
        if book == 'BetMGM':
            min_edge = 22.0
        elif book in SOFT_BOOKS:
            min_edge = SOFT_BOOK_MIN_EDGE.get(mtype, 20.0)
        else:
            min_edge = SHARP_BOOK_MIN_EDGE.get(mtype, 20.0)
        # v26.0 Phase 3: extracted to pipeline.gates.gate_soccer_spread_block.
        from pipeline.gates import gate_soccer_spread_block
        if gate_soccer_spread_block(p):
            return False
        # Soccer totals: backtest 92W-62L +104u at 59.7% — the real edge
        # v23.1: Respect book tiers — sharp books (BetMGM etc) still need 20%.
        # Live soccer is 1W-3L -11.3u; don't let 5% floor bypass sharp-book gate.
        if mtype == 'TOTAL' and 'soccer' in sport:
            min_edge = max(min_edge, SOCCER_TOTAL_MIN_EDGE if book in SOFT_BOOKS else 20.0)
        # v24: Baseball totals unified at 20% for all books
        # But respect BetMGM's higher floor (22%)
        if mtype == 'TOTAL' and 'baseball' in sport:
            min_edge = max(min_edge, BASEBALL_TOTAL_MIN_EDGE)
        # Walters ML: Elo-backed moneyline picks — unified at 20%
        if mtype == 'MONEYLINE' and 'Elo' in str(p.get('context', '')):
            min_edge = max(min_edge, 20.0)
        
        # Early bets by sport (post-rebuild):
        #   Baseball EARLY: 18W-7L +41.6u — lines settle early, no surcharge needed
        #   NHL EARLY: 5W-2L +9.2u — same, no surcharge
        #   NBA EARLY: 2W-1L +2.1u — small sample but OK
        #   NCAAB EARLY: 1W-3L -11.0u — already hard-blocked above
        # Only apply surcharge to sports where early bets are unproven/losing.
        timing = p.get('timing', 'EARLY')
        if timing == 'EARLY' and 'soccer' not in sport:
            # Baseball + NHL + Tennis exempt — early lines are reliable in these sports
            if 'baseball' not in sport and 'hockey' not in sport and 'tennis' not in sport:
                min_edge += 5.0

        # v25.6 (4/10/2026): Friday surcharge REMOVED.
        # The v21 surcharge added +3% to min_edge on Fridays based on a
        # 3W-7L early sample (-24u). But the model edge cap is 20%, so
        # min_edge=23% is mathematically IMPOSSIBLE to satisfy. The
        # surcharge silently killed 100% of NCAA baseball Friday picks.
        # 14-day backtest of 257 NCAA games (v25.4) showed Friday UNDERs
        # at 38-20 (66% win rate, +51.9u) — Friday is actually the BEST
        # UNDER day, not the worst. The original 3W-7L data was a small
        # sample mirage.
        # Verified 4/10 Friday: 7 OVERs + 4 UNDERs all at edge=17-20%,
        # all blocked by required=23%. Removing the surcharge unblocks
        # them. The v25.4 removal of the v22 outright Friday block was
        # incomplete because this surcharge was a SECOND Friday filter
        # I didn't know about.

        # v25: MLB midweek gate REMOVED. The -0.5 DOW adjustment in pitcher_scraper
        # was inflating edges on midweek games, then the gate blocked them all.
        # Now: no DOW adjustment + no gate = let the 20% edge threshold decide.
        # Picks with real edge from pitching/power ratings will fire normally.

        _min_u = MIN_UNITS
        _edge = p.get('edge_pct', 0)
        _units = p.get('units', 0)

        # v25.80 CLV_MICRO_EDGE (LIVE): fire picks at 13-20% edge when the
        # consensus line has already moved >= 0.5 since opener (either
        # direction — TOWARD us OR mild reversal AGAINST). The FLAT bucket
        # (|move| < 0.25) remains excluded as the historical bleed zone.
        #   Signal rationale (2026-04-23 analysis):
        #     16-18% edge has 45.2% POS CLV rate vs 27-30% at 20%+ edges.
        #     13-20% + |move|>=0.5 historical cohort: 51 picks, +9.3u.
        #   Borderline (0.25 <= |move| < 0.5) still shadow-logs as
        #   CLV_MICRO_EDGE_BORDERLINE for forward tracking; does NOT fire.
        #   Stake is forced to 5u per user spec (vs Kelly-scaled lower).
        _clv_micro_edge_active = False
        if (conn is not None
                and _edge < min_edge and 13.0 <= _edge < 20.0
                and p.get('market_type') in ('SPREAD', 'TOTAL')):
            try:
                from pipeline.score_helpers import compute_opener_move_for_pick as _compute_opener_move_for_pick
                _opener_move = _compute_opener_move_for_pick(conn, p)
                if _opener_move is not None and abs(_opener_move) >= 0.5:
                    _clv_micro_edge_active = True
                    # Force 5u stake for consistency with full-edge picks
                    p['units'] = 5.0
                    _units = 5.0
                    _dir = 'TOWARD' if _opener_move > 0 else 'AGAINST'
                    _tag = f' | CLV_MICRO_EDGE (edge={_edge:.1f}%, pre_move={_opener_move:+.2f} {_dir})'
                    p['context'] = (p.get('context', '') or '') + _tag
                elif _opener_move is not None and abs(_opener_move) >= 0.25 and _units >= _min_u:
                    # Borderline — shadow-log for forward tracking, do not fire
                    from datetime import datetime as _dt
                    _dir = 'TOWARD' if _opener_move > 0 else 'AGAINST'
                    conn.execute("""
                        INSERT INTO shadow_blocked_picks (created_at, sport, event_id,
                            selection, market_type, book, line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (_dt.now().isoformat(), p.get('sport'), p.get('event_id'),
                          p.get('selection'), p.get('market_type'), p.get('book'),
                          p.get('line'), p.get('odds'), _edge, _units,
                          f'CLV_MICRO_EDGE_BORDERLINE (edge={_edge:.1f}%, pre_move={_opener_move:+.2f} {_dir})'))
                    conn.commit()
            except Exception:
                pass

        if not (_units >= _min_u and (_edge >= min_edge or _clv_micro_edge_active)):
            return False
        # v26.0 Phase 3: confidence + soft-market context extracted.
        from pipeline.gates import gate_confidence, gate_soft_market_context
        if gate_confidence(p, REQUIRED_CONFIDENCE):
            return False
        if gate_soft_market_context(p, SOFT_MARKETS):
            return False
        
        # v26.0 Phase 3: extracted to pipeline.gates.gate_elo_only_ml_soft_market.
        from pipeline.gates import gate_elo_only_ml_soft_market
        if gate_elo_only_ml_soft_market(p, SHARP_MARKETS):
            return False

        # v26.0 Phase 3: heavy fav / heavy dog / NHL puck juice gates extracted.
        from pipeline.gates import (gate_heavy_favorite_ml, gate_heavy_dog_ml,
                                    gate_nhl_puck_line_juice)
        odds = p.get('odds', -110)
        if gate_heavy_favorite_ml(p):
            return False
        if gate_heavy_dog_ml(p):
            return False
        # Small dog stake cap (+100..+150): not a gate, just a stake mutation.
        if mtype == 'MONEYLINE' and odds > 0:
            p['units'] = min(p.get('units', 5.0), 4.5)
        if gate_nhl_puck_line_juice(p):
            return False

        # v26.0 Phase 3: extracted to pipeline.gates.gate_nhl_spread_dog_elo_floor.
        from pipeline.gates import gate_nhl_spread_dog_elo_floor
        if gate_nhl_spread_dog_elo_floor(p, conn):
            return False
        sel = p.get('selection', '')
        line = p.get('line')

        # v26.0 Phase 3: NCAAB blocks extracted to pipeline.gates.
        from pipeline.gates import gate_early_ncaab_block, gate_ncaab_totals_block
        if gate_early_ncaab_block(p):
            return False
        if gate_ncaab_totals_block(p):
            return False

        # v26.0 Phase 3: extracted to pipeline.gates.gate_dog_spread_graduated_edge.
        from pipeline.gates import gate_dog_spread_graduated_edge
        if gate_dog_spread_graduated_edge(p, SHARP_MARKETS):
            return False

        # v26.0 Phase 3: extracted to pipeline.gates.gate_baseball_pace_pitching_conflict.
        from pipeline.gates import gate_baseball_pace_pitching_conflict
        if gate_baseball_pace_pitching_conflict(p, conn):
            return False

        # v26.0 Phase 3: extracted to pipeline.gates.gate_clv_block.
        from pipeline.gates import gate_clv_block
        if gate_clv_block(p, conn):
            return False

        # v26.0 Phase 3: extracted to pipeline.gates.gate_sharp_opposes_block.
        from pipeline.gates import gate_sharp_opposes_block
        if gate_sharp_opposes_block(p, conn):
            return False

        # v26.0 Phase 3: HARD_VETO_DK_NCAA_BB_UNDERS extracted to pipeline.gates.
        # Same gate, same log format, same block behavior.
        from pipeline.gates import gate_dk_ncaa_bb_unders_veto
        if gate_dk_ncaa_bb_unders_veto(p, conn):
            return False

        # v26.0 Phase 3: extracted to pipeline.gates.gate_context_direction_veto.
        from pipeline.gates import gate_context_direction_veto
        if gate_context_direction_veto(p, conn):
            return False

        # v26.0 Phase 3: extracted to pipeline.gates.gate_line_against.
        from pipeline.gates import gate_line_against
        if gate_line_against(p, conn, clv_micro_edge_active=_clv_micro_edge_active):
            return False

        return True

    game_filtered = [p for p in game_picks if _passes_filter(p)]
    
    # ── Exclude offshore/non-legal books from recommendations ──
    # Data from these books is still used for consensus/edge calculations,
    # but we don't recommend bets on books the user can't access.
    EXCLUDED_BOOKS = {'Bovada', 'BetOnline.ag', 'BetUS', 'MyBookie.ag', 'LowVig.ag'}
    game_filtered = [p for p in game_filtered if p.get('book') not in EXCLUDED_BOOKS]
    
    # ── v25.51: Context Model picks (ELO_DIVERGENCE_RESCUE + CONTEXT_STANDALONE) bypass the soft/sharp
    # merge cap. They have their own threshold-based gate (disagreement must
    # exceed sport-specific bar), so the MAX_SHARP_PICKS=6 cap shouldn't also
    # compete them against each other. BOOK_ARB/PROP_BOOK_ARB already bypass
    # merge entirely via post-merge append; Context picks go through merge
    # because we want the concentration cap + direction cap checks, but we
    # don't want them to push out each other under the sharp cap.
    CONTEXT_BYPASS_SIDES = {'DATA_SPREAD', 'DATA_TOTAL'}
    context_bypass = [p for p in game_filtered if p.get('side_type') in CONTEXT_BYPASS_SIDES]
    game_filtered_for_cap = [p for p in game_filtered if p.get('side_type') not in CONTEXT_BYPASS_SIDES]

    # ── Split into soft and sharp ──
    soft_picks = [p for p in game_filtered_for_cap if p.get('sport') in SOFT_MARKETS]
    sharp_picks = [p for p in game_filtered_for_cap if p.get('sport') in SHARP_MARKETS]

    # Sort each tier by edge quality
    soft_picks.sort(key=lambda x: x['star_rating']*100 + x['edge_pct'], reverse=True)
    sharp_picks.sort(key=lambda x: x['star_rating']*100 + x['edge_pct'], reverse=True)

    # ── Dedup within each tier ──
    def _dedup(picks):
        seen_em = {}
        game_sides = {}
        # v12 FIX: Track spread/ML per team — only keep the better edge
        # Iowa State +7.5 AND Iowa State ML is redundant. Keep whichever has more edge.
        team_game_best = {}  # key: "event_id|team" → best pick
        deduped = []
        for p in picks:
            key = f"{p['event_id']}|{p['market_type']}"
            if key in seen_em:
                continue
            if p['market_type'] in ('SPREAD', 'MONEYLINE'):
                eid = p['event_id']
                sel = p['selection']
                home = p.get('home', '')
                away = p.get('away', '')
                pick_side = None
                pick_team = None
                if home and home in sel:
                    pick_side = 'home'
                    pick_team = home
                elif away and away in sel:
                    pick_side = 'away'
                    pick_team = away
                # Block contradictory sides (can't bet both teams)
                if pick_side and eid in game_sides:
                    if game_sides[eid] != pick_side:
                        continue
                elif pick_side:
                    game_sides[eid] = pick_side
                # Block same-team spread+ML — keep only the higher edge
                if pick_team:
                    team_key = f"{eid}|{pick_team}"
                    existing = team_game_best.get(team_key)
                    if existing:
                        # Already have a pick for this team — keep the better one
                        if p.get('edge_pct', 0) > existing.get('edge_pct', 0):
                            # New pick is better — remove old, add new
                            deduped.remove(existing)
                            team_game_best[team_key] = p
                        else:
                            # Existing pick is better — skip this one
                            continue
                    else:
                        team_game_best[team_key] = p
            seen_em[key] = True
            deduped.append(p)
        return deduped

    soft_deduped = _dedup(soft_picks)
    sharp_deduped = _dedup(sharp_picks)

    # ── Apply caps ──
    # Soft markets: cap at 10 total, max 5 per sport (prevents NCAAB flooding)
    # v21: Same-direction cap — max 4 per sport per direction (OVER/UNDER/DOG/FAV)
    # Prevents all-under or all-over loading on a single sport.
    MAX_SOFT_PICKS = 10
    MAX_PER_SPORT_SOFT = 5
    MAX_PER_SPORT_DIRECTION = 4  # v21: 7 unders on 3/29 showed directional risk

    # Pre-load today's existing bets to count across runs
    _existing_dir_counts = {}  # key: "sport|direction" → count
    if conn is not None:
        try:
            _today = datetime.now().strftime('%Y-%m-%d')
            _today_bets = conn.execute("""
                SELECT sport, side_type FROM bets
                WHERE DATE(created_at) = ? AND units >= 3.5
            """, (_today,)).fetchall()
            for _sp, _side in _today_bets:
                _dir_key = f"{_sp}|{_side or ''}"
                _existing_dir_counts[_dir_key] = _existing_dir_counts.get(_dir_key, 0) + 1
        except Exception:
            pass

    def _infer_side(p):
        """Infer side_type from pick data (picks don't have side_type until saved)."""
        mtype = p.get('market_type', '')
        sel = p.get('selection', '')
        line = p.get('line', 0)
        if mtype == 'TOTAL':
            return 'OVER' if 'OVER' in sel.upper() else 'UNDER'
        elif mtype == 'SPREAD':
            return 'DOG' if (line or 0) > 0 else 'FAVORITE'
        elif mtype == 'MONEYLINE':
            odds = p.get('odds', -110)
            return 'DOG' if odds > 0 else 'FAVORITE'
        return p.get('side_type', '')

    _shadow_blocked = []  # Track all cap-blocked picks for performance monitoring

    sport_soft_counts = {}
    sport_dir_counts = dict(_existing_dir_counts)  # Start from existing bets
    soft_final = []
    for p in soft_deduped:
        sp = p.get('sport', '')
        side = _infer_side(p)
        dir_key = f"{sp}|{side}"
        if sport_soft_counts.get(sp, 0) >= MAX_PER_SPORT_SOFT:
            _shadow_blocked.append((p, 'SPORT_CAP'))
            continue
        if sport_dir_counts.get(dir_key, 0) >= MAX_PER_SPORT_DIRECTION:
            print(f"  DIRECTION CAP: skipped {p['selection'][:50]} — already have {sport_dir_counts[dir_key]} {side} picks for {sp}")
            _shadow_blocked.append((p, 'DIRECTION_CAP'))
            continue
        if len(soft_final) >= MAX_SOFT_PICKS:
            _shadow_blocked.append((p, 'TOTAL_SOFT_CAP'))
            continue
        sport_soft_counts[sp] = sport_soft_counts.get(sp, 0) + 1
        sport_dir_counts[dir_key] = sport_dir_counts.get(dir_key, 0) + 1
        soft_final.append(p)
    
    # Sharp markets: cap of 4, best edges across NBA/NHL/EPL/La Liga
    sharp_final = sharp_deduped[:MAX_SHARP_PICKS]
    for p in sharp_deduped[MAX_SHARP_PICKS:]:
        _shadow_blocked.append((p, 'SHARP_CAP'))
    
    # ── Merge: soft first, then sharp ──
    game_final = soft_final + sharp_final

    # v25.51: Re-inject Context Model picks (DATA_SPREAD / DATA_TOTAL). They
    # bypass the sharp/soft cap above (Context picks shouldn't compete with
    # each other for 6 slots — they pass their own disagreement-threshold
    # gate in model_engine). They still go through the concentration cap
    # below so same-event collisions are resolved normally.
    #
    # v25.67 (2026-04-22): Per-sport daily cap on Context CONTEXT_STANDALONE picks.
    # This morning's pipeline fired 9 MLS Context OVERs in a single run —
    # v25.65 direction-rules handle most of that via BLOCK/SHADOW, but a
    # busy slate in a profitable direction (e.g. Serie A UNDER) could still
    # pile up picks with heavy correlation risk. Cap keeps exposure bounded.
    MAX_CONTEXT_PER_SPORT_DAILY = 5

    # Count today's already-placed Context picks per sport (from prior
    # runs in the same day — main.py dedup fires multiple times per day).
    _existing_ctx_by_sport = {}
    try:
        _today = datetime.now().strftime('%Y-%m-%d')
        for _sp_row in conn.execute("""
            SELECT sport, COUNT(*) FROM bets
            WHERE DATE(created_at) = ?
              AND side_type IN ('DATA_SPREAD','DATA_TOTAL')
              AND units >= 3.5
            GROUP BY sport
        """, (_today,)).fetchall():
            _existing_ctx_by_sport[_sp_row[0]] = _sp_row[1]
    except Exception:
        pass

    # Sort context_bypass by disagreement magnitude so the cap keeps the
    # highest-conviction picks when capped. Context picks tag disagreement
    # in the context/notes string — extract a rough conviction score.
    import re as _re_ctx
    def _ctx_conviction(p):
        _ctx_str = p.get('context') or p.get('notes') or ''
        m = _re_ctx.search(r'disagreement=?\s*([+-]?\d+\.?\d*)', _ctx_str)
        if m:
            try:
                return abs(float(m.group(1)))
            except Exception:
                pass
        m = _re_ctx.search(r'gap=?\s*([+-]?\d+\.?\d*)', _ctx_str)
        if m:
            try:
                return abs(float(m.group(1)))
            except Exception:
                pass
        return 0
    context_bypass.sort(key=_ctx_conviction, reverse=True)

    _ctx_counts = dict(_existing_ctx_by_sport)
    for _ctx_pick in context_bypass:
        _sp_ctx = _ctx_pick.get('sport', '')
        if _ctx_counts.get(_sp_ctx, 0) >= MAX_CONTEXT_PER_SPORT_DAILY:
            _shadow_blocked.append((_ctx_pick, 'CONTEXT_DAILY_SPORT_CAP'))
            continue
        _cp_edge = _ctx_pick.get('edge_pct', 0) or 0
        if _cp_edge == 0:
            _ctx_pick['_ctx_rank'] = 1
        _ctx_counts[_sp_ctx] = _ctx_counts.get(_sp_ctx, 0) + 1
        game_final.append(_ctx_pick)

    # v17: Per-game concentration cap — max 1 pick per event (spreads/totals/ML)
    # Stacking spread + total on the same game creates correlated risk.
    # If both hit, great. If the game goes sideways, you lose 2x on one event.
    # Keep only the highest-edge pick per event. Props exempt (different players).
    game_event_best = {}
    game_capped = []
    for p in game_final:
        eid = p.get('event_id', '')
        existing = game_event_best.get(eid)
        if existing:
            # Already have a pick on this game — keep the higher edge
            if p.get('edge_pct', 0) > existing.get('edge_pct', 0):
                game_capped.remove(existing)
                _shadow_blocked.append((existing, 'GAME_CAP'))
                game_event_best[eid] = p
                game_capped.append(p)
                print(f"  CONCENTRATION CAP: kept {p['selection'][:40]} over {existing['selection'][:40]} (same game)")
            else:
                _shadow_blocked.append((p, 'GAME_CAP'))
                print(f"  CONCENTRATION CAP: skipped {p['selection'][:40]} — already have {existing['selection'][:40]} on this game")
        else:
            game_event_best[eid] = p
            game_capped.append(p)
    game_final = game_capped

    # ── Props: lower unit floor (plus-money odds produce lower Kelly) ──
    # A 10% edge at +150 gives ~1.5u Kelly. 3.0u filter kills all props.
    # 2.0u minimum still filters weak edges while letting real props through.
    PROP_MIN_UNITS = 2.0
    PROP_MIN_EDGE = 8.0
    
    # GUARDRAIL: Higher minimum edge for threes/low-line props
    # Plus-money odds amplify small consensus disagreements into large "edges"
    # A 5% raw disagreement at +170 looks like 13%+ edge — need higher bar
    PROP_MIN_EDGE_THREES = 12.0  # Threes, SOG, shots — low-line plus-money markets
    LOW_LINE_MARKETS = {'player_threes', 'player_shots_on_goal', 'player_power_play_points',
                        'player_blocked_shots', 'player_blocks', 'player_steals',
                        'player_shots', 'player_shots_on_target'}
    
    def _get_prop_market_key(p):
        """Extract the API market key from a prop pick's selection."""
        sel = p.get('selection', '')
        # Map display labels back to market keys
        label_map = {
            'THREES': 'player_threes', 'POINTS': 'player_points',
            'REBOUNDS': 'player_rebounds', 'ASSISTS': 'player_assists',
            'BLOCKS': 'player_blocks', 'STEALS': 'player_steals',
            'SOG': 'player_shots_on_goal', 'PPP': 'player_power_play_points',
            'BLK_SHOTS': 'player_blocked_shots',
            'SHOTS': 'player_shots', 'SOT': 'player_shots_on_target',
        }
        for label, key in label_map.items():
            if label in sel:
                return key
        return 'unknown'
    
    def _get_prop_team(p):
        """Determine which team a player belongs to (best guess from selection + game info)."""
        # We can't know for sure, but we use the selection text
        # The prop engine doesn't track team affiliation, so we return a generic key
        sel = p.get('selection', '')
        # Extract player name (everything before OVER/UNDER)
        for word in ['OVER', 'UNDER', 'Over', 'Under']:
            if word in sel:
                return sel.split(word)[0].strip()
        return sel
    
    # ── Prop filters ──
    # 1. UNDER enable (v25.17, 4/14): UNDERs fire now that player_prop_model has
    #    proper UNDER infrastructure (hit-rate blend, cross-book, batting order).
    #    The old "OVER only" filter was based on a 3/23 backtest against a
    #    pre-v25 engine and is no longer valid.
    # 2. No FanDuel — 2W-13L (-50.8u). FanDuel lines look like edges but aren't.
    # 3. Book count filter REMOVED v24 — was counting game-level books, not prop-level.
    #    Model builds own projection from box scores; book count is irrelevant.
    #    Was blocking Wembanyama 21.8% block edges because the GAME had 7+ books.
    # 4. No medium dog odds (+151 to +250) — 3W-15L (-49.8u).
    PROP_EXCLUDED_RECS = {'FanDuel'}  # Still use for consensus calc, never recommend
    # Manual scrubs — specific picks the user has explicitly removed for today.
    # Format: tuple of (player_substring, market_substring, commence_date_YYYYMMDD).
    MANUAL_PROP_SCRUBS = {
        ('Thomas Harley', 'SOG', '2026-04-15'),
    }
    prop_filtered = []
    for p in (prop_picks or []):
        if p.get('units', 0) < PROP_MIN_UNITS:
            continue
        # Manual scrub check
        _scrub_sel = (p.get('selection') or '').upper()
        _scrub_commence = (p.get('commence') or '')[:10].replace('-', '')
        _scrub_date = _scrub_commence[:4] + '-' + _scrub_commence[4:6] + '-' + _scrub_commence[6:8] if len(_scrub_commence) >= 8 else ''
        if any(ps[0].upper() in _scrub_sel and ps[1].upper() in _scrub_sel and ps[2] == _scrub_date for ps in MANUAL_PROP_SCRUBS):
            continue
        market_key = _get_prop_market_key(p)
        min_edge = PROP_MIN_EDGE_THREES if market_key in LOW_LINE_MARKETS else PROP_MIN_EDGE
        # BetMGM 22% floor applies to props too
        if p.get('book', '') == 'BetMGM':
            min_edge = max(min_edge, 22.0)
        if p.get('edge_pct', 0) < min_edge:
            continue
        if p.get('book', '') in PROP_EXCLUDED_RECS:
            continue
        # Filter: Block high odds props — cap at +200
        # v24: Was +151-250 dead zone (from March 23 consensus engine data).
        # Most binary 0.5 props (RBI, blocks) are +150-200 naturally.
        # Old filter killed every prop. Now: allow up to +200, block +201+.
        odds = p.get('odds', -110)
        if odds > 200:
            continue
        prop_filtered.append(p)
    
    prop_filtered.sort(key=lambda x: x['star_rating']*100 + x['edge_pct'], reverse=True)
    
    # Dedup: same player + same stat type = keep best
    seen_props = set()
    prop_deduped = []
    for p in prop_filtered:
        sel = p['selection']
        parts = sel.split()
        dedup_key = p['event_id'] + '|'
        for i, part in enumerate(parts):
            if part in ('OVER', 'UNDER'):
                dedup_key += ' '.join(parts[:i]) + '|' + parts[-1]
                break
        if dedup_key in seen_props:
            continue
        seen_props.add(dedup_key)
        prop_deduped.append(p)
    
    # GUARDRAIL: Per-team prop cap (max 1 prop per team)
    # v24: Changed from per-game to per-team. Two Pirates RBI overs = same
    # lineup risk (if Pirates get shut out, both lose). But a Pirates batter
    # + Padres pitcher K on the same game is fine — independent outcomes.
    MAX_PROPS_PER_TEAM = 1
    team_prop_counts = {}  # key: team name → count
    prop_team_capped = []
    for p in prop_deduped:
        sel = p.get('selection', '')
        # Get player's team from the pick data
        player_team = None
        if p.get('home') and p.get('away'):
            # Determine which team the player is on from box_scores
            player_name = sel.split(' OVER ')[0].strip() if ' OVER ' in sel else sel
            if conn:
                _t = conn.execute("""
                    SELECT team FROM box_scores WHERE player = ?
                    AND sport = ? ORDER BY game_date DESC LIMIT 1
                """, (player_name, p.get('sport', ''))).fetchone()
                if _t:
                    player_team = _t[0]

        if player_team:
            if team_prop_counts.get(player_team, 0) >= MAX_PROPS_PER_TEAM:
                _shadow_blocked.append((p, 'PROP_TEAM_CAP'))
                continue
            team_prop_counts[player_team] = team_prop_counts.get(player_team, 0) + 1

        prop_team_capped.append(p)

    # v25.1: Same-event prop cap — max 1 prop per event_id across all teams.
    # Gorman (Cardinals) + Abrams (Nationals) = same game, different teams,
    # but both lost = -10u correlated swing. Per-team cap missed this.
    MAX_PROPS_PER_EVENT = 1
    event_prop_counts = {}
    # Count existing same-event props from earlier runs today
    if conn:
        try:
            for _ee in conn.execute("""
                SELECT event_id FROM bets
                WHERE market_type = 'PROP' AND DATE(created_at) = DATE('now') AND units >= 3.5
            """).fetchall():
                event_prop_counts[_ee[0]] = event_prop_counts.get(_ee[0], 0) + 1
        except Exception:
            pass
    prop_event_capped = []
    for p in prop_team_capped:
        eid = p['event_id']
        if event_prop_counts.get(eid, 0) >= MAX_PROPS_PER_EVENT:
            _shadow_blocked.append((p, 'PROP_EVENT_CAP'))
            continue
        event_prop_counts[eid] = event_prop_counts.get(eid, 0) + 1
        prop_event_capped.append(p)
    prop_game_capped = prop_event_capped

    # GUARDRAIL: Per-stat-type cap per game (max 2 per stat type per game)
    # Allows one from each team but prevents 4 three-point unders from same game.
    # User requested: different teams in same game are OK.
    MAX_SAME_STAT_PER_GAME = 2
    stat_game_counts = {}
    prop_final = []
    for p in prop_game_capped:
        eid = p['event_id']
        market_key = _get_prop_market_key(p)
        stat_game_key = f"{eid}|{market_key}"
        if stat_game_counts.get(stat_game_key, 0) >= MAX_SAME_STAT_PER_GAME:
            _shadow_blocked.append((p, 'PROP_STAT_CAP'))
            continue
        stat_game_counts[stat_game_key] = stat_game_counts.get(stat_game_key, 0) + 1
        prop_final.append(p)

    # ── Correlated bet reduction ──
    # When we have multiple picks on the same game (e.g., spread + total),
    # the bets are ~60% correlated. Reduce the lower-edge pick's units by 25%
    # to account for the extra risk of correlated exposure.
    game_pick_groups = {}
    for p in game_final:
        eid = p.get('event_id', '')
        if eid not in game_pick_groups:
            game_pick_groups[eid] = []
        game_pick_groups[eid].append(p)

    for eid, group in game_pick_groups.items():
        if len(group) > 1:
            # Sort by edge — keep the best pick at full size, reduce others
            group.sort(key=lambda x: x.get('edge_pct', 0), reverse=True)
            for p in group[1:]:
                old_units = p['units']
                p['units'] = max(0.5, round((old_units * 0.75) * 2) / 2)  # 25% reduction, round to 0.5

    # ── v23.2: Same-game prop conflict filter ──
    # Block props that contradict a game-line total on the same matchup.
    # e.g., Yelich RBI OVER shouldn't fire alongside Brewers UNDER.
    if game_final and prop_final:
        import re as _re
        # Build set of (event_id, direction) from game totals
        game_total_directions = {}
        for gp in game_final:
            if gp.get('market_type') == 'TOTAL':
                eid = gp.get('event_id', '')
                sel = gp.get('selection', '')
                if 'OVER' in sel:
                    game_total_directions[eid] = 'OVER'
                elif 'UNDER' in sel:
                    game_total_directions[eid] = 'UNDER'

        if game_total_directions:
            filtered_props = []
            for pp in prop_final:
                eid = pp.get('event_id', '')
                sel = pp.get('selection', '')
                game_dir = game_total_directions.get(eid)
                if game_dir:
                    # OVER props (hits, runs, RBIs, points) contradict game UNDER
                    # UNDER props contradict game OVER
                    prop_dir = 'OVER' if 'OVER' in sel else 'UNDER' if 'UNDER' in sel else None
                    if prop_dir and prop_dir != game_dir:
                        print(f"    ⚠ Prop conflict blocked: {sel} ({prop_dir}) vs game-line {game_dir}")
                        continue
                filtered_props.append(pp)
            prop_final = filtered_props

    # ── Final merge (no cap) ──
    all_picks = game_final + prop_final

    soft_count = sum(1 for p in all_picks if p.get('sport') in SOFT_MARKETS and p['market_type'] != 'PROP')
    sharp_count = sum(1 for p in all_picks if p.get('sport') in SHARP_MARKETS and p['market_type'] != 'PROP')
    prop_count = sum(1 for p in all_picks if p['market_type'] == 'PROP')
    print(f"\n  Selected: {len(all_picks)} picks ({soft_count} soft mkt, {sharp_count} sharp mkt, {prop_count} props)")
    if sharp_count:
        sharp_sports = set(p['sport'].replace('basketball_','').replace('icehockey_','').replace('soccer_','').upper()
                          for p in all_picks if p.get('sport') in SHARP_MARKETS and p['market_type'] != 'PROP')
        print(f"    Sharp market picks ({sharp_count}/{MAX_SHARP_PICKS} max): {', '.join(sharp_sports)}")

    # ── Shadow log: save all cap-blocked picks for performance tracking ──
    if _shadow_blocked and conn is not None:
        try:
            conn.execute("""CREATE TABLE IF NOT EXISTS shadow_blocked_picks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT, sport TEXT, event_id TEXT, selection TEXT,
                market_type TEXT, book TEXT, line REAL, odds REAL,
                edge_pct REAL, units REAL, reason TEXT
            )""")
            for _bp, _reason in _shadow_blocked:
                conn.execute("""INSERT INTO shadow_blocked_picks
                    (created_at, sport, event_id, selection, market_type, book, line, odds, edge_pct, units, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (datetime.now().isoformat(), _bp.get('sport',''), _bp.get('event_id',''),
                     _bp.get('selection',''), _bp.get('market_type',''), _bp.get('book',''),
                     _bp.get('line'), _bp.get('odds'), _bp.get('edge_pct', 0),
                     _bp.get('units', 0), _reason))
            conn.commit()
            print(f"  Shadow log: {len(_shadow_blocked)} blocked picks saved for tracking")
        except Exception as _e:
            print(f"  Shadow log: {_e}")

    return all_picks


def _merge_prop_sources(consensus_props, model_props):
    """Merge consensus and projection model prop picks. Dedup by player+stat: keep higher edge."""
    if not model_props:
        return consensus_props or []
    if not consensus_props:
        return model_props or []
    # Index consensus by (event_id, player_key, stat_label)
    best = {}
    for p in consensus_props:
        sel = p.get('selection', '')
        parts = sel.split()
        # Key: event_id + first part of selection (player) + last part (stat label)
        dk = f"{p.get('event_id','')}|{parts[0] if parts else ''}|{parts[-1] if parts else ''}"
        if dk not in best or p.get('edge_pct', 0) > best[dk].get('edge_pct', 0):
            best[dk] = p
    for p in model_props:
        sel = p.get('selection', '')
        parts = sel.split()
        dk = f"{p.get('event_id','')}|{parts[0] if parts else ''}|{parts[-1] if parts else ''}"
        if dk not in best or p.get('edge_pct', 0) > best[dk].get('edge_pct', 0):
            best[dk] = p
    return list(best.values())


