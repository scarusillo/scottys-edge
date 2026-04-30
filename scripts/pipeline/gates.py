"""
Stage 3 — Gates (and supporting block-loggers).

Each gate is a predicate function:
    gate_xxx(pick, conn) -> bool

Returns True when the gate fires (pick should be blocked). The function is
also responsible for logging the block to `shadow_blocked_picks` (when
applicable) and emitting the operator-facing print line. The caller's job is
just to honor the return: if any gate returns True, drop the pick.

This module is grown gate-by-gate during the v26.0 refactor. Each gate
docstring covers:
  - When it fires (the boolean condition)
  - What it logs (reason text format, if any)
  - Backtest / motivation summary

Most gates do NOT log to shadow_blocked_picks — they're "trivial" filters
(thin markets, soccer spreads disabled, NCAAB totals disabled, etc.) where
the block is structural and uninteresting for analysis. Only meaningful
gates whose firings we want to track post-hoc (HARD_VETO, SHARP_OPPOSES,
LINE_AGAINST, PITCHING_GATE, CONTEXT_DIRECTION_VETO) write rows.
"""
from datetime import datetime


def log_divergence_block(conn, sport, event_id, home, away,
                         model_spread, market_spread, reason_detail):
    """Log a pick blocked by `max_spread_divergence` to shadow_blocked_picks.

    Divergence blocks fire BEFORE we know which bet type would have been
    generated, so `selection` just records the matchup. `reason_detail`
    explains which of the 3 divergence paths fired:
      - insufficient_elo_games (one team lacks enough Elo seasoning)
      - post_elo_rescue (post-blend divergence still > cap)
      - ml_only_implied (ML-only path took the divergence branch)

    Extracted from model_engine.py in v26.0 Phase 4.
    """
    try:
        div = (abs(model_spread - market_spread)
               if (model_spread is not None and market_spread is not None) else None)
        div_str = f"{div:.1f}" if div is not None else "?"
        ms_str = f"{model_spread:+.1f}" if model_spread is not None else "?"
        msp_str = f"{market_spread:+.1f}" if market_spread is not None else "?"
        conn.execute(
            """INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                market_type, line, odds, edge_pct, units, reason)
               VALUES (?, ?, ?, ?, 'SPREAD', NULL, NULL, NULL, NULL, ?)""",
            (datetime.now().isoformat(), sport, event_id, f"{home} vs {away}",
             f"DIVERGENCE_GATE ({reason_detail}, div={div_str}, "
             f"ms={ms_str}, mkt_sp={msp_str})"))
        conn.commit()
    except Exception:
        pass


def gate_dk_ncaa_bb_unders_veto(pick, conn):
    """v25.56 HARD_VETO_DK_NCAA_BB_UNDERS — surgical veto on a broken cohort.

    Fires when ALL of:
      - sport == 'baseball_ncaa'
      - book == 'DraftKings'
      - market_type == 'TOTAL'
      - selection contains 'UNDER'

    Why: post-rebuild DraftKings × NCAA Baseball UNDERs went 9W-18L (-51u, 33%
    WR on 27 picks). DK NCAA OVERs not vetoed (7-8 -10u, marginal juice bleed).
    Even with positive CLV the cohort still lost — structural miscalibration,
    not a pricing gap. Existing v25.22-24 DK gates didn't narrow it (post-gate
    record still 1-5 at -21u).

    Logs to `shadow_blocked_picks` with reason
    `HARD_VETO_DK_NCAA_BB_UNDERS (v25.56 — 9-18 -51u post-rebuild)`.

    Returns:
        True if the pick is blocked by this gate; False otherwise.
    """
    sport = pick.get('sport') or ''
    book = pick.get('book') or ''
    mtype = pick.get('market_type') or ''
    selection_upper = (pick.get('selection') or '').upper()

    if not (sport == 'baseball_ncaa'
            and book == 'DraftKings'
            and mtype == 'TOTAL'
            and 'UNDER' in selection_upper):
        return False

    if conn is not None:
        try:
            conn.execute(
                """INSERT INTO shadow_blocked_picks
                   (created_at, sport, event_id, selection, market_type, book,
                    line, odds, edge_pct, units, reason)
                   VALUES (?, ?, ?, ?, 'TOTAL', 'DraftKings', ?, ?, ?, ?, ?)""",
                (datetime.now().isoformat(), sport, pick.get('event_id', ''),
                 pick.get('selection', ''), pick.get('line'), pick.get('odds'),
                 pick.get('edge_pct', 0), pick.get('units', 0),
                 'HARD_VETO_DK_NCAA_BB_UNDERS (v25.56 — 9-18 -51u post-rebuild)'))
            conn.commit()
        except Exception:
            pass

    print(f"    🚫 HARD_VETO_DK_NCAA_BB_UNDERS: "
          f"{(pick.get('selection') or '')[:55]} — DK NCAA UNDERs 9-18 -51u post-rebuild")
    return True


# ─────────────────────────────────────────────────────────────────────────
# Batch 1: simple self-contained gates (no DB queries, no logging)
# ─────────────────────────────────────────────────────────────────────────


def gate_soccer_spread_block(pick):
    """v16: Block ALL soccer SPREAD picks — backtest 80W-86L -70u.

    Soccer totals (92W-62L +104u, 59.7%) and EPL/Ligue 1 spreads at small
    samples were profitable, but spreads as a whole bleed. Disabled outright
    until a sport-specific re-enable lands.
    """
    return (pick.get('market_type') == 'SPREAD'
            and 'soccer' in (pick.get('sport') or ''))


def gate_heavy_favorite_ml(pick):
    """Block ML picks at -300 or worse.

    Laying -300 risks $300 to win $100 — one loss wipes 3 wins. If the
    model likes a heavy favorite, the spread is the play instead.
    """
    return (pick.get('market_type') == 'MONEYLINE'
            and (pick.get('odds') or -110) <= -300)


def gate_heavy_dog_ml(pick):
    """v17: Block ML dogs at +151 or higher.

    Data: small dogs (+100..+150) 4W-1L +17.4u, 63% ROI. Mid dogs (+151..+200)
    0W-2L -10.0u. The +151 cliff is real — block dogs above it entirely.
    """
    return (pick.get('market_type') == 'MONEYLINE'
            and (pick.get('odds') or 0) >= 151)


def gate_nhl_puck_line_juice(pick):
    """v25: Block NHL spread picks priced -130 or worse.

    -130 and below: 15W-11L -11.3u (58% WR but avg odds need 64% to profit).
    -115 to -129: 5W-1L +15.5u. Juice eats all edge on heavy puck lines.
    """
    return (pick.get('market_type') == 'SPREAD'
            and 'hockey' in (pick.get('sport') or '')
            and (pick.get('odds') or -110) <= -130)


def gate_early_ncaab_block(pick):
    """Block early-timing NCAAB picks entirely.

    Data: early NCAAB is 4W-7L -33.9% ROI. Lines haven't settled. The 8%
    surcharge wasn't enough; outright block is the right call. Late NCAAB is
    where the value lives.
    """
    return (pick.get('timing', 'EARLY') == 'EARLY'
            and pick.get('sport') == 'basketball_ncaab')


def gate_ncaab_totals_block(pick):
    """Block ALL NCAAB total picks.

    Data: NCAAB totals 0W-3L -14u. Model has no signal — TOTAL_STD=22
    generates fake 18-35% edges. NCAAB spreads are profitable (late
    15W-8L +30.3u); totals are not. Disabled until the totals model is
    rebuilt for college basketball.
    """
    return (pick.get('market_type') == 'TOTAL'
            and pick.get('sport') == 'basketball_ncaab')


# ─────────────────────────────────────────────────────────────────────────
# Batch 2: gates with conn lookups or sport-tier dependencies
# ─────────────────────────────────────────────────────────────────────────


def gate_elo_only_ml_soft_market(pick, sharp_markets):
    """Block Elo-only ML picks in soft markets.

    Data: NCAAB Elo-only ML is 3W-4L -3.6u — cross-conference Elo breaks
    down. NBA/NHL Elo is better calibrated (larger samples, no conference
    issue). Block Elo-only ML for soft markets only. Sharp markets allowed.
    Baseball ML exempt (uses pitcher data path, not Elo-only).
    """
    if pick.get('market_type') != 'MONEYLINE':
        return False
    sport = pick.get('sport') or ''
    if 'baseball' in sport or sport in sharp_markets:
        return False
    ctx = str(pick.get('context', '') or '')
    return ctx.strip() == 'Elo probability edge'


def gate_nhl_spread_dog_elo_floor(pick, conn):
    """v21: Block NHL spread DOG picks when team Elo < 1475.

    Bottom ~5 NHL teams have 60%+ blowout rate when losing. Blackhawks
    (Elo 1431) went 1W-4L -15u as puck-line dog. Blocks all spread dogs
    (any +line) when team Elo is below the floor.
    """
    if pick.get('market_type') != 'SPREAD':
        return False
    if 'hockey' not in (pick.get('sport') or ''):
        return False
    line = pick.get('line')
    if line is None or line <= 0:
        return False
    sel = pick.get('selection') or ''
    team_name = sel.rsplit(' ', 1)[0].strip() if sel else ''
    if not team_name or conn is None:
        return False
    try:
        elo_row = conn.execute(
            "SELECT elo FROM elo_ratings WHERE sport='icehockey_nhl' AND team=?",
            (team_name,)).fetchone()
    except Exception:
        return False
    return bool(elo_row and elo_row[0] < 1475)


def gate_dog_spread_graduated_edge(pick, sharp_markets):
    """v12 graduated edge requirement for spread dogs (excludes tennis).

    Elo compresses, so the model systematically favors dogs. Different
    edge bars by line size + market tier:
      - Small dogs (line ≤ 3.5): soft markets need ≥ 20% edge; sharp markets
        keep normal threshold (already-required min_edge).
      - Med dogs (line ≤ 7.5): sharp need ≥ 15%, soft need ≥ 17%.
      - Big dogs (8+): no extra requirement (working at the standard floor).
    """
    if pick.get('market_type') != 'SPREAD':
        return False
    line = pick.get('line')
    if line is None or line <= 0:
        return False
    sport = pick.get('sport') or ''
    if 'tennis' in sport:
        return False
    is_sharp = sport in sharp_markets
    edge = pick.get('edge_pct') or 0
    if line <= 3.5:
        if not is_sharp and edge < 20.0:
            return True
    elif line <= 7.5:
        if is_sharp and edge < 15.0:
            return True
        if not is_sharp and edge < 17.0:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────
# Batch 3: DB-heavy gates (line lookups, steam signals, regex parsing)
# ─────────────────────────────────────────────────────────────────────────


def gate_clv_block(pick, conn):
    """v25.x CLV-aware filter: block when consensus line moved ≥ 1.5 against us.

    Data: positive CLV bets 16W-4L (76%); CLV > 1pt: 12W-0L. Negative CLV:
    1W-3L (25%). If opener exists and the line has moved 1.5+ pts against us,
    block — the market has already corrected and we're chasing a stale edge.

    ML CLV is intentionally skipped (odds-based, not line-based — different
    formula belongs in a separate gate).
    """
    if conn is None:
        return False
    import re as _re
    mtype = pick.get('market_type')
    eid = pick.get('event_id') or ''
    sel = pick.get('selection') or ''
    if not (eid and sel and mtype):
        return False
    try:
        if mtype == 'TOTAL':
            lm_sel = 'Over' if 'OVER' in sel.upper() else 'Under'
            lm_mkt = 'totals'
        elif mtype == 'SPREAD':
            lm_sel = _re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
            lm_mkt = 'spreads'
        else:
            lm_sel = sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
            lm_mkt = 'h2h'
        opener = conn.execute(
            """SELECT line, odds FROM openers
               WHERE event_id = ? AND market = ? AND selection LIKE ?
               ORDER BY timestamp ASC LIMIT 1""",
            (eid, lm_mkt, f'%{lm_sel}%')).fetchone()
        current = conn.execute(
            """SELECT line, odds FROM odds
               WHERE event_id = ? AND market = ? AND selection LIKE ?
               ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1""",
            (eid, lm_mkt, f'%{lm_sel}%')).fetchone()
        if not (opener and current and opener[0] is not None and current[0] is not None):
            return False
        if mtype == 'TOTAL':
            if 'OVER' in sel.upper():
                clv_move = current[0] - opener[0]  # rose = good for over
            else:
                clv_move = opener[0] - current[0]  # dropped = good for under
        elif mtype == 'SPREAD':
            clv_move = current[0] - opener[0]      # more points = good for dog
        else:
            return False  # ML skipped
        if clv_move <= -1.5:
            print(f"  ⚠ CLV BLOCK: {sel} — line moved {clv_move:+.1f} against us "
                  f"(opener={opener[0]}, now={current[0]})")
            return True
    except Exception:
        return False
    return False


def gate_nba_playoff_inseries_disagreement(pick, conn):
    """v25.91 NBA_PLAYOFF_INSERIES_GATE (SHADOW MODE) — log NBA Path 2 TOTAL
    picks where the in-series running total disagrees with bet direction.

    Motivation: Context Model is built on regular-season form factors and has
    no awareness of in-series playoff scoring trajectory. When the same two
    teams have already played 2+ games in a series, those games are the most
    informative signal we have — and the Context Model ignores them.

    Backtest 4/19-4/26 NBA Path 2 cohort (n=10):
      Original: 3W-6L-1T, -16.66u
      Would block 3 picks (2 LOSS + 1 already-TAINTED), recover +10u
      Cohort would become 3W-4L-1T, -6.66u
      Path 1 (game-line) backtested negative — Path 2 only.

    Rule: BLOCK if avg of last ≤5 same-matchup playoff games (since 4/15)
    is on the OPPOSITE side of the line from the bet direction.
    Requires n_prior >= 2 (single prior game is too noisy).

    SHADOW: logs to shadow_blocked_picks but does NOT block. Promote when
    n>=10 shadow logs validate the agreement pattern (~2 weeks).
    """
    if pick.get('sport') != 'basketball_nba': return False
    if pick.get('market_type') != 'TOTAL': return False
    cf = pick.get('context_factors') or pick.get('context') or ''
    if 'DATA_TOTAL' not in cf: return False
    if conn is None: return False

    line = pick.get('line')
    eid = pick.get('event_id') or ''
    sel = pick.get('selection') or ''
    if line is None or not eid:
        return False

    # Resolve home/away from the results-side companion to the picked event.
    # Pick payloads carry the matchup; if absent, fall back to results table.
    home = pick.get('home')
    away = pick.get('away')
    if not (home and away):
        try:
            row = conn.execute(
                "SELECT home, away FROM results WHERE event_id=? LIMIT 1", (eid,)
            ).fetchone()
            if row:
                home, away = row[0], row[1]
        except Exception:
            return False
    if not (home and away):
        return False

    try:
        prior = conn.execute(
            """SELECT home_score+away_score FROM results
               WHERE ((home=? AND away=?) OR (home=? AND away=?))
                 AND date(commence_time) < date('now')
                 AND date(commence_time) >= '2026-04-15'
                 AND home_score IS NOT NULL
               ORDER BY commence_time DESC LIMIT 5""",
            (home, away, away, home)
        ).fetchall()
    except Exception:
        return False

    if len(prior) < 2:
        return False  # need >= 2 prior games for a usable series average

    series_avg = sum(p[0] for p in prior) / len(prior)
    is_under = 'UNDER' in sel.upper()
    series_says_under = series_avg < line

    if series_says_under == is_under:
        return False  # series agrees with bet direction → pass

    # Disagreement → SHADOW log, but DO NOT block (return False)
    print(f"  📋 NBA_PLAYOFF_INSERIES_SHADOW: {sel[:55]} — "
          f"series avg {series_avg:.1f} vs line {line} ({len(prior)} prior games)")
    try:
        conn.execute(
            """INSERT INTO shadow_blocked_picks
               (created_at, sport, event_id, selection, market_type, book,
                line, odds, edge_pct, units, reason)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now().isoformat(), 'basketball_nba', eid, sel, 'TOTAL',
             pick.get('book', ''), line, pick.get('odds'),
             pick.get('edge_pct', 0), pick.get('units', 0),
             f'NBA_PLAYOFF_INSERIES_SHADOW (v25.91, '
             f'series_avg={series_avg:.1f}, line={line}, n_prior={len(prior)})'))
        conn.commit()
    except Exception:
        pass
    return False  # SHADOW — never blocks


SHARP_OPPOSES_BLOCK_SPORTS = {'icehockey_nhl', 'baseball_ncaa'}


def gate_sharp_opposes_block(pick, conn):
    """v25.35 SHARP_OPPOSES_BLOCK — block when opener→current moved against
    us past the steam-engine threshold for NHL or NCAA Baseball.

    Backtest post-Apr-1:
      NHL    17 picks, 8-9, -14.26u
      NCAA   5 picks, 1-3-1, -10.65u

    Catches movements smaller than CLV's -1.5 cutoff (NHL threshold 0.5,
    NCAA BB 1.0). MLB stays monitored only (50% hit, juice drag).
    """
    sport = pick.get('sport') or ''
    if sport not in SHARP_OPPOSES_BLOCK_SPORTS or conn is None:
        return False
    mtype = pick.get('market_type') or ''
    sel = pick.get('selection') or ''
    eid = pick.get('event_id') or ''
    line = pick.get('line')
    if line is None or not eid:
        return False
    if mtype == 'TOTAL':
        steam_side = 'OVER' if 'OVER' in sel.upper() else 'UNDER'
    elif mtype == 'SPREAD':
        st = (pick.get('side_type') or '').upper()
        if st in ('FAVORITE', 'DOG'):
            steam_side = st
        else:
            steam_side = 'FAVORITE' if line < 0 else 'DOG'
    else:
        return False
    try:
        from steam_engine import get_steam_signal
        sig, info = get_steam_signal(conn, sport, eid, mtype,
                                      steam_side, line, pick.get('odds'))
        if sig != 'SHARP_OPPOSES':
            return False
        mv = (info or {}).get('movement', 0)
        print(f"  ⚠ SHARP_OPPOSES_BLOCK: {sel[:55]} — line moved {mv:+.1f} against us")
        try:
            conn.execute(
                """INSERT INTO shadow_blocked_picks
                   (created_at, sport, event_id, selection, market_type, book,
                    line, odds, edge_pct, units, reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (datetime.now().isoformat(), sport, eid, sel, mtype,
                 pick.get('book', ''), line, pick.get('odds'),
                 pick.get('edge_pct', 0), pick.get('units', 0),
                 f'SHARP_OPPOSES_BLOCK ({sport}, move={mv:+.1f})'))
            conn.commit()
        except Exception:
            pass
        return True
    except Exception:
        return False


def gate_baseball_pace_pitching_conflict(pick, conn):
    """Session 3/23: block baseball totals where pace or pitching context
    contradicts the bet direction.

    Conflict 1 — pace: fast-paced (+) conflicts with UNDER; slow-paced (-)
    conflicts with OVER.
    Conflict 2 — pitching: pitching edge ≥ +0.5 conflicts with UNDER (weak
    pitcher allows runs); pitching edge ≤ -0.5 conflicts with OVER (suppresses).

    Block rules:
      1. Pitching ≥ 1.0 against bet direction → strong single conflict.
      2. Pace conflicts AND pitching doesn't support the bet (neutral or
         against) → block.

    Logs to shadow_blocked_picks as PITCHING_GATE (rule 1) or PACE_GATE
    (rule 2). Only applies to baseball totals — NHL/soccer have different
    dynamics.
    """
    if pick.get('market_type') != 'TOTAL' or 'baseball' not in (pick.get('sport') or ''):
        return False
    import re as _re
    ctx = str(pick.get('context', '') or '')
    sel = pick.get('selection', '') or ''
    is_over = 'OVER' in sel.upper()
    is_under = 'UNDER' in sel.upper()

    pace_match = _re.search(r'(fast|slow)-paced\s*\(([+-]?\d+\.?\d*)\)', ctx, _re.IGNORECASE)
    pace_conflicts = False
    if pace_match:
        pace_val = float(pace_match.group(2))
        if is_under and pace_val > 0:
            pace_conflicts = True
        elif is_over and pace_val < 0:
            pace_conflicts = True

    pitch_match = _re.search(r'Pitching edge:.*?\(([+-]?\d+\.?\d*)\s*pts?\)', ctx, _re.IGNORECASE)
    pitch_conflicts = False
    if pitch_match:
        pitch_val = float(pitch_match.group(1))
        if is_over and pitch_val <= -0.5:
            pitch_conflicts = True
        elif is_under and pitch_val >= 0.5:
            pitch_conflicts = True

    strong_pitch = (pitch_match and pitch_conflicts
                    and abs(float(pitch_match.group(1))) >= 1.0)

    pitch_supports_bet = False
    if pitch_match:
        pv = float(pitch_match.group(1))
        if is_under and pv <= -0.2:
            pitch_supports_bet = True
        elif is_over and pv >= 0.2:
            pitch_supports_bet = True

    pace_unsupported = pace_conflicts and not pitch_supports_bet
    if not (pace_unsupported or strong_pitch):
        return False

    conflict_type = []
    if pace_conflicts: conflict_type.append('pace')
    if pitch_conflicts: conflict_type.append('pitching')
    if pace_unsupported and not pitch_conflicts: conflict_type.append('no pitching support')
    print(f"    ⚠ BLOCKED: {sel[:50]} — signal conflict ({', '.join(conflict_type)} vs bet side)")
    if conn is not None:
        try:
            gate_name = 'PITCHING_GATE' if strong_pitch else 'PACE_GATE'
            conn.execute(
                """INSERT INTO shadow_blocked_picks
                   (created_at, sport, event_id, selection, market_type, book,
                    line, odds, edge_pct, units, reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (datetime.now().isoformat(), pick.get('sport', ''), pick.get('event_id', ''),
                 pick.get('selection', ''), pick.get('market_type', ''), pick.get('book', ''),
                 pick.get('line'), pick.get('odds'), pick.get('edge_pct', 0),
                 pick.get('units', 0),
                 f"{gate_name} (signal conflict: {', '.join(conflict_type)})"))
            conn.commit()
        except Exception:
            pass
    return True


# ─────────────────────────────────────────────────────────────────────────
# Batch 4: complex multi-condition gates
# ─────────────────────────────────────────────────────────────────────────


CONTEXT_VETO_SPORTS_TOTAL = {
    'basketball_nba', 'icehockey_nhl', 'baseball_mlb',
    'soccer_usa_mls', 'soccer_spain_la_liga',
    'soccer_germany_bundesliga', 'soccer_france_ligue_one',
}
CONTEXT_VETO_SPORTS_SPREAD = {
    'icehockey_nhl', 'basketball_nba', 'soccer_italy_serie_a',
}
CONTEXT_VETO_EXEMPT_SIDE_TYPES = {
    'SPREAD_FADE_FLIP', 'PROP_FADE_FLIP', 'DATA_SPREAD',
    'DATA_TOTAL', 'BOOK_ARB', 'PROP_BOOK_ARB', 'FADE_FLIP',
    'RAW_EDGE_FLIP',  # v25.95: already a Context-driven flip
    'MLB_ML_FADE_FLIP',  # v25.98: fades own model by design
}


def gate_context_direction_veto(pick, conn):
    """v25.52 CONTEXT_DIRECTION_VETO — block when Context Model direction
    disagrees with the pick.

    Context Model is the primary brain; edge-based picks defer on direction
    when the two conflict. Fade-flip / Context / arb picks are exempt
    (they intentionally bet against the model).

    30-day backtest on 116 eligible TOTAL/SPREAD picks:
      Context AGREES (81): 49-32 (60%) +36.92u
      Context DISAGREES (35, would be vetoed): 15-20 (43%) -41.24u
    Expected save: ~+41u per 30 days.

    Routes per market:
      - TOTAL (sport in CONTEXT_VETO_SPORTS_TOTAL):
        compute_context_total → over/under direction → block if pick disagrees.
      - SPREAD (sport in CONTEXT_VETO_SPORTS_SPREAD):
        compute_context_spread → home/away favorite direction → block if pick on
        opposite side.
    """
    if conn is None:
        return False
    side_type = pick.get('side_type') or ''
    if side_type in CONTEXT_VETO_EXEMPT_SIDE_TYPES:
        return False
    sport = pick.get('sport') or ''
    mtype = pick.get('market_type') or ''
    eid = pick.get('event_id') or ''
    sel = pick.get('selection') or ''
    line = pick.get('line')
    if not eid:
        return False

    try:
        mc = conn.execute(
            """SELECT home, away, commence_time, model_spread,
                      best_home_spread, best_over_total
               FROM market_consensus
               WHERE event_id=? AND tag='CURRENT' LIMIT 1""",
            (eid,)).fetchone()
        if not (mc and mc[0] and mc[1] and mc[2]):
            return False
        home, away, commence, ms, mkt_sp, mkt_tot = mc
        commence_date = commence[:10]

        ctx_disagrees = False
        ctx_reason = ''
        if mtype == 'TOTAL' and sport in CONTEXT_VETO_SPORTS_TOTAL and mkt_tot is not None:
            from context_spread_model import compute_context_total
            ctx_tot, _ = compute_context_total(conn, sport, home, away,
                                                eid, mkt_tot, commence_date)
            pick_side = 'OVER' if 'OVER' in sel.upper() else 'UNDER'
            ctx_side = ('OVER' if ctx_tot > mkt_tot else
                        ('UNDER' if ctx_tot < mkt_tot else pick_side))
            if pick_side != ctx_side:
                ctx_disagrees = True
                ctx_reason = (f'CONTEXT_DIRECTION_VETO (totals: pick {pick_side}, '
                              f'Context {ctx_side} — ctx_tot={ctx_tot:.2f} vs mkt={mkt_tot})')
        elif (mtype == 'SPREAD' and sport in CONTEXT_VETO_SPORTS_SPREAD
              and ms is not None and mkt_sp is not None):
            from context_spread_model import compute_context_spread
            ms_ctx, _ = compute_context_spread(conn, sport, home, away,
                                                 eid, ms, commence_date)
            ctx_fav_home = (ms_ctx < mkt_sp)
            pick_on_home = None
            if side_type == 'FAVORITE':
                pick_on_home = (mkt_sp < 0)
            elif side_type == 'DOG':
                pick_on_home = (mkt_sp > 0)
            else:
                pick_on_home = bool(home and home.split()[0] in sel.split()[0])
            if pick_on_home is not None and ctx_fav_home != pick_on_home:
                ctx_disagrees = True
                ctx_reason = (f'CONTEXT_DIRECTION_VETO (spread: pick on '
                              f'{"home" if pick_on_home else "away"}, Context favors '
                              f'{"home" if ctx_fav_home else "away"} — '
                              f'ms_ctx={ms_ctx:+.1f} vs mkt_sp={mkt_sp:+.1f})')
        if not ctx_disagrees:
            return False
        print(f"    🧠 CONTEXT_DIRECTION_VETO: {sel[:60]} — {ctx_reason}")
        try:
            conn.execute(
                """INSERT INTO shadow_blocked_picks
                   (created_at, sport, event_id, selection, market_type, book,
                    line, odds, edge_pct, units, reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (datetime.now().isoformat(), pick.get('sport', ''), pick.get('event_id', ''),
                 pick.get('selection', ''), pick.get('market_type', ''), pick.get('book', ''),
                 pick.get('line'), pick.get('odds'), pick.get('edge_pct', 0),
                 pick.get('units', 0), ctx_reason))
            conn.commit()
        except Exception:
            pass
        return True
    except Exception:
        return False  # Fail-open — pick still fires


# ─────────────────────────────────────────────────────────────────────────
# Batch 5: remaining gates (thin market, confidence, soft context, line-against)
# ─────────────────────────────────────────────────────────────────────────


def gate_thin_market_block(pick, get_book_count, min_books):
    """v23: Block when fewer than `min_books` post the relevant market.

    Thin markets produce fake edges — Oklahoma St had only 2 books carrying
    the line and showed a phantom 18% edge. The threshold is a knob; the
    function takes it as a parameter so callers can tune per-deployment.
    """
    mtype = pick.get('market_type', 'SPREAD')
    odds_market = {'SPREAD': 'spreads', 'TOTAL': 'totals',
                   'MONEYLINE': 'h2h'}.get(mtype, 'h2h')
    book_count = get_book_count(pick.get('event_id', ''), odds_market)
    return book_count < min_books


def gate_confidence(pick, required_confidence):
    """v23: Block picks that are not in the required confidence tier.

    ELITE + HIGH allowed. STRONG blocked (3W-7L -22.5u). Picks with
    `confidence` set to None are exempt — those come from non-tier-aware
    sources (e.g. arb scanners) and are filtered elsewhere.
    """
    conf = pick.get('confidence')
    if conf is None:
        return False
    return conf not in required_confidence


def gate_soft_market_context(pick, soft_markets):
    """v17: Soft markets need context confirmation OR a sport-specific high
    edge bar.

    Data: context-confirmed 42W-26L +54.2u (17.4% ROI). Raw model (no ctx):
    1W-2L -4.4u — no signal without context.

    Exceptions (skip the block when edge is high enough on a contextless pick):
      - March Madness (Mar 17–Apr 7 NCAAB): edge ≥ 20%
      - Soccer: edge ≥ 5% (lines are sharp-set globally; Elo is the signal)
      - Tennis: edge ≥ 20% (no B2B/rest/revenge — surface Elo is the signal)
    """
    sport = pick.get('sport') or ''
    if sport not in soft_markets:
        return False
    has_context = bool(pick.get('context', ''))
    if has_context:
        return False
    edge = pick.get('edge_pct') or 0
    ctx_min = 20.0  # v24: unified context gate
    if edge >= ctx_min:
        return False  # Edge alone is high enough to fire without context
    # Sport-specific exceptions when below the unified gate
    is_march_madness = (
        sport == 'basketball_ncaab'
        and (datetime.now().month == 3 or
             (datetime.now().month == 4 and datetime.now().day <= 7)))
    if is_march_madness and edge >= 20.0:
        return False
    if 'soccer' in sport and edge >= 5.0:
        return False
    if 'tennis' in sport and edge >= 20.0:
        return False
    return True


LINE_AGAINST_EXEMPT_SIDE_TYPES = {
    'SPREAD_FADE_FLIP', 'PROP_FADE_FLIP', 'DATA_SPREAD', 'DATA_TOTAL',
    'BOOK_ARB', 'PROP_BOOK_ARB', 'FADE_FLIP', 'PROP_CAREER_FADE',
}


def gate_line_against(pick, conn, clv_micro_edge_active=False):
    """v25.80 LINE_AGAINST_GATE — block 20%+ edge picks where the consensus
    line has already moved ≥ 0.5 AGAINST our side before fire.

    Historical bleed (2026-04-23 analysis): 47 picks, 21-24 (45% WR), -31.7u
    on SPREAD/TOTAL. Concentration: NCAA baseball -24.9u (29), DK -22.5u (10),
    Caesars -19.0u (8). 8/47 overlapped existing v25.35 SHARP_OPPOSES — this
    catches the rest.

    Exempt: fade-flip / Context / arb side types intentionally bet against
    market movement and have their own logic.

    Args:
        clv_micro_edge_active: when True, the pick is below 20% edge and being
        kept alive by CLV_MICRO_EDGE — already cleared the line-movement check
        upstream and shouldn't be re-blocked here.
    """
    if conn is None:
        return False
    if clv_micro_edge_active:
        return False
    edge = pick.get('edge_pct') or 0
    if edge < 20.0:
        return False
    if pick.get('market_type') not in ('SPREAD', 'TOTAL'):
        return False
    side_type = pick.get('side_type') or ''
    if side_type in LINE_AGAINST_EXEMPT_SIDE_TYPES:
        return False
    try:
        from pipeline.score_helpers import compute_opener_move_for_pick
        om = compute_opener_move_for_pick(conn, pick)
        if om is None or om > -0.5:
            return False
        detail = f'edge={edge:.1f}%, opener_move={om:+.2f}'
        try:
            conn.execute(
                """INSERT INTO shadow_blocked_picks (created_at, sport, event_id,
                    selection, market_type, book, line, odds, edge_pct, units,
                    reason, reason_category, reason_detail)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (datetime.now().isoformat(), pick.get('sport'), pick.get('event_id'),
                 pick.get('selection'), pick.get('market_type'), pick.get('book'),
                 pick.get('line'), pick.get('odds'), edge,
                 pick.get('units', 0),
                 f'LINE_AGAINST_GATE ({detail})',
                 'LINE_AGAINST_GATE', detail))
            conn.commit()
        except Exception:
            pass
        print(f"    🚫 LINE_AGAINST_GATE: {(pick.get('selection') or '')[:55]} "
              f"— pre-bet line moved {om:+.2f} against us")
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────
# v25.99 CLV_PREDICTOR_GATE — block bottom-decile predicted CLV picks +
# stake-boost top-decile predicted CLV picks.
#
# Backtest LOO over n=155 graded since 4/15:
#   block at predicted_clv <= -0.40: 42 picks blocked → +63.99u saved
#     (blocked cohort 36.6% WR, kept cohort 60.0% WR vs 52% sample-wide)
#   boost at predicted_clv >=  +0.80: 47 picks boosted (1.4×) → +8.15u extra
#   combined sample +8.81u → +80.95u (9× improvement)
# ─────────────────────────────────────────────────────────────────────────

CLV_BLOCK_THRESHOLD = -0.40
CLV_BOOST_THRESHOLD = 0.80
CLV_BOOST_MULTIPLIER = 1.4
CLV_MIN_TRAINING_N = 30

CLV_GATE_EXEMPT_SIDE_TYPES = {
    'BOOK_ARB', 'PROP_BOOK_ARB',
    'MLB_ML_FADE_FLIP',
}

_CLV_MODEL_CACHE = None


def _ensure_clv_model(conn):
    global _CLV_MODEL_CACHE
    if _CLV_MODEL_CACHE is None:
        try:
            from clv_model import load_training, fit
            rows = load_training(conn)
            if len(rows) < CLV_MIN_TRAINING_N:
                _CLV_MODEL_CACHE = (None, None)
                return None
            _CLV_MODEL_CACHE = fit(rows)
        except Exception:
            _CLV_MODEL_CACHE = (None, None)
            return None
    if _CLV_MODEL_CACHE[0] is None:
        return None
    return _CLV_MODEL_CACHE


def _score_clv(pick, conn):
    if pick.get('side_type') in CLV_GATE_EXEMPT_SIDE_TYPES:
        return None
    model = _ensure_clv_model(conn)
    if model is None:
        return None
    bl, devs = model
    try:
        from clv_model import predict, featurize
        pred, _ = predict(bl, devs, featurize(pick))
        pick['_clv_predicted'] = pred
        return pred
    except Exception:
        return None


def gate_clv_predictor_block(pick, conn):
    """Block when predicted_clv <= CLV_BLOCK_THRESHOLD."""
    pred = _score_clv(pick, conn)
    if pred is None:
        return False
    if pred > CLV_BLOCK_THRESHOLD:
        return False
    detail = f'predicted_clv={pred:+.2f} <= {CLV_BLOCK_THRESHOLD:+.2f}'
    try:
        conn.execute(
            """INSERT INTO shadow_blocked_picks (created_at, sport, event_id,
                selection, market_type, book, line, odds, edge_pct, units,
                reason, reason_category, reason_detail)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now().isoformat(), pick.get('sport'), pick.get('event_id'),
             pick.get('selection'), pick.get('market_type'), pick.get('book'),
             pick.get('line'), pick.get('odds'), pick.get('edge_pct', 0),
             pick.get('units', 0),
             f'CLV_PREDICTOR_BLOCK ({detail})',
             'CLV_PREDICTOR_BLOCK', detail))
        conn.commit()
    except Exception:
        pass
    print(f"    \U0001f6ab CLV_PREDICTOR_BLOCK: {(pick.get('selection') or '')[:55]} "
          f"({detail})")
    return True


def apply_clv_top_decile_boost(pick, conn):
    """Stake-boost picks whose predicted_clv >= CLV_BOOST_THRESHOLD."""
    if pick.get('_clv_boosted'):
        return
    pred = pick.get('_clv_predicted')
    if pred is None:
        pred = _score_clv(pick, conn)
    if pred is None or pred < CLV_BOOST_THRESHOLD:
        return
    original = pick.get('units', 0) or 0
    new_units = round(original * CLV_BOOST_MULTIPLIER, 1)
    if new_units > original:
        pick['units'] = new_units
        pick['_clv_boosted'] = True
        print(f"    \U0001f4c8 CLV_BOOST: {(pick.get('selection') or '')[:55]} "
              f"({original:.1f}u → {new_units:.1f}u, predicted_clv={pred:+.2f})")
