"""
Cross-book arbitrage scanners.

Two channels:
  _scan_arbs            — game-line arbs (spreads / totals / ML across books)
  _prop_book_arb_scan   — player-prop arbs (sharp FD/BR vs soft DK/BetMGM/...)

Both produce pick dicts ready to feed into the merge stage.

Extracted from main.py in v26.0 Phase 8.

Re-exported from main for back-compat.
"""
from datetime import datetime


def _american_to_decimal(odds):
    """American odds → decimal multiplier (excluding stake). Returns 1.0 for zero/None."""
    if odds is None:
        return 1.0
    if odds > 0:
        return 1 + odds / 100
    if odds < 0:
        return 1 + 100 / abs(odds)
    return 1.0


def _scan_arbs(conn, min_margin=0.0, max_margin=5.0):
    """Scan today's odds for cross-book arbitrage opportunities.
    Returns a formatted string for the caption email, or '' if none found.
    min_margin: minimum arb % to report (0.0 = any arb)
    max_margin: cap to filter stale lines (>5% is almost always stale)
    """
    from collections import defaultdict
    from datetime import datetime

    today = datetime.now().strftime('%Y-%m-%d')
    now_utc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    arbs = []

    for market_type in ['totals', 'spreads', 'h2h']:
        rows = conn.execute("""
            SELECT event_id, home, away, sport, selection, book, odds, line, commence_time
            FROM odds
            WHERE snapshot_date = ?
            AND market = ?
            AND book IN ('DraftKings','FanDuel','BetMGM','Caesars','BetRivers',
                         'Fanatics','ESPN BET','PointsBet')
        """, (today, market_type)).fetchall()

        events = defaultdict(lambda: {'side1': [], 'side2': [], 'home': '', 'away': '',
                                       'sport': '', 'commence': ''})

        for eid, home, away, sport, sel, book, odds, line, commence in rows:
            if market_type == 'totals':
                key = (eid, line)
                events[key]['home'] = home
                events[key]['away'] = away
                events[key]['sport'] = sport
                events[key]['commence'] = commence or ''
                if 'Over' in sel:
                    events[key]['side1'].append((book, odds, f'OVER {line}'))
                elif 'Under' in sel:
                    events[key]['side2'].append((book, odds, f'UNDER {line}'))
            elif market_type == 'spreads':
                key = (eid, line)
                events[key]['home'] = home
                events[key]['away'] = away
                events[key]['sport'] = sport
                events[key]['commence'] = commence or ''
                if home and home in sel:
                    events[key]['side1'].append((book, odds, f'{home} {line:+.1f}'))
                elif away and away in sel:
                    events[key]['side2'].append((book, odds, f'{away} {line:+.1f}'))
            elif market_type == 'h2h':
                key = eid
                events[key]['home'] = home
                events[key]['away'] = away
                events[key]['sport'] = sport
                events[key]['commence'] = commence or ''
                if home and home in sel:
                    events[key]['side1'].append((book, odds, home))
                elif away and away in sel:
                    events[key]['side2'].append((book, odds, away))

        for key, data in events.items():
            if not data['side1'] or not data['side2']:
                continue

            # Only look at games that haven't started
            if data['commence'] and data['commence'] < now_utc:
                continue

            best1 = max(data['side1'], key=lambda x: x[1])
            best2 = max(data['side2'], key=lambda x: x[1])

            dec1 = _american_to_decimal(best1[1])
            dec2 = _american_to_decimal(best2[1])
            total_implied = 1 / dec1 + 1 / dec2
            margin = (1 - total_implied) * 100

            if min_margin <= margin <= max_margin:
                arbs.append({
                    'game': f"{data['away']}@{data['home']}",
                    'sport': data['sport'],
                    'market': market_type,
                    'side1': f"{best1[2]}: {best1[1]:+.0f} ({best1[0]})",
                    'side2': f"{best2[2]}: {best2[1]:+.0f} ({best2[0]})",
                    'margin': margin,
                    'commence': data['commence'],
                })

    if not arbs:
        return ''

    arbs.sort(key=lambda x: -x['margin'])

    lines = [f"\n\nARB OPPORTUNITIES ({len(arbs)} found)\n{'='*40}"]
    for a in arbs[:10]:  # Top 10
        sport_short = a['sport'].split('_')[-1] if a['sport'] else '?'
        lines.append(f"\n  {a['game']} ({sport_short}) — {a['margin']:+.2f}% arb")
        lines.append(f"    {a['side1']}")
        lines.append(f"    {a['side2']}")

    if len(arbs) > 10:
        lines.append(f"\n  ... and {len(arbs) - 10} more")

    lines.append(f"\n  Note: verify lines are still live before placing. Arbs close fast.")

    return '\n'.join(lines)


# ═══════════════════════════════════════════════════════════════════
# RUN — Full picks pipeline (11:00 AM / 5:30 PM EST)
# ═══════════════════════════════════════════════════════════════════



def _prop_book_arb_scan(conn, existing_eids=None):
    """v25.31: scan for player props where sharp (FD/BR) and soft (DK/BetMGM/Caesars/
    Fanatics/ESPN BET) books disagree on the line by enough to indicate inefficiency.

    Fire on the SOFT side at the SOFT book — bet direction follows which book is
    offering the softer/easier number. Excludes Bovada, BetOnline, BetUS, MyBookie,
    LowVig (per EXCLUDED_BOOKS). Applies MIN_ODDS (-150) and dedupes against
    existing prop picks (model + Option C flips) by (event_id, player, stat).
    """
    from datetime import datetime, timezone, timedelta
    from config import MIN_ODDS as _PBA_MIN_ODDS
    import statistics as _stats
    existing_eids = existing_eids or set()
    SHARP = {'FanDuel', 'BetRivers'}
    SOFT = {'DraftKings', 'BetMGM', 'Caesars', 'Fanatics', 'ESPN BET'}
    EXCLUDED = {'Bovada', 'BetOnline.ag', 'BetUS', 'MyBookie.ag', 'LowVig.ag'}
    # v25.34: thresholds tightened per user decision (2026-04-19). First-day
    # live sim showed 1.5+ pt gap went 2W-1L while 1.0 gap went 1W-4L. Raised
    # all minimums ~1.5× to eliminate the narrow-gap-loser tier. Revisit after
    # 2 weeks of live data if wins are getting cut off.
    THRESHOLDS = {
        # NBA + NHL (was 1.5 / 1.0 / 0.5 respectively)
        'player_points': 2.0, 'player_assists': 1.5, 'player_rebounds': 1.5,
        'player_threes': 1.0, 'player_blocks': 1.0, 'player_steals': 1.0,
        'player_shots_on_goal': 1.0,
        # MLB pitcher (was 1.0)
        'pitcher_strikeouts': 1.5,
        # MLB batter stats EXCLUDED. Per main-line audit 2026-04-20:
        #   - batter_rbis, batter_runs_scored: no sharp book posts them
        #   - batter_hits: both BetRivers and DK post main line at 0.5 >98% of
        #     the time; real arb fires ≈ 1 per 14 days (backtest verified).
        #   - batter_total_bases: same pattern + lacks 2B/3B data for grading.
        # NBA combo — v25.37 SHADOW MODE (2026-04-20). Detect arb candidates,
        # log to shadow_blocked_picks, DO NOT fire live bets. Revisit after
        # 2 weeks of snapshot accumulation for a proper backtest.
        'player_points_rebounds_assists': 2.5,
    }
    SHADOW_ONLY_MARKETS = {'player_points_rebounds_assists'}
    STAT_LABEL = {
        'player_points': 'POINTS', 'player_assists': 'ASSISTS', 'player_rebounds': 'REBOUNDS',
        'player_threes': 'THREES', 'player_blocks': 'BLOCKS', 'player_steals': 'STEALS',
        'player_shots_on_goal': 'SOG',
        'player_points_rebounds_assists': 'PRA',
        'batter_runs_scored': 'RUNS', 'batter_rbis': 'RBI', 'batter_total_bases': 'TOTAL BASES',
        'batter_hits': 'HITS', 'pitcher_strikeouts': 'STRIKEOUTS',
    }
    picks_out = []

    # Pull latest prop_snapshots per (event_id, player, market, book, side, line) for today
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    rows = conn.execute("""
        SELECT sport, event_id, commence_time, home, away, book, market, player, side, line, odds
        FROM prop_snapshots
        WHERE DATE(captured_at) = ?
          AND captured_at = (
              SELECT MAX(captured_at) FROM prop_snapshots p2
              WHERE p2.event_id = prop_snapshots.event_id
                AND p2.player = prop_snapshots.player
                AND p2.book = prop_snapshots.book
                AND p2.market = prop_snapshots.market
                AND p2.side = prop_snapshots.side
                AND p2.line = prop_snapshots.line
                AND DATE(p2.captured_at) = ?
          )
    """, (today, today)).fetchall()

    # Organize by (event_id, player, market, side) then by book -> lines[]
    from collections import defaultdict
    try:
        from zoneinfo import ZoneInfo
        _ET = ZoneInfo('America/New_York')
    except Exception:
        _ET = None
    _today_et = datetime.now(_ET).strftime('%Y-%m-%d') if _ET else datetime.now().strftime('%Y-%m-%d')
    by_group = defaultdict(lambda: defaultdict(list))  # (eid,player,market,side) -> book -> [(line, odds)]
    meta = {}  # (eid,player,market,side) -> (sport, commence, home, away)
    for sport, eid, commence, home, away, book, market, player, side, line, odds in rows:
        if book in EXCLUDED: continue
        # v25.33: only fire on TODAY's games in ET
        if commence and _ET:
            try:
                _dt_et = datetime.fromisoformat(commence.replace('Z','+00:00')).astimezone(_ET)
                if _dt_et.strftime('%Y-%m-%d') != _today_et:
                    continue  # game not today in ET
            except Exception:
                pass
        key = (eid, player, market, side)
        by_group[key][book].append((line, odds))
        meta[key] = (sport, commence, home, away)

    # For each group, find sharp/soft book disagreement
    fired = set()  # (eid, player, market) — fire only one direction per player/stat
    for key, book_map in by_group.items():
        eid, player, market, side = key
        if (eid, player, market) in fired: continue
        threshold = THRESHOLDS.get(market)
        if threshold is None: continue
        sport, commence, home, away = meta[key]

        # Collapse per-book alternates: pick the entry whose odds are closest to -110
        # (the standard "main line" pricing). This prevents a book's alternate-line
        # menu (0.5, 1.5, 2.5, ...) from falsely looking like a "sharp" signal at 1.5.
        def collapse(book, entries):
            if not entries: return None
            return min(entries, key=lambda e: abs((e[1] if e[1] is not None else -110) - (-110)))

        sharp_lines = []  # [(book, line, odds)]
        soft_lines = []
        for book, entries in book_map.items():
            col = collapse(book, entries)
            if not col: continue
            if book in SHARP:
                sharp_lines.append((book, col[0], col[1]))
            elif book in SOFT:
                soft_lines.append((book, col[0], col[1]))

        if not sharp_lines or not soft_lines:
            continue

        sharp_median = _stats.median([x[1] for x in sharp_lines])

        # Find the soft book with the biggest gap vs sharp median, in the favorable
        # direction for THIS side (OVER wants soft < sharp; UNDER wants soft > sharp)
        # Upper gap cap: gap > 2 × threshold usually = alternate-line pollution
        # (sharp books didn't post a main line, only high/low alternates, skewing
        # the median). Backtest 4/4-4/17 showed gap > 2×thr went 1-6 (14% WR, -4.1u)
        # while gap <= 2×thr went 18-3 (85.7%, +13.4u at -110). Conservative skip.
        UPPER_CAP_MULT = 2.0
        upper = threshold * UPPER_CAP_MULT

        best_soft = None  # (book, line, odds, gap)
        for sb, sl, so in soft_lines:
            gap = sl - sharp_median
            # If we're on OVER side and soft line < sharp → OVER at soft is easier
            # If we're on UNDER side and soft line > sharp → UNDER at soft is easier
            favorable = (side == 'Over' and gap < 0) or (side == 'Under' and gap > 0)
            if not favorable: continue
            if abs(gap) < threshold: continue
            if abs(gap) > upper:
                # Alternate-line pollution protection — skip this soft book
                continue
            if best_soft is None or abs(gap) > abs(best_soft[3]):
                best_soft = (sb, sl, so, gap)
        if not best_soft:
            continue
        soft_book, soft_line, soft_odds, gap = best_soft

        # MIN_ODDS safety (favorite floor) + MAX prop-odds cap (v25.36).
        # The projection engine uses MAX_PROP_ODDS=140 (player_prop_model.py).
        # Mirror that here so BOOK_ARB respects the same +140 ceiling — no
        # plus-money longshots slip through the arb scanner.
        if soft_odds is None or soft_odds <= _PBA_MIN_ODDS or soft_odds > 140:
            continue

        # Dedup: skip if model or Option C already has a pick for this event/player/stat
        dedup_key = (eid, player, market)
        if eid in existing_eids:
            continue
        if dedup_key in fired:
            continue
        fired.add(dedup_key)

        label = STAT_LABEL.get(market, market.upper())
        sel = f"{player} {'OVER' if side == 'Over' else 'UNDER'} {soft_line} {label}"
        reason = (f"PROP_BOOK_ARB — Sharp median {sharp_median:.1f} vs {soft_book} {soft_line} "
                  f"(gap {gap:+.1f}). Betting {side.upper()} at {soft_book} — easier number on sharp's side.")

        # v25.37: SHADOW_ONLY markets log the candidate but do NOT fire a live
        # pick. Morning agent reads shadow_blocked_picks + grades counterfactual
        # to decide when to promote from shadow to live.
        if market in SHADOW_ONLY_MARKETS:
            _shadow_reason = (f"PROP_BOOK_ARB_SHADOW ({market}, {sel[:40]}, "
                              f"sharp_med={sharp_median:.1f}, soft={soft_line}, gap={gap:+.1f}, "
                              f"book={soft_book}, odds={soft_odds:+.0f})")
            try:
                conn.execute("""INSERT INTO shadow_blocked_picks
                    (created_at, sport, event_id, selection, market_type, book,
                     line, odds, edge_pct, units, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (datetime.now().isoformat(), sport, eid, sel, 'PROP',
                     soft_book, soft_line, soft_odds, round(abs(gap) * 5.0, 1),
                     5.0, _shadow_reason))
                conn.commit()
                print(f"  👁 PROP_BOOK_ARB_SHADOW: {sel[:50]} @ {soft_book} {soft_odds:+.0f} | gap={gap:+.1f}")
            except Exception as _se:
                print(f"  ⚠ shadow log failed: {_se}")
            continue  # do NOT add to picks_out

        pick = {
            'sport': sport, 'event_id': eid, 'commence': commence,
            'home': home, 'away': away,
            'market_type': 'PROP', 'selection': sel,
            'book': soft_book, 'line': soft_line, 'odds': soft_odds,
            'model_spread': None,
            'model_prob': 0, 'implied_prob': 0,
            'edge_pct': round(abs(gap) * 5.0, 1),
            'star_rating': 3, 'units': 5.0,
            'confidence': 'BOOK_ARB', 'spread_or_ml': 'PROP',
            'timing': 'STANDARD',
            'notes': reason,
            'context': reason,
            'side_type': 'PROP_BOOK_ARB',
            '_signals': {
                'sharp_median': sharp_median, 'soft_line': soft_line,
                'gap': gap, 'book_count_sharp': len(sharp_lines), 'book_count_soft': len(soft_lines),
            },
            '_source': 'PROP_BOOK_ARB',
        }
        picks_out.append(pick)
        print(f"  💡 PROP_BOOK_ARB: {sel} @ {soft_book} {soft_odds:+.0f} | sharp_med={sharp_median:.1f} soft={soft_line} gap={gap:+.1f}")

    # Volume cap: max 3 prop arb picks per run. Keeps highest-gap signals only,
    # prevents prop arb from bloating slate volume past the v24 "fewer, higher
    # quality" target (~7 picks/day). Revisit if first 20 live arbs go positive.
    MAX_PROP_ARB_PER_RUN = 3
    if len(picks_out) > MAX_PROP_ARB_PER_RUN:
        picks_out.sort(key=lambda p: abs(p['_signals']['gap']), reverse=True)
        _dropped = picks_out[MAX_PROP_ARB_PER_RUN:]
        picks_out = picks_out[:MAX_PROP_ARB_PER_RUN]
        # v25.34: log volume-capped picks to shadow_blocked_picks so the morning
        # agent can monitor what the cap is filtering. If dropped picks outperform
        # the kept ones over 2+ weeks, revisit the cap.
        try:
            for _dp in _dropped:
                _gap = _dp['_signals']['gap']
                conn.execute("""
                    INSERT INTO shadow_blocked_picks (created_at, sport, event_id,
                        selection, market_type, book, line, odds, edge_pct, units, reason)
                    VALUES (?, ?, ?, ?, 'PROP', ?, ?, ?, ?, ?, ?)
                """, (datetime.now(timezone.utc).isoformat(), _dp['sport'], _dp['event_id'],
                      _dp['selection'], _dp['book'], _dp['line'], _dp['odds'],
                      _dp['edge_pct'], _dp['units'],
                      f"PROP_BOOK_ARB_VOLUME_CAP (gap={_gap:+.1f}, dropped in favor of top {MAX_PROP_ARB_PER_RUN})"))
            conn.commit()
        except Exception:
            pass
        print(f"  ⚠ PROP_BOOK_ARB volume cap: kept top {MAX_PROP_ARB_PER_RUN} by gap, dropped {len(_dropped)} (logged to shadow)")
    return picks_out


