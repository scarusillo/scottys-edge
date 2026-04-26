"""
main.py v11 — Scotty's Edge Command Center

FULLY AUTOMATED 4-RUN DAILY SCHEDULE:
  8:00 AM EST  →  python main.py opener --email     📌 Capture opening lines (CLV baseline)
  11:00 AM EST →  python main.py run --email         🎯 Morning picks (NCAAB, soccer)
  5:30 PM EST  →  python main.py run --email         🎯 Afternoon picks (NBA, NHL)
  9:00 AM EST  →  python main.py grade --email       📊 Grade yesterday + CLV report

COMMANDS:
  python main.py run                    Full pipeline (all sports)
  python main.py run --sport nba        Single sport
  python main.py run --email            Run + email results
  python main.py opener                 Capture opening lines (for CLV)
  python main.py opener --email         Opener + email confirmation
  python main.py predict                Model only (FREE, no odds fetch)
  python main.py props                  Fetch + evaluate player props
  python main.py grade                  Grade yesterday's bets + CLV
  python main.py grade --email          Grade + email report
  python main.py report                 7-day performance
  python main.py report --days 30       30-day performance
  python main.py injuries               Scrape ESPN injuries (FREE)
  python main.py injuries --manual      Manual injury entry
  python main.py bootstrap              Rebuild ratings (FREE)
  python main.py reboot-ratings         Wipe + rebuild ratings
  python main.py budget                 API usage guide
  python main.py email-test             Test email setup
  python main.py setup-scheduler        Auto-schedule commands
  python main.py log                    View pick log summary
  python espn_debug.py                  Debug ESPN connection
"""
import sys, os, io, warnings

# Fix Windows console encoding — emojis in grader.py/reports crash cp1252
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Suppress SSL warnings
warnings.filterwarnings('ignore', message='.*Unverified HTTPS.*')

SPORT_MAP = {
    'nba': 'basketball_nba', 'ncaab': 'basketball_ncaab', 'cbb': 'basketball_ncaab',
    'nhl': 'icehockey_nhl', 'epl': 'soccer_epl',
    'seriea': 'soccer_italy_serie_a', 'serie_a': 'soccer_italy_serie_a',
    'laliga': 'soccer_spain_la_liga', 'la_liga': 'soccer_spain_la_liga',
    'bundesliga': 'soccer_germany_bundesliga', 'bund': 'soccer_germany_bundesliga',
    'ligue1': 'soccer_france_ligue_one', 'l1': 'soccer_france_ligue_one',
    'ucl': 'soccer_uefa_champs_league', 'champions': 'soccer_uefa_champs_league',
    'mls': 'soccer_usa_mls',
    'ligamx': 'soccer_mexico_ligamx', 'liga_mx': 'soccer_mexico_ligamx',
    'ncaa_baseball': 'baseball_ncaa', 'college_baseball': 'baseball_ncaa',
    'cbb_base': 'baseball_ncaa', 'ncaabb': 'baseball_ncaa',
    'mlb': 'baseball_mlb',
    # Tennis: special alias → triggers dynamic tournament detection
    'tennis': 'tennis_auto', 'atp': 'tennis_auto', 'wta': 'tennis_auto',
}
# v13: NCAA baseball RE-ENABLED. ESPN scoring now 100% (1585/1585 games, 0 phantom grades).
# Backtest: 26W-17L ATS (60.5%), +11.6u, +10.3% ROI. Smaller market = real edges.
# v16: Soccer re-enabled after full context rebuild (derby, UCL rotation, standings,
# referee, congestion, home/away splits). Divergence cap raised 0.75→1.0.
# ML still disabled in model_engine.py. MLS totals still disabled.
_DISABLED_SPORTS = set()
ALL_SPORTS = [s for s in set(SPORT_MAP.values()) if s not in _DISABLED_SPORTS and s != 'tennis_auto']
PROP_SPORTS = ['basketball_nba', 'icehockey_nhl', 'baseball_mlb']
# v12 FIX: Soccer props removed to save API budget. Soccer prop markets
# (shots, shots on target) are thinly covered by US books — rarely produce
# actionable edges. Saves ~150 usage per run × 2 runs = 300/day.

# No artificial caps — if the model finds edges, show them all.
# Sharp market cap stays because those markets are too efficient to bet heavily.
# No artificial caps — quality thresholds control output volume.
# Sharp market cap and per-sport cap are inside _merge_and_select.

# v26.0 Phase 8: main.py modularization — helpers moved to pipeline/.
from pipeline.validation import (  # noqa: F401
    _validate_picks,
)
from pipeline.email_html import (  # noqa: F401
    _generate_html_card,
    _generate_results_html,
    _social_media_card,
)
from pipeline.arb_scanner import (  # noqa: F401
    _scan_arbs,
    _prop_book_arb_scan,
)
from pipeline.sora_prompt import (  # noqa: F401
    _generate_kling_prompt,
)
from pipeline.stage_5_merge import _merge_prop_sources  # noqa: F401

# v26.0 Phase 8 (CLI): cmd_X commands moved to scripts/cli/.
from cli.data_capture import (  # noqa: F401
    cmd_opener,
    cmd_snapshot,
    cmd_props,
    cmd_run_soccer,
    cmd_scores,
    cmd_injuries,
)
from cli.admin import (  # noqa: F401
    cmd_bootstrap,
    cmd_reboot_ratings,
    cmd_email_test,
    cmd_budget,
    cmd_log,
    cmd_historical,
    cmd_elo,
    cmd_setup_scheduler,
    cmd_fix_names,
)
from cli.grading import (  # noqa: F401
    cmd_scrub,
    cmd_unscrub,
)
from cli.preview import (  # noqa: F401
    cmd_predict,
    cmd_report,
    cmd_backtest,
)

def get_sports(args):
    if '--sport' in args:
        i = args.index('--sport')
        if i+1 < len(args):
            sport_val = SPORT_MAP.get(args[i+1], args[i+1])
            if sport_val == 'tennis_auto':
                # Dynamic detection: find active tennis tournaments
                return _detect_tennis_sports()
            return [sport_val]
    return ALL_SPORTS


def _detect_tennis_sports():
    """Find which tennis tournaments are currently active on the Odds API."""
    try:
        from odds_api import detect_active_tennis
        active = detect_active_tennis()
        if active:
            print(f"  🎾 Active tennis tournaments: {', '.join(active)}")
            return active
        else:
            print("  🎾 No active tennis tournaments right now")
            return []
    except Exception as e:
        print(f"  ⚠ Tennis detection failed: {e}")
        return []

def has_flag(args, flag):
    return flag in args


# ═══════════════════════════════════════════════════════════════════
# OPENER — Capture opening lines (8:00 AM EST)
# ═══════════════════════════════════════════════════════════════════

def _american_to_decimal(odds):
    if odds > 0: return 1 + odds / 100
    elif odds < 0: return 1 + 100 / abs(odds)
    return 1

def cmd_run(args):
    import sqlite3, logging
    from datetime import datetime

    # Pipeline logging — append to data/pipeline.log
    _log_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'pipeline.log')
    logging.basicConfig(
        filename=_log_file, level=logging.INFO,
        format='%(asctime)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
    )
    _log = logging.getLogger('pipeline')

    sports = get_sports(args)
    do_email = has_flag(args, '--email')
    _hour = datetime.now().hour
    run_type = 'Evening' if _hour >= 17 else ('Afternoon' if _hour >= 13 else 'Morning')

    # Auto-detect active tennis tournaments and append to sports list
    if not any(s.startswith('tennis_') for s in sports):
        try:
            active_tennis = _detect_tennis_sports()
            if active_tennis:
                sports.extend(active_tennis)
        except Exception:
            pass

    _log.info(f"=== {run_type} Run START | Sports: {', '.join(sports)} ===")

    print("="*60)
    print(f"  SCOTTY'S EDGE v11 — {run_type} Run")
    print(f"  {datetime.now().strftime('%Y-%m-%d %I:%M %p ') + ('EDT' if 3 <= __import__('datetime').datetime.now().month <= 10 else 'EST')}")
    print(f"  Sports: {', '.join(s.split('_')[-1].upper() for s in sports)}")
    print("="*60)

    # v25.34: per-step timing so we can see where 20-min runs are spending time
    import time as _time
    _step_t0 = _time.time()
    _step_timings = {}
    def _mark(step):
        nonlocal _step_t0
        now = _time.time()
        _step_timings[step] = now - _step_t0
        _step_t0 = now

    # v26.0 Phase 8: Steps 1-5b extracted to pipeline.run_steps.data_refresh.
    from pipeline.run_steps.data_refresh import run_data_refresh
    total_odds_fetched = run_data_refresh(sports, run_type, _log, _mark, PROP_SPORTS)

    # Step 5b: Injury Data — REMOVED (duplicate of Step 2)
    # fetch_and_apply_all() already ran above in Step 2.

    # Step 5c: Research Agent — pre-game injury changes (consolidated into picks email)
    research_brief = None
    if do_email:
        try:
            from agent_research import generate_research_brief
            brief, alerts = generate_research_brief()
            if alerts:
                research_brief = f"🏥 INJURY CHANGES: {len(alerts)} since opener\n{brief}"
                # Only send standalone alert for 3+ high-impact changes
                if len(alerts) >= 3:
                    from emailer import send_email
                    send_email(f"⚠️ INJURY ALERT - {len(alerts)} changes", brief)
                    print(f"  Research agent: {len(alerts)} injury alerts — standalone alert + included in picks")
                else:
                    print(f"  Research agent: {len(alerts)} injury alerts — included in picks email")
            else:
                research_brief = "🏥 No new injury changes since opener"
                print("  Research agent: no new injury alerts")
        except Exception as e:
            print(f"  Research agent: {e}")

    # Step 6: Game Predictions (from cached odds — NOT fresh fetch)
    # v12.2 FIX: Predictions run on snapshot/cached odds, not live API.
    # The Step 3 fresh fetch is for CLV tracking only and does NOT affect picks.
    print("\n🧠 Step 6: Scotty's Edge Analysis...")
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db)

    from model_engine import generate_predictions, print_picks, save_picks_to_db, picks_to_text
    game_picks = []
    # Resolve tennis_auto → active tournament keys before generating predictions
    resolved_sports = []
    for sp in sports:
        if sp == 'tennis_auto':
            resolved_sports.extend(_detect_tennis_sports())
        else:
            resolved_sports.append(sp)
    # v25.34: per-sport timing inside Step 6 so we can see which sport is slow.
    _step6_breakdown = []
    for sp in resolved_sports:
        _sp_t0 = _time.time()
        picks = generate_predictions(conn, sport=sp)
        _sp_dur = _time.time() - _sp_t0
        _step6_breakdown.append((sp, _sp_dur, len(picks)))
        game_picks.extend(picks)
    _mark('step6_predictions')

    # v25.34: split Step 7 timing into 7a (consensus) and 7b (projection model)
    # so we can see which engine is the bottleneck.
    # Step 7a: Player Props — Edge Consensus Method
    print("\n🎯 Step 7a: Player Props — Edge Consensus Analysis...")
    consensus_props = []
    _s7a_t0 = _time.time()
    try:
        from props_engine import evaluate_props
        consensus_props = evaluate_props(conn)
        if consensus_props:
            print(f"  ✅ {len(consensus_props)} consensus prop edges found")
        else:
            print("  No consensus edges found (books are in agreement)")
    except Exception as e:
        print(f"  Props consensus: {e}")
        import traceback; traceback.print_exc()
    _step7a_dur = _time.time() - _s7a_t0
    _mark('step7a_consensus')

    # Step 7b: Player Props — Projection Model
    print("\n🔮 Step 7b: Player Props — Projection Model...")
    model_props = []
    _s7b_t0 = _time.time()
    try:
        from player_prop_model import generate_prop_projections
        model_props = generate_prop_projections(conn)
        if model_props:
            print(f"  ✅ {len(model_props)} projection-based prop picks")
    except Exception as e:
        print(f"  Props projection: {e}")
        import traceback; traceback.print_exc()
    _step7b_dur = _time.time() - _s7b_t0
    _mark('step7b_projection')

    # Step 7c: Merge consensus + model props (dedup: keep higher edge)
    prop_picks = _merge_prop_sources(consensus_props, model_props)

    # Step 7d: PROP_BOOK_ARB scanner (v25.31) — fire pure book-disagreement picks.
    # When sharp (FD/BR) and soft (DK/BetMGM/Caesars/Fanatics/ESPN BET) post
    # meaningfully different lines on the same player+stat+side, bet the soft
    # side at the soft book — the sharp is more likely right, and soft's line
    # gives us a better number. Mirror of v25.28 book-arb for game lines.
    try:
        prop_arb_picks = _prop_book_arb_scan(conn, existing_eids={p.get('event_id') for p in prop_picks})
        if prop_arb_picks:
            print(f"  💡 PROP_BOOK_ARB added {len(prop_arb_picks)} prop arb pick(s)")
            prop_picks = list(prop_picks) + prop_arb_picks
    except Exception as e:
        print(f"  Prop book-arb: {e}")

    # ═══ MERGE, DEDUP, SELECT BEST PICKS ═══
    all_picks = _merge_and_select(game_picks, prop_picks, conn=conn)

    # ═══ DEDUP vs EARLIER RUNS TODAY ═══
    # Remove picks already posted in a previous run today.
    # v16 FIX: Dedup by TEAM NAMES + SIDE, not event_id. The Odds API sometimes
    # assigns new event_ids when lines move, causing the same game to appear
    # multiple times on different cards. Each duplicate inflates the record.
    # Only the FIRST bet on a matchup+side counts. Later runs with moved lines
    # are duplicates even if the event_id or line number changed.
    import re as _re
    if all_picks:
        today_str = datetime.now().strftime('%Y-%m-%d')
        already_posted = conn.execute("""
            SELECT sport, market_type, selection, event_id FROM bets
            WHERE created_at >= ? AND result IS NULL
        """, (today_str,)).fetchall()
        # v17: Track event_ids already bet today for concentration cap
        # v25.38: exclude PROPs — props are independent of game-line picks on
        # the same event (a player prop doesn't correlate with spread/total).
        # Previously, a prop misrouted to a different game's event_id (Odds API
        # quirk) would block legitimate SPREAD_FADE_FLIP / game-line picks.
        # Seen 2026-04-20: Ayo Dosunmu UNDER mapped to DEN/MIN event_id,
        # blocking the SPREAD_FADE_FLIP fade on MIN +7.5.
        posted_event_ids = set(
            row[3] for row in already_posted
            if row[3] and row[1] != 'PROP'
        )
        posted_keys = set()
        for row in already_posted:
            sport, mtype, sel = row[0], row[1], row[2]
            # Strip line numbers to get just the side/team
            if mtype == 'SPREAD':
                side = _re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
            elif mtype == 'TOTAL':
                # "Creighton@Miami OVER 12.5" → "Creighton@Miami OVER"
                side = _re.sub(r'\s+\d+\.?\d*$', '', sel).strip()
            elif mtype == 'MONEYLINE':
                side = sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
            else:
                side = sel
            posted_keys.add(f"{sport}|{mtype}|{side}")

        before = len(all_picks)
        all_picks_before_dedup = list(all_picks)
        new_picks = []
        duped = []
        for p in all_picks:
            sport = p.get('sport', '')
            mtype = p.get('market_type', '')
            sel = p.get('selection', '')
            if mtype == 'SPREAD':
                side = _re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
            elif mtype == 'TOTAL':
                side = _re.sub(r'\s+\d+\.?\d*$', '', sel).strip()
            elif mtype == 'MONEYLINE':
                side = sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
            else:
                side = sel
            key = f"{sport}|{mtype}|{side}"
            if key in posted_keys:
                duped.append(sel)
            # v17: Concentration cap — skip if we already have a bet on this game from earlier run
            elif p.get('event_id') in posted_event_ids and mtype != 'PROP':
                print(f"  CONCENTRATION CAP: skipped {sel[:50]} — already have a bet on this game")
                duped.append(sel)
            else:
                new_picks.append(p)

        # Track line movement on deduped picks — useful for social captions
        line_moves = []
        if duped:
            print(f"\n  Deduped {len(duped)} picks already posted earlier today:")
            for d in duped:
                print(f"    - {d}")
            # Check if lines moved in our favor on deduped picks
            for p in all_picks_before_dedup:
                sel = p.get('selection', '')
                mtype = p.get('market_type', '')
                if mtype == 'TOTAL':
                    side = _re.sub(r'\s+\d+\.?\d*$', '', sel).strip()
                elif mtype == 'SPREAD':
                    side = _re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
                else:
                    side = sel.replace(' ML', '').strip()
                key = f"{p.get('sport','')}|{mtype}|{side}"
                if key in posted_keys and sel not in [np.get('selection','') for np in new_picks]:
                    # This was deduped — check original line vs new line
                    orig = conn.execute("""
                        SELECT selection, line, odds FROM bets
                        WHERE created_at >= ? AND market_type = ? AND sport = ?
                        AND result IS NULL
                        ORDER BY created_at ASC LIMIT 1
                    """, (today_str, mtype, p.get('sport',''))).fetchone()
                    if orig:
                        orig_line = orig[1]
                        new_line = p.get('line')
                        if orig_line is not None and new_line is not None and orig_line != new_line:
                            if mtype == 'TOTAL':
                                if 'OVER' in sel.upper():
                                    moved_favor = new_line > orig_line  # Higher total = harder for over
                                    direction = 'up' if new_line > orig_line else 'down'
                                else:
                                    moved_favor = new_line < orig_line
                                    direction = 'down' if new_line < orig_line else 'up'
                            else:
                                moved_favor = new_line > orig_line  # More points for dog
                                direction = f"{orig_line:+.1f} → {new_line:+.1f}"
                            if moved_favor:
                                line_moves.append(f"LINE MOVE: {side} (line moved {direction}, {orig_line} → {new_line}) — we got the better number")
                            else:
                                line_moves.append(f"LINE MOVE: {side} ({orig_line} → {new_line})")
            if line_moves:
                print(f"\n  📈 Line movements on today's picks:")
                for lm in line_moves:
                    print(f"    {lm}")
        all_picks = new_picks

    # ═══ VALIDATION — Catch logical errors before saving ═══
    if all_picks:
        all_picks = _validate_picks(all_picks)

        # ═══ PRE-SAVE CONCENTRATION CHECK — Safety net ═══
        # Catches concentration risk that slipped past filters (e.g., cross-run accumulation).
        # Checks existing bets + new picks combined. Blocks and warns, doesn't silently drop.
        try:
            _today_str = datetime.now().strftime('%Y-%m-%d')
            _existing = conn.execute("""
                SELECT sport, side_type, COUNT(*) as cnt, SUM(units) as u
                FROM bets WHERE DATE(created_at) = ? AND units >= 3.5
                GROUP BY sport, side_type
            """, (_today_str,)).fetchall()
            _dir_totals = {}
            _sport_totals = {}
            for _sp, _side, _cnt, _u in _existing:
                _dir_totals[f"{_sp}|{_side}"] = _cnt
                _sport_totals[_sp] = _sport_totals.get(_sp, 0) + _u

            _blocked = []
            _passed = []
            for p in all_picks:
                sp = p.get('sport', '')
                mtype = p.get('market_type', '')
                sel = p.get('selection', '')
                units = p.get('units', 5.0)
                # Infer side
                if mtype == 'TOTAL':
                    side = 'OVER' if 'OVER' in sel.upper() else 'UNDER'
                elif mtype == 'SPREAD':
                    side = 'DOG' if (p.get('line', 0) or 0) > 0 else 'FAVORITE'
                elif mtype == 'MONEYLINE':
                    side = 'DOG' if (p.get('odds', -110) or -110) > 0 else 'FAVORITE'
                else:
                    side = ''
                dir_key = f"{sp}|{side}"
                dir_count = _dir_totals.get(dir_key, 0)
                sport_units = _sport_totals.get(sp, 0)

                if dir_count >= 4:
                    print(f"  ⚠ CONCENTRATION BLOCK: {sel[:50]} — {dir_count} {side} picks already for {sp}")
                    _blocked.append(p)
                else:
                    _dir_totals[dir_key] = dir_count + 1
                    _sport_totals[sp] = sport_units + units
                    _passed.append(p)

            if _blocked:
                print(f"  Concentration check: blocked {len(_blocked)}, passed {len(_passed)}")
                try:
                    conn.execute("""CREATE TABLE IF NOT EXISTS shadow_blocked_picks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT, sport TEXT, event_id TEXT, selection TEXT,
                        market_type TEXT, book TEXT, line REAL, odds REAL,
                        edge_pct REAL, units REAL, reason TEXT,
                        reason_category TEXT, reason_detail TEXT
                    )""")
                    for _bp in _blocked:
                        conn.execute("""INSERT INTO shadow_blocked_picks
                            (created_at, sport, event_id, selection, market_type, book, line, odds, edge_pct, units, reason)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (datetime.now().isoformat(), _bp.get('sport',''), _bp.get('event_id',''),
                             _bp.get('selection',''), _bp.get('market_type',''), _bp.get('book',''),
                             _bp.get('line'), _bp.get('odds'), _bp.get('edge_pct', 0),
                             _bp.get('units', 0), 'CROSS_RUN_CAP'))
                    conn.commit()
                except Exception as _e:
                    print(f"  Shadow log: {_e}")
            all_picks = _passed
        except Exception as e:
            print(f"  Concentration check: {e}")

        # ═══ BOOK_ARB LINE-STABILITY GATE (v25.42) ═══
        # Require each book's opener to have been in our database for at least
        # 60 minutes before trusting an opener-gap arb signal. UCSB@Cal Baptist
        # UNDER 13.5 (bet 990, 2026-04-20) fired 13 minutes after both FD and
        # DK posted openers — FD opened at 10.5 while DK opened at 13.5, and
        # within 30 min FD caught up to 13.5. The "arb" was a just-posted
        # stale soft line, not asymmetric sharp information. Post-mortem
        # parked the gate in agent_todo.md; shipping now.
        BOOK_ARB_MIN_OPENER_AGE_MIN = 60
        def _arb_lines_stable(sport, event_id, market, books):
            """Return True if every named book's opener has aged >= threshold.

            Uses openers.timestamp as first_seen. If a row lacks a timestamp,
            treat it as stable (can't prove instability on missing data)."""
            try:
                from datetime import timezone
                _now = datetime.now(timezone.utc)
                for bk in books:
                    _ts = conn.execute("""
                        SELECT MIN(timestamp) FROM openers
                        WHERE sport=? AND event_id=? AND market=? AND book=?
                    """, (sport, event_id, market, bk)).fetchone()
                    if not _ts or not _ts[0]:
                        continue
                    _fs = datetime.fromisoformat(_ts[0].replace('Z', '+00:00'))
                    _age_min = (_now - _fs).total_seconds() / 60.0
                    if _age_min < BOOK_ARB_MIN_OPENER_AGE_MIN:
                        return False, bk, round(_age_min, 1)
                return True, None, None
            except Exception:
                return True, None, None  # fail-open on transient errors

        # ═══ NCAA BASEBALL BOOK-ARB (v25.25) ═══
        # FD vs DK opener disagreement ≥ 2.0 runs = soft-vs-sharp inefficiency.
        # Fire the soft side of DK regardless of model: OVER at DK when DK<FD,
        # UNDER at DK when DK>FD. Tagged side_type='BOOK_ARB' for monitoring.
        # Skip events we already picked (avoid competing with model or Option C).
        # Currency check: current FD-DK gap must still be ≥1.0 in original direction.
        try:
            _existing_eids = {p.get('event_id') for p in all_picks if p.get('sport') == 'baseball_ncaa'}
            _today_str = datetime.now().strftime('%Y-%m-%d')
            _cand = conn.execute("""
                SELECT DISTINCT event_id FROM openers
                WHERE sport='baseball_ncaa' AND market='totals' AND book='FanDuel'
                  AND snapshot_date = ?
                INTERSECT
                SELECT DISTINCT event_id FROM openers
                WHERE sport='baseball_ncaa' AND market='totals' AND book='DraftKings'
                  AND snapshot_date = ?
            """, (_today_str, _today_str)).fetchall()
            _book_arb = []
            for (_eid,) in _cand:
                if _eid in _existing_eids:
                    continue
                _fd_o = conn.execute(
                    "SELECT line FROM openers WHERE event_id=? AND market='totals' AND book='FanDuel' AND snapshot_date=? LIMIT 1",
                    (_eid, _today_str)).fetchone()
                _dk_o = conn.execute(
                    "SELECT line FROM openers WHERE event_id=? AND market='totals' AND book='DraftKings' AND snapshot_date=? LIMIT 1",
                    (_eid, _today_str)).fetchone()
                if not _fd_o or not _dk_o:
                    continue
                _fd_open, _dk_open = _fd_o[0], _dk_o[0]
                _opener_gap = abs(_fd_open - _dk_open)
                if _opener_gap < 2.0:
                    continue
                _side = 'OVER' if _dk_open < _fd_open else 'UNDER'
                # Current latest DK line for this side
                _cur_dk = conn.execute("""
                    SELECT o.line, o.odds, o.home, o.away, o.commence_time FROM odds o
                    WHERE o.event_id=? AND o.market='totals' AND o.book='DraftKings'
                      AND UPPER(o.selection) LIKE ?
                      AND NOT EXISTS (
                          SELECT 1 FROM odds o2 WHERE o2.event_id=o.event_id AND o2.book=o.book
                          AND o2.market=o.market AND o2.selection=o.selection
                          AND (o2.snapshot_date > o.snapshot_date
                               OR (o2.snapshot_date=o.snapshot_date AND o2.snapshot_time > o.snapshot_time))
                      ) LIMIT 1
                """, (_eid, f'%{_side}%')).fetchone()
                if not _cur_dk:
                    continue
                _dk_line, _dk_odds, _home, _away, _commence = _cur_dk
                # Current FD line
                _cur_fd = conn.execute("""
                    SELECT line FROM odds o WHERE event_id=? AND market='totals' AND book='FanDuel'
                      AND UPPER(selection) LIKE ?
                      AND NOT EXISTS (
                          SELECT 1 FROM odds o2 WHERE o2.event_id=o.event_id AND o2.book=o.book
                          AND o2.market=o.market AND o2.selection=o.selection
                          AND (o2.snapshot_date > o.snapshot_date
                               OR (o2.snapshot_date=o.snapshot_date AND o2.snapshot_time > o.snapshot_time))
                      ) LIMIT 1
                """, (_eid, f'%{_side}%')).fetchone()
                if not _cur_fd:
                    continue
                _fd_now = _cur_fd[0]
                # Currency check: gap must still exist in the same direction
                if _side == 'OVER' and _dk_line >= _fd_now:
                    print(f"  ⚠ NCAA_BOOK_ARB skipped: DK {_dk_line} no longer softer than FD {_fd_now} for OVER")
                    continue
                if _side == 'UNDER' and _dk_line <= _fd_now:
                    print(f"  ⚠ NCAA_BOOK_ARB skipped: DK {_dk_line} no longer softer than FD {_fd_now} for UNDER")
                    continue
                _current_gap = abs(_fd_now - _dk_line)
                if _current_gap < 1.0:
                    print(f"  ⚠ NCAA_BOOK_ARB skipped: current gap {_current_gap:.1f} too thin")
                    continue
                # v25.33: only bet on TODAY's games (ET)
                if _commence:
                    try:
                        from zoneinfo import ZoneInfo
                        _ET = ZoneInfo('America/New_York')
                        _dt_et = datetime.fromisoformat(_commence.replace('Z','+00:00')).astimezone(_ET)
                        _today_et = datetime.now(_ET).strftime('%Y-%m-%d')
                        if _dt_et.strftime('%Y-%m-%d') != _today_et:
                            continue  # not today in ET
                    except Exception:
                        pass
                # Skip started games (compare UTC timestamps as strings — good enough)
                try:
                    if _commence:
                        _now_utc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                        if _commence < _now_utc:
                            continue
                except Exception:
                    pass
                # Global MIN_ODDS policy — never fire at worse than -150
                from config import MIN_ODDS as _NBA_MIN_ODDS
                _eff_odds = _dk_odds if _dk_odds is not None else -110
                if _eff_odds <= _NBA_MIN_ODDS:
                    print(f"  ⚠ NCAA_BOOK_ARB skipped: DK {_side} {_eff_odds:+.0f} worse than MIN_ODDS {_NBA_MIN_ODDS}")
                    continue
                # v25.42 line-stability gate — both books' openers must be >= 60 min old
                _stable, _young_book, _age = _arb_lines_stable(
                    'baseball_ncaa', _eid, 'totals', ['FanDuel', 'DraftKings'])
                if not _stable:
                    print(f"  ⚠ NCAA_BOOK_ARB_LINE_UNSETTLED: {_young_book} opener only {_age} min old (need {BOOK_ARB_MIN_OPENER_AGE_MIN})")
                    try:
                        _sel_for_log = f"{_away}@{_home} {_side} {_dk_line}"
                        conn.execute("""INSERT INTO shadow_blocked_picks
                            (created_at, sport, event_id, selection, market_type, book,
                             line, odds, edge_pct, units, reason)
                            VALUES (?, ?, ?, ?, 'TOTAL', 'DraftKings', ?, ?, ?, ?, ?)""",
                            (datetime.now().isoformat(), 'baseball_ncaa', _eid, _sel_for_log,
                             _dk_line, _dk_odds, round(_opener_gap * 5.0, 1), 5.0,
                             f'BOOK_ARB_LINE_UNSETTLED ({_young_book} {_age} min)'))
                        conn.commit()
                    except Exception:
                        pass
                    continue
                _sel = f"{_away}@{_home} {_side} {_dk_line}"
                _pick = {
                    'sport': 'baseball_ncaa', 'event_id': _eid, 'market_type': 'TOTAL',
                    'selection': _sel, 'book': 'DraftKings',
                    'line': _dk_line, 'odds': _dk_odds,
                    'edge_pct': round(_opener_gap * 5.0, 1),
                    'confidence': 'BOOK_ARB', 'units': 5.0,
                    'context': f'BOOK_ARB: FD opener {_fd_open} vs DK opener {_dk_open} (opener gap {_opener_gap:.1f}) | current FD {_fd_now} vs DK {_dk_line} (gap {_current_gap:.1f})',
                    'commence': _commence, 'home': _home, 'away': _away,
                    'star_rating': 3, 'model_prob': 0, 'implied_prob': 0,
                    'side_type': 'BOOK_ARB', 'model_spread': None, 'timing': 'UNKNOWN',
                }
                _book_arb.append(_pick)
                print(f"  💡 NCAA_BOOK_ARB: {_sel} @ {_dk_odds:+.0f} | opener gap {_opener_gap:.1f} | current gap {_current_gap:.1f}")
            if _book_arb:
                print(f"  💡 Added {len(_book_arb)} NCAA book-arb pick(s) to slate")
                all_picks = list(all_picks) + _book_arb
        except Exception as e:
            print(f"  NCAA book-arb: {e}")

        # ═══ NCAA BASEBALL NO-SHARP SKIP (v25.24) ═══
        # Backtest: 11 no-sharp NCAA TOTAL picks went 3W-7L, -23.6u across all books.
        # When neither FanDuel nor BetRivers posts an opener, the market is too thin/noisy
        # (Sun Belt, midweek non-conf, backup-pitcher-heavy slates). Our model's edge
        # estimate on these is based on unreliable run-allowed stats and day-of-week filters
        # that don't have enough N for mid-majors. Absorbs the Thursday bleed and
        # mid-major team bleed without separate rules.
        try:
            _ncaa_sharp_passed = []
            _ncaa_no_sharp_blocked = []
            for p in all_picks:
                if not (p.get('sport') == 'baseball_ncaa' and p.get('market_type') == 'TOTAL'):
                    _ncaa_sharp_passed.append(p)
                    continue
                eid = p.get('event_id', '')
                rows = conn.execute(
                    "SELECT book FROM openers WHERE event_id=? AND market='totals' "
                    "AND book IN ('FanDuel','BetRivers')", (eid,)
                ).fetchall()
                if rows:
                    _ncaa_sharp_passed.append(p)
                else:
                    _ncaa_no_sharp_blocked.append(p)
                    print(f"  ⚠ NCAA_NO_SHARP_SKIP: {p.get('selection','')[:55]} — neither FD nor BR posted opener")
            if _ncaa_no_sharp_blocked:
                _now = datetime.now().isoformat()
                for _p in _ncaa_no_sharp_blocked:
                    conn.execute("""INSERT INTO shadow_blocked_picks
                        (created_at, sport, event_id, selection, market_type, book, line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (_now, _p.get('sport',''), _p.get('event_id',''),
                         _p.get('selection',''), _p.get('market_type',''), _p.get('book',''),
                         _p.get('line'), _p.get('odds'), _p.get('edge_pct', 0),
                         _p.get('units', 0), 'NCAA_NO_SHARP_SKIP (neither FanDuel nor BetRivers posted opener)'))
                conn.commit()
            all_picks = _ncaa_sharp_passed
        except Exception as e:
            print(f"  NCAA no-sharp skip: {e}")

        # ═══ BOOK-ARB GATES (v25.28) — NBA totals, NHL + MLB spreads ═══
        # Signal: sharp (FD/BR) and soft (DK/BetMGM/Caesars/Fanatics/ESPN BET) disagree
        # on the opener by a meaningful gap. Bet the soft side at the soft book — we're
        # taking the easier number on the side the sharp thinks will win.
        #
        # Backtest across paired-opener events with results (see session 2026-04-18):
        #   NBA totals  gap ≥ 1.0:  50 bets, 70.0% WR, +16.6u, +33.2% ROI
        #   NHL spreads gap ≥ 1.5:  42 bets, 85.7% WR, +7.7u,  +18.4% ROI (flipped favorite)
        #   MLB spreads gap ≥ 1.5:  34 bets, 79.4% WR, +6.5u,  +19.2% ROI (flipped favorite)
        # (NBA spreads, MLB totals, NCAA baseball spreads all showed no edge or reversed signal.)
        #
        # User decision 4/18: max-play sizing (5.0u), matching the existing v25.25 NCAA
        # Baseball book-arb gate. Sample is <60 bets per new gate (95% CI lower bound on
        # NBA WR is ~55%), but user accepts variance risk on backtested edge.
        # Revisit sizing if first 50 live bets underperform backtest median by >10% WR.
        try:
            _today_str = datetime.now().strftime('%Y-%m-%d')
            _SOFT_BOOKS = ('DraftKings','BetMGM','Caesars','Fanatics','ESPN BET')
            _SHARP_BOOKS = ('FanDuel','BetRivers')
            from config import MIN_ODDS as _BA_MIN_ODDS  # e.g. -150

            def _book_arb_scan(sport_, market_, opener_thr, current_thr):
                """Find book-arb opportunities. Returns list of pick dicts."""
                # Events where BOTH a sharp and a soft book posted openers today
                cands = conn.execute("""
                    SELECT DISTINCT o.event_id FROM openers o
                    WHERE o.sport=? AND o.market=? AND o.snapshot_date=?
                      AND o.book IN ('FanDuel','BetRivers')
                    INTERSECT
                    SELECT DISTINCT o.event_id FROM openers o
                    WHERE o.sport=? AND o.market=? AND o.snapshot_date=?
                      AND o.book IN ('DraftKings','BetMGM','Caesars','Fanatics','ESPN BET')
                """, (sport_, market_, _today_str, sport_, market_, _today_str)).fetchall()

                existing_eids = {p.get('event_id') for p in all_picks if p.get('sport') == sport_}
                picks_out = []

                for (eid,) in cands:
                    if eid in existing_eids:
                        continue
                    # Pull home/away/commence from odds
                    ev_meta = conn.execute("""
                        SELECT home, away, commence_time FROM odds
                        WHERE event_id=? LIMIT 1
                    """, (eid,)).fetchone()
                    if not ev_meta:
                        continue
                    _home, _away, _commence = ev_meta

                    # v25.33: only bet on TODAY's games (ET). Book-arb scanner was
                    # picking up Monday opener gaps (e.g. Raptors/Cavs Game 2) because
                    # early openers are posted 48h out. We only bet same-day.
                    if _commence:
                        try:
                            from zoneinfo import ZoneInfo
                            _ET = ZoneInfo('America/New_York')
                            _dt_et = datetime.fromisoformat(_commence.replace('Z','+00:00')).astimezone(_ET)
                            _today_et = datetime.now(_ET).strftime('%Y-%m-%d')
                            if _dt_et.strftime('%Y-%m-%d') != _today_et:
                                continue  # not today in ET
                        except Exception:
                            pass

                    # Skip started games
                    if _commence:
                        _now_utc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                        if _commence < _now_utc:
                            continue

                    rows = conn.execute("""
                        SELECT book, selection, line, odds FROM openers
                        WHERE event_id=? AND market=? AND snapshot_date=?
                    """, (eid, market_, _today_str)).fetchall()

                    if market_ == 'totals':
                        # Get Over line per book (same line as Under)
                        over_by_book = {b: (ln, od) for b, sel, ln, od in rows if (sel or '').lower() == 'over'}
                        best = None  # (soft_book, sharp_book, gap, sharp_ln, soft_ln, soft_odds_over, soft_odds_under)
                        under_by_book = {b: (ln, od) for b, sel, ln, od in rows if (sel or '').lower() == 'under'}
                        for sharp in _SHARP_BOOKS:
                            if sharp not in over_by_book: continue
                            sharp_ln = over_by_book[sharp][0]
                            for soft in _SOFT_BOOKS:
                                if soft not in over_by_book: continue
                                soft_ln, _ = over_by_book[soft]
                                gap = soft_ln - sharp_ln
                                if abs(gap) < opener_thr: continue
                                if best is None or abs(gap) > abs(best[2]):
                                    soft_over_odds = over_by_book[soft][1] or -110
                                    soft_under_odds = under_by_book.get(soft, (None, -110))[1] or -110
                                    best = (soft, sharp, gap, sharp_ln, soft_ln, soft_over_odds, soft_under_odds)
                        if not best: continue
                        soft, sharp, gap, sharp_open, soft_open, so_over, so_under = best
                        side = 'UNDER' if gap > 0 else 'OVER'
                        # Currency check: current soft line vs current sharp line still
                        # mispriced in the same direction, by at least current_thr
                        _cur_soft = conn.execute("""
                            SELECT line, odds FROM odds o WHERE event_id=? AND market='totals'
                              AND book=? AND UPPER(selection) LIKE ?
                              AND NOT EXISTS (
                                  SELECT 1 FROM odds o2 WHERE o2.event_id=o.event_id AND o2.book=o.book
                                  AND o2.market=o.market AND o2.selection=o.selection
                                  AND (o2.snapshot_date > o.snapshot_date
                                       OR (o2.snapshot_date=o.snapshot_date AND o2.snapshot_time > o.snapshot_time))
                              ) LIMIT 1
                        """, (eid, soft, f'%{side}%')).fetchone()
                        _cur_sharp = conn.execute("""
                            SELECT line FROM odds o WHERE event_id=? AND market='totals'
                              AND book=? AND UPPER(selection) LIKE ?
                              AND NOT EXISTS (
                                  SELECT 1 FROM odds o2 WHERE o2.event_id=o.event_id AND o2.book=o.book
                                  AND o2.market=o.market AND o2.selection=o.selection
                                  AND (o2.snapshot_date > o.snapshot_date
                                       OR (o2.snapshot_date=o.snapshot_date AND o2.snapshot_time > o.snapshot_time))
                              ) LIMIT 1
                        """, (eid, sharp, f'%{side}%')).fetchone()
                        if not _cur_soft or not _cur_sharp: continue
                        cur_soft_ln, cur_soft_odds = _cur_soft
                        cur_sharp_ln = _cur_sharp[0]
                        cur_gap = cur_soft_ln - cur_sharp_ln
                        # Direction must still match opener direction and meet current threshold
                        if (gap > 0 and cur_gap <= 0) or (gap < 0 and cur_gap >= 0):
                            print(f"  ⚠ {sport_}_BOOK_ARB_TOTAL skipped: gap reversed ({cur_soft_ln} vs {cur_sharp_ln})")
                            continue
                        if abs(cur_gap) < current_thr:
                            print(f"  ⚠ {sport_}_BOOK_ARB_TOTAL skipped: current gap {abs(cur_gap):.1f} < {current_thr}")
                            continue
                        # Global MIN_ODDS policy — never fire at worse than -150
                        _eff_odds = cur_soft_odds if cur_soft_odds is not None else -110
                        if _eff_odds <= _BA_MIN_ODDS:
                            print(f"  ⚠ {sport_}_BOOK_ARB_TOTAL skipped: {side} at {soft} {_eff_odds:+.0f} worse than MIN_ODDS {_BA_MIN_ODDS}")
                            continue
                        # v25.42 line-stability gate — both chosen books must be >= 60 min old
                        _stable, _young_book, _age = _arb_lines_stable(
                            sport_, eid, market_, [soft, sharp])
                        if not _stable:
                            print(f"  ⚠ {sport_}_BOOK_ARB_TOTAL_LINE_UNSETTLED: {_young_book} opener only {_age} min old (need {BOOK_ARB_MIN_OPENER_AGE_MIN})")
                            try:
                                conn.execute("""INSERT INTO shadow_blocked_picks
                                    (created_at, sport, event_id, selection, market_type, book,
                                     line, odds, edge_pct, units, reason)
                                    VALUES (?, ?, ?, ?, 'TOTAL', ?, ?, ?, ?, ?, ?)""",
                                    (datetime.now().isoformat(), sport_, eid,
                                     f"{_away}@{_home} {side} {cur_soft_ln}", soft,
                                     cur_soft_ln, cur_soft_odds,
                                     round(abs(gap) * 5.0, 1), 5.0,
                                     f'BOOK_ARB_LINE_UNSETTLED ({_young_book} {_age} min)'))
                                conn.commit()
                            except Exception:
                                pass
                            continue
                        _sel = f"{_away}@{_home} {side} {cur_soft_ln}"
                        _reason = (
                            f'BOOK ARB — Sharp {sharp} opened total at {sharp_open}, '
                            f'soft {soft} opened at {soft_open} (opener gap {gap:+.1f}). '
                            f'Betting {side} {cur_soft_ln} at {soft}: easier number on the side sharp likes. '
                            f'Current gap {cur_gap:+.1f}.'
                        )
                        pick = {
                            'sport': sport_, 'event_id': eid, 'market_type': 'TOTAL',
                            'selection': _sel, 'book': soft,
                            'line': cur_soft_ln, 'odds': cur_soft_odds or -110,
                            'edge_pct': round(abs(gap) * 5.0, 1),
                            'confidence': 'BOOK_ARB', 'units': 5.0,
                            'context': _reason,
                            'commence': _commence, 'home': _home, 'away': _away,
                            'star_rating': 3, 'model_prob': 0, 'implied_prob': 0,
                            'side_type': 'BOOK_ARB', 'model_spread': None, 'timing': 'UNKNOWN',
                        }
                        picks_out.append(pick)
                        print(f"  💡 {sport_}_BOOK_ARB_TOTAL: {_sel} @ {soft} {cur_soft_odds:+.0f} | opener gap {gap:+.1f}, current {cur_gap:+.1f}")

                    elif market_ == 'spreads':
                        # Per-team lines; compare home line across books
                        home_by_book = {b: (ln, od) for b, sel, ln, od in rows if sel == _home}
                        away_by_book = {b: (ln, od) for b, sel, ln, od in rows if sel == _away}
                        best = None  # (soft, sharp, gap, bet_team, bet_line, bet_odds)
                        for sharp in _SHARP_BOOKS:
                            if sharp not in home_by_book: continue
                            sharp_h = home_by_book[sharp][0]
                            for soft in _SOFT_BOOKS:
                                if soft not in home_by_book: continue
                                soft_h = home_by_book[soft][0]
                                gap = soft_h - sharp_h
                                if abs(gap) < opener_thr: continue
                                if best is None or abs(gap) > abs(best[2]):
                                    # gap > 0: soft has home weaker than sharp → sharp likes home → bet HOME at soft
                                    # gap < 0: bet AWAY at soft
                                    if gap > 0:
                                        bet_team, bet_ln, bet_odds = _home, soft_h, (home_by_book[soft][1] or -110)
                                    else:
                                        a = away_by_book.get(soft, (-soft_h, -110))
                                        bet_team, bet_ln, bet_odds = _away, a[0], (a[1] or -110)
                                    best = (soft, sharp, gap, bet_team, bet_ln, bet_odds)
                        if not best: continue
                        soft, sharp, gap, bet_team, bet_line_open, bet_odds_open = best
                        # Currency check via latest odds
                        _cur = conn.execute("""
                            SELECT line, odds FROM odds o WHERE event_id=? AND market='spreads'
                              AND book=? AND selection=?
                              AND NOT EXISTS (
                                  SELECT 1 FROM odds o2 WHERE o2.event_id=o.event_id AND o2.book=o.book
                                  AND o2.market=o.market AND o2.selection=o.selection
                                  AND (o2.snapshot_date > o.snapshot_date
                                       OR (o2.snapshot_date=o.snapshot_date AND o2.snapshot_time > o.snapshot_time))
                              ) LIMIT 1
                        """, (eid, soft, bet_team)).fetchone()
                        _cur_sharp = conn.execute("""
                            SELECT line FROM odds o WHERE event_id=? AND market='spreads'
                              AND book=? AND selection=?
                              AND NOT EXISTS (
                                  SELECT 1 FROM odds o2 WHERE o2.event_id=o.event_id AND o2.book=o.book
                                  AND o2.market=o.market AND o2.selection=o.selection
                                  AND (o2.snapshot_date > o.snapshot_date
                                       OR (o2.snapshot_date=o.snapshot_date AND o2.snapshot_time > o.snapshot_time))
                              ) LIMIT 1
                        """, (eid, sharp, bet_team)).fetchone()
                        if not _cur or not _cur_sharp: continue
                        cur_line, cur_odds = _cur
                        cur_sharp = _cur_sharp[0]
                        # bet_team should be better-priced at soft than at sharp; measure cushion
                        cur_gap = cur_line - cur_sharp
                        if abs(cur_gap) < current_thr:
                            print(f"  ⚠ {sport_}_BOOK_ARB_SPREAD skipped: current gap {abs(cur_gap):.1f} < {current_thr}")
                            continue
                        # Global MIN_ODDS policy — never fire at worse than -150
                        _eff_odds = cur_odds if cur_odds is not None else -110
                        if _eff_odds <= _BA_MIN_ODDS:
                            print(f"  ⚠ {sport_}_BOOK_ARB_SPREAD skipped: {bet_team} at {soft} {_eff_odds:+.0f} worse than MIN_ODDS {_BA_MIN_ODDS}")
                            continue
                        # v25.42 line-stability gate — both chosen books must be >= 60 min old
                        _stable, _young_book, _age = _arb_lines_stable(
                            sport_, eid, market_, [soft, sharp])
                        if not _stable:
                            print(f"  ⚠ {sport_}_BOOK_ARB_SPREAD_LINE_UNSETTLED: {_young_book} opener only {_age} min old (need {BOOK_ARB_MIN_OPENER_AGE_MIN})")
                            try:
                                conn.execute("""INSERT INTO shadow_blocked_picks
                                    (created_at, sport, event_id, selection, market_type, book,
                                     line, odds, edge_pct, units, reason)
                                    VALUES (?, ?, ?, ?, 'SPREAD', ?, ?, ?, ?, ?, ?)""",
                                    (datetime.now().isoformat(), sport_, eid,
                                     f"{_away}@{_home} {bet_team} {cur_line:+g}", soft,
                                     cur_line, cur_odds,
                                     round(abs(gap) * 4.0, 1), 5.0,
                                     f'BOOK_ARB_LINE_UNSETTLED ({_young_book} {_age} min)'))
                                conn.commit()
                            except Exception:
                                pass
                            continue
                        _sel = f"{_away}@{_home} {bet_team} {cur_line:+g}"
                        _sharp_h = home_by_book[sharp][0]
                        _soft_h = home_by_book[soft][0]
                        _reason = (
                            f'BOOK ARB — Sharp {sharp} opened home at {_sharp_h:+g}, '
                            f'soft {soft} opened home at {_soft_h:+g} (opener gap {gap:+.1f}). '
                            f'Betting {bet_team} {cur_line:+g} at {soft}: easier spread on the team sharp likes. '
                            f'Current gap {cur_gap:+.1f}.'
                        )
                        pick = {
                            'sport': sport_, 'event_id': eid, 'market_type': 'SPREAD',
                            'selection': _sel, 'book': soft,
                            'line': cur_line, 'odds': cur_odds or -110,
                            'edge_pct': round(abs(gap) * 4.0, 1),
                            'confidence': 'BOOK_ARB', 'units': 5.0,
                            'context': _reason,
                            'commence': _commence, 'home': _home, 'away': _away,
                            'star_rating': 3, 'model_prob': 0, 'implied_prob': 0,
                            'side_type': 'BOOK_ARB', 'model_spread': None, 'timing': 'UNKNOWN',
                        }
                        picks_out.append(pick)
                        print(f"  💡 {sport_}_BOOK_ARB_SPREAD: {_sel} @ {soft} {cur_odds:+.0f} | opener gap {gap:+.1f}, current {cur_gap:+.1f}")
                return picks_out

            _new_picks = []
            _new_picks += _book_arb_scan('basketball_nba', 'totals',  opener_thr=1.0, current_thr=0.5)
            _new_picks += _book_arb_scan('icehockey_nhl',  'spreads', opener_thr=1.5, current_thr=1.0)
            _new_picks += _book_arb_scan('baseball_mlb',   'spreads', opener_thr=1.5, current_thr=1.0)
            if _new_picks:
                print(f"  💡 v25.28 book-arb added {len(_new_picks)} pick(s) across NBA totals / NHL & MLB spreads")
                all_picks = list(all_picks) + _new_picks
        except Exception as e:
            print(f"  Book-arb (v25.28): {e}")

        # ═══ NCAA BASEBALL DK GATE (v25.23 tight-skip + Option C fade-flip) ═══
        # Two-pass gate on NCAA Baseball TOTAL picks at DraftKings:
        #   (1) TIGHT CONSENSUS SKIP: if DK line == FD opener and (BR absent or BR == DK), skip.
        #       Historical: 7 tight picks → 2W-5L, -16.30u. Market perfectly priced → no real edge.
        #   (2) OPTION C FADE-FLIP: if FD or BR opens against our direction, FLIP the pick
        #       to the opposite side (model said UNDER but sharps priced OVER → we bet OVER).
        #       Historical: 7 flip candidates → flipped record ~5W-2L, +17u (vs 2W-5L, -11.48u
        #       taken as model said). Uses sharp book as primary signal.
        #   (2a) v25.27 SHARP-AGREE BLOCK: if one sharp strictly disagrees but the OTHER
        #        sharp strictly agrees with the model direction, don't flip. Split-sharp
        #        disagreements where a sharp actively endorses the model (e.g. BR opens
        #        UNDER the DK line on our UNDER pick) are a different regime than the
        #        backtested 7 (all had either both-disagree or one-disagree+other-neutral).
        #        Auburn@Florida 4/17 was the triggering case: BR opened 9.0 on our UNDER 9.5
        #        (sharp agrees), FD opened 10.5 (disagrees). Flipped OVER lost; UNDER 9.5
        #        would have won (final 8 runs).
        # Flipped picks are flagged in context_factors as 'FADE_FLIP' so monitoring/agents
        # can track them specifically — CRITICAL for catching if this strategy breaks down.
        try:
            _ncaa_dk_kept = []
            _ncaa_dk_skipped = []
            _ncaa_dk_flipped = []
            _ncaa_dk_flip_blocked = []  # v25.27: flip candidates where a sharp agreed
            for p in all_picks:
                if not (p.get('sport') == 'baseball_ncaa'
                        and p.get('market_type') == 'TOTAL'
                        and p.get('book') == 'DraftKings'):
                    _ncaa_dk_kept.append(p)
                    continue
                eid = p.get('event_id', '')
                bet_line = p.get('line')
                sel = p.get('selection', '') or ''
                is_under = 'UNDER' in sel.upper()
                rows = conn.execute(
                    "SELECT book, line FROM openers WHERE event_id=? AND market='totals' "
                    "AND book IN ('FanDuel','BetRivers')", (eid,)
                ).fetchall()
                fd_open = next((l for b, l in rows if b == 'FanDuel'), None)
                br_open = next((l for b, l in rows if b == 'BetRivers'), None)

                if bet_line is None:
                    _ncaa_dk_kept.append(p)
                    continue

                # (1) Tight consensus check — both sharps match DK line exactly
                tight = False
                if fd_open is not None and fd_open == bet_line:
                    if br_open is None or br_open == bet_line:
                        tight = True
                elif br_open is not None and br_open == bet_line and fd_open is None:
                    tight = True

                if tight:
                    _ncaa_dk_skipped.append((p, f'all books at {bet_line}'))
                    print(f"  ⚠ NCAA_DK_TIGHT_SKIP: {sel[:50]} — all books at {bet_line} (market efficient)")
                    continue

                # (2a) v25.27: if any sharp strictly AGREES with the model direction,
                # block the flip — split-sharp-with-agreement is outside the backtest regime.
                sharp_agrees = False
                agree_src = None
                if is_under:
                    if fd_open is not None and fd_open < bet_line:
                        sharp_agrees, agree_src = True, f'FD opened {fd_open} < bet UNDER {bet_line}'
                    elif br_open is not None and br_open < bet_line:
                        sharp_agrees, agree_src = True, f'BR opened {br_open} < bet UNDER {bet_line}'
                else:  # OVER
                    if fd_open is not None and fd_open > bet_line:
                        sharp_agrees, agree_src = True, f'FD opened {fd_open} > bet OVER {bet_line}'
                    elif br_open is not None and br_open > bet_line:
                        sharp_agrees, agree_src = True, f'BR opened {br_open} > bet OVER {bet_line}'

                # (2) Sharp disagreement → FLIP (Option C). Only evaluate if no sharp agrees.
                flip_reason = None
                if not sharp_agrees:
                    if is_under:
                        if fd_open is not None and fd_open > bet_line:
                            flip_reason = f'FD opened {fd_open} > bet UNDER {bet_line} (sharp prices OVER)'
                        elif br_open is not None and br_open > bet_line:
                            flip_reason = f'BR opened {br_open} > bet UNDER {bet_line} (sharp prices OVER)'
                    else:  # OVER
                        if fd_open is not None and fd_open < bet_line:
                            flip_reason = f'FD opened {fd_open} < bet OVER {bet_line} (sharp prices UNDER)'
                        elif br_open is not None and br_open < bet_line:
                            flip_reason = f'BR opened {br_open} < bet OVER {bet_line} (sharp prices UNDER)'
                elif sharp_agrees:
                    # Log the blocked flip for monitoring — were we about to flip?
                    would_flip = False
                    if is_under:
                        would_flip = (fd_open is not None and fd_open > bet_line) or (br_open is not None and br_open > bet_line)
                    else:
                        would_flip = (fd_open is not None and fd_open < bet_line) or (br_open is not None and br_open < bet_line)
                    if would_flip:
                        _ncaa_dk_flip_blocked.append((p, agree_src))
                        print(f"  🛑 NCAA_DK_FLIP_BLOCKED: {sel[:45]} — sharp agrees with model ({agree_src})")

                if flip_reason:
                    # Flip selection + fetch opposite-side odds at DK for same line
                    if is_under:
                        new_sel = sel.replace('UNDER', 'OVER')
                        opp_marker = 'OVER'
                    else:
                        new_sel = sel.replace('OVER', 'UNDER')
                        opp_marker = 'UNDER'
                    # Look up latest DK odds for the opposite side at same line
                    opp_row = conn.execute("""
                        SELECT odds FROM odds o WHERE event_id=? AND market='totals'
                        AND book='DraftKings' AND line=?
                        AND UPPER(selection) LIKE ?
                        AND NOT EXISTS (
                            SELECT 1 FROM odds o2 WHERE o2.event_id=o.event_id AND o2.book=o.book
                            AND o2.market=o.market AND o2.selection=o.selection AND o2.line=o.line
                            AND (o2.snapshot_date > o.snapshot_date
                                 OR (o2.snapshot_date=o.snapshot_date AND o2.snapshot_time > o.snapshot_time))
                        )
                        LIMIT 1
                    """, (eid, bet_line, f'%{opp_marker}%')).fetchone()
                    new_odds = opp_row[0] if opp_row else p.get('odds')
                    # Global MIN_ODDS policy — never fire at worse than -150 even on a flip
                    from config import MIN_ODDS as _FF_MIN_ODDS
                    _eff_flip_odds = new_odds if new_odds is not None else -110
                    if _eff_flip_odds <= _FF_MIN_ODDS:
                        print(f"  ⚠ NCAA_DK_FADE_FLIP skipped: flip side {new_sel[:35]} @ {_eff_flip_odds:+.0f} worse than MIN_ODDS {_FF_MIN_ODDS}")
                        # Drop the original too (sharp disagreed, flip price unacceptable → no play)
                        continue
                    # Build flipped pick
                    p_new = dict(p)
                    p_new['selection'] = new_sel
                    p_new['odds'] = new_odds
                    prior_ctx = (p_new.get('context', '') or '').strip()
                    fade_tag = f'FADE_FLIP: {flip_reason}'
                    p_new['context'] = f'{prior_ctx} | {fade_tag}'.strip(' |') if prior_ctx else fade_tag
                    p_new['side_type'] = 'FADE_FLIP'  # makes it queryable
                    _ncaa_dk_flipped.append((p, p_new, flip_reason))
                    _ncaa_dk_kept.append(p_new)
                    print(f"  🔄 NCAA_DK_FADE_FLIP: {sel[:45]} → {new_sel[:45]} @ {new_odds} ({flip_reason})")
                    continue

                # Default: keep as-is (sharp agrees or no sharp data)
                _ncaa_dk_kept.append(p)

            # Log skips and flips to shadow_blocked_picks
            _now = datetime.now().isoformat()
            for _p, _rsn in _ncaa_dk_skipped:
                conn.execute("""INSERT INTO shadow_blocked_picks
                    (created_at, sport, event_id, selection, market_type, book, line, odds, edge_pct, units, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (_now, _p.get('sport',''), _p.get('event_id',''),
                     _p.get('selection',''), _p.get('market_type',''), _p.get('book',''),
                     _p.get('line'), _p.get('odds'), _p.get('edge_pct', 0),
                     _p.get('units', 0), f'NCAA_DK_TIGHT_SKIP ({_rsn})'))
            for _p_orig, _p_new, _rsn in _ncaa_dk_flipped:
                conn.execute("""INSERT INTO shadow_blocked_picks
                    (created_at, sport, event_id, selection, market_type, book, line, odds, edge_pct, units, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (_now, _p_orig.get('sport',''), _p_orig.get('event_id',''),
                     _p_orig.get('selection',''), _p_orig.get('market_type',''), _p_orig.get('book',''),
                     _p_orig.get('line'), _p_orig.get('odds'), _p_orig.get('edge_pct', 0),
                     _p_orig.get('units', 0),
                     f'NCAA_DK_FADE_FLIP ({_rsn}) → betting {_p_new["selection"]}'))
            for _p, _agree in _ncaa_dk_flip_blocked:
                conn.execute("""INSERT INTO shadow_blocked_picks
                    (created_at, sport, event_id, selection, market_type, book, line, odds, edge_pct, units, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (_now, _p.get('sport',''), _p.get('event_id',''),
                     _p.get('selection',''), _p.get('market_type',''), _p.get('book',''),
                     _p.get('line'), _p.get('odds'), _p.get('edge_pct', 0),
                     _p.get('units', 0), f'NCAA_DK_FLIP_BLOCKED (sharp agrees: {_agree})'))
            if _ncaa_dk_skipped or _ncaa_dk_flipped or _ncaa_dk_flip_blocked:
                conn.commit()
            if _ncaa_dk_flipped:
                print(f"  🔄 Option C active: {len(_ncaa_dk_flipped)} NCAA DK pick(s) flipped to opposite side")
            all_picks = _ncaa_dk_kept
        except Exception as e:
            print(f"  NCAA DK gate: {e}")

        # ═══ NCAA BASEBALL SHARP LINE-MOVE GATE (v25.26) ═══
        # Runs on ALL NCAA Baseball TOTAL picks regardless of book.
        # If FanDuel's current total has moved ≥ 1.5 runs AGAINST our bet direction
        # since its opener, sharp money has pounded the other side — skip.
        # Rationale: bet 954 (TTU@Utah OVER 15.5 at BetRivers) was scrubbed 4/17
        # because FD moved 17.5→15.5 (sharp on UNDER) and no book-scoped gate caught it.
        try:
            _lm_kept, _lm_blocked = [], []
            for p in all_picks:
                if not (p.get('sport') == 'baseball_ncaa' and p.get('market_type') == 'TOTAL'):
                    _lm_kept.append(p); continue
                _eid = p.get('event_id', '')
                _bet_line = p.get('line')
                _sel = p.get('selection', '') or ''
                _is_under = 'UNDER' in _sel.upper()
                if _bet_line is None:
                    _lm_kept.append(p); continue
                _fd_open_r = conn.execute(
                    "SELECT line FROM openers WHERE event_id=? AND market='totals' AND book='FanDuel' LIMIT 1",
                    (_eid,)).fetchone()
                if not _fd_open_r:
                    _lm_kept.append(p); continue
                _fd_open = _fd_open_r[0]
                _fd_cur_r = conn.execute("""
                    SELECT line FROM odds o WHERE event_id=? AND market='totals' AND book='FanDuel'
                    AND NOT EXISTS (
                        SELECT 1 FROM odds o2 WHERE o2.event_id=o.event_id AND o2.book=o.book
                        AND o2.market=o.market AND o2.selection=o.selection
                        AND (o2.snapshot_date > o.snapshot_date
                             OR (o2.snapshot_date=o.snapshot_date AND o2.snapshot_time > o.snapshot_time))
                    ) LIMIT 1
                """, (_eid,)).fetchone()
                if not _fd_cur_r:
                    _lm_kept.append(p); continue
                _fd_cur = _fd_cur_r[0]
                _move = _fd_cur - _fd_open  # + = line went up
                _against = False
                _rsn = ''
                if _is_under and _move >= 1.5:
                    _against = True
                    _rsn = f'FD total climbed {_fd_open} → {_fd_cur} (+{_move:.1f}, sharp on OVER)'
                elif (not _is_under) and _move <= -1.5:
                    _against = True
                    _rsn = f'FD total dropped {_fd_open} → {_fd_cur} ({_move:+.1f}, sharp on UNDER)'
                if _against:
                    _lm_blocked.append((p, _rsn))
                    print(f"  ⚠ NCAA_SHARP_LINE_MOVE: {_sel[:50]} — {_rsn}")
                else:
                    _lm_kept.append(p)
            if _lm_blocked:
                _now = datetime.now().isoformat()
                for _p, _rsn in _lm_blocked:
                    conn.execute("""INSERT INTO shadow_blocked_picks
                        (created_at, sport, event_id, selection, market_type, book, line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (_now, _p.get('sport',''), _p.get('event_id',''),
                         _p.get('selection',''), _p.get('market_type',''), _p.get('book',''),
                         _p.get('line'), _p.get('odds'), _p.get('edge_pct', 0),
                         _p.get('units', 0), f'NCAA_SHARP_LINE_MOVE ({_rsn})'))
                conn.commit()
            all_picks = _lm_kept
        except Exception as e:
            print(f"  NCAA sharp line-move gate: {e}")

        saved_picks = save_picks_to_db(conn, all_picks)
        if saved_picks is not None:
            all_picks = saved_picks  # Only use picks that actually saved to DB
    print_picks(all_picks)
    _log.info(f"Step 6: Predictions complete | {len(all_picks)} picks")

    # v26.0 Phase 8: Step 6b extracted to pipeline.run_steps.model_b_shadow.
    from pipeline.run_steps.model_b_shadow import tag_with_model_b
    _model_b_report = tag_with_model_b(conn, all_picks)

    # Step 8: Log picks
    try:
        from pick_logger import log_picks
        log_picks(all_picks, run_type)
    except Exception as e:
        print(f"  Logging: {e}")

    # v26.0 Phase 8: Step 9 email block extracted to pipeline.run_steps.email_send.
    from pipeline.run_steps.email_send import prepare_and_send_email
    _skip_remainder, png_card_path, png_card_paths = prepare_and_send_email(
        conn, all_picks, do_email, run_type, total_odds_fetched,
        _step_timings, _step6_breakdown, research_brief, _model_b_report,
        _log, _mark)
    if _skip_remainder:
        return


    # v26.0 Phase 8: Step 9c extracted to pipeline.run_steps.social_post.
    from pipeline.run_steps.social_post import post_to_social
    post_to_social(all_picks, png_card_path, png_card_paths)
    # v25.3: Step 10 (Twitter/X content) removed — @Scottys_Edge account
    # permanently suspended April 2026. Discord + Instagram only.

    conn.close()
    _log.info(f"=== {run_type} Run END | {len(all_picks)} picks ===")


def _compute_opener_move_for_pick(conn, p):
    """v26.0 Phase 3: thin wrapper kept for backwards compatibility.

    Logic moved to pipeline.score_helpers.compute_opener_move_for_pick.
    """
    from pipeline.score_helpers import compute_opener_move_for_pick
    return compute_opener_move_for_pick(conn, p)


def _merge_and_select(game_picks, prop_picks, conn=None):
    """v26.0 Phase 5: extracted to pipeline.stage_5_merge.merge_and_select.
    Thin wrapper preserved so existing imports (`from main import _merge_and_select`)
    keep working — used by tests/shadow_predict.py and a few archived scripts.
    """
    from pipeline.stage_5_merge import merge_and_select
    return merge_and_select(game_picks, prop_picks, conn=conn)


# ═══════════════════════════════════════════════════════════════════
# OTHER COMMANDS
# ═══════════════════════════════════════════════════════════════════

def cmd_grade(args):
    import sqlite3
    from datetime import datetime, timedelta
    from grader import daily_grade_and_report
    do_email = has_flag(args, '--email')
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")

    # v26.0 Phase 8: score fetching extracted to pipeline.run_steps.grade_scores.
    from pipeline.run_steps.grade_scores import fetch_all_scores
    fetch_all_scores(ALL_SPORTS)

    # ═══ Run the actual grader ═══
    report = daily_grade_and_report(conn)

    # v25: Post-grade quick analysis — auto-generates a 5-line summary
    # with CLV flags, same-game correlation, and model error detection
    try:
        _grade_bets = conn.execute("""
            SELECT selection, sport, result, pnl_units, clv, market_type, event_id, odds
            FROM graded_bets
            WHERE DATE(graded_at) = (SELECT MAX(DATE(graded_at)) FROM graded_bets
                WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5)
            AND result IN ('WIN','LOSS','PUSH')
            ORDER BY pnl_units DESC
        """).fetchall()
        if _grade_bets:
            _gw = sum(1 for b in _grade_bets if b[2] == 'WIN')
            _gl = sum(1 for b in _grade_bets if b[2] == 'LOSS')
            _gpnl = sum(b[3] for b in _grade_bets)
            _day_label = 'GREEN DAY' if _gpnl > 0 else 'RED DAY'

            _clv_vals = [b[4] for b in _grade_bets if b[4] is not None]
            _avg_clv = sum(_clv_vals) / len(_clv_vals) if _clv_vals else 0
            _neg_clv = [b for b in _grade_bets if b[4] is not None and b[4] < -3.0]
            _pos_clv = [b for b in _grade_bets if b[4] is not None and b[4] > 3.0]

            # Detect same-game correlation (multiple picks on same event)
            _event_counts = {}
            for b in _grade_bets:
                if b[6]:
                    _event_counts[b[6]] = _event_counts.get(b[6], 0) + 1
            _correlated = {k: v for k, v in _event_counts.items() if v >= 2}

            # Bankroll tracking
            from config import BANKROLL_START, UNIT_VALUE
            _season_units = conn.execute("SELECT COALESCE(SUM(pnl_units), 0) FROM graded_bets WHERE result IN ('WIN','LOSS','PUSH') AND units >= 3.5").fetchone()[0]
            _bankroll_pnl = _season_units * UNIT_VALUE
            _bankroll_current = BANKROLL_START + _bankroll_pnl
            _day_dollars = _gpnl * UNIT_VALUE

            _qa_lines = []
            _dd_sign = '+' if _day_dollars >= 0 else '-'
            _bp_sign = '+' if _bankroll_pnl >= 0 else '-'
            _qa_lines.append(f"{_gw}W-{_gl}L {_gpnl:+.1f}u ({_dd_sign}${abs(_day_dollars):,.0f}) \u2014 {_day_label}")
            _qa_lines.append(f"Bankroll: ${_bankroll_current:,.0f} (started ${BANKROLL_START:,} | {_bp_sign}${abs(_bankroll_pnl):,.0f})")

            if _neg_clv:
                _qa_lines.append(f"CLV flags: {len(_neg_clv)} pick(s) with CLV < -3% (model error)")
                for b in _neg_clv:
                    _qa_lines.append(f"  {b[0][:40]} CLV:{b[4]:+.1f}%")
            if _pos_clv:
                _qa_lines.append(f"Sharp reads: {len(_pos_clv)} pick(s) with CLV > +3% (market confirmed)")
            if _correlated:
                _qa_lines.append(f"Same-game correlation: {len(_correlated)} event(s) with 2+ picks")
            _qa_lines.append(f"Avg CLV: {_avg_clv:+.1f}%")

            _qa_text = '\n'.join(_qa_lines)
            print(f"\n  POST-GRADE ANALYSIS:\n  {'=' * 40}")
            for _ql in _qa_lines:
                print(f"  {_ql}")

            # Write to file for email inclusion
            _qa_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'quick_grade_analysis.txt')
            with open(_qa_path, 'w', encoding='utf-8') as _qf:
                _qf.write(_qa_text)
    except Exception as e:
        print(f"  Post-grade analysis: {e}")

    # Auto Elo rebuild
    print("  Rebuilding Elo ratings...")
    try:
        from elo_engine import build_all_elo
        build_all_elo()
        print("  Elo ratings updated")
    except Exception as e:
        print(f"  Elo rebuild: {e}")

    # Auto team ratings rebuild (off/def power ratings from results)
    print("  Rebuilding team ratings...")
    try:
        from team_ratings_engine import build_all_team_ratings
        build_all_team_ratings()
        print("  Team ratings updated")
    except Exception as e:
        print(f"  Team ratings rebuild: {e}")

    # Soccer standings refresh (FREE — ESPN API)
    print("  Updating soccer standings...")
    try:
        from soccer_standings import fetch_standings
        fetch_standings(verbose=False)
        print("  Soccer standings updated")
    except Exception as e:
        print(f"  Soccer standings: {e}")

    # Pitcher data update (FREE — ESPN box scores)
    print("  Updating pitcher data...")
    try:
        from pitcher_scraper import scrape_pitcher_data, build_pitching_quality
        from pitcher_scraper import scrape_mlb_pitchers, scrape_mlb_pitcher_history
        scrape_pitcher_data(days_back=3, verbose=False)
        scrape_mlb_pitcher_history(days_back=3, verbose=False)
        build_pitching_quality(verbose=False)
        scrape_mlb_pitchers(verbose=False)
        print("  Pitcher data updated (college + MLB)")
    except Exception as e:
        print(f"  Pitcher data: {e}")

    # NHL goalie data update (FREE — ESPN scoreboard)
    print("  Updating NHL goalie data...")
    try:
        from pitcher_scraper import scrape_nhl_goalies, scrape_nhl_goalie_history
        scrape_nhl_goalies(verbose=False)
        scrape_nhl_goalie_history(days_back=3, verbose=False)
        print("  NHL goalie data updated")
    except Exception as e:
        print(f"  NHL goalie data: {e}")

    # Referee data update (FREE — ESPN game summaries)
    print("  Updating referee data...")
    try:
        from referee_engine import scrape_officials
        for ref_sport in ['basketball_nba', 'basketball_ncaab', 'icehockey_nhl']:
            scrape_officials(ref_sport, days_back=3, verbose=False)
        print("  Referee data updated")
    except Exception as e:
        print(f"  Referee data: {e}")

    # Generate PNG cards
    card_paths = []
    try:
        from card_image import generate_results_card, generate_stats_card
        results_pngs = generate_results_card(conn, start_date='2026-03-04')
        if results_pngs:
            if isinstance(results_pngs, list):
                card_paths.extend(results_pngs)
            else:
                card_paths.append(results_pngs)
        stats_png = generate_stats_card(conn, start_date='2026-03-04')
        if stats_png:
            card_paths.append(stats_png)
    except Exception as e:
        print(f"  PNG cards: {e}")

    # Generate captions
    results_caption = ""
    try:
        _game_date = conn.execute("SELECT MAX(DATE(created_at)) FROM graded_bets WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5").fetchone()[0]
        _game_dt = datetime.strptime(_game_date, '%Y-%m-%d')
        _date_str = _game_dt.strftime('%A %B %d')
        _yb = conn.execute("SELECT selection, result, pnl_units, sport FROM graded_bets WHERE DATE(created_at) = (SELECT MAX(DATE(created_at)) FROM graded_bets WHERE result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5) AND result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5 ORDER BY pnl_units DESC").fetchall()
        _all = conn.execute("SELECT result, pnl_units FROM graded_bets WHERE DATE(created_at) >= '2026-03-04' AND result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5").fetchall()
        _yw = sum(1 for b in _yb if b[1]=='WIN')
        _yl = sum(1 for b in _yb if b[1]=='LOSS')
        _yp = sum(b[2] or 0 for b in _yb)
        _tw = sum(1 for b in _all if b[0]=='WIN')
        _tl = sum(1 for b in _all if b[0]=='LOSS')
        _tp = sum(b[1] or 0 for b in _all)
        _twp = _tw/(_tw+_tl)*100 if (_tw+_tl)>0 else 0
        _lines = []
        _sport_counts = {}
        for b in _yb:
            icon = "\u2705" if b[1]=='WIN' else "\u274c"
            _lines.append(f"{icon} {b[0]} | {b[2]:+.1f}u")
            sp = b[3] or ''
            if 'basketball_nba' in sp: _sport_counts['nba'] = _sport_counts.get('nba', 0) + 1
            elif 'basketball_ncaab' in sp: _sport_counts['ncaab'] = _sport_counts.get('ncaab', 0) + 1
            elif 'hockey' in sp: _sport_counts['nhl'] = _sport_counts.get('nhl', 0) + 1
            elif 'baseball' in sp: _sport_counts['baseball'] = _sport_counts.get('baseball', 0) + 1
            elif 'soccer' in sp: _sport_counts['soccer'] = _sport_counts.get('soccer', 0) + 1
        # Build sport-aware hashtags (IG only — Twitter removed v25.3)
        _sport_ig = {
            'nba': ['#NBA', '#NBABets', '#NBAPicksToday'],
            'ncaab': ['#CBB', '#CollegeBasketball', '#MarchMadness'],
            'nhl': ['#NHL', '#NHLBets', '#HockeyBets'],
            'baseball': ['#CollegeBaseball', '#NCAACWS', '#BaseballBets'],
            'soccer': ['#Soccer', '#SoccerBets', '#FootballBets'],
        }
        # March Madness override for NCAAB in March/early April
        _now_m = datetime.now().month
        _now_d = datetime.now().day
        _is_march_madness = 'ncaab' in _sport_counts and (_now_m == 3 or (_now_m == 4 and _now_d <= 7))
        if _is_march_madness:
            _sport_ig['ncaab'] = ['#MarchMadness', '#CollegeBasketball', '#CBBPicks']
        # Sort sports by frequency, pick top 2
        _top_sports = sorted(_sport_counts, key=_sport_counts.get, reverse=True)[:2]
        _ig_sport_tags = []
        for s in _top_sports:
            _ig_sport_tags.extend(_sport_ig.get(s, []))
        # Dedupe while preserving order
        _ig_sport_tags = list(dict.fromkeys(_ig_sport_tags))
        # IG: core discoverable tags + up to 4 sport tags + community (max ~10)
        _ig_hashtags = ['#SportsBetting', '#BettingPicks', '#FreePicks', '#GamblingTwitter'] + _ig_sport_tags[:4] + ['#BettingCommunity', '#PicksOfTheDay']
        _ig_hashtags = list(dict.fromkeys(_ig_hashtags))
        # Build sport emojis
        _emoji_map = {'nba': '\U0001f3c0', 'ncaab': '\U0001f3c0', 'nhl': '\U0001f3d2', 'baseball': '\u26be', 'soccer': '\u26bd'}
        _sport_emojis = ''.join(dict.fromkeys(_emoji_map.get(s, '') for s in _top_sports))
        if _yp >= 10: verdict = "\U0001f525 HUGE DAY"
        elif _yp >= 0: verdict = "\u2705 GREEN DAY"
        elif _yp >= -5: verdict = "Minor loss"
        else: verdict = "Tough day. Full transparency \u2014 every pick tracked."
        ig = f"{_sport_emojis} Scotty's Edge \u2014 {_date_str} Results\n\n"
        ig += f"{_yw}W-{_yl}L | {_yp:+.1f}u"
        if _yp >= 10: ig += " \U0001f525"
        ig += f"\n\n{verdict}\n\n"
        ig += "\n".join(_lines)
        from config import BANKROLL_START, UNIT_VALUE
        _br_pnl = _tp * UNIT_VALUE
        _br_current = BANKROLL_START + _br_pnl
        _br_sign = '+' if _br_pnl >= 0 else '-'
        ig += f"\n\nSeason: {_tw}W-{_tl}L | {_tp:+.1f}u ({_br_sign}${abs(_br_pnl):,.0f}) | {_twp:.1f}%"
        ig += f"\nBankroll: ${_br_current:,.0f}"
        ig += "\nEvery pick tracked & graded \U0001f4ca"
        if _yp < 0:
            ig += "\n\nIt's all part of the game."
        ig += "\n\n\u26a0\ufe0f Not gambling advice \u2022 21+ \u2022 1-800-GAMBLER"
        ig += f"\n\nFull stats: scarusillo.github.io/scottys-edge/dashboard.html"
        ig += f"\n\nFollow for daily picks:\n\U0001f4f1 IG: @scottys_edge\n\U0001f4ac Discord: discord.gg/JQ6rRfuN\n\n{' '.join(_ig_hashtags)}"
        # v25.3: Twitter caption block removed — @Scottys_Edge suspended April 2026.
        results_caption = "INSTAGRAM CAPTION:\n" + "="*40 + "\n" + ig
        # v25.34: persist caption to disk so Reel posts can read the current
        # caption without re-querying the DB. Previously _reel_caption.txt
        # was only written by ad-hoc scripts.
        try:
            _rc_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards', '_reel_caption.txt')
            with open(_rc_path, 'w', encoding='utf-8') as _rcf:
                _rcf.write(ig)
        except Exception as _rc_e:
            print(f"  Caption file write: {_rc_e}")
        print("  Captions generated")
    except Exception as e:
        print(f"  Captions: {e}")

    conn.close()

    if do_email:
        from emailer import send_email, send_grading_email
        today = datetime.now().strftime('%Y-%m-%d')

        # ── Collect agent reports for consolidated email ──
        agent_conn = sqlite3.connect(db)
        agent_sections = []

        # Agent: Verification — catch misgrades before they reach followers
        try:
            from agent_verify import run_all_checks
            verify_report, issue_count = run_all_checks(agent_conn)
            if issue_count > 0:
                agent_sections.append(f"⚠️ VERIFICATION: {issue_count} issues\n{verify_report}")
                # ALSO send standalone alert for urgency
                send_email(f"VERIFICATION ALERT - {issue_count} issues - {today}", verify_report)
                print(f"  Verification: {issue_count} issues — alert sent + included in grade")
            else:
                agent_sections.append("✅ VERIFICATION: All checks passed")
                print("  Verification: all checks passed")
        except Exception as e:
            print(f"  Verification agent: {e}")

        # Agent: Analyst — morning briefing (only on AM run, skip PM)
        if datetime.now().hour < 12:
            try:
                from agent_analyst import generate_briefing
                briefing = generate_briefing(agent_conn)
                agent_sections.append(f"📊 MORNING BRIEFING:\n{briefing}")
                print("  Analyst briefing: included in grade email")
            except Exception as e:
                print(f"  Analyst agent: {e}")
        else:
            print("  Analyst briefing: skipped (PM run)")

        # Agent: Tournament — NCAA March Madness monitor (active Mar 9 - Apr 7)
        try:
            month = datetime.now().month
            day = datetime.now().day
            if (month == 3 and day >= 9) or (month == 4 and day <= 7):
                from agent_tournament import generate_tournament_report
                tourn_report = generate_tournament_report(agent_conn)
                agent_sections.append(f"🏆 TOURNAMENT MONITOR:\n{tourn_report}")
                print("  Tournament agent: included in grade email")
        except Exception as e:
            print(f"  Tournament agent: {e}")

        # Agent: Volume — daily pick volume analysis
        try:
            from agent_volume import generate_volume_report
            vol_report = generate_volume_report(agent_conn)
            if 'Volume is low' in vol_report or 'near-misses' in vol_report:
                agent_sections.append(f"⚠️ VOLUME:\n{vol_report}")
                print("  Volume agent: low volume — included in grade email")
            else:
                agent_sections.append("📈 VOLUME: Healthy")
                print("  Volume agent: volume healthy")
        except Exception as e:
            print(f"  Volume agent: {e}")

        # Agent: Totals — over/under model health
        try:
            from agent_totals import find_totals_gap
            gap, last_date = find_totals_gap(agent_conn)
            if gap and gap >= 5:
                from agent_totals import generate_totals_report
                tot_report = generate_totals_report(agent_conn)
                agent_sections.append(f"⚠️ TOTALS: {gap} day gap\n{tot_report}")
                print(f"  Totals agent: {gap} day gap — included in grade email")
            else:
                agent_sections.append(f"🎯 TOTALS: Last pick {gap or '?'} days ago — OK")
                print(f"  Totals agent: OK")
        except Exception as e:
            print(f"  Totals agent: {e}")

        # Agent: Growth — weekly digest + milestones + content insights
        try:
            from agent_growth import check_milestones, generate_full_report

            milestones = check_milestones(agent_conn)
            if milestones:
                milestone_text = "\n".join([f"  • [{m['type'].upper()}] {m['caption']}" for m in milestones])
                agent_sections.append(f"🏅 MILESTONES:\n{milestone_text}")
                print(f"  Growth agent: {len(milestones)} milestones")
            else:
                agent_sections.append("🏅 MILESTONES: None today")

            if datetime.now().weekday() == 6:
                report_text = generate_full_report(agent_conn)
                agent_sections.append(f"📱 WEEKLY GROWTH:\n{report_text}")
                print("  Growth agent: weekly report included")
        except Exception as e:
            print(f"  Growth agent: {e}")

        # Agent: Sport Review — per-sport deep analysis
        try:
            from agent_sport_review import generate_sport_review
            review_report, review_alerts = generate_sport_review(agent_conn)
            if review_report:
                agent_sections.append(f"📋 SPORT REVIEW:\n{review_report}")
                print(f"  Sport review: {len(review_alerts)} alerts")
        except Exception as e:
            print(f"  Sport review agent: {e}")

        agent_conn.close()

        # Build consolidated agent block
        agent_block = ""
        if agent_sections:
            agent_block = "\n\n" + "═" * 50 + "\n"
            agent_block += "  AGENT BRIEFING\n"
            agent_block += "═" * 50 + "\n\n"
            agent_block += "\n\n".join(agent_sections)

        # Email 1: HTML results card + PNG attachments (with agent briefing appended)
        results_html_content = None
        if report:
            try:
                results_path, results_html_content = _generate_results_html(report)
            except Exception as e:
                results_html_content = None
                print(f"  Results HTML: {e}")

        # Generate Kling video prompt so it can be included in the email
        kling_section = ""
        try:
            _kling_conn = sqlite3.connect(db)
            _generate_kling_prompt(_kling_conn)
            _kling_conn.close()
            _kling_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards', 'kling_prompt.txt')
            if os.path.exists(_kling_path):
                with open(_kling_path, 'r', encoding='utf-8') as _kf:
                    _kling_text = _kf.read()
                kling_section = f"\n\n{'='*60}\n  KLING VIDEO PROMPT\n{'='*60}\n\n{_kling_text}"
                print(f"  Kling prompt saved + included in grade email")
        except Exception as e:
            print(f"  Kling prompt: {e}")

        # Generate P&L dashboard (updates docs/dashboard.html for GitHub Pages)
        dashboard_section = ""
        try:
            from generate_dashboard import generate as gen_dashboard
            gen_dashboard()
            dashboard_section = f"\n\n{'='*60}\nP&L DASHBOARD: https://scarusillo.github.io/scottys-edge/dashboard.html\n{'='*60}"
        except Exception as e:
            print(f"  Dashboard: {e}")

        grade_report = (report or "") + agent_block + kling_section + dashboard_section
        email_ok = send_grading_email(grade_report, html_body=results_html_content, attachment_paths=card_paths)
        if not email_ok:
            print("  ❌ EMAIL FAILED — grades were saved but not delivered. Check GMAIL_APP_PASSWORD env var.")

        # Email 2: Captions + PNG cards (so you can copy captions & post images from phone)
        if results_caption:
            if card_paths:
                from emailer import _send_multi_attachment
                _send_multi_attachment(f"Social Captions - {today}", results_caption, card_paths)
            else:
                send_email(f"Social Captions - {today}", results_caption)
            print("  Captions email sent")

        # Email 3: Daily diagnostic (warnings + factor analysis)
        try:
            import subprocess
            diag_script = os.path.join(os.path.dirname(__file__), 'sport_diagnostic.py')
            if os.path.exists(diag_script):
                result_diag = subprocess.run(
                    ['python', diag_script, '--short'],
                    capture_output=True, text=True, encoding='utf-8', timeout=30
                )
                diag_output = result_diag.stdout
                if diag_output and 'WARNING' in diag_output:
                    send_email(f"Diagnostic Warnings - {today}", diag_output)
                    print("  Diagnostic warnings email sent")
                else:
                    print("  Diagnostic: no warnings")
        except Exception as e:
            print(f"  Diagnostic: {e}")

    # Post results to Discord + Instagram (carousel: wins, losses, stats cards)
    # Video Reel is posted separately by user with Kling AI video
    if report:
        try:
            from social_media import post_results_social
            post_results_social(report)
        except Exception as e:
            print(f"  Discord results: {e}")

        # Instagram: post results cards as carousel (NOT video — that's separate)
        try:
            from social_media import post_results_to_instagram
            if card_paths:
                post_results_to_instagram(card_paths, report)
        except Exception as e:
            print(f"  Instagram results carousel: {e}")

        # Kling prompt already generated and included in grade email above

    # Auto-update landing page stats + results (GitHub Pages)
    try:
        from update_landing_page import get_stats, get_recent_results, build_results_html, update_html
        _lp_conn = sqlite3.connect(db)
        _lp_overall, _lp_sports = get_stats(_lp_conn)
        _lp_days = get_recent_results(_lp_conn, days=3)
        _lp_conn.close()
        _lp_results_html = build_results_html(_lp_days)
        _w, _l, _pnl, _wp, _roi = update_html(_lp_overall, _lp_sports, results_html=_lp_results_html)
        import subprocess as _sp
        _sp.run(['git', '-C', os.path.join(os.path.dirname(__file__), '..'), 'add', 'docs/index.html'], capture_output=True)
        _sp.run(['git', '-C', os.path.join(os.path.dirname(__file__), '..'), 'commit', '-m',
                 f'Update landing page stats: {_w}W-{_l}L +{_pnl}u'], capture_output=True)
        _sp.run(['git', '-C', os.path.join(os.path.dirname(__file__), '..'), 'push'], capture_output=True)
        print(f"  Landing page updated: {_w}W-{_l}L | +{_pnl}u | {_wp}% | ROI +{_roi}%")
    except Exception as e:
        print(f"  Landing page: {e}")

    # Export briefing data for cloud agent (lightweight JSON, not full DB)
    # Also generate local morning briefing markdown
    try:
        from export_briefing_data import export_data, generate_local_briefing
        export_data()
        generate_local_briefing()
        # v25.89: write daily gate-observability card (closes "didn't-fire" blind spot)
        try:
            from gate_observability import write_daily_card
            _gh_conn = sqlite3.connect(db)
            _gh_path = write_daily_card(_gh_conn)
            _gh_conn.close()
            print(f"  Gate health card: {_gh_path}")
        except Exception as _ge:
            print(f"  Gate health card: {_ge}")
        import subprocess as _bp
        _repo = os.path.join(os.path.dirname(__file__), '..')
        _bp.run(['git', '-C', _repo, 'add', 'data/briefing_data.json', 'data/morning_briefing.md', 'data/gate_health.md'], capture_output=True)
        _bp.run(['git', '-C', _repo, 'commit', '-m',
                 f'Update briefing data {datetime.now().strftime("%Y-%m-%d")}'], capture_output=True)
        # v24: Pull --rebase before push to handle concurrent pushes (e.g. pages workflow)
        # Without this, push silently fails if origin advanced since our last pull
        _bp.run(['git', '-C', _repo, 'pull', '--rebase'], capture_output=True)
        _push = _bp.run(['git', '-C', _repo, 'push'], capture_output=True)
        if _push.returncode != 0:
            print(f"  ⚠ git push failed: {_push.stderr.decode(errors='replace').strip()}")
            # Retry once after pull
            _bp.run(['git', '-C', _repo, 'pull', '--rebase'], capture_output=True)
            _bp.run(['git', '-C', _repo, 'push'], capture_output=True)
    except Exception as e:
        print(f"  Briefing data export: {e}")

    # v26.0 Phase 8: trajectory backfill extracted to pipeline.run_steps.trajectory_backfill.
    from pipeline.run_steps.trajectory_backfill import backfill_trajectory_features
    backfill_trajectory_features(db)

    # v26.0 Phase 8: data retention extracted to pipeline.run_steps.data_retention.
    from pipeline.run_steps.data_retention import prune_to_archive
    prune_to_archive(db)

    # Upload slim DB to GitHub Releases for cloud agents
    try:
        from upload_db import create_slim_db, upload_to_github
        create_slim_db()
        upload_to_github()
    except Exception as e:
        print(f"  DB upload: {e}")

    # v25.3: Twitter results thread removed — account suspended April 2026.


# v25.3: cmd_twitter() removed — @Scottys_Edge account suspended April 2026.
# tweet_formatter.py archived to scripts/archive/.

COMMANDS = {
    'run': cmd_run, 'opener': cmd_opener, 'snapshot': cmd_snapshot, 'predict': cmd_predict,
    'props': cmd_props, 'grade': cmd_grade, 'report': cmd_report,
    'bootstrap': cmd_bootstrap, 'scores': cmd_scores, 'injuries': cmd_injuries,
    'reboot-ratings': cmd_reboot_ratings, 'email-test': cmd_email_test,
    'run-soccer': cmd_run_soccer,
    'budget': cmd_budget, 'log': cmd_log, 'setup-scheduler': cmd_setup_scheduler,
    'historical': cmd_historical, 'elo': cmd_elo, 'fix-names': cmd_fix_names,
    'backtest': cmd_backtest, 'scrub': cmd_scrub, 'unscrub': cmd_unscrub,
}


def main():
    if len(sys.argv) < 2:
        print(__doc__); return
    cmd = sys.argv[1]
    if cmd in COMMANDS:
        COMMANDS[cmd](sys.argv[2:])
    else:
        print(f"Unknown command: {cmd}"); print(__doc__)

if __name__ == '__main__':
    try:
        main()
    except Exception as _uncaught_e:
        import traceback, logging
        _tb = traceback.format_exc()
        # Always dump tracebacks to pipeline.log — scheduled tasks don't capture
        # stderr so without this, uncaught exceptions are invisible.
        try:
            _err_log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          '..', 'data', 'pipeline.log')
            with open(_err_log_path, 'a', encoding='utf-8') as _f:
                from datetime import datetime as _dt
                _ts = _dt.now().strftime('%Y-%m-%d %H:%M:%S')
                _f.write(f"{_ts}  UNCAUGHT EXCEPTION: {_uncaught_e}\n")
                for _line in _tb.splitlines():
                    _f.write(f"{_ts}    {_line}\n")
        except Exception:
            pass
        # Also try to print to stderr (for manual runs)
        try:
            print(f"\nUNCAUGHT EXCEPTION: {_uncaught_e}\n{_tb}", file=sys.stderr)
        except Exception:
            pass
        sys.exit(1)
