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
  python main.py run --twitter          Run + generate Twitter thread + visual card
  python main.py run --email --twitter  Run + email + Twitter (full pipeline)
  python main.py opener                 Capture opening lines (for CLV)
  python main.py opener --email         Opener + email confirmation
  python main.py predict                Model only (FREE, no odds fetch)
  python main.py props                  Fetch + evaluate player props
  python main.py grade                  Grade yesterday's bets + CLV
  python main.py grade --email          Grade + email report
  python main.py grade --twitter        Grade + generate results thread
  python main.py twitter                Generate Twitter thread from today's picks
  python main.py twitter --results      Generate results thread from graded bets
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

def cmd_opener(args):
    """
    Capture opening lines for all today's games.
    This is the CLV baseline — we compare our bet lines to closing lines.
    Low API cost: just odds + scores (~18 calls).
    """
    from datetime import datetime
    do_email = has_flag(args, '--email')
    sports = get_sports(args)

    # Auto-detect active tennis tournaments
    if not any(s.startswith('tennis_') for s in sports):
        try:
            active_tennis = _detect_tennis_sports()
            if active_tennis:
                sports.extend(active_tennis)
        except Exception:
            pass

    print("="*60)
    print(f"  📌 OPENING LINE CAPTURE — {datetime.now().strftime('%Y-%m-%d %I:%M %p ') + ('EDT' if 3 <= __import__('datetime').datetime.now().month <= 10 else 'EST')}")
    print("="*60)

    # Fetch scores (grade yesterday's games)
    print("\n📊 Fetching scores...")
    try:
        from odds_api import fetch_scores
        for sp in sports:
            try: fetch_scores(sp, days_back=3)
            except Exception as e: print(f"  {sp}: {e}")
    except Exception as e: print(f"  {e}")

    # Fetch current odds (these become "openers" via auto-capture)
    print("\n📈 Capturing opening lines...")
    try:
        from odds_api import fetch_odds
        for sp in sports:
            try: fetch_odds(sp, tag='OPENER')
            except Exception as e: print(f"  {sp}: {e}")
    except Exception as e: print(f"  {e}")

    # v12 FIX: NO prop fetching at opener. Props haven't stabilized at 8am
    # and cost ~279 usage per run. Props are fetched at 11am and 5pm runs only.
    print("\n✅ Opening lines captured — CLV baseline set (props at 11am)")

    if do_email:
        try:
            from emailer import send_email
            send_email("📌 Opening Lines Captured",
                f"Opening lines captured at {datetime.now().strftime('%I:%M %p EST')}.\n"
                f"CLV baseline set for today's games.\n"
                f"Picks coming at 11 AM and 5:30 PM.")
        except Exception: pass


# ═══════════════════════════════════════════════════════════════════
# SNAPSHOT — Pre-game odds capture (6:30 PM EST)
# ═══════════════════════════════════════════════════════════════════

def cmd_snapshot(args):
    """
    Capture a pre-game odds snapshot closer to tip-off.
    
    Our CLV "closing line" is the last snapshot before gametime.
    Running at 6:30 PM gets us within 30-90 mins of tip for most games,
    much better than the 5:00 PM run's 2+ hour gap.
    
    Ultra-low API cost: ~11 calls (just odds, no props, no picks).
    No picks generated. No model runs. Just odds data for CLV accuracy.
    """
    from datetime import datetime
    sports = get_sports(args)

    print("=" * 60)
    print(f"  📸 PRE-GAME SNAPSHOT — {datetime.now().strftime('%Y-%m-%d %I:%M %p ') + ('EDT' if 3 <= __import__('datetime').datetime.now().month <= 10 else 'EST')}")
    print("=" * 60)

    print("\n📈 Capturing pre-game odds...")
    try:
        from odds_api import fetch_odds
        for sp in sports:
            try:
                fetch_odds(sp, tag='SNAPSHOT')
                print(f"  ✅ {sp}")
            except Exception as e:
                print(f"  {sp}: {e}")
    except Exception as e:
        print(f"  {e}")

    print("\n✅ Pre-game snapshot captured — CLV closing lines updated.")
    print("   These become the 'closing line' for tonight's games.")


# ═══════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════
# ARB SCANNER — Cross-book arbitrage detection
# ═══════════════════════════════════════════════════════════════════

NY_LEGAL_BOOKS_ARB = {'DraftKings', 'FanDuel', 'BetMGM', 'Caesars', 'BetRivers',
                      'Fanatics', 'ESPN BET', 'PointsBet'}

def _american_to_decimal(odds):
    if odds > 0: return 1 + odds / 100
    elif odds < 0: return 1 + 100 / abs(odds)
    return 1

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

    # Step 1: Scores
    print("\n📊 Step 1: Fetching scores...")
    try:
        from odds_api import fetch_scores
        for sp in sports:
            try: fetch_scores(sp, days_back=3)
            except Exception as e: print(f"  {sp}: {e}")
    except Exception as e: print(f"  {e}")
    _log.info("Step 1: Scores fetch complete")

    # Step 2: Injuries (FREE)
    print("\n🏥 Step 2: Injuries (FREE)...")
    try:
        from injury_scraper import fetch_and_apply_all
        fetch_and_apply_all()
    except Exception as e: print(f"  {e}")

    # Step 3: Fetch fresh odds so predictions use CURRENT market lines.
    # Stale lines produce stale picks (e.g., Lakers +3.5 when market moved to +6.5).
    # The model should always evaluate against what subscribers can actually bet NOW.
    print("\n📈 Step 3: Fetching current odds...")
    total_odds_fetched = 0
    try:
        from odds_api import fetch_odds
        for sp in sports:
            try:
                fetch_odds(sp, tag='CURRENT')
            except Exception as e:
                print(f"  {sp}: {e}")
    except Exception as e:
        print(f"  Odds fetch: {e}")

    # Health check: count how many odds rows were stored this run
    try:
        _hc_db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
        _hc_conn = sqlite3.connect(_hc_db)
        _today = datetime.now().strftime('%Y-%m-%d')
        total_odds_fetched = _hc_conn.execute(
            "SELECT COUNT(*) FROM odds WHERE snapshot_date = ? AND tag = 'CURRENT'",
            (_today,)
        ).fetchone()[0]
        _hc_conn.close()
    except Exception as e:
        print(f"  Health check DB error: {e}")

    if total_odds_fetched == 0:
        print("\n" + "!" * 60)
        print("  ⚠ WARNING: ZERO odds rows fetched across ALL sports!")
        print("  Possible API outage or off-day.")
        print("!" * 60)
        try:
            from emailer import send_email as _alert_email
            _alert_email(
                "⚠ ALERT: Zero odds fetched — possible API outage",
                f"Scotty's Edge {run_type} Run at {datetime.now().strftime('%I:%M %p')}\n\n"
                f"Zero odds data was fetched across all {len(sports)} sports.\n"
                f"This could indicate an API outage or a legitimate off-day.\n\n"
                f"Sports checked: {', '.join(sports)}\n"
                f"Pipeline will continue but picks may be empty."
            )
        except Exception as e:
            print(f"  Alert email failed: {e}")

    _log.info(f"Step 3: Odds fetch complete | {total_odds_fetched} rows")

    # Step 5: Player Props
    print("\n🎯 Step 4: Player props...")
    try:
        from odds_api import fetch_props
        for sp in sports:
            if sp in PROP_SPORTS:
                try: fetch_props(sp)
                except Exception as e: print(f"  {sp} props error: {e}")
            else:
                print(f"  {sp}: props not available")
    except Exception as e: print(f"  Props: {e}")

    # Step 4b: Pitcher data (FREE — ESPN box scores + day-of-week quality)
    if any('baseball' in s for s in sports):
        print("\n⚾ Step 4b: Pitcher data (FREE)...")
        try:
            from pitcher_scraper import scrape_pitcher_data, build_pitching_quality, scrape_mlb_pitchers
            scrape_pitcher_data(days_back=3, verbose=True)
            build_pitching_quality(verbose=True)
            # MLB probable pitchers — MUST run before predictions so the
            # pitcher gate in model_engine can skip games with TBD starters
            if any(s == 'baseball_mlb' for s in sports):
                print("  Fetching MLB probable pitchers...")
                scrape_mlb_pitchers(verbose=True)
        except Exception as e:
            print(f"  Pitcher scraper: {e}")

    # Step 4b2: NHL goalie data (FREE — ESPN scoreboard)
    if any(s == 'icehockey_nhl' for s in sports):
        print("\n\U0001f3d2 Step 4b2: NHL goalie data (FREE)...")
        try:
            from pitcher_scraper import scrape_nhl_goalies
            scrape_nhl_goalies(verbose=True)
        except Exception as e:
            print(f"  NHL goalie scraper: {e}")

    # Step 4c: Referee/official data (FREE — ESPN game summaries)
    print("\n🏛️ Step 4c: Referee data (FREE)...")
    try:
        from referee_engine import scrape_officials
        for ref_sport in ['basketball_nba', 'basketball_ncaab', 'icehockey_nhl']:
            scrape_officials(ref_sport, days_back=3, verbose=False)
        print("  Referee data updated")
    except Exception as e:
        print(f"  Referee engine: {e}")

    # Step 5: Bootstrap missing ratings (FREE)
    print("\n🔧 Step 5: Ratings check...")
    from bootstrap_ratings import bootstrap_all
    bootstrap_all()

    # Step 5b: Elo ratings from game results (FREE — independent of market)
    print("\n🏆 Step 5b: Elo ratings from results...")
    try:
        from elo_engine import build_elo_ratings, get_elo_ratings
        import sqlite3 as _sq
        _conn = _sq.connect(os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db'))
        for sp in sports:
            elo_data = get_elo_ratings(_conn, sp)
            if elo_data:
                confident = sum(1 for v in elo_data.values() if v['confidence'] != 'LOW')
                print(f"  ✅ {sp}: {confident} teams with Elo confidence")
            else:
                # Try building from results
                results_count = _conn.execute(
                    "SELECT COUNT(*) FROM results WHERE sport=? AND completed=1", (sp,)
                ).fetchone()[0]
                if results_count >= 20:
                    build_elo_ratings(sp, verbose=True)
                else:
                    print(f"  ⚠ {sp}: {results_count} results — need 20+ for Elo (run: python historical_scores.py)")
        _conn.close()
    except Exception as e:
        print(f"  Elo: {e} (run historical_scores.py + elo_engine.py to enable)")

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
    for sp in resolved_sports:
        picks = generate_predictions(conn, sport=sp)
        game_picks.extend(picks)

    # Step 7a: Player Props — Edge Consensus Method
    print("\n🎯 Step 7: Player Props — Edge Consensus Analysis...")
    consensus_props = []
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

    # Step 7b: Player Props — Projection Model
    print("\n🔮 Step 7b: Player Props — Projection Model...")
    model_props = []
    try:
        from player_prop_model import generate_prop_projections
        model_props = generate_prop_projections(conn)
        if model_props:
            print(f"  ✅ {len(model_props)} projection-based prop picks")
    except Exception as e:
        print(f"  Props projection: {e}")
        import traceback; traceback.print_exc()

    # Step 7c: Merge consensus + model props (dedup: keep higher edge)
    prop_picks = _merge_prop_sources(consensus_props, model_props)

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
        posted_event_ids = set(row[3] for row in already_posted if row[3])
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
                        edge_pct REAL, units REAL, reason TEXT
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

        save_picks_to_db(conn, all_picks)
    print_picks(all_picks)
    _log.info(f"Step 6: Predictions complete | {len(all_picks)} picks")

    # Step 8: Log picks
    try:
        from pick_logger import log_picks
        log_picks(all_picks, run_type)
    except Exception as e:
        print(f"  Logging: {e}")

    # Step 9: Email (with inline HTML card)
    html_content = None
    if all_picks:
        try:
            html_path, html_content = _generate_html_card(all_picks)
        except Exception as e:
            html_path = None
            html_content = None
            print(f"  HTML card: {e}")

    # Generate PNG card
    png_card_path = None
    png_card_paths = []
    if all_picks:
        try:
            from card_image import generate_card_image
            result = generate_card_image(all_picks)
            if isinstance(result, list):
                png_card_paths = result
                png_card_path = result[0]  # Primary card for email attachment
            else:
                png_card_path = result
                png_card_paths = [result]
        except Exception as e:
            print(f"  PNG card: {e}")

    if do_email:
        print("\n📧 Step 9: Sending email...")
        if all_picks:
            from emailer import send_picks_email, send_email
            text = picks_to_text(all_picks, f"{run_type} Picks")
            # Append research intel to picks email
            if research_brief:
                text += "\n\n" + "═" * 50 + "\n"
                text += "  PRE-GAME INTEL\n"
                text += "═" * 50 + "\n\n"
                text += research_brief
            social = _social_media_card(all_picks)
            full_text = text + "\n\n" + social
            email_ok = send_picks_email(full_text, run_type, html_body=html_content,
                            attachment_path=png_card_path,
                            attachment_paths=png_card_paths if len(png_card_paths) > 1 else None)
            if not email_ok:
                print("  ❌ EMAIL FAILED — picks were saved but not delivered. Check GMAIL_APP_PASSWORD env var.")

            # Separate caption email (plain text, copyable from phone)
            try:
                from card_image import generate_caption, generate_pick_writeups, generate_thread
                ig_caption = generate_caption(all_picks)
                if ig_caption:
                    tw_caption = ig_caption.split("\n")[0]
                    tw_caption += f"\n\n{len([p for p in all_picks if p.get('units',0)>=4.5])} plays locked in. Every pick tracked."
                    tw_caption += "\n\n#ScottysEdge #SportsBetting"

                    # Per-pick write-ups for engagement posts
                    writeups = generate_pick_writeups(all_picks)

                    caption_text = "INSTAGRAM CAPTION:\n" + "="*40 + "\n" + ig_caption
                    caption_text += "\n\n" + "TWITTER CAPTION:\n" + "="*40 + "\n" + tw_caption
                    if writeups:
                        caption_text += "\n\n" + "INDIVIDUAL PICK POSTS (copy-paste for engagement):\n" + "="*40 + writeups

                    # Twitter threads — multi-tweet deep analysis per pick
                    thread_text = generate_thread(all_picks)
                    if thread_text:
                        caption_text += "\n\n" + "TWITTER THREADS (copy-paste each tweet separately):\n" + "="*40 + thread_text

                    # v17: Growth playbook — accounts to engage, ready-to-post content
                    _season = conn.execute("""
                        SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
                               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
                               SUM(pnl_units)
                        FROM graded_bets WHERE DATE(created_at) >= '2026-03-04'
                        AND result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5
                    """).fetchone()
                    _sw, _sl, _sp = _season[0] or 0, _season[1] or 0, _season[2] or 0
                    _wr = _sw/(_sw+_sl)*100 if (_sw+_sl) > 0 else 0
                    # Use all today's picks (including earlier runs) for the caption playbook
                    _all_today = conn.execute("""
                        SELECT selection, odds, units FROM bets
                        WHERE created_at >= ? AND result IS NULL ORDER BY units DESC
                    """, (today,)).fetchall() if 'today' in dir() else []
                    _best_pick = _all_today[0] if _all_today else (all_picks[0] if all_picks else None)
                    _bp_sel = _best_pick[0] if _best_pick and isinstance(_best_pick, tuple) else (_best_pick['selection'] if _best_pick else 'Check card')
                    _bp_odds_raw = _best_pick[1] if _best_pick and isinstance(_best_pick, tuple) else (_best_pick.get('odds') if _best_pick else None)
                    _bp_odds = f"({_bp_odds_raw:+.0f})" if _bp_odds_raw else ''

                    # v24: Reddit post — standalone for r/sportsbetting + thread comments
                    _pick_lines = []
                    for _p in sorted(all_picks, key=lambda x: x.get('units', 0), reverse=True):
                        if _p.get('units', 0) >= 3.5:
                            _odds_str = f"({_p['odds']:+.0f})" if _p.get('odds') else ''
                            _u = _p.get('units', 0)
                            _tag = ' — MAX PLAY' if _u >= 5.0 else ''
                            _pick_lines.append(f"**{_p['selection']}** {_odds_str} {_u:.0f}u{_tag}")
                    if _pick_lines:
                        _today_str = datetime.now().strftime('%A %m/%d')
                        _reddit_body = f"Title: {_today_str} Picks — {_sw}W-{_sl}L ({_wr:.0f}%) season, all tracked\n\n"
                        _reddit_body += "Body:\n\n"
                        _reddit_body += f"{len(_pick_lines)} plays for {datetime.now().strftime('%A')}. Full transparency — every pick graded, every loss shown.\n\n"
                        _reddit_body += "\n\n".join(_pick_lines)
                        _reddit_body += f"\n\nSeason: {_sw}-{_sl} ({_wr:.0f}%) | {_sp:+.0f}u\n\n"
                        _reddit_body += "All picks tracked at scottys_edge on IG. Discord: discord.gg/JQ6rRfuN\n\n"
                        _reddit_body += "---\nPost in: r/sportsbetting (standalone), r/sportsbook (daily thread comment)"
                        caption_text += "\n\n" + "REDDIT POST (r/sportsbetting):\n" + "="*40 + "\n" + _reddit_body

                    growth_section = f"""

GROWTH PLAYBOOK
{'='*40}

ACCOUNTS TO TAG (on your image, not caption):
  IG: @actionnetworkhq @baborofficial @bettingcappers @vegasinsider
  Twitter: @ActionNetworkHQ @BettingPros @covers @PrizePicks @br_betting

ACCOUNTS TO COMMENT ON (within 30 min of their posts):
  @ActionNetworkHQ @ESPNBet @BleacherReport — reply with your model's take

READY-TO-TWEET (copy-paste):
{'='*40}

Tweet 1 (Free pick — post BEFORE games start):
Today's free MAX PLAY: {_bp_sel} {_bp_odds}

{_sw}W-{_sl}L ({_wr:.0f}%) | {_sp:+.0f}u on the season. Every pick tracked.

Full card in bio.

#SportsBetting #FreePicks #BettingTwitter

Tweet 2 (Quote-tweet a big account's game preview):
Our model has {_bp_sel} as the biggest edge on the board tonight.

{_sw}W-{_sl}L this season. Data-driven, no gut picks.

Tweet 3 (Engagement — reply to injury/line news):
This is exactly why we have {_bp_sel} today. The model saw this edge before the line moved.

{_sw}W-{_sl}L season. Link in bio.

COMMENT TEMPLATE (for big account posts):
{'='*40}
"Our model agrees — [their pick] is the play. {_sw}W-{_sl}L on the season, all tracked."
"Model disagrees here — we have {_bp_sel} as the value side. {_wr:.0f}% win rate this season."

TONIGHT'S CHECKLIST:
{'='*40}
[ ] Post picks card to IG feed + story (tag 4 accounts ON image)
[ ] Tweet free MAX PLAY (Tweet 1 above)
[ ] Quote-tweet 1 big account with your take (Tweet 2)
[ ] Comment on 2 big account posts (within 30 min)
[ ] After wins hit: post results card + "Called it" story
"""
                    caption_text += growth_section

                    # v24: Timing confidence tags — early picks historically capture better CLV
                    try:
                        _current_hour = datetime.now().hour
                        _timing_lines = []
                        for _p in all_picks:
                            if _p.get('units', 0) < 3.5:
                                continue
                            _sel = _p.get('selection', '')
                            if _current_hour < 8:
                                _timing_lines.append(f"  EARLY LINE CAPTURE: {_sel} — historically +1.04 avg CLV, 61% WR before 8am")
                            elif _current_hour < 11:
                                _timing_lines.append(f"  MORNING CAPTURE: {_sel} — lines still settling, monitor for movement")
                            elif _current_hour >= 17:
                                _timing_lines.append(f"  LATE ENTRY: {_sel} — evening picks historically 52% WR, lines fully baked")
                        if _timing_lines:
                            caption_text += f"\n\nTIMING INSIGHT\n{'='*40}\n"
                            caption_text += '\n'.join(_timing_lines)
                            if _current_hour < 11:
                                caption_text += "\n\n  Early/morning picks capture the most CLV. These are your highest-conviction windows."
                            elif _current_hour >= 17:
                                caption_text += "\n\n  Evening picks have thinner edges — market has had all day to settle. Size conservatively."
                    except Exception:
                        pass

                    # v24: Arb scanner — find cross-book arbitrage opportunities
                    try:
                        arb_section = _scan_arbs(conn)
                        if arb_section:
                            caption_text += arb_section
                    except Exception as _arb_e:
                        print(f"  Arb scan: {_arb_e}")

                    today = datetime.now().strftime('%Y-%m-%d')
                    send_email(f"Social Captions - {run_type} {today}", caption_text)
                    print("  Captions + pick write-ups email sent")
            except Exception as e:
                print(f"  Captions: {e}")
        else:
            # No picks found — send no-edge card + captions on the 11am and 5:30pm
            # scheduled runs only. Other hours (8am opener, ad-hoc) skip to avoid clutter.
            _hour = datetime.now().hour
            if _hour not in (10, 11, 17):
                print("  No new picks — skipping email (only 11am/5:30pm runs send no-edge cards)")
                return
            from emailer import send_picks_email, send_email
            from datetime import datetime
            try:
                from card_image import generate_card_image, generate_caption
                no_edge_path = generate_card_image([])  # Generates no-edge card
                today = datetime.now().strftime('%Y-%m-%d')
                no_edge_msg = "No plays today — model didn't find enough edge."
                if total_odds_fetched == 0:
                    no_edge_msg += "\n\nNote: Zero odds data was available — this may indicate an API outage rather than a genuine no-edge day."
                email_ok = send_picks_email(no_edge_msg, run_type, attachment_path=no_edge_path)
                if not email_ok:
                    print("  ❌ EMAIL FAILED — picks were saved but not delivered. Check GMAIL_APP_PASSWORD env var.")
                # Caption email
                caption = generate_caption([])
                if caption:
                    caption_text = "INSTAGRAM CAPTION:\n" + "="*40 + "\n" + caption + "\n\n" + "TWITTER CAPTION:\n" + "="*40 + "\nNo plays tonight.\n\nDiscipline is the edge. We only bet when the data says to bet.\n\nBack tomorrow.\n\n#SportsBetting #FreePicks #BettingCommunity"
                    send_email(f"Social Captions - {run_type} {today}", caption_text)
                print("  No-edge card + caption sent")
            except Exception as e:
                print(f"  No-edge email: {e}")

    _log.info(f"Step 9: Email {'sent' if do_email else 'skipped'}")

    # Step 9c: Auto-post to Discord + Twitter + Instagram
    if all_picks:
        try:
            from social_media import post_picks_social
            post_picks_social(all_picks)
        except Exception as e:
            print(f"  Social media: {e}")
        # Instagram: post card images if available
        try:
            from social_media import post_picks_to_instagram
            if png_card_paths:
                post_picks_to_instagram(png_card_paths, all_picks)
            elif png_card_path:
                post_picks_to_instagram([png_card_path], all_picks)
        except Exception as e:
            print(f"  Instagram: {e}")

    # Step 10: Twitter/X content
    do_twitter = has_flag(args, '--twitter')
    if do_twitter:
        print("\n🐦 Step 10: Generating Twitter content...")
        if all_picks:
            from tweet_formatter import generate_tweet_thread, copy_thread_to_clipboard, save_card
            tweets = generate_tweet_thread(all_picks)
            copy_thread_to_clipboard(tweets)
            save_card(all_picks)
        else:
            print("  No picks to format.")

    conn.close()
    _log.info(f"=== {run_type} Run END | {len(all_picks)} picks ===")


def _generate_html_card(picks):
    """Generate a screenshot-ready HTML pick card and save to desktop."""
    from datetime import datetime, timedelta
    from model_engine import _to_eastern, _eastern_tz_label, kelly_label
    
    tz = _eastern_tz_label()
    now = datetime.now()
    date_str = now.strftime('%B %d, %Y')
    day_str = now.strftime('%A').upper()
    
    # Group picks by sport
    sport_groups = {}
    sport_icons = {
        'basketball_nba': '🏀', 'basketball_ncaab': '🏀',
        'icehockey_nhl': '🏒',
        'baseball_ncaa': '⚾',
        'soccer_epl': '⚽', 'soccer_germany_bundesliga': '⚽',
        'soccer_france_ligue_one': '⚽', 'soccer_italy_serie_a': '⚽',
        'soccer_spain_la_liga': '⚽', 'soccer_usa_mls': '⚽',
        'soccer_uefa_champs_league': '⚽', 'soccer_mexico_ligamx': '⚽',
    }
    sport_labels = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL', 'baseball_ncaa': 'NCAA BASEBALL',
        'soccer_epl': 'EPL', 'soccer_germany_bundesliga': 'BUNDESLIGA',
        'soccer_france_ligue_one': 'LIGUE 1', 'soccer_italy_serie_a': 'SERIE A',
        'soccer_spain_la_liga': 'LA LIGA', 'soccer_usa_mls': 'MLS',
        'soccer_uefa_champs_league': 'UCL', 'soccer_mexico_ligamx': 'LIGA MX',
    }
    # Tennis: dynamically add icons and labels from config
    try:
        from config import TENNIS_SPORTS, TENNIS_LABELS
        for _tk in TENNIS_SPORTS:
            sport_icons[_tk] = '🎾'
            sport_labels[_tk] = TENNIS_LABELS.get(_tk, _tk.split('_')[-1].upper())
    except ImportError:
        pass
    
    for p in picks:
        sp = p.get('sport', 'other')
        label = sport_labels.get(sp, sp.upper())
        if label not in sport_groups:
            sport_groups[label] = {'icon': sport_icons.get(sp, '🏟️'), 'picks': []}
        sport_groups[label]['picks'].append(p)
    
    # Build pick HTML blocks — grouped by sport, sorted within each group
    pick_blocks = []

    # Render sport sections in a consistent order
    sport_order = ['NBA', 'NHL', 'NCAAB', 'NCAA BASEBALL',
                   'EPL', 'LA LIGA', 'SERIE A', 'BUNDESLIGA', 'LIGUE 1', 'MLS', 'LIGA MX', 'UCL',
                   # Tennis tournaments (added dynamically but need ordering)
                   'AUS OPEN', 'FRENCH OPEN', 'WIMBLEDON', 'US OPEN',
                   'INDIAN WELLS', 'MIAMI OPEN', 'MONTE CARLO', 'MADRID OPEN',
                   'ITALIAN OPEN', 'CANADIAN OPEN', 'CINCINNATI', 'SHANGHAI',
                   'PARIS MASTERS', 'DUBAI', 'QATAR OPEN', 'CHINA OPEN',
                   'AUS OPEN (W)', 'FRENCH OPEN (W)', 'WIMBLEDON (W)', 'US OPEN (W)',
                   'INDIAN WELLS (W)', 'MIAMI OPEN (W)', 'MADRID OPEN (W)',
                   'ITALIAN OPEN (W)', 'CANADIAN OPEN (W)', 'CINCINNATI (W)',
                   'DUBAI (W)', 'QATAR OPEN (W)', 'CHINA OPEN (W)', 'WUHAN OPEN']

    for sport_label in sport_order:
        if sport_label not in sport_groups:
            continue
        sg = sport_groups[sport_label]
        sport_picks = sorted(sg['picks'], key=lambda p: p['units'], reverse=True)

        pick_blocks.append(f"""
    <div class="sport-header">{sport_label}</div>""")

        for p in sport_picks:
            kl = kelly_label(p['units'])
            sp = p.get('sport', 'other')
            icon = sport_icons.get(sp, '🏟️')
            game_time = ''
            if p.get('commence'):
                try:
                    gt = datetime.fromisoformat(p['commence'].replace('Z', '+00:00'))
                    est = _to_eastern(gt)
                    game_time = est.strftime('%I:%M %p') + f' {tz}'
                except Exception:
                    pass

            conv_class = 'conviction-max' if kl == 'MAX PLAY' else 'conviction-strong' if kl == 'STRONG' else 'conviction-solid'
            ctx_html = f'<div class="pick-context">📍 {p.get("context", "")}</div>' if p.get('context') else ''

            pick_blocks.append(f"""
    <div class="pick">
      <div class="pick-icon">{icon}</div>
      <div class="pick-info">
        <div class="pick-team">{p['selection']}</div>
        <div class="pick-matchup">{p['home']} vs {p['away']} • {game_time}</div>
        {ctx_html}
      </div>
      <div class="pick-meta">
        <div class="pick-odds">{p['odds']:+.0f}</div>
        <div class="pick-units">{p['units']:.1f} units</div>
        <div class="pick-conviction {conv_class}">{kl}</div>
      </div>
    </div>""")

    # Add any ungrouped sports
    grouped_labels = set(sport_order)
    for sport_label, sg in sport_groups.items():
        if sport_label not in grouped_labels:
            sport_picks = sorted(sg['picks'], key=lambda p: p['units'], reverse=True)
            pick_blocks.append(f"""
    <div class="sport-header">{sport_label}</div>""")
            for p in sport_picks:
                kl = kelly_label(p['units'])
                sp = p.get('sport', 'other')
                icon = sport_icons.get(sp, '🏟️')
                game_time = ''
                if p.get('commence'):
                    try:
                        gt = datetime.fromisoformat(p['commence'].replace('Z', '+00:00'))
                        est = _to_eastern(gt)
                        game_time = est.strftime('%I:%M %p') + f' {tz}'
                    except Exception:
                        pass
                conv_class = 'conviction-max' if kl == 'MAX PLAY' else 'conviction-strong' if kl == 'STRONG' else 'conviction-solid'
                ctx_html = f'<div class="pick-context">📍 {p.get("context", "")}</div>' if p.get('context') else ''
                pick_blocks.append(f"""
    <div class="pick">
      <div class="pick-icon">{icon}</div>
      <div class="pick-info">
        <div class="pick-team">{p['selection']}</div>
        <div class="pick-matchup">{p['home']} vs {p['away']} • {game_time}</div>
        {ctx_html}
      </div>
      <div class="pick-meta">
        <div class="pick-odds">{p['odds']:+.0f}</div>
        <div class="pick-units">{p['units']:.1f} units</div>
        <div class="pick-conviction {conv_class}">{kl}</div>
      </div>
    </div>""")

    picks_html = '\n'.join(pick_blocks)
    tu = sum(p['units'] for p in picks)
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scotty's Edge — {date_str}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Barlow:wght@400;500;600;700&family=Barlow+Condensed:wght@400;600;700&display=swap');
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    background: #0a0a0a;
    display: flex; justify-content: center; align-items: center;
    min-height: 100vh; padding: 40px;
    font-family: 'Barlow', sans-serif;
  }}
  .card {{
    width: 1080px;
    background: linear-gradient(165deg, #0d1117 0%, #0f1923 40%, #111d2b 100%);
    border-radius: 24px; overflow: hidden; position: relative;
    border: 1px solid rgba(255,255,255,0.06);
  }}
  .card::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 4px;
    background: linear-gradient(90deg, #00e676, #00c853, #69f0ae);
  }}
  .card::after {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0;
    background-image: linear-gradient(rgba(255,255,255,0.02) 1px, transparent 1px),
      linear-gradient(90deg, rgba(255,255,255,0.02) 1px, transparent 1px);
    background-size: 40px 40px; pointer-events: none;
  }}
  .header {{
    padding: 48px 52px 32px; position: relative; z-index: 1;
    display: flex; justify-content: space-between; align-items: flex-start;
  }}
  .brand {{ display: flex; flex-direction: column; gap: 4px; }}
  .logo {{
    font-family: 'Bebas Neue', sans-serif; font-size: 52px;
    color: #ffffff; letter-spacing: 3px; line-height: 1;
  }}
  .logo span {{ color: #00e676; }}
  .subtitle {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 15px;
    color: rgba(255,255,255,0.35); letter-spacing: 4px;
    text-transform: uppercase; font-weight: 600;
  }}
  .date-block {{ text-align: right; }}
  .date-day {{
    font-family: 'Bebas Neue', sans-serif; font-size: 28px;
    color: rgba(255,255,255,0.8); letter-spacing: 2px;
  }}
  .date-full {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 14px;
    color: rgba(255,255,255,0.3); letter-spacing: 2px; text-transform: uppercase;
  }}
  .divider {{
    height: 1px; background: linear-gradient(90deg, transparent, rgba(0,230,118,0.3), transparent);
    margin: 0 52px; position: relative; z-index: 1;
  }}
  .picks-section {{ padding: 36px 52px 20px; position: relative; z-index: 1; }}
  .section-label {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 13px;
    color: #00e676; letter-spacing: 4px; text-transform: uppercase;
    font-weight: 700; margin-bottom: 24px;
  }}
  .pick {{
    display: flex; align-items: center; padding: 24px 28px;
    background: rgba(255,255,255,0.03); border-radius: 16px;
    margin-bottom: 16px; border: 1px solid rgba(255,255,255,0.04);
    position: relative; overflow: hidden;
  }}
  .pick::before {{
    content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 4px;
    background: #00e676; border-radius: 0 4px 4px 0;
  }}
  .pick-icon {{ font-size: 32px; margin-right: 20px; min-width: 40px; text-align: center; }}
  .pick-info {{ flex: 1; }}
  .pick-team {{
    font-family: 'Barlow', sans-serif; font-size: 22px; font-weight: 700;
    color: #ffffff; margin-bottom: 4px;
  }}
  .pick-matchup {{
    font-family: 'Barlow', sans-serif; font-size: 14px;
    color: rgba(255,255,255,0.4); margin-bottom: 4px;
  }}
  .pick-context {{ font-size: 12px; color: #00e676; opacity: 0.7; font-weight: 500; }}
  .sport-header {{
    font-family: 'Bebas Neue', sans-serif; font-size: 24px;
    color: rgba(255,255,255,0.7); letter-spacing: 3px;
    margin-top: 20px; margin-bottom: 12px; padding-bottom: 8px;
    border-bottom: 1px solid rgba(255,255,255,0.08);
    position: relative; z-index: 1;
  }}
  .pick-meta {{ text-align: right; min-width: 140px; }}
  .pick-odds {{ font-family: 'Bebas Neue', sans-serif; font-size: 32px; color: #ffffff; line-height: 1; }}
  .pick-units {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 14px;
    color: rgba(255,255,255,0.5); letter-spacing: 1px; margin-top: 4px;
  }}
  .pick-conviction {{
    display: inline-block; font-family: 'Barlow Condensed', sans-serif;
    font-size: 11px; font-weight: 700; letter-spacing: 2px;
    text-transform: uppercase; padding: 3px 10px; border-radius: 4px; margin-top: 6px;
  }}
  .conviction-max {{ background: rgba(0,230,118,0.15); color: #00e676; }}
  .conviction-strong {{ background: rgba(255,193,7,0.15); color: #ffc107; }}
  .conviction-solid {{ background: rgba(100,181,246,0.15); color: #64b5f6; }}
  .footer {{
    padding: 28px 52px 20px; position: relative; z-index: 1;
    display: flex; justify-content: space-between; align-items: center;
  }}
  .footer-left {{ display: flex; gap: 32px; }}
  .stat {{ text-align: center; }}
  .stat-value {{ font-family: 'Bebas Neue', sans-serif; font-size: 28px; color: #ffffff; }}
  .stat-label {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 11px;
    color: rgba(255,255,255,0.3); letter-spacing: 2px; text-transform: uppercase;
  }}
  .footer-right {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 13px;
    color: rgba(255,255,255,0.25); letter-spacing: 2px; text-transform: uppercase;
  }}
  .disclaimer {{
    padding: 20px 52px 32px; position: relative; z-index: 1;
    border-top: 1px solid rgba(255,255,255,0.04);
  }}
  .disclaimer p {{
    font-family: 'Barlow', sans-serif; font-size: 9px;
    color: rgba(255,255,255,0.2); line-height: 1.5; text-align: center;
  }}
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div class="brand">
      <div class="logo">SCOTTY'S <span>EDGE</span></div>
      <div class="subtitle">Data-Driven Sports Picks</div>
    </div>
    <div class="date-block">
      <div class="date-day">{day_str}</div>
      <div class="date-full">{date_str}</div>
    </div>
  </div>
  <div class="divider"></div>
  <div class="picks-section">
    <div class="section-label">Today's Plays</div>
    {picks_html}
  </div>
  <div class="divider"></div>
  <div class="footer">
    <div class="footer-left">
      <div class="stat">
        <div class="stat-value">{len(picks)}</div>
        <div class="stat-label">Plays</div>
      </div>
      <div class="stat">
        <div class="stat-value">{tu:.0f}u</div>
        <div class="stat-label">Total</div>
      </div>
    </div>
    <div class="footer-right">Every pick tracked & graded</div>
  </div>
  <div class="disclaimer">
    <p>For entertainment and informational purposes only. Not gambling advice. Past performance does not guarantee future results. Please gamble responsibly. If you or someone you know has a gambling problem, call 1-800-GAMBLER. Must be 21+ and located in a legal jurisdiction. Scotty's Edge does not accept or place bets on behalf of users.</p>
  </div>
</div>
</body>
</html>"""
    
    # Save to desktop
    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')
    if not os.path.exists(desktop):
        desktop = os.path.join(os.path.expanduser('~'), 'OneDrive', 'Desktop')
    
    filepath = os.path.join(desktop, 'scottys_edge_picks.html')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  📱 Social media card saved: {filepath}")
    return filepath, html


def _generate_results_html(report_text):
    """Generate a public-facing HTML results card for email/Instagram."""
    import re
    from datetime import datetime
    
    now = datetime.now()
    date_str = now.strftime('%B %d, %Y')
    day_str = now.strftime('%A').upper()
    
    # Parse key stats from report
    record = re.search(r'Record: (\d+)W-(\d+)L', report_text)
    pl = re.search(r'P/L: ([+-]?\d+\.\d+)u', report_text)
    roi = re.search(r'ROI: ([+-]?\d+\.\d+)%', report_text)
    
    wins = int(record.group(1)) if record else 0
    losses = int(record.group(2)) if record else 0
    total = wins + losses
    pct = f"{wins/(total)*100:.1f}" if total > 0 else "0.0"
    pl_str = pl.group(1) if pl else "0.00"
    roi_str = roi.group(1) if roi else "0.0"
    pl_val = float(pl_str)
    
    # Parse today's picks
    today_picks_html = ""
    in_picks = False
    pick_date = ""
    for line in report_text.split('\n'):
        if 'PICKS FROM' in line:
            in_picks = True
            m = re.search(r'PICKS FROM (\S+)', line)
            if m: pick_date = m.group(1)
            continue
        if in_picks and ('=====' in line):
            break
        if in_picks and line.strip():
            # Parse pick line: emoji + selection + pnl + CLV
            clean = line.strip()
            if not clean:
                continue
            # Determine color based on win/loss
            is_win = '✅' in clean or '✓' in clean
            is_loss = '❌' in clean or '✗' in clean
            color = '#00e676' if is_win else '#ff5252' if is_loss else '#888'
            
            today_picks_html += f'    <div class="result-row" style="color: {color};">{clean}</div>\n'
    
    # Parse best sports
    sport_html = ""
    in_sport = False
    for line in report_text.split('\n'):
        if '── BY SPORT' in line:
            in_sport = True
            continue
        if in_sport and '──' in line:
            break
        if in_sport and line.strip() and 'W-' in line:
            clean = line.strip()
            sport_html += f'    <div class="sport-row">{clean}</div>\n'
    
    # Color accent based on P/L
    accent = '#00e676' if pl_val >= 0 else '#ff5252'
    pl_prefix = '+' if pl_val >= 0 else ''
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scotty's Edge — Results {date_str}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Barlow:wght@400;500;600;700&family=Barlow+Condensed:wght@400;600;700&display=swap');
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    background: #0a0a0a;
    display: flex; justify-content: center; align-items: center;
    min-height: 100vh; padding: 40px;
    font-family: 'Barlow', sans-serif;
  }}
  .card {{
    width: 1080px;
    background: linear-gradient(165deg, #0d1117 0%, #0f1923 40%, #111d2b 100%);
    border-radius: 24px; overflow: hidden; position: relative;
    border: 1px solid rgba(255,255,255,0.06);
  }}
  .card::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 4px;
    background: linear-gradient(90deg, {accent}, {accent}88);
  }}
  .header {{
    padding: 48px 52px 32px; position: relative; z-index: 1;
    display: flex; justify-content: space-between; align-items: flex-start;
  }}
  .brand {{ display: flex; flex-direction: column; gap: 4px; }}
  .logo {{
    font-family: 'Bebas Neue', sans-serif; font-size: 48px;
    color: #ffffff; letter-spacing: 3px; line-height: 1;
  }}
  .logo span {{ color: {accent}; }}
  .subtitle {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 15px;
    color: rgba(255,255,255,0.35); letter-spacing: 4px;
    text-transform: uppercase; font-weight: 600;
  }}
  .date-block {{ text-align: right; }}
  .date-day {{
    font-family: 'Bebas Neue', sans-serif; font-size: 24px;
    color: rgba(255,255,255,0.8); letter-spacing: 2px;
  }}
  .date-full {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 13px;
    color: rgba(255,255,255,0.3); letter-spacing: 2px;
  }}
  .divider {{
    height: 1px; background: linear-gradient(90deg, transparent, {accent}44, transparent);
    margin: 0 52px;
  }}
  .stats-row {{
    display: flex; justify-content: center; gap: 60px;
    padding: 36px 52px;
  }}
  .stat {{ text-align: center; }}
  .stat-value {{
    font-family: 'Bebas Neue', sans-serif; font-size: 48px;
    color: #ffffff; line-height: 1;
  }}
  .stat-value.positive {{ color: #00e676; }}
  .stat-value.negative {{ color: #ff5252; }}
  .stat-label {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 12px;
    color: rgba(255,255,255,0.3); letter-spacing: 3px;
    text-transform: uppercase; margin-top: 4px;
  }}
  .section {{
    padding: 24px 52px;
  }}
  .section-label {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 12px;
    color: {accent}; letter-spacing: 4px; text-transform: uppercase;
    font-weight: 700; margin-bottom: 16px;
  }}
  .result-row {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 16px;
    padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.04);
  }}
  .sport-row {{
    font-family: 'Barlow Condensed', sans-serif; font-size: 15px;
    color: rgba(255,255,255,0.6); padding: 4px 0;
  }}
  .footer {{
    padding: 20px 52px 32px;
    border-top: 1px solid rgba(255,255,255,0.04);
  }}
  .footer p {{
    font-family: 'Barlow', sans-serif; font-size: 9px;
    color: rgba(255,255,255,0.2); line-height: 1.5; text-align: center;
  }}
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div class="brand">
      <div class="logo">SCOTTY'S <span>EDGE</span></div>
      <div class="subtitle">Daily Results</div>
    </div>
    <div class="date-block">
      <div class="date-day">{day_str}</div>
      <div class="date-full">{date_str}</div>
    </div>
  </div>
  
  <div class="divider"></div>
  
  <div class="stats-row">
    <div class="stat">
      <div class="stat-value">{wins}W-{losses}L</div>
      <div class="stat-label">Record</div>
    </div>
    <div class="stat">
      <div class="stat-value {('positive' if pl_val >= 0 else 'negative')}">{pl_prefix}{pl_str}u</div>
      <div class="stat-label">Profit / Loss</div>
    </div>
    <div class="stat">
      <div class="stat-value {('positive' if pl_val >= 0 else 'negative')}">{roi_str}%</div>
      <div class="stat-label">ROI</div>
    </div>
    <div class="stat">
      <div class="stat-value">{pct}%</div>
      <div class="stat-label">Win Rate</div>
    </div>
  </div>
  
  <div class="divider"></div>
  
  <div class="section">
    <div class="section-label">Latest Results</div>
{today_picks_html}
  </div>
  
  <div class="divider"></div>
  
  <div class="section">
    <div class="section-label">By Sport</div>
{sport_html}
  </div>
  
  <div class="footer">
    <p>For entertainment and informational purposes only. Not gambling advice. Past performance does not guarantee future results. Please gamble responsibly. If you or someone you know has a gambling problem, call 1-800-GAMBLER. Must be 21+ and located in a legal jurisdiction.</p>
  </div>
</div>
</body>
</html>"""
    
    cards_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards')
    os.makedirs(cards_dir, exist_ok=True)

    filepath = os.path.join(cards_dir, 'scottys_edge_results.html')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  📊 Results card saved: {filepath}")
    return filepath, html


def _social_media_card(picks):
    """
    Generate copy-paste ready social media content.
    Twitter tweets are individually labeled and guaranteed under 280 chars.
    """
    from datetime import datetime, timedelta
    from model_engine import _to_eastern, _eastern_tz_label, kelly_label
    
    tz = _eastern_tz_label()
    now = datetime.now()
    date_str = now.strftime('%B %d')
    day_str = now.strftime('%A')
    
    # Sort by confidence — highest first
    picks = sorted(picks, key=lambda p: p['units'], reverse=True)
    
    lines = []
    lines.append("=" * 50)
    lines.append("📱 SOCIAL MEDIA — COPY & PASTE BELOW")
    lines.append("=" * 50)
    
    # ── TWITTER FORMAT — split into 280-char tweets ──
    lines.append("")
    lines.append("── TWITTER/X (copy each tweet separately) ──")
    
    tu = sum(p['units'] for p in picks)
    footer = f"\n{len(picks)} plays • {tu:.0f}u\n⚠️ Not gambling advice • 21+\n#ScottysEdge"
    
    header = f"🎯 Scotty's Edge — {day_str} {date_str}\n\n"
    
    # Build pick lines
    pick_lines = []
    for p in picks:
        kl = kelly_label(p['units'])
        odds_str = f"{p['odds']:+.0f}" if p['odds'] else ''
        tier = '🔥' if kl == 'MAX PLAY' else '⭐'
        pick_lines.append(f"{tier} {p['selection']} ({odds_str}) {p['units']:.0f}u")
    
    # Greedily fit picks into tweets under 280 chars
    tweets = []
    current = header
    for i, pl in enumerate(pick_lines):
        test = current + pl + "\n"
        # Check if this is the last pick — need room for footer
        remaining = pick_lines[i+1:] if i+1 < len(pick_lines) else []
        if not remaining:
            # Last pick — add footer
            if len(test + footer) <= 280:
                current = test + footer
            else:
                tweets.append(current.strip())
                current = pl + "\n" + footer
        elif len(test) > 250:
            # Getting close to limit, start new tweet
            tweets.append(current.strip())
            current = pl + "\n"
        else:
            current = test
    tweets.append(current.strip())
    
    for i, tweet in enumerate(tweets):
        lines.append("")
        label = "TWEET 1 (main)" if i == 0 else f"REPLY {i+1}"
        lines.append(f"── {label} ({len(tweet)}/280 chars) ──")
        lines.append(tweet)
    
    # ── INSTAGRAM / DISCORD FORMAT (visual) ──
    lines.append("")
    lines.append("── INSTAGRAM / DISCORD ──")
    lines.append("")
    lines.append(f"🏀⚽🏒 SCOTTY'S EDGE")
    lines.append(f"📅 {now.strftime('%B %d, %Y')}")
    lines.append(f"━━━━━━━━━━━━━━━━━━━━")
    
    # Group by sport
    by_sport = {}
    for p in picks:
        sport_label = p.get('sport', '').replace('basketball_', '').replace('icehockey_', '').replace('soccer_', '').replace('baseball_', '').upper()
        sport_nice = {
            'NBA': '🏀 NBA', 'NCAAB': '🏀 NCAAB', 'NHL': '🏒 NHL',
            'NCAA': '⚾ NCAA Baseball', 'EPL': '⚽ EPL',
            'GERMANY_BUNDESLIGA': '⚽ Bundesliga',
            'FRANCE_LIGUE_ONE': '⚽ Ligue 1',
            'ITALY_SERIE_A': '⚽ Serie A',
            'SPAIN_LA_LIGA': '⚽ La Liga',
            'USA_MLS': '⚽ MLS',
        }.get(sport_label, f'🏟️ {sport_label}')
        if sport_nice not in by_sport:
            by_sport[sport_nice] = []
        by_sport[sport_nice].append(p)
    
    for sport_name, sport_picks in by_sport.items():
        lines.append(f"")
        lines.append(f"{sport_name}")
        for p in sport_picks:
            kl = kelly_label(p['units'])
            icon = '🔥' if kl == 'MAX PLAY' else '⭐' if kl == 'STRONG' else '✅'
            odds_str = f"{p['odds']:+.0f}" if p['odds'] else ''
            
            game_time = ''
            if p.get('commence'):
                try:
                    gt = datetime.fromisoformat(p['commence'].replace('Z', '+00:00'))
                    est = _to_eastern(gt)
                    game_time = est.strftime('%I:%M %p')
                except Exception:
                    pass
            
            lines.append(f"  {icon} {p['selection']}")
            lines.append(f"     {odds_str} • {p['units']:.0f}u {kl} • {game_time} {tz}")
    
    lines.append(f"")
    lines.append(f"━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📊 {len(picks)} plays • {tu:.0f}u total")
    lines.append(f"Every pick tracked & graded")
    lines.append(f"")
    lines.append(f"⚠️ For entertainment & informational purposes only.")
    lines.append(f"Not gambling advice. Must be 21+. Gamble responsibly.")
    lines.append(f"1-800-GAMBLER | scottysedge.com")
    lines.append(f"")
    lines.append("=" * 50)
    
    return '\n'.join(lines)


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


def _merge_and_select(game_picks, prop_picks, conn=None):
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
    # Soccer totals: backtest 9W-2L +10.6u at 5%+ edge (82% hit rate).
    # Bundesliga 63.8% overs, UCL 72.8%. Model correctly identifies
    # over/under tendencies from team attack/defense rates.
    SOCCER_TOTAL_MIN_EDGE = 5.0
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
        mtype = p.get('market_type', 'SPREAD')
        sport = p.get('sport', '')
        book = p.get('book', '')

        # v23: Minimum book count — thin markets produce fake edges
        odds_market = {'SPREAD': 'spreads', 'TOTAL': 'totals', 'MONEYLINE': 'h2h'}.get(mtype, 'h2h')
        book_count = _get_book_count(p.get('event_id', ''), odds_market)
        if book_count < MIN_BOOKS:
            return False

        # v24: Unified 20% edge floor for all books
        # BetMGM: 22% floor — 16-22% bucket was 10W-14L -27.5u, 22%+ is 11W-8L +11.7u
        if book == 'BetMGM':
            min_edge = 22.0
        elif book in SOFT_BOOKS:
            min_edge = SOFT_BOOK_MIN_EDGE.get(mtype, 20.0)
        else:
            min_edge = SHARP_BOOK_MIN_EDGE.get(mtype, 20.0)
        # v16: Soccer spreads DISABLED — backtest 80W-86L -70u.
        # Only totals are profitable (92W-62L +104u, 59.7%).
        # EPL/Ligue 1 spreads showed profit but not enough sample to trust yet.
        if mtype == 'SPREAD' and 'soccer' in sport:
            return False  # Block all soccer spreads — backtest negative
        # Soccer totals: backtest 92W-62L +104u at 59.7% — the real edge
        # v23.1: Respect book tiers — sharp books (BetMGM etc) still need 20%.
        # Live soccer is 1W-3L -11.3u; don't let 5% floor bypass sharp-book gate.
        if mtype == 'TOTAL' and 'soccer' in sport:
            min_edge = SOCCER_TOTAL_MIN_EDGE if book in SOFT_BOOKS else 20.0
        # v24: Baseball totals unified at 20% for all books
        if mtype == 'TOTAL' and 'baseball' in sport:
            min_edge = BASEBALL_TOTAL_MIN_EDGE
        # Walters ML: Elo-backed moneyline picks — unified at 20%
        if mtype == 'MONEYLINE' and 'Elo' in str(p.get('context', '')):
            min_edge = 20.0
        
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

        # ── Friday surcharge: college baseball ──
        # Data: Friday college baseball is 3W-7L, -24u. First game of weekend
        # series — pitching rotations and lineups are least predictable.
        # Aces pitch Fridays but markets already price that; the real issue is
        # lineup uncertainty and early-season roster flux.
        # +3% edge requirement filters marginal Friday plays.
        if sport == 'baseball_ncaa' and datetime.now().strftime('%A') == 'Friday':
            min_edge += 3.0
            # Tag context so it's traceable in graded_bets
            _existing_ctx = p.get('context', '')
            _fri_tag = 'Fri series-opener surcharge (+3%)'
            if _fri_tag not in _existing_ctx:
                p['context'] = f"{_existing_ctx} | {_fri_tag}" if _existing_ctx else _fri_tag

        _min_u = MIN_UNITS
        if not (p.get('units', 0) >= _min_u and p.get('edge_pct', 0) >= min_edge):
            return False
        # v23: ELITE + HIGH. HIGH allowed for soft book 15-20% edges (17W-8L +27u).
        # STRONG still blocked (3W-7L -22.5u).
        if p.get('confidence') not in REQUIRED_CONFIDENCE and p.get('confidence') is not None:
            return False
        
        # v17: Soft market context requirement.
        # Data: Context-confirmed 42W-26L +54.2u (17.4% ROI).
        #       Raw model (no ctx) 1W-2L -4.4u — no signal without context.
        # Context at 20-25% edge is the sweet spot: 17W-5L +51.4u.
        # Lowered gate from 20% to 18% — context 15-20% is 10W-8L (slight positive).
        #
        # Exceptions: March Madness (15%+), Soccer (5%+) — both work without context.
        is_soft = sport in SOFT_MARKETS
        has_context = bool(p.get('context', ''))
        edge = p.get('edge_pct', 0)
        _is_march_madness = (sport == 'basketball_ncaab'
            and (datetime.now().month == 3 or (datetime.now().month == 4 and datetime.now().day <= 7)))
        _ctx_min = 20.0  # v24: Unified context gate
        if is_soft and not has_context and edge < _ctx_min:
            # v14: March Madness exception. Tournament neutral-site games
            # don't trigger context (no B2B, no home/away rest). The Elo
            # spread itself is the signal. Allow picks through at 15% edge.
            if _is_march_madness and edge >= 20.0:
                pass  # v21: Raised from 18% — 20%+ bucket is +82.2u, below is negative
            # Soccer exception: European soccer lines are set by sharp global
            # books. Context rarely fires (no B2B, no revenge in soccer). The
            # Elo spread edge IS the signal. Backtest: EPL +21%, L1 +27% ROI
            # at 5%+ edge without context. Allow soccer through at 5%+ edge.
            elif 'soccer' in sport and edge >= 5.0:
                pass  # Allow through — soccer Elo edges are proven
            # Tennis exception: Same rationale as soccer — no B2B, no rest,
            # no revenge in tennis. Surface-split Elo IS the signal.
            # Context rarely fires for individual sport. Allow at 15%+ edge.
            elif 'tennis' in sport and edge >= 20.0:
                pass  # v21: Raised from 18% — 20%+ bucket is +82.2u, below is negative
            else:
                return False
        
        # ── Elo-only ML filter ──
        # Data: NCAAB Elo-only ML is 3W-4L -3.6u — cross-conference Elo breaks down.
        # But NBA/NHL Elo is better calibrated (larger samples, no conference issue).
        # Block Elo-only ML for soft markets only. Sharp markets (NBA/NHL) allowed.
        # Baseball ML exempt — uses pitcher data path, not Elo-only path.
        if mtype == 'MONEYLINE' and 'baseball' not in sport and sport not in SHARP_MARKETS:
            ctx_str = str(p.get('context', ''))
            _elo_only = (ctx_str.strip() == 'Elo probability edge')
            if _elo_only:
                return False  # Block Elo-only ML in soft markets — no situational edge

        # ── Heavy favorite ML filter ──
        # Laying -300 or worse is terrible risk/reward for subscribers.
        # A -450 ML risks $450 to win $100 — one loss wipes 4 wins.
        # If the model likes a heavy favorite, the spread is the play.
        odds = p.get('odds', -110)
        if mtype == 'MONEYLINE' and odds <= -300:
            return False

        # ── Underdog ML filter ──
        # v17: Tightened from +200 to +150. Data shows:
        #   Small dogs (+100 to +150): 4W-1L +17.4u, 63% ROI — printing money
        #   Med dogs (+151 to +200): 0W-2L -10.0u — complete loss
        # Cap at +150. Dogs +151+ blocked entirely.
        if mtype == 'MONEYLINE' and odds >= 151:
            return False  # Block dogs +151 and higher
        if mtype == 'MONEYLINE' and odds > 0:
            # Small dogs (+100 to +150): cap at 4.5u
            p['units'] = min(p.get('units', 5.0), 4.5)
        
        # ── NHL puck line juice cap ──
        # Data: NHL +1.5 at -175 to -275 is 6W-3L +0.1u — breakeven due to vig.
        # At -200+ you need 67% just to break even. Block the heaviest juice only.
        # -175 to -199 is 3W-0L +8.2u (profitable). -200+ is 3W-3L -8.1u (not).
        if mtype == 'SPREAD' and 'hockey' in sport and odds <= -200:
            return False  # Too much juice on puck line — -200+ is breakeven at best

        # ── NHL spread dog Elo floor ──
        # v21: Raised from 1450 — Calgary (1460) lost by 7 on +2.5. Bottom ~5 teams blocked.
        # Blackhawks (Elo 1431) 1W-4L -15u as puck line dog, 60% blowout rate
        # when losing. Bottom-tier teams get blown out too often for ANY spread
        # to cover. Blocks all spread dogs (any line > 0: +1.5, +2.5, etc.) when Elo < 1475.
        sel = p.get('selection', '')
        line = p.get('line')
        if mtype == 'SPREAD' and 'hockey' in sport and line is not None and line > 0:
            team_name = sel.rsplit(' ', 1)[0].strip() if sel else ''
            if team_name and conn:
                elo_row = conn.execute(
                    "SELECT elo FROM elo_ratings WHERE sport='icehockey_nhl' AND team=?",
                    (team_name,)
                ).fetchone()
                if elo_row and elo_row[0] < 1475:
                    return False  # v21: Block bottom ~5 NHL dogs from ALL spread lines — blowout rate too high

        # ── Early NCAAB block ──
        # Data: Early NCAAB is 4W-7L, -33.9% ROI. Lines haven't settled.
        # The 8% surcharge wasn't enough. Block early NCAAB entirely.
        # Late NCAAB is where the value lives.
        if timing == 'EARLY' and sport == 'basketball_ncaab':
            return False  # Block early NCAAB — lines too unsettled

        # ── NCAAB totals block ──
        # Data: NCAAB totals are 0W-3L, -14.0u. Model has no signal —
        # TOTAL_STD=22 generates fake 18-35% edges. NCAAB spreads are
        # profitable (late 15W-8L +30.3u), totals are not.
        if mtype == 'TOTAL' and sport == 'basketball_ncaab':
            return False

        # v12 FIX: Graduated edge requirements for dogs.
        # Elo compression makes the model systematically favor dogs.
        # Small dogs (1-3.5) in NCAAB: 5W-9L — most efficiently priced.
        # But NBA small dogs are part of our 10W-4L record — don't over-filter.
        # Split by market tier: sharp markets trust the model, soft markets tighten.
        line = p.get('line')
        if mtype == 'SPREAD' and line is not None and line > 0 and 'tennis' not in sport:
            is_sharp = sport in SHARP_MARKETS
            if line <= 3.5:
                # Small dogs: sharp markets (NBA/NHL) keep normal threshold
                # Soft markets (NCAAB) need 20% — books nail these
                if not is_sharp and edge < 20.0:
                    return False
            elif line <= 7.5:
                # Med dogs: require 15% for sharp, 17% for soft
                if is_sharp and edge < 15.0:
                    return False
                elif not is_sharp and edge < 17.0:
                    return False
            # Big dogs (8+): keep current thresholds, they're working

        # ── Baseball total signal conflict filter ──
        # Session 3/23 analysis: all 3 losses were baseball totals where internal
        # signals contradicted the bet direction. Since we only recommend MAX PLAYs,
        # conflicted picks should be suppressed entirely rather than size-reduced.
        #
        # Conflict 1: Pace direction vs bet side
        #   - "fast-paced (+X)" means higher scoring → conflicts with UNDER
        #   - "slow-paced (-X)" means lower scoring → conflicts with OVER
        # Conflict 2: Pitching edge vs bet side
        #   - Negative pitching adj (team suppresses runs) → conflicts with OVER
        #   - Positive pitching adj (team allows runs) → conflicts with UNDER
        #
        # Data: 3 losses all had conflicts + CLV=0.0. 13 wins had aligned signals.
        # Only applies to baseball totals — NHL/soccer have different dynamics.
        if mtype == 'TOTAL' and 'baseball' in sport:
            import re as _re
            ctx = str(p.get('context', ''))
            sel = p.get('selection', '')
            is_over = 'OVER' in sel.upper()
            is_under = 'UNDER' in sel.upper()

            # Check pace direction from context string
            _pace_match = _re.search(r'(fast|slow)-paced\s*\(([+-]?\d+\.?\d*)\)', ctx, _re.IGNORECASE)
            _pace_conflicts = False
            if _pace_match:
                _pace_val = float(_pace_match.group(2))
                # Positive pace = fast = more runs. Negative pace = slow = fewer runs.
                if is_under and _pace_val > 0:
                    _pace_conflicts = True  # Fast-paced but betting Under
                elif is_over and _pace_val < 0:
                    _pace_conflicts = True  # Slow-paced but betting Over

            # Check pitching edge direction from context string
            # Format: "Pitching edge: Team Name (+/-X.X pts)"
            _pitch_match = _re.search(r'Pitching edge:.*?\(([+-]?\d+\.?\d*)\s*pts?\)', ctx, _re.IGNORECASE)
            _pitch_conflicts = False
            if _pitch_match:
                _pitch_val = float(_pitch_match.group(1))
                # Negative pitching adj = pitcher suppresses runs (Under signal)
                # Positive pitching adj = pitcher allows runs (Over signal)
                if is_over and _pitch_val <= -0.5:
                    _pitch_conflicts = True  # Strong Under pitcher but betting Over
                elif is_under and _pitch_val >= 0.5:
                    _pitch_conflicts = True  # Weak pitcher but betting Under

            # Block rules (tested against 3/22 data — 13W-3L):
            # 1. Pitching edge ≥ 1.0 pts against bet direction → strong single conflict
            # 2. Pace conflicts AND pitching doesn't support the bet (neutral or against)
            #    i.e. pace says Over but betting Under, and pitching isn't helping the Under
            #
            # What this catches:
            #   - UCF/TCU OVER: pitching -1.0 (strong Under pitcher) → blocked (rule 1)
            #   - Wake/UVA UNDER: pace +0.6 (fast, Over signal) + pitching -0.1 (neutral) → blocked (rule 2)
            # What this preserves:
            #   - Houston/Kansas UNDER: pace +1.7 conflicts but pitching -0.3 supports Under → pass
            #   - Troy/SoMiss UNDER: pace -0.7 supports Under (no pace conflict) → pass
            #   - Creighton/Miami OVER: pitching -0.5 (below 1.0 threshold) → pass
            _strong_pitch = _pitch_conflicts and abs(float(_pitch_match.group(1))) >= 1.0 if _pitch_match and _pitch_conflicts else False

            # Check if pitching SUPPORTS the bet direction (counteracts pace conflict)
            _pitch_supports_bet = False
            if _pitch_match:
                _pv = float(_pitch_match.group(1))
                # Negative pitching = suppresses runs (supports Under)
                # Positive pitching = allows runs (supports Over)
                if is_under and _pv <= -0.2:
                    _pitch_supports_bet = True
                elif is_over and _pv >= 0.2:
                    _pitch_supports_bet = True

            _pace_unsupported = _pace_conflicts and not _pitch_supports_bet

            if _pace_unsupported or _strong_pitch:
                _conflict_type = []
                if _pace_conflicts: _conflict_type.append('pace')
                if _pitch_conflicts: _conflict_type.append('pitching')
                if _pace_unsupported and not _pitch_conflicts: _conflict_type.append('no pitching support')
                print(f"    ⚠ BLOCKED: {sel[:50]} — signal conflict ({', '.join(_conflict_type)} vs bet side)")
                return False

        # ── CLV-aware filter ──
        # Data: Positive CLV bets 16W-4L (76%), CLV > 1pt is 12W-0L.
        # Negative CLV bets 1W-3L (25%). Line movement against us = negative expected CLV.
        # If opener exists and line moved 1.5+ pts against us, block the bet.
        # This catches stale edges where the market has already corrected.
        if conn is not None:
            try:
                import re
                eid = p.get('event_id', '')
                sel = p.get('selection', '')
                if mtype == 'TOTAL':
                    # Totals: extract "Over" or "Under" for opener lookup
                    _lm_sel = 'Over' if 'OVER' in sel.upper() else 'Under'
                    _lm_mkt = 'totals'
                elif mtype == 'SPREAD':
                    # Spreads: strip the line number to get team name
                    _lm_sel = re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
                    _lm_mkt = 'spreads'
                else:
                    _lm_sel = sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
                    _lm_mkt = 'h2h'
                # Query opener and current line directly
                _opener = conn.execute("""
                    SELECT line, odds FROM openers
                    WHERE event_id = ? AND market = ? AND selection LIKE ?
                    ORDER BY timestamp ASC LIMIT 1
                """, (eid, _lm_mkt, f'%{_lm_sel}%')).fetchone()
                _current = conn.execute("""
                    SELECT line, odds FROM odds
                    WHERE event_id = ? AND market = ? AND selection LIKE ?
                    ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1
                """, (eid, _lm_mkt, f'%{_lm_sel}%')).fetchone()
                if _opener and _current and _opener[0] is not None and _current[0] is not None:
                    if mtype == 'TOTAL':
                        if 'OVER' in sel.upper():
                            _clv_move = _current[0] - _opener[0]  # Line rose = good for over (easier to clear)
                        else:
                            _clv_move = _opener[0] - _current[0]  # Line dropped = good for under (easier to stay under)
                    elif mtype == 'SPREAD':
                        _clv_move = _current[0] - _opener[0]  # More points = good for dog
                    else:
                        # ML: compare implied probs from odds
                        _clv_move = 0.0  # Skip ML CLV for now — odds-based, not line-based
                    if _clv_move <= -1.5:
                        print(f"  ⚠ CLV BLOCK: {sel} — line moved {_clv_move:+.1f} against us (opener={_opener[0]}, now={_current[0]})")
                        return False
            except Exception:
                pass  # Line movement data unavailable — don't block

        return True
    
    game_filtered = [p for p in game_picks if _passes_filter(p)]
    
    # ── Exclude offshore/non-legal books from recommendations ──
    # Data from these books is still used for consensus/edge calculations,
    # but we don't recommend bets on books the user can't access.
    EXCLUDED_BOOKS = {'Bovada', 'BetOnline.ag', 'BetUS', 'MyBookie.ag', 'LowVig.ag'}
    game_filtered = [p for p in game_filtered if p.get('book') not in EXCLUDED_BOOKS]
    
    # ── Split into soft and sharp ──
    soft_picks = [p for p in game_filtered if p.get('sport') in SOFT_MARKETS]
    sharp_picks = [p for p in game_filtered if p.get('sport') in SHARP_MARKETS]
    
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
    
    # ── Prop filters — backtest 3/23: cleaned 14W-29L (-18u) into profitable ──
    # 1. OVER only — Unders are 3W-14L (-49u). Books price downside accurately.
    # 2. No FanDuel — 2W-13L (-50.8u). FanDuel lines look like edges but aren't.
    # 3. Book count filter REMOVED v24 — was counting game-level books, not prop-level.
    #    Model builds own projection from box scores; book count is irrelevant.
    #    Was blocking Wembanyama 21.8% block edges because the GAME had 7+ books.
    # 4. No medium dog odds (+151 to +250) — 3W-15L (-49.8u).
    PROP_EXCLUDED_RECS = {'FanDuel'}  # Still use for consensus calc, never recommend
    prop_filtered = []
    for p in (prop_picks or []):
        if p.get('units', 0) < PROP_MIN_UNITS:
            continue
        market_key = _get_prop_market_key(p)
        min_edge = PROP_MIN_EDGE_THREES if market_key in LOW_LINE_MARKETS else PROP_MIN_EDGE
        if p.get('edge_pct', 0) < min_edge:
            continue
        # Filter 1: OVER only — block all UNDER props
        sel = p.get('selection', '')
        if 'UNDER' in sel.upper():
            continue
        # Filter 2: Block FanDuel recommendations
        if p.get('book', '') in PROP_EXCLUDED_RECS:
            continue
        # Filter 3: Block high odds props — cap at +200
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
    prop_game_capped = prop_team_capped
    
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


# ═══════════════════════════════════════════════════════════════════
# OTHER COMMANDS
# ═══════════════════════════════════════════════════════════════════

def cmd_predict(args):
    """Preview picks WITHOUT saving to DB. Use 'run' for actual picks."""
    import sqlite3
    from model_engine import generate_predictions, print_picks
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db)
    all_picks = []
    for sp in get_sports(args):
        picks = generate_predictions(conn, sport=sp)
        all_picks.extend(picks)
    all_picks.sort(key=lambda x: x['star_rating']*100 + x['edge_pct'], reverse=True)
    conn.close()
    # v12 FIX: predict is PREVIEW ONLY. Does NOT save to DB.
    # Previously predict saved unfiltered picks, which dedup then prevented
    # 'run' from correcting. Use 'run' to save actual picks.
    print(f"\n  ⚠ PREVIEW ONLY — {len(all_picks)} raw picks (not saved, not filtered)")
    print(f"  Use 'python main.py run --email' to generate filtered picks.\n")
    print_picks(all_picks)


def cmd_props(args):
    """Fetch and evaluate player props."""
    import sqlite3
    print("🎯 Fetching player props...")
    try:
        from odds_api import fetch_props
        for sp in PROP_SPORTS:
            try:
                fetch_props(sp)
                print(f"  ✅ {sp}: props fetched")
            except Exception as e:
                print(f"  {sp}: {e}")
    except Exception as e:
        print(f"  {e}")

    print("\n🧠 Evaluating props...")
    from props_engine import evaluate_props, print_props
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db)
    picks = evaluate_props(conn)
    conn.close()
    print_props(picks)


def _generate_kling_prompt(conn):
    """Generate a Kling 3.0 video prompt based on today's graded results."""
    bets = conn.execute("""
        SELECT sport, result, pnl_units FROM graded_bets
        WHERE DATE(created_at) = (
            SELECT MAX(DATE(created_at)) FROM graded_bets
            WHERE result IN ('WIN','LOSS') AND units >= 3.5
        )
        AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchall()

    if not bets:
        return

    # Group by sport
    sport_labels = {
        'basketball_nba': ('NBA', 'basketball player in orange jersey dribbling a ball, well-lit with orange spotlight'),
        'icehockey_nhl': ('NHL', 'hockey player in blue jersey skating with a stick, well-lit with blue spotlight'),
        'baseball_ncaa': ('College Baseball', 'college baseball player in white uniform swinging a bat, well-lit with green spotlight'),
        'baseball_mlb': ('MLB', 'baseball player in pinstripe uniform pitching, well-lit with red spotlight'),
        'basketball_ncaab': ('NCAAB', 'college basketball player shooting, well-lit with orange spotlight'),
    }

    sport_records = {}
    for sp, result, pnl in bets:
        label = sport_labels.get(sp, (sp, 'athlete'))[0]
        if 'soccer' in sp:
            label = 'Soccer'
        elif 'tennis' in sp:
            label = 'Tennis'
        if label not in sport_records:
            sport_records[label] = {'W': 0, 'L': 0}
        if result == 'WIN':
            sport_records[label]['W'] += 1
        else:
            sport_records[label]['L'] += 1

    # Season totals
    season = conn.execute("""
        SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END)
        FROM graded_bets WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchone()
    tw, tl = season[0] or 0, season[1] or 0
    twp = round(tw / (tw + tl) * 100) if (tw + tl) > 0 else 0

    # Build athlete scenes
    scenes = []
    scene_num = 2
    time_per = min(3, 9 // len(sport_records)) if sport_records else 3
    t = 3
    for label, rec in sport_records.items():
        sp_key = [k for k, v in sport_labels.items() if v[0] == label]
        athlete_desc = sport_labels.get(sp_key[0], ('', 'athlete'))[1] if sp_key else 'athlete walking through'
        record_str = f"{rec['W']}-{rec['L']}"
        color = 'green' if rec['W'] > rec['L'] else ('red' if rec['L'] > rec['W'] else 'white')
        scenes.append(f'Scene {scene_num} ({t}-{t+time_per}s): A {athlete_desc}. The LED scoreboard behind shows "{record_str}" in {color} numbers.')
        scene_num += 1
        t += time_per

    total_w = sum(r['W'] for r in sport_records.values())
    total_l = sum(r['L'] for r in sport_records.values())

    prompt = f"""Cinematic vertical video (9:16), 15 seconds. Dark sports broadcast studio with green neon lighting.

Scene 1 (0-3s): Camera enters a dark premium sports studio. A large neon sign on the wall reads "SCOTTY'S EDGE" with EDGE glowing bright green neon and SCOTTY'S in white neon tubes. The sign flickers on dramatically. Green neon light reflects off glossy black floors. Slow cinematic camera push toward the sign. A LED scoreboard below the sign shows "{total_w}-{total_l}" in large white numbers.

{chr(10).join(scenes)}

Scene {scene_num} ({t}-15s): Camera slowly pulls back to reveal the full studio. The neon "SCOTTY'S EDGE" sign glows on the wall. Scoreboard shows large green glowing numbers "{tw}-{tl}" with "{twp}%" below it pulsing green. All green neon lighting pulses slowly. Premium broadcast sign-off. Cinematic fade.

Style: Dark ESPN SportsCenter studio. Athletes are fully visible and well-lit with sport-specific colored lighting, NOT silhouettes. Green neon is the signature accent color. Numbers appear on LED scoreboards within the studio. Smooth slow camera movements. Premium sports broadcast quality."""

    # Save prompt to file and email
    prompt_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'cards', 'kling_prompt.txt')
    with open(prompt_path, 'w', encoding='utf-8') as f:
        f.write(prompt)
    print(f"  Kling prompt saved: {prompt_path}")

    # Email the prompt
    try:
        from emailer import send_email
        send_email(f"Kling Video Prompt — {datetime.now().strftime('%Y-%m-%d')}", prompt)
        print("  Kling prompt emailed")
    except Exception:
        pass


def cmd_grade(args):
    import sqlite3
    from datetime import datetime
    from grader import daily_grade_and_report
    do_email = has_flag(args, '--email')
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")

    print("  Fetching latest scores...")
    try:
        from odds_api import fetch_scores
        for sp in ALL_SPORTS:
            try: fetch_scores(sp, days_back=3)
            except Exception: pass
    except Exception: pass

    # v21 FIX: Odds API scores endpoint often returns 0 results for tennis.
    # ESPN scraper reliably returns completed match scores (FREE).
    print("  Fetching ESPN tennis scores...")
    try:
        from historical_scores import fetch_tennis_scores
        for tour in ('atp', 'wta'):
            try:
                t_new = fetch_tennis_scores(tour, days_back=5, verbose=False)
                if t_new:
                    print(f"  ESPN tennis ({tour.upper()}): {t_new} new results")
            except Exception as e:
                print(f"  ESPN tennis ({tour.upper()}): {e}")
    except Exception as e:
        print(f"  ESPN tennis scores: {e}")

    # v12 FIX: Odds API doesn't return college baseball scores.
    # Fetch from ESPN scraper instead. No API cost.
    print("  Fetching ESPN baseball scores...")
    try:
        from historical_scores import fetch_season_scores
        fetch_season_scores('baseball_ncaa', days_back=5, verbose=False)
    except Exception as e:
        print(f"  ESPN baseball scores: {e}")

    # Fetch ESPN box scores for player prop grading (FREE — NBA, NCAAB, NHL)
    print("  Fetching ESPN box scores (props)...")
    try:
        from box_scores import fetch_all_box_scores
        fetch_all_box_scores(days_back=3)
    except Exception as e:
        print(f"  ESPN box scores: {e}")

    # v12.2: ESPN team endpoint backup — scoreboard misses games.
    # The team-specific schedule endpoint has ALL games including doubleheaders.
    print("  Backfilling missing scores (ESPN team endpoint)...")
    try:
        from espn_team_scores import backfill_missing
        team_new = backfill_missing(days_back=3, verbose=True)
        if team_new:
            print(f"  ESPN team endpoint: {team_new} new results")
    except Exception as e:
        print(f"  ESPN team backup: {e}")

    # v15: NCAA.com backup — ESPN misses ~40% of college baseball games.
    print("  Fetching NCAA.com baseball scores...")
    try:
        from ncaa_scores import fetch_ncaa_scores
        ncaa_new = fetch_ncaa_scores('baseball_ncaa', days_back=5, verbose=True)
        if ncaa_new:
            print(f"  NCAA.com: {ncaa_new} new results")
    except Exception as e:
        print(f"  NCAA.com scores: {e}")

    # v14: Proactively backfill thin-data teams for better Elo accuracy.
    # ESPN scoreboard misses ~40% of college games. The team endpoint gets ALL games.
    # Only looks up active teams (appearing in today's odds) with <min_games results.
    try:
        from espn_team_scores import backfill_thin_teams
        for backfill_sport in ['basketball_ncaab', 'baseball_ncaa']:
            backfill_thin_teams(backfill_sport, min_games=8, max_lookups=30, verbose=True)
    except Exception as e:
        print(f"  ESPN thin-team backfill: {e}")

    report = daily_grade_and_report(conn)

    # Auto Elo rebuild
    print("  Rebuilding Elo ratings...")
    try:
        from elo_engine import build_all_elo
        build_all_elo()
        print("  Elo ratings updated")
    except Exception as e:
        print(f"  Elo rebuild: {e}")

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
        # Build sport-aware hashtags
        _sport_ig = {
            'nba': ['#NBA', '#NBABets', '#NBAPicksToday'],
            'ncaab': ['#CBB', '#CollegeBasketball', '#MarchMadness'],
            'nhl': ['#NHL', '#NHLBets', '#HockeyBets'],
            'baseball': ['#CollegeBaseball', '#NCAACWS', '#BaseballBets'],
            'soccer': ['#Soccer', '#SoccerBets', '#FootballBets'],
        }
        _sport_tw = {
            'nba': ['#NBA', '#NBABets'],
            'ncaab': ['#CBB', '#MarchMadness'],
            'nhl': ['#NHL', '#NHLBets'],
            'baseball': ['#CollegeBaseball', '#BaseballBets'],
            'soccer': ['#Soccer', '#SoccerBets'],
        }
        # March Madness override for NCAAB in March/early April
        _now_m = datetime.now().month
        _now_d = datetime.now().day
        _is_march_madness = 'ncaab' in _sport_counts and (_now_m == 3 or (_now_m == 4 and _now_d <= 7))
        if _is_march_madness:
            _sport_ig['ncaab'] = ['#MarchMadness', '#CollegeBasketball', '#CBBPicks']
            _sport_tw['ncaab'] = ['#MarchMadness', '#CBB']
        # Sort sports by frequency, pick top 2
        _top_sports = sorted(_sport_counts, key=_sport_counts.get, reverse=True)[:2]
        _ig_sport_tags = []
        _tw_sport_tags = []
        for s in _top_sports:
            _ig_sport_tags.extend(_sport_ig.get(s, []))
            _tw_sport_tags.extend(_sport_tw.get(s, []))
        # Dedupe while preserving order
        _ig_sport_tags = list(dict.fromkeys(_ig_sport_tags))
        _tw_sport_tags = list(dict.fromkeys(_tw_sport_tags))
        # IG: core discoverable tags + up to 4 sport tags + community (max ~10)
        _ig_hashtags = ['#SportsBetting', '#BettingPicks', '#FreePicks', '#GamblingTwitter'] + _ig_sport_tags[:4] + ['#BettingCommunity', '#PicksOfTheDay']
        _ig_hashtags = list(dict.fromkeys(_ig_hashtags))
        # Twitter: keep it tight — 3-4 discoverable tags + sport tags (max ~5)
        _tw_hashtags = ['#SportsBetting', '#FreePicks'] + _tw_sport_tags[:3] + ['#GamblingX']
        _tw_hashtags = list(dict.fromkeys(_tw_hashtags))
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
        ig += f"\n\nSeason: {_tw}W-{_tl}L | {_tp:+.1f}u | {_twp:.1f}%"
        ig += "\nEvery pick tracked & graded \U0001f4ca"
        ig += "\n\n\u26a0\ufe0f Not gambling advice \u2022 21+ \u2022 1-800-GAMBLER"
        ig += f"\n\nFollow for daily picks:\n\U0001f4f1 IG: @scottys_edge\n\U0001f426 X: @Scottys_edge\n\U0001f4ac Discord: discord.gg/JQ6rRfuN\n\n{' '.join(_ig_hashtags)}"
        tw = f"{_sport_emojis} Scotty's Edge \u2014 {_date_str}\n\n"
        tw += f"{_yw}W-{_yl}L | {_yp:+.1f}u"
        if _yp >= 10: tw += " \U0001f525"
        tw += f"\n\n{verdict}"
        tw += f"\n\nSeason: {_tw}W-{_tl}L | {_tp:+.1f}u | {_twp:.1f}%"
        tw += "\nEvery pick tracked. Every loss shown. \U0001f4ca"
        tw += f"\n\n\U0001f4f1 @scottys_edge | \U0001f426 @Scottys_edge\n\n{' '.join(_tw_hashtags)}"
        results_caption = "INSTAGRAM CAPTION:\n" + "="*40 + "\n" + ig + "\n\n" + "TWITTER CAPTION:\n" + "="*40 + "\n" + tw
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

        grade_report = (report or "") + agent_block + kling_section
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
        import subprocess as _bp
        _repo = os.path.join(os.path.dirname(__file__), '..')
        _bp.run(['git', '-C', _repo, 'add', 'data/briefing_data.json', 'data/morning_briefing.md'], capture_output=True)
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

    # Upload slim DB to GitHub Releases for cloud agents
    try:
        from upload_db import create_slim_db, upload_to_github
        create_slim_db()
        upload_to_github()
    except Exception as e:
        print(f"  DB upload: {e}")

    do_twitter = has_flag(args, '--twitter')
    if do_twitter:
        print("\n🐦 Generating results thread...")
        try:
            from tweet_formatter import results_from_db
            results_from_db(days_back=2)
        except Exception as e:
            print(f"  Twitter results: {e}")


def cmd_run_soccer(args):
    """Weekend early morning run — soccer totals only.
    European games kick off 10am-3pm ET on weekends.
    This 7am run catches them before kickoff."""
    print("=" * 60)
    print("  SCOTTY'S EDGE — Weekend Soccer Run")
    print(f"  {datetime.now().strftime('%Y-%m-%d %I:%M %p')} EDT")
    print("=" * 60)

    # Force soccer-only sports
    soccer_sports = [s for s in ALL_SPORTS if 'soccer' in s]
    if not soccer_sports:
        print("  No soccer leagues enabled.")
        return

    # Reuse cmd_run logic but override sports
    import sys
    # Inject --sport flags for each soccer league
    soccer_args = list(args)
    # Run the standard pipeline but only for soccer
    original_get_sports = globals().get('get_sports')

    def _soccer_only(a):
        return soccer_sports

    # Monkey-patch get_sports temporarily
    import main as _self
    _orig = _self.get_sports
    _self.get_sports = _soccer_only
    try:
        cmd_run(args)
    finally:
        _self.get_sports = _orig


def cmd_report(args):
    import sqlite3
    from grader import performance_report
    days = 7
    if '--days' in args:
        days = int(args[args.index('--days')+1])
    sport = None
    sports = get_sports(args)
    if len(sports) == 1: sport = sports[0]
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db)
    performance_report(conn, days=days, sport=sport)
    conn.close()


def cmd_bootstrap(args):
    from bootstrap_ratings import bootstrap_all
    bootstrap_all()


def cmd_scores(args):
    from odds_api import fetch_scores
    for sp in get_sports(args):
        try: fetch_scores(sp, days_back=3)
        except Exception as e: print(f"  {sp}: {e}")


def cmd_injuries(args):
    if '--manual' in args:
        from injury_scraper import manual_injury_entry
        manual_injury_entry()
    else:
        from injury_scraper import fetch_and_apply_all
        fetch_and_apply_all()


def cmd_reboot_ratings(args):
    import sqlite3
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
    conn = sqlite3.connect(db)
    for sp in get_sports(args):
        conn.execute("DELETE FROM power_ratings WHERE sport=?", (sp,))
        print(f"  Wiped {sp}")
    conn.commit(); conn.close()
    from bootstrap_ratings import bootstrap_all
    bootstrap_all()


def cmd_email_test(args):
    from emailer import send_email
    from datetime import datetime
    success = send_email("✅ Scotty's Edge v11 — Email Test",
               f"Email is working!\nSent at {datetime.now().strftime('%I:%M %p EST')}\n\n"
               f"Your automated schedule (4 runs/day):\n"
               f"  8:00 AM — Opening lines capture (CLV baseline)\n"
               f"  11:00 AM — Morning picks\n"
               f"  5:30 PM — Afternoon picks\n"
               f"  9:00 AM — Daily grading + CLV report\n")
    if not success:
        print("""
  ────────────────────────────────────────────────────────
  EMAIL TROUBLESHOOTING:

  1. Make sure 2FA is enabled on your Google account:
     https://myaccount.google.com/signinoptions/two-step-verification

  2. Create an App Password (NOT your regular password):
     https://myaccount.google.com/apppasswords

  3. The password is 16 characters with NO spaces:
     setx GMAIL_APP_PASSWORD "abcdefghijklmnop"

  4. Close and REOPEN CMD after setting it.
  ────────────────────────────────────────────────────────""")


def cmd_budget(args):
    print("""
  📊 API BUDGET — 20,000 credits/month
  ══════════════════════════════════════════════════════════════
  Command                              | API Calls | Cost
  -------------------------------------|-----------|------
  python main.py opener                | ~18       | PAID (odds+scores)
  python main.py run                   | ~30       | PAID (odds+scores+props)
  python main.py run --sport nba       | ~5        | PAID
  python main.py predict               | 0         | FREE
  python main.py props                 | ~10       | PAID (props only)
  python main.py grade                 | ~6        | PAID (scores only)
  python main.py injuries              | 0         | FREE
  python main.py bootstrap             | 0         | FREE
  python main.py report                | 0         | FREE

  4-RUN DAILY SCHEDULE:
    8am opener (18) + 11am run (30) + 5:30pm run (30) + 9am grade (6) = 84/day
    84/day × 30 days = 2,520/month (12.6% of budget)

  Plenty of room for manual runs and experimentation.
  ══════════════════════════════════════════════════════════════""")


def cmd_log(args):
    from pick_logger import get_log_summary
    print(get_log_summary())


def cmd_historical(args):
    """Pull full-season historical scores from ESPN (FREE) + fix names + build Elo ratings."""
    from historical_scores import fetch_all_historical
    sports = get_sports(args) if '--sport' in ' '.join(args) else None
    days = int(args[args.index('--days') + 1]) if '--days' in args else None
    fetch_all_historical(sports=sports, days_back=days)

    # Fix team name mismatches between ESPN and Odds API
    print()
    from fix_names import full_fix
    full_fix(sports=sports, diagnose_only=False)


def cmd_elo(args):
    """Build/rebuild Elo ratings from existing results."""
    from elo_engine import build_all_elo, analyze_model
    sports = get_sports(args) if '--sport' in ' '.join(args) else None
    build_all_elo(sports=sports)

    if '--analyze' in args:
        target = sports or list(ELO_CONFIG.keys()) if 'ELO_CONFIG' in dir() else get_sports(args)
        for sp in target:
            analyze_model(sp)


def cmd_setup_scheduler(args):
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"""
  🕐 AUTO-SCHEDULER SETUP — 4 Daily Runs (Windows Task Scheduler)
  ══════════════════════════════════════════════════════════════

  STEP 1: Make sure env variables are set permanently:
    setx ODDS_API_KEY "your_odds_api_key_here"
    setx GMAIL_APP_PASSWORD "your16charpassword"

  STEP 2: Open CMD as Administrator and run these 4 commands:

    schtasks /create /tn "BettingModel_Opener" /tr "cmd /c cd /d {scripts_dir} && python main.py opener --email" /sc daily /st 08:00 /f

    schtasks /create /tn "BettingModel_Morning" /tr "cmd /c cd /d {scripts_dir} && python main.py run --email" /sc daily /st 11:00 /f

    schtasks /create /tn "BettingModel_Afternoon" /tr "cmd /c cd /d {scripts_dir} && python main.py run --email" /sc daily /st 17:30 /f

    schtasks /create /tn "BettingModel_Grade" /tr "cmd /c cd /d {scripts_dir} && python main.py grade --email" /sc daily /st 09:00 /f

  STEP 3: Verify all 4 tasks:
    schtasks /query /tn "BettingModel_Opener"
    schtasks /query /tn "BettingModel_Morning"
    schtasks /query /tn "BettingModel_Afternoon"
    schtasks /query /tn "BettingModel_Grade"

  NOTE: Computer must be ON (not sleeping) for tasks to run.

  WHY 5 RUNS:
    7am SAT/SUN — Weekend soccer picks (European games kick off 10am-3pm ET)
    8am  — Captures opening lines (CLV requires knowing where lines started)
    11am — Morning picks (NCAAB games start early afternoon)
    5:30pm — Afternoon picks (NBA/NHL evening games)
    9am  — Grades yesterday + CLV analysis (the #1 metric)

  STEP 4 (OPTIONAL): Weekend soccer early run (Saturday & Sunday only):
    schtasks /create /tn "BettingModel_Soccer_Weekend" /tr "cmd /c cd /d {scripts_dir} && python main.py run-soccer --email" /sc weekly /d SAT,SUN /st 07:00 /f
  ══════════════════════════════════════════════════════════════""")


def cmd_fix_names(args):
    """Diagnose and fix team name mismatches."""
    from fix_names import full_fix
    sports = get_sports(args) if '--sport' in ' '.join(args) else None
    diagnose_only = '--diagnose' in args
    full_fix(sports=sports, diagnose_only=diagnose_only)


def cmd_backtest(args):
    """Backtest model accuracy against historical results."""
    from backtest import run_all_backtests
    sports = None
    if '--sport' in ' '.join(args):
        sports_list = get_sports(args)
        sports = sports_list
    min_edge = 2.0
    if '--min-edge' in args:
        idx = args.index('--min-edge')
        min_edge = float(args[idx + 1])
    run_all_backtests(sports=sports, min_edge=min_edge)


def cmd_twitter(args):
    """Generate Twitter content from today's picks or results."""
    from tweet_formatter import twitter_from_db, results_from_db
    if '--results' in args:
        results_from_db()
    else:
        twitter_from_db()


COMMANDS = {
    'run': cmd_run, 'opener': cmd_opener, 'snapshot': cmd_snapshot, 'predict': cmd_predict,
    'props': cmd_props, 'grade': cmd_grade, 'report': cmd_report,
    'bootstrap': cmd_bootstrap, 'scores': cmd_scores, 'injuries': cmd_injuries,
    'reboot-ratings': cmd_reboot_ratings, 'email-test': cmd_email_test,
    'run-soccer': cmd_run_soccer,
    'budget': cmd_budget, 'log': cmd_log, 'setup-scheduler': cmd_setup_scheduler,
    'historical': cmd_historical, 'elo': cmd_elo, 'fix-names': cmd_fix_names,
    'backtest': cmd_backtest, 'twitter': cmd_twitter,
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
    main()
