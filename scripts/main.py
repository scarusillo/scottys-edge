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
                # Skip started games (compare UTC timestamps as strings — good enough)
                try:
                    if _commence:
                        _now_utc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                        if _commence < _now_utc:
                            continue
                except Exception:
                    pass
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
        # Standard (non-max) sizing at 3.5u — passes the units≥3.5 tracked-record filter.
        # Sample is still thin (<60 bets per gate) — revisit sizing after ~100 live bets.
        try:
            _today_str = datetime.now().strftime('%Y-%m-%d')
            _SOFT_BOOKS = ('DraftKings','BetMGM','Caesars','Fanatics','ESPN BET')
            _SHARP_BOOKS = ('FanDuel','BetRivers')

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
                        _sel = f"{_away}@{_home} {side} {cur_soft_ln}"
                        pick = {
                            'sport': sport_, 'event_id': eid, 'market_type': 'TOTAL',
                            'selection': _sel, 'book': soft,
                            'line': cur_soft_ln, 'odds': cur_soft_odds or -110,
                            'edge_pct': round(abs(gap) * 5.0, 1),
                            'confidence': 'BOOK_ARB', 'units': 3.5,
                            'context': f'BOOK_ARB: {sharp} opener {sharp_open} vs {soft} opener {soft_open} (gap {gap:+.1f}) | current gap {cur_gap:+.1f}',
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
                        _sel = f"{_away}@{_home} {bet_team} {cur_line:+g}"
                        pick = {
                            'sport': sport_, 'event_id': eid, 'market_type': 'SPREAD',
                            'selection': _sel, 'book': soft,
                            'line': cur_line, 'odds': cur_odds or -110,
                            'edge_pct': round(abs(gap) * 4.0, 1),
                            'confidence': 'BOOK_ARB', 'units': 3.5,
                            'context': f'BOOK_ARB: {sharp} home opener {home_by_book[sharp][0]} vs {soft} home opener {home_by_book[soft][0]} (gap {gap:+.1f}) | current gap {cur_gap:+.1f}',
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

    # Step 6b: Model B shadow tagging — cross-book disagreement analysis
    _model_b_report = ""
    if all_picks:
        try:
            from market_model import tag_picks_with_model_b, generate_shadow_report
            _mb_summary = tag_picks_with_model_b(conn, all_picks)
            _model_b_report = generate_shadow_report(all_picks)
            print(f"  Model B: {_mb_summary['agree']} agree, {_mb_summary['disagree']} disagree, {_mb_summary['unknown']} unknown")
            # Log Model B tags to DB for historical tracking
            try:
                conn.execute("""CREATE TABLE IF NOT EXISTS model_b_shadow (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT, event_id TEXT, selection TEXT, sport TEXT,
                    model_a_edge REAL, model_b_agrees INTEGER, model_b_level TEXT,
                    model_b_edge REAL, model_b_reason TEXT
                )""")
                from datetime import datetime as _dt
                _now = _dt.now().isoformat()
                for _p in all_picks:
                    _mb_val = 1 if _p.get('model_b_agrees') is True else (0 if _p.get('model_b_agrees') is False else -1)
                    conn.execute("""INSERT INTO model_b_shadow
                        (created_at, event_id, selection, sport, model_a_edge, model_b_agrees, model_b_level, model_b_edge, model_b_reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (_now, _p.get('event_id',''), _p.get('selection','')[:80], _p.get('sport',''),
                         _p.get('edge_pct',0), _mb_val, _p.get('model_b_level',''), _p.get('model_b_edge',0), _p.get('model_b_reason','')[:200]))
                conn.commit()
            except Exception:
                pass
        except Exception as e:
            print(f"  Model B: {e}")

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

    # v25: Pipeline sanity check — flag risky picks before emailing
    _warnings = []
    if all_picks:
        try:
            _today_str = datetime.now().strftime('%Y-%m-%d')

            # Check 1: Same player lost on ANY prop recently (not just exact selection)
            # Catches: Abrams lost on RUNS yesterday, now firing on RBIS today
            for p in all_picks:
                if p.get('market_type') == 'PROP':
                    import re as _re
                    _pm = _re.match(r'^(.+?)\s+(OVER|UNDER)\s+', p.get('selection', ''))
                    _player_name = _pm.group(1) if _pm else p.get('selection', '')
                    _recent_loss = conn.execute("""
                        SELECT selection, pnl_units, clv, DATE(created_at) as dt
                        FROM graded_bets
                        WHERE selection LIKE ? AND result = 'LOSS'
                        AND DATE(created_at) >= DATE('now', '-7 days')
                        ORDER BY created_at DESC LIMIT 1
                    """, (f"{_player_name}%",)).fetchone()
                    if _recent_loss:
                        _clv_info = f", CLV:{_recent_loss[2]:+.1f}%" if _recent_loss[2] is not None else ""
                        _warnings.append(
                            f"REPEAT LOSS: {p['selection'][:40]} — {_player_name} lost {_recent_loss[1]:+.1f}u on {_recent_loss[3]} ({_recent_loss[0][:30]}){_clv_info}")

            # Check 2: Single game with 3+ picks (concentration)
            _game_picks = {}
            for p in all_picks:
                eid = p.get('event_id', '')
                if eid:
                    _game_picks[eid] = _game_picks.get(eid, 0) + 1
            for eid, cnt in _game_picks.items():
                if cnt >= 3:
                    _warnings.append(f"CONCENTRATION: {cnt} picks on same game (event {eid[:12]}...)")

            # Check 3: Total exposure today >30u
            _today_units = conn.execute("""
                SELECT COALESCE(SUM(units), 0) FROM bets
                WHERE DATE(created_at) = ? AND result IS NULL
            """, (_today_str,)).fetchone()[0]
            _new_units = sum(p.get('units', 0) for p in all_picks)
            if _today_units + _new_units > 30:
                _warnings.append(f"EXPOSURE: {_today_units + _new_units:.0f}u total today (existing {_today_units:.0f}u + new {_new_units:.0f}u)")

            if _warnings:
                print(f"\n  ⚠️ SANITY CHECK WARNINGS:")
                for _w in _warnings:
                    print(f"    {_w}")
        except Exception as e:
            print(f"  Sanity check: {e}")

    if do_email:
        print("\n📧 Step 9: Sending email...")
        if all_picks:
            from emailer import send_picks_email, send_email
            text = picks_to_text(all_picks, f"{run_type} Picks")
            # Append sanity check warnings to email
            if _warnings:
                text += "\n\n" + "⚠️ " * 10 + "\n"
                text += "  SANITY CHECK WARNINGS\n"
                text += "⚠️ " * 10 + "\n\n"
                for _w in _warnings:
                    text += f"  {_w}\n"
            # Append research intel to picks email
            if research_brief:
                text += "\n\n" + "═" * 50 + "\n"
                text += "  PRE-GAME INTEL\n"
                text += "═" * 50 + "\n\n"
                text += research_brief
            # Append Model B shadow report
            if _model_b_report:
                text += "\n\n" + _model_b_report
            social = _social_media_card(all_picks)
            full_text = text + "\n\n" + social
            email_ok = send_picks_email(full_text, run_type, html_body=html_content,
                            attachment_path=png_card_path,
                            attachment_paths=png_card_paths if len(png_card_paths) > 1 else None)
            if not email_ok:
                print("  ❌ EMAIL FAILED — picks were saved but not delivered. Check GMAIL_APP_PASSWORD env var.")

            # Separate caption email (plain text, copyable from phone)
            # v25.3: Twitter caption + threads removed — account suspended April 2026.
            try:
                from card_image import generate_caption, generate_pick_writeups
                ig_caption = generate_caption(all_picks)
                if ig_caption:
                    # Per-pick write-ups for engagement posts
                    writeups = generate_pick_writeups(all_picks)

                    caption_text = "INSTAGRAM CAPTION:\n" + "="*40 + "\n" + ig_caption
                    if writeups:
                        caption_text += "\n\n" + "INDIVIDUAL PICK POSTS (copy-paste for engagement):\n" + "="*40 + writeups

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

                    # v25.3: Twitter sections removed from growth playbook —
                    # @Scottys_Edge suspended April 2026. IG + Discord + Reddit only.
                    growth_section = f"""

GROWTH PLAYBOOK
{'='*40}

ACCOUNTS TO TAG (on your image, not caption):
  IG: @actionnetworkhq @baborofficial @bettingcappers @vegasinsider

ACCOUNTS TO COMMENT ON (within 30 min of their posts):
  @ActionNetworkHQ @ESPNBet @BleacherReport — reply with your model's take

TONIGHT'S CHECKLIST:
{'='*40}
[ ] Post picks card to IG feed + story (tag 4 accounts ON image)
[ ] Comment on 2 big account posts (within 30 min)
[ ] After wins hit: post results card + "Called it" story
"""
                    # v25: Reddit engagement comments (for team subs + betting subs)
                    from card_image import generate_engagement_comments
                    _eng_comments = generate_engagement_comments(all_picks)
                    if _eng_comments:
                        _reddit_comments = [c for c in _eng_comments if c['platform'] == 'reddit']
                        if _reddit_comments:
                            caption_text += "\n\n" + "REDDIT COMMENTS (team subs + betting subs):\n" + "=" * 40
                            _seen_targets = set()
                            for _rc in _reddit_comments:
                                _key = (_rc['target'], _rc['pick'])
                                if _key in _seen_targets:
                                    continue
                                _seen_targets.add(_key)
                                caption_text += f"\n\n{_rc['target']} — {_rc['game']} ({_rc['sport']}):\n"
                                caption_text += f"{_rc['comment']}"

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

                    # Save engagement comments JSON for Cowork automation
                    try:
                        from card_image import save_engagement_comments
                        _cw_path = save_engagement_comments(all_picks)
                        if _cw_path:
                            _cw_count = len(generate_engagement_comments(all_picks))
                            print(f"  Cowork comments saved: {_cw_path} ({_cw_count} comments)")
                    except Exception as _cw_e:
                        print(f"  Cowork comments: {_cw_e}")
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

    # Step 9c: Auto-post to Discord + Instagram (Twitter removed v25.3)
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

    # v25.3: Step 10 (Twitter/X content) removed — @Scottys_Edge account
    # permanently suspended April 2026. Discord + Instagram only.

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
    Generate copy-paste ready social media content for Instagram + Discord.
    v25.3: Twitter format removed — @Scottys_Edge suspended April 2026.
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

    # v25.3: Twitter/X format removed — @Scottys_Edge suspended April 2026.

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
    tu = sum(p.get('units', 0) or 0 for p in picks)
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
        # v25: Tightened from -200 to -130. Full data: -130 and below is
        # 15W-11L -11.3u (58% win rate but avg odds -178 needs 64% to profit).
        # -115 to -129 is 5W-1L +15.5u. The juice eats all edge on heavy lines.
        if mtype == 'SPREAD' and 'hockey' in sport and odds <= -130:
            return False  # Puck line juice too heavy — need -129 or better

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
                # v25.3: log to shadow_blocked_picks for observability
                try:
                    _gate_name = 'PITCHING_GATE' if _strong_pitch else 'PACE_GATE'
                    conn.execute("""INSERT INTO shadow_blocked_picks
                        (created_at, sport, event_id, selection, market_type, book,
                         line, odds, edge_pct, units, reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (datetime.now().isoformat(), p.get('sport',''), p.get('event_id',''),
                         p.get('selection',''), p.get('market_type',''), p.get('book',''),
                         p.get('line'), p.get('odds'), p.get('edge_pct', 0),
                         p.get('units', 0),
                         f"{_gate_name} (signal conflict: {', '.join(_conflict_type)})"))
                    conn.commit()
                except Exception:
                    pass
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

    # Group by sport — each entry: (display label, athlete description, spotlight color)
    sport_labels = {
        'basketball_nba': ('NBA', 'An NBA guard in a crisp team jersey drives hard to the rim, mid-stride, ball cocked for a finish', 'deep orange'),
        'icehockey_nhl': ('NHL', 'An NHL forward in full pads winds up for a slap shot, ice mist swirling around his skates', 'electric blue'),
        'baseball_ncaa': ('College Baseball', 'A college baseball pitcher mid-windup on a lit mound, leg kicked high, jersey pinstripes sharp', 'crimson'),
        'baseball_mlb': ('MLB', 'An MLB slugger in full uniform frozen at the top of his swing, bat blurred, eyes locked on the ball', 'warm red'),
        'basketball_ncaab': ('NCAAB', 'A college basketball player elevates for a mid-range jumper, wrist flicked, ball spinning off fingertips', 'amber'),
    }
    # Display-label overrides (soccer leagues + tennis tours map to generic labels
    # with their own athlete cinematography — matches the landing-page / card style)
    label_athletes = {
        'Soccer': ('A soccer striker mid-strike, boot connecting with the ball, stadium lights catching the turf', 'golden'),
        'Tennis': ('A tennis player mid-serve, racket fully extended overhead, ball suspended at contact point', 'white'),
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
    twp = round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0

    # Build athlete scenes — Sora 12s format, scenes 2..N span 3-9s (6s of runtime)
    scenes = []
    scene_num = 2
    n_sports = len(sport_records) or 1
    mid_budget = 6.0  # 3s–9s
    time_per = mid_budget / n_sports
    t = 3.0
    for label, rec in sport_records.items():
        sp_key = [k for k, v in sport_labels.items() if v[0] == label]
        if sp_key:
            _, athlete_desc, spotlight = sport_labels[sp_key[0]]
        elif label in label_athletes:
            athlete_desc, spotlight = label_athletes[label]
        else:
            athlete_desc, spotlight = ('an athlete framed in a sport-specific spotlight', 'white')
        record_str = f"{rec['W']}-{rec['L']}"
        color = 'glowing green' if rec['W'] > rec['L'] else ('glowing red' if rec['L'] > rec['W'] else 'white')
        t_end = t + time_per
        scenes.append(
            f'Scene {scene_num} ({t:.1f}-{t_end:.1f}s): {athlete_desc}, bathed in a {spotlight} spotlight against deep black. '
            f'A sleek LED scoreboard behind reads "{label} {record_str}" in {color} numbers. Subtle camera dolly.'
        )
        scene_num += 1
        t = t_end

    total_w = sum(r['W'] for r in sport_records.values())
    total_l = sum(r['L'] for r in sport_records.values())

    prompt = f"""Cinematic vertical video (9:16), 12 seconds. Dark premium sports broadcast studio with signature green neon lighting. No voiceover — all text is on-screen only.

Scene 1 (0-3s): Camera glides into a dark premium sports studio. A large neon sign on the wall reads "SCOTTY'S EDGE" — "EDGE" in bright green neon, "SCOTTY'S" in crisp white neon tubes. The sign flickers dramatically to life. Green neon light reflects across glossy black floors. On-screen text appears in a clean broadcast font beneath the sign: "WELCOME TO SCOTTY'S EDGE". A smaller LED scoreboard below shows tonight's record "{total_w}-{total_l}" in bold white numerals. Slow cinematic push toward the sign.

{chr(10).join(scenes)}

Scene {scene_num} ({t:.1f}-12.0s): Camera pulls back smoothly to reveal the full studio. The neon "SCOTTY'S EDGE" sign glows steady above. A massive LED scoreboard shows season record "{tw}-{tl}" in large glowing green numerals with "{twp}%" pulsing beneath. Green neon accents across the studio pulse slowly in rhythm. Premium broadcast sign-off, cinematic fade to black.

Style: Dark ESPN SportsCenter aesthetic. Athletes are fully visible and well-lit with sport-specific colored spotlights — NOT silhouettes. Green neon is the signature accent color throughout. All numbers appear on LED scoreboards inside the studio. Smooth, slow camera movements. Premium sports broadcast quality. 9:16 vertical, 12 seconds total."""

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
    from datetime import datetime, timedelta
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

    # ═══ DATA RETENTION — prune old odds/props snapshots ═══
    # Keep 7 days of snapshots. Old data is backed up in GitHub releases.
    # Without pruning, odds grows ~7.5K rows/run × 15 runs/day = 112K/day.
    # After 30 days that's 3.4M rows, slowing every query.
    try:
        _prune_conn = sqlite3.connect(db)
        _cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        _odds_before = _prune_conn.execute('SELECT COUNT(*) FROM odds').fetchone()[0]
        _prune_conn.execute('DELETE FROM odds WHERE snapshot_date < ?', (_cutoff,))
        _props_before = _prune_conn.execute('SELECT COUNT(*) FROM props').fetchone()[0]
        _prune_conn.execute("DELETE FROM props WHERE commence_time < datetime('now', '-7 days')")
        _prune_conn.commit()
        _odds_after = _prune_conn.execute('SELECT COUNT(*) FROM odds').fetchone()[0]
        _props_after = _prune_conn.execute('SELECT COUNT(*) FROM props').fetchone()[0]
        _prune_conn.close()
        _odds_pruned = _odds_before - _odds_after
        _props_pruned = _props_before - _props_after
        if _odds_pruned > 0 or _props_pruned > 0:
            print(f"  🗑️ Retention: pruned {_odds_pruned:,} odds + {_props_pruned:,} props (>7 days old)")
    except Exception as e:
        print(f"  Retention pruning: {e}")

    # Upload slim DB to GitHub Releases for cloud agents
    try:
        from upload_db import create_slim_db, upload_to_github
        create_slim_db()
        upload_to_github()
    except Exception as e:
        print(f"  DB upload: {e}")

    # v25.3: Twitter results thread removed — account suspended April 2026.


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


def cmd_scrub(args):
    """Scrub a bet: mark bets.units=0/result='TAINTED' and write a matching
    full-column graded_bets row so it's excluded from every downstream query.

    Usage: python main.py scrub <bet_id> [reason]

    Replaces the ad-hoc direct-SQL scrub pattern that left minimal graded_bets
    rows and tripped the code auditor. One path, full column set, audit-traceable.
    """
    import sqlite3
    from datetime import datetime
    db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

    if not args or args[0].startswith('--'):
        print("Usage: python main.py scrub <bet_id> [reason]")
        return
    try:
        bid = int(args[0])
    except ValueError:
        print(f"bet_id must be an integer, got: {args[0]}"); return
    reason = ' '.join(args[1:]).strip() if len(args) > 1 else 'manual scrub'

    conn = sqlite3.connect(db)
    try:
        bet = conn.execute("""
            SELECT id, created_at, sport, event_id, market_type, selection,
                   book, line, odds, edge_pct, confidence, units, result,
                   side_type, spread_bucket, edge_bucket, timing,
                   context_factors, context_confirmed, market_tier,
                   model_spread, day_of_week
            FROM bets WHERE id = ?
        """, (bid,)).fetchone()
        if not bet:
            print(f"Bet id={bid} not found."); return

        (_id, created, sport, eid, mtype, sel, book, line, odds, edge, conf,
         units, result, side_type, spread_bucket, edge_bucket, timing,
         context, context_confirmed, market_tier, model_spread, dow) = bet

        if result == 'TAINTED' and units == 0:
            existing = conn.execute("SELECT id FROM graded_bets WHERE bet_id=?", (bid,)).fetchone()
            if existing:
                print(f"Bet {bid} already scrubbed (bets + graded_bets consistent). No-op.")
                return
            print(f"Bet {bid} is TAINTED in bets but missing graded_bets row — backfilling.")

        tagged_ctx = (context or '').strip()
        scrub_tag = f'SCRUB: {reason}'
        new_ctx = f'{tagged_ctx} | {scrub_tag}'.strip(' |') if tagged_ctx else scrub_tag

        now = datetime.now().isoformat()
        conn.execute("""
            UPDATE bets SET units=0, result='TAINTED', context_factors=?
            WHERE id=?
        """, (new_ctx, bid))

        existing = conn.execute("SELECT id, result FROM graded_bets WHERE bet_id=?", (bid,)).fetchone()
        if existing:
            conn.execute("""
                UPDATE graded_bets SET result='TAINTED', units=0, pnl_units=0,
                    context_factors=?, graded_at=?
                WHERE bet_id=?
            """, (new_ctx, now, bid))
            print(f"Updated existing graded_bets row (was {existing[1]}) to TAINTED.")
        else:
            conn.execute("""
                INSERT INTO graded_bets (graded_at, bet_id, sport, event_id, selection,
                    market_type, book, line, odds, edge_pct, confidence, units,
                    result, pnl_units, closing_line, clv, created_at,
                    side_type, spread_bucket, edge_bucket, timing,
                    context_factors, context_confirmed, market_tier, model_spread, day_of_week)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now, bid, sport, eid, sel, mtype, book, line, odds, edge, conf,
                  0, 'TAINTED', 0, None, None, created,
                  side_type, spread_bucket, edge_bucket, timing,
                  new_ctx, context_confirmed, market_tier, model_spread, dow))
            print(f"Inserted new graded_bets row marked TAINTED.")

        conn.commit()
        print(f"✓ Scrubbed bet {bid}: {sel[:60]} — reason: {reason}")
    finally:
        conn.close()


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
    'backtest': cmd_backtest, 'scrub': cmd_scrub,
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
