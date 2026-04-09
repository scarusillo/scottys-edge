"""
agent_odds_monitor.py — Scotty's Edge Odds Monitor

With 100k API calls/month, we can fetch odds frequently and catch edges
as they appear throughout the day instead of hoping fixed 11am/5:30pm
runs happen to coincide with value.

Architecture:
  1. Fetch fresh odds snapshot (costs ~11 API calls per cycle)
  2. Compare to previous snapshot — detect significant line movement
  3. Run pick evaluation on the fresh snapshot
  4. If new MAX PLAYs found that weren't in previous run, save + email
  5. Repeat every 90 minutes from 10am to 9pm

This replaces the fixed 11am/5:30pm/7:30pm schedulers with a smarter
system that catches edges whenever they appear.

API Budget:
  11 sports × 8 cycles/day × 30 days = ~2,640 calls/month for odds
  Plus scores, props, openers = ~5,000 total
  Well within 100k budget

Usage:
    python agent_odds_monitor.py              # Single cycle (fetch + evaluate)
    python agent_odds_monitor.py --daemon     # Run continuously every 90 min
    python agent_odds_monitor.py --no-email   # Cycle without email
    python agent_odds_monitor.py --dry-run    # Evaluate without saving
"""
import sqlite3, os, sys, time, json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

ALL_SPORTS = [
    'basketball_nba', 'basketball_ncaab', 'icehockey_nhl',
    'baseball_mlb', 'baseball_ncaa',
    'soccer_epl', 'soccer_italy_serie_a', 'soccer_spain_la_liga',
    'soccer_germany_bundesliga', 'soccer_france_ligue_one',
    'soccer_uefa_champs_league', 'soccer_usa_mls',
]

CYCLE_INTERVAL_MINUTES = 90  # Run every 90 minutes
FIRST_CYCLE_HOUR = 10        # Start at 10am
LAST_CYCLE_HOUR = 21         # Last cycle at 9pm


def fetch_fresh_odds():
    """Fetch current odds for all sports. Returns number of API calls used."""
    calls = 0
    try:
        from odds_api import fetch_odds
        for sp in ALL_SPORTS:
            try:
                fetch_odds(sp, tag='MONITOR')
                calls += 1
            except Exception as e:
                print(f"    {sp}: {e}")
    except Exception as e:
        print(f"  Odds fetch error: {e}")
    return calls


def detect_line_movement(conn):
    """Compare current odds to previous snapshot. Flag significant moves."""
    movements = []
    
    # Get the two most recent snapshots for each game
    games = conn.execute("""
        SELECT DISTINCT event_id, home, away, sport
        FROM odds
        WHERE snapshot_date = DATE('now')
    """).fetchall()

    for eid, home, away, sport in games:
        # Get latest and previous spread for home team
        rows = conn.execute("""
            SELECT line, odds, snapshot_date || ' ' || snapshot_time FROM odds
            WHERE event_id=? AND market='SPREAD' AND selection=?
            ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 2
        """, (eid, home)).fetchall()

        if len(rows) >= 2:
            current_line, current_odds, current_time = rows[0]
            prev_line, prev_odds, prev_time = rows[1]
            
            if current_line is not None and prev_line is not None:
                move = abs(current_line - prev_line)
                if move >= 0.5:  # Half point or more
                    movements.append({
                        'event_id': eid,
                        'home': home, 'away': away,
                        'sport': sport,
                        'prev_line': prev_line,
                        'current_line': current_line,
                        'move': move,
                    })
    
    return movements


def evaluate_picks():
    """Run the model on current odds, apply same filters as cmd_run."""
    conn = sqlite3.connect(DB_PATH)

    from model_engine import generate_predictions

    # Step 1: Generate raw predictions per sport
    game_picks = []
    for sp in ALL_SPORTS:
        picks = generate_predictions(conn, sport=sp)
        game_picks.extend(picks)

    # Step 2: Apply merge/filter (edge thresholds, confidence, unit minimums)
    try:
        from main import _merge_and_select
        all_picks = _merge_and_select(game_picks, [], conn=conn)
    except Exception as e:
        print(f"  Merge filter error: {e}")
        all_picks = game_picks

    # Step 3: Dedup against existing bets today (same logic as cmd_run)
    import re as _re
    today_str = datetime.now().strftime('%Y-%m-%d')
    already_posted = conn.execute("""
        SELECT sport, market_type, selection, event_id FROM bets
        WHERE created_at >= ? AND result IS NULL
    """, (today_str,)).fetchall()

    posted_event_ids = set(row[3] for row in already_posted if row[3])
    posted_keys = set()
    for row in already_posted:
        sport, mtype, sel = row[0], row[1], row[2]
        if mtype == 'SPREAD':
            side = _re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
        elif mtype == 'TOTAL':
            side = _re.sub(r'\s+\d+\.?\d*$', '', sel).strip()
        elif mtype == 'MONEYLINE':
            side = sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
        else:
            side = sel
        posted_keys.add(f"{sport}|{mtype}|{side}")

    new_picks = []
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
            continue
        if p.get('event_id') in posted_event_ids and mtype != 'PROP':
            print(f"  CONCENTRATION CAP: skipped {sel[:50]} — already have a bet on this game")
            continue
        new_picks.append(p)
        posted_keys.add(key)

    # Step 4: Concentration check (max 4 same-direction per sport)
    _existing_dirs = conn.execute("""
        SELECT sport, side_type, COUNT(*) as cnt
        FROM bets WHERE DATE(created_at) = ? AND units >= 3.5
        GROUP BY sport, side_type
    """, (today_str,)).fetchall()
    _dir_totals = {}
    for _sp, _side, _cnt in _existing_dirs:
        _dir_totals[f"{_sp}|{_side}"] = _cnt

    final_picks = []
    for p in new_picks:
        sp = p.get('sport', '')
        mtype = p.get('market_type', '')
        sel = p.get('selection', '')
        if mtype == 'TOTAL':
            side = 'OVER' if 'OVER' in sel.upper() else 'UNDER'
        elif mtype == 'SPREAD':
            side = 'DOG' if (p.get('line', 0) or 0) > 0 else 'FAVORITE'
        elif mtype == 'MONEYLINE':
            side = 'DOG' if (p.get('odds', -110) or -110) > 0 else 'FAVORITE'
        else:
            side = ''
        dir_key = f"{sp}|{side}"
        if _dir_totals.get(dir_key, 0) >= 4:
            print(f"  CONCENTRATION BLOCK: {sel[:50]} — {_dir_totals[dir_key]} {side} already for {sp}")
            continue
        _dir_totals[dir_key] = _dir_totals.get(dir_key, 0) + 1
        final_picks.append(p)

    conn.close()
    print(f"  After filters: {len(game_picks)} raw → {len(all_picks)} filtered → {len(final_picks)} new")
    return final_picks


def save_and_notify(new_plays, all_picks, do_email=True):
    """Save new picks to DB, generate card, post IG story, email, Discord."""
    if not new_plays:
        return

    print(f"\n  NEW EDGES DETECTED:")
    for p in new_plays:
        print(f"    {p['units']:.1f}u  {p['selection']:40s} {p.get('sport','')}")

    # Step 1: Save picks to DB
    try:
        from model_engine import save_picks_to_db
        conn = sqlite3.connect(DB_PATH)
        save_picks_to_db(conn, new_plays)
        conn.close()
        print(f"  Saved {len(new_plays)} picks to DB")
    except Exception as e:
        print(f"  DB save error: {e}")

    # Step 2: Log picks
    try:
        from pick_logger import log_picks
        hour = datetime.now().hour
        run_type = "Morning" if hour < 12 else "Afternoon" if hour < 17 else "Evening"
        log_picks(new_plays, run_type)
    except Exception as e:
        print(f"  Pick log error: {e}")

    # Step 3: Generate PNG card
    png_card_path = None
    try:
        from card_image import generate_card_image
        png_card_path = generate_card_image(new_plays)
        if png_card_path:
            print(f"  Card: {png_card_path}")
    except Exception as e:
        print(f"  Card error: {e}")

    # Step 4: Post to Discord
    try:
        from social_media import post_picks_social
        post_picks_social(new_plays)
    except Exception as e:
        print(f"  Discord error: {e}")

    # Step 5: Post to Instagram story
    if png_card_path:
        try:
            from social_media import post_picks_to_instagram
            post_picks_to_instagram([png_card_path], new_plays)
        except Exception as e:
            print(f"  Instagram error: {e}")

    # Step 6: Email
    if do_email:
        try:
            from emailer import send_picks_email, send_email
            from card_image import generate_caption
            from model_engine import picks_to_text

            hour = datetime.now().hour
            run_type = "Morning" if hour < 12 else "Afternoon" if hour < 17 else "Evening"
            today = datetime.now().strftime('%Y-%m-%d')

            text = picks_to_text(new_plays)
            send_picks_email(text, run_type, attachment_path=png_card_path)

            caption = generate_caption(new_plays)
            if caption:
                send_email(f"Social Captions - {run_type} {today}", caption)
            print("  Email sent")
        except Exception as e:
            print(f"  Email error: {e}")


def run_single_cycle(do_email=True, dry_run=False):
    """Run one fetch-evaluate-notify cycle."""
    now = datetime.now()
    print(f"\n{'='*60}")
    print(f"  ODDS MONITOR — Cycle at {now.strftime('%I:%M %p')}")
    print(f"{'='*60}")
    
    # Step 1: Fetch fresh odds
    print("\n  Fetching fresh odds...")
    calls = fetch_fresh_odds()
    print(f"  {calls} API calls used")
    
    # Step 2: Detect line movement
    conn = sqlite3.connect(DB_PATH)
    movements = detect_line_movement(conn)
    conn.close()
    
    if movements:
        print(f"\n  LINE MOVEMENT ({len(movements)} games):")
        for m in movements[:5]:
            sport = m['sport'].split('_')[-1].upper()
            print(f"    {sport}: {m['away']} @ {m['home']} | {m['prev_line']:+.1f} → {m['current_line']:+.1f} ({m['move']:+.1f})")
    else:
        print("\n  No significant line movement detected")
    
    # Step 3: Evaluate picks (filtered, deduped, concentration-checked)
    print("\n  Evaluating picks...")
    new_plays = evaluate_picks()

    if new_plays:
        for p in sorted(new_plays, key=lambda x: x.get('units', 0), reverse=True):
            print(f"    {p['units']:.1f}u {p['edge_pct']:.1f}%  {p['selection']:35s} {p.get('sport','').split('_')[-1]}")

    if dry_run:
        print("\n  DRY RUN — not saving or emailing")
        return len(new_plays)

    if new_plays:
        print(f"\n  {len(new_plays)} NEW picks found!")
        save_and_notify(new_plays, new_plays, do_email=do_email)
    else:
        print("\n  No new picks this cycle — waiting for next")

    return len(new_plays)


def run_daemon():
    """Run continuously throughout the day."""
    print("=" * 60)
    print("  ODDS MONITOR — DAEMON MODE")
    print(f"  Cycles every {CYCLE_INTERVAL_MINUTES} minutes")
    print(f"  Active hours: {FIRST_CYCLE_HOUR}:00 — {LAST_CYCLE_HOUR}:00")
    print("  Press Ctrl+C to stop")
    print("=" * 60)
    
    daily_calls = 0
    daily_new_plays = 0
    cycles = 0
    
    while True:
        now = datetime.now()
        
        # Only run during active hours
        if now.hour < FIRST_CYCLE_HOUR or now.hour > LAST_CYCLE_HOUR:
            next_start = now.replace(hour=FIRST_CYCLE_HOUR, minute=0, second=0)
            if now.hour > LAST_CYCLE_HOUR:
                next_start += timedelta(days=1)
            wait = (next_start - now).total_seconds()
            print(f"\n  Outside active hours. Sleeping until {next_start.strftime('%I:%M %p')}...")
            time.sleep(min(wait, 3600))  # Check every hour max
            continue
        
        # Run cycle
        try:
            new = run_single_cycle(do_email=True)
            daily_new_plays += new
            cycles += 1
            daily_calls += 11  # Approximate
            
            print(f"\n  Daily stats: {cycles} cycles | ~{daily_calls} API calls | {daily_new_plays} new plays")
        except Exception as e:
            print(f"\n  Cycle error: {e}")
        
        # Reset daily stats at midnight
        if now.hour == 0 and now.minute < 5:
            daily_calls = 0
            daily_new_plays = 0
            cycles = 0
        
        # Sleep until next cycle
        print(f"\n  Next cycle at {(now + timedelta(minutes=CYCLE_INTERVAL_MINUTES)).strftime('%I:%M %p')}")
        time.sleep(CYCLE_INTERVAL_MINUTES * 60)


if __name__ == '__main__':
    if '--daemon' in sys.argv:
        run_daemon()
    else:
        do_email = '--no-email' not in sys.argv
        dry_run = '--dry-run' in sys.argv
        run_single_cycle(do_email=do_email, dry_run=dry_run)
