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
    """Run the model on current odds and return picks."""
    conn = sqlite3.connect(DB_PATH)
    
    from model_engine import generate_predictions
    
    all_picks = []
    for sp in ALL_SPORTS:
        picks = generate_predictions(conn, sport=sp)
        all_picks.extend(picks)
    
    conn.close()
    return all_picks


def get_existing_picks_today():
    """Get picks already saved today to avoid duplicates."""
    conn = sqlite3.connect(DB_PATH)
    existing = conn.execute("""
        SELECT selection, sport FROM bets 
        WHERE DATE(created_at) = DATE('now')
    """).fetchall()
    conn.close()
    return set(f"{sel}|{sport}" for sel, sport in existing)


def find_new_max_plays(all_picks, existing_keys):
    """Find MAX PLAYs that haven't been saved yet today."""
    new_plays = []
    for p in all_picks:
        if p.get('units', 0) >= 4.5:
            key = f"{p['selection']}|{p.get('sport', '')}"
            if key not in existing_keys:
                new_plays.append(p)
    return new_plays


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
    
    # Step 3: Evaluate picks
    print("\n  Evaluating picks...")
    all_picks = evaluate_picks()
    
    max_plays = [p for p in all_picks if p.get('units', 0) >= 4.5]
    print(f"  Found {len(all_picks)} total picks, {len(max_plays)} MAX PLAYs")
    
    for p in sorted(all_picks, key=lambda x: x.get('units', 0), reverse=True)[:10]:
        tier = "MAX" if p['units'] >= 4.5 else "STR" if p['units'] >= 4.0 else "---"
        print(f"    {tier} {p['units']:.1f}u {p['edge_pct']:.1f}%  {p['selection']:35s} {p.get('sport','').split('_')[-1]}")
    
    if dry_run:
        print("\n  DRY RUN — not saving or emailing")
        return len(max_plays)
    
    # Step 4: Check for new MAX PLAYs
    existing = get_existing_picks_today()
    new_plays = find_new_max_plays(all_picks, existing)
    
    if new_plays:
        print(f"\n  {len(new_plays)} NEW MAX PLAYs not yet saved!")
        save_and_notify(new_plays, all_picks, do_email=do_email)
    else:
        if max_plays:
            print(f"\n  {len(max_plays)} MAX PLAYs already saved — no new edges")
        else:
            # Check if we should send no-edge card
            # Only send at specific times (11am, 5:30pm) to avoid spam
            if now.hour in (11, 17) and now.minute < 30:
                print("\n  No MAX PLAYs — generating no-edge card")
                if do_email:
                    try:
                        from card_image import generate_card_image, generate_caption
                        from emailer import send_picks_email, send_email
                        
                        no_edge_path = generate_card_image([])
                        hour = now.hour
                        run_type = "Morning" if hour < 12 else "Afternoon" if hour < 17 else "Evening"
                        send_picks_email("No plays — model didn't find enough edge.", run_type, attachment_path=no_edge_path)
                        
                        caption = generate_caption([])
                        if caption:
                            today = now.strftime('%Y-%m-%d')
                            send_email(f"Social Captions - {run_type} {today}", caption)
                        print("  No-edge card sent")
                    except Exception as e:
                        print(f"  No-edge email: {e}")
            else:
                print("\n  No MAX PLAYs this cycle — waiting for next")
    
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
