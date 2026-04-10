"""
odds_api.py — Interface to The Odds API for live odds, scores, and results.

SETUP:
1. Get your API key from https://the-odds-api.com
2. Set it in the database: UPDATE settings SET value='YOUR_KEY' WHERE key='ODDS_API_KEY'
   OR set environment variable: export ODDS_API_KEY=your_key_here

ENDPOINTS USED:
- /v4/sports/{sport}/odds          → Current odds from all books
- /v4/sports/{sport}/scores        → Live + completed game scores (KEY NEW ADDITION)
- /v4/sports/{sport}/events        → Event list with IDs

API LIMITS:
- Free tier: 500 requests/month
- Paid tiers scale up. Each request with multiple markets counts as 1 request
  but returns usage based on markets × sports requested.
"""
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
BASE_URL = "https://api.the-odds-api.com/v4"

# Sports we track (Odds API sport keys)
TRACKED_SPORTS = [
    'basketball_nba',
    'basketball_ncaab',
    'icehockey_nhl',
    'soccer_epl',
    'soccer_italy_serie_a',
    'soccer_spain_la_liga',
    'soccer_germany_bundesliga',    # Was missing — fully configured, never fetched odds
    'soccer_france_ligue_one',      # Was missing — same
    'soccer_uefa_champs_league',    # Was missing — same
    'soccer_usa_mls',               # Was missing — same
    'soccer_mexico_ligamx',
    'baseball_ncaa',
    'baseball_mlb',             # v17: Opening Day 2026-03-26
]
# Tennis keys are NOT in TRACKED_SPORTS — they're event-based and most are
# inactive at any given time. detect_active_tennis() finds live ones dynamically.

# Books legal in NY (filter to these)
NY_LEGAL_BOOKS = [
    'draftkings', 'fanduel', 'betmgm', 'caesars', 'betrivers',
    'espnbet', 'pointsbetus', 'fanatics',
]

def get_api_key():
    key = os.environ.get('ODDS_API_KEY')
    if key:
        return key
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT value FROM settings WHERE key='ODDS_API_KEY'").fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    raise ValueError("No ODDS_API_KEY found. Set via environment variable or database settings.")


def _api_get(endpoint, params=None):
    """Make a GET request to the Odds API with retry logic."""
    api_key = get_api_key()
    if params is None:
        params = {}
    params['apiKey'] = api_key
    url = f"{BASE_URL}{endpoint}?{urlencode(params)}"
    req = Request(url)

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            with urlopen(req, timeout=30) as resp:
                remaining = resp.headers.get('x-requests-remaining', '?')
                used = resp.headers.get('x-requests-used', '?')
                print(f"  API: {endpoint} — Requests used: {used}, remaining: {remaining}")
                return json.loads(resp.read().decode())
        except (URLError, HTTPError, TimeoutError, OSError) as e:
            backoff = 2 ** attempt  # 2s, 4s, 8s
            if attempt < max_retries:
                print(f"  ⚠ API retry {attempt}/{max_retries} for {endpoint} — {e} — waiting {backoff}s...")
                time.sleep(backoff)
            else:
                print(f"  ❌ API failed after {max_retries} retries for {endpoint} — {e}")
                return []


# ══════════════════════════════════════════════════════════════════════
# TENNIS — Dynamic tournament detection
# ══════════════════════════════════════════════════════════════════════

def detect_active_tennis():
    """
    Poll the free /v4/sports endpoint to find which tennis tournaments
    currently have active markets. Returns list of active sport keys.

    This costs 0 API usage — the /sports endpoint is free.
    Avoids wasting 30 API calls on inactive tournament keys.
    """
    try:
        data = _api_get('/sports', {'all': 'false'})
        if not data:
            return []
        active = []
        for sport in data:
            key = sport.get('key', '')
            if key.startswith('tennis_') and sport.get('active', False):
                active.append(key)
        return active
    except Exception as e:
        print(f"  ⚠ Tennis detection failed: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════
# FETCH CURRENT ODDS
# ══════════════════════════════════════════════════════════════════════

def fetch_odds(sport, markets='h2h,spreads,totals', tag='CURRENT'):
    """
    Fetch current odds for a sport and store in database.
    This replaces the Google Sheets ODDS_CURRENT tab.
    """
    data = _api_get(f"/sports/{sport}/odds", {
        'regions': 'us',
        'markets': markets,
        'oddsFormat': 'american',
        'dateFormat': 'iso',
    })

    if not data:
        print(f"  ⚠ {sport}: API returned 0 events — possible outage or off-day")
        return data

    conn = sqlite3.connect(DB_PATH)
    # v25.3: real UTC, not local-ET-pretending-to-be-UTC. Same fix as fetch_props.
    # snapshot_date and snapshot_time are now real UTC so the grader's CLV
    # closing-line filter (latest snapshot before commence_time) actually works.
    # Previously: NCAAB ML CLVs like Saint Mary's -37% were measurement artifacts
    # from in-game snapshots being treated as pre-game.
    now = datetime.now(timezone.utc)
    rows = []
    _in_progress_skipped = 0

    for event in data:
        event_id = event['id']
        home = event['home_team']
        away = event['away_team']
        commence = event['commence_time']

        # v25.3: Skip in-progress games — never capture in-game game-line prices.
        # Same defense-in-depth pattern as fetch_props. Some bookmakers leave
        # markets open during games with live odds; we never want those in our
        # closing-line snapshots.
        try:
            gt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
            if gt <= now:
                _in_progress_skipped += 1
                continue
        except Exception:
            pass

        for book in event.get('bookmakers', []):
            book_name = book['title']
            for market in book.get('markets', []):
                market_key = market['key']
                for outcome in market.get('outcomes', []):
                    rows.append((
                        now.strftime('%Y-%m-%d'),
                        now.strftime('%H:%M:%S'),
                        tag,
                        sport,
                        event_id,
                        commence,
                        home, away,
                        book_name,
                        market_key,
                        outcome['name'],
                        outcome.get('point'),
                        outcome.get('price'),
                    ))

    conn.executemany("""
        INSERT INTO odds (snapshot_date, snapshot_time, tag, sport, event_id,
            commence_time, home, away, book, market, selection, line, odds)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

    # ── Auto-capture openers (first-seen lines for each event) ──
    _capture_openers(conn, rows, sport)

    # Also update market_consensus
    _update_consensus(conn, data, sport, tag)
    conn.close()
    msg = f"  Stored {len(rows)} odds rows for {sport}"
    if _in_progress_skipped:
        msg += f" (skipped {_in_progress_skipped} in-progress events)"
    print(msg)
    return data


def _capture_openers(conn, odds_rows, sport):
    """Store first-seen lines as openers. Only inserts if event not yet seen."""
    # v25.3: real UTC, same fix as fetch_odds. Openers feed CLV calculations.
    now = datetime.now(timezone.utc)
    inserted = 0
    for r in odds_rows:
        # r = (date, time, tag, sport, event_id, commence, home, away, book, market, selection, line, odds)
        _, _, _, _, eid, commence, home, away, book, market, sel, line, odds = r
        try:
            existing = conn.execute(
                "SELECT 1 FROM openers WHERE event_id=? AND book=? AND market=? AND selection=? LIMIT 1",
                (eid, book, market, sel)
            ).fetchone()
            if not existing:
                conn.execute("""
                    INSERT INTO openers (snapshot_date, sport, event_id, book, market,
                        selection, line, odds, timestamp)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (now.strftime('%Y-%m-%d'), sport, eid, book, market,
                      sel, line, odds, now.isoformat()))
                inserted += 1
        except Exception as e:
            print(f"  ⚠ Opener insert failed for {eid}: {e}")
    if inserted > 0:
        conn.commit()
        print(f"  📌 {inserted} opening lines captured for {sport}")


def _update_consensus(conn, api_data, sport, tag):
    """Compute market consensus from all books for each event.
    
    KEY DESIGN: Uses MEDIAN spread across books as the true market number.
    This prevents the bug where picking "best number" independently for each
    side leads to contradictory spreads (both sides shown as underdog).
    
    The MEDIAN is what the model evaluates edge against.
    The BOOK recommendation is the best NY-legal line available.
    
    For MLs: picks best available odds from NY-legal books.
    """
    NY_LEGAL = {'DraftKings', 'FanDuel', 'BetMGM', 'Caesars', 'BetRivers',
                'ESPN BET', 'PointsBet (US)', 'Fanatics', 'Hard Rock Bet', 'Bally Bet'}
    
    now = datetime.now().strftime('%Y-%m-%d')

    for event in api_data:
        event_id = event['id']
        home = event['home_team']
        away = event['away_team']
        commence = event['commence_time']

        # ── Collect ALL lines from ALL books ──
        home_spreads = []     # [(point, price, book, is_legal)]
        away_spreads = []
        over_totals = []
        under_totals = []
        home_mls = []         # [(price, book, is_legal)]
        away_mls = []

        for book in event.get('bookmakers', []):
            bn = book['title']
            is_legal = bn in NY_LEGAL
            
            for market in book.get('markets', []):
                mk = market['key']
                for o in market.get('outcomes', []):
                    name, price, point = o['name'], o.get('price'), o.get('point')

                    if mk == 'h2h':
                        if name == home and price:
                            home_mls.append((price, bn, is_legal))
                        elif name == away and price:
                            away_mls.append((price, bn, is_legal))

                    elif mk == 'spreads':
                        if name == home and price and point is not None:
                            home_spreads.append((point, price, bn, is_legal))
                        elif name == away and price and point is not None:
                            away_spreads.append((point, price, bn, is_legal))

                    elif mk == 'totals':
                        if name == 'Over' and price and point is not None:
                            over_totals.append((point, price, bn, is_legal))
                        elif name == 'Under' and price and point is not None:
                            under_totals.append((point, price, bn, is_legal))

        # ── Compute MEDIAN spread (true market consensus) ──
        # This is what the model evaluates against. No single book outlier
        # can skew it. Both sides are guaranteed to be mirrors.
        best = {
            'home_spread': None, 'home_spread_odds': None, 'home_spread_book': None,
            'away_spread': None, 'away_spread_odds': None, 'away_spread_book': None,
            'over_total': None, 'over_odds': None, 'over_book': None,
            'under_total': None, 'under_odds': None, 'under_book': None,
            'home_ml': None, 'home_ml_book': None,
            'away_ml': None, 'away_ml_book': None,
        }

        if home_spreads:
            # Median home spread across all books
            sorted_spreads = sorted([s[0] for s in home_spreads])
            mid = len(sorted_spreads) // 2
            if len(sorted_spreads) % 2 == 0 and len(sorted_spreads) > 1:
                median_hs = (sorted_spreads[mid - 1] + sorted_spreads[mid]) / 2
            else:
                median_hs = sorted_spreads[mid]
            
            # Find best NY-legal book for this spread (closest to or better than median)
            # "Better" for home = less negative (higher number)
            legal_spreads = [s for s in home_spreads if s[3]]  # legal only
            if not legal_spreads:
                legal_spreads = home_spreads  # fallback to all
            
            # Among legal books, find the one with the best number, then best juice
            legal_spreads.sort(key=lambda s: (s[0], s[1]), reverse=True)
            pick = legal_spreads[0]
            best['home_spread'] = pick[0]  # Actual line from this book
            best['home_spread_odds'] = pick[1]
            best['home_spread_book'] = pick[2]

        if away_spreads:
            sorted_spreads = sorted([s[0] for s in away_spreads])
            mid = len(sorted_spreads) // 2
            if len(sorted_spreads) % 2 == 0 and len(sorted_spreads) > 1:
                median_as = (sorted_spreads[mid - 1] + sorted_spreads[mid]) / 2
            else:
                median_as = sorted_spreads[mid]
            
            legal_spreads = [s for s in away_spreads if s[3]]
            if not legal_spreads:
                legal_spreads = away_spreads
            # For away/dog: MORE points is better (+6.5 > +3.5).
            # Sort by spread ascending (most positive = most points), then best juice.
            legal_spreads.sort(key=lambda s: (s[0], s[1]), reverse=True)
            pick = legal_spreads[0]
            best['away_spread'] = pick[0]
            best['away_spread_odds'] = pick[1]
            best['away_spread_book'] = pick[2]

        # ── Sanity check: home and away spreads must be mirrors ──
        # If they're not (different books have wildly different lines),
        # use the median from home side and derive away from it.
        if best['home_spread'] is not None and best['away_spread'] is not None:
            expected_away = -best['home_spread']
            if abs(best['away_spread'] - expected_away) > 1.0:
                # Spreads are contradictory — force consistency using home median
                # Derive away from home to guarantee they're mirrors
                best['away_spread'] = -best['home_spread']
                # Find the legal book closest to the corrected away spread
                if away_spreads:
                    target = best['away_spread']
                    legal_as = [s for s in away_spreads if s[3]] or away_spreads
                    legal_as.sort(key=lambda s: (abs(s[0] - target), -s[1]))
                    pick = legal_as[0]
                    best['away_spread'] = pick[0]
                    best['away_spread_odds'] = pick[1]
                    best['away_spread_book'] = pick[2]

        # ── Totals: use median, pick best legal book ──
        if over_totals:
            sorted_t = sorted([t[0] for t in over_totals])
            mid = len(sorted_t) // 2
            median_ot = sorted_t[mid]
            
            legal_t = [t for t in over_totals if t[3]] or over_totals
            # For over: prefer lower total (easier to hit), then best juice
            legal_t.sort(key=lambda t: (t[0], -t[1]))
            pick = legal_t[0]
            best['over_total'] = pick[0]
            best['over_odds'] = pick[1]
            best['over_book'] = pick[2]

        if under_totals:
            sorted_t = sorted([t[0] for t in under_totals])
            mid = len(sorted_t) // 2
            median_ut = sorted_t[mid]
            
            legal_t = [t for t in under_totals if t[3]] or under_totals
            # For under: prefer higher total (easier to hit), then best juice
            legal_t.sort(key=lambda t: (-t[0], -t[1]))
            pick = legal_t[0]
            best['under_total'] = pick[0]
            best['under_odds'] = pick[1]
            best['under_book'] = pick[2]

        # ── MLs: best available odds from NY-legal books ──
        if home_mls:
            legal_ml = [m for m in home_mls if m[2]] or home_mls
            legal_ml.sort(key=lambda m: m[0], reverse=True)  # best odds
            best['home_ml'] = legal_ml[0][0]
            best['home_ml_book'] = legal_ml[0][1]

        if away_mls:
            legal_ml = [m for m in away_mls if m[2]] or away_mls
            legal_ml.sort(key=lambda m: m[0], reverse=True)
            best['away_ml'] = legal_ml[0][0]
            best['away_ml_book'] = legal_ml[0][1]

        # Get model spread from power ratings
        from model_engine import compute_model_spread, get_latest_ratings
        ratings = get_latest_ratings(conn, sport)
        model_spread = compute_model_spread(home, away, ratings, sport)

        # ── DELETE old rows for this event, then INSERT fresh ──
        conn.execute("""
            DELETE FROM market_consensus
            WHERE sport = ? AND event_id = ? AND tag = ?
        """, (sport, event_id, tag))

        conn.execute("""
            INSERT INTO market_consensus
                (snapshot_date, tag, sport, event_id, commence_time, home, away,
                 best_home_spread, best_home_spread_odds, best_home_spread_book,
                 best_away_spread, best_away_spread_odds, best_away_spread_book,
                 best_over_total, best_over_odds, best_over_book,
                 best_under_total, best_under_odds, best_under_book,
                 best_home_ml, best_home_ml_book, best_away_ml, best_away_ml_book,
                 model_spread)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (now, tag, sport, event_id, commence, home, away,
              best['home_spread'], best['home_spread_odds'], best['home_spread_book'],
              best['away_spread'], best['away_spread_odds'], best['away_spread_book'],
              best['over_total'], best['over_odds'], best['over_book'],
              best['under_total'], best['under_odds'], best['under_book'],
              best['home_ml'], best['home_ml_book'],
              best['away_ml'], best['away_ml_book'],
              model_spread))

    conn.commit()


# ══════════════════════════════════════════════════════════════════════
# FETCH SCORES & RESULTS (CRITICAL NEW FEATURE)
# ══════════════════════════════════════════════════════════════════════

def fetch_scores(sport, days_back=3):
    """
    Fetch completed game scores. This is the #1 missing piece.
    The /scores endpoint returns live and completed games.
    """
    data = _api_get(f"/sports/{sport}/scores", {
        'daysFrom': days_back,
        'dateFormat': 'iso',
    })

    conn = sqlite3.connect(DB_PATH)
    inserted = 0

    for event in data:
        if not event.get('completed', False):
            continue

        event_id = event['id']
        home = event['home_team']
        away = event['away_team']
        commence = event['commence_time']

        scores = {s['name']: int(s['score']) for s in event.get('scores', []) if s.get('score')}
        home_score = scores.get(home)
        away_score = scores.get(away)

        if home_score is None or away_score is None:
            continue

        winner = home if home_score > away_score else (away if away_score > home_score else 'DRAW')
        margin = home_score - away_score
        total = home_score + away_score

        # Get closing line (last odds snapshot before game time)
        closing = conn.execute("""
            SELECT line, odds FROM odds
            WHERE event_id = ? AND market = 'spreads' AND selection = ?
            ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1
        """, (event_id, home)).fetchone()

        closing_spread = closing[0] if closing else None

        closing_total_row = conn.execute("""
            SELECT line FROM odds
            WHERE event_id = ? AND market = 'totals' AND selection = 'Over'
            ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1
        """, (event_id,)).fetchone()
        closing_total = closing_total_row[0] if closing_total_row else None

        closing_ml = conn.execute("""
            SELECT odds FROM odds
            WHERE event_id = ? AND market = 'h2h' AND selection = ?
            ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1
        """, (event_id, home)).fetchone()
        closing_ml_home = closing_ml[0] if closing_ml else None

        closing_ml_a = conn.execute("""
            SELECT odds FROM odds
            WHERE event_id = ? AND market = 'h2h' AND selection = ?
            ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1
        """, (event_id, away)).fetchone()
        closing_ml_away = closing_ml_a[0] if closing_ml_a else None

        # ATS result
        ats_result = None
        if closing_spread is not None:
            home_margin_vs_spread = margin + closing_spread  # closing_spread is from home perspective
            if home_margin_vs_spread > 0:
                ats_result = 'WIN'
            elif home_margin_vs_spread < 0:
                ats_result = 'LOSS'
            else:
                ats_result = 'PUSH'

        ou_result = None
        if closing_total is not None:
            if total > closing_total:
                ou_result = 'OVER'
            elif total < closing_total:
                ou_result = 'UNDER'
            else:
                ou_result = 'PUSH'

        try:
            conn.execute("""
                INSERT OR REPLACE INTO results
                    (sport, event_id, commence_time, home, away,
                     home_score, away_score, winner, completed,
                     closing_spread, closing_total, closing_ml_home, closing_ml_away,
                     ats_home_result, ou_result, actual_total, actual_margin, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,?)
            """, (sport, event_id, commence, home, away,
                  home_score, away_score, winner,
                  closing_spread, closing_total, closing_ml_home, closing_ml_away,
                  ats_result, ou_result, total, margin,
                  datetime.now().isoformat()))
            inserted += 1
        except Exception as e:
            print(f"  Error inserting result for {event_id}: {e}")

    conn.commit()

    # NOTE: Grading is handled exclusively by grader.py (daily_grade_and_report).
    # Previously _grade_bets was called here, but this created a dual grading path
    # where soccer ML draws could be graded as PUSH (old logic) before grader.py
    # could correctly grade them as LOSS. Single grading path = no conflicts.
    conn.close()
    print(f"  Stored {inserted} results for {sport}")
    return inserted


def _grade_bets(conn, sport):
    """Grade open bets against actual results."""
    open_bets = conn.execute("""
        SELECT b.id, b.event_id, b.market_type, b.selection, b.line, b.odds,
               b.model_prob, b.implied_prob
        FROM bets b
        WHERE b.sport = ? AND b.result IS NULL
    """, (sport,)).fetchall()

    for bet in open_bets:
        bet_id, event_id = bet[0], bet[1]
        result_row = conn.execute("""
            SELECT home_score, away_score, winner, actual_margin, actual_total,
                   closing_spread, closing_ml_home, closing_ml_away, home, away
            FROM results WHERE event_id = ? AND completed = 1
        """, (event_id,)).fetchone()

        if not result_row:
            continue

        home_score, away_score, winner, margin, total = result_row[:5]
        closing_spread, closing_ml_home, closing_ml_away = result_row[5:8]
        home, away = result_row[8:10]

        market_type = bet[2]
        selection = bet[3]
        line = bet[4]
        odds = bet[5]

        result = None
        profit = 0

        if market_type == 'SPREAD':
            team = selection.split(' ')[0]  # rough parse
            if home in selection:
                cover_margin = margin + line  # line is from this team's perspective
                if cover_margin > 0:
                    result = 'WIN'
                elif cover_margin < 0:
                    result = 'LOSS'
                else:
                    result = 'PUSH'
            elif away in selection:
                cover_margin = -margin + line
                if cover_margin > 0:
                    result = 'WIN'
                elif cover_margin < 0:
                    result = 'LOSS'
                else:
                    result = 'PUSH'

        elif market_type == 'MONEYLINE':
            if home in selection:
                result = 'WIN' if winner == home else ('PUSH' if winner == 'DRAW' else 'LOSS')
            elif away in selection:
                result = 'WIN' if winner == away else ('PUSH' if winner == 'DRAW' else 'LOSS')

        # Calculate profit
        if result == 'WIN':
            if odds > 0:
                profit = odds / 100.0
            else:
                profit = 100.0 / abs(odds)
        elif result == 'LOSS':
            profit = -1.0
        else:
            profit = 0.0

        # Calculate CLV
        clv = None
        if market_type == 'MONEYLINE' and closing_ml_home and closing_ml_away:
            from model_engine import american_to_implied_prob
            bet_implied = american_to_implied_prob(odds)
            if home in selection and closing_ml_home:
                close_implied = american_to_implied_prob(closing_ml_home)
                if bet_implied and close_implied:
                    clv = (close_implied - bet_implied) * 100
            elif away in selection and closing_ml_away:
                close_implied = american_to_implied_prob(closing_ml_away)
                if bet_implied and close_implied:
                    clv = (close_implied - bet_implied) * 100

        conn.execute("""
            UPDATE bets SET result=?, profit=?, clv=?
            WHERE id=?
        """, (result, profit, clv, bet_id))

    conn.commit()


# ══════════════════════════════════════════════════════════════════════
# FETCH PLAYER PROPS
# ══════════════════════════════════════════════════════════════════════

def fetch_props(sport, event_id=None):
    """Fetch player props for a sport.
    
    IMPORTANT: The Odds API v4 requires per-event fetching for player props.
    The bulk /odds endpoint returns 422 for player prop markets.
    We must use: /sports/{sport}/events/{eventId}/odds?markets=player_points,...
    
    We get event IDs from already-fetched odds data (FREE — already in DB).
    Each event costs 1 API call.
    
    CRITICAL: Market keys are SPORT-SPECIFIC. Basketball uses player_threes,
    NHL uses player_shots_on_goal, soccer has limited prop coverage.
    """
    # Sport-specific prop market keys from The Odds API documentation
    PROP_MARKETS_BY_SPORT = {
        'basketball_nba': 'player_points,player_rebounds,player_assists,player_threes,player_blocks,player_steals',
        'basketball_ncaab': 'player_points,player_rebounds,player_assists,player_threes',
        'icehockey_nhl': 'player_points,player_assists,player_shots_on_goal,player_power_play_points,player_blocked_shots',
        # Soccer: limited props — shots and shots on target from US bookmakers
        'soccer_epl': 'player_shots,player_shots_on_target,player_assists',
        'soccer_italy_serie_a': 'player_shots,player_shots_on_target,player_assists',
        'soccer_spain_la_liga': 'player_shots,player_shots_on_target,player_assists',
        'soccer_germany_bundesliga': 'player_shots,player_shots_on_target,player_assists',
        'soccer_france_ligue_one': 'player_shots,player_shots_on_target,player_assists',
        'soccer_uefa_champs_league': 'player_shots,player_shots_on_target,player_assists',
        'soccer_usa_mls': 'player_shots,player_shots_on_target,player_assists',
        'baseball_mlb': 'batter_hits,batter_total_bases,batter_home_runs,batter_rbis,batter_runs_scored,batter_strikeouts,pitcher_strikeouts,pitcher_outs,pitcher_hits_allowed,pitcher_earned_runs',
    }
    prop_markets = PROP_MARKETS_BY_SPORT.get(sport, 'player_points,player_assists')
    
    conn = sqlite3.connect(DB_PATH)
    # v25.2: real UTC, not local-ET-pretending-to-be-UTC. Every downstream use of `now`
    # (commence_time filter, props.snapshot_date/time, prop_snapshots.captured_at) needs
    # to be real UTC so it can be compared to commence_time from the API (also real UTC).
    # Previously: bet at 11:00 ET wrote captured_at='11:00:00Z', grader compared against
    # commence_time='15:00:00Z' (real UTC = 11:00 ET) and string-sorted them as if both
    # were UTC, picking in-game snapshots as the "closing line". Cruz CLV -25.5% bug.
    now = datetime.now(timezone.utc)
    rows = []

    if event_id:
        event_ids = [(event_id,)]
    else:
        # Get TODAY's event IDs from market_consensus (already fetched, no extra cost)
        now_utc = now.strftime('%Y-%m-%dT%H:%M:%SZ')
        tomorrow_5am = (now + timedelta(days=1)).strftime('%Y-%m-%dT05:00:00Z')
        event_ids = conn.execute("""
            SELECT DISTINCT event_id 
            FROM market_consensus 
            WHERE sport=? AND commence_time >= ? AND commence_time <= ?
        """, (sport, now_utc, tomorrow_5am)).fetchall()
        
        if not event_ids:
            # Wider fallback — any future game
            event_ids = conn.execute("""
                SELECT DISTINCT event_id 
                FROM market_consensus 
                WHERE sport=? AND commence_time >= ?
                ORDER BY commence_time LIMIT 15
            """, (sport, now_utc)).fetchall()
    
    if not event_ids:
        print(f"  {sport}: no upcoming events for props")
        conn.close()
        return

    # v25.1: MLB prop timing gate — 3hr window for new pick evaluation,
    # but always fetch for events with existing PENDING prop bets (for CLV).
    # Without closing-line snapshots, prop CLV is skewed (e.g., Gorman RBI
    # showed -12.3% because last snapshot was 3hrs pre-game).
    if 'baseball' in sport:
        MLB_PROP_WINDOW_HOURS = 3
        from datetime import timezone as _tz
        now_tz = datetime.now(_tz.utc)

        # Events with pending prop bets today — always fetch these for CLV
        pending_eids = set(r[0] for r in conn.execute("""
            SELECT DISTINCT event_id FROM bets
            WHERE market_type = 'PROP' AND result = 'PENDING'
            AND sport LIKE ? AND DATE(created_at) >= DATE('now', '-1 day')
        """, (f'%{sport.split("_")[-1]}%',)).fetchall())

        _filtered = []
        _skipped = 0
        _clv_only = 0
        _in_progress = 0
        for eid_row in event_ids:
            eid = eid_row[0]

            # Get commence time once — shared by in-progress check + 3hr window
            gt = None
            ct_row = conn.execute(
                "SELECT commence_time FROM market_consensus WHERE event_id=? LIMIT 1",
                (eid,)).fetchone()
            if ct_row and ct_row[0]:
                try:
                    gt = datetime.fromisoformat(ct_row[0].replace('Z', '+00:00'))
                except Exception:
                    pass

            # v25.2: HARD SKIP in-progress games (defense in depth). The upstream
            # SQL filter at the top of fetch_props already excludes these via
            # `commence_time >= now_utc`, but this explicit check prevents
            # regressions if that filter is ever modified. Applies to BOTH new
            # events AND pending-bet events (no in-game prop captures, ever).
            if gt is not None and gt <= now_tz:
                _in_progress += 1
                continue

            # Always fetch events with pending bets (for closing line CLV)
            if eid in pending_eids:
                _clv_only += 1
                _filtered.append(eid_row)
                continue

            # Gate new events to 3hr window
            if gt is not None:
                hours_until = (gt - now_tz).total_seconds() / 3600
                if hours_until > MLB_PROP_WINDOW_HOURS:
                    _skipped += 1
                    continue
            _filtered.append(eid_row)
        if _skipped or _clv_only or _in_progress:
            print(f"  {sport}: skipped {_skipped} events >3hrs out, {_in_progress} in-progress, fetching {_clv_only} for CLV tracking")
        event_ids = _filtered
        if not event_ids:
            print(f"  {sport}: no events within prop window")
            conn.close()
            return

    # v12 FIX: Cap at 15 events per sport to control API budget.
    # NCAAB can have 25+ games on a Saturday — each one costs ~5 usage.
    MAX_PROP_EVENTS = 15
    if len(event_ids) > MAX_PROP_EVENTS:
        print(f"  {sport}: capping props at {MAX_PROP_EVENTS} events (of {len(event_ids)} available)")
        event_ids = event_ids[:MAX_PROP_EVENTS]
    
    print(f"  {sport}: fetching props for {len(event_ids)} events...")
    success_count = 0
    
    for eid_row in event_ids:
        eid = eid_row[0]
        try:
            data = _api_get(f"/sports/{sport}/events/{eid}/odds", {
                'regions': 'us',
                'markets': prop_markets,
                'oddsFormat': 'american',
            })
            
            if not isinstance(data, dict):
                continue
            
            home = data.get('home_team', '')
            away = data.get('away_team', '')
            commence = data.get('commence_time', '')
            
            event_rows = 0
            for book in data.get('bookmakers', []):
                for market in book.get('markets', []):
                    for o in market.get('outcomes', []):
                        # Odds API props format:
                        #   name = "Over" or "Under"
                        #   description = "Player Name"
                        #   point = 24.5 (the line)
                        #   price = -110 (the odds)
                        desc = o.get('description', '')  # Player name
                        name = o.get('name', '')          # Over/Under
                        
                        if desc and name:
                            selection = f"{desc} - {name}"
                        else:
                            selection = name or desc or ''
                        
                        rows.append((
                            now.strftime('%Y-%m-%d'),
                            now.strftime('%H:%M:%S'),
                            'CURRENT',
                            sport, eid, commence, home, away,
                            book['title'], market['key'],
                            selection, o.get('point'), o.get('price'),
                        ))
                        event_rows += 1
            
            if event_rows > 0:
                success_count += 1
                
        except Exception as e:
            err = str(e)
            if '422' in err:
                print(f"    {eid[:12]}... — props not available for this event")
            elif '404' in err:
                continue  # Event not found, skip silently
            else:
                print(f"    {eid[:12]}... — error: {err}")
            continue
    
    if rows:
        conn.executemany("""
            INSERT INTO props (snapshot_date, snapshot_time, tag, sport, event_id,
                commence_time, home, away, book, market, selection, line, odds)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)

        # ── Also store parsed prop_snapshots for line movement tracking ──
        snap_rows = []
        opener_rows = []
        captured_at = now.strftime('%Y-%m-%dT%H:%M:%SZ')

        for r in rows:
            _, _, _, sp, eid, commence, home, away, book_name, mkt, sel, line, price = r
            if line is None or price is None:
                continue
            # Parse player name and side from selection ("Player Name - Over/Under")
            player, side = '', ''
            if ' - Over' in sel:
                player = sel.split(' - Over')[0].strip()
                side = 'Over'
            elif ' - Under' in sel:
                player = sel.split(' - Under')[0].strip()
                side = 'Under'
            if not player:
                continue

            # Implied probability
            imp = 100.0/(price+100.0) if price > 0 else abs(price)/(abs(price)+100.0) if price else None

            snap_rows.append((
                captured_at, sp, eid, commence, home, away,
                book_name, mkt, player, side, line, price, imp
            ))

            # Track openers (first-seen line for this player/prop/event)
            if side == 'Over':
                opener_rows.append((captured_at, sp, eid, player, mkt, line, price, None))
            else:
                opener_rows.append((captured_at, sp, eid, player, mkt, line, None, price))

        if snap_rows:
            conn.executemany("""
                INSERT INTO prop_snapshots (captured_at, sport, event_id, commence_time,
                    home, away, book, market, player, side, line, odds, implied_prob)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, snap_rows)

        # Openers — INSERT OR IGNORE so only first fetch wins
        for orow in opener_rows:
            try:
                if orow[6] is not None:  # Over odds
                    conn.execute("""
                        INSERT OR IGNORE INTO prop_openers 
                        (first_seen, sport, event_id, player, market, opening_line, opening_over_odds, opening_under_odds)
                        VALUES (?,?,?,?,?,?,?,?)
                    """, orow)
                else:  # Under odds
                    conn.execute("""
                        UPDATE prop_openers SET opening_under_odds=?
                        WHERE event_id=? AND player=? AND market=? AND opening_under_odds IS NULL
                    """, (orow[7], orow[2], orow[3], orow[4]))
            except Exception as e:
                print(f"  ⚠ Prop opener update failed: {e}")

        conn.commit()
    
    conn.close()
    print(f"  ✅ {sport}: {len(rows)} props stored from {success_count}/{len(event_ids)} events")
    if rows:
        # Count unique players
        players = set()
        for r in rows:
            sel = r[10]
            if ' - Over' in sel:
                players.add(sel.split(' - Over')[0].strip())
            elif ' - Under' in sel:
                players.add(sel.split(' - Under')[0].strip())
        print(f"     {len(players)} unique players with prop lines")


# ══════════════════════════════════════════════════════════════════════
# DAILY RUNNER
# ══════════════════════════════════════════════════════════════════════

def daily_run(sports=None):
    """
    Complete daily workflow:
    1. Fetch scores (grade yesterday's bets)
    2. Fetch current odds
    3. Generate predictions
    4. Save & display picks
    """
    from model_engine import generate_predictions, print_picks, save_picks_to_db

    if sports is None:
        sports = TRACKED_SPORTS

    print("═" * 70)
    print(f"  DAILY RUN — {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    print("═" * 70)

    # Step 1: Fetch completed scores
    print("\n📊 Step 1: Fetching game results...")
    for sp in sports:
        try:
            fetch_scores(sp, days_back=3)
        except Exception as e:
            print(f"  Error fetching scores for {sp}: {e}")

    # Step 2: Fetch current odds
    print("\n📈 Step 2: Fetching current odds...")
    for sp in sports:
        try:
            fetch_odds(sp, tag='CURRENT')
        except Exception as e:
            print(f"  Error fetching odds for {sp}: {e}")

    # Step 3: Generate predictions
    print("\n🧠 Step 3: Running model...")
    conn = sqlite3.connect(DB_PATH)
    all_picks = []
    for sp in sports:
        picks = generate_predictions(conn, sport=sp)
        all_picks.extend(picks)

    # Step 4: Save and display
    if all_picks:
        save_picks_to_db(conn, all_picks)
    conn.close()

    print_picks(all_picks, "TODAY'S PICKS")
    return all_picks


if __name__ == '__main__':
    daily_run()
