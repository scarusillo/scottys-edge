"""
grader.py v11 — Performance Tracking with CLV Analysis

THE #1 METRIC: Closing Line Value (CLV)
Walters p.267: "If you're consistently beating the closing line,
you're a winning bettor. Period. Results will come."

This grader:
  1. Grades bets against actual scores (W/L/P)
  2. Computes CLV by comparing our bet line to the closing line
  3. Accumulates player_results for the props historical signal
  4. Generates reports by sport, confidence, CLV performance

DATA MODEL NOTE — `graded_bets` is authoritative, not `bets`:
  - TAINTED/DUPLICATE rows in `bets` are intentionally excluded from
    `graded_bets` (filters at lines 429, 909, 1149, 1832). Counts:
    ~101 TAINTED in `bets` have no `graded_bets` row as of 2026-04-20.
    These are pre-v25.34 scrubs that were never graded — leave them.
  - `graded_bets` rows with `bet_id IS NULL` are backfills (e.g. ids
    1413-1415: PROP_BOOK_ARB picks detected by v25.31 scanner but
    blocked by the pre-v25.34 `_passes_filter` bug, retroactively
    graded once the bug was fixed). They contribute real P/L but
    won't JOIN against `bets`.
  - Performance queries must use `graded_bets` directly; never JOIN
    `bets` -> `graded_bets` unless you are explicitly filtering to
    picks that passed all gates at save time.
"""
import sqlite3, os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def ensure_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS graded_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            graded_at TEXT,
            bet_id INTEGER,
            sport TEXT,
            event_id TEXT,
            selection TEXT,
            market_type TEXT,
            book TEXT,
            line REAL,
            odds REAL,
            edge_pct REAL,
            confidence TEXT,
            units REAL,
            result TEXT,
            pnl_units REAL,
            closing_line REAL,
            clv REAL,
            created_at TEXT,
            side_type TEXT,
            spread_bucket TEXT,
            edge_bucket TEXT,
            timing TEXT,
            context_factors TEXT,
            context_confirmed INT,
            market_tier TEXT,
            model_spread REAL,
            day_of_week TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date TEXT NOT NULL,
            sport TEXT NOT NULL,
            event_id TEXT NOT NULL,
            player TEXT NOT NULL,
            stat_type TEXT NOT NULL,
            actual_value REAL,
            prop_line REAL,
            result TEXT,
            UNIQUE(event_id, player, stat_type)
        )
    """)
    # Migrate existing graded_bets tables
    existing = {row[1] for row in conn.execute("PRAGMA table_info(graded_bets)").fetchall()}
    migrations = {
        'side_type': 'TEXT', 'spread_bucket': 'TEXT', 'edge_bucket': 'TEXT',
        'timing': 'TEXT', 'context_factors': 'TEXT', 'context_confirmed': 'INT',
        'market_tier': 'TEXT', 'model_spread': 'REAL', 'day_of_week': 'TEXT',
    }
    for col, dtype in migrations.items():
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE graded_bets ADD COLUMN {col} {dtype}")
            except Exception:
                pass
    conn.commit()


# ═══════════════════════════════════════════════════════════════════
# CLV COMPUTATION
# ═══════════════════════════════════════════════════════════════════

def _get_prop_closing_line(conn, event_id, market, selection, bet_book=None):
    """
    v25: Get closing line for props from prop_snapshots table.

    The odds table only stores game lines (spreads/totals/h2h).
    Prop odds live in prop_snapshots with schema:
      (event_id, book, market, player, side, line, odds, captured_at, commence_time)

    Only uses PRE-GAME snapshots (captured_at < commence_time) to avoid
    in-game live odds contaminating CLV calculations.

    Selection format: "Player Name OVER/UNDER 0.5 STAT"
    """
    import re as _re

    # Parse player name and side from selection
    m = _re.match(r'^(.+?)\s+(OVER|UNDER)\s+[\d.]+\s+', selection, _re.IGNORECASE)
    if not m:
        return None, None, None, None

    player_name = m.group(1).strip()
    side = m.group(2).capitalize()  # 'Over' or 'Under'

    # Extract the bet line from selection
    line_m = _re.search(r'(OVER|UNDER)\s+([\d.]+)', selection, _re.IGNORECASE)
    if not line_m:
        return None, None, None, None
    bet_line = float(line_m.group(2))

    # Get the game start time to filter out in-game snapshots
    ct_row = conn.execute("""
        SELECT commence_time FROM prop_snapshots
        WHERE event_id=? AND commence_time IS NOT NULL LIMIT 1
    """, (event_id,)).fetchone()
    commence_filter = ""
    params_extra = ()
    if ct_row and ct_row[0]:
        commence_filter = " AND captured_at < ?"
        params_extra = (ct_row[0],)

    # Priority 1: Same book, same player, same market, same line, latest PRE-GAME snapshot
    if bet_book:
        row = conn.execute(f"""
            SELECT line, odds, captured_at, book FROM prop_snapshots
            WHERE event_id=? AND market=? AND player=? AND side=? AND line=? AND book=?
            {commence_filter}
            ORDER BY captured_at DESC LIMIT 1
        """, (event_id, market, player_name, side, bet_line, bet_book) + params_extra).fetchone()
        if row:
            return row[0], row[1], row[2], row[3]

    # Priority 2: Consensus across all books — latest PRE-GAME snapshot per book
    rows = conn.execute(f"""
        SELECT ps.line, ps.odds, ps.book, ps.captured_at
        FROM prop_snapshots ps
        INNER JOIN (
            SELECT book, MAX(captured_at) as max_cap
            FROM prop_snapshots
            WHERE event_id=? AND market=? AND player=? AND side=? AND line=?
            {commence_filter}
            GROUP BY book
        ) latest ON ps.book = latest.book AND ps.captured_at = latest.max_cap
        WHERE ps.event_id=? AND ps.market=? AND ps.player=? AND ps.side=? AND ps.line=?
        {commence_filter}
    """, (event_id, market, player_name, side, bet_line) + params_extra +
         (event_id, market, player_name, side, bet_line) + params_extra).fetchall()

    # Fallback: STRIKEOUTS can be pitcher or batter — try the other if no rows
    if not rows and market == 'pitcher_strikeouts':
        rows = conn.execute(f"""
            SELECT ps.line, ps.odds, ps.book, ps.captured_at
            FROM prop_snapshots ps
            INNER JOIN (
                SELECT book, MAX(captured_at) as max_cap
                FROM prop_snapshots
                WHERE event_id=? AND market='batter_strikeouts' AND player=? AND side=? AND line=?
                {commence_filter}
                GROUP BY book
            ) latest ON ps.book = latest.book AND ps.captured_at = latest.max_cap
            WHERE ps.event_id=? AND ps.market='batter_strikeouts' AND ps.player=? AND ps.side=? AND ps.line=?
            {commence_filter}
        """, (event_id, player_name, side, bet_line) + params_extra +
             (event_id, player_name, side, bet_line) + params_extra).fetchall()

    if not rows:
        return None, None, None, None

    # Median of implied probabilities across books (same as h2h logic)
    def _impl(o):
        o = float(o)
        if o > 0: return 100.0 / (o + 100.0)
        elif o < 0: return abs(o) / (abs(o) + 100.0)
        return 0.5

    odds_rows = [(r[1], r) for r in rows if r[1] is not None]
    if odds_rows:
        sorted_rows = sorted(
            [(_impl(o), o, r) for o, r in odds_rows],
            key=lambda x: x[0]
        )
        mid = len(sorted_rows) // 2
        _, median_odds, median_row = sorted_rows[mid]
        return median_row[0], median_odds, median_row[3], f"consensus({len(odds_rows)})"

    row = rows[0]
    return row[0], row[1], row[3], row[2]


def get_closing_line(conn, event_id, market, selection, bet_book=None):
    """
    Get the closing line for a bet.

    v12 FIX: Prioritizes SAME BOOK comparison. Cross-book CLV is unreliable
    because different books have different lines (especially ML and totals).

    Priority:
      1. Same book's last snapshot (apples-to-apples)
      2. Consensus median across all books (if same book unavailable)

    Returns: (line, odds, snapshot_time, book) or (None, None, None, None)
    """
    import re as _re
    
    # Normalize the selection to match odds table format
    odds_selection = selection
    
    if selection.startswith('OVER ') or selection.startswith('Over '):
        odds_selection = 'Over'
    elif selection.startswith('UNDER ') or selection.startswith('Under '):
        odds_selection = 'Under'
    elif market == 'spreads':
        odds_selection = _re.sub(r'\s*[+-]?\d+\.?\d*$', '', selection).strip()
    elif market == 'h2h':
        odds_selection = selection.replace(' ML (cross-mkt)', '').replace(' ML', '').strip()
    
    if '@' in odds_selection and ('OVER' in selection or 'UNDER' in selection):
        odds_selection = 'Over' if 'OVER' in selection else 'Under'

    # v25.16: Filter out in-game live odds (mirrors _get_prop_closing_line)
    ct_row = conn.execute(
        "SELECT commence_time FROM odds WHERE event_id=? AND commence_time IS NOT NULL LIMIT 1",
        (event_id,)
    ).fetchone()
    commence_filter = ""
    params_ct = ()
    if ct_row and ct_row[0]:
        commence_filter = " AND (snapshot_date || ' ' || snapshot_time) < ?"
        params_ct = (ct_row[0],)

    # Priority 1: Same book's last snapshot (apples-to-apples comparison)
    if bet_book:
        row = conn.execute("""
            SELECT line, odds, snapshot_date, snapshot_time, book FROM odds
            WHERE event_id=? AND market=? AND selection=? AND book=?""" + commence_filter + """
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT 1
        """, (event_id, market, odds_selection, bet_book) + params_ct).fetchone()
        if row:
            snap_ts = f"{row[2]} {row[3]}" if row[2] and row[3] else None
            return row[0], row[1], snap_ts, row[4]

    # Priority 2: Consensus — get ALL books' last snapshot, take median
    # Use a subquery to get each book's latest snapshot
    rows = conn.execute("""
        SELECT o.line, o.odds, o.book, o.snapshot_date, o.snapshot_time
        FROM odds o
        INNER JOIN (
            SELECT book, MAX(snapshot_date || ' ' || snapshot_time) as max_snap
            FROM odds
            WHERE event_id=? AND market=? AND selection=?""" + commence_filter + """
            GROUP BY book
        ) latest ON o.book = latest.book
            AND (o.snapshot_date || ' ' || o.snapshot_time) = latest.max_snap
        WHERE o.event_id=? AND o.market=? AND o.selection=?""" + commence_filter + """
    """, (event_id, market, odds_selection) + params_ct + (event_id, market, odds_selection) + params_ct).fetchall()
    
    if not rows:
        # v25: Check prop_snapshots table for prop markets
        # Props are stored separately from game odds — 5M+ rows of closing data
        return _get_prop_closing_line(conn, event_id, market, selection, bet_book)

    # For spreads/totals: median of lines across books
    if market in ('spreads', 'totals'):
        valid_lines = sorted([r[0] for r in rows if r[0] is not None])
        if valid_lines:
            mid = len(valid_lines) // 2
            median_line = valid_lines[mid]
            # Find the row closest to median for the odds value
            best = min(rows, key=lambda r: abs((r[0] or 0) - median_line))
            snap_ts = f"{best[3]} {best[4]}" if best[3] and best[4] else None
            return median_line, best[1], snap_ts, f"consensus({len(rows)})"
    
    # For ML (h2h): median of implied probabilities across books
    elif market == 'h2h':
        odds_list = [(r[1], r) for r in rows if r[1] is not None]
        if odds_list:
            # Sort by implied probability
            def _to_impl(o):
                o = float(o)
                if o > 0: return 100.0 / (o + 100.0)
                elif o < 0: return abs(o) / (abs(o) + 100.0)
                return 0.5
            
            implied_with_rows = sorted(
                [(_to_impl(o), o, r) for o, r in odds_list],
                key=lambda x: x[0]
            )
            mid = len(implied_with_rows) // 2
            median_impl, median_odds_val, median_row = implied_with_rows[mid]
            snap_ts = f"{median_row[3]} {median_row[4]}" if median_row[3] and median_row[4] else None
            return None, median_odds_val, snap_ts, f"consensus({len(odds_list)})"
    
    # Fallback: just return the single latest row
    row = rows[0]
    snap_ts = f"{row[3]} {row[4]}" if row[3] and row[4] else None
    return row[0], row[1], snap_ts, row[2]


def compute_clv(bet_line, closing_line, market_type, selection, bet_odds=None, closing_odds=None):
    """
    Compute Closing Line Value.

    For SPREAD: CLV = bet_line - closing_line (positive = we got a better number)
      If we bet AWAY +3.5 and it closed at +2.5, we got 1 extra point → CLV = +1.0

    For MONEYLINE: CLV based on implied prob difference
      If we bet +150 (40% implied) and it closed at +130 (43.5% implied),
      market moved toward us → CLV = +3.5%

    For TOTALS: CLV = points of line movement in our favor
    """
    if market_type == 'SPREAD':
        if bet_line is None or closing_line is None:
            return None
        # Positive CLV = we got more points (better number)
        clv = bet_line - closing_line
        return round(clv, 1)

    elif market_type == 'TOTAL':
        if bet_line is None or closing_line is None:
            return None
        if 'OVER' in (selection or ''):
            clv = closing_line - bet_line  # line went up = we got value
        else:
            clv = bet_line - closing_line  # line went down = we got value
        return round(clv, 1)

    elif market_type == 'MONEYLINE':
        # CLV for moneylines: compare implied probabilities
        # If closing implied > bet implied, the line moved toward us = positive CLV
        if bet_odds is None or closing_odds is None:
            return None
        bet_implied = _american_to_implied(bet_odds)
        close_implied = _american_to_implied(closing_odds)
        if bet_implied is None or close_implied is None:
            return None
        # Positive = closing line implies higher probability = we got value
        clv = (close_implied - bet_implied) * 100
        return round(clv, 1)

    elif market_type == 'PROP':
        # Props: same logic as moneyline (odds-based CLV)
        if bet_odds is not None and closing_odds is not None:
            bet_implied = _american_to_implied(bet_odds)
            close_implied = _american_to_implied(closing_odds)
            if bet_implied is not None and close_implied is not None:
                clv = (close_implied - bet_implied) * 100
                return round(clv, 1)
        # Fall back to line-based CLV for props
        if bet_line is not None and closing_line is not None:
            if 'OVER' in (selection or ''):
                clv = closing_line - bet_line
            else:
                clv = bet_line - closing_line
            return round(clv, 1)

    return None


def _american_to_implied(odds):
    """Convert American odds to implied probability (0-1)."""
    if odds is None:
        return None
    if odds > 0:
        return 100.0 / (odds + 100.0)
    elif odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    return None


# ═══════════════════════════════════════════════════════════════════
# METADATA INFERENCE (for old bets without analytics columns)
# ═══════════════════════════════════════════════════════════════════

def _infer_side_type(mtype, sel, line, odds):
    """Infer side type from bet data when metadata isn't available."""
    if mtype == 'TOTAL':
        return 'OVER' if 'OVER' in (sel or '') else 'UNDER'
    elif mtype == 'PROP':
        return 'PROP_OVER' if 'OVER' in (sel or '') else 'PROP_UNDER'
    elif mtype == 'MONEYLINE':
        return 'DOG' if odds and odds > 0 else 'FAVORITE'
    elif mtype == 'SPREAD':
        if line is not None:
            if line > 0: return 'DOG'
            elif line < 0: return 'FAVORITE'
        return 'PK'
    return 'UNKNOWN'


def _infer_market_tier(sport):
    """Infer market tier from sport."""
    soft = {'basketball_ncaab', 'soccer_usa_mls', 'soccer_germany_bundesliga',
            'soccer_france_ligue_one', 'soccer_italy_serie_a', 'soccer_uefa_champs_league',
            'soccer_mexico_ligamx', 'baseball_ncaa'}
    return 'SOFT' if sport in soft else 'SHARP'


# ═══════════════════════════════════════════════════════════════════
# MAIN GRADING ENGINE
# ═══════════════════════════════════════════════════════════════════

def grade_bets(conn, days_back=3):
    """Grade recent bets against actual scores. Compute CLV.
    
    v12 FIX: Deduplicates by SIDE (team/over-under), not by full selection.
    "Nebraska +1.5" and "Nebraska +0.0" are the SAME bet at different snapshots.
    Only grade the FIRST occurrence (earliest created_at) for each side of each game.
    """
    ensure_tables(conn)
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

    # Get ALL ungraded bets first, then dedup in Python (SQL can't easily strip lines)
    # v12 FIX: Also re-grade PENDING bets — they had no score last time but might now
    # v17: Grade 3.5u+ picks (ELITE + STRONG). Sub-3.5u picks are tracked
    # in the bets table but not graded or shown in the record.
    all_bets = conn.execute("""
        SELECT id, sport, event_id, market_type, selection,
               book, line, odds, edge_pct, confidence, units, created_at, context_factors
        FROM bets
        WHERE DATE(created_at) >= ?
        AND units >= 3.5
        AND (result IS NULL OR result NOT IN ('TAINTED', 'DUPLICATE'))
        AND (
            id NOT IN (SELECT bet_id FROM graded_bets WHERE bet_id IS NOT NULL)
            OR id IN (SELECT bet_id FROM graded_bets WHERE result = 'PENDING')
        )
        ORDER BY created_at
    """, (cutoff,)).fetchall()

    # Remove old PENDING grades so they can be re-graded
    if all_bets:
        pending_ids = [b[0] for b in all_bets]
        conn.execute(f"""
            DELETE FROM graded_bets WHERE result = 'PENDING'
            AND bet_id IN ({','.join('?' * len(pending_ids))})
        """, pending_ids)
        conn.commit()

    if not all_bets:
        print("  No ungraded bets found.")
        return []

    # Dedup by TEAM NAMES + SIDE (not event_id)
    # v16 FIX: Odds API assigns different event_ids when lines move.
    # "Creighton@Miami OVER 12.5" and "Creighton@Miami OVER 13.0" are the
    # SAME bet at different snapshots. Only grade the first one.
    # Key on sport + date + side (strips line numbers and event_id).
    import re
    seen_sides = {}  # key: "sport|date|market_type|side" → first bet tuple
    bets = []
    dupe_ids = []
    for b in all_bets:
        bid, sport, eid, mtype, sel = b[0], b[1], b[2], b[3], b[4]
        bet_date = b[11][:10] if b[11] else ''  # created_at date

        if mtype == 'SPREAD':
            side = re.sub(r'\s*[+-]?\d+\.?\d*$', '', sel).strip()
        elif mtype == 'TOTAL':
            # Strip line number: "Creighton@Miami OVER 12.5" → "Creighton@Miami OVER"
            side = re.sub(r'\s+\d+\.?\d*$', '', sel).strip()
        elif mtype == 'MONEYLINE':
            side = sel.replace(' ML', '').replace(' (cross-mkt)', '').strip()
        else:
            side = sel  # Props: player-specific, keep full selection

        key = f"{sport}|{bet_date}|{mtype}|{side}"
        if key in seen_sides:
            dupe_ids.append(bid)
            continue
        seen_sides[key] = True
        bets.append(b)
    
    if dupe_ids:
        print(f"  Skipping {len(dupe_ids)} duplicate bets (same game/side, different line)")
        # Mark dupes as graded so they don't keep showing up
        now = datetime.now().isoformat()
        for did in dupe_ids:
            conn.execute("""
                INSERT OR IGNORE INTO graded_bets (graded_at, bet_id, result, pnl_units)
                VALUES (?, ?, 'DUPLICATE', 0)
            """, (now, did))
        conn.commit()

    graded = []
    for b in bets:
        bid, sport, eid, mtype, sel, book, line, odds, edge, conf, units, created, bet_ctx = b

        # v25.18: Guard — skip if already graded by an earlier iteration's
        # duplicate-marking block. Without this, a bet marked DUPLICATE by
        # iteration N gets graded again as WIN/LOSS by iteration N+K,
        # creating two graded_bets records for the same bet_id.
        if conn.execute("SELECT 1 FROM graded_bets WHERE bet_id=?", (bid,)).fetchone():
            continue

        # Pull analytical metadata from bets table
        meta = conn.execute("""
            SELECT side_type, spread_bucket, edge_bucket, timing,
                   context_factors, context_confirmed, market_tier, model_spread, day_of_week
            FROM bets WHERE id = ?
        """, (bid,)).fetchone()
        
        side_type = meta[0] if meta and meta[0] else _infer_side_type(mtype, sel, line, odds)
        spread_bucket = meta[1] if meta and meta[1] else ''
        edge_bucket = meta[2] if meta and meta[2] else ''
        timing = meta[3] if meta and meta[3] else ''
        context_factors = meta[4] if meta and meta[4] else ''
        context_confirmed = meta[5] if meta and meta[5] is not None else 0
        market_tier = meta[6] if meta and meta[6] else _infer_market_tier(sport)
        model_spread = meta[7] if meta else None
        day_of_week = meta[8] if meta and meta[8] else ''

        # Look up score from results table
        bet_date = created[:10] if created else None
        score = conn.execute("""
            SELECT home_score, away_score, home, away, completed
            FROM results
            WHERE event_id = ? AND completed = 1
            AND sport = ?
            LIMIT 1
        """, (eid, sport)).fetchone()
        
        # v12.2 SAFETY: Verify the matched result date is near the bet date.
        if score and bet_date:
            date_check = conn.execute("""
                SELECT commence_time FROM results
                WHERE event_id = ? AND sport = ? AND completed = 1 LIMIT 1
            """, (eid, sport)).fetchone()
            if date_check and date_check[0]:
                result_date = date_check[0][:10]
                try:
                    if abs((datetime.strptime(bet_date, '%Y-%m-%d') - datetime.strptime(result_date, '%Y-%m-%d')).days) > 2:
                        print(f"  ⚠ Date mismatch: {sel} bet={bet_date} result={result_date} — skipping stale match")
                        score = None
                except Exception:
                    pass

        if not score:
            # v12 FIX: Fallback — match by team name + date when event IDs differ.
            # Baseball results use ESPN IDs, bets use Odds API IDs — they never match.
            # Get teams from odds table, then find matching result by team + date.
            teams_row = conn.execute("""
                SELECT DISTINCT home, away FROM odds
                WHERE event_id=? LIMIT 1
            """, (eid,)).fetchone()
            
            if not teams_row:
                teams_row = conn.execute("""
                    SELECT DISTINCT home, away FROM market_consensus
                    WHERE event_id=? LIMIT 1
                """, (eid,)).fetchone()
            
            if teams_row:
                bet_home, bet_away = teams_row
                
                # v12 FIX: Odds API and ESPN use different team names.
                # Map common mismatches so fallback lookup works.
                TEAM_ALIASES = {
                    # "St" vs "State" pattern
                    'Appalachian St Mountaineers': 'App State Mountaineers',
                    'Florida St Seminoles': 'Florida State Seminoles',
                    'Georgia St Panthers': 'Georgia State Panthers',
                    'Kennesaw St Owls': 'Kennesaw State Owls',
                    'Michigan St Spartans': 'Michigan State Spartans',
                    'Mississippi St Bulldogs': 'Mississippi State Bulldogs',
                    'Oklahoma St Cowboys': 'Oklahoma State Cowboys',
                    'Oregon St Beavers': 'Oregon State Beavers',
                    'San Diego St Aztecs': 'San Diego State Aztecs',
                    'Nicholls St Colonels': 'Nicholls Colonels',
                    'San Jose St Spartans': 'San Jose State Spartans',
                    'Boise St Broncos': 'Boise State Broncos',
                    'Fresno St Bulldogs': 'Fresno State Bulldogs',
                    'Arizona St Sun Devils': 'Arizona State Sun Devils',
                    'Kansas St Wildcats': 'Kansas State Wildcats',
                    'Penn St Nittany Lions': 'Penn State Nittany Lions',
                    'Iowa St Cyclones': 'Iowa State Cyclones',
                    'Ohio St Buckeyes': 'Ohio State Buckeyes',
                    'Washington St Cougars': 'Washington State Cougars',
                    'Wichita St Shockers': 'Wichita State Shockers',
                    # CSU vs Cal State
                    'CSU Fullerton Titans': 'Cal State Fullerton Titans',
                    'CSU Northridge Matadors': 'Cal State Northridge Matadors',
                    'CSU Bakersfield Roadrunners': 'Cal State Bakersfield Roadrunners',
                    # Different mascot/name
                    'Grand Canyon Antelopes': 'Grand Canyon Lopes',
                    'Long Beach State Dirtbags': 'Long Beach State Beach',
                    "Florida Int'l Golden Panthers": 'Florida International Panthers',
                    'UT-Arlington Mavericks': 'UT Arlington Mavericks',
                    # Other known mismatches
                    'UMass Minutemen': 'Massachusetts Minutemen',
                    "Hawaii Rainbow Warriors": "Hawai'i Rainbow Warriors",
                    'SE Missouri St Redhawks': 'Southeast Missouri State Redhawks',
                    'UTRGV Vaqueros': 'UT Rio Grande Valley Vaqueros',
                    'McNeese St Cowboys': 'McNeese Cowboys',
                }
                bet_home = TEAM_ALIASES.get(bet_home, bet_home)
                bet_away = TEAM_ALIASES.get(bet_away, bet_away)
                bet_date = created[:10] if created else None
                if bet_date:
                    # v12 FIX: Match BOTH teams, not just one. Single-team LIMIT 1
                    # was grabbing wrong games (Charlotte Mar 3 win instead of Mar 4 loss).
                    # ALSO: Only match games on or after bet date — prevents matching
                    # yesterday's game when today's hasn't been played yet (Toronto bug).
                    score = conn.execute("""
                        SELECT home_score, away_score, home, away, completed
                        FROM results
                        WHERE sport=? AND completed=1
                        AND ((home=? AND away=?) OR (home=? AND away=?))
                        AND DATE(commence_time) BETWEEN DATE(?) AND DATE(?, '+1 day')
                        ORDER BY commence_time DESC
                        LIMIT 1
                    """, (sport, bet_home, bet_away, bet_away, bet_home,
                          bet_date, bet_date)).fetchone()
                    
                    if not score:
                        # Try with just one team but only on/after bet date
                        score = conn.execute("""
                            SELECT home_score, away_score, home, away, completed
                            FROM results
                            WHERE sport=? AND completed=1
                            AND (home=? OR away=? OR home=? OR away=?)
                            AND DATE(commence_time) BETWEEN DATE(?) AND DATE(?, '+1 day')
                            ORDER BY ABS(JULIANDAY(commence_time) - JULIANDAY(?)) ASC
                            LIMIT 1
                        """, (sport, bet_home, bet_home, bet_away, bet_away,
                              bet_date, bet_date, bet_date + 'T20:00:00Z')).fetchone()
            
            if not score:
                # Last resort: fuzzy match — try just the mascot name
                if teams_row:
                    for team_name in [bet_home, bet_away]:
                        parts = team_name.split()
                        if len(parts) >= 2:
                            mascot = parts[-1]  # "Panthers", "Lopes", etc.
                            school = parts[0]    # "Georgia", "Grand", etc.
                            score = conn.execute("""
                                SELECT home_score, away_score, home, away, completed
                                FROM results
                                WHERE sport=? AND completed=1
                                AND (home LIKE ? OR away LIKE ?)
                                AND (home LIKE ? OR away LIKE ?)
                                AND DATE(commence_time) BETWEEN DATE(?) AND DATE(?, '+1 day')
                                LIMIT 1
                            """, (sport, f'%{school}%', f'%{school}%',
                                  f'%{mascot}%', f'%{mascot}%',
                                  bet_date, bet_date)).fetchone()
                            if score:
                                break
            
            if not score:
                # v22 FIX: Retry with fresh connection — concurrent grade processes
                # can cause INSERT OR REPLACE race where rows are momentarily invisible.
                try:
                    fresh = sqlite3.connect(DB_PATH, timeout=15)
                    score = fresh.execute("""
                        SELECT home_score, away_score, home, away, completed
                        FROM results
                        WHERE event_id = ? AND completed = 1 AND sport = ?
                        LIMIT 1
                    """, (eid, sport)).fetchone()
                    fresh.close()
                    if score:
                        print(f"  ✓ Found score on retry: {sel} ({sport})")
                except Exception:
                    pass

            if not score:
                # v22: Parse team names from selection as last resort
                # Selection format: "Away Team@Home Team OVER/UNDER X.X" or "Team Name +/-X.X"
                import re as _re
                sel_teams = _re.split(r'\s+(?:OVER|UNDER|ML|\+|-)\s*', sel)[0] if sel else ''
                if '@' in sel_teams:
                    parts = sel_teams.split('@')
                    if len(parts) == 2:
                        sel_away, sel_home = parts[0].strip(), parts[1].strip()
                        bet_date = created[:10] if created else None
                        if bet_date:
                            score = conn.execute("""
                                SELECT home_score, away_score, home, away, completed
                                FROM results
                                WHERE sport=? AND completed=1
                                AND ((home=? AND away=?) OR (home=? AND away=?))
                                AND DATE(commence_time) BETWEEN DATE(?) AND DATE(?, '+1 day')
                                ORDER BY commence_time DESC LIMIT 1
                            """, (sport, sel_home, sel_away, sel_away, sel_home,
                                  bet_date, bet_date)).fetchone()
                            if score:
                                print(f"  ✓ Matched by selection name: {sel} ({sport})")

            # v25.82: Tennis ML fallback — selection format "Player Name ML"
            # has no '@' separator, so the home/away split above never runs.
            # Match by player name against results.home OR results.away.
            if not score and sport and 'tennis' in sport and mtype == 'MONEYLINE':
                player_name = re.sub(r'\s+ML\s*$', '', sel).strip() if sel else ''
                # Also strip " (cross-mkt)" annotation if present
                player_name = player_name.replace(' (cross-mkt)', '').strip()
                if player_name and created:
                    bet_date = created[:10]
                    score = conn.execute("""
                        SELECT home_score, away_score, home, away, completed
                        FROM results
                        WHERE sport=? AND completed=1
                          AND (home=? OR away=?)
                          AND DATE(commence_time) BETWEEN DATE(?) AND DATE(?, '+1 day')
                        ORDER BY commence_time DESC LIMIT 1
                    """, (sport, player_name, player_name,
                          bet_date, bet_date)).fetchone()
                    if score:
                        print(f"  ✓ Matched tennis ML by player name: {player_name}")

            if not score:
                print(f"  ⚠ No score found: {sel} ({sport}) — event ID mismatch, team lookup failed")
                continue

        h_score, a_score, home, away, _ = score

        # v24 FIX: Tennis spread/total lines are in GAMES, not sets.
        # The results table stores set scores (2-0, 2-1) but lines like +4.5
        # mean games. Use tennis_metadata.set_scores to get actual game counts.
        grade_h, grade_a = h_score, a_score
        if sport and 'tennis' in sport and mtype in ('SPREAD', 'TOTAL'):
            try:
                # Find the ESPN event_id from results that matched this bet
                espn_eid = conn.execute("""
                    SELECT event_id FROM results
                    WHERE sport=? AND completed=1
                    AND ((home=? AND away=?) OR (home=? AND away=?))
                    AND DATE(commence_time) BETWEEN DATE(?) AND DATE(?, '+1 day')
                    ORDER BY commence_time DESC LIMIT 1
                """, (sport, home, away, away, home,
                      created[:10], created[:10])).fetchone()
                if not espn_eid:
                    espn_eid = conn.execute("""
                        SELECT event_id FROM results
                        WHERE event_id=? AND sport=? AND completed=1 LIMIT 1
                    """, (eid, sport)).fetchone()
                if espn_eid:
                    tm = conn.execute("""
                        SELECT set_scores FROM tennis_metadata WHERE event_id=?
                    """, (espn_eid[0],)).fetchone()
                    if tm and tm[0]:
                        import json as _json
                        sets = _json.loads(tm[0])
                        # set_scores format: [[h_games, a_games], ...] per set
                        # player1 = home in results table
                        grade_h = sum(s[0] for s in sets)
                        grade_a = sum(s[1] for s in sets)
                        print(f"  ✓ Tennis games score: {home} {grade_h} - {grade_a} {away} (from set_scores)")
            except Exception as e:
                print(f"  ⚠ Tennis game-score lookup failed: {e}, using set scores")

        # Determine W/L/P
        # v12.1: Props use box score player stats, not team scores
        if mtype == 'PROP':
            try:
                from box_scores import grade_prop
                # v25.18: Use game commence_time for prop date, not bet creation date.
                # Bets placed 1+ days before tip-off would look up the wrong day's
                # box scores, leaving props stuck PENDING indefinitely.
                # v25.19: Convert commence_time UTC→ET before extracting date.
                # box_scores.game_date is local ET; late NHL/NBA games roll to next
                # UTC day, so raw [:10] slice mismatched and left props PENDING.
                _ct_row = conn.execute(
                    "SELECT commence_time FROM results WHERE event_id=? AND sport=? LIMIT 1",
                    (eid, sport)
                ).fetchone() or conn.execute(
                    "SELECT commence_time FROM odds WHERE event_id=? LIMIT 1", (eid,)
                ).fetchone()
                prop_date = None
                if _ct_row and _ct_row[0]:
                    try:
                        _ts = _ct_row[0].replace('Z', '+00:00')
                        prop_date = datetime.fromisoformat(_ts).astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')
                    except Exception:
                        prop_date = _ct_row[0][:10]
                elif created:
                    prop_date = created[:10]
                result = grade_prop(conn, sel, line, prop_date, sport=sport)
            except ImportError:
                result = 'PENDING'  # box_scores.py not installed yet
            except Exception as e:
                print(f"  ⚠ Prop grading error: {e}")
                result = 'PENDING'
        else:
            result = determine_result(sel, mtype, line, grade_h, grade_a, home, away, sport=sport)

        # v25.34: SCRUB safety net. If the bet's context_factors contains a
        # 'SCRUB:' tag (added by `main.py scrub`), force TAINTED regardless of
        # what the game result would grade to. Keeps the record honest — a
        # pick we flagged as "no reliable basis to bet" shouldn't count as a
        # win just because the outcome happened to land. To remove the tag
        # and re-grade normally, run `main.py unscrub <bet_id>`.
        if bet_ctx and 'SCRUB:' in bet_ctx and result in ('WIN', 'LOSS', 'PUSH'):
            print(f"  🚫 SCRUB override: bet {bid} would be {result} but SCRUB tag forces TAINTED — {sel[:60]}")
            result = 'TAINTED'
        pnl = calculate_pnl(result, odds, units)

        # Compute CLV
        market_key = _market_key(mtype, sel)
        closing_line_val, closing_odds, closing_snap_ts, closing_book = get_closing_line(conn, eid, market_key, sel, bet_book=book)
        clv = compute_clv(line, closing_line_val, mtype, sel, bet_odds=odds, closing_odds=closing_odds)

        # Build display name — include teams for totals
        display_sel = sel
        if mtype == 'TOTAL' and home and away:
            short_home = home.split()[-1] if home else ''
            short_away = away.split()[-1] if away else ''
            display_sel = f"{short_away}@{short_home} {sel}"

        # Save grading (mark ALL duplicate bet_ids as graded to prevent re-grading)
        now = datetime.now().isoformat()
        conn.execute("""
            INSERT INTO graded_bets (graded_at, bet_id, sport, event_id, selection,
                market_type, book, line, odds, edge_pct, confidence, units,
                result, pnl_units, closing_line, clv, created_at,
                side_type, spread_bucket, edge_bucket, timing,
                context_factors, context_confirmed, market_tier, model_spread, day_of_week)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (now, bid, sport, eid, sel, mtype, book, line, odds, edge, conf,
              units, result, pnl, closing_line_val, clv, created,
              side_type, spread_bucket, edge_bucket, timing,
              context_factors, context_confirmed, market_tier, model_spread, day_of_week))

        # v17: Backfill result/profit/clv into source bets table
        # Previously only graded_bets got updated — bets table was 97% empty
        conn.execute("""
            UPDATE bets SET result=?, profit=?, closing_line=?, clv=?
            WHERE id=?
        """, (result, pnl, closing_line_val, clv, bid))

        # Also mark any duplicate copies as graded
        dupes = conn.execute("""
            SELECT id FROM bets
            WHERE event_id=? AND selection=? AND market_type=? AND id != ?
            AND id NOT IN (SELECT bet_id FROM graded_bets WHERE bet_id IS NOT NULL)
        """, (eid, sel, mtype, bid)).fetchall()
        for (dupe_id,) in dupes:
            conn.execute("""
                INSERT INTO graded_bets (graded_at, bet_id, sport, event_id, selection,
                    market_type, book, line, odds, edge_pct, confidence, units,
                    result, pnl_units, closing_line, clv, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now, dupe_id, sport, eid, sel, mtype, book, line, odds, edge, conf,
                  0, 'DUPLICATE', 0, None, None, created))

        graded.append({
            'selection': display_sel, 'market_type': mtype, 'result': result,
            'pnl': pnl, 'edge': edge, 'confidence': conf, 'units': units,
            'sport': sport, 'odds': odds, 'clv': clv,
            'closing_line': closing_line_val, 'bet_line': line,
            'side_type': side_type, 'spread_bucket': spread_bucket,
            'context_confirmed': context_confirmed, 'market_tier': market_tier,
            'timing': timing, 'context_factors': context_factors,
            'model_spread': model_spread, 'event_id': eid,
        })

    conn.commit()
    return graded


def _market_key(market_type, selection=''):
    """Map our market_type to the odds table market key.

    For props, we need to determine the specific market from the selection text.
    Selections look like: "LeBron James OVER 25.5 POINTS"
    """
    if market_type == 'PROP':
        sel_upper = (selection or '').upper()
        # Match against all prop labels (same mapping as props_engine.PROP_LABEL)
        PROP_MARKET_MAP = {
            'POINTS': 'player_points', 'REBOUNDS': 'player_rebounds',
            'ASSISTS': 'player_assists', 'THREES': 'player_threes',
            'BLOCKS': 'player_blocks', 'STEALS': 'player_steals',
            'SOG': 'player_shots_on_goal', 'PPP': 'player_power_play_points',
            'BLK_SHOTS': 'player_blocked_shots',
            'SHOTS': 'player_shots', 'SOT': 'player_shots_on_target',
            'HITS': 'batter_hits', 'TOTAL_BASES': 'batter_total_bases',
            'HOME_RUNS': 'batter_home_runs', 'RBIS': 'batter_rbis',
            'RUNS': 'batter_runs_scored', 'STRIKEOUTS': 'pitcher_strikeouts',
            'OUTS': 'pitcher_outs', 'HITS ALLOWED': 'pitcher_hits_allowed',
            'EARNED RUNS': 'pitcher_earned_runs', 'WALKS': 'pitcher_bb',
            'STOLEN_BASES': 'batter_stolen_bases',
        }
        # Sort by key length descending so 'HITS ALLOWED' matches before 'HITS',
        # 'EARNED RUNS' before 'RUNS', etc.
        for label, market in sorted(PROP_MARKET_MAP.items(), key=lambda x: -len(x[0])):
            if label in sel_upper:
                return market
        return 'player_points'  # Default fallback
    
    return {
        'SPREAD': 'spreads',
        'MONEYLINE': 'h2h',
        'TOTAL': 'totals',
    }.get(market_type, 'h2h')


# ═══════════════════════════════════════════════════════════════════
# PLAYER RESULTS ACCUMULATION (feeds the props historical signal)
# ═══════════════════════════════════════════════════════════════════

def accumulate_player_results(conn, days_back=3):
    """
    After games finish, record player prop results from our graded bets.
    
    Without box score data, we can't get exact stat values. But from graded bets:
      - WIN on "LeBron James OVER 25.5 POINTS" → actual > 25.5
      - LOSS on "LeBron James OVER 25.5 POINTS" → actual ≤ 25.5
      - WIN on "LeBron James UNDER 25.5 POINTS" → actual < 25.5
      - LOSS on "LeBron James UNDER 25.5 POINTS" → actual ≥ 25.5
    
    This feeds the historical signal in props_engine (over_rate tracking).
    Data accumulates over time — more games = stronger signal.
    """
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    # Get graded prop bets
    graded_props = conn.execute("""
        SELECT gb.sport, gb.event_id, gb.selection, gb.line, gb.result, gb.created_at
        FROM graded_bets gb
        WHERE gb.market_type = 'PROP'
        AND gb.result IN ('WIN', 'LOSS')
        AND DATE(gb.created_at) >= ?
        AND gb.result NOT IN ('DUPLICATE', 'TAINTED')
    """, (cutoff,)).fetchall()
    
    if not graded_props:
        count = conn.execute("SELECT COUNT(*) FROM player_results").fetchone()[0]
        print(f"  Player results in DB: {count} (no new prop results to add)")
        return count
    
    # Map selection keywords back to stat types
    STAT_MAP = {
        'POINTS': 'pts', 'PTS': 'pts',
        'REBOUNDS': 'reb', 'REB': 'reb',
        'ASSISTS': 'ast', 'AST': 'ast',
        'THREES': 'threes', '3PT': 'threes', 'THREE': 'threes',
    }
    
    added = 0
    for sport, event_id, selection, line, result, created_at in graded_props:
        if not selection or line is None:
            continue
        
        # Parse: "LeBron James OVER 25.5 POINTS"
        parts = selection.split()
        player = None
        side = None
        stat_type = None
        
        for i, part in enumerate(parts):
            if part in ('OVER', 'UNDER'):
                player = ' '.join(parts[:i])
                side = part
                # Look for stat type in remaining parts
                for remaining in parts[i+1:]:
                    remaining_upper = remaining.upper()
                    if remaining_upper in STAT_MAP:
                        stat_type = STAT_MAP[remaining_upper]
                        break
                break
        
        if not player or not side or not stat_type:
            continue
        
        # Determine result for player_results table
        # OVER/UNDER result (not bet result — they differ based on which side we bet)
        if side == 'OVER':
            prop_result = 'OVER' if result == 'WIN' else 'UNDER'
        else:  # side == 'UNDER'
            prop_result = 'UNDER' if result == 'WIN' else 'OVER'
        
        # v12.1: Try to get REAL value from ESPN box scores
        estimated_actual = None
        try:
            from box_scores import lookup_player_stat, PROP_TO_STAT
            stat_type_key = PROP_TO_STAT.get(stat_type.upper(), stat_type)
            real_val = lookup_player_stat(conn, player, stat_type_key, game_date, sport=sport)
            if real_val is not None:
                estimated_actual = real_val
        except ImportError:
            pass
        except Exception:
            pass
        
        # Fallback: estimate from win/loss if no box score
        if estimated_actual is None:
            if prop_result == 'OVER':
                estimated_actual = line + 1.0
            else:
                estimated_actual = line - 1.0
        
        game_date = created_at[:10] if created_at else datetime.now().strftime('%Y-%m-%d')
        
        try:
            conn.execute("""
                INSERT OR IGNORE INTO player_results 
                (game_date, sport, event_id, player, stat_type, actual_value, prop_line, result)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (game_date, sport, event_id, player, stat_type, estimated_actual, line, prop_result))
            added += 1
        except Exception:
            pass  # UNIQUE constraint = already recorded
    
    if added > 0:
        conn.commit()
    
    count = conn.execute("SELECT COUNT(*) FROM player_results").fetchone()[0]
    print(f"  Player results in DB: {count} (+{added} new from graded props)")
    return count


# ═══════════════════════════════════════════════════════════════════
# RESULT DETERMINATION
# ═══════════════════════════════════════════════════════════════════

def _team_in_selection(team, selection):
    """Check if a team name matches the selection, handling ESPN vs Odds API name differences.
    
    Examples:
      'App State Mountaineers' in 'Appalachian St Mountaineers ML' → True
      'Carolina Hurricanes' in 'Carolina Hurricanes -1.5' → True
      'South Carolina Gamecocks' in 'Charleston Southern Buccaneers ML' → False
    """
    if not team or not selection:
        return False
    # Exact substring match
    if team in selection:
        return True
    
    team_parts = team.split()
    if len(team_parts) < 2:
        return False
    
    mascot = team_parts[-1]
    # School name = everything except mascot
    school = ' '.join(team_parts[:-1])
    
    # Strategy: match school AND mascot both present as whole words
    # This prevents "South Carolina" matching "Charleston Southern"
    import re
    
    def _word_in(word, text):
        """Check if word appears as a whole word in text."""
        return bool(re.search(r'\b' + re.escape(word) + r'\b', text))
    
    # Check if mascot is present (whole word)
    if _word_in(mascot, selection):
        return True
    
    # Handle "St" vs "State" abbreviation — exact school name with swap
    if ' State' in school:
        school_short = school.replace(' State', ' St')
        if school_short in selection:
            return True
    if ' St' in school:
        school_long = school.replace(' St', ' State')
        if school_long in selection:
            return True
    
    # Handle abbreviation patterns specifically
    # "Appalachian St" vs "App State" — already handled by mascot + St/State swap
    # "Florida Int'l" vs "Florida International" — check without punctuation
    import string
    clean_school = school.translate(str.maketrans('', '', string.punctuation))
    clean_sel = selection.translate(str.maketrans('', '', string.punctuation))
    if len(clean_school) >= 8 and clean_school in clean_sel:
        return True
    
    return False


def determine_result(selection, market_type, line, h_score, a_score, home, away, sport=None):
    """Determine if a bet won, lost, or pushed."""
    if h_score is None or a_score is None:
        return 'PENDING'

    margin = h_score - a_score

    if market_type == 'MONEYLINE':
        is_soccer = sport and 'soccer' in sport
        draw_result = 'LOSS' if is_soccer else 'PUSH'
        if _team_in_selection(home, selection):
            return 'WIN' if h_score > a_score else (draw_result if h_score == a_score else 'LOSS')
        elif _team_in_selection(away, selection):
            return 'WIN' if a_score > h_score else (draw_result if h_score == a_score else 'LOSS')
        return 'PENDING'

    elif market_type == 'SPREAD':
        if line is None:
            return 'PENDING'
        if _team_in_selection(home, selection):
            adjusted = margin + line
        elif _team_in_selection(away, selection):
            adjusted = -margin + line
        else:
            return 'PENDING'
        if adjusted > 0: return 'WIN'
        elif adjusted < 0: return 'LOSS'
        return 'PUSH'

    elif market_type == 'TOTAL':
        if line is None:
            return 'PENDING'
        actual_total = h_score + a_score
        if 'OVER' in selection:
            if actual_total > line: return 'WIN'
            elif actual_total < line: return 'LOSS'
            return 'PUSH'
        elif 'UNDER' in selection:
            if actual_total < line: return 'WIN'
            elif actual_total > line: return 'LOSS'
            return 'PUSH'

    return 'PENDING'


def calculate_pnl(result, odds, units):
    """Calculate profit/loss in units."""
    if result == 'WIN':
        if odds > 0:
            return round(units * (odds / 100.0), 2)
        else:
            return round(units * (100.0 / abs(odds)), 2)
    elif result == 'LOSS':
        return -units
    return 0.0


# ═══════════════════════════════════════════════════════════════════
# REPORTS
# ═══════════════════════════════════════════════════════════════════

def performance_report(conn=None, days=7, sport=None, start_date=None):
    """
    Professional performance report with multi-dimensional analytics.
    
    Tracks performance across every meaningful dimension:
      - Overall + CLV (the Walters metric)
      - By sport
      - By side (favorites vs dogs vs overs vs unders vs props)
      - By spread bucket (small/med/big dogs and favs)
      - By conviction tier (MAX PLAY / STRONG / SOLID)
      - By market tier (soft vs sharp)
      - By context (context-confirmed vs raw model)
      - By edge bucket (projected edge size)
      - By timing (early vs late)
      - By day of week
      - Streak tracking
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close_conn = True

    cutoff = start_date if start_date else (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    query = """
        SELECT sport, selection, market_type, result, pnl_units, edge_pct,
               confidence, units, odds, created_at, closing_line, clv,
               side_type, spread_bucket, edge_bucket, timing,
               context_factors, context_confirmed, market_tier, model_spread, day_of_week,
               line
        FROM graded_bets WHERE DATE(created_at) >= ?
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND units >= 3.5
    """
    params = [cutoff]
    if sport:
        query += " AND sport = ?"
        params.append(sport)
    query += " ORDER BY created_at"

    bets = conn.execute(query, params).fetchall()

    if not bets:
        report = f"  No graded bets in last {days} days."
        print(report)
        if close_conn: conn.close()
        return report

    # ── Parse into dicts for easier manipulation ──
    records = []
    for b in bets:
        r = {
            'sport': b[0], 'selection': b[1], 'market_type': b[2],
            'result': b[3], 'pnl': b[4] or 0, 'edge': b[5] or 0,
            'confidence': b[6] or 'UNKNOWN', 'units': b[7] or 0,
            'odds': b[8], 'created_at': b[9], 'closing_line': b[10],
            'clv': b[11],
            'side_type': b[12] or _infer_side_type(b[2], b[1], b[21], b[8]),
            'spread_bucket': b[13] or '',
            'edge_bucket': b[14] or '',
            'timing': b[15] or '',
            'context_factors': b[16] or '',
            'context_confirmed': b[17] if b[17] is not None else 0,
            'market_tier': b[18] or _infer_market_tier(b[0]),
            'model_spread': b[19],
            'day_of_week': b[20] or '',
            'line': b[21],
        }
        records.append(r)

    lines = []
    
    # ═══════════════════════════════════════════════════════════════
    # HEADER
    # ═══════════════════════════════════════════════════════════════
    lines.append(f"{'='*70}")
    lines.append(f"  SCOTTY'S EDGE — PERFORMANCE ANALYTICS")
    period_str = f"Since {start_date}" if start_date else f"Last {days} days"
    lines.append(f"  Period: {period_str} | Generated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    lines.append(f"{'='*70}")

    # ═══════════════════════════════════════════════════════════════
    # 1. OVERALL P&L
    # ═══════════════════════════════════════════════════════════════
    wins = sum(1 for r in records if r['result'] == 'WIN')
    losses = sum(1 for r in records if r['result'] == 'LOSS')
    pushes = sum(1 for r in records if r['result'] == 'PUSH')
    total_pnl = sum(r['pnl'] for r in records)
    total_wagered = sum(r['units'] for r in records)
    roi = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0
    wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0

    lines.append(f"\n  ┌─ OVERALL ─────────────────────────────────────────────┐")
    lines.append(f"  │  Record: {wins}W-{losses}L-{pushes}P ({wp:.1f}%)                    ")
    lines.append(f"  │  P/L: {total_pnl:+.2f}u | Wagered: {total_wagered:.1f}u | ROI: {roi:+.1f}%")
    lines.append(f"  └────────────────────────────────────────────────────────┘")

    # ═══════════════════════════════════════════════════════════════
    # v12 FIX: Split CLV by market type. Spread/Total CLV = POINTS moved.
    # ML/Prop CLV = IMPLIED PROBABILITY % shift. Mixing them is meaningless.
    
    spread_clv = [r['clv'] for r in records if r['clv'] is not None and r.get('market_type') == 'SPREAD']
    total_clv = [r['clv'] for r in records if r['clv'] is not None and r.get('market_type') == 'TOTAL']
    ml_clv = [r['clv'] for r in records if r['clv'] is not None and r.get('market_type') == 'MONEYLINE']
    prop_clv = [r['clv'] for r in records if r['clv'] is not None and r.get('market_type') == 'PROP']

    # CLV: Only show spreads (the meaningful, comparable metric).
    # Totals CLV is noisy (small sample), ML CLV uses implied probability
    # which swings 30%+ on tournament games — not meaningful.
    # Keep it simple: one number that matters.
    if spread_clv:
        avg_s = sum(spread_clv) / len(spread_clv)
        pos_s = sum(1 for c in spread_clv if c > 0)
        rate_s = pos_s / len(spread_clv) * 100
        status = 'BEATING CLOSING LINE' if avg_s > 0 else ('NEAR ZERO' if avg_s > -0.5 else 'BEHIND CLOSING LINE')
        lines.append(f"\n  -- CLV (Closing Line Value) --")
        lines.append(f"  Spreads: {avg_s:+.1f} pts avg | +CLV: {pos_s}/{len(spread_clv)} ({rate_s:.0f}%) | {status}")
    else:
        lines.append(f"\n  -- CLV: No spread data yet --")

    # 3. BY SIDE TYPE — Dogs vs Favorites vs Totals vs Props
    # ═══════════════════════════════════════════════════════════════
    lines.append(f"\n  ── BY SIDE TYPE ──")
    _breakdown(lines, records, 'side_type', {
        'DOG': '🐕 Dogs', 'FAVORITE': '⭐ Favorites', 'PK': '🤝 Pick\'em',
        'OVER': '📈 Overs', 'UNDER': '📉 Unders',
        'PROP_OVER': '🎯 Prop Over', 'PROP_UNDER': '🎯 Prop Under',
    })

    # ═══════════════════════════════════════════════════════════════
    # 4. BY SPREAD BUCKET — Where is the edge coming from?
    # ═══════════════════════════════════════════════════════════════
    spread_records = [r for r in records if r['spread_bucket'] and r['spread_bucket'] != 'N/A']
    if spread_records:
        lines.append(f"\n  ── BY SPREAD BUCKET ──")
        _breakdown(lines, spread_records, 'spread_bucket', {
            'SMALL_DOG': 'Small Dog (1-3.5)', 'MED_DOG': 'Med Dog (4-7.5)', 'BIG_DOG': 'Big Dog (8+)',
            'SMALL_FAV': 'Small Fav (1-3.5)', 'MED_FAV': 'Med Fav (4-7.5)', 'BIG_FAV': 'Big Fav (8+)',
            'PK': 'Pick\'em',
        })

    # ═══════════════════════════════════════════════════════════════
    # 5. BY SPORT
    # ═══════════════════════════════════════════════════════════════
    lines.append(f"\n  ── BY SPORT ──")
    _breakdown(lines, records, 'sport', {
        'basketball_ncaab': 'NCAAB', 'basketball_nba': 'NBA', 'icehockey_nhl': 'NHL',
        'baseball_ncaa': 'NCAA Baseball', 'baseball_mlb': 'MLB',
        'soccer_epl': 'EPL', 'soccer_italy_serie_a': 'Serie A', 'soccer_spain_la_liga': 'La Liga',
        'soccer_germany_bundesliga': 'Bundesliga', 'soccer_france_ligue_one': 'Ligue 1',
        'soccer_uefa_champs_league': 'UCL', 'soccer_usa_mls': 'MLS',
        'soccer_mexico_ligamx': 'Liga MX',
    })

    # ═══════════════════════════════════════════════════════════════
    # 6. BY CONVICTION TIER
    # ═══════════════════════════════════════════════════════════════
    lines.append(f"\n  ── BY CONVICTION ──")
    _breakdown(lines, records, 'confidence', {
        'ELITE': '🔥 MAX PLAY', 'HIGH': '⭐ STRONG', 'STRONG': '✅ SOLID',
        'MEDIUM': '📊 LEAN',
    })

    # ═══════════════════════════════════════════════════════════════
    # 7. BY MARKET TIER — Soft vs Sharp
    # ═══════════════════════════════════════════════════════════════
    lines.append(f"\n  ── BY MARKET TIER ──")
    _breakdown(lines, records, 'market_tier', {
        'SOFT': '🟢 Soft Markets (NCAAB, Serie A, etc.)',
        'SHARP': '🔴 Sharp Markets (NBA, EPL, etc.)',
    })

    # ═══════════════════════════════════════════════════════════════
    # 8. CONTEXT CONFIRMED vs RAW MODEL
    # ═══════════════════════════════════════════════════════════════
    ctx_records = [r for r in records if r['context_confirmed'] is not None]
    if ctx_records:
        lines.append(f"\n  ── CONTEXT CONFIRMED vs RAW MODEL ──")
        ctx_yes = [r for r in ctx_records if r['context_confirmed'] == 1]
        ctx_no = [r for r in ctx_records if r['context_confirmed'] == 0]
        if ctx_yes:
            _print_group(lines, '📍 Context-Confirmed', ctx_yes)
        if ctx_no:
            _print_group(lines, '   Raw Model (no context)', ctx_no)

    # ═══════════════════════════════════════════════════════════════
    # 9. BY EDGE BUCKET — Do bigger projected edges perform better?
    # ═══════════════════════════════════════════════════════════════
    edge_records = [r for r in records if r['edge_bucket']]
    if edge_records:
        lines.append(f"\n  ── BY PROJECTED EDGE SIZE ──")
        _breakdown(lines, edge_records, 'edge_bucket', {
            'EDGE_8_12': 'Edge 8-12%', 'EDGE_12_16': 'Edge 12-16%',
            'EDGE_16_20': 'Edge 16-20%', 'EDGE_20_PLUS': 'Edge 20%+',
        })

    # ═══════════════════════════════════════════════════════════════
    # 10. BY TIMING — Early vs Late
    # ═══════════════════════════════════════════════════════════════
    timing_records = [r for r in records if r['timing'] in ('EARLY', 'LATE')]
    if timing_records:
        lines.append(f"\n  ── BY TIMING ──")
        _breakdown(lines, timing_records, 'timing', {
            'EARLY': '⏰ Early Bets', 'LATE': '⏳ Late Bets',
        })

    # ═══════════════════════════════════════════════════════════════
    # 11. CONTEXT FACTOR LEADERBOARD — Which factors drive winners?
    # ═══════════════════════════════════════════════════════════════
    factor_stats = _context_factor_breakdown(records)
    if factor_stats:
        lines.append(f"\n  ── CONTEXT FACTOR PERFORMANCE ──")
        for factor, stats in sorted(factor_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
            w, l = stats['W'], stats['L']
            t = w + l
            if t < 2: continue
            wp_f = w / t * 100 if t else 0
            lines.append(f"    {factor:30s} {w}W-{l}L ({wp_f:.0f}%) | {stats['pnl']:+.1f}u")

    # ═══════════════════════════════════════════════════════════════
    # 12. STREAKS & TRENDS
    # ═══════════════════════════════════════════════════════════════
    lines.append(f"\n  ── STREAKS & TRENDS ──")
    current_streak, streak_type = _get_current_streak(records)
    lines.append(f"  Current streak: {current_streak} {streak_type}")
    
    # Last 10 results
    last_10 = records[-10:]
    l10_w = sum(1 for r in last_10 if r['result'] == 'WIN')
    l10_l = sum(1 for r in last_10 if r['result'] == 'LOSS')
    l10_pnl = sum(r['pnl'] for r in last_10)
    lines.append(f"  Last 10: {l10_w}W-{l10_l}L | {l10_pnl:+.1f}u")
    
    # Best and worst day
    days_data = {}
    for r in records:
        d = r['created_at'][:10] if r['created_at'] else ''
        if d:
            if d not in days_data:
                days_data[d] = 0
            days_data[d] += r['pnl']
    if days_data:
        best_day = max(days_data, key=days_data.get)
        worst_day = min(days_data, key=days_data.get)
        lines.append(f"  Best day:  {best_day} ({days_data[best_day]:+.1f}u)")
        lines.append(f"  Worst day: {worst_day} ({days_data[worst_day]:+.1f}u)")

    # ═══════════════════════════════════════════════════════════════
    # 13. RECENT PICKS — All picks from the most recent day
    # ═══════════════════════════════════════════════════════════════
    # Find the most recent betting day
    if records:
        latest_date = max(r['created_at'][:10] for r in records if r.get('created_at'))
        todays_picks = [r for r in records if r.get('created_at', '')[:10] == latest_date]
        lines.append(f"\n  ── PICKS FROM {latest_date} ({len(todays_picks)} plays) ──")
        for r in todays_picks:
            emoji = '✅' if r['result'] == 'WIN' else ('❌' if r['result'] == 'LOSS' else '➖')
            side_icon = '🐕' if r['side_type'] == 'DOG' else ('⭐' if r['side_type'] == 'FAVORITE' else '📊')
            ctx_icon = '📍' if r['context_confirmed'] else '  '
            clv_str = f"CLV={r['clv']:+.1f}" if r['clv'] is not None else ""
            sel_short = r['selection'][:40]
            lines.append(f"    {emoji}{side_icon}{ctx_icon} {sel_short:42s} {r['pnl']:+.1f}u {clv_str}")

    lines.append(f"\n{'='*70}")
    report = '\n'.join(lines)
    print(report)

    if close_conn: conn.close()
    return report


# ═══════════════════════════════════════════════════════════════════
# REPORT HELPERS
# ═══════════════════════════════════════════════════════════════════

def _breakdown(lines, records, key, labels):
    """Print a W-L breakdown grouped by a key field."""
    groups = {}
    for r in records:
        val = r.get(key, 'UNKNOWN')
        if val not in groups:
            groups[val] = {'W': 0, 'L': 0, 'P': 0, 'pnl': 0, 'wager': 0, 'clv': []}
        if r['result'] == 'WIN': groups[val]['W'] += 1
        elif r['result'] == 'LOSS': groups[val]['L'] += 1
        else: groups[val]['P'] += 1
        groups[val]['pnl'] += r['pnl']
        groups[val]['wager'] += r['units']
        if r['clv'] is not None:
            groups[val]['clv'].append(r['clv'])

    for key_val, label in labels.items():
        if key_val not in groups:
            continue
        d = groups[key_val]
        t = d['W'] + d['L']
        if t == 0: continue
        wp_v = d['W'] / t * 100
        roi_v = d['pnl'] / d['wager'] * 100 if d['wager'] > 0 else 0
        clv_str = f"CLV={sum(d['clv'])/len(d['clv']):+.1f}" if d['clv'] else ""
        lines.append(f"    {label:35s} {d['W']:2d}W-{d['L']:2d}L ({wp_v:4.0f}%) | {d['pnl']:+6.1f}u | ROI {roi_v:+5.1f}% {clv_str}")


def _print_group(lines, label, records):
    """Print stats for a group of records."""
    w = sum(1 for r in records if r['result'] == 'WIN')
    l = sum(1 for r in records if r['result'] == 'LOSS')
    t = w + l
    pnl = sum(r['pnl'] for r in records)
    wager = sum(r['units'] for r in records)
    roi = pnl / wager * 100 if wager > 0 else 0
    clv_vals = [r['clv'] for r in records if r['clv'] is not None]
    clv_str = f"CLV={sum(clv_vals)/len(clv_vals):+.1f}" if clv_vals else ""
    wp = w / t * 100 if t else 0
    lines.append(f"    {label:35s} {w:2d}W-{l:2d}L ({wp:4.0f}%) | {pnl:+6.1f}u | ROI {roi:+5.1f}% {clv_str}")


def _context_factor_breakdown(records):
    """Parse context_factors strings and track which factors correlate with wins."""
    factor_stats = {}
    for r in records:
        if not r.get('context_factors'):
            continue
        # Parse pipe-separated factors: "Away 3-in-5 (+0.5) | Altitude (+0.5)"
        factors = [f.strip() for f in r['context_factors'].split('|')]
        for f in factors:
            if not f: continue
            # Normalize: strip the adjustment value for grouping
            # "Away 3-in-5 (+0.5)" → "Rest/Schedule"
            # "Cross-country trip (+0.5)" → "Travel"
            category = _categorize_factor(f)
            if category not in factor_stats:
                factor_stats[category] = {'W': 0, 'L': 0, 'pnl': 0}
            if r['result'] == 'WIN':
                factor_stats[category]['W'] += 1
            elif r['result'] == 'LOSS':
                factor_stats[category]['L'] += 1
            factor_stats[category]['pnl'] += r['pnl']
    return factor_stats


def _categorize_factor(factor_str):
    """Map a context factor description to a category."""
    f = factor_str.lower()
    if any(x in f for x in ['b2b', '3-in-5', 'rest', 'extra rest']):
        return 'Rest / Schedule'
    if any(x in f for x in ['sharp', 'public']):
        return 'Line Movement'
    if any(x in f for x in ['home team', 'road team', 'weak road', 'strong home']):
        return 'Home/Away Splits'
    if any(x in f for x in ['cross-country', 'west coast', 'early start']):
        return 'Travel / Timezone'
    if 'altitude' in f:
        return 'Altitude'
    if 'letdown' in f:
        return 'Motivation / Letdown'
    if any(x in f for x in ['fast-paced', 'slow-paced', 'pace']):
        return 'Pace of Play'
    if 'h2h' in f:
        return 'Head-to-Head History'
    if 'ref' in f:
        return 'Referee Tendencies'
    if 'familiar' in f or 'division' in f:
        return 'Division Familiarity'
    return factor_str[:30]


def _get_current_streak(records):
    """Calculate current W/L streak."""
    if not records:
        return 0, ''
    streak = 0
    streak_type = records[-1]['result']
    for r in reversed(records):
        if r['result'] == streak_type and r['result'] in ('WIN', 'LOSS'):
            streak += 1
        else:
            break
    return streak, streak_type


# ═══════════════════════════════════════════════════════════════════
# POST-GRADING LOSS ANALYSIS
# ═══════════════════════════════════════════════════════════════════

def analyze_losses(conn, graded_bets):
    """
    Analyze losses from the current grading batch and return a formatted report.

    Categories:
      BAD LUCK     — CLV >= 0 and loss margin <= 3 points
      MODEL GAP    — model spread was 3+ points off the closing line
      CONTEXT TRAP — a context factor with < 40% win rate fired
      SHARP FADE   — CLV <= -1.5 (market moved hard against us)

    Also identifies toxic context factors (3+ occurrences, < 45% win rate).
    """
    import re

    losses = [g for g in graded_bets if g['result'] == 'LOSS']
    if not losses:
        return ""

    # ── Build historical context factor W/L from ALL graded bets in DB ──
    all_graded = conn.execute("""
        SELECT context_factors, result FROM graded_bets
        WHERE result IN ('WIN', 'LOSS')
        AND context_factors IS NOT NULL AND context_factors != ''
    """).fetchall()

    factor_records = {}  # factor_tag -> {'wins': int, 'losses': int}
    factor_exact = {}   # exact tag (with adjustment) for granular toxic detection
    for ctx_str, result in all_graded:
        # Context factors are stored as pipe-separated summaries like:
        #   "Home on B2B (+1.5) | Sharp money agrees (+2.0)"
        for tag in ctx_str.split('|'):
            tag = tag.strip()
            if not tag:
                continue
            # Track exact (with adjustment) for granular detection
            if tag not in factor_exact:
                factor_exact[tag] = {'wins': 0, 'losses': 0}
            if result == 'WIN':
                factor_exact[tag]['wins'] += 1
            else:
                factor_exact[tag]['losses'] += 1
            # Normalize: strip the adjustment value for grouping
            normalized = re.sub(r'\s*\([^)]*\)\s*$', '', tag).strip()
            if not normalized:
                continue
            if normalized not in factor_records:
                factor_records[normalized] = {'wins': 0, 'losses': 0}
            if result == 'WIN':
                factor_records[normalized]['wins'] += 1
            else:
                factor_records[normalized]['losses'] += 1

    # ── Analyze each loss ──
    analysis = []
    for g in losses:
        sel = g['selection']
        clv = g['clv']
        bet_line = g.get('bet_line')
        closing_line = g.get('closing_line')
        model_spread = g.get('model_spread')
        ctx_str = g.get('context_factors', '') or ''
        pnl = g['pnl']
        sport = g['sport']
        mtype = g['market_type']

        categories = []

        # ── BAD LUCK: CLV >= 0 and lost by <= 3 points ──
        # For spreads/totals the loss margin approximation uses the line.
        # We don't have the raw score diff here, so we use pnl as a proxy:
        # a close loss on a flat -110 bet still loses full units,
        # so instead check CLV — if we had the better number, it was bad luck.
        if clv is not None and clv >= 0:
            # Close loss heuristic: small pnl (1 unit bet = ~1u loss max)
            # or if we can check the line vs closing line delta
            margin_close = False
            if bet_line is not None and closing_line is not None:
                margin_close = abs(bet_line - closing_line) <= 3
            elif clv >= 0:
                margin_close = True  # Had CLV, still lost = unlucky
            if margin_close:
                categories.append('BAD LUCK')

        # ── MODEL GAP: model spread 3+ pts off closing line ──
        # Only compare for SPREAD bets (for totals, model_spread is a small offset, not comparable)
        # closing_line is stored as the dog spread (positive), model_spread is signed (negative = fav)
        if mtype == 'SPREAD' and model_spread is not None and closing_line is not None:
            # Both should be on the same scale: model_spread is e.g. -6.72 (fav by 6.72)
            # closing_line is e.g. 10.5 (dog gets +10.5, meaning fav -10.5)
            closing_as_margin = -closing_line  # convert to same sign convention as model_spread
            gap = abs(model_spread - closing_as_margin)
            if gap >= 3:
                categories.append(f'MODEL GAP ({gap:.1f} pts off)')
        # For totals, compare bet_line vs closing_line to detect adverse line moves
        elif mtype == 'TOTAL' and bet_line is not None and closing_line is not None:
            gap = abs(bet_line - closing_line)
            if gap >= 2:
                categories.append(f'MODEL GAP (line moved {gap:.1f} pts)')

        # ── CONTEXT TRAP: a context factor with < 40% historical win rate fired ──
        trap_factors = []
        if ctx_str:
            for tag in ctx_str.split('|'):
                tag = tag.strip()
                if not tag:
                    continue
                # Check exact factor first (e.g. "Home bounce-back (+0.8)")
                exact_rec = factor_exact.get(tag)
                if exact_rec:
                    total = exact_rec['wins'] + exact_rec['losses']
                    if total >= 2 and exact_rec['wins'] / total < 0.40:
                        trap_factors.append(f"{tag} ({exact_rec['wins']}W-{exact_rec['losses']}L)")
                        continue
                # Fall back to normalized grouping
                normalized = re.sub(r'\s*\([^)]*\)\s*$', '', tag).strip()
                rec = factor_records.get(normalized)
                if rec:
                    total = rec['wins'] + rec['losses']
                    if total >= 2:
                        wr = rec['wins'] / total
                        if wr < 0.40:
                            trap_factors.append(f"{normalized} ({rec['wins']}W-{rec['losses']}L)")
            if trap_factors:
                categories.append(f"CONTEXT TRAP: {', '.join(trap_factors)}")

        # ── SHARP FADE: CLV <= -1.5 (market moved hard against us) ──
        if clv is not None and clv <= -1.5:
            categories.append(f'SHARP FADE (CLV {clv:+.1f})')

        # Default if nothing triggered
        if not categories:
            categories.append('STANDARD LOSS')

        analysis.append({
            'selection': sel,
            'sport': sport,
            'market_type': mtype,
            'pnl': pnl,
            'clv': clv,
            'categories': categories,
        })

    # ── Identify toxic context factors ──
    # Check both normalized (grouped) and exact (with adjustment) factors
    toxic = []
    seen = set()
    # Exact factors: 3+ bets, < 45% win rate (catches e.g. "Home bounce-back (+0.8)" at 0W-3L)
    for factor, rec in factor_exact.items():
        total = rec['wins'] + rec['losses']
        if total >= 3:
            wr = rec['wins'] / total
            if wr < 0.45:
                toxic.append((factor, rec['wins'], rec['losses'], wr))
                seen.add(factor)
    # Grouped factors: 3+ bets, < 45% win rate
    for factor, rec in factor_records.items():
        if factor in seen:
            continue
        total = rec['wins'] + rec['losses']
        if total >= 3:
            wr = rec['wins'] / total
            if wr < 0.45:
                toxic.append((factor, rec['wins'], rec['losses'], wr))
    toxic.sort(key=lambda x: x[3])  # worst win rate first

    # ── Format report ──
    lines = []
    lines.append(f"\n{'─'*60}")
    lines.append(f"  LOSS ANALYSIS — {len(losses)} loss{'es' if len(losses) != 1 else ''}")
    lines.append(f"{'─'*60}")

    for a in analysis:
        clv_str = f"CLV={a['clv']:+.1f}" if a['clv'] is not None else "CLV=N/A"
        lines.append(f"  {a['selection']}")
        lines.append(f"    {a['pnl']:+.1f}u | {clv_str} | {', '.join(a['categories'])}")

    if toxic:
        lines.append(f"\n  TOXIC CONTEXT FACTORS (3+ bets, <45% WR):")
        for factor, w, l, wr in toxic:
            lines.append(f"    {factor}: {w}W-{l}L ({wr:.0%})")

    # Summary counts
    cat_counts = {}
    for a in analysis:
        for c in a['categories']:
            # Group by prefix (strip details)
            prefix = c.split(':')[0].split('(')[0].strip()
            cat_counts[prefix] = cat_counts.get(prefix, 0) + 1

    if cat_counts:
        lines.append(f"\n  BREAKDOWN: {' | '.join(f'{k}: {v}' for k, v in cat_counts.items())}")

    lines.append(f"{'─'*60}")
    return '\n'.join(lines)


def generate_subscriber_recap(conn, graded_bets, overall_stats):
    """Generate a short, confident subscriber-facing recap of yesterday's results.

    Args:
        conn: sqlite3 connection
        graded_bets: list of dicts from grade_bets() for the current batch
        overall_stats: dict with keys 'wins', 'losses', 'pnl' for the full season

    Returns:
        str: 2-3 sentence recap ready to share with subscribers
    """
    SPORT_NAMES = {
        'basketball_nba': 'NBA', 'basketball_ncaab': 'NCAAB',
        'icehockey_nhl': 'NHL', 'baseball_ncaa': 'College Baseball',
        'soccer_epl': 'EPL', 'soccer_italy_serie_a': 'Serie A',
        'soccer_spain_la_liga': 'La Liga', 'soccer_germany_bundesliga': 'Bundesliga',
        'soccer_france_ligue_one': 'Ligue 1', 'soccer_uefa_champs_league': 'UCL',
        'soccer_usa_mls': 'MLS', 'soccer_mexico_ligamx': 'Liga MX',
    }

    settled = [g for g in graded_bets if g['result'] in ('WIN', 'LOSS')]
    if not settled:
        return ""

    wins = sum(1 for g in settled if g['result'] == 'WIN')
    losses = sum(1 for g in settled if g['result'] == 'LOSS')
    pnl = sum(g['pnl'] for g in settled)

    # Group by sport
    sport_stats = {}
    for g in settled:
        sport = g.get('sport', '') or ''
        # Collapse soccer leagues into "Soccer" for the recap
        if 'soccer' in sport:
            label = 'Soccer'
        elif 'tennis' in sport:
            label = 'Tennis'
        else:
            label = SPORT_NAMES.get(sport, sport.split('_')[-1].upper() if sport else 'Other')
        if label not in sport_stats:
            sport_stats[label] = {'wins': 0, 'losses': 0, 'pnl': 0.0}
        if g['result'] == 'WIN':
            sport_stats[label]['wins'] += 1
        else:
            sport_stats[label]['losses'] += 1
        sport_stats[label]['pnl'] += g['pnl']

    # Best and worst sport by P/L
    sorted_sports = sorted(sport_stats.items(), key=lambda x: x[1]['pnl'], reverse=True)
    best_sport, best = sorted_sports[0]
    worst_sport, worst = sorted_sports[-1] if len(sorted_sports) > 1 else (None, None)

    # Build the sport color lines
    color_parts = []
    # Positive callout for best sport
    best_rec = f"{best['wins']}-{best['losses']}"
    color_parts.append(f"{best_sport} carried us going {best_rec}")

    # Brief acknowledgment for worst sport (only if it actually lost units and is different)
    if worst_sport and worst_sport != best_sport and worst['pnl'] < 0:
        if worst['wins'] == 0:
            color_parts.append(f"{worst_sport} didn't connect")
        else:
            color_parts.append(f"{worst_sport} was rough")

    color_line = ". ".join(color_parts) + "."

    # Season totals
    season_w = overall_stats['wins']
    season_l = overall_stats['losses']
    season_pnl = overall_stats['pnl']
    season_wp = season_w / (season_w + season_l) * 100 if (season_w + season_l) > 0 else 0

    # Build recap
    lines = []
    lines.append("YESTERDAY'S RECAP")
    lines.append(f"{wins}W-{losses}L, {pnl:+.1f}u — {color_line}")
    lines.append(f"Still {season_pnl:+.1f}u on the season ({season_w}W-{season_l}L, {season_wp:.1f}% win rate).")

    recap = "\n".join(lines)
    return recap


def daily_grade_and_report(conn=None):
    """Full daily grading: grade bets, compute CLV, accumulate player data, report."""
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close_conn = True

    ensure_tables(conn)

    print(f"\n{'='*60}")
    print(f"  DAILY GRADING — {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    print(f"{'='*60}")

    # Grade last 3 days of bets
    graded = grade_bets(conn, days_back=3)

    if graded:
        wins = sum(1 for g in graded if g['result'] == 'WIN')
        losses = sum(1 for g in graded if g['result'] == 'LOSS')
        pnl = sum(g['pnl'] for g in graded)
        clv_vals = [g['clv'] for g in graded if g['clv'] is not None]
        avg_clv = sum(clv_vals) / len(clv_vals) if clv_vals else None

        print(f"\n  Graded {len(graded)} bets: {wins}W-{losses}L | {pnl:+.2f}u")
        if avg_clv is not None:
            clv_label = 'beating closing line' if avg_clv > 0 else 'below closing line'
            print(f"  Avg CLV: {avg_clv:+.2f} pts ({clv_label})")

        for g in graded:
            e = 'W' if g['result'] == 'WIN' else ('L' if g['result'] == 'LOSS' else 'P')
            clv_str = f"CLV={g['clv']:+.1f}" if g['clv'] is not None else ""
            print(f"    [{e}] {g['selection']:40s} {g['result']:5s} {g['pnl']:+.1f}u {clv_str}")
    else:
        print("  No new bets to grade (scores may not be in yet)")

    # Automatic loss analysis
    if graded and any(g['result'] == 'LOSS' for g in graded):
        try:
            loss_report = analyze_losses(conn, graded)
            if loss_report:
                print(loss_report)
        except Exception as e:
            print(f"  Loss analysis: {e}")

    # Subscriber recap — clean summary for followers (no CLV / MODEL GAP)
    subscriber_recap = ""
    if graded and any(g['result'] in ('WIN', 'LOSS') for g in graded):
        try:
            season_all = conn.execute("""
                SELECT result, pnl_units FROM graded_bets
                WHERE DATE(created_at) >= '2026-03-04'
                AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
                AND units >= 3.5
            """).fetchall()
            overall_stats = {
                'wins': sum(1 for r in season_all if r[0] == 'WIN'),
                'losses': sum(1 for r in season_all if r[0] == 'LOSS'),
                'pnl': sum(r[1] or 0 for r in season_all),
            }
            subscriber_recap = generate_subscriber_recap(conn, graded, overall_stats)
            if subscriber_recap:
                _sep = '\u2500' * 60
                print(f"\n{_sep}")
                print(f"  SUBSCRIBER RECAP")
                print(f"{_sep}")
                indented = subscriber_recap.replace(chr(10), chr(10) + '  ')
                print(f"  {indented}")
                print(f"{_sep}")
        except Exception as e:
            print(f"  Subscriber recap: {e}")

    # Check for stale PENDING bets (possible postponements)
    pending_warning = ""
    try:
        stale_pending = conn.execute("""
            SELECT b.id, b.selection, b.sport, b.created_at
            FROM bets b
            WHERE b.units >= 3.5
            AND b.id NOT IN (
                SELECT bet_id FROM graded_bets
                WHERE bet_id IS NOT NULL AND result != 'PENDING'
            )
            AND JULIANDAY('now') - JULIANDAY(b.created_at) > 1.0
            AND DATE(b.created_at) >= DATE('now', '-7 days')
        """).fetchall()
        if len(stale_pending) >= 3:
            pending_warning = f"\n\u26a0\ufe0f {len(stale_pending)} picks still pending (no result found) \u2014 possible postponements:\n"
            for row in stale_pending:
                pending_warning += f"  - {row[1]} ({row[2]}) placed {row[3][:10]}\n"
            print(pending_warning)
    except Exception as e:
        print(f"  Pending check: {e}")

    # v17: Update power ratings from recent game results
    # The update_ratings_post_game() function existed but was never called,
    # leaving power ratings stale since bootstrap (Feb 27). Now we process
    # all completed games from the last 3 days to keep ratings current.
    try:
        from model_engine import update_ratings_post_game
        rated_sports = ['basketball_nba', 'basketball_ncaab', 'icehockey_nhl',
                        'baseball_ncaa', 'soccer_epl', 'soccer_italy_serie_a',
                        'soccer_spain_la_liga', 'soccer_germany_bundesliga',
                        'soccer_france_ligue_one', 'soccer_usa_mls',
                        'soccer_mexico_ligamx', 'soccer_uefa_champs_league']
        rating_cutoff = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        # Get the latest power_ratings timestamp per sport to avoid reprocessing
        latest_ts = {}
        for sp in rated_sports:
            row = conn.execute(
                "SELECT MAX(run_timestamp) FROM power_ratings WHERE sport=?", (sp,)
            ).fetchone()
            latest_ts[sp] = row[0] if row and row[0] else '2000-01-01'

        updated_count = 0
        for sp in rated_sports:
            games = conn.execute("""
                SELECT home, away, home_score, away_score, commence_time
                FROM results
                WHERE sport=? AND completed=1 AND home_score IS NOT NULL
                AND commence_time >= ? AND commence_time > ?
                ORDER BY commence_time ASC
            """, (sp, rating_cutoff, latest_ts[sp])).fetchall()
            for g in games:
                update_ratings_post_game(conn, sp, g[0], g[1], g[2], g[3])
                updated_count += 1
        if updated_count:
            print(f"\n  Power ratings updated: {updated_count} games processed")
    except Exception as e:
        print(f"  Power ratings update: {e}")

    # Accumulate player results for historical signal
    accumulate_player_results(conn, days_back=3)

    # Generate 7-day report
    # v12.2: Use full record since March 4 (model launch date), not rolling 7 days.
    # Discord and email should show the complete track record.
    report = performance_report(conn, start_date='2026-03-04')

    # Append subscriber recap to report
    if subscriber_recap and report:
        report += "\n\n" + subscriber_recap

    # Append pending warning to report
    if pending_warning and report:
        report += "\n" + pending_warning

    if close_conn: conn.close()
    return report


if __name__ == '__main__':
    daily_grade_and_report()
