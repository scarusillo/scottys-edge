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
"""
import sqlite3, os
from datetime import datetime, timedelta

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
            except:
                pass
    conn.commit()


# ═══════════════════════════════════════════════════════════════════
# CLV COMPUTATION
# ═══════════════════════════════════════════════════════════════════

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

    # Priority 1: Same book's last snapshot (apples-to-apples comparison)
    if bet_book:
        row = conn.execute("""
            SELECT line, odds, snapshot_date, snapshot_time, book FROM odds
            WHERE event_id=? AND market=? AND selection=? AND book=?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT 1
        """, (event_id, market, odds_selection, bet_book)).fetchone()
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
            WHERE event_id=? AND market=? AND selection=?
            GROUP BY book
        ) latest ON o.book = latest.book
            AND (o.snapshot_date || ' ' || o.snapshot_time) = latest.max_snap
        WHERE o.event_id=? AND o.market=? AND o.selection=?
    """, (event_id, market, odds_selection, event_id, market, odds_selection)).fetchall()
    
    if not rows:
        return None, None, None, None
    
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
    # v12.2: Only grade 4.5u+ picks (public threshold). Sub-4.5u picks are tracked
    # in the bets table but not graded or shown in the record.
    all_bets = conn.execute("""
        SELECT id, sport, event_id, market_type, selection,
               book, line, odds, edge_pct, confidence, units, created_at
        FROM bets
        WHERE DATE(created_at) >= ?
        AND units >= 4.5
        AND (result IS NULL OR result NOT IN ('TAINTED'))
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
        bid, sport, eid, mtype, sel, book, line, odds, edge, conf, units, created = b

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
                except:
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
                print(f"  ⚠ No score found: {sel} ({sport}) — event ID mismatch, team lookup failed")
                continue

        h_score, a_score, home, away, _ = score

        # Determine W/L/P
        # v12.1: Props use box score player stats, not team scores
        if mtype == 'PROP':
            try:
                from box_scores import grade_prop
                bet_date = created[:10] if created else None
                result = grade_prop(conn, sel, line, bet_date, sport=sport)
            except ImportError:
                result = 'PENDING'  # box_scores.py not installed yet
            except Exception as e:
                print(f"  ⚠ Prop grading error: {e}")
                result = 'PENDING'
        else:
            result = determine_result(sel, mtype, line, h_score, a_score, home, away, sport=sport)
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
            'timing': timing,
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
        if 'REBOUNDS' in sel_upper or 'REB' in sel_upper:
            return 'player_rebounds'
        elif 'ASSISTS' in sel_upper or 'AST' in sel_upper:
            return 'player_assists'
        elif 'THREES' in sel_upper or '3PT' in sel_upper or 'THREE' in sel_upper:
            return 'player_threes'
        else:
            return 'player_points'  # Default for props
    
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
        AND units >= 4.5
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
        'baseball_ncaa': 'NCAA Baseball',
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

    # Check for stale PENDING bets (possible postponements)
    pending_warning = ""
    try:
        stale_pending = conn.execute("""
            SELECT b.id, b.selection, b.sport, b.created_at
            FROM bets b
            WHERE b.units >= 4.5
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

    # Accumulate player results for historical signal
    accumulate_player_results(conn, days_back=3)

    # Generate 7-day report
    # v12.2: Use full record since March 4 (model launch date), not rolling 7 days.
    # Discord and email should show the complete track record.
    report = performance_report(conn, start_date='2026-03-04')

    # Append pending warning to report
    if pending_warning and report:
        report += "\n" + pending_warning

    if close_conn: conn.close()
    return report


if __name__ == '__main__':
    daily_grade_and_report()
