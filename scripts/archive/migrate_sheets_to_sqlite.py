"""
migrate_sheets_to_sqlite.py — One-time import of your three Excel workbooks into SQLite.
Run after schema.py has created the database.
"""
import sqlite3
import pandas as pd
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

ODDS_FEED = None
DATA_STORE = None
MASTER = None

def set_paths(odds_feed, data_store, master):
    global ODDS_FEED, DATA_STORE, MASTER
    ODDS_FEED = odds_feed
    DATA_STORE = data_store
    MASTER = master

def safe_str(val):
    if pd.isna(val):
        return None
    return str(val)

def safe_float(val):
    if pd.isna(val):
        return None
    try:
        return float(val)
    except Exception:
        return None

def migrate_odds(conn):
    """Import ODDS_CURRENT, ODDS_HISTORY, SNAPSHOTS from both workbooks."""
    print("Migrating odds data...")
    c = conn.cursor()
    total = 0

    sources = [
        (ODDS_FEED, 'ODDS_CURRENT'),
        (ODDS_FEED, 'ODDS_HISTORY'),
        (ODDS_FEED, 'SNAPSHOTS'),
        (DATA_STORE, 'SNAPSHOTS'),
        (MASTER, 'SNAPSHOTS'),
        (MASTER, 'ODDS_CURRENT'),
        (MASTER, 'ODDS_HISTORY'),
    ]

    for filepath, sheet_name in sources:
        try:
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            if len(df) <= 1 and df.iloc[0].isna().all():
                continue
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    safe_str(r.get('Date')),
                    safe_str(r.get('Time')),
                    safe_str(r.get('Tag')),
                    safe_str(r.get('Sport')),
                    safe_str(r.get('EventId')),
                    safe_str(r.get('CommenceTime')),
                    safe_str(r.get('Home')),
                    safe_str(r.get('Away')),
                    safe_str(r.get('Book')),
                    safe_str(r.get('Market')),
                    safe_str(r.get('Selection')),
                    safe_float(r.get('Line')),
                    safe_float(r.get('Odds')),
                ))
            c.executemany("""
                INSERT INTO odds (snapshot_date, snapshot_time, tag, sport, event_id,
                    commence_time, home, away, book, market, selection, line, odds)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            total += len(rows)
            print(f"  {os.path.basename(filepath)}:{sheet_name} → {len(rows)} rows")
        except Exception as e:
            print(f"  Skipping {os.path.basename(filepath)}:{sheet_name}: {e}")

    conn.commit()
    print(f"  Total odds rows: {total}")

def migrate_openers(conn):
    """Import OPENERS from all workbooks."""
    print("Migrating openers...")
    c = conn.cursor()
    total = 0

    sources = [
        (ODDS_FEED, 'OPENERS', {'OpenerLine': 'Line', 'OpenerOdds': 'Odds', 'OpenerTimestamp': 'Timestamp'}),
        (DATA_STORE, 'OPENERS', {'Line': 'Line', 'Odds': 'Odds', 'Timestamp': 'Timestamp'}),
        (MASTER, 'OPENERS', {'Line': 'Line', 'Odds': 'Odds', 'Timestamp': 'Timestamp'}),
    ]

    for filepath, sheet_name, col_map in sources:
        try:
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            if len(df) < 1:
                continue
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    safe_str(r.get('Date')),
                    safe_str(r.get('Sport')),
                    safe_str(r.get('EventId')),
                    safe_str(r.get('Book', r.get('D'))),
                    safe_str(r.get('Market', r.get('E'))),
                    safe_str(r.get('Selection', r.get('F'))),
                    safe_float(r.get(col_map.get('Line', 'Line'), r.get('OpenerLine'))),
                    safe_float(r.get(col_map.get('Odds', 'Odds'), r.get('OpenerOdds'))),
                    safe_str(r.get(col_map.get('Timestamp', 'Timestamp'), r.get('OpenerTimestamp'))),
                ))
            c.executemany("""
                INSERT INTO openers (snapshot_date, sport, event_id, book, market,
                    selection, line, odds, timestamp)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, rows)
            total += len(rows)
            print(f"  {os.path.basename(filepath)}:{sheet_name} → {len(rows)} rows")
        except Exception as e:
            print(f"  Skipping {os.path.basename(filepath)}:{sheet_name}: {e}")

    conn.commit()
    print(f"  Total opener rows: {total}")

def migrate_props(conn):
    """Import all prop sheets."""
    print("Migrating props...")
    c = conn.cursor()
    total = 0

    sources = [
        (ODDS_FEED, 'PROPS_CURRENT'),
        (ODDS_FEED, 'PROPS_HISTORY'),
        (ODDS_FEED, 'PROPS_SNAPSHOTS'),
        (DATA_STORE, 'PROPS_SNAPSHOTS'),
        (MASTER, 'PROPS_CURRENT'),
        (MASTER, 'PROPS_HISTORY'),
        (MASTER, 'PROPS_SNAPSHOTS'),
    ]

    for filepath, sheet_name in sources:
        try:
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            if len(df) <= 1 and df.iloc[0].isna().all():
                continue
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    safe_str(r.get('Date')),
                    safe_str(r.get('Time')),
                    safe_str(r.get('Tag')),
                    safe_str(r.get('Sport')),
                    safe_str(r.get('EventId')),
                    safe_str(r.get('CommenceTime')),
                    safe_str(r.get('Home')),
                    safe_str(r.get('Away')),
                    safe_str(r.get('Book')),
                    safe_str(r.get('Market')),
                    safe_str(r.get('Selection')),
                    safe_float(r.get('Line')),
                    safe_float(r.get('Odds')),
                ))
            c.executemany("""
                INSERT INTO props (snapshot_date, snapshot_time, tag, sport, event_id,
                    commence_time, home, away, book, market, selection, line, odds)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            total += len(rows)
            print(f"  {os.path.basename(filepath)}:{sheet_name} → {len(rows)} rows")
        except Exception as e:
            print(f"  Skipping {os.path.basename(filepath)}:{sheet_name}: {e}")

    conn.commit()
    print(f"  Total prop rows: {total}")

def migrate_power_ratings(conn):
    """Import power ratings and ratings output."""
    print("Migrating power ratings...")
    c = conn.cursor()

    # RATINGS_OUTPUT has the actual fitted ratings
    try:
        df = pd.read_excel(ODDS_FEED, sheet_name='RATINGS_OUTPUT')
        rows = []
        for _, r in df.iterrows():
            rows.append((
                safe_str(r.get('Run_Timestamp')),
                safe_str(r.get('Sport')),
                safe_str(r.get('Team')),
                safe_float(r.get('Base_Rating')),
                0, 0, 0, None,
                safe_float(r.get('Base_Rating')),  # final = base until adjustments
                safe_float(r.get('GamesUsed')),
                safe_float(r.get('Iters')),
                safe_float(r.get('LR')),
                safe_float(r.get('Reg')),
            ))
        c.executemany("""
            INSERT INTO power_ratings (run_timestamp, sport, team, base_rating,
                rest_adjust, injury_adjust, situational_adjust, manual_override,
                final_rating, games_used, iterations, learning_rate, regularization)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        print(f"  RATINGS_OUTPUT → {len(rows)} rows")
    except Exception as e:
        print(f"  Skipping RATINGS_OUTPUT: {e}")

    # Also import the POWER_RATINGS sheet (has NBA/NHL/Soccer team stubs)
    try:
        df = pd.read_excel(ODDS_FEED, sheet_name='POWER_RATINGS')
        existing = set(c.execute("SELECT sport, team FROM power_ratings").fetchall())
        rows = []
        for _, r in df.iterrows():
            key = (safe_str(r.get('Sport')), safe_str(r.get('Team')))
            if key not in existing and key[0] and key[1]:
                rows.append((
                    datetime.now().isoformat(),
                    key[0], key[1],
                    safe_float(r.get('Base_Rating')),
                    safe_float(r.get('Home_Court', 2.5)),
                    safe_float(r.get('Rest_Adjust', 0)),
                    safe_float(r.get('Injury_Adjust', 0)),
                    safe_float(r.get('Situational_Adjust', 0)),
                    None,
                    safe_float(r.get('Final_Rating')),
                    None, None, None, None,
                ))
        if rows:
            c.executemany("""
                INSERT INTO power_ratings (run_timestamp, sport, team, base_rating,
                    home_court, rest_adjust, injury_adjust, situational_adjust,
                    manual_override, final_rating, games_used, iterations,
                    learning_rate, regularization)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            print(f"  POWER_RATINGS stubs → {len(rows)} rows")

    except Exception as e:
        print(f"  Skipping POWER_RATINGS: {e}")

    conn.commit()

def migrate_market_consensus(conn):
    """Import market consensus from master workbook."""
    print("Migrating market consensus...")
    c = conn.cursor()

    try:
        df = pd.read_excel(MASTER, sheet_name='MARKET_CONSENSUS')
        rows = []
        for _, r in df.iterrows():
            rows.append((
                datetime.now().strftime('%Y-%m-%d'),
                '530PM',
                safe_str(r.get('Sport')),
                safe_str(r.get('EventId')),
                safe_str(r.get('CommenceTime')),
                safe_str(r.get('Home')),
                safe_str(r.get('Away')),
                safe_float(r.get('Best_Home_Spread')),
                safe_float(r.get('Best_Home_Spread_Odds')),
                safe_str(r.get('Best_Home_Spread_Book')),
                safe_float(r.get('Best_Away_Spread')),
                safe_float(r.get('Best_Away_Spread_Odds')),
                safe_str(r.get('Best_Away_Spread_Book')),
                safe_float(r.get('Best_Over_Total')),
                safe_float(r.get('Best_Over_Odds')),
                safe_str(r.get('Best_Over_Book')),
                safe_float(r.get('Best_Under_Total')),
                safe_float(r.get('Best_Under_Odds')),
                safe_str(r.get('Best_Under_Book')),
                safe_float(r.get('Best_Home_ML')),
                safe_str(r.get('Best_Home_ML_Book')),
                safe_float(r.get('Best_Away_ML')),
                safe_str(r.get('Best_Away_ML_Book')),
                None,  # consensus_spread (compute later)
                None,  # consensus_total
                safe_float(r.get('Model_Spread')),
                safe_float(r.get('Model_Total')),
                None, None,
            ))
        c.executemany("""
            INSERT INTO market_consensus (snapshot_date, tag, sport, event_id,
                commence_time, home, away, best_home_spread, best_home_spread_odds,
                best_home_spread_book, best_away_spread, best_away_spread_odds,
                best_away_spread_book, best_over_total, best_over_odds, best_over_book,
                best_under_total, best_under_odds, best_under_book, best_home_ml,
                best_home_ml_book, best_away_ml, best_away_ml_book,
                consensus_spread, consensus_total, model_spread, model_total,
                spread_edge, total_edge)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        print(f"  MARKET_CONSENSUS → {len(rows)} rows")
    except Exception as e:
        print(f"  Error: {e}")

    conn.commit()

def run_migration(odds_feed, data_store, master):
    set_paths(odds_feed, data_store, master)
    conn = sqlite3.connect(DB_PATH)
    migrate_odds(conn)
    migrate_openers(conn)
    migrate_props(conn)
    migrate_power_ratings(conn)
    migrate_market_consensus(conn)
    conn.close()
    print("\n✅ Migration complete!")

    # Print summary
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for table in ['odds', 'openers', 'props', 'power_ratings', 'market_consensus', 'results', 'bets', 'injuries']:
        count = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")
    conn.close()

if __name__ == '__main__':
    run_migration(
        '/mnt/user-data/uploads/BS2_ODDS_FEED__2_.xlsx',
        '/mnt/user-data/uploads/BS2_DATA_STORE__1_.xlsx',
        '/mnt/user-data/uploads/BS3_Master_Model_Workbook_v1__2_.xlsx',
    )
