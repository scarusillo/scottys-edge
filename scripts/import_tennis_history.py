"""Import Jeff Sackmann's historical ATP/WTA match data to seed surface Elo.

Source: https://github.com/JeffSackmann/tennis_atp and tennis_wta (CC license).
Problem it solves: our clay Elo only has ~1,265 matches — most Madrid / French
Open players sit at default 1500. Historical 2023-2024 adds ~4,000 clay matches
across both tours, enough to seed meaningful surface ratings for the top 200+
players before this clay season.

Usage:
    python scripts/import_tennis_history.py                 # load + rebuild
    python scripts/import_tennis_history.py --no-rebuild    # load only
    python scripts/import_tennis_history.py --wipe          # drop + reload

CSVs expected at: data/tennis_history/{atp,wta}_matches_{year}.csv
"""
import argparse
import csv
import os
import re
import sqlite3
import sys

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
CSV_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'tennis_history')

SCHEMA = """
CREATE TABLE IF NOT EXISTS tennis_match_history (
    id INTEGER PRIMARY KEY,
    tour TEXT NOT NULL,           -- 'atp' or 'wta'
    surface TEXT NOT NULL,        -- 'Clay', 'Hard', 'Grass', 'Carpet'
    tourney_date TEXT NOT NULL,   -- ISO date (YYYY-MM-DD)
    tourney_name TEXT,
    round TEXT,
    winner_name TEXT NOT NULL,
    loser_name TEXT NOT NULL,
    score TEXT,
    winner_games INTEGER,
    loser_games INTEGER,
    source TEXT DEFAULT 'sackmann',
    UNIQUE(tour, tourney_date, tourney_name, winner_name, loser_name)
);
CREATE INDEX IF NOT EXISTS idx_tmh_surface ON tennis_match_history(surface, tour, tourney_date);
CREATE INDEX IF NOT EXISTS idx_tmh_winner ON tennis_match_history(winner_name);
CREATE INDEX IF NOT EXISTS idx_tmh_loser ON tennis_match_history(loser_name);
"""


_set_re = re.compile(r'(\d+)-(\d+)(?:\(\d+\))?')


def parse_games(score: str) -> tuple[int, int]:
    """Parse a score string like '6-4 7-6(3) 6-4' into (winner_games, loser_games).

    Handles tiebreak parens, retirement 'RET', walkover 'W/O', 'DEF'.
    """
    if not score:
        return 0, 0
    # Strip notes
    s = score.upper().replace('RET', '').replace('DEF.', '').replace('W/O', '').strip()
    w_tot = l_tot = 0
    for m in _set_re.finditer(s):
        w_tot += int(m.group(1))
        l_tot += int(m.group(2))
    return w_tot, l_tot


def parse_date(yyyymmdd: str) -> str:
    if not yyyymmdd or len(yyyymmdd) != 8:
        return ''
    return f'{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}'


def load_csv(conn: sqlite3.Connection, tour: str, path: str) -> tuple[int, int]:
    """Load one CSV. Returns (rows_inserted, rows_skipped)."""
    inserted = 0
    skipped = 0
    cur = conn.cursor()
    with open(path, 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            winner = (row.get('winner_name') or '').strip()
            loser = (row.get('loser_name') or '').strip()
            surface = (row.get('surface') or '').strip()
            date = parse_date(row.get('tourney_date', ''))
            if not winner or not loser or not surface or not date:
                skipped += 1
                continue
            w_games, l_games = parse_games(row.get('score', ''))
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO tennis_match_history
                    (tour, surface, tourney_date, tourney_name, round,
                     winner_name, loser_name, score, winner_games, loser_games)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (tour, surface, date, row.get('tourney_name', ''),
                      row.get('round', ''), winner, loser,
                      row.get('score', ''), w_games, l_games))
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except sqlite3.IntegrityError:
                skipped += 1
    conn.commit()
    return inserted, skipped


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--wipe', action='store_true',
                   help='Drop table + reload')
    p.add_argument('--no-rebuild', action='store_true',
                   help='Skip Elo rebuild after import')
    args = p.parse_args()

    conn = sqlite3.connect(DB, timeout=30)
    if args.wipe:
        conn.execute('DROP TABLE IF EXISTS tennis_match_history')
        conn.commit()
        print('[wipe] dropped tennis_match_history')
    conn.executescript(SCHEMA)
    conn.commit()

    total_in = 0
    total_skip = 0
    for fn in sorted(os.listdir(CSV_DIR)):
        if not fn.endswith('.csv'):
            continue
        parts = fn.replace('.csv', '').split('_')
        tour = parts[0]  # 'atp' or 'wta'
        path = os.path.join(CSV_DIR, fn)
        ins, skp = load_csv(conn, tour, path)
        total_in += ins
        total_skip += skp
        print(f'  {fn}: inserted {ins:>4d}, skipped {skp:>4d}')

    print(f'\nTotal: inserted {total_in:,}, skipped {total_skip:,}')

    # Summary
    cur = conn.cursor()
    print('\nHistorical match coverage:')
    for row in cur.execute("""
        SELECT tour, surface, COUNT(*),
               MIN(tourney_date), MAX(tourney_date)
        FROM tennis_match_history
        GROUP BY tour, surface ORDER BY tour, surface
    """).fetchall():
        print(f'  {row[0]} {row[1]:8s}  n={row[2]:>5,}  {row[3]} → {row[4]}')

    conn.close()

    if not args.no_rebuild:
        print('\n>>> Rebuilding tennis Elo with historical data included...')
        # Defer import until after data loaded
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(__file__))
        from elo_engine import build_tennis_elo
        build_tennis_elo(verbose=True)


if __name__ == '__main__':
    main()
