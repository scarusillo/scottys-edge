"""
schema.py — Full SQLite schema for the betting model.
Run once to create the database, safe to re-run (uses IF NOT EXISTS).
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

def create_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # ── 1. TEAMS ──────────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        sport       TEXT NOT NULL,
        canonical   TEXT NOT NULL,
        UNIQUE(sport, canonical)
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS team_aliases (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        sport       TEXT NOT NULL,
        alias       TEXT NOT NULL,
        canonical   TEXT NOT NULL,
        UNIQUE(sport, alias)
    )""")

    # ── 2. ODDS (current + historical snapshots) ─────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS odds (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date   TEXT NOT NULL,
        snapshot_time   TEXT,
        tag             TEXT,           -- 'CURRENT', '11AM', '530PM'
        sport           TEXT NOT NULL,
        event_id        TEXT NOT NULL,
        commence_time   TEXT,
        home            TEXT NOT NULL,
        away            TEXT NOT NULL,
        book            TEXT NOT NULL,
        market          TEXT NOT NULL,   -- h2h, spreads, totals
        selection       TEXT NOT NULL,
        line            REAL,
        odds            REAL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_odds_event ON odds(event_id, market, tag)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_odds_date ON odds(snapshot_date, sport)")

    # ── 3. OPENERS ────────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS openers (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date   TEXT NOT NULL,
        sport           TEXT NOT NULL,
        event_id        TEXT NOT NULL,
        book            TEXT NOT NULL,
        market          TEXT NOT NULL,
        selection       TEXT NOT NULL,
        line            REAL,
        odds            REAL,
        timestamp       TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_openers_event ON openers(event_id, market)")

    # ── 4. PLAYER PROPS ──────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS props (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date   TEXT NOT NULL,
        snapshot_time   TEXT,
        tag             TEXT,
        sport           TEXT NOT NULL,
        event_id        TEXT NOT NULL,
        commence_time   TEXT,
        home            TEXT NOT NULL,
        away            TEXT NOT NULL,
        book            TEXT NOT NULL,
        market          TEXT NOT NULL,   -- player_points, player_assists, etc.
        selection       TEXT NOT NULL,   -- "Player Name - Over/Under"
        line            REAL,
        odds            REAL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_props_event ON props(event_id, market, tag)")

    # ── 4b. PROP SNAPSHOTS (timestamped for line movement tracking) ──
    c.execute("""
    CREATE TABLE IF NOT EXISTS prop_snapshots (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        captured_at     TEXT NOT NULL,       -- ISO timestamp
        sport           TEXT NOT NULL,
        event_id        TEXT NOT NULL,
        commence_time   TEXT,
        home            TEXT NOT NULL,
        away            TEXT NOT NULL,
        book            TEXT NOT NULL,
        market          TEXT NOT NULL,       -- player_points, etc.
        player          TEXT NOT NULL,       -- player name (parsed)
        side            TEXT NOT NULL,       -- 'Over' or 'Under'
        line            REAL,
        odds            REAL,
        implied_prob    REAL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ps_player ON prop_snapshots(player, market, event_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ps_event ON prop_snapshots(event_id, captured_at)")

    # ── 4c. PROP OPENERS (first-seen lines for each player/prop/event) ──
    c.execute("""
    CREATE TABLE IF NOT EXISTS prop_openers (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        first_seen      TEXT NOT NULL,
        sport           TEXT NOT NULL,
        event_id        TEXT NOT NULL,
        player          TEXT NOT NULL,
        market          TEXT NOT NULL,
        opening_line    REAL,
        opening_over_odds  REAL,
        opening_under_odds REAL,
        UNIQUE(event_id, player, market)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_po_event ON prop_openers(event_id, player, market)")

    # ── 4d. PLAYER RESULTS (accumulated from graded props) ──
    c.execute("""
    CREATE TABLE IF NOT EXISTS player_results (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        game_date       TEXT NOT NULL,
        sport           TEXT NOT NULL,
        event_id        TEXT NOT NULL,
        player          TEXT NOT NULL,
        stat_type       TEXT NOT NULL,       -- pts, reb, ast, threes
        actual_value    REAL,
        prop_line       REAL,               -- what the line was
        result          TEXT,               -- 'OVER', 'UNDER', 'PUSH'
        UNIQUE(event_id, player, stat_type)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_plr_player ON player_results(player, stat_type)")

    # ── 5. POWER RATINGS ─────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS power_ratings (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        run_timestamp       TEXT NOT NULL,
        sport               TEXT NOT NULL,
        team                TEXT NOT NULL,
        base_rating         REAL,
        home_court          REAL DEFAULT 0,
        rest_adjust         REAL DEFAULT 0,
        injury_adjust       REAL DEFAULT 0,
        situational_adjust  REAL DEFAULT 0,
        manual_override     REAL,
        final_rating        REAL,
        games_used          INTEGER,
        iterations          INTEGER,
        learning_rate       REAL,
        regularization      REAL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pr_sport ON power_ratings(sport, team, run_timestamp)")

    # ── 6. GAME RESULTS (NEW — the most critical addition) ───────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS results (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        sport           TEXT NOT NULL,
        event_id        TEXT NOT NULL UNIQUE,
        commence_time   TEXT,
        home            TEXT NOT NULL,
        away            TEXT NOT NULL,
        home_score      INTEGER,
        away_score      INTEGER,
        winner          TEXT,           -- home team name or away team name
        completed       INTEGER DEFAULT 0,
        -- closing lines (captured right before game starts)
        closing_spread  REAL,           -- home spread
        closing_total   REAL,
        closing_ml_home REAL,
        closing_ml_away REAL,
        -- ATS / OU results (computed)
        ats_home_result TEXT,           -- 'WIN', 'LOSS', 'PUSH'
        ou_result       TEXT,           -- 'OVER', 'UNDER', 'PUSH'
        actual_total    INTEGER,
        actual_margin   INTEGER,        -- home_score - away_score
        fetched_at      TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_results_sport ON results(sport, commence_time)")

    # ── 7. BETS (track every recommendation + outcome) ────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS bets (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at      TEXT NOT NULL,
        sport           TEXT NOT NULL,
        event_id        TEXT NOT NULL,
        market_type     TEXT NOT NULL,   -- SPREAD, MONEYLINE, TOTAL, PROP
        selection       TEXT NOT NULL,
        book            TEXT NOT NULL,
        line            REAL,
        odds            REAL,
        model_prob      REAL,
        implied_prob    REAL,
        edge_pct        REAL,
        confidence      TEXT,
        units           REAL,
        -- filled in after game completes
        result          TEXT,           -- 'WIN', 'LOSS', 'PUSH'
        profit          REAL,
        closing_line    REAL,
        clv             REAL,           -- closing line value
        -- metadata for analysis
        side_type       TEXT,
        spread_bucket   TEXT,
        edge_bucket     TEXT,
        timing          TEXT,
        context_factors TEXT,
        context_confirmed INT,
        context_adj     REAL,
        market_tier     TEXT,
        model_spread    REAL,
        day_of_week     TEXT,
        FOREIGN KEY(event_id) REFERENCES results(event_id)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_bets_date ON bets(created_at)")

    # ── 8. INJURY REPORTS ─────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS injuries (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        report_date     TEXT NOT NULL,
        sport           TEXT NOT NULL,
        team            TEXT NOT NULL,
        player          TEXT NOT NULL,
        status          TEXT NOT NULL,   -- OUT, DOUBTFUL, QUESTIONABLE, PROBABLE
        injury_type     TEXT,
        point_impact    REAL DEFAULT 0,  -- estimated spread impact
        source          TEXT,
        updated_at      TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_injuries_team ON injuries(sport, team, report_date)")

    # ── 9. MARKET CONSENSUS (best lines per event) ───────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS market_consensus (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date       TEXT NOT NULL,
        tag                 TEXT,
        sport               TEXT NOT NULL,
        event_id            TEXT NOT NULL,
        commence_time       TEXT,
        home                TEXT NOT NULL,
        away                TEXT NOT NULL,
        best_home_spread    REAL,
        best_home_spread_odds REAL,
        best_home_spread_book TEXT,
        best_away_spread    REAL,
        best_away_spread_odds REAL,
        best_away_spread_book TEXT,
        best_over_total     REAL,
        best_over_odds      REAL,
        best_over_book      TEXT,
        best_under_total    REAL,
        best_under_odds     REAL,
        best_under_book     TEXT,
        best_home_ml        REAL,
        best_home_ml_book   TEXT,
        best_away_ml        REAL,
        best_away_ml_book   TEXT,
        consensus_spread    REAL,         -- median across books
        consensus_total     REAL,
        model_spread        REAL,
        model_total         REAL,
        spread_edge         REAL,
        total_edge          REAL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_mc_event ON market_consensus(event_id, tag)")

    # ── 10. GRADED BETS (performance tracking with CLV) ────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS graded_bets (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        graded_at       TEXT,
        bet_id          INTEGER,
        sport           TEXT,
        event_id        TEXT,
        selection       TEXT,
        market_type     TEXT,
        book            TEXT,
        line            REAL,
        odds            REAL,
        edge_pct        REAL,
        confidence      TEXT,
        units           REAL,
        result          TEXT,
        pnl_units       REAL,
        closing_line    REAL,
        clv             REAL,
        created_at      TEXT,
        side_type       TEXT,
        spread_bucket   TEXT,
        edge_bucket     TEXT,
        timing          TEXT,
        context_factors TEXT,
        context_confirmed INT,
        market_tier     TEXT,
        model_spread    REAL,
        day_of_week     TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_gb_date ON graded_bets(created_at)")

    # ── 10b. TENNIS METADATA ────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS tennis_metadata (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id        TEXT NOT NULL UNIQUE,
        tournament      TEXT,
        surface         TEXT,        -- 'hard', 'clay', 'grass'
        round           TEXT,        -- 'R1', 'R2', 'R3', 'R4', 'QF', 'SF', 'F'
        best_of         INTEGER,     -- 3 or 5
        set_scores      TEXT,        -- JSON: [[6,4],[3,6],[7,5]]
        total_games     INTEGER,     -- sum of all games across all sets
        player1_rank    INTEGER,
        player2_rank    INTEGER,
        match_duration_min INTEGER
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tm_event ON tennis_metadata(event_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tm_surface ON tennis_metadata(surface, tournament)")

    # ── 11. SETTINGS ──────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key     TEXT PRIMARY KEY,
        value   TEXT
    )""")

    default_settings = [
        ('ODDS_API_KEY', ''),
        ('NY_LEGAL_BOOKS', 'DraftKings,FanDuel,BetMGM,Caesars,BetRivers,Bally Bet,ESPN BET,PointsBet,Fanatics'),
        ('EXCLUDED_BOOKS', 'Bovada'),
        ('REPORT_EMAIL', ''),
        ('NCAAB_LOGISTIC_SCALE', '3.7'),   # Fixed calibration
        ('NBA_LOGISTIC_SCALE', '4.0'),
        ('NHL_LOGISTIC_SCALE', '3.5'),
        ('MIN_EDGE_SPREAD', '3.0'),         # Minimum edge % to flag a bet
        ('MIN_EDGE_ML', '5.0'),
        ('MIN_EDGE_PROP', '4.0'),
        ('KELLY_FRACTION', '0.125'),
        ('MAX_UNITS', '3.0'),
    ]
    c.executemany("INSERT OR IGNORE INTO settings VALUES (?,?)", default_settings)

    conn.commit()
    conn.close()
    print(f"Database created at {os.path.abspath(DB_PATH)}")

if __name__ == '__main__':
    create_db()
