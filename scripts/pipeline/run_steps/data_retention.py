"""
cmd_grade post-grade — odds / props / prop_snapshots retention pruning.

Keeps 7 days of snapshot rows LIVE in the main DB for query speed. Older
rows are MOVED (not deleted) to a separate archive DB file (sibling to
main DB) so backtests can still UNION live + archive when they need
deeper history. Single ATTACH covers all three tables.

History:
  v25.75 (2026-04-22): odds_archive moved to separate file
  v25.79 (2026-04-23): props + prop_snapshots also archived

Backtest scripts that need pre-7-day history use scripts/archive_db.py to
ATTACH the archive on demand.

Extracted from main.py cmd_grade() in v26.0 Phase 8.
"""
import os
import sqlite3
from datetime import datetime, timedelta


def prune_to_archive(db):
    """Move pre-7-day rows from odds/props/prop_snapshots to archive DB."""
    # ═══ DATA RETENTION — archive old odds/props/prop_snapshots ═══
    # Keep 7 days of snapshots LIVE in main DB for speed. Pre-prune rows
    # are moved to a SEPARATE archive DB file (data/betting_model_archive.db)
    # so main DB stays lean and queries stay fast.
    # v25.75 (2026-04-22): odds_archive moved to separate file.
    # v25.79 (2026-04-23): props + prop_snapshots also archived to separate
    # file (was DELETE-only / in-main bloat). Single ATTACH covers all three.
    # Backtest scripts: use scripts/archive_db.py to UNION live + archive.
    try:
        _prune_conn = sqlite3.connect(db)
        _cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        # Archive DB path (sibling to main DB)
        _archive_db = os.path.join(os.path.dirname(db), 'betting_model_archive.db')
        _prune_conn.execute(f"ATTACH DATABASE '{_archive_db}' AS arc")
        _prune_conn.executescript('''
            CREATE TABLE IF NOT EXISTS arc.odds_archive (
                id              INTEGER PRIMARY KEY,
                snapshot_date   TEXT NOT NULL,
                snapshot_time   TEXT,
                tag             TEXT,
                sport           TEXT NOT NULL,
                event_id        TEXT NOT NULL,
                commence_time   TEXT,
                home            TEXT NOT NULL,
                away            TEXT NOT NULL,
                book            TEXT NOT NULL,
                market          TEXT NOT NULL,
                selection       TEXT NOT NULL,
                line            REAL,
                odds            REAL
            );
            CREATE INDEX IF NOT EXISTS arc.idx_oa_event ON odds_archive(event_id, snapshot_date);
            CREATE INDEX IF NOT EXISTS arc.idx_oa_sport_date ON odds_archive(sport, snapshot_date);
            CREATE INDEX IF NOT EXISTS arc.idx_oa_market ON odds_archive(market, snapshot_date);

            CREATE TABLE IF NOT EXISTS arc.props_archive (
                id              INTEGER PRIMARY KEY,
                snapshot_date   TEXT NOT NULL,
                snapshot_time   TEXT,
                tag             TEXT,
                sport           TEXT NOT NULL,
                event_id        TEXT NOT NULL,
                commence_time   TEXT,
                home            TEXT NOT NULL,
                away            TEXT NOT NULL,
                book            TEXT NOT NULL,
                market          TEXT NOT NULL,
                selection       TEXT NOT NULL,
                line            REAL,
                odds            REAL
            );
            CREATE INDEX IF NOT EXISTS arc.idx_arc_pa_event ON props_archive(event_id, snapshot_date);
            CREATE INDEX IF NOT EXISTS arc.idx_arc_pa_sport_date ON props_archive(sport, snapshot_date);
            CREATE INDEX IF NOT EXISTS arc.idx_arc_pa_market ON props_archive(market, snapshot_date);

            CREATE TABLE IF NOT EXISTS arc.prop_snapshots_archive (
                id              INTEGER PRIMARY KEY,
                captured_at     TEXT NOT NULL,
                sport           TEXT NOT NULL,
                event_id        TEXT NOT NULL,
                commence_time   TEXT,
                home            TEXT NOT NULL,
                away            TEXT NOT NULL,
                book            TEXT NOT NULL,
                market          TEXT NOT NULL,
                player          TEXT NOT NULL,
                side            TEXT NOT NULL,
                line            REAL,
                odds            REAL,
                implied_prob    REAL
            );
            CREATE INDEX IF NOT EXISTS arc.idx_arc_psa_player ON prop_snapshots_archive(player, market, event_id);
            CREATE INDEX IF NOT EXISTS arc.idx_arc_psa_event ON prop_snapshots_archive(event_id, captured_at);
            CREATE INDEX IF NOT EXISTS arc.idx_arc_psa_date ON prop_snapshots_archive(captured_at);
        ''')

        # 1. Odds — archive + delete (snapshot_date < cutoff)
        _odds_before = _prune_conn.execute('SELECT COUNT(*) FROM odds').fetchone()[0]
        _prune_conn.execute('''
            INSERT INTO arc.odds_archive
            SELECT * FROM odds WHERE snapshot_date < ?
        ''', (_cutoff,))
        _prune_conn.execute('DELETE FROM odds WHERE snapshot_date < ?', (_cutoff,))

        # 2. Props — archive + delete (commence_time > 7 days old)
        _props_before = _prune_conn.execute('SELECT COUNT(*) FROM props').fetchone()[0]
        _prune_conn.execute('''
            INSERT INTO arc.props_archive
            SELECT * FROM props WHERE commence_time < datetime('now', '-7 days')
        ''')
        _prune_conn.execute("DELETE FROM props WHERE commence_time < datetime('now', '-7 days')")

        # 3. Prop_snapshots — archive + delete (captured_at > 7 days old)
        _ps_before = _prune_conn.execute('SELECT COUNT(*) FROM prop_snapshots').fetchone()[0]
        _prune_conn.execute('''
            INSERT INTO arc.prop_snapshots_archive
            SELECT * FROM prop_snapshots WHERE captured_at < datetime('now', '-7 days')
        ''')
        _prune_conn.execute("DELETE FROM prop_snapshots WHERE captured_at < datetime('now', '-7 days')")

        _prune_conn.commit()
        try:
            _prune_conn.execute("DETACH DATABASE arc")
        except Exception:
            pass

        _odds_after = _prune_conn.execute('SELECT COUNT(*) FROM odds').fetchone()[0]
        _props_after = _prune_conn.execute('SELECT COUNT(*) FROM props').fetchone()[0]
        _ps_after = _prune_conn.execute('SELECT COUNT(*) FROM prop_snapshots').fetchone()[0]
        _prune_conn.close()
        _odds_pruned = _odds_before - _odds_after
        _props_pruned = _props_before - _props_after
        _ps_pruned = _ps_before - _ps_after
        if _odds_pruned > 0 or _props_pruned > 0 or _ps_pruned > 0:
            print(f"  🗃️ Retention: archived {_odds_pruned:,} odds + {_props_pruned:,} props + {_ps_pruned:,} prop_snapshots (>7 days old → betting_model_archive.db)")
    except Exception as e:
        print(f"  Retention pruning: {e}")

