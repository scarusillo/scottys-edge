"""
upload_db.py — Create a slim agent DB and upload to GitHub Releases.

Creates a trimmed copy of the production DB with only recent odds data
(last 3 days) and all reference tables. Compresses and uploads as
GitHub release 'db-latest' for cloud agents to download.

Usage:
    python upload_db.py          # Create slim DB + upload
    python upload_db.py --local  # Create slim DB only (no upload)
"""
import sqlite3, os, sys, gzip, subprocess
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
SLIM_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model_slim.db')
GZ_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db.gz')

# Tables to copy in full (small, needed by agents)
FULL_TABLES = [
    'bets', 'graded_bets', 'results', 'power_ratings', 'elo_ratings',
    'market_consensus', 'pitcher_stats', 'team_pitching_quality',
    'injuries', 'officials', 'soccer_standings', 'settings',
    'nhl_probable_goalies', 'nhl_goalie_stats', 'mlb_probable_pitchers',
    'shadow_blocked_picks', 'sqlite_sequence',
]

# Tables to copy with a date filter (large, only recent data needed)
RECENT_TABLES = {
    'odds': ('snapshot_date', 3),
    'props': ('snapshot_date', 3),
    'prop_snapshots': ('captured_at', 3),
    'openers': ('snapshot_date', 7),
    'line_snapshots': ('snapshot_time', 3),
}


def create_slim_db():
    """Create a trimmed copy of the production DB."""
    if os.path.exists(SLIM_PATH):
        os.remove(SLIM_PATH)

    src = sqlite3.connect(DB_PATH)
    dst = sqlite3.connect(SLIM_PATH)

    total_rows = 0

    # Full tables
    for table in FULL_TABLES:
        try:
            schema = src.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'"
            ).fetchone()
            if not schema or not schema[0]:
                continue
            dst.execute(schema[0])
            rows = src.execute(f'SELECT * FROM [{table}]').fetchall()
            if rows:
                placeholders = ','.join(['?' for _ in rows[0]])
                dst.executemany(f'INSERT INTO [{table}] VALUES ({placeholders})', rows)
            total_rows += len(rows)
            print(f"  {table}: {len(rows):,} rows")
        except Exception as e:
            print(f"  {table}: SKIP ({e})")

    # Recent tables
    for table, (date_col, days) in RECENT_TABLES.items():
        try:
            schema = src.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'"
            ).fetchone()
            if not schema or not schema[0]:
                continue
            dst.execute(schema[0])
            where = f"{date_col} >= DATE('now', '-{days} days')"
            rows = src.execute(f'SELECT * FROM [{table}] WHERE {where}').fetchall()
            if rows:
                placeholders = ','.join(['?' for _ in rows[0]])
                dst.executemany(f'INSERT INTO [{table}] VALUES ({placeholders})', rows)
            total_rows += len(rows)
            print(f"  {table}: {len(rows):,} rows (last {days} days)")
        except Exception as e:
            print(f"  {table}: SKIP ({e})")

    dst.commit()
    src.close()
    dst.close()

    size_mb = os.path.getsize(SLIM_PATH) / 1024 / 1024
    print(f"\n  Slim DB: {size_mb:.0f} MB, {total_rows:,} total rows")

    # Compress the SLIM DB — full 3.2 GB prod DB compresses to ~280 MB and
    # was timing out GitHub release uploads (leaving release in draft with
    # 0 assets, which is why cloud agents got 404). Slim compresses to
    # ~30-50 MB and uploads reliably. Slim already contains all authoritative
    # tables (bets, graded_bets, elo_ratings, etc.) the cloud pipeline needs.
    print("  Compressing slim DB...")
    with open(SLIM_PATH, 'rb') as f_in:
        with gzip.open(GZ_PATH, 'wb', compresslevel=6) as f_out:
            while True:
                chunk = f_in.read(8 * 1024 * 1024)  # 8MB chunks
                if not chunk:
                    break
                f_out.write(chunk)

    gz_mb = os.path.getsize(GZ_PATH) / 1024 / 1024
    print(f"  Compressed: {gz_mb:.0f} MB")

    # Clean up slim DB (keep .gz only)
    os.remove(SLIM_PATH)

    return gz_mb


def upload_to_github():
    """Upload compressed DB to GitHub Releases as db-latest.

    Uses `upload --clobber` on an existing release rather than delete+create.
    Delete+create was leaving the release in draft with 0 assets when the
    upload timed out, which 404'd cloud agents. --clobber preserves the
    release and tag; if the upload fails, the prior asset remains downloadable.
    """
    today = datetime.now().strftime('%Y-%m-%d %H:%M')
    repo = 'scarusillo/scottys-edge'

    # Fast path: release + tag already exist, just replace the asset
    upload = subprocess.run(
        ['gh', 'release', 'upload', 'db-latest', GZ_PATH,
         '--repo', repo, '--clobber'],
        capture_output=True, text=True
    )

    if upload.returncode == 0:
        # Refresh title/notes + make sure it's published (not draft)
        subprocess.run(
            ['gh', 'release', 'edit', 'db-latest',
             '--repo', repo,
             '--title', f'Database Snapshot — {today}',
             '--notes', f'Slim agent DB. Generated {today}.',
             '--draft=false'],
            capture_output=True
        )
        print(f"  Uploaded to GitHub Releases: db-latest")
        return True

    # Slow path: release doesn't exist yet — create it fresh, pinned to main
    # so the tag actually resolves (prevents the "untagged-xxxx" draft trap).
    result = subprocess.run(
        ['gh', 'release', 'create', 'db-latest', GZ_PATH,
         '--repo', repo,
         '--target', 'main',
         '--title', f'Database Snapshot — {today}',
         '--notes', f'Slim agent DB. Generated {today}.',
         '--latest=false'],
        capture_output=True, text=True
    )

    if result.returncode == 0:
        print(f"  Created GitHub Release: db-latest")
        return True
    else:
        print(f"  Upload failed. upload stderr: {upload.stderr} | create stderr: {result.stderr}")
        return False


if __name__ == '__main__':
    print(f"=== DB Upload — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
    gz_mb = create_slim_db()

    if '--local' not in sys.argv:
        upload_to_github()
    else:
        print("  --local mode: skipping upload")
