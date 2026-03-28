"""
sync_db_to_cloud.py — Upload database to GitHub Release for cloud agent access

Compresses and uploads betting_model.db as a GitHub Release asset.
Cloud agents download it when they run. Free, no extra accounts needed.

Run nightly via Task Scheduler or after grading.
"""
import gzip
import os
import subprocess
import shutil
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
COMPRESSED_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db.gz')
RELEASE_TAG = 'db-latest'


def compress_db():
    """Compress the database with gzip."""
    if not os.path.exists(DB_PATH):
        print(f"  DB not found: {DB_PATH}")
        return False

    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    print(f"  Compressing {db_size:.0f} MB database...")

    with open(DB_PATH, 'rb') as f_in:
        with gzip.open(COMPRESSED_PATH, 'wb', compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)

    gz_size = os.path.getsize(COMPRESSED_PATH) / (1024 * 1024)
    print(f"  Compressed: {gz_size:.0f} MB ({gz_size/db_size:.0%} of original)")
    return True


def upload_to_github():
    """Upload compressed DB as a GitHub Release asset using gh CLI."""
    # Delete existing release if it exists
    subprocess.run(
        ['gh', 'release', 'delete', RELEASE_TAG, '--yes', '--cleanup-tag'],
        capture_output=True, text=True
    )

    # Create new release with the compressed DB
    result = subprocess.run(
        ['gh', 'release', 'create', RELEASE_TAG,
         COMPRESSED_PATH,
         '--title', f'Database Snapshot — {datetime.now().strftime("%Y-%m-%d %H:%M")}',
         '--notes', f'Auto-uploaded by sync_db_to_cloud.py\nSize: {os.path.getsize(COMPRESSED_PATH)/(1024*1024):.0f} MB compressed',
         '--latest=false'],
        capture_output=True, text=True
    )

    if result.returncode == 0:
        print(f"  Uploaded to GitHub Release: {RELEASE_TAG}")
        return True
    else:
        print(f"  Upload failed: {result.stderr}")
        return False


def sync_db():
    """Compress and upload database to GitHub."""
    if compress_db():
        success = upload_to_github()
        # Clean up compressed file
        if os.path.exists(COMPRESSED_PATH):
            os.remove(COMPRESSED_PATH)
        return success
    return False


# Also keep the OneDrive copy as a local backup
def sync_to_onedrive():
    """Copy DB to OneDrive sync folder as backup."""
    onedrive_dest = os.path.join(os.path.expanduser('~'), 'OneDrive', 'scottys_edge_data', 'betting_model.db')
    os.makedirs(os.path.dirname(onedrive_dest), exist_ok=True)
    try:
        shutil.copy2(DB_PATH, onedrive_dest)
        print(f"  OneDrive backup synced")
    except Exception as e:
        print(f"  OneDrive backup failed: {e}")


if __name__ == '__main__':
    print("Syncing database to cloud...")
    sync_db()
    sync_to_onedrive()
