"""
sync_db_to_cloud.py — Copy database to OneDrive for cloud agent access

Copies betting_model.db to the OneDrive sync folder. The OneDrive client
automatically uploads it to Microsoft's cloud. Cloud agents download
it via a shared link (no API credentials needed).

Run nightly via Task Scheduler or after grading.
"""
import shutil
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
ONEDRIVE_DEST = os.path.join(os.path.expanduser('~'), 'OneDrive', 'scottys_edge_data', 'betting_model.db')


def sync_db():
    """Copy the database to OneDrive sync folder."""
    if not os.path.exists(DB_PATH):
        print(f"  DB not found: {DB_PATH}")
        return False

    db_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)

    # Ensure destination directory exists
    os.makedirs(os.path.dirname(ONEDRIVE_DEST), exist_ok=True)

    try:
        shutil.copy2(DB_PATH, ONEDRIVE_DEST)
        print(f"  DB synced to OneDrive: {db_size_mb:.0f} MB at {datetime.now().strftime('%H:%M:%S')}")
        return True
    except Exception as e:
        print(f"  DB sync failed: {e}")
        return False


if __name__ == '__main__':
    print("Syncing database to OneDrive...")
    sync_db()
