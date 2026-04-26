"""
Date / timezone helpers — Eastern-time conversions used by display + emails
+ social posts + main.py CLI commands.

Extracted from model_engine.py in v26.0 Phase 7 final.

Re-exported from model_engine for back-compat:
  `from model_engine import _to_eastern, _eastern_tz_label` keeps working.
"""
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
    EASTERN = ZoneInfo('America/New_York')
except ImportError:
    EASTERN = None


def _to_eastern(utc_dt):
    """Convert UTC datetime to Eastern time (handles DST automatically).

    Falls back to a rough DST window (March-November = EDT, else EST) when
    `zoneinfo` is unavailable or the input is naive.
    """
    if EASTERN and utc_dt.tzinfo:
        return utc_dt.astimezone(EASTERN)
    month = utc_dt.month
    if 3 <= month <= 10:
        return utc_dt - timedelta(hours=4)  # EDT
    return utc_dt - timedelta(hours=5)  # EST


def _eastern_tz_label():
    """Return 'EDT' or 'EST' based on current date.

    Uses naive `datetime.now()` since this is for display labels only —
    actual timezone math goes through `_to_eastern` above.
    """
    now = datetime.now()
    if 3 <= now.month <= 10:
        return 'EDT'
    return 'EST'
