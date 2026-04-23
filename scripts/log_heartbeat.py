"""
log_heartbeat.py — append a heartbeat entry to data/engagement_log.json

Called by the scotty-edge-engagement scheduled task at the START and END of every run,
whether or not any comments get posted. Purpose: make scheduler gaps visible.

If the log only has "post"/"skip" entries, a day with nothing to post looks identical
to a day where the scheduler never fired. The heartbeat fixes that — a run that the
scheduler fired but found nothing to do still leaves a trace.

Usage:
  python log_heartbeat.py start
  python log_heartbeat.py end --posted 3 --skipped 7 --note "barstool throttled"

The log entry uses target="_heartbeat" and platform="scheduler" so it's easy to
distinguish from real posting entries.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# engagement_log lives next to this script in ../data/
HERE = Path(__file__).resolve().parent
LOG_PATH = HERE.parent / "data" / "engagement_log.json"


def append_entry(entry: dict) -> None:
    """Append a single entry to engagement_log.json. Creates the file if missing."""
    if LOG_PATH.exists():
        try:
            with LOG_PATH.open("r", encoding="utf-8") as f:
                log = json.load(f)
            if not isinstance(log, list):
                log = []
        except (json.JSONDecodeError, OSError):
            log = []
    else:
        log = []

    log.append(entry)

    tmp = LOG_PATH.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)
    os.replace(tmp, LOG_PATH)


def main() -> int:
    parser = argparse.ArgumentParser(description="Write a heartbeat entry to engagement_log.json")
    parser.add_argument("phase", choices=["start", "end"], help="Run phase")
    parser.add_argument("--posted", type=int, default=0, help="Count of comments posted this run")
    parser.add_argument("--skipped", type=int, default=0, help="Count of comments skipped this run")
    parser.add_argument("--failed", type=int, default=0, help="Count of comments that failed this run")
    parser.add_argument("--note", type=str, default="", help="Free-form note for context")
    parser.add_argument("--queue-size", type=int, default=-1, help="Number of IG comments in cowork_comments.json at run time")
    args = parser.parse_args()

    if args.phase == "start":
        status = "heartbeat_run_start"
        if args.queue_size >= 0:
            status += f" — queue={args.queue_size} IG comments"
        if args.note:
            status += f" — {args.note}"
    else:  # end
        status = (
            f"heartbeat_run_end — posted={args.posted} skipped={args.skipped} failed={args.failed}"
        )
        if args.note:
            status += f" — {args.note}"

    entry = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "platform": "scheduler",
        "target": "_heartbeat",
        "game": "",
        "status": status,
    }

    append_entry(entry)
    print(f"heartbeat written: {entry['timestamp']} {args.phase}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
