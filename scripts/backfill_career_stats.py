"""
backfill_career_stats.py — One-time script to expand career_stats_cache coverage.

Runs get_career_stat() for every NBA player × prop stat that has appeared in
our prop_snapshots in the last 14 days. Enables PROP_CAREER_FADE gate
(v25.87) to fire on the full volume of active prop players instead of
the 47% currently matched.

Safe to re-run: the 7-day cache in career_stats.py skips fresh entries.
Misses are also cached to avoid hammering ESPN.
"""
import sqlite3
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from career_stats import get_career_stat

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Stats we gate on for each sport (matches CAREER_STAT_MAP)
SPORT_STATS = {
    'basketball_nba': ['pts', 'reb', 'ast', 'threes', 'blk', 'stl'],
}


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=14)).isoformat()

    for sport, stats in SPORT_STATS.items():
        # Unique players with recent prop snapshots
        players = [r[0] for r in c.execute("""
            SELECT DISTINCT player FROM prop_snapshots
            WHERE sport = ? AND captured_at > ? AND player IS NOT NULL
        """, (sport, cutoff))]

        print(f"[{sport}] {len(players)} unique players in last 14 days")

        hit_count = 0
        miss_count = 0
        skip_cached = 0

        for i, player in enumerate(players, 1):
            if i % 25 == 0:
                print(f"  [{i}/{len(players)}] hits={hit_count} miss={miss_count} cached={skip_cached}")

            for stat in stats:
                # Check cache freshness directly (avoid calling ESPN on fresh rows)
                fresh = c.execute("""
                    SELECT career_avg FROM career_stats_cache
                    WHERE player=? AND sport=? AND stat_type=?
                      AND fetched_at > datetime('now','-7 days')
                """, (player, sport, stat)).fetchone()
                if fresh is not None:
                    skip_cached += 1
                    continue

                try:
                    avg, games = get_career_stat(conn, player, stat, sport)
                    if avg is not None:
                        hit_count += 1
                    else:
                        miss_count += 1
                except Exception as e:
                    print(f"    error {player}/{stat}: {e}")
                    miss_count += 1

                # Gentle rate limit — ESPN tolerates but don't hammer
                time.sleep(0.1)

        print(f"[{sport}] done: {hit_count} new hits, {miss_count} misses, {skip_cached} already cached")

    # Final coverage report
    print()
    print("=== Coverage after backfill ===")
    for row in c.execute("""
        SELECT sport, COUNT(DISTINCT player) players,
               SUM(CASE WHEN career_avg IS NOT NULL THEN 1 ELSE 0 END) with_data,
               SUM(CASE WHEN career_avg IS NULL THEN 1 ELSE 0 END) without_data
        FROM career_stats_cache
        GROUP BY sport
    """):
        print(f"  {row}")


if __name__ == '__main__':
    main()
