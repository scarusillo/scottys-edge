# Scotty's Edge — Engagement Morning Briefing (Apr 29, 2026)

## ⚠️ Scheduler Health — UPSTREAM OUTAGE day 3

The scotty-edge-engagement scheduler is HEALTHY: 15 starts / 15 ends on Apr 28 (expected ~15). Heartbeats are clean.

**The cowork_comments.json producer is NOT.** mtime stuck at Apr 26 16:41 UTC (~72h stale), generated_at 2026-04-26T12:41, game_date=null. Every Apr 28 run no-opped because all 100 in-scope items reference Apr 26 games already settled (24h freshness rule fires).

picks_log.jsonl, briefing_data.json, and the IG results graphic for Apr 29 (261W-208L | +82.59u, posted today) are all updating fine. The generator job alone is broken. **Operator intervention required.**

## Yesterday's Activity — Instagram
- Comments posted: 0
- Comments skipped: ~750 across 15 hourly runs (50 IG × 15)
- Reason: queue 2+ days stale, picks reference Apr 26 games

## Yesterday's Activity — TikTok
- Comments posted: 0
- Comments skipped: ~750 across 15 hourly runs (50 TT × 15)
- Reason: same stale queue — not a TikTok-specific regression, IG and TT both blocked at the source

## Profile Snapshots
- Instagram (@scottys_edge): 108 followers (-9 from 117 on Apr 27, -25 from 133 on Apr 26), 123 posts (+2). Today's Apr 29 results graphic posted — no comments yet.
- TikTok (@scottys_edge): 3 followers, 25 following, 16 total likes. Profile grid still showing "Something went wrong" — 6th consecutive day. Cannot sample video performance.

## Comment Performance
No new comments to sample — zero posted yesterday on either platform.

## Recommendation
Today's priority is fixing the cowork_comments generator job, not running the scheduler. Every hourly run while upstream is broken is wasted compute, and the IG follower bleed (-25 in 3 days) will continue. While the generator is being looked at, the TikTok grid error also needs attention — 6 days of "Something went wrong" suggests the saved IG/TT session state may be expired or rate-limited.

Snapshot appended to `data/daily_metrics.json`.
