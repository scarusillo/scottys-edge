# scotty-edge-engagement — 2026-04-21 11:31 EDT run report

## Outcome: ABORTED EARLY — shell filled disk on DB copy, never recovered

## Timeline

- 15:31:42 UTC (11:31 EDT) — heartbeat_run_start logged via `scripts/log_heartbeat.py start --queue-size 20`
- 15:32 UTC — ran `cp data/betting_model.db /tmp/bm.db` to work around `database disk image is malformed` error on direct read. The DB is 3.2 GB; `/tmp` filled before the copy finished.
- 15:32 UTC onward — every subsequent `mcp__workspace__bash` call returned `RPC error -1: ... no space left on device`. No way to run `log_heartbeat.py end`, query the DB, or drive Chrome MCP browser sessions.
- ~15:33 UTC — wrote `heartbeat_run_end` directly into `data/engagement_log.json` via the Edit file tool. Counts: `posted=0 skipped=0 failed=0`.

## Queue state at run start

- `cowork_comments.json` generated_at 2026-04-21T09:09:39 (~6.5 h old, still same-day)
- 60 total comments: 20 IG, 20 Reddit, 20 TikTok
- Reddit / TikTok out of scope (Reddit blocked in-browser per memory; TikTok not in workflow)
- IG: 20 queued, 16 already posted or skipped earlier today (10:31, 11:32, 13:31, 14:31 runs)
- **IG remaining unposted entering this run (4):**
  - actionnetworkhq × Celtics -14.0
  - barstoolsports × Celtics -14.0
  - actionnetworkhq × Spurs -11.5
  - barstoolsports × Spurs -11.5

All four are betting-aggregator accounts. Earlier runs today skipped aggregators repeatedly with the note *"no game-specific posts visible in feed"* and *"comments limited on recent reel"* — so even with a healthy shell, the expected outcome was more skips than posts.

## Record drift

- Queue text embeds `211W-165L (56.1%)`
- DB at 12:33 UTC run showed `218W-172L` (verified by prior scheduled run)
- Delta: 7 wins / 7 losses accrued since the queue was generated. Per the 12:33 heartbeat note, this drift was judged acceptable; the 14:31 run posted one @spurs comment with the record manually updated to 218-172.

## Root cause

Direct read via `sqlite3.connect('.../betting_model.db')` raised `database disk image is malformed`, likely because the WAL file (`betting_model.db-wal`, 0 bytes, mtime 15:32) was being touched by another process mid-read. The workaround — copy to /tmp and read the copy — did not account for DB size (3.2 GB) vs sandbox /tmp capacity and exhausted the filesystem, which in turn broke the bash server's ability to write its own srt-settings state files (`/etc/srt-settings/*.tmp: no space left on device`). The bash server never auto-recovered within this session.

## Recommended fixes for the next run

1. **Don't copy the DB.** Use `sqlite3.connect('file:.../betting_model.db?mode=ro&immutable=1', uri=True)` — the `immutable=1` flag makes SQLite ignore the WAL entirely and reads the main DB file in place. No copy, no disk risk.
2. **If that still errors** (actual corruption): read from `betting_model.db.gz` (280 MB) after gunzip to a smaller temp path, OR grep recent pick records out of `picks_log.jsonl` as a fallback for record verification.
3. **Guard the sandbox disk.** Add a `df /tmp` check to `log_heartbeat.py start` and refuse to proceed if free space <4 GB — or have the helper `rm -f /tmp/bm.db` on startup.
4. **Heartbeat-end fallback path.** `log_heartbeat.py` should accept a `--file-only` mode that writes straight to `engagement_log.json` without needing the shell (or the calling agent should be comfortable doing it via Edit, as happened here).
5. **Queue-level refresh.** The queue is now ~7 hours old and the record text has drifted by 14 graded bets. Worth regenerating `cowork_comments.json` (run the model with `--email`) if the next 1–2 runs also early-exit on this queue — rephrasing 4 comments with a stale record template is worse than letting the model rewrite them.

## Counts for the day so far (from engagement_log.json)

- Posted today (IG): 5 — coloradoavalanche, lakings, houstonrockets, sixers, spurs
- Skipped today (IG): 11 — aggregators (actionnetworkhq, barstoolsports) mostly, plus @celtics (comments-limited on recent reels)
- Heartbeats today: 6 start / 5 end (this run's end is the 6th)

## This run's final counts

- posted: 0
- skipped: 0
- failed: 0 (aborted before any posting attempt)

Next scheduled fire: 2026-04-21T16:31 UTC (12:31 EDT).
