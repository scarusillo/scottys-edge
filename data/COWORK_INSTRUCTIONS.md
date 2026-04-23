# Cowork Engagement Instructions

## What This Is

Every time the betting model finds picks and runs with `--email`, it generates a file at:

```
data/cowork_comments.json
```

This file contains pre-written comments tailored for 3 platforms — **Instagram**, **TikTok**, and **Reddit** — targeting team pages and betting pages associated with the games we have picks on.

**Twitter/X is dead.** Account @Scottys_Edge was permanently suspended April 2026. Do not attempt to post to X.

**TikTok was added Apr 19 2026.** Both Instagram and TikTok comments should be posted each run. Reddit is currently out of scope for automated posting (browser flow unreliable).

## File Locations

```
C:\Users\carus\OneDrive\Desktop\scottys_edge\betting_model\data\cowork_comments.json
```

The database (read-only reference for season record, pick history):
```
C:\Users\carus\OneDrive\Desktop\scottys_edge\betting_model\data\betting_model.db
```

## CRITICAL: Record Verification Before Posting

**Before posting ANY comment, verify the season record is current.** The comments embed the season record (e.g., "154W-118L"). If the model graded new results since the comments were generated, the record in the comments may be stale.

To verify, query the database:
```sql
SELECT
  SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as W,
  SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as L
FROM graded_bets WHERE units >= 3.5 AND result IN ('WIN','LOSS')
```

If the W-L in the database doesn't match the W-L in the comment text, **update the comment text before posting** with the correct numbers. Never post stale records.

## JSON Structure

```json
{
  "generated_at": "2026-04-09T06:03:26",
  "total_comments": 28,
  "by_platform": { "ig": 14, "reddit": 14 },
  "comments": [
    {
      "platform": "ig",
      "target": "actionnetworkhq",
      "target_type": "betting",
      "comment": "Model flags Vanderbilt ML (-125) — 20% edge. Running 154W-118L...",
      "game": "Oklahoma Sooners vs Vanderbilt Commodores",
      "pick": "Vanderbilt ML",
      "sport": "NCAA Baseball"
    }
  ]
}
```

Each comment entry tells you:
- **platform** — where to post (ig, reddit)
- **target** — the account or subreddit to comment on
- **target_type** — "team" (team official page) or "betting" (betting media page)
- **comment** — the text to post (verify record first!)
- **game** — which game this is about (so you find the right post)
- **pick** / **sport** — context for finding relevant posts

## How It Should Work

### General Flow

1. **Heartbeat at run start (REQUIRED).** Before any other work, call `python scripts/log_heartbeat.py start --queue-size <N> --note "<short context>"`. This writes a `_heartbeat` entry to `engagement_log.json` so scheduler gaps are visible — a run that fires but finds nothing to do still leaves a trace. **Do not skip this even if you're about to exit early** (outside posting window, stale queue, etc.).
2. **Check `data/cowork_comments.json`** for new picks
3. Check the `generated_at` timestamp — if it changed since your last check, there are new picks
4. **Verify the season record** against the database before posting
5. For each comment entry, find a **recent post from the target account about that game**
6. Post the comment on that post
7. **Only today's picks matter.** If the `generated_at` date is from a previous day, ignore
8. **Heartbeat at run end (REQUIRED).** Before exiting, call `python scripts/log_heartbeat.py end --posted <P> --skipped <S> --failed <F> --note "<short summary>"`. Run this on every exit path — including early exits from stale queue, outside-window, or CAPTCHA flags.

### Why heartbeats matter

The scheduled task `scotty-edge-engagement` runs hourly 6:30am–8:30pm. On Apr 20 2026, the scheduler went silent from ~10:41am to the next morning — no sessions spawned — and the outage was invisible because `engagement_log.json` only records posts/skips, not that a run happened. Heartbeats make missed fires detectable in the daily summary.

### The File Contains ALL of Today's Picks

The file accumulates picks throughout the day. Each model run appends new picks to the existing file (deduped — no duplicates). When the timestamp changes, new picks were added. To find which comments are new, compare the `total_comments` count to what you last saw — the new ones are at the end of the list.

Yesterday's comments are automatically cleared when the first run of a new day writes to the file.

---

## Instagram Rules (Critical — Avoid Bot Detection)

1. **Space comments out.** Wait 3-5 minutes between each Instagram comment.
2. **Do not comment more than 8-10 times per hour.**
3. **Vary the comment slightly each time.** Swap a word, rephrase the opening. Instagram fingerprints identical text.
4. **Do NOT use the Instagram API or any API calls.** Browser only. Navigate to `instagram.com/<username>`.
5. **Scroll naturally.** Browse a couple posts before commenting.
6. **Like the post before commenting.**
7. **Do not tag other accounts in comments.** No @ tags.
8. **If a CAPTCHA or "suspicious activity" prompt appears, stop immediately.**
9. **Session behavior matters.** Browse feed first, check notifications, look at stories before engaging.

### Instagram Summary
- 3-5 min gaps between comments
- Max 8-10 comments/hour
- Slightly rephrase each comment
- Browser only, no API
- Like before commenting
- Scroll before engaging
- Stop on any CAPTCHA

---

## TikTok Rules (mirror of Instagram)

TikTok posting uses the same browser-based flow as Instagram. Login is the **@scottys_edge** account.

1. URL pattern: `tiktok.com/@<target>` (e.g., `tiktok.com/@barstoolsports`)
2. Find the target's most recent video about the game in the `game` field
3. **Like the video** before commenting — same anti-bot signal as IG
4. Open the comments panel, type the **rephrased** comment, post
5. **3–5 min gaps** between TikTok comments
6. **Max 8–10 TikTok comments per hour**
7. Rephrase each comment — TikTok also fingerprints duplicate text
8. **Do NOT** use @ mentions in the comment body
9. If a CAPTCHA or rate-limit appears, stop TikTok for this run and move on
10. Same 24-hour freshness rule: skip if the target's latest relevant video is older than 24h
11. Log each action to `engagement_log.json` with `platform="tiktok"`

**Run order:** Instagram first (higher-risk platform, do it when the session is fresh), then TikTok, then heartbeat_end.

---

## Reddit Rules

1. **For team subreddits** — find the game thread or matchup discussion. Comment there. Do not create new posts.
2. **For betting subreddits** (r/sportsbetting, r/sportsbook) — find the daily discussion thread or "picks" thread.
3. **Reddit comments are longer and more analytical** — this is intentional. Reddit responds to data, not hype.
4. **Do not post the same comment in multiple subreddits.** Reddit cross-references duplicate text.
5. **One comment per subreddit per session.**

---

## What NOT To Do

- Do not comment on posts that are not about the specific game
- Do not post identical text across multiple accounts on the same platform
- Do not engage in arguments or reply to replies
- Do not comment on old posts (>24 hours)
- Do not follow/unfollow accounts
- Do not DM anyone
- Do not post if `cowork_comments.json` is from a previous day
- **Do not post to Twitter/X** — account is permanently suspended
- **Do not post stale records** — always verify against the database first

## Our Accounts

- Instagram: @scottys_edge
- Reddit: u/Tall_Database_2086
- Discord: discord.gg/JQ6rRfuN
