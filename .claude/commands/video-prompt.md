Generate the daily Sora video prompt based on today's graded results.

# THE ONE RULE

**Output the contents of `data/cards/kling_prompt.txt` verbatim. Do not write your own version.**

That file is produced by `_generate_kling_prompt` in `main.py`. It queries the DB
directly, so every number (tonight's record, per-sport W-L, season W-L, season %)
is already correct. Your only job is to read the file and paste it back.

If you find yourself composing sentences, stop — you're doing it wrong.

# OBSERVED FAILURE MODES (all caused by Claude paraphrasing)

Every one of these has happened in a prior session and cost the user a correction:

1. **Wrong season record** (e.g. `206-162` instead of `200-157`). Caused by
   hallucinating numbers from conversation context instead of reading the file.
2. **Missing "WELCOME TO SCOTTY'S EDGE" on-screen text in Scene 1**. Caused by
   "cleaning up" the prompt and deleting the line.
3. **Missing "No voiceover — all text is on-screen only" header**. Sora adds a
   voiceover if this line is absent.
4. **Scene reordering or athlete-description rewrites**. The order and exact
   athlete copy in the file are load-bearing — leave them alone.

The slash command exists so these mistakes stop recurring. Do not re-create them.

# PROCEDURE (follow in order — do not skip steps)

## Step 1 — Read the file with the Read tool

Use the Read tool on `data/cards/kling_prompt.txt`. Do not rely on anything you
remember about the prompt from earlier in the conversation — the file is the
source of truth and memory drifts.

## Step 2 — Freshness check

The file's mtime must match today's date (same calendar day as the latest
`graded_at` in the DB). If it's stale, STOP and tell the user:

> "kling_prompt.txt is stale (last updated {mtime}). Run `python main.py grade --email` to regenerate."

Do NOT hand-author a prompt to fill the gap. Do NOT paste the stale file.

## Step 3 — DB cross-check (record sanity)

Run this to confirm the record in the file matches the DB:

```bash
PYTHONIOENCODING=utf-8 python -c "
import sqlite3
con = sqlite3.connect('data/betting_model.db')
r = con.execute('''SELECT SUM(CASE WHEN result=\"WIN\" THEN 1 ELSE 0 END),
       SUM(CASE WHEN result=\"LOSS\" THEN 1 ELSE 0 END), SUM(pnl_units)
FROM graded_bets WHERE DATE(created_at) >= \"2026-03-04\"
AND result NOT IN (\"DUPLICATE\",\"PENDING\",\"TAINTED\") AND units >= 3.5''').fetchone()
w, l, pnl = r
print(f'{w}-{l} +{pnl:.1f}u {round(w/(w+l)*100,1)}%')"
```

The printed `W-L` and `%` MUST appear in the file's final scene. If they
don't, STOP and report the mismatch. Do not "split the difference" or pick one.

## Step 4 — Pre-output checklist (MANDATORY — recite before pasting)

Before you output anything, confirm to yourself that the text you're about to
paste contains ALL of these verbatim from the file:

- [ ] Header line includes **"No voiceover — all text is on-screen only"**
- [ ] Scene 1 includes **"WELCOME TO SCOTTY'S EDGE"** as on-screen text
- [ ] Scene 1 includes tonight's record in the form `"W-L"` in bold white numerals
- [ ] Every middle scene (per-sport) is copied EXACTLY — same order, same athlete
      description, same spotlight color, same scoreboard label/record
- [ ] Final scene's season record matches Step 3's output exactly
- [ ] Final scene includes the season `%` next to the record
- [ ] Final "Style:" line is present verbatim

If any box fails, you've paraphrased. Re-read the file and try again.

## Step 5 — Output

Paste the full contents of `kling_prompt.txt` between `---` separators. Prefix
with one line only:

> "Here's today's prompt (DB-verified, {W}-{L} matches the grade):"

That's it. No summary. No scene-by-scene breakdown. No commentary. The user
copy-pastes directly into Sora.

# STYLISTIC EDITS (only if the user explicitly asks)

If the user says "change the spotlight color" or "make the batter a slugger" or
similar, edit ONLY that element. Preserve:

- The "No voiceover — all text is on-screen only" header
- The "WELCOME TO SCOTTY'S EDGE" on-screen text in Scene 1
- Every number (tonight's record, per-sport records, season record, %)
- Scene count and ordering
- The final "Style:" block

When in doubt, change less. Never touch numbers under any circumstances — those
come from the DB via the generator, never from you.

# FORMAT INVARIANTS (for reference when editing)

- Duration: **12 seconds** (Sora, NOT Kling — filename is legacy)
- Aspect: 9:16 vertical
- Audio: on-screen text only, NEVER voiceover
- Athletes: fully visible, well-lit, IN MOTION — not silhouettes, not static poses
- Spotlight colors: baseball = red/crimson, hockey = blue, soccer = golden,
  basketball = orange, tennis = white
- All numbers live on in-scene LED scoreboards

# ABSOLUTE DON'TS

- Do NOT write record numbers from memory or context
- Do NOT round or re-round the win %
- Do NOT invent a "projected" record
- Do NOT drop the Scene 1 "WELCOME TO SCOTTY'S EDGE" line
- Do NOT drop the "No voiceover" header
- Do NOT reorder scenes
- Do NOT paraphrase athlete descriptions
- Do NOT change "WIN"/"LOSS" labels when querying the DB
