# Scotty's Edge — Master Agent To-Do List
**Last updated:** 2026-04-20 — post v25.35 / v25.36 / v25.37 ship session

---

## 🔴 OPEN CRITICAL — TOP PRIORITY

### 1. Secondary spread model (data-driven) — coexist with Elo model + fade flip

**Concept:** Keep the existing Elo-based spread model AS-IS. It's our "divergence
detector" — its wrongness feeds `SPREAD_FADE_FLIP` (+140u backtest). Build a
SECOND spread model alongside it that uses real inputs to find genuine spread
edges the Elo model misses.

**Why not replace Elo:**
- Fade flip is actively printing money because Elo is broken in playoffs
- Replacing Elo would kill that edge for an unproven new model
- Both can coexist — different games get picked by different engines

**Proposed architecture:**
```
For each game:
    elo_spread      = existing Elo-based projection
    data_spread     = NEW model (injuries, lineup, rest, motivation, H2H, form)
    market_spread   = best market line

    Path 1 — FADE_FLIP (keep):
        IF |elo_spread - market_spread| > max_div:
            SPREAD_FADE_FLIP fires  (opposite side of Elo)

    Path 2 — DATA-DRIVEN PICKS (new):
        data_edge = (data_model vs market) at best book
        IF data_edge >= 20% AND |data_spread - market_spread| < max_div:
            Fire own-pick at market line  (real edge)

    Path 3 — BOTH models agree AND market disagrees (rare, high conviction):
        Stake boost (+1u) or fire at lower edge threshold
```

**Data inputs to build (ranked by impact):**
| Input | Source | Complexity | Impact |
|-------|--------|------------|--------|
| Injury list (starters out) | ESPN injuries API | Medium | 🔥 Biggest single win |
| Confirmed starting lineup | ESPN boxscore pre-game | Medium | Captures rest decisions |
| Rest days / back-to-back | Schedule data (have) | Easy | 1-2 pts per B2B |
| Motivation (seeding/tanking/elim) | Standings + rules | Hard | Big playoff impact |
| Recent form vs season avg | Existing game_results | Easy | Hot/cold streaks |
| H2H history | Existing game_results | Easy | Matchup-specific |

**Integration points:**
- `context_engine.py` already has `spread_adj` infrastructure — add new adjustment types
- Each input contributes spread_adj delta applied to `ms` (e.g., star out → -5 pts)
- Keep elo_spread computation untouched (for fade flip continuity)
- Add `data_spread` as a parallel output used for Path 2 picks

**Scope:** 2-4 weeks of evening work. Most time is API plumbing + player name normalization. Model changes are small.

**When to start:**
- Wait 3-4 weeks to let SPREAD_FADE_FLIP mature (2026-04-20 → 2026-05-11 minimum)
- If fade flip win rate drops below 55% before then → start earlier
- If fade flip holds 60%+ → start 2026-05-11 as planned

**Success metric:** Data-driven picks produce +20u+ over a 2-week backtest on historical spreads where fade flip didn't fire (i.e., games inside `max_div` threshold).

---

## 🟡 OTHER OPEN

### Minimum line-stability time gate for BOOK_ARB (long-term)

Late-posted NCAA baseball lines can produce "opener gap 3.0" signals that
are actually just books converging on a freshly-posted market, not real
asymmetric information. Scanner fires on the stale/generous soft line,
then books converge within 30-60 min and the arb closes before the user
places the bet.

**Example (2026-04-20):** UC Santa Barbara @ Cal Baptist UNDER 13.5
fired at 3:13 PM EDT after lines posted at 3:00 PM EDT. FD opened at
10.5, DK at 13.5, gap 3.0. Within ~30 minutes FD moved to 13.5 — the
arb had closed by the time the user read the email.

**Proposed fix:** require BOOK_ARB to see at least 60 minutes of
stable line data before firing. Implementation:
- Track `first_seen_timestamp` for each book's opener in `openers` table
- Before firing BOOK_ARB, verify `(now - first_seen) > 60 min`
- If not, skip and log to shadow_blocked_picks with reason `BOOK_ARB_LINE_UNSETTLED`

**Expected impact:** eliminates the "just-opened market" failure mode.
Sharper books catch legitimate mid-day arbs; filters freshly-posted
NCAA baseball where books haven't converged yet.

**Risk:** if we only fire after 60 min stability, we miss the window
when both books are settled AND the gap still exists (rare but real).
Could tune to 30 min if 60 is too conservative.

**Start trigger:** after 1-2 more similar close-too-fast BOOK_ARB fires
are observed. Current v25.25/26/27 logic works for stable mid-day NCAA
baseball markets; this is a post-v25.25 patch.

---

Other monitors unchanged from pre-session list.

## ✅ COMPLETED (2026-04-11)

- [x] **Retasked all 15 hourly `ScottysEdge_XXAM/PM` scheduled tasks to use `auto_run_afternoon.bat` wrapper** — verified via `schtasks /Query`. All 15 tasks (06AM through 08PM) now point at the bat wrapper instead of calling `python.exe` directly. Every future run will capture both stdout and stderr to `auto_run.log`, and combined with v25.9's uncaught exception handler, the pipeline is now completely observable — no more silent failures.

## ✅ RESOLVED THIS SESSION (2026-04-11)

### Infrastructure + Observability
- [x] **db-latest upload pipeline** — fixed `upload_db.py` with `--draft=false` so cloud agents can fetch the DB
- [x] **`Retention pruning: timedelta not defined` silent bug** — fixed cmd_grade timedelta import (v25.7)
- [x] **ESPN backfill DB connection leak** — wrapped `backfill_missing` + `backfill_thin_teams` in try/finally (v25.7)
- [x] **Pipeline observability** — uncaught exception handler added to `main.py` (v25.9). Any future crash writes full traceback to `pipeline.log`.
- [x] **🔥 `_social_media_card` NameError** — **ROOT CAUSE of today's silent pipeline failures.** One undefined variable `tu` in an unwrapped code block killed every "has picks" scheduled run. Fixed in v25.10.

### Briefing + Stats Card Fixes
- [x] **Shadowed factor filter** — briefing stops showing phantom alerts (v25.7)
- [x] **Tennis lumping** — single "TENNIS" row in both stats card and morning briefing (v25.7 + v25.8)

### Twitter Cleanup Residue
- [x] `#GamblingTwitter` hashtag removed (v25.7)
- [x] 152-line `generate_thread()` dead function removed (v25.7)
- [x] X handle dropped from `SOCIALS` constant (v25.8)

### Dead Code / Unused Imports
- [x] `home_away_split_adjustment` removed from `context_engine.py` (v25.7)
- [x] `calculate_true_vig` removed from `scottys_edge.py` (v25.7)
- [x] 9 unused imports purged across 7 files (v25.7)

### Pre-Run Validator Agent Prompt Fixes
- [x] Removed phantom gate checks: `CONCENTRATION_CAP`, `MIN_EDGE`, `BLOWOUT_GATE` (zero-block false alarm), tennis `clay`
- [x] Agent now queries real gate names: `DIRECTION_CAP`, `SPORT_CAP`, `GAME_CAP`, `CROSS_RUN_CAP`, etc.
- [x] Saved `reference_gate_reason_strings.md` to memory so this discovery persists

### Apr 6–10 Drawdown Post-Mortem
- [x] Investigated, traced all -26.4u of losses to v24-v25.3 fixes already shipped. Net new action items: zero.

### Same-Day Rescues
- [x] Today's 10 picks manually posted to IG story (both card slides)
- [x] Glossary carousel posted to IG feed (pinnable — 2 slides)
- [x] Picks email sent manually (subject: "🎯 RESCUE — Morning Picks — 2026-04-11")
- [x] Daily Reel posted to IG with 6-2 +13.4u recap

## 🟡 Today-Specific (check if still relevant)

- [x] **TOR/ATL power ratings sanity check** — stale, resolved 2026-04-18. Picks fired on these teams post-4/11 went 2-1 (+3.4u); model calibration is fine.
- [x] **`pitcher_scraper` Imai classification** — stale, resolved 2026-04-18. Imai has only 7.5 IP total across 3 starts — well below the 30-IP ERA gate, so he can't affect model calculations. Original note confirmed "not a bug."

## 🟢 Monitor / Backlog

- [ ] **Daily check:** Home letdown spot 2nd-half P/L (currently -9.1u, threshold -15u, cushion 5.9u)
- [ ] H2H high-scoring factor — 1W-2L, -5.3u (watch for accumulation)
- [ ] Chase Field park double-counting — 1W-3L, -10.4u (already reduced from +0.9 → +0.6 in v23.1)
- [ ] Add tennis surface-specialist Elo override (deferred until tennis returns — clay tightening in v24 may already be sufficient)
- [x] **v24 two-week review** — completed Apr 18 (2 days early). Verdict: successful. CLV +0.21 → +0.41; record -18u is variance on 88 bets (CI [39.6%, 60.4%] includes pre-v24 56.8%). See `project_v24_review_apr18.md`. Next review: May 2.

### Cleanups completed 2026-04-18

- [x] **`config.py` SPORT_CONFIG drift** — removed dead-code copy. Runtime uses `model_engine.py` only; verified all live imports.
- [x] **Drop `prop_snapshots_pre_v25_2`** — already dropped in a prior session, confirmed via DB scan.
- [x] **Verify `model_total` writing to `market_consensus`** — column exists but 0/10,499 rows populated. Not a bug (nothing reads it), but documented: no live writer, low-priority cleanup if we ever reclaim the column.

### Added 2026-04-18 (moved from in-session task list — passive, sample-gated)

- [ ] **NBA props sample** — currently 3W-7L, -17.1u, avg CLV +1.32. Flag if WR stays &lt;40% through 20 picks. Trigger: n ≥ 20.
- [ ] **NCAA Baseball NO_MOVEMENT signal** — 1W-5L, -20.6u when sharps don't move the line. Possible fade candidate. Trigger: re-check at n ≥ 15 (currently n=6).
- [ ] **NBA SHARP_CONFIRMS stake-boost decision** — zero picks fired with this signal yet. Per `project_steam_monitor.md`, decision point at 15-20 picks.
- [ ] **NBA model calibration review** — 28W-27L, -7.7u season-to-date despite positive CLV. Audit recommends full calibration review at the May checkpoint.
- [ ] **ATP tennis surface/round model** — Monte Carlo closed 1-3, -10.7u (n=4, small sample but early indicator). Review surface-specific calibration and round-filter before the next clay event.

## 📝 Do NOT Act On (stale phantom alerts)

Morning briefing items referencing already-shadowed factors:
- Home fast-paced (shadowed v21)
- Away bounce-back (shadowed v21)
- Altitude (shadowed v21)
- Home hot streak (shadowed v21)
- Away revenge (shadowed v21)
- Away letdown (shadowed v24)
- Friday game (resolved — was driven by NCAA UNDERs that are now blocked)

These are ghost alerts from historical data. The briefing-script suppression fix (v25.7) prevents new phantom alerts going forward, but older briefings may still reference these.

---

## Commits This Session (2026-04-11)

- `6365678` **v25.7** — Audit cleanup (8 items: db-latest, timedelta, ESPN try/finally, tennis lump, Twitter cleanup, dead code, unused imports, shadowed factor filter)
- `bec00c6` **v25.8** — Lump tennis tournaments in morning briefing + drop X from socials
- `a584761` **v25.9** — Pipeline observability (main.py uncaught exception handler)
- `580fd33` **v25.10** — 🔥 CRITICAL FIX: `_social_media_card` NameError `tu` (root cause of pipeline failures)
