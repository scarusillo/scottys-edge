# Scotty's Edge — Master Agent To-Do List
**Last updated:** 2026-04-11 from Code Auditor + Morning Briefing + Pre-Run Validator + Pipeline Debug Session

---

## 🔴 OPEN CRITICAL

None. 🎉

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

- [ ] Sanity-check TOR + ATL power ratings if model fires picks on those teams today
- [ ] Audit `pitcher_scraper` for opener/bulk vs starter classification — Tatsuya Imai logged 0.1 IP as starter (decided: working as designed, not a bug)

## 🟢 Monitor / Backlog

- [ ] **Daily check:** Home letdown spot 2nd-half P/L (currently -9.1u, threshold -15u, cushion 5.9u)
- [ ] H2H high-scoring factor — 1W-2L, -5.3u (watch for accumulation)
- [ ] Chase Field park double-counting — 1W-3L, -10.4u (already reduced from +0.9 → +0.6 in v23.1)
- [ ] Add tennis surface-specialist Elo override (deferred until tennis returns — clay tightening in v24 may already be sufficient)
- [ ] **v24 two-week review** — due April 20, 2026. Validate all new gates with production data.

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
