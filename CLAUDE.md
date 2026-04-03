# Scotty's Edge Betting Model

## First Thing Every Session

At the start of every conversation, pull the latest code, check the morning briefing, and check for cloud agent GitHub Issues:
```bash
cd /c/Users/carus/OneDrive/Desktop/scottys_edge/betting_model && git pull && cat data/morning_briefing.md
```
Then check for agent reports (committed to the repo by cloud agents):
```bash
cat data/agent_morning_briefing.md 2>/dev/null | head -80
cat data/agent_code_audit.md 2>/dev/null | head -40
cat data/agent_line_movement.md 2>/dev/null | head -60
```
Summarize any action items from the briefing AND the agent reports before the user asks. The 3 cloud agents commit reports daily: Morning Briefing (5:30am), Code Auditor (6:00am), Line Movement Scout (4:00pm). Reports land via `git pull`.

## Quick Reference

```bash
# Always use this on Windows:
PYTHONIOENCODING=utf-8 python main.py <command> [flags]

# Common commands:
python main.py run --email          # Full pipeline: odds -> predict -> picks -> email + Discord
python main.py grade --email        # Grade yesterday's bets + CLV + performance report
python main.py opener --email       # Capture opening lines (8am)
python main.py predict              # Preview model output (free, no odds fetch)
python main.py backtest             # Backtest against historical results
python main.py report --days 7      # N-day performance report
python main.py run-soccer --email   # Soccer-only (weekend mornings)
python main.py snapshot             # Pre-game line snapshot for CLV
```

## Project Structure

```
betting_model/
  CLAUDE.md                         # This file
  data/
    betting_model.db                # Production SQLite DB (authoritative copy)
    picks_log.jsonl                 # JSONL pick log
    pipeline.log                    # Pipeline execution log
    auto_run.log                    # Scheduled task log
    cards/                          # Generated PNG pick cards
  scripts/                          # All executable code lives here
    main.py                         # CLI entry point — all commands dispatch from here
    model_engine.py                 # Prediction engine (spreads, totals, ML)
    scottys_edge.py                 # Core edge math (key numbers, Kelly, star system)
    config.py                       # Single source of truth for ALL thresholds/constants
    grader.py                       # Grade bets W/L/P + CLV analysis
    context_engine.py               # Contextual adjustments (rest, travel, pace, etc.)
    elo_engine.py                   # Elo ratings from game results
    odds_api.py                     # The Odds API interface
    emailer.py                      # Gmail SMTP (picks, grades, captions, alerts)
    social_media.py                 # Discord webhook + Twitter/X
    card_image.py                   # Instagram PNG cards (2160x2700, @2x retina)
    player_prop_model.py            # Own-projection prop engine (box score based)
    props_engine.py                 # Cross-book prop disagreement engine
    pitcher_scraper.py              # Baseball pitcher rotation + quality + DOW adj
    historical_scores.py            # ESPN scoreboard scraper (free)
    espn_team_scores.py             # ESPN team endpoint (fallback for missing scores)
    ncaa_scores.py                  # NCAA.com scraper (backup for college scores)
    weather_engine.py               # OpenWeatherMap for outdoor totals
    referee_engine.py               # Official tendency tracking
    line_tracker.py                 # Intraday line movement snapshots
    agent_verify.py                 # Grade integrity verification (6 checks)
    agent_analyst.py                # Post-grade morning briefing
    agent_sport_review.py           # Per-sport health cards
    agent_volume.py                 # Pick volume diagnostics
    agent_totals.py                 # Over/under model health
    agent_research.py               # Pre-run injury/lineup check
    agent_growth.py                 # Social media growth tracking
    agent_tournament.py             # NCAA tournament monitor
  .claude/
    commands/                       # Custom slash commands (/grade, /loss-analysis, etc.)
    settings.json                   # Hooks (notification on input needed)
```

## Database

SQLite at `data/betting_model.db`. Key tables:

| Table | Purpose |
|-------|---------|
| `bets` | All placed bets with full metadata |
| `graded_bets` | Graded bets with W/L/P result, CLV, context |
| `odds` | Raw odds snapshots (tagged OPENER/CURRENT/SNAPSHOT) |
| `market_consensus` | Best lines across books per event |
| `results` | Game results (scores, closing lines) |
| `power_ratings` | Model power ratings per team |
| `elo_ratings` | Elo ratings (sport, team, elo, games_played, sos) |
| `props` / `prop_snapshots` | Player prop odds |
| `player_results` | Actual player stat outcomes |
| `box_scores` | ESPN box score data for prop modeling |
| `pitcher_stats` | Pitcher stat lines |
| `team_pitching_quality` | Aggregate pitching quality by day-of-week |
| `injuries` | ESPN injury reports |
| `officials` | Referee assignments + tendencies |
| `soccer_standings` | League tables for motivation factors |
| `tennis_metadata` | Surface, round, set scores |

## Sports Supported

**Team:** NBA, NCAAB, NHL, MLB, NCAA Baseball
**Soccer:** EPL, Serie A, La Liga, Bundesliga, Ligue 1, UCL, MLS, Liga MX
**Individual:** Tennis (ATP + WTA) -- surface-split Elo, dynamic tournament detection

**Prop Sports:** NBA, NHL, MLB

## Key Thresholds (config.py)

- **Min edge to fire:** 8-15% depending on sport (sharp markets higher)
- **Kelly fraction:** 0.125 (1/8 Kelly)
- **Unit range:** 0.5 - 5.0 units
- **Context cap:** +/- 3.0 points total adjustment
- **Concentration cap:** Max 1 game-line pick per event (props exempt)
- **Market tiers:** SOFT (NCAAB, MLS, college baseball, tennis) vs SHARP (NBA, NHL, EPL, La Liga, MLB)
- **Merge caps:** Max 4 sharp + 10 soft picks per run; min 3.0u game lines, 2.0u props

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `ODDS_API_KEY` | Yes (or in DB `settings` table) | The Odds API |
| `GMAIL_APP_PASSWORD` | Yes for email | Gmail App Password |
| `DISCORD_WEBHOOK_URL` | Optional (has hardcoded default) | Discord webhook |
| `OPENWEATHER_API_KEY` | Optional | Weather adjustments |
| `PYTHONIOENCODING` | Set to `utf-8` | Prevents emoji crash on Windows |

## Important Gotchas

1. **Always `PYTHONIOENCODING=utf-8`** when running Python on Windows. Emojis in output crash cp1252.
2. **DB path:** All scripts use `data/betting_model.db` (not `scripts/`). There's a legacy copy in scripts/ — ignore it.
3. **ESPN groups=50:** Only works for basketball. Do NOT use for baseball — it filters to one conference.
4. **Concentration cap persists across runs:** `cmd_run` checks today's existing bets before saving.
5. **Tennis auto-detection:** Uses Odds API `/sports` endpoint to find active tournaments dynamically.
6. **Mid-day caption emails:** Only include NEW picks, not the full card.
7. **Never block a sport without root cause analysis.** User wants surgical fixes, not blanket disabling.
8. **Always backtest parameter changes** against all historical picks before recommending.
9. **Separate pre/post-rebuild results** when analyzing performance. Pre-rebuild losses are NOT the current model.
10. **NY legal books only** for recommendations: DraftKings, FanDuel, BetMGM, Caesars, BetRivers, ESPN BET, PointsBet, Fanatics.

## Daily Schedule (Windows Task Scheduler)

| Time | Command | Purpose |
|------|---------|---------|
| 8:00 AM | `opener --email` | Capture opening lines for CLV |
| 9:00 AM | `grade --email` | Grade yesterday + performance report |
| 7:00 AM Sat/Sun | `run-soccer --email` | Weekend European soccer |
| 6 AM - 8 PM (hourly) | `run --email` | 15 cycles/day, dedup filters old picks |

## External APIs

| API | Cost | Purpose |
|-----|------|---------|
| The Odds API | Paid (100k calls/mo) | Live odds, scores, props |
| ESPN (multiple endpoints) | Free | Scores, box scores, injuries, standings, refs |
| NCAA.com | Free | Backup college scores |
| Gmail SMTP | Free | All email delivery |
| Discord Webhooks | Free | Channel picks/results posting |
| OpenWeatherMap | Free (1000/day) | Outdoor totals adjustment |

## Dependencies

- Python 3.9+ (uses `zoneinfo`)
- Pillow (`pip install Pillow`) for PNG card generation
- No other external packages required
