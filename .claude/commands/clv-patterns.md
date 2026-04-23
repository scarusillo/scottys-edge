Analyze CLV patterns across graded bets and surface actionable cohorts.

Steps:
1. Run the tracker to refresh the report:
   `PYTHONIOENCODING=utf-8 python scripts/clv_patterns.py --min-n 10`
2. Read the generated report at `data/clv_patterns_report.md`.
3. Present a concise summary (tables, not prose) covering:
   - **Baseline** — current POS/ZERO/NEG/NULL CLV bucket win rates + P/L. Confirms the CLV→win signal is still holding.
   - **⭐ Top boost candidates** — top 3-5 cohorts with avg CLV ≥ +0.5 AND n ≥ 20. Call out any new ones that appeared since last run.
   - **🚩 Gate candidates** — any cohort with avg CLV ≤ -0.3 AND n ≥ 20. These deserve immediate attention; propose a backtest before any gate ships.
   - **Anomalies** — cohorts with positive avg CLV but NEGATIVE P/L (right-side wrong-result). List them; these are juice-drag or variance candidates, not CLV bugs.
   - **DK vs. FD/BR routing health** — one-line status on whether DraftKings book routing is still dragging on any sport × market slice.
4. End with one clear question: "Want me to backtest any of these cohorts before the next pipeline run?"

Report flags:
- ⭐ = avg CLV ≥ +0.5, n ≥ 20 (boost candidate)
- 🚩 = avg CLV ≤ -0.3, n ≥ 20 (gate candidate)

Do NOT:
- Ship any gate or stake change without a backtest — the report is proposals only.
- Overstate thin samples (n < 15 is noise; don't recommend action on those).
- Duplicate the Factor Health section from `data/morning_briefing.md` — this is about CLV, not P/L-by-factor.

Useful flags:
- `--days 30` — restrict to last 30 days only (trend vs full history)
- `--min-n 20` — raise significance floor for a tighter view
- `--out -` — stdout only, skip the file write

DB path: `data/betting_model.db`. Always run Python with `PYTHONIOENCODING=utf-8`.
