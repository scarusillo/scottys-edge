"""One-shot script: send today's changes summary email."""
from emailer import send_html_email

plain = """Scotty's Edge - Today's Changes Summary (2026-04-20)

================================================
WHAT WE SHIPPED TODAY
================================================

v25.35 - SHARP_OPPOSES_BLOCK gate (NHL + NCAA Baseball)
  Blocks game-line picks where opener->current line moved against us.
  Backtest: +24.91u saved over 14 days.

v25.36 - SPREAD_FADE_FLIP (NBA + NHL)
  When DIVERGENCE_GATE triggers, bet the OPPOSITE side of our Elo model.
  5u stake, odds capped at +140.
  Backtest: NBA 38-18 (68%), NHL 17-4 (81%), combined +140u.

v25.37 - NBA PRA book-arb SHADOW mode
  Collects Points+Rebounds+Assists prop snapshots. Logs arb candidates
  to shadow_blocked_picks without firing. Promote at n=15 + 55% WR.

v25.38 - Concentration cap bug fix
  Props no longer populate the event-watchlist that blocks game-line
  picks. (Dosunmu prop was blocking DEN/MIN fade flip today.)

v25.39 - CONTEXT MODEL live (NHL + MLS + EPL)
  Separate module (context_model.py) layers 13+ real-world signals onto
  Elo: injuries, recent form, rest/B2B, tanking motivation, playoff HCA,
  injury amplification, series momentum, home/away splits, H2H regular
  season, extended form (last 20), pace, star concentration.
  When Elo diverges but Context brings adjusted spread back within
  threshold, fires DATA_SPREAD pick on Context's preferred side.

  Backtest:
    NHL: 14 picks, 11-3 (78.6%), +35.00u
    MLS: 5-0 (100%), +22.73u
    EPL: 2-0 (100%), +9.09u

  NBA explicitly excluded - Elo errors too large for Context adjustments
  to close (1 of 58 blocked picks unblocked).

================================================
HOW CONTEXT MODEL PHASES EVOLVED
================================================
Phase 1 (injury + recent form):           30% WR, -21u
Phase 2 (+rest/B2B + tanking motivation): 43% WR, -6u
Phase 3 (+playoff HCA + momentum):        33% WR (regressed)
Phase 4 (+home/away splits):              75% WR - breakthrough
Phase 5 (+H2H + ext form + pace + star):  73% WR multi-sport

================================================
OTHER WORK
================================================
- SQL injection patches on 2 backtest scripts
- Data model doc (graded_bets vs bets) in grader.py + CLAUDE.md
- Opener capture scheduled daily at 5:30am ET
  (catch-up-on-login enabled for weekends)
- Morning agent schedule updated (5:30/5:35/5:40am)

================================================
WHERE WE ARE NOW
================================================
Live gates/engines:
  - DIVERGENCE_GATE (Elo blocks)
  - SPREAD_FADE_FLIP (NBA + NHL) - unchanged, just gated by Context
  - DATA_SPREAD / Context Model (NHL + MLS + EPL) - NEW
  - PROP_FADE_FLIP (all prop sports)
  - PROP_BOOK_ARB (NBA + NHL + MLB pitcher K)
  - PRA Shadow (NBA combo props logging)
  - SHARP_OPPOSES_BLOCK (NHL + NCAA Baseball)

================================================
WHAT'S NEXT
================================================
1. NBA spread model redesign - replace Elo baseline with market_median.
   Only way to recover NBA spread picks.
2. Confirmed lineup scrape - ESPN pre-game boxscore. Catches late
   scratches our injury table misses.
3. Derivative markets (1st half, team totals) - less efficient.
4. Weight tuning after 2-3 weeks of live Context data.

Commits: 459b3b1 (Context Model), 3beed64 (cap fix), c06836b (fade flip),
ce985c6 (block gate), 9b435d5 (PRA shadow)
"""

html = """<html><body style='font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;line-height:1.5;color:#222;max-width:720px;padding:20px'>
<h1 style='color:#0a5'>Scotty's Edge - 2026-04-20 Changes Summary</h1>
<p style='font-size:14px;color:#666'>Saved for re-read. All commits pushed to main.</p>

<h2>What Shipped Today</h2>
<table cellpadding=6 style='border-collapse:collapse;font-size:14px' border=1 bordercolor='#ddd'>
<tr style='background:#f0f0f0'><th>Version</th><th>What</th><th>Backtest</th></tr>
<tr><td><b>v25.35</b></td><td>SHARP_OPPOSES_BLOCK (NHL + NCAA Baseball)</td><td>+24.91u / 14d</td></tr>
<tr><td><b>v25.36</b></td><td>SPREAD_FADE_FLIP (NBA + NHL) - fade our Elo model</td><td>+140u / 14d</td></tr>
<tr><td><b>v25.37</b></td><td>NBA PRA book-arb SHADOW mode</td><td>Pending data</td></tr>
<tr><td><b>v25.38</b></td><td>Concentration cap bug fix (props no longer block game lines)</td><td>Unblocked fade flip</td></tr>
<tr><td><b>v25.39</b></td><td>CONTEXT MODEL LIVE (NHL + MLS + EPL)</td><td>+66.82u / 14d, 85.7%</td></tr>
</table>

<h2>Context Model - Phase-by-phase progression</h2>
<table cellpadding=6 style='border-collapse:collapse;font-size:14px' border=1 bordercolor='#ddd'>
<tr style='background:#f0f0f0'><th>Phase</th><th>Signals Added</th><th>Result</th></tr>
<tr><td>1</td><td>Injury point_impact + last-5 form delta</td><td>30% WR, -21u</td></tr>
<tr><td>2</td><td>+ rest/B2B (-2.5 pts) + tanking (-3.0)</td><td>43% WR, -6u</td></tr>
<tr><td>3</td><td>+ playoff HCA (+1.0) + injury amp (x1.3) + series momentum</td><td>33% WR (regressed)</td></tr>
<tr><td>4</td><td>+ <b>team-specific home/away splits</b></td><td><b>75% WR - breakthrough</b></td></tr>
<tr><td>5</td><td>+ H2H reg-season + last-20 form + pace + star concentration</td><td>73% multi-sport / 78.6% NHL</td></tr>
</table>

<h2>Live Backtest (Context Model, 14 days)</h2>
<table cellpadding=6 style='border-collapse:collapse;font-size:14px' border=1 bordercolor='#ddd'>
<tr style='background:#f0f0f0'><th>Sport</th><th>Picks</th><th>Record</th><th>Win%</th><th>P/L</th></tr>
<tr><td><b>NHL</b></td><td>14</td><td>11-3</td><td>78.6%</td><td style='color:green'>+35.00u</td></tr>
<tr><td>MLS</td><td>5</td><td>5-0</td><td>100%</td><td style='color:green'>+22.73u</td></tr>
<tr><td>EPL</td><td>2</td><td>2-0</td><td>100%</td><td style='color:green'>+9.09u</td></tr>
<tr><td>NBA</td><td colspan=4 style='color:#999'>Excluded - Elo errors too large to fix with adjustments</td></tr>
</table>

<h2>What Each Engine Finds</h2>
<ul>
<li><b>DIVERGENCE_GATE:</b> Blocks picks where Elo disagrees with market by > threshold. Bad signal in; we don't bet.</li>
<li><b>SPREAD_FADE_FLIP:</b> When Elo diverges on NBA/NHL, we bet the OPPOSITE side - our model is wrong ~70% of the time on big disagreements.</li>
<li><b>Context Model / DATA_SPREAD:</b> When Elo diverges but Context's adjusted projection agrees with market, we know the disagreement was explainable (injury, form, etc). Bet Context's preferred side.</li>
<li><b>Fade + Context coexistence:</b> On NHL games, Context fires FIRST. If Context doesn't unblock, fade flip fires as fallback. They target different games.</li>
</ul>

<h2>Where We Need to Go Next</h2>
<ol>
<li><b>NBA spread model redesign</b> - replace Elo baseline with market_median. Only way to recover NBA spread picks.</li>
<li><b>Confirmed lineup scrape</b> - ESPN pre-game boxscore. Catches late scratches our injury table misses.</li>
<li><b>Derivative markets</b> - 1st half / team totals - less efficient than main lines.</li>
<li><b>Weight tuning</b> - after 2-3 weeks of live Context data, re-fit Phase weights against real outcomes.</li>
</ol>

<p style='color:#666;font-size:12px;margin-top:24px'>Commits: 459b3b1 (Context Model) | 3beed64 (cap fix) | c06836b (fade flip) | ce985c6 (block gate) | 9b435d5 (PRA shadow)</p>
</body></html>"""

ok = send_html_email("Scotty's Edge - 2026-04-20 Changes Summary", plain, html)
print('Email sent' if ok else 'Email FAILED')
