Deep-dive analysis of recent losses.

Steps:
1. Query the graded_bets table for the most recent day with losses:
   SELECT selection, sport, market_type, line, odds, edge_pct, confidence, units, pnl_units, closing_line, clv, context_factors, model_spread, timing, created_at
   FROM graded_bets WHERE result='LOSS' ORDER BY graded_at DESC LIMIT 20
2. For each loss, find the actual score from the results table
3. Analyze each loss:
   - Was CLV positive (good bet, bad luck) or negative (model wrong)?
   - Did the context factors make sense?
   - Was the line moving against us?
4. Look for patterns across the losses (same sport, same market type, same context factor)
5. Check if any toxic context factors are emerging (3+ bets, <45% win rate)
6. Present findings and recommend whether any changes are needed (but verify with historical data before recommending changes)

DB path: C:\Users\carus\OneDrive\Desktop\scottys_edge\betting_model\data\betting_model.db
Always use PYTHONIOENCODING=utf-8 for Python commands.
