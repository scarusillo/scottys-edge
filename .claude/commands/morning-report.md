Generate a comprehensive morning briefing.

Steps:
1. Run the grader if not already run today: PYTHONIOENCODING=utf-8 python main.py grade --email
2. Query overall performance stats from graded_bets
3. Check for any pending/ungraded bets
4. Report:
   - Yesterday's results (record, P/L)
   - Season-to-date record, P/L, ROI
   - Current streak
   - Last 10 trend
   - Today's schedule (what sports have games)
   - Any verification alerts or data issues
   - Top performing and worst performing context factors
5. Check the picks_log.jsonl for today's picks already generated
6. If picks exist, summarize today's card

DB path: C:\Users\carus\OneDrive\Desktop\scottys_edge\betting_model\data\betting_model.db
Always use PYTHONIOENCODING=utf-8 for Python commands.
