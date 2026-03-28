Run a backtest against historical picks. Pass a sport name as argument, or omit for all sports.

Steps:
1. cd to C:\Users\carus\OneDrive\Desktop\scottys_edge\betting_model\scripts
2. Run: PYTHONIOENCODING=utf-8 python main.py backtest $ARGUMENTS
3. Summarize results: record, P/L, ROI, CLV performance
4. Compare to the current live performance from graded_bets
5. Flag any significant discrepancies between backtest and live results

If the user provided specific parameters to test (e.g., "backtest with friday adjustment at -0.3"), modify the relevant code temporarily, run the backtest, then revert the change and report the comparison.
