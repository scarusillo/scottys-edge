@echo off
cd /d C:\Users\carus\OneDrive\Desktop\scottys_edge\betting_model\scripts
set PYTHONIOENCODING=utf-8
echo ============================================ >> ..\data\auto_run.log
echo   ODDS MONITOR DAEMON START %DATE% %TIME% >> ..\data\auto_run.log
echo ============================================ >> ..\data\auto_run.log
python agent_odds_monitor.py --daemon >> ..\data\odds_monitor.log 2>&1
