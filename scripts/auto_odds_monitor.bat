@echo off
cd /d C:\Users\carus\OneDrive\Desktop\scottys_edge\betting_model\scripts
set PYTHONIOENCODING=utf-8
SET "PYTHON_EXE=C:\Users\carus\AppData\Local\Python\pythoncore-3.14-64\python.exe"
echo ============================================ >> ..\data\auto_run.log
echo   ODDS MONITOR DAEMON START %DATE% %TIME% >> ..\data\auto_run.log
echo ============================================ >> ..\data\auto_run.log
"%PYTHON_EXE%" agent_odds_monitor.py --daemon >> ..\data\odds_monitor.log 2>&1
