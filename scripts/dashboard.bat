@echo off
rem Opens the Streamlit dashboard (runs on Windows, no Spark needed): http://localhost:8501
rem Started from option 5 of BuyBox_Analytics.bat; works when double-clicked too.
title BuyBox Analytics - dashboard
cd /d "%~dp0.."
if not exist ".venv\Scripts\python.exe" (
  echo Creating Python environment for the dashboard ...
  python -m venv .venv
  .venv\Scripts\pip install -r dashboard\requirements.txt
)
.venv\Scripts\python -m streamlit run dashboard\app.py --server.headless false
pause
