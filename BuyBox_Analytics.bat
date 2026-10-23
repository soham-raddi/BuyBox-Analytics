@echo off
setlocal
title BuyBox Analytics
cd /d "%~dp0"

:menu
cls
echo.
echo   =====================================================
echo      BuyBox Analytics - Big Data Analytics Mini Project
echo   =====================================================
echo.
echo     1. Live demo from scratch  (step by step, pauses between steps)
echo     2. Run the full pipeline   (no pauses, about 17 minutes)
echo     3. HDFS demo               (commands + HDFS web UI)
echo     4. Spark demo              (Spark jobs + Spark web UI)
echo     5. Open the dashboard
echo     6. Stop Hadoop and Cassandra
echo     0. Exit
echo.
set "choice="
set /p "choice=   Choose an option: "
if "%choice%"=="1" goto demo
if "%choice%"=="2" goto pipeline
if "%choice%"=="3" goto hdfs
if "%choice%"=="4" goto spark
if "%choice%"=="5" goto dashboard
if "%choice%"=="6" goto stop
if "%choice%"=="0" goto :eof
goto menu

:demo
echo.
set "quick="
set /p "quick=   Quick mode with a 20%% sample (about 10 min instead of 17)? [y/N]: "
set "mode="
if /i "%quick%"=="y" set "mode=quick"
wsl -d Ubuntu-24.04 --cd "%~dp0." -- bash scripts/live_demo.sh %mode%
start "BuyBox Analytics - dashboard" "%~dp0scripts\dashboard.bat"
goto menu

:pipeline
wsl -d Ubuntu-24.04 --cd "%~dp0." -- bash run_all.sh
pause
goto menu

:hdfs
wsl -d Ubuntu-24.04 --cd "%~dp0." -- bash scripts/hdfs_demo.sh
goto menu

:spark
wsl -d Ubuntu-24.04 --cd "%~dp0." -- bash -c "source scripts/env.sh && bash scripts/start_hadoop.sh && python scripts/spark_demo.py"
goto menu

:dashboard
start "BuyBox Analytics - dashboard" "%~dp0scripts\dashboard.bat"
goto menu

:stop
wsl -d Ubuntu-24.04 --cd "%~dp0." -- bash scripts/stop_services.sh
pause
goto menu
