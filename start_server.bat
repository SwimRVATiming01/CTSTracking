@echo off
title CTS Tracker Server
echo ============================================================
echo  CTS Tracker Server
echo ============================================================
echo.
cd /d "%~dp0"
echo Starting server...
echo.
echo To stop the server press CTRL+C
echo.
python cts_tracker.py
echo.
echo ============================================================
echo  Server stopped.
echo ============================================================
pause
