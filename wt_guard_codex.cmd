@echo off
python -S "%~dp0wt_guard.py" codex
exit /b %errorlevel%
