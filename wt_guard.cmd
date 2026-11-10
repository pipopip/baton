@echo off
python -S "%~dp0wt_guard.py" claude
exit /b %errorlevel%
