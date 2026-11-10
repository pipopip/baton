@echo off
rem Rewritten with the selected Python interpreter by bt install.
python -S "%~dp0hook.py" %1
exit /b 0
