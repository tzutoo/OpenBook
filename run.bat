@echo off
cd /d "%~dp0"
echo Running OpenBook...
target\release\cli_ob.exe
pause
