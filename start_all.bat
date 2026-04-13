@echo off
setlocal
cd /d %~dp0
powershell -ExecutionPolicy Bypass -File "%~dp0start_all.ps1"
endlocal
