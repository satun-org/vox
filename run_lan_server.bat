@echo off
chcp 65001 >nul
title VOX LAN Server - offline backup
cd /d "%~dp0"

echo ==================================================
echo   VOX LAN Server  (offline backup / LAN mode)
echo   Start the server, then open the URL it prints
echo   from any phone on the SAME Wi-Fi network.
echo ==================================================
echo.

where python >nul 2>nul
if not errorlevel 1 goto run_python
where py >nul 2>nul
if not errorlevel 1 goto run_py_launcher

echo  [X] Python not found.
echo       Install Python 3.8+ from https://www.python.org/downloads/
echo       (tick "Add python.exe to PATH" during install)
echo.
pause
exit /b 1

:run_python
python lan_server.py %*
goto finished

:run_py_launcher
py -3 lan_server.py %*
goto finished

:finished
echo.
echo  Server stopped. Press any key to close this window.
pause >nul