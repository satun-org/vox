@echo off
chcp 65001 >nul
title VOX LAN Backup - Setup
cd /d "%~dp0"

echo ==================================================
echo   VOX LAN Backup - setup for this machine
echo   (offline backup server, same Wi-Fi network)
echo ==================================================
echo.

REM ---- 1) ต้องอยู่ในโฟลเดอร์โปรเจกต์ที่คัดลอกมาทั้งชุด ----
if exist "lan_server.py" if exist "index.html" goto files_ok
echo  [X] This folder is incomplete.
echo       Copy the WHOLE project folder to this machine first,
echo       then run this file from inside that folder.
echo.
pause
exit /b 1

:files_ok

REM ---- 2) มี Python ไหม ----
where python >nul 2>nul
if not errorlevel 1 goto have_python
where py >nul 2>nul
if not errorlevel 1 goto have_py_launcher
goto need_python

:have_python
set "PYCMD=python"
goto prepare

:have_py_launcher
set "PYCMD=py -3"
goto prepare

:need_python
echo  [!] Python is not installed on this machine.
echo      VOX needs Python 3.8+ to run the backup server.
echo.
where winget >nul 2>nul
if errorlevel 1 goto download_python
set "INSTALLPY="
set /p INSTALLPY=  Install Python now with winget? [y/n]
if /i not "%INSTALLPY%"=="y" goto download_python
echo.
winget install --id Python.Python.3.12 -e --accept-package-agreements --accept-source-agreements
echo.
echo  Close this window and run setup_lan_backup.bat again.
pause
exit /b 0

:download_python
echo  Open this page and install Python 3.8+:
echo      https://www.python.org/downloads/
echo  Tick "Add python.exe to PATH" during install,
echo  then run setup_lan_backup.bat again.
start "" "https://www.python.org/downloads/"
pause
exit /b 1

REM ---- 3) เตรียมระบบ: ตรวจไฟล์ + สร้างใบรับรอง ----
:prepare
echo  [OK] Python found:
%PYCMD% --version
echo.
echo  Preparing this machine (files + TLS certificate)...
echo.
%PYCMD% lan_server.py --setup
if errorlevel 1 goto failed

echo.
set "GO="
set /p GO=  Start the LAN server now? [y/n]
if /i "%GO%"=="y" call run_lan_server.bat %*

echo.
echo  Setup finished.
exit /b 0

:failed
echo.
echo  [X] Setup failed - read the messages above.
pause
exit /b 1