@echo off
REM ─────────────────────────────────────────────────────────────────────────────
REM Gmail Scraper — nightly batch wrapper
REM Scheduled via Windows Task Scheduler at 11:00 PM daily.
REM
REM Setup:
REM   1. Edit PROJ_DIR to match your installation path
REM   2. Edit VENV_PYTHON to match your venv python path
REM   3. In Task Scheduler:
REM        Action:  cmd /c "C:\path\to\scheduled_gmail_scraper.bat"
REM        Start in: (leave blank — script sets its own working dir)
REM        Run whether user is logged on or not
REM        Run with highest privileges: No (not needed)
REM ─────────────────────────────────────────────────────────────────────────────

REM ── Configuration ────────────────────────────────────────────────────────────
set PROJ_DIR=C:\Users\admin\Desktop\Projects\ai-projects\gmail-scraper
set VENV_PYTHON=%PROJ_DIR%\venv\Scripts\python.exe
set LOG_DIR=%PROJ_DIR%\logs

REM ── Timestamp for log file name ───────────────────────────────────────────────
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set DT=%%I
set TIMESTAMP=%DT:~0,8%_%DT:~8,6%

REM ── Ensure logs directory exists ─────────────────────────────────────────────
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM ── Change to project directory ───────────────────────────────────────────────
cd /d "%PROJ_DIR%"

REM ── Run scraper ───────────────────────────────────────────────────────────────
echo [%date% %time%] Starting Gmail scraper >> "%LOG_DIR%\scraper_%TIMESTAMP%.log" 2>&1
"%VENV_PYTHON%" -u main.py >> "%LOG_DIR%\scraper_%TIMESTAMP%.log" 2>&1
set SCRAPER_EXIT=%ERRORLEVEL%

echo [%date% %time%] Scraper finished with exit code %SCRAPER_EXIT% >> "%LOG_DIR%\scraper_%TIMESTAMP%.log" 2>&1

if %SCRAPER_EXIT% NEQ 0 (
    echo [%date% %time%] Scraper FAILED - check log for details >> "%LOG_DIR%\scraper_%TIMESTAMP%.log" 2>&1
    exit /b %SCRAPER_EXIT%
)

echo [%date% %time%] Scraper completed successfully >> "%LOG_DIR%\scraper_%TIMESTAMP%.log" 2>&1
exit /b 0
