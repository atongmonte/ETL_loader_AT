@echo off
setlocal

:: -----------------------------------------------------------------------------
:: Configuration
:: -----------------------------------------------------------------------------
set "SCRIPT_DIR=%~dp0"
set "PYTHON_EXE=%SCRIPT_DIR%.venv\Scripts\python.exe"
set "LOADER_SCRIPT=%SCRIPT_DIR%main_loader.py"
set "CONFIG_FILE=%SCRIPT_DIR%etl_config.yaml"

:: Single persistent log file, trimmed when it exceeds 5 MB.
set "LOG_DIR=%SCRIPT_DIR%logs"
set "LOGFILE=%LOG_DIR%\Cardinal_ETL.log"
set "MAX_LOG_BYTES=5242880"
set "KEEP_LOG_LINES=1000"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:: -----------------------------------------------------------------------------
:: Trim an oversized log while retaining its most recent lines.
:: -----------------------------------------------------------------------------
if exist "%LOGFILE%" (
    for %%A in ("%LOGFILE%") do if %%~zA GTR %MAX_LOG_BYTES% (
        powershell.exe -NoProfile -Command "Get-Content -LiteralPath '%LOGFILE%' -Tail %KEEP_LOG_LINES% | Set-Content -LiteralPath '%LOGFILE%.tmp' -Encoding UTF8" >nul 2>&1
        if exist "%LOGFILE%.tmp" (
            move /y "%LOGFILE%.tmp" "%LOGFILE%" >nul 2>&1
            echo [%date% %time%] --- log truncated to the last %KEEP_LOG_LINES% lines because it exceeded 5 MB --- >> "%LOGFILE%"
        )
    )
)

:: -----------------------------------------------------------------------------
:: Start logging.
:: -----------------------------------------------------------------------------
echo. >> "%LOGFILE%"
echo ============================================================ >> "%LOGFILE%"
echo [%date% %time%] STARTING Cardinal ETL run >> "%LOGFILE%"
echo Working directory: %SCRIPT_DIR% >> "%LOGFILE%"
echo Python executable: %PYTHON_EXE% >> "%LOGFILE%"
echo Loader script:     %LOADER_SCRIPT% >> "%LOGFILE%"
echo Configuration:     %CONFIG_FILE% >> "%LOGFILE%"
echo Log file:          %LOGFILE% >> "%LOGFILE%"
echo ============================================================ >> "%LOGFILE%"
echo. >> "%LOGFILE%"

:: -----------------------------------------------------------------------------
:: Validate required files.
:: -----------------------------------------------------------------------------
if not exist "%PYTHON_EXE%" (
    echo [%date% %time%] ERROR: Cardinal virtual environment was not found: "%PYTHON_EXE%" >> "%LOGFILE%"
    set "EXIT_CODE=3"
    goto :FINISH_ERROR
)

if not exist "%LOADER_SCRIPT%" (
    echo [%date% %time%] ERROR: Cardinal loader was not found: "%LOADER_SCRIPT%" >> "%LOGFILE%"
    set "EXIT_CODE=3"
    goto :FINISH_ERROR
)

if not exist "%CONFIG_FILE%" (
    echo [%date% %time%] ERROR: Cardinal configuration was not found: "%CONFIG_FILE%" >> "%LOGFILE%"
    set "EXIT_CODE=3"
    goto :FINISH_ERROR
)

:: -----------------------------------------------------------------------------
:: Run the loader and capture both stdout and stderr.
:: -----------------------------------------------------------------------------
pushd "%SCRIPT_DIR%"
if errorlevel 1 (
    echo [%date% %time%] ERROR: Could not change to working directory: "%SCRIPT_DIR%" >> "%LOGFILE%"
    set "EXIT_CODE=3"
    goto :FINISH_ERROR
)

echo [%date% %time%] Launching Cardinal ETL loader... >> "%LOGFILE%"
echo. >> "%LOGFILE%"

"%PYTHON_EXE%" "%LOADER_SCRIPT%" --config "%CONFIG_FILE%" %* >> "%LOGFILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

popd

if "%EXIT_CODE%"=="0" (set "RUN_STATUS=SUCCESS") else (set "RUN_STATUS=FAILURE")

echo. >> "%LOGFILE%"
echo ------------------------------------------------------------ >> "%LOGFILE%"
echo [%date% %time%] FINISHED - exit code = %EXIT_CODE% >> "%LOGFILE%"
echo [%date% %time%] RUN_STATUS: %RUN_STATUS% (exit code %EXIT_CODE%) >> "%LOGFILE%"
echo ------------------------------------------------------------ >> "%LOGFILE%"
goto :FINISH

:FINISH_ERROR
echo. >> "%LOGFILE%"
echo ------------------------------------------------------------ >> "%LOGFILE%"
echo [%date% %time%] RUN_STATUS: FAILURE (exit code %EXIT_CODE%) >> "%LOGFILE%"
echo ------------------------------------------------------------ >> "%LOGFILE%"

:FINISH
echo [%date% %time%] Batch wrapper completed >> "%LOGFILE%"
endlocal & exit /b %EXIT_CODE%
