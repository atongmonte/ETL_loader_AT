@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PYTHON_EXE=%SCRIPT_DIR%.venv\Scripts\python.exe"
set "LOADER_SCRIPT=%SCRIPT_DIR%main_loader.py"
set "CONFIG_FILE=%SCRIPT_DIR%etl_config.yaml"

if not exist "%PYTHON_EXE%" (
    echo ERROR: Cardinal virtual environment was not found: "%PYTHON_EXE%" 1>&2
    exit /b 3
)

if not exist "%LOADER_SCRIPT%" (
    echo ERROR: Cardinal loader was not found: "%LOADER_SCRIPT%" 1>&2
    exit /b 3
)

if not exist "%CONFIG_FILE%" (
    echo ERROR: Cardinal configuration was not found: "%CONFIG_FILE%" 1>&2
    exit /b 3
)

pushd "%SCRIPT_DIR%" || exit /b 3
"%PYTHON_EXE%" "%LOADER_SCRIPT%" --config "%CONFIG_FILE%" %*
set "EXIT_CODE=%ERRORLEVEL%"
popd

exit /b %EXIT_CODE%
