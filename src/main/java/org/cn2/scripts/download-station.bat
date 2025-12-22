@echo off
setlocal EnableDelayedExpansion

set "BASE_URL=https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/"
set "LIST_FILE=file_station_list.txt"
set "OUTPUT_DIR=..\..\..\..\resources\data\station"
set "MAX_FILES=500"

if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

echo Fetching file list from %BASE_URL%...

:: Use PowerShell to scrape the file list because valid parsing needs regex
powershell -Command "$web = Invoke-WebRequest -Uri '%BASE_URL%'; $regex = 'href=\"([^\"]+\.csv\.gz)\"'; [regex]::Matches($web.Content, $regex) | Select-Object -First %MAX_FILES% | ForEach-Object { '%BASE_URL%' + $_.Groups[1].Value } | Out-File -Encoding ascii '%LIST_FILE%'"

:: Check if file list was created and has content
for %%A in ("%LIST_FILE%") do if %%~zA==0 (
    echo Error: No files found or network issue.
    goto :EOF
)

echo Found files. Downloading to %OUTPUT_DIR%...

:: Check if aria2c exists
where aria2c >nul 2>nul
if %errorlevel% neq 0 (
    echo Error: aria2c is not installed or not in PATH.
    echo Please install aria2c or use the provided java downloader.
    exit /b 1
)

:: Run aria2c
aria2c -i "%LIST_FILE%" ^
       -x16 -s16 -j4 ^
       -d "%OUTPUT_DIR%" ^
       --continue=true ^
       --file-allocation=none

echo Download complete.
endlocal
