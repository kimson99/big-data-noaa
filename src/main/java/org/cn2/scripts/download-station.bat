@echo off
setlocal

set "BASE_URL=https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/"
set "LIST_FILE=file_station_list.txt"
set "OUTPUT_DIR=..\..\..\..\resources\data\station"
set "MAX_FILES=500"

if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

echo Fetching file list from %BASE_URL%...

:: 🔥 DÒNG DUY NHẤT — không ngắt, không ^, không lỗi parse
powershell -NoProfile -ExecutionPolicy Bypass -Command "$u='%BASE_URL%';$w=Invoke-WebRequest -Uri $u -UseBasicParsing;$r='href=\"([^\"]+\.csv\.gz)\"';$m=[regex]::Matches($w.Content,$r)|Select-Object -First %MAX_FILES%;if(!$m){exit 1};$m|%%{$u+$_.Groups[1].Value}|Out-File '%LIST_FILE%' -Enc ascii"

if errorlevel 1 (
    echo ❌ Failed to fetch file list. Check internet or URL.
    exit /b 1
)

:: Kiểm tra aria2c
where aria2c >nul 2>&1 || (
    echo ❌ aria2c.exe not found. Put it in this folder or add to PATH.
    exit /b 1
)

echo Downloading %MAX_FILES% files to %OUTPUT_DIR%...
aria2c -i "%LIST_FILE%" -x16 -s16 -j4 -d "%OUTPUT_DIR%" --continue=true --file-allocation=none --max-tries=3

echo Done.
del "%LIST_FILE%"