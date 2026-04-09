@echo off
call "C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\Tools\VsDevCmd.bat" -arch=amd64
cd /d "%~dp0"
echo Building OpenBook...
cargo build --release
if %ERRORLEVEL% EQU 0 (
    echo.
    echo Build successful! Running OpenBook...
    target\release\cli_ob.exe
) else (
    echo.
    echo Build failed with error code %ERRORLEVEL%
)
pause