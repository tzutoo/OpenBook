@echo off
call "C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\Tools\VsDevCmd.bat" -arch=amd64
cd /d "%~dp0"
echo Running OpenBook tests...
cargo test --release
if %ERRORLEVEL% EQU 0 (
    echo.
    echo All tests passed!
) else (
    echo.
    echo Tests failed with error code %ERRORLEVEL%
)
pause