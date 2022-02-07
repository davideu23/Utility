@echo off
echo Input name of the new enviroment to activate it.
set /p venv_name=Name of venv: 
cd %venv_name%\Scripts\
choice /c yn /n /m "Do you want to also open spyder (y/n)"
set INPUT=%ERRORLEVEL%
if %INPUT% EQU 1 goto 1
if %INPUT% EQU 2 goto 2
:1
activate.bat && spyder.exe
:2
start activate.bat
pause