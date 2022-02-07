@echo off
echo Input name of the new enviroment to create it a new one.
set /p venv_name=Name of venv: 

python -m venv %venv_name%
echo Successfuly created a new venv in %cd%\%venv_name%

choice /c yn /n /m "Do you want to activate your venv (y/n)"
set INPUT=%ERRORLEVEL%
if %INPUT% EQU 1 goto 1
if %INPUT% EQU 2 goto 2
:1
cd %venv_name%\Scripts\
start activate.bat
:2
pause