@echo off
echo Input name of the new enviroment to activate it.
set /p venv_name=Name of venv: 
cd %venv_name%\Scripts\
choice /c sin /n /m "Do you want to open (s)pyder, (i)dle or (n)one"
set INPUT=%ERRORLEVEL%
if %INPUT% EQU s goto s
if %INPUT% EQU i goto i
if %INPUT% EQU n goto n
:s
activate.bat && spyder.exe
:i
activate.bat && python -m idlelib.idle
:n
start activate.bat
pause