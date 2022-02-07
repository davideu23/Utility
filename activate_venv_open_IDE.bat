@echo off
echo Input name of the new enviroment to activate it.
set /p venv_name=Name of venv: 
cd %venv_name%\Scripts\
choice /c sin /n /m "Do you want to open (s)pyder, (i)dle or (n)one"
goto %ERRORLEVEL%
:1
activate.bat && spyder.exe
:2
activate.bat && python -m idlelib.idle
:3
start activate.bat
pause