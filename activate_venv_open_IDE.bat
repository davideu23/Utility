@echo off

:venvName
echo Input name of the new enviroment to activate it.
set /p venv_name=Name of venv: 


:checkInput
if exist %venv_name%\Scripts\activate.bat (
	cd %venv_name%\Scripts\
	goto selectIDE
) else (
	echo Virtual enviroment not found.
	goto venvName
)
:selectIDE
choice /C SIN  /m "Do you want to open (s)pyder, (i)dle or (n)one"
	goto %ERRORLEVEL%
:1
activate.bat && spyder.exe
:2
activate.bat && python -m idlelib.idle
:3
start activate.bat
pause
