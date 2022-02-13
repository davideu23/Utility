@echo off
:venvName
echo Input name of the new enviroment to create it a new one.
set /p venv_name=Name of venv: 
:createVenv
REM python -m venv %venv_name%
python -m venv %venv_name%
if %errorlevel% == 0 (
	echo Successfuly created a new venv in %cd%\%venv_name%
	goto activateVenv
) else (
	echo Error found please check your enviroment variable
	pause
	exit
)
:activateVenv
choice /c yn /n /m "Do you want to activate your venv (y/n)"
GOTO %errorlevel%
:1
start %venv_name%\Scripts\activate.bat
:2
exit