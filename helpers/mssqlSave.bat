@echo off

rem Copyright 2016 Jonathan Schultz
rem
rem This program is free software: you can redistribute it and/or modify
rem it under the terms of the GNU General Public License as published by
rem the Free Software Foundation, either version 3 of the License, or
rem (at your option) any later version.
rem
rem This program is distributed in the hope that it will be useful,
rem but WITHOUT ANY WARRANTY; without even the implied warranty of
rem MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
rem GNU General Public License for more details.
rem
rem You should have received a copy of the GNU General Public License
rem along with this program.  If not, see <http://www.gnu.org/licenses/>.

set DB=%2
IF "%DB%"=="" (
    set DB=nvivo
    )
set INSTANCE=%3
IF "%INSTANCE%"=="" (
    set INSTANCE=QSRNVIVO10
    )
set server=%COMPUTERNAME%\%INSTANCE%

FOR /F "tokens=* USEBACKQ" %%F IN (`sqlcmd -W -S %server% -h -1 -Q "SET NOCOUNT ON; SELECT filename FROM master.dbo.sysdatabases where name='%DB%'"`) DO (
SET FILENAME=%%F
)
sqlcmd -S %server% -Q "EXEC sp_detach_db %DB%"
copy "%FILENAME%" "%~1" >nul
del "%FILENAME%"