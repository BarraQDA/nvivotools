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

set TEMPDIR=%TMP%\SQL

IF NOT EXIST "%TEMPDIR%\" (
    mkdir "%TEMPDIR%"
)
icacls "%TEMPDIR%" /grant Everyone:(OI)(CI)F

set DB=%1
IF "%DB%"=="" (
    set DB=nvivo
    )

set INSTANCE=%2
IF "%INSTANCE%"=="" (
    set INSTANCE=QSRNVIVO10
    )
set server=%COMPUTERNAME%\%INSTANCE%

sqlcmd -S %server% -Q "CREATE DATABASE %DB%"
sqlcmd -S %server% -Q "CREATE LOGIN nvivotools WITH PASSWORD='nvivotools'"
sqlcmd -S %server% -Q "use %DB%; IF EXISTS (SELECT * FROM sys.database_principals WHERE name = N'nvivotools') DROP USER nvivotools" >nul
sqlcmd -S %server% -Q "use %DB%; CREATE USER nvivotools FROM LOGIN nvivotools" >nul
sqlcmd -S %server% -Q "use %DB%; GRANT CONTROL TO nvivotools" >nul
