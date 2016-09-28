@echo off
set TEMPDIR=%TMP%\SQL

IF NOT EXIST "%TEMPDIR%\" (
    mkdir "%TEMPDIR%"
)
icacls "%TEMPDIR%" /grant Everyone:(OI)(CI)F >nul

set DB=%2
IF "%DB%"=="" (
    set DB=nvivo
)

set INSTANCE=%3
IF "%INSTANCE%"=="" (
    set INSTANCE=QSRNVIVO10
)
set server=%COMPUTERNAME%\%INSTANCE%

del "%TEMPDIR%\%~n1*" >nul 2>&1
copy "%~1" %TEMPDIR% >nul
icacls "%TEMPDIR%\%~n1%~x1" /grant Everyone:(OI)(CI)F >nul
sqlcmd -S %server% -Q "CREATE DATABASE %DB% ON (FILENAME='%TEMPDIR%\%~n1%~x1') FOR ATTACH" >nul
rem sqlcmd -S %server% -Q "CREATE LOGIN nvivotools WITH PASSWORD='nvivotools'"
sqlcmd -S %server% -Q "use %DB%; IF EXISTS (SELECT * FROM sys.database_principals WHERE name = N'nvivotools') DROP USER nvivotools" >nul
sqlcmd -S %server% -Q "use %DB%; CREATE USER nvivotools FROM LOGIN nvivotools" >nul
sqlcmd -S %server% -Q "use %DB%; GRANT CONTROL TO nvivotools" >nul
