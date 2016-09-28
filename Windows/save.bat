@echo off
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
