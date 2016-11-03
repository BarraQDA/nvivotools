@echo off
set INSTANCE=%1
IF "%INSTANCE%"=="" (
    set INSTANCE=QSRNVIVO10
    )

set server=%COMPUTERNAME%\%INSTANCE%
sqlcmd -W -S %server% -h -1 -Q "SET NOCOUNT ON; SELECT name FROM master.dbo.sysdatabases"
