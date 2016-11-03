@echo off
set DB=%1
IF "%DB%"=="" (
    set DB=nvivo
    )

    set INSTANCE=%2
IF "%INSTANCE%"=="" (
    set INSTANCE=QSRNVIVO10
    )

set server=%COMPUTERNAME%\%INSTANCE%
sqlcmd -S %server% -Q "EXEC sp_detach_db %DB%"
