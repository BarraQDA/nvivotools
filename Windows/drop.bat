set DB=%1
IF "%DB%"=="" (
    set DB=NVivo
    )

set INSTANCE=%2
IF "%INSTANCE%"=="" (
    set INSTANCE=QSRNVIVO10
    )

set server=%COMPUTERNAME%\%INSTANCE%
sqlcmd -S %server% -Q "DROP DATABASE %DB%"
