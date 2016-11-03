set TEMPDIR=%TMP%\SQL
mkdir %TEMPDIR%
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
rem sqlcmd -S %server% -Q "CREATE LOGIN nvivotools WITH PASSWORD='nvivotools'"
sqlcmd -S %server% -Q "use %DB%; IF EXISTS (SELECT * FROM sys.database_principals WHERE name = N'nvivotools') DROP USER nvivotools" >nul
sqlcmd -S %server% -Q "use %DB%; CREATE USER nvivotools FROM LOGIN nvivotools" >nul
sqlcmd -S %server% -Q "use %DB%; GRANT CONTROL TO nvivotools" >nul
