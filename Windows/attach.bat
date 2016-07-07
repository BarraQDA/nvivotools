set TEMPDIR=%TMP%\SQL
mkdir %TEMPDIR%
icacls "%TEMPDIR%" /grant Everyone:(OI)(CI)F

set DB=%2
IF "%DB%"=="" (
    set DB=NVivo
    )

set INSTANCE=%3
IF "%INSTANCE%"=="" (
    set INSTANCE=QSRNVIVO10
    )

del "%TEMPDIR%\%~n1*"
copy "%~1" %TEMPDIR%
icacls "%TEMPDIR%\%~n1%~x1" /grant Everyone:(OI)(CI)F
sqlcmd -S %server% -Q "CREATE DATABASE %DB% ON (FILENAME='%TEMPDIR%\%~n1%~x1') FOR ATTACH"
rem sqlcmd -S %server% -Q "CREATE LOGIN nvivotools WITH PASSWORD='nvivotools'"
sqlcmd -S %server% -Q "use %DB%; CREATE USER nvivotools FROM LOGIN nvivotools"
sqlcmd -S %server% -Q "use %DB%; GRANT CONTROL TO nvivotools"
