# nvivotools
A range of tools to help you get more out of NVivo(tm)

The python scripts NormaliseNVP and DenormaliseNVP do much as their names imply. They take two arguments in sqlalchemy format (eg sqlite:///filename.db or mssql+pymssql://user:password@sqlservername/database) and convert the former to the latter.

They should both work equally well with python2.x or python3.x, but do require the following modules to be installed (use pip): future, pymssql, sqlalchemy.

# Setting up SQL Server for NVivo 10
- Enable TCP/IP connections in SQL Server configuration manager
- Set port for SQL Server - default is 1433
- Set access to SQL Server: https://blogs.technet.microsoft.com/sqlman/2011/06/14/tips-tricks-you-have-lost-access-to-sql-server-now-what/
 https://www.mssqltips.com/sqlservertip/2538/enabling-dedicated-administrator-connection-in-sql-server-2008-express-edition/
- Firewall: Allow C:\Program Files\Microsoft SQL Server\MSSQL10_50.QSRNVIVO10\MSSQL\Binn\sqlservr.exe


Start sql server with appended ';-T7806'? Don't think this was necessary

C:\Users\jschultz>sqlcmd -S windows7-ts\qsrnvivo10
1> create login nvivotools with password='password'
2> go
1> create user nvivotools from login nvivotools
2> go
1> grant control to nvivotools
2> go

EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE',
			  N'Software\Microsoft\MSSQLServer\MSSQLServer',
			  N'LoginMode', REG_DWORD, [1 for Windows only, 2 for SQL Server + Windows]