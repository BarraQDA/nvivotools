# NVivotools

A range of tools to help you get more out of NVivo(tm).

Thanks to the wonderful [Wooey](https://github.com/wooey/Wooey) some of these tools are now available for use on our server at [http://wooey.barraqda.org](http://wooey.barraqda.org).

## Introduction

The core of NVivotools its ability to convert qualitative research data (sources, nodes, coding, etc.) into and out of NVivo's proprietary format. Some reasons why you might want to do this include:

1. Freeing your work. Make your research data available to whomever your want (including your future self), not only those with their own current NVivo licence.

2. Choose the tools you want to manipulate your data. NVivo's GUI isn't bad, but sometimes you'd prefer to be able to automate. Use some of the plethora of data management tools or your own coding skills to take charge of your data.

3. Interface with the rest of your IT world. Make NVivo part of your tookit, not your whole world.

## Background

NVivo stores its projects in the form of a relational database. The Windows version uses Microsoft SQL Server while the Mac version uses SQL Anywhere. NVivotools currently only works with Microsoft Windows NVivo files though a Mac version should be fairly simple to write using [sqlalchemy-sqlany](https://github.com/sqlanywhere/sqlalchemy-sqlany).

NVivo files for Windows (with extension .nvp) are simply Microsoft SQL Server files. NVivotools works with them just as NVivo does, by attaching them to SQL Server. Unlike NVivo, NVivotools uses something called [Tabular Data Stream](https://en.wikipedia.org/wiki/Tabular_Data_Stream) (TDS) to communicate with SQL server. This approach has the advantage of abstracting the database access so that NVivotools does not need to know too much about the messy details of SQL Server. It does, however, mean that SQL Server needs to be set up to allow TDS connections.

I hope this file will be comprehensible not only to those who know more than I do about Windows, SQL Server and the like, but to relative newcomers. I assume that these operations are being performed on a computer with NVivo 10 already installed, but very little else. I also give some pointers for working with NVivo 11.

## Setting up SQL Server

This is the most difficult part of the process, so I describe it in the greatest detail.

### Install Microsoft SQL Server

Start by accepting that SQL Server, like pretty much everything Microsoft creates, is awful. Each version differs, often in subtle and undocumented ways, that make it incompatible with previous versions. Installation and configuration often require a GUI, making them impossible (or very difficult) to automate. However, SQL Server does have many enthusiastic users who write prolifically about their experiences. So if you have trouble with any of this, the first place to look for help is on the web by googling the text of any error message you need to investigate or concise description of a problem you encounter.

The version of SQL Server that NVivo 10 uses is called Microsoft SQL Server 2008 R2. This program is installed as part of the NVivo installation process. If you want to use NVivotools without having NVivo installed, you'll need to download and install [Microsoft SQL Server 2008 R2 Express](https://www.microsoft.com/en-au/download/details.aspx?id=30438) (free). NVivo 11 uses Microsoft SQL Server 2014, but only installs a stripped-down 'LocalDB' version. To use NVivotools with NVivo 11, you'll need to download and install [Microsoft SQL Server 2014 Express](https://www.microsoft.com/en-au/download/details.aspx?id=42299) (free) yourself.

### Set up SQL Server

NVivotools accesses SQL Server using TDS, which operates over the network protocol TCP/IP. This means that you need to configure SQL Server to allow access over TCP/IP. It may be possible to do this using the command line, but I found it simpler to use the SQL Server Configuration Manager, which you'll find from the Start Menu in the folder for the relevant version of SQL Server. When you find it you need to:

#### 1. Enable TCP/IP connections

In the left panel of the SQL Server Configuration Manager click on 'Protocol for QSRNVIVO10' under 'SQL Server Network Configuration' or 'SQL Server Network Configuration (32bit)' and find a list of protocol names. The one you want is 'TCP/IP'. Right-click on this one, then click on 'Properties'. Under the 'Protocol' tab you need to change the value of 'Enabled' to 'Yes'. Then go to the 'IP Addresses' tab, scroll to the bottom of the list of values until you find a header 'IPAll'. Expand this heading by clicking on it until you see the value 'TCP Port' underneath it. Change this value to the TDS default of '1433'. Then click 'OK' to accept the TCP/IP configuration.

If you are using either NVivo 11 or a system without NVivo 10 installed, you'll need to select the network protocol for a different server instance. Whichever instance you select is the one you will need to use later on, so note the name.

If you know what you are doing you can of course set the TCP port for just the IP devices you are going to use and/or use a port other than 1433. Just beware that the instructions below assume you have used the default values.

Don't close the Configuration Manager just yet, as you'll need to use it to restart the server a few steps further on.

#### 2. Configure SQL Server authentication

Microsoft SQL Server is able to use two different kinds of authentication to control access to its databases. The default setting is to only allow 'Windows authentication'. And you guessed it, we need the other kind 'SQL Server authentication'. To configure the server to allow both kinds of authentication, you need to make a small change to the Windows registry. There are a variety of ways of doing this; I will only describe the most standard way of doing so using the registry editor regedit.

Run regedit.exe from the Start Menu by typing 'regedit' into the Search box. You will need to authorise changes to the system - don't be too alarmed, you are in total control of any changes so as long as you are careful and/or follow these instructions closely no harm will result. That said, no guarantees!

Using the left pane in the regedit window, navigate to HKEY_LOCAL_MACHINE -> SOFTWARE -> Microsoft -> Microsoft SQL Server -> MSSQL10_50.QSRNVIVO10 -> MSSQLServer. Once again if you are using a different version of NVivo or Microsoft SQL Server these names (especially `MSSQL10_50.QSRNVIVO10` may vary). When you get there, you will see a list of values in the right pane. Look for 'LoginMode'; right-click on it, select 'Modify' and change the value to '2'.

#### 3. Create an account ('login' in MSSQL parlance)

Start the SQLCMD program from the command line as follows:

    sqlcmd -S LOCALHOST\QSRNVIVO10

Then enter the following commands:

    create login nvivotools with password='nvivotools'
    go
    sp_addsrvrolemember nvivotools,sysadmin
    go

[Some sources](https://www.mssqltips.com/sqlservertip/2538/enabling-dedicated-administrator-connection-in-sql-server-2008-express-edition/) suggest that you may need to restart the server with `;-T7806` appended to the command line. (And some people still take Microsoft seriously?) I haven't always found this necessary but if you have trouble then it may be worth trying.

#### 4. Restart server

Another piece of Microsoft brilliance - you can't request that the server simply read a new network configuration - you have to restart the whole thing. Back at the SQL Server Configuration Manager window, click on 'SQL Server Services' in the left frame, then right-click on 'SQL Server (QSRNVIVO10)' and select 'Restart'.

As in the previous section, if you are using a server instance other than 'QSRNVIVO10', that will be the server you need to restart.

#### 5. (Optional) Punch a hole in the Windows firewall

If you want to use NVivotools from a different computer than the one running SQL Server (I do this so that I can keep as far away from Windows as possible, but you may find other reasons to do so) you'll need to tell the firewall to allow incoming network connections to SQL Server. You'll need to find the SQL Server executable (something like `C:\Program Files\Microsoft SQL Server\MSSQL10_50.QSRNVIVO10\MSSQL\Binn\sqlservr.exe`) and configure the Microsoft Firewall to allow connections to that program.

#### More Information

Here are a few links that describe other ways of configuring the SQL Server authentication.

- [https://blogs.technet.microsoft.com/sqlman/2011/06/14/tips-tricks-you-have-lost-access-to-sql-server-now-what/](https://blogs.technet.microsoft.com/sqlman/2011/06/14/tips-tricks-you-have-lost-access-to-sql-server-now-what/)
- [https://www.mssqltips.com/sqlservertip/2538/enabling-dedicated-administrator-connection-in-sql-server-2008-express-edition/](https://www.mssqltips.com/sqlservertip/2538/enabling-dedicated-administrator-connection-in-sql-server-2008-express-edition/)

## Install Python

Although I have done my best to make NVivotools work with both Python versions 2 and 3, one of the libraries on which it depends (PDFMiner) currently only supports Python version 2. So for now you will need to install Python 2.

### Windows

Install a recent version of Python 2 from [Python Releases for Windows](https://www.python.org/downloads/windows/). During the installation process you will be asked whether to add Python to the path - say 'Yes' to keep things simple.

### Linux

Use your distro package manager to install Python 2.

### Mac

The instructions below have been tested on a fresh Mac OSX version 10.11 (El Capitan). They should work with little or no changes on other versions.

Although OSX ships with a version of Python, it seems to be unable to work correctly with the SQLAlchemy package on which NVivotools depends (more precisely - if anyone knows enough about this stuff to figure it out - it fails to load the dbcapi). There is some suspicion that this problem may be related to OSX's System Integrity Protection (SIP), which was only introduced with El Capitan. It is therefore possible that the following section may not be required on earlier versions of OSX (or if you disable SIP, which we do not recommend).

Instead of using the shipped version of Python, install a recent version of Python 2 from [Python Releases for Mac OS X](https://www.python.org/downloads/mac-osx/). Note that Mac releases of Python appear to lack the `dateutils` module, so you will need to install that one explicitly:

## Installing Python libraries

You should use 'pip', even under Linux, to be sure of having up-to-date versions of the libraries. The following commands install the libraries only for the current user, which is the recommended approach at least during the testing phases.

Using a command window, first make sure you have an up-to-date version of pip:

    pip install --user --upgrade pip

then install the required modules:

    pip install --user future pdfminer Pillow sqlalchemy python-dateutil

If you plan to access NVivo for Windows files, you will also need

    pip install --user pymssql

while if you are going to access NVivo for Mac files, you will need

    pip install --user sqlalchemy_sqlany

User [abers](https://github.com/abers) [found](https:/sre/github.com/BarraQDA/nvivotools/issues/1#issue-181693962) a problem on Raspberry Pi (possibly other ARM systems) where the `pymssql` library requires other packages (`freetds-common`, `libsybdb5`) to be installed. This problem was resolved by installing those packages using the package manager (eg `apt-get` for Debian-based systems) before using pip to install pymssql.

## Optional extras

In order to convert data among various formats, NVivotools uses a number of helper applications. Depending on the use you plan to make of NVivotools you may or may not need these applications.

These packages are typically trivial to install on Linux systems, but require a little more work under Windows.

### LibreOffice/OpenOffice and unoconv

If you are going to import textual data into NVivo, you will need either [LibreOffice](http://www.libreoffice.org) or [OpenOffice](http://www.openoffice.org), plus the [unoconv](http://dag.wiee.rs/home-made/unoconv/) package.

### Linux

Simply install the packages using your usual package manager, whether a GUI like `synaptic` on Ubuntu, or command line such as `apt-get` on Debian-based systems.

### Windows

1. Install one of [LibreOffice](http://www.libreoffice.org) or [OpenOffice](http://www.openoffice.org) by following the links from the website.

2. Download [unoconv](http://dag.wiee.rs/home-made/unoconv/) from its [github release](https://github.com/dagwieers/unoconv/archive/0.7.zip). From this zip file, extract `unoconv-0.7/unoconv` and copy it to the folder where the NVivotools scripts.

### Mac

According to [this report](https://github.com/dagwieers/unoconv/issues/263#issuecomment-183237795) on Mac OSX you may have to install an [older version](https://downloadarchive.documentfoundation.org/libreoffice/old/4.2.5.2/mac/x86_64/LibreOffice_4.2.5.2_MacOS_x86-64.dmg) of LibreOffice in order for unoconv to work.

`unoconv` should be installable using `pip`:

    pip install --user unoconv

and ensure that unoconv is on your path.

## And you are ready to go

Until I write a GUI front end for NVivotools, you'll need to use a command line. The main work is done in the two Python scripts [`NormaliseDB.py`](NormaliseDB.py) and [`DenormaliseDB.py`](DenormaliseDB.py), together with the module [`NVivo.py`](NVivo.py) and included script [`DataTypes.py`](DataTypes.py)  These scripts take two arguments (in sqlalchemy format, eg `sqlite:///filename.db` or `mssql+pymssql://user:password@sqlservername/database`) and convert the former to the latter. A useful switch is `-w`/`--windows`, which instructs the scripts to convert certain text fields to or from a strangely garbled format that NVivo for Windows (but not for Mac) uses.

Before you can use these scripts you'll need to get your NVivo file attached to an instance of Microsoft SQL Server. The helper batch scripts in the subdirectory 'Windows' should be of some help. You'll need to put both directories into your PATH to make them work.

- [`attach.bat`](Windows/attach.bat) attaches a file to an SQL Server instance and allows access to the user 'nvivotools' with password 'nvivotools'. It takes as its first argument the name of the file to attach, a second optional argument is the name to assign the database (the default is 'nvivo') and the third is the name of the SQL Server instance to use (default QSRNVIVO10).
- [`save.bat`](Windows/save.bat) drops a database and saves it to the given filename. It takes the same arguments as attach.bat  WARNING: This script will overwrite the file if it already exists. Make a backup and/or use a different filename.
- [`list.bat`](Windows/list.bat) lists databases on a given SQL Server instance.
- [`drop.bat`](Windows/drop.bat) drops a database without saving it.
- [`create.bat`](Windows/create.bat) creates an empty database.
- [`normalise.bat`](Windows/normalise.bat)/[`denormalise.bat`](Windows/denormalise.bat) put the whole lot together. They take two arguments: an NVivo filename and a SQLite filename (respectively in the case of normalise and in the opposite order in the case of denormalise) and call the other scripts to make everything happen.

