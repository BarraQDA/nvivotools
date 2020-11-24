#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Jonathan Schultz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import print_function
import os
import sys
import subprocess
import tempfile
import shutil
import random

class mssqlAPI(object):

    # Function to execute a command either locally or remotely
    def executecommand(self, command):
        if not self.server:     # ie server is on same machine as this script
            return subprocess.check_output(command, text=True).strip()
        else:
            # This quoting of arguments is a bit of a hack but seems to work
            return subprocess.check_output(['ssh', self.server] + [('"' + word + '"') if ' ' in word else word for word in command], text=True).strip()

    # Function to execute a helper script either locally or remotely
    def executescript(self, script, arglist=None):
        if not self.server:     # ie server is on same machine as this script
            return subprocess.check_output([self.helperpath + script] + (arglist or []), text=True).strip()
        else:
            subprocess.call(['scp', '-q', self.helperpath + script, self.server + ':' + self.tmpdir])
            return subprocess.check_output(['ssh', self.server, self.tmpdir + '\\' + script] + (arglist or []), text=True).strip()

    def __init__(self, server, port=None, instance=None, version=None, verbosity=1):
        self.server    = server
        self.port      = port
        self.instance  = instance
        self.verbosity = verbosity

        self.helperpath = os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'helpers' + os.path.sep

        if self.server is None:  # ie MSSQL server is on local machine
            if os.name != 'nt':
                raise RuntimeError("This does not appear to be a Windows machine so --server must be specified.")
        else:
            self.tmpdir = subprocess.check_output(['ssh', self.server, r'echo %tmp%'], text=True).strip()


        if self.instance is None:
            regquery = self.executecommand(['reg', 'query', 'HKLM\\Software\\Microsoft\\Microsoft SQL Server\\Instance Names\\SQL']).splitlines()
            for regqueryline in regquery[1:]:
                regquerydata = regqueryline.split()
                instancename = regquerydata[0]
                instanceversion = regquerydata[2].split('.')[0]
                if self.verbosity > 1:
                    print("Found SQL server instance " + instancename + "  version " + instanceversion, file=sys.stderr)
                if (not version or instanceversion == version):
                    self.instance = instancename
                    break
            else:
                raise RuntimeError('No suitable SQL self.server self.instance found')

        if self.verbosity > 1:
            print("Using MSSQL instance: " + self.instance, file=sys.stderr)

        if self.port is None:
            regquery = self.executecommand(['reg', 'query', 'HKLM\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\' + self.instance + '\\MSSQLServer\\SuperSocketNetLib\\Tcp']).splitlines()
            self.port = int(regquery[1].split()[2])

        if self.verbosity > 1:
            print("Using port: " + str(self.port), file=sys.stderr)

    def attach(self, filename, dbname):
        # Generate a filename for the temporary MDB file
        if self.server is None:  # ie MSSQL server is on local machine
            mdbFilename = tempfile.mktemp()
            shutil.copy(filename, mdbFilename)
        else:
            mdbFilename = self.tmpdir + r'\mssqltools' + str(random.randint(0,99999)).zfill(5)
            subprocess.call(['scp', '-q', filename, self.server + ':' + mdbFilename])

        self.executescript('mssqlAttach.bat', [mdbFilename, dbname, self.instance])
        if self.verbosity > 1:
            print("Attached database " + dbname, file=sys.stderr)

    def create(self, dbname):
        self.executescript('mssqlCreate.bat', [dbname, self.instance])
        if self.verbosity > 1:
            print("Created database " + dbname, file=sys.stderr)

    def detach(self, dbname):
        serverinstance = 'localhost\\' + self.instance

        self.executecommand(['sqlcmd', '-S', serverinstance, '-Q', 'EXEC sp_detach_db ' + dbname])

    def drop(self, dbname):
        serverinstance = 'localhost\\' + self.instance

        self.executecommand(['sqlcmd', '-S', serverinstance, '-Q', 'DROP DATABASE ' + dbname])

    def save(self, filename, dbname):
        if self.server is None:  # ie MSSQL server is on local machine
            self.executescript('mssqlSave.bat', [filename, dbname, self.instance])
        else:
            mdbFilename = self.tmpdir + r'\mssqltools' + str(random.randint(0,99999)).zfill(5)
            self.executescript('mssqlSave.bat', [mdbFilename, dbname, self.instance])
            subprocess.call(['scp', '-qT', self.server + ':' + mdbFilename, filename])

    def list(self):
        serverinstance = 'localhost\\' + self.instance

        dblist = self.executecommand(['sqlcmd', '-W', '-S', serverinstance, '-h', '-1', '-Q', 'SET NOCOUNT ON; SELECT name FROM master.dbo.sysdatabases']).split()

        # Ignore the first four internal databases: master, tempdb, model and msdb
        return dblist[4:]
