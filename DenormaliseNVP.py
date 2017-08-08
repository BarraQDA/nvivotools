#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016 Jonathan Schultz
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
import argparse
import NVivo
import os
import sys
import shutil
import subprocess
import tempfile

def DenormaliseNVP(arglist):
    parser = argparse.ArgumentParser(description='Create an NVivo for Mac file from a normalised SQLite file.')

    parser.add_argument('-v', '--verbosity', type=int, default=1)

    parser.add_argument('-nv', '--nvivoversion', choices=["10", "11"], default="10",
                        help='NVivo version (10 or 11)')

    parser.add_argument('-S', '--server', type=str,
                        help="IP address/name of Microsoft SQL Server")
    parser.add_argument('-P', '--port', type=int,
                        help="Port of Microsoft SQL Server")
    parser.add_argument('-i', '--instance', type=str,
                        help="Microsoft SQL Server instance")

    parser.add_argument('-u', '--users', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                        help='User action.')
    parser.add_argument('-p', '--project', choices=["skip", "overwrite"], default="overwrite",
                        help='Project action.')
    parser.add_argument('-nc', '--node-categories', choices=["skip", "merge", "overwrite"], default="merge",
                        help='Node category action.')
    parser.add_argument('-n', '--nodes', choices=["skip", "merge"], default="merge",
                        help='Node action.')
    parser.add_argument('-na', '--node-attributes', choices=["skip", "merge", "overwrite"], default="merge",
                        help='Node attribute table action.')
    parser.add_argument('-sc', '--source-categories', choices=["skip", "merge", "overwrite"], default="merge",
                        help='Source category action.')
    parser.add_argument('--sources', choices=["skip", "merge", "overwrite"], default="merge",
                        help='Source action.')
    parser.add_argument('-sa', '--source-attributes', choices=["skip", "merge", "overwrite"], default="merge",
                        help='Source attribute action.')
    parser.add_argument('-t', '--taggings', choices=["skip", "merge"], default="merge",
                        help='Tagging action.')
    parser.add_argument('-a', '--annotations', choices=["skip", "merge"], default="merge",
                        help='Annotation action.')

    parser.add_argument('-b', '--base', dest='basefile', type=argparse.FileType('rb'), nargs='?',
                        help="Base NVP file to insert into")

    parser.add_argument('infile', type=str,
                        help="Input normalised SQLite (.norm) file")
    parser.add_argument('outfilename', metavar='outfile', type=str, nargs='?',
                        help="Output NVivo (.nvp) file")

    args = parser.parse_args(arglist)

    # Function to execute a command either locally or remotely
    def executecommand(command):
        if not args.server:     # ie server is on same machine as this script
            return subprocess.check_output(command).strip()
        else:
            # This quoting of arguments is a bit of a hack but seems to work
            return subprocess.check_output(['ssh', args.server] + [('"' + word + '"') if ' ' in word else word for word in command]).strip()

    # Function to execute a helper script either locally or remotely
    def executescript(script, arglist=None):
        if not args.server:     # ie server is on same machine as this script
            return subprocess.check_output([helperpath + script] + (arglist or [])).strip()
        else:
            subprocess.call(['scp', '-q', helperpath + script, args.server + ':' + tmpdir])
            return subprocess.check_output(['ssh', args.server, tmpdir + '\\' + script] + (arglist or [])).strip()

    # Fill in extra arguments that NVivo module expects
    args.mac       = False
    args.windows   = True

    if args.outfilename is None:
        args.outfilename = args.infile.rsplit('.',1)[0] + '.nvp'

    if args.basefile is None:
        args.basefile = os.path.dirname(os.path.realpath(__file__)) + os.path.sep + ('emptyNVivo10Win.nvp' if args.nvivoversion == '10' else 'emptyNVivo11Win.nvp')

    if args.server is None:
        if os.name != 'nt':
            raise RuntimeError("This does not appear to be a Windows machine so --server must be specified.")

        tmpoutfilename = tempfile.mktemp()
        shutil.copy(args.basefile, tmpoutfilename)
    else:
        tmpdir = subprocess.check_output(['ssh', args.server, r'echo %tmp%']).strip()
        tmpoutfilename = subprocess.check_output(['ssh', args.server, r'echo %tmp%\nvivotools%random%.nvp']).strip()
        subprocess.call(['scp', '-q', args.basefile, args.server + ':' + tmpoutfilename])

    helperpath = os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'helpers' + os.path.sep

    if args.instance is None:
        regquery = executecommand(['reg', 'query', 'HKLM\\Software\\Microsoft\\Microsoft SQL Server\\Instance Names\\SQL']).splitlines()
        for regqueryline in regquery[1:]:
            regquerydata = regqueryline.split()
            instancename = regquerydata[0]
            instanceversion = regquerydata[2].split('.')[0]
            if args.verbosity >= 2:
                print("Found SQL server instance " + instancename + "  version " + instanceversion, file=sys.stderr)
            if (args.nvivoversion == '10' and instanceversion == 'MSSQL10_50') or (args.nvivoversion == '11' and instanceversion == 'MSSQL12'):
                args.instance = instancename
                break
        else:
            raise RuntimeError('No suitable SQL server instance found')

    if args.verbosity > 0:
        print("Using MSSQL instance: " + args.instance, file=sys.stderr)

    if args.port is None:
        regquery = executecommand(['reg', 'query', 'HKLM\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\' + args.instance + '\\MSSQLServer\\SuperSocketNetLib\\Tcp']).splitlines()
        args.port = int(regquery[1].split()[2])

    if args.verbosity > 0:
        print("Using port: " + str(args.port), file=sys.stderr)

    # Get reasonably distinct yet recognisable DB name
    dbname = 'nvivo' + str(os.getpid())

    executescript('mssqlAttach.bat', [tmpoutfilename, dbname, args.instance])
    if args.verbosity > 0:
        print("Attached database " + dbname, file=sys.stderr)

    try:
        args.indb = 'sqlite:///' + args.infile
        args.outdb = 'mssql+pymssql://nvivotools:nvivotools@' + (args.server or 'localhost') + ((':' + str(args.port)) if args.port else '') + '/' + dbname

        NVivo.Denormalise(args)

        executescript('mssqlSave.bat', [tmpoutfilename, dbname, args.instance])
        if args.verbosity > 0:
            print("Saved database " + dbname, file=sys.stderr)

        if args.server is None:
            shutil.move(tmpoutfilename, args.outfilename)
        else:
            subprocess.call(['scp', '-q', args.server + ':' + tmpoutfilename, args.outfilename])

    except:
        raise

    finally:
        executescript('mssqlDrop.bat', [dbname, args.instance])
        if args.verbosity > 0:
            print("Dropped database " + dbname, file=sys.stderr)

if __name__ == '__main__':
    DenormaliseNVP(None)
