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

import argparse
import NVivo
import os
import shutil
import subprocess
import tempfile

def NormaliseNVP(arglist):
    parser = argparse.ArgumentParser(description='Normalise an NVivo for Windows file.')

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
    parser.add_argument('-nc', '--node-categories', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                        help='Node category action.')
    parser.add_argument('-n', '--nodes', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                        help='Node action.')
    parser.add_argument('-na', '--node-attributes', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                        help='Node attribute table action.')
    parser.add_argument('-sc', '--source-categories', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                        help='Source category action.')
    parser.add_argument('--sources', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                        help='Source action.')
    parser.add_argument('-sa', '--source-attributes', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                        help='Source attribute action.')
    parser.add_argument('-t', '--taggings', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                        help='Tagging action.')
    parser.add_argument('-a', '--annotations', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                        help='Annotation action.')

    parser.add_argument('infile', type=str,
                        help="Input NVivo for Mac file (extension .nvpx)")
    parser.add_argument('outfilename', type=str, nargs='?',
                        help="Output normalised SQLite file")

    args = parser.parse_args(arglist)

    # Fill in extra arguments that NVivo module expects
    args.mac       = False
    args.windows   = True

    if args.server is None:
        if os.name != 'nt':
            raise RuntimeError("This is not a Windows machine so --server must be specified.")

        infile = file(args.infile, 'rb')
        tmpinfilename = tempfile.mktemp()
        tmpinfileptr  = file(tmpinfilename, 'wb')
        tmpinfileptr.write(args.infile.read())
        args.infile.close()
        tmpinfileptr.close()
    else:
        subprocess.call(['scp', '-q', args.infile, args.server + ':'])

    tmpoutfilename = tempfile.mktemp()

    if args.outfilename is None:
        args.outfilename = args.infile.rsplit('.',1)[0] + '.norm'

    helperpath = os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'Windows' + os.path.sep

    if args.instance is None:
        if not args.server:     # ie server is on same machine as this script
            args.instance = subprocess.check_output([helperpath + 'mssqlInstance.bat'], stdout=PIPE)
        else:
            subprocess.call(['scp', '-q', helperpath + 'mssqlInstance.bat', args.server + ':'])
            args.instance = subprocess.check_output(['ssh', args.server, 'mssqlInstance.bat']).strip()

        #args.instance = proc.stdout.readline()[0:-len(os.linesep)]
        if args.verbosity > 0:
            print("Using MSSQL instance: " + args.instance)

    # Get reasonably distinct yet recognisable DB name
    dbname = 'nt' + str(os.getpid())

    if args.server is None:
        subprocess.call([helperpath + 'mssqlAttach.bat', tmpinfilename, dbname, args.instance])
    else:
        subprocess.call(['scp', '-q', helperpath + 'mssqlAttach.bat', args.server + ':'])
        subprocess.call(['ssh', args.server, 'mssqlAttach.bat', os.path.basename(args.infile), dbname, args.instance])

    if args.verbosity > 0:
        print("Attached database " + dbname)

    try:
        args.indb = 'mssql+pymssql://nvivotools:nvivotools@' + (args.server or 'localhost') + ((':' + str(args.port)) if args.port else '') + '/' + dbname
        args.outdb = 'sqlite:///' + tmpoutfilename

        NVivo.Normalise(args)

        if args.server is None:
            proc = Popen([helperpath + 'mssqlDrop.bat', dbname, args.instance])
            proc.wait()
        else:
            subprocess.call(['scp', '-q', helperpath + 'mssqlDrop.bat', args.server + ':'])
            subprocess.call(['ssh', args.server, 'mssqlDrop.bat', dbname, args.instance])

        if args.verbosity > 0:
            print("Dropped database " + dbname)

        shutil.move(tmpoutfilename, args.outfilename)

    except:
        if args.verbosity > 0:
            print("Dropping database " + dbname)
        if args.server is None:
            subprocess.call([helperpath + 'mssqlDrop.bat', dbname, args.instance])
        else:
            subprocess.call(['scp', '-q', helperpath + 'mssqlDrop.bat', args.server + ':'])
            subprocess.call(['ssh', args.server, 'mssqlDrop.bat', dbname, args.instance])
        raise

    finally:
        if args.server is None:
            os.remove(tmpinfilename)

if __name__ == '__main__':
    NormaliseNVP(None)
