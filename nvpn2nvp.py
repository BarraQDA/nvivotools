#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Jonathan Schultz
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

from argrecord import ArgumentHelper, ArgumentRecorder
import argparse
import NVivo
from mssqlTools import mssqlAPI
import os
import sys
import shutil
import subprocess
import tempfile

def DenormaliseNVP(arglist):
    parser = ArgumentRecorder(description='Create an NVivo for Mac file from a normalised SQLite file.')

    parser.add_argument('-nv', '--nvivoversion', choices=["10", "11"], default="10",
                        help='NVivo version (10 or 11)')

    parser.add_argument('-S', '--server', type=str,
                        help="IP address/name of Microsoft SQL Server")
    parser.add_argument('-P', '--port', type=int,
                        help="Port of Microsoft SQL Server")
    parser.add_argument('-i', '--instance', type=str,
                        help="Microsoft SQL Server instance")
    parser.add_argument('-U', '--sshuser', type=str,
                        help="User name for ssh connections to server")

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

    parser.add_argument('-v', '--verbosity', type=int, default=1, private=True)
    parser.add_argument('--logfile',         type=str, help="Logfile, default is <outfile>.log",
                                             private=True)
    parser.add_argument('--no-logfile',      action='store_true', help='Do not output a logfile')

    parser.add_argument('infile', type=str, input=True,
                        help="Input normalised SQLite (.nvpn) file")
    parser.add_argument('outfile', type=str, nargs='?', output=True,
                        help="Output NVivo for Windows (.nvp) file or directory; default is <infile>.nvp")

    args = parser.parse_args(arglist)

    # Function to execute a command either locally or remotely
    def executecommand(command):
        if not args.server:     # ie server is on same machine as this script
            return subprocess.check_output(command, text=True).strip()
        else:
            print(['ssh', ((args.sshuser + '@') if args.sshuser else '') + args.server] + [('"' + word + '"') if ' ' in word else word for word in command])
            # This quoting of arguments is a bit of a hack but seems to work
            return subprocess.check_output(['ssh', ((args.sshuser + '@') if args.sshuser else '') + args.server] + [('"' + word + '"') if ' ' in word else word for word in command], text=True).strip()

    if args.outfile is None:
        args.outfile = args.infile.rsplit('.',1)[0] + '.nvp'
    elif os.path.isdir(args.outfile):
        args.outfile = os.path.join(args.outfile,
                                    os.path.basename(args.infile.name.rsplit('.',1)[0] + '.nvp'))

    if not args.no_logfile:
        logfilename = args.outfile.rsplit('.',1)[0] + '.log'
        incomments = ArgumentHelper.read_comments(logfilename) or ArgumentHelper.separator()
        logfile = open(logfilename, 'w')
        parser.write_comments(args, logfile, incomments=incomments)
        logfile.close()

    # Fill in extra arguments that NVivo module expects
    args.mac       = False
    args.windows   = True

    if args.basefile is None:
        args.basefile = os.path.dirname(os.path.realpath(__file__)) + os.path.sep + ('emptyNVivo10Win.nvp' if args.nvivoversion == '10' else 'emptyNVivo11Win.nvp')

    if args.server is None:
        if os.name != 'nt':
            raise RuntimeError("This does not appear to be a Windows machine so --server must be specified.")

    mssqlapi = mssqlAPI(args.server,
                        user=args.sshuser,
                        port=args.port,
                        instance=args.instance,
                        version = ('MSSQL12' if args.nvivoversion == '11' else 'MSSQL10_50'),
                        verbosity = args.verbosity)

    # Get reasonably distinct yet recognisable DB name
    dbname = 'nvivo' + str(os.getpid())

    mssqlapi.attach(args.basefile, dbname)
    try:
        args.indb = 'sqlite:///' + args.infile
        args.outdb = 'mssql+pymssql://nvivotools:nvivotools@' + (args.server or 'localhost') + ((':' + str(args.port)) if args.port else '') + '/' + dbname

        NVivo.Denormalise(args)

        mssqlapi.save(args.outfile, dbname)

    except:
        raise

    finally:
        mssqlapi.drop(dbname)

if __name__ == '__main__':
    DenormaliseNVP(None)
