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

parser = argparse.ArgumentParser(description='Convert an RQDA project to NVivo for Mac (.nvpx) format.')

# --cmdline argument means retain full output file path name, otherwise strip directory,
# so that things work under Wooey.
parser.add_argument('--cmdline', action='store_true',
                    help=argparse.SUPPRESS)

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-nv', '--nvivoversion', choices=["10", "11"], default="10",
                    help='NVivo version (10 or 11)')

parser.add_argument('-u', '--users', choices=["skip", "overwrite"], default="merge",
                    help='User action.')
parser.add_argument('-p', '--project', choices=["skip", "overwrite"], default="overwrite",
                    help='Project action.')
parser.add_argument('-nc', '--node-categories', choices=["skip", "overwrite"], default="merge",
                    help='Node category action.')
parser.add_argument('-n', '--nodes', choices=["skip", "overwrite"], default="merge",
                    help='Node action.')
parser.add_argument('-c', '--cases', choices=["skip", "overwrite"], default="merge",
                    help='case action.')
parser.add_argument('-ca', '--case-attributes', choices=["skip", "overwrite"], default="merge",
                    help='Case attribute table action.')
parser.add_argument('-sc', '--source-categories', choices=["skip", "overwrite"], default="merge",
                    help='Source category action.')
parser.add_argument('-s', '--sources', choices=["skip", "overwrite"], default="merge",
                    help='Source action.')
parser.add_argument('-sa', '--source-attributes', choices=["skip", "overwrite"], default="merge",
                    help='Source attribute action.')
parser.add_argument('-t', '--taggings', choices=["skip", "overwrite"], default="merge",
                    help='Tagging action.')
parser.add_argument('-a', '--annotations', choices=["skip", "overwrite"], default="merge",
                    help='Annotation action.')

parser.add_argument('-b', '--base', dest='basefile', type=argparse.FileType('rb'), nargs='?',
                    help="Base NVPX file to insert into")

parser.add_argument('infile', type=argparse.FileType('rb'),
                    help="Input RQDA file")
parser.add_argument('outfilename', metavar='outfile', type=str, nargs='?',
                    help="Output NVPX file")

args = parser.parse_args()

# Fill in extra arguments that NVivo module expects
args.mac       = True
args.windows   = False

import NVivo
import RQDA
import os
import shutil
import signal
import tempfile
import time
from subprocess import Popen, PIPE

tmpinfilename = tempfile.mktemp()
tmpinfileptr  = file(tmpinfilename, 'wb')
tmpinfileptr.write(args.infile.read())
args.infile.close()
tmpinfileptr.close()

if args.outfilename is None:
    args.outfilename = args.infile.name.rsplit('.',1)[0] + '.nvpx'

if args.basefile is None:
    args.basefile = file(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + ('emptyNVivo10Mac.nvpx' if args.nvivoversion == '10' else 'emptyNVivo11Mac.nvpx'), 'rb')

tmpnormfilename = tempfile.mktemp()

args.indb = 'sqlite:///' + tmpinfilename
args.outdb = 'sqlite:///' + tmpnormfilename
RQDA.RQDA2Norm(args)

os.remove(tmpinfilename)

tmpoutfilename = tempfile.mktemp()
tmpoutfileptr  = file(tmpoutfilename, 'wb')
tmpoutfileptr.write(args.basefile.read())
args.basefile.close()
tmpoutfileptr.close()

# Find a free sock for SQL Anywhere server to bind to
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("",0))
freeport = str(s.getsockname()[1])
s.close()

DEVNULL = open(os.devnull, 'wb')
dbproc = Popen(['sh', os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'sqlanysrv.sh', '-x TCPIP(port='+freeport+')', '-ga', '-xd',  tmpoutfilename, '-n', 'NVivo'+freeport], stdout=PIPE, stdin=DEVNULL)

# Wait until SQL Anywhere engine starts...
while dbproc.poll() is None:
    line = dbproc.stdout.readline()
    if line == 'Now accepting requests\n':
        break

if args.verbosity > 0:
    print("Started database server on port " + freeport)

args.indb  = 'sqlite:///' + tmpnormfilename
args.outdb = 'sqlalchemy_sqlany://wiwalisataob2aaf:iatvmoammgiivaam@localhost:' + freeport + '/NVivo' + freeport

# Small hack
args.node_attributes = args.case_attributes

NVivo.Denormalise(args)

if not args.cmdline:
    args.outfilename = os.path.basename(args.outfilename)

shutil.move(tmpoutfilename, args.outfilename)
os.remove(tmpnormfilename)
