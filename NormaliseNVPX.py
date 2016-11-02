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

parser = argparse.ArgumentParser(description='Normalise an NVivo for Mac file.')

parser.add_argument('-u', '--users', choices=["", "skip", "merge", "overwrite", "replace"], default="merge",
                    help='User action.')
parser.add_argument('-p', '--project', choices=["", "skip", "replace"], default="replace",
                    help='Project action.')
parser.add_argument('-nc', '--node-categories', choices=["", "skip", "merge", "overwrite", "replace"], default="merge",
                    help='Node category action.')
parser.add_argument('-n', '--nodes', choices=["", "skip", "merge", "overwrite", "replace"], default="merge",
                    help='Node action.')
parser.add_argument('-na', '--node-attributes', choices=["", "skip", "merge", "overwrite", "replace"], default="merge",
                    help='Node attribute table action.')
parser.add_argument('-sc', '--source-categories', choices=["", "skip", "merge", "overwrite", "replace"], default="merge",
                    help='Source category action.')
parser.add_argument('--sources', choices=["", "skip", "merge", "overwrite", "replace"], default="merge",
                    help='Source action.')
parser.add_argument('-sa', '--source-attributes', choices=["", "skip", "merge", "overwrite", "replace"], default="merge",
                    help='Source attribute action.')
parser.add_argument('-t', '--taggings', choices=["", "skip", "merge", "overwrite", "replace"], default="merge",
                    help='Tagging action.')
parser.add_argument('-a', '--annotations', choices=["", "skip", "merge", "overwrite", "replace"], default="merge",
                    help='Annotation action.')

parser.add_argument('infile', type=argparse.FileType('rb'),
                    help="Input NVivo for Mac file (extension .nvpx)")
parser.add_argument('outfilename', type=str, nargs='?',
                    help="Output normalised SQLite file")

args = parser.parse_args()

# Fill in extra arguments that NVivo module expects
args.mac       = True
args.structure = True
args.windows   = False
args.verbosity = 1

import NVivo
import os
import shutil
import signal
from subprocess import Popen, PIPE
import tempfile
import time

tmpinfilename = tempfile.mktemp()
tmpinfileptr  = file(tmpinfilename, 'wb')
tmpinfileptr.write(args.infile.read())
args.infile.close()
tmpinfileptr.close()

tmpoutfilename = tempfile.mktemp()

if args.outfilename is None:
    args.outfilename = os.path.basename(args.infile.name.rsplit('.',1)[0] + '.norm')

import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("",0))
freeport = str(s.getsockname()[1])
s.close()

DEVNULL = open(os.devnull, 'wb')
dbproc = Popen(['sh', os.path.dirname(os.path.realpath(__file__)) + '/sqlanysrv.sh', '-x TCPIP(port='+freeport+')', '-ga',  tmpinfilename, '-n', 'NVivo'+freeport], stdout=PIPE, stdin=DEVNULL)

# Wait until SQL Anywhere engine starts...
while dbproc.poll() is None:
    line = dbproc.stdout.readline()
    if line == 'Now accepting requests\n':
        break
args.indb = 'sqlalchemy_sqlany://wiwalisataob2aaf:iatvmoammgiivaam@localhost:' + freeport + '/NVivo' + freeport
args.outdb = 'sqlite:///' + tmpoutfilename

NVivo.Normalise(args)

shutil.move(tmpoutfilename, args.outfilename)
os.remove(tmpinfilename)
