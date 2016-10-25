#!/usr/bin/python
# -*- coding: utf-8 -*-
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

parser = argparse.ArgumentParser(description='Create an NVivo for Mac file from a normalised SQLite file.')

table_choices = ["", "skip", "replace", "merge"]
parser.add_argument('-p', '--project', choices=table_choices, default="replace",
                    help='Project action.')
parser.add_argument('-nc', '--node-categories', choices=table_choices, default="replace",
                    help='Node category action.')
parser.add_argument('-n', '--nodes', choices=table_choices, default="replace",
                    help='Node action.')
parser.add_argument('-na', '--node-attributes', choices=table_choices, default="replace",
                    help='Node attribute table action.')
parser.add_argument('-sc', '--source-categories', choices=table_choices, default="replace",
                    help='Source category action.')
parser.add_argument('--sources', choices=table_choices, default="replace",
                    help='Source action.')
parser.add_argument('-sa', '--source-attributes', choices=table_choices, default="replace",
                    help='Source attribute action.')
parser.add_argument('-t', '--taggings', choices=table_choices, default="replace",
                    help='Tagging action.')
parser.add_argument('-a', '--annotations', choices=table_choices, default="replace",
                    help='Annotation action.')
parser.add_argument('-u', '--users', choices=table_choices, default="replace",
                    help='User action.')

parser.add_argument('infile', type=argparse.FileType('rb'),
                    help="Input normalised SQLite file (extension .norm)")
parser.add_argument('outfile', type=argparse.FileType('rb'),
                    help="NVivo for Mac file to insert into")

args = parser.parse_args()

# Fill in extra arguments that NVivo module expects
args.mac       = True
args.structure = False
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
tmpoutfileptr  = file(tmpoutfilename, 'wb')
tmpoutfileptr.write(args.outfile.read())
args.outfile.close()
tmpoutfileptr.close()

import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("",0))
freeport = str(s.getsockname()[1])
s.close()

DEVNULL = open(os.devnull, 'wb')
dbproc = Popen(['sh', os.path.dirname(os.path.realpath(__file__)) + '/sqlany.sh', '-x TCPIP(port='+freeport+')', '-ga',  tmpoutfilename, '-n', 'NVivo'+freeport], stdout=PIPE, stdin=DEVNULL)

# Wait until SQL Anywhere engine starts...
while dbproc.poll() is None:
    line = dbproc.stdout.readline()
    if line == 'Now accepting requests\n':
        break
args.indb = 'sqlite:///' + tmpinfilename
args.outdb = 'sqlalchemy_sqlany://wiwalisataob2aaf:iatvmoammgiivaam@localhost:' + freeport + '/NVivo' + freeport

NVivo.Denormalise(args)

shutil.move(tmpoutfilename, os.path.basename(args.outfilename))
os.remove(tmpinfilename)
