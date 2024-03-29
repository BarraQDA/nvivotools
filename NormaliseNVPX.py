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
import os
import subprocess
import re
import sys
import glob
import argparse
import NVivo
import shutil
import tempfile

parser = argparse.ArgumentParser(description='Normalise an NVivo for Mac file.')

# --cmdline argument means retain full output file path name, otherwise strip directory,
# so that things work under Wooey.
parser.add_argument('--cmdline', action='store_true',
                    help=argparse.SUPPRESS)

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-nv', '--nvivoversion', 
                    choices=["10", "11", "12"], default="10",
                    help='NVivo version (10, 11 or 12)')

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
parser.add_argument('-s', '--sources', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                    help='Source action.')
parser.add_argument('-sa', '--source-attributes', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                    help='Source attribute action.')
parser.add_argument('-t', '--taggings', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                    help='Tagging action.')
parser.add_argument('-a', '--annotations', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                    help='Annotation action.')

parser.add_argument('--sqlanywhere', type=str,
                    help="Path to SQL Anywhere installation")

parser.add_argument('infile', type=argparse.FileType('rb'),
                    help="Input NVivo for Mac file (extension .nvpx)")
parser.add_argument('outfile', type=str, nargs='?',
                    help="Output normalised SQLite file")

args = parser.parse_args()

helperpath = os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'helpers' + os.path.sep

# On non-Windows OS, need to set up environment for SQL Anywhere server and restart process.
if os.name != 'nt':
    # Check if already done
    if not os.environ.get('_restart'):
        if args.sqlanywhere:
            os.environ['sqlanywhere'] = args.sqlanywhere
        envlines = subprocess.check_output(helperpath + 'sqlanyenv.sh', text=True).splitlines()
        for envline in envlines:
            env = re.match(r"(?P<name>\w+)=(?P<quote>['\"]?)(?P<value>.*)(?P=quote)", envline, re.MULTILINE | re.DOTALL).groupdict()
            os.environ[env['name']] = env['value']

        os.environ['_restart'] = 'TRUE'
        os.execve(sys.argv[0], sys.argv, os.environ)
else:
    dbengfile = None
    if args.sqlanywhere:
        dbengpaths = glob.glob(args.sqlanywhere + '\\dbeng*.exe')
        if dbengpaths:
            dbengfile = os.path.basename(dbengpaths[0])
    else:
        pathlist=os.environ['PATH'].split(';')
        for path in pathlist:
            dbengpaths = glob.glob(path + '\\dbeng*.exe')
            if dbengpaths:
                dbengfile = os.path.basename(dbengpaths[0])
                break

    if not dbengfile:
        raise RuntimeError("Could not find SQL Anywhere executable")

# Fill in extra arguments that NVivo module expects
args.mac     = True
args.windows = False

tmpinfilename = tempfile.mktemp()
tmpinfileptr  = open(tmpinfilename, 'wb')
tmpinfileptr.write(args.infile.read())
args.infile.close()
tmpinfileptr.close()

tmpoutfile = tempfile.mktemp()

if args.outfile is None:
    args.outfile = args.infile.name.rsplit('.',1)[0] + '.norm'
elif os.path.isdir(args.outfile):
    args.outfile = os.path.join(args.outfile,
                                os.path.basename(args.infile.name.rsplit('.',1)[0] + '.norm'))

# Find a free sock for SQL Anywhere server to bind to
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("",0))
freeport = str(s.getsockname()[1])
s.close()

DEVNULL = open(os.devnull, 'wb')

if os.name != 'nt':
    dbproc = subprocess.Popen(['sh', helperpath + 'sqlanysrv.sh', '-x TCPIP(port='+freeport+')', '-ga',  tmpinfilename, '-n', 'NVivo'+freeport], text=True,
                              stdout=subprocess.PIPE, stdin=DEVNULL)
    # Wait until SQL Anywhere engine starts...
    while dbproc.poll() is None:
        line = dbproc.stdout.readline()
        if line == 'Now accepting requests\n':
            break
else:
    dbproc = subprocess.Popen(['dbspawn', dbengfile, '-x TCPIP(port='+freeport+')', '-ga',  tmpinfilename, '-n', 'NVivo'+freeport], text=True,
                              stdout=subprocess.PIPE, stdin=DEVNULL)
    # Wait until SQL Anywhere engine starts...
    while dbproc.poll() is None:
        line = dbproc.stdout.readline()
        if 'SQL Anywhere Start Server In Background Utility' in line:
            break

if dbproc.poll() is not None:
    raise RuntimeError("Failed to start database server")

if args.verbosity > 0:
    print("Started database server on port " + freeport, file=sys.stderr)

args.indb = 'sqlalchemy_sqlany://wiwalisataob2aaf:iatvmoammgiivaam@localhost:' + freeport + '/NVivo' + freeport
args.outdb = 'sqlite:///' + tmpoutfile

chdir = os.environ.get('CHDIR')
if chdir:
    cwd = os.getcwd()
    os.chdir(chdir)

NVivo.Normalise(args)

if chdir:
    os.chdir(cwd)

if not args.cmdline:
    args.outfile = os.path.basename(args.outfile)

if os.path.exists(args.outfile):
    shutil.move(args.outfile, args.outfile + '.bak')

shutil.move(tmpoutfile, args.outfile)
# Need to change file mode so that delete works under Windows
os.chmod(tmpinfilename, 0o777)
os.remove(tmpinfilename)
