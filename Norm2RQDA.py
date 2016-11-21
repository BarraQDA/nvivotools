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

parser = argparse.ArgumentParser(description='Create an RQDA file from a normalised SQLite file.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-u', '--users', choices=["skip", "merge", "overwrite", "replace"], default="merge",
                    help='User action.')
parser.add_argument('-p', '--project', choices=["skip", "merge", "overwrite"], default="merge",
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

parser.add_argument('infile', type=str,
                    help="Input normalised SQLite file (extension .norm)")
parser.add_argument('outfile', type=str, nargs='?',
                    help="RQDA file to create.")

args = parser.parse_args()

import RQDA
import os
import shutil
import signal
from subprocess import Popen, PIPE
import tempfile
import time

try:
    if args.infile != '-':
        args.indb = 'sqlite:///' + args.infile

        if args.outfile is None:
            args.outfile = args.infile.rsplit('.',1)[0] + '.rqda'
    else:
        args.indb = '-'

    tmpoutfilename = tempfile.mktemp()
    tmpoutfileptr  = file(tmpoutfilename, 'wb')
    if os.path.isfile(args.outfile):
        shutil.copy(args.outfile, tmpoutfilename)
    args.outdb = 'sqlite:///' + tmpoutfilename

    RQDA.Denormalise(args)

    shutil.move(tmpoutfilename, args.outfile)

except:
    raise
    os.remove(tmpoutfilename)
