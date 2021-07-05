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
import RQDA
import os
import shutil
import tempfile

parser = argparse.ArgumentParser(description='Convert a normalised NVivo project to RQDA.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-u', '--users', choices=["skip", "overwrite"], default="merge",
                    help='User action.')
parser.add_argument('-p', '--project', choices=["skip", "overwrite"], default="overwrite",
                    help='Project action.')
parser.add_argument('-nc', '--node-categories', choices=["skip", "overwrite"], default="merge",
                    help='Node category action.')
parser.add_argument('-n', '--nodes', choices=["skip", "overwrite"], default="merge",
                    help='Node action.')
parser.add_argument('-na', '--node-attributes', choices=["skip", "overwrite"], default="merge",
                    help='Node attribute table action.')
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

parser.add_argument('infile', type=argparse.FileType('rb'),
                    help="Input normalised (.norm) file")
parser.add_argument('outfilename', type=str, nargs='?',
                    help="Output RQDA file")

args = parser.parse_args()

tmpinfilename = tempfile.mktemp()
tmpinfileptr  = open(tmpinfilename, 'wb')
tmpinfileptr.write(args.infile.read())
args.infile.close()
tmpinfileptr.close()

tmpoutfilename = tempfile.mktemp()

if args.outfilename is None:
    args.outfilename = args.infile.name.rsplit('.',1)[0] + '.rqda'

args.indb  = 'sqlite:///' + tmpinfilename
args.outdb = 'sqlite:///' + tmpoutfilename

RQDA.Norm2RQDA(args)

shutil.move(tmpoutfilename, os.path.basename(args.outfilename))
os.remove(tmpinfilename)
