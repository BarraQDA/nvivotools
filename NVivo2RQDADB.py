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
import RQDA
import os
import shutil
import tempfile

parser = argparse.ArgumentParser(description='Convert an NVivo project to an RQDA project.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-w', '--windows', action='store_true',
                    help='Correct NVivo for Windows string coding. Use if offloaded file will be used with Windows version of NVivo.')
parser.add_argument('-m', '--mac',  action='store_true',
                    help='Use NVivo for Mac database format.')
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

parser.add_argument('innvivodb', type=str,
                    help="Input database")
parser.add_argument('outrqdadb', type=str, nargs='?',
                    help="Output database")

args = parser.parse_args()

if args.outrqdadb is None:
    args.outrqdadb = args.innvivodb.rsplit('.',1)[0] + '.rqda'

tmpnormfilename = tempfile.mktemp()

args.indb  = args.innvivodb
args.outdb = 'sqlite:///' + tmpnormfilename

NVivo.Normalise(args)

args.node_attributes = args.case_attributes

args.indb  = 'sqlite:///' + tmpnormfilename
args.outdb = args.outrqdadb
RQDA.Norm2RQDA(args)

os.remove(tmpnormfilename)
