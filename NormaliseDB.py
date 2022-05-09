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

parser = argparse.ArgumentParser(description='Normalise an offloaded NVivo project.')
parser.add_argument('-w', '--windows', action='store_true',
                    help='Correct NVivo for Windows string coding. Use if names and descriptions appear wierd.')
parser.add_argument('-m', '--mac',  action='store_true',
                    help='Use NVivo for Mac database format.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-nv', '--nvivoversion', choices=["10", "11"], default="10",
                    help='NVivo version (10 or 11)')

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

parser.add_argument('indb', type=str,
                    help='SQLAlchemy path of input NVivo database or "-" to create empty project.')
parser.add_argument('outdb', type=str, nargs='?',
                    help='SQLAlchemy path of output normalised database.')

args = parser.parse_args()

NVivo.Normalise(args)
