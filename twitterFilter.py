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
import sys
import unicodecsv
from textblob import TextBlob
import string
import unicodedata

parser = argparse.ArgumentParser(description='Filter twitter CSV file on text column.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-s', '--search', type=str, help='Search string to filter twitter content.')
parser.add_argument('-o', '--outfile', type=str, nargs='?',
                    help='Output file name, otherwise use stdout.')

parser.add_argument('infile', type=str,
                    help='Input CSV file, of "-" to use stdin.')

args = parser.parse_args()

searchlc = args.search.lower()

if args.infile == '-':
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

inreader=unicodecsv.DictReader(infile)

# Open output file already so we catch file error before doing all the hard work
if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

fieldnames = [ 'username', 'date', 'retweets', 'favorites', 'text', 'lang', 'geo', 'mentions', 'hashtags', 'id', 'permalink']
csvwriter=unicodecsv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
csvwriter.writeheader()

for row in inreader:
    if searchlc in row['text'].lower():
        csvwriter.writerow(row)

outfile.close()
