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
import sys
import unicodecsv
import string
import unicodedata
import pymp
import re

parser = argparse.ArgumentParser(description='Twitter feed regular expression.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('--textblob', action='store_true', help='Use textblob for analysis')

parser.add_argument('-j', '--jobs', type=int, help='Number of parallel tasks, default is number of CPUs')

parser.add_argument('-r', '--regexp', type=str, help='Regular expression.')
parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
parser.add_argument('-t', '--threshold', type=float,
                    help='Threshold value for word to be output')
parser.add_argument('-l', '--limit', type=int, default=0,
                    help='Limit number of words to output')
parser.add_argument('-o', '--outfile', type=str, nargs='?',
                    help='Output file name, otherwise use stdout.')

parser.add_argument('infile', type=str, nargs='?',
                    help='Input CSV file, if missing use stdin.')

args = parser.parse_args()

if args.jobs is None:
    import multiprocessing
    args.jobs = multiprocessing.cpu_count()

if args.regexp is None:
    raise RuntimeError("Regular expression must be provided.")

if args.verbosity > 1:
    print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Open output file already so we catch file error before doing all the hard work
if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

if args.ignorecase:
    regexp = re.compile(args.regexp, re.IGNORECASE)
else:
    regexp = re.compile(args.regexp)

inreader=unicodecsv.DictReader(infile)
rows = [row['text'] for row in inreader]
rowcount = len(rows)
fields = list(regexp.groupindex)
mergedresults = pymp.shared.dict()
with pymp.Parallel(args.jobs) as p:
    results = {}
    for rowindex in p.range(0, rowcount):
        matches = regexp.finditer(rows[rowindex])
        for match in matches:
            if args.ignorecase:
                index = tuple(value.lower() for value in match.groupdict().values())
            else:
                index = tuple(match.groupdict().values())
            results[index] = results.get(index, 0) + 1

    with p.lock:
        if args.verbosity > 1:
            print("Merging " + str(len(results)) + " results from thread: " + str(p.thread_num), file=sys.stderr)

        for index in results:
            mergedresults[index] = mergedresults.get(index, 0) + results[index]

if args.verbosity > 1:
    print("Sorting " + str(len(mergedresults)) + " results.", file=sys.stderr)

sortedresults = sorted([{'match': match, 'score':mergedresults[match]}
                                for match in mergedresults.keys()
                                if mergedresults[match] >= args.threshold or 0],
                           key=lambda item: item['score'],
                           reverse=True)

if args.limit != 0:
    sortedresults = sortedresults[0:args.limit]

for result in sortedresults:
    for idx in range(len(fields)):
        result[fields[idx]] = result['match'][idx]

outfile.write('# ' + args.regexp)
if args.ignorecase:
    outfile.write(', re.IGNORECASE')
outfile.write('\n')

outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=fields + ['score'], extrasaction='ignore')
outunicodecsv.writeheader()
if len(sortedresults) > 0:
    outunicodecsv.writerows(sortedresults)
outfile.close()
