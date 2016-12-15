#!/usr/bin/python
# -*- coding: utf-8 -*-

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
parser.add_argument('-t', '--threshold', type=float,
                    help='Threshold value for word to be output')
parser.add_argument('-l', '--limit', type=int, default=100,
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

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Open output file already so we catch file error before doing all the hard work
if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

regexp = re.compile(args.regexp)

inreader=unicodecsv.DictReader(infile)
rows = [dict(row) for row in inreader]
rowcount = len(rows)
mergedresults = pymp.shared.dict()
mergedfields = None
with pymp.Parallel(args.jobs) as p:
	results = {}
	fields = None
	for rowindex in p.range(0, rowcount):
		match = regexp.search(rows[rowindex]['text'])
		if match is not None:
			regexpdict = match.groupdict()
			if fields is None:
				fields = list(regexpdict)
			index = tuple(regexpdict.values())
			results[index] = results.get(index, 0) + 1

	with p.lock:
		if fields is not None and mergedfields is None:
			mergedfields = fields

		# Accessing shared dict is really slow so do everything to minimise
		mergedresultskeys = mergedresults.keys()
		for match in results.keys():
			if match in mergedresultskeys:
				mergedresults[match] += results[match]
			else:
				mergedresults[match]  = results[match]
				mergedresultskeys += {match}

sortedresults = sorted([{'match': match, 'score':mergedresults[match]}
                                for match in mergedresults.keys()
                                if mergedresults[match] >= args.threshold or 0],
                           key=lambda item: item['score'],
                           reverse=True)

if args.limit != 0:
    sortedresults = sortedresults[0:args.limit]

for result in sortedresults:
	for idx in range(len(mergedfields)):
		result[mergedfields[idx]] = result['match'][idx]

outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=mergedfields + ['score'], extrasaction='ignore')
outunicodecsv.writeheader()
outunicodecsv.writerows(sortedresults)
outfile.close()
