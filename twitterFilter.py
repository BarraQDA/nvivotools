#!/usr/bin/python
# -*- coding: utf-8 -*-

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
