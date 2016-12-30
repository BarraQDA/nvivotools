#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse
import sys
import unicodecsv
from TwitterFeed import TwitterFeed

parser = argparse.ArgumentParser(description='Scrape twitter feed using pyquery.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-u', '--user', type=str,                 help='Twitter username to match.')
parser.add_argument('--since', type=str,                     help='Lower bound search date (YYYY-MM-DD).')
parser.add_argument('--until', type=str,                     help='Upper bound search date (YYYY-MM-DD).')
parser.add_argument('-l', '--language', type=str,             help='Language code to filter.')
parser.add_argument('-q', '--query', type=str,                 help='Search string to filter twitter content.')
parser.add_argument('-t', '--top', action='store_true',     help='Search for top tweets.')
parser.add_argument('--limit', type=int,                     help='Limit number of tweets to output.')
parser.add_argument('-o', '--outfile', type=str, nargs='?', help='Output file name, otherwise use stdout.')

args = parser.parse_args()

if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'a+')
    # Position file at end. File open mode should do this already but apparently
    # doesn't work correctly
    outfile.seek(0,2)

lastid = None
if args.outfile is not None and outfile.tell() > 0:
    outfile.seek(0,0)
    inreader=unicodecsv.DictReader(outfile)
    fieldnames = inreader.fieldnames
    # Read all lines to find last id
    lastid = None
    lastdate = None
    for row in inreader:
        lastid = row['id']
        lastdate = row['date']

    if args.until is None and lastdate is not None:
        args.until = dateparser.parse(lastdate).strftime("%Y-%m-%d")
        print("Continuing scrape from: " + args.until, file=sys.stderr)

    csvwriter=unicodecsv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
    overlap = False
else:
    print("Creating new output file...", file=sys.stderr)
    fieldnames = [ 'user', 'date', 'retweets', 'favorites', 'text', 'lang', 'geo',         'mentions', 'hashtags', 'id', 'permalink']
    csvwriter=unicodecsv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
    csvwriter.writeheader()
    overlap = True

if args.user is not None and (args.user.startswith("\'") or args.user.startswith("\"")) and (args.user.endswith("\'") or args.user.endswith("\"")):
    args.user = args.user[1:-1]

freshtweets = False
abortAfter = 2
abortCount = 0
tweetCount = 0
lastDate = None

twitterfeed = TwitterFeed(args.language, args.user, args.since, args.until, args.query)

while True:
    try:
        tweet = twitterfeed.next()
    except StopIteration:
        abortCount += 1
        if abortCount == abortAfter:
            break
        print("Retrying...", file=sys.stderr)
        twitterfeed = TwitterFeed(args.language, args.user, args.since,
                            lastDate.strftime("%Y-%m-%d") if lastDate is not None else args.until,
                            args.query)

        break

    # Ignore non-descending tweet IDs. This can happen when we do a restart
    # following search exhaustion.
    if lastid is not None and tweet['id'] >= lastid:
        overlap = True
        if freshtweets:
            raise RuntimeError("Out of order tweets! Id: " + tweet['id'] + " came after " + lastid)

        continue

    freshtweets = True
    abortCount = 0

    if not overlap:
        overlap = True
        # Add an empty tweet to signal possible missing tweets
        print("Possible missing tweets between id: " + lastid + " and " + tweet['id'], file=sys.stderr)
        csvwriter.writerow({})

    csvwriter.writerow(tweet)
    lastid = tweet['id']
    tweetCount += 1

    if args.limit is not None and tweetCount >= args.limit:
        break

outfile.close()
