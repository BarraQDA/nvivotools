#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import twitter
import sys
import unicodecsv
import string
import pytz
from datetime import datetime

parser = argparse.ArgumentParser(description='Mass twitter search.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-c', '--count',  type=int, help='Number of tweets to return', default=100)

parser.add_argument('-o', '--outfile', type=str, nargs='?',
                    help='Output file name, otherwise use stdout.')

parser.add_argument('-m', '--maxid',  type=str, nargs='?',
                    help='Maximum status id.')

parser.add_argument('search', type=str, help='Search string.')

args = parser.parse_args()

# Hard code keys for now
api = twitter.Api(
            consumer_key='ODCA5b8DKy7vi0CShoaWhCqvs',
            consumer_secret='YTQZgVxSzMGKbbyTug5kU18GmTGrq89DsGNQbGIfFTEFyF2ZR9',
            access_token_key='780828796555399168-Fo9gqAru4jRVlrfP4EGhmqE9ae8E3li',
            access_token_secret='uJNTOAHiV6blaHgy5Qq0rXNWW8nD2MBDtdtTyrM6ZIoms'
      )

searchlc = args.search.lower()
totaltweets = 0
maxquerycount = 100
if args.maxid is None:
    maxid = None
else:
    maxid = int(args.maxid)

if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

outunicodecsv=unicodecsv.writer(outfile, quoting=unicodecsv.QUOTE_ALL)
outunicodecsv.writerow([
        'ID',
        'User',
        'Date (UTC)',
        'Text',
        'Language',
        'Retweets',
        'Favorites'
    ])

while totaltweets < args.count:
    querycount = min(args.count-totaltweets, maxquerycount)
    query = 'q='+args.search+'&count='+str(querycount)
    if maxid is not None:
        query += '&max_id='+str(maxid)
    if args.verbosity > 1:
        sys.stderr.write('Query: ' + query + '\n')
    tweets = api.GetSearch(raw_query=query)
    if len(tweets) == 0:
        break

    for tweet in tweets:
        if tweet.retweeted_status is None \
        and searchlc in tweet.text.lower():
            row = [
                    "'" + tweet.id_str,
                    tweet.user.screen_name,
                    datetime.fromtimestamp(tweet.created_at_in_seconds, tz=pytz.UTC).isoformat(),
                    tweet.text,
                    tweet.lang,
                    tweet.retweet_count,
                    tweet.favorite_count
                ]
            outunicodecsv.writerow(row)
            totaltweets += 1
            if totaltweets % maxquerycount == 0:
                sys.stderr.write('.')

    maxid = tweets[-1].id - 1

if totaltweets >= maxquerycount:
    sys.stderr.write('\n')

outfile.close()
