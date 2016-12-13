#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
from requests_oauthlib import OAuth1Session
import webbrowser
import twitter
import sys
import unicodecsv
import string
import pytz
from datetime import datetime


import warnings
warnings.simplefilter("error", DeprecationWarning)

parser = argparse.ArgumentParser(description='Mass twitter search.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

# Twitter authentication stuff
parser.add_argument('-a', '--application-only-auth', action='store_true')
parser.add_argument('--access-token-key', type=str,
                    help='Access token key for Twitter authentication')
parser.add_argument('--access-token-secret', type=str,
                    help='Access token secret for Twitter authentication')

parser.add_argument('-c', '--count',  type=int, help='Number of tweets to return', default=100)

parser.add_argument('-o', '--outfile', type=str, nargs='?',
                    help='Output file name, otherwise use stdout.')

parser.add_argument('-m', '--maxid',  type=str, nargs='?',
                    help='Maximum status id.')

parser.add_argument('search', type=str, help='Search string.')

args = parser.parse_args()

# Twitter URLs
REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'
AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'
SIGNIN_URL = 'https://api.twitter.com/oauth/authenticate'

# Hard code keys for now
consumer_key='ODCA5b8DKy7vi0CShoaWhCqvs'
consumer_secret='YTQZgVxSzMGKbbyTug5kU18GmTGrq89DsGNQbGIfFTEFyF2ZR9'

if args.application_only_auth:
    api = twitter.Api(
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                application_only_auth=True
        )
else:
    if args.access_token_key is None or args.access_token_secret is None:
        oauth_client = OAuth1Session(consumer_key, client_secret=consumer_secret, callback_uri='oob')

        resp = oauth_client.fetch_request_token(REQUEST_TOKEN_URL)
        url = oauth_client.authorization_url(AUTHORIZATION_URL)

        sys.stderr.write('Opening browser for Twitter authentication: ' + url + '\n')

        webbrowser.open(url)
        sys.stderr.write('Enter your pincode? ')
        pincode = raw_input()

        oauth_client = OAuth1Session(consumer_key, client_secret=consumer_secret,
                                    resource_owner_key=resp.get('oauth_token'),
                                    resource_owner_secret=resp.get('oauth_token_secret'),
                                    verifier=pincode)
        resp = oauth_client.fetch_access_token(ACCESS_TOKEN_URL)
        args.access_token_key = resp.get('oauth_token')
        args.access_token_secret = resp.get('oauth_token_secret')

        sys.stderr.write('To re-use access token next time use the following arguments:\n')
        sys.stderr.write('    --access-token-key ' + args.access_token_key + ' --access-token-secret ' + args.access_token_secret + '\n')

    api = twitter.Api(
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                access_token_key=args.access_token_key,
                access_token_secret=args.access_token_secret
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
        'id',
        'username',
        'date',
        'text',
        'lang',
        'retweets',
        'favorites'
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
