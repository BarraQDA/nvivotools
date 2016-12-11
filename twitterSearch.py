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

parser = argparse.ArgumentParser(description='Mass twitter search.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-c', '--count',  type=int, help='Number of tweets to return', default=100)

parser.add_argument('-o', '--outfile', type=str, nargs='?',
                    help='Output file name, otherwise use stdout.')

parser.add_argument('-m', '--maxid',  type=str, nargs='?',
                    help='Maximum status id.')

parser.add_argument('search', type=str, help='Search string.')

args = parser.parse_args()

## Hard code keys for now
consumer_key='ODCA5b8DKy7vi0CShoaWhCqvs'
consumer_secret='YTQZgVxSzMGKbbyTug5kU18GmTGrq89DsGNQbGIfFTEFyF2ZR9'

#REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
#ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'
#AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'
#SIGNIN_URL = 'https://api.twitter.com/oauth/authenticate'

#oauth_client = OAuth1Session(consumer_key, client_secret=consumer_secret, callback_uri='oob')

#print('\nRequesting temp token from Twitter...\n')

#try:
    #resp = oauth_client.fetch_request_token(REQUEST_TOKEN_URL)
#except ValueError as e:
    #raise 'Invalid response from Twitter requesting temp token: {0}'.format(e)

#url = oauth_client.authorization_url(AUTHORIZATION_URL)

#print('I will try to start a browser to visit the following Twitter page '
        #'if a browser will not start, copy the URL to your browser '
        #'and retrieve the pincode to be used '
        #'in the next step to obtaining an Authentication Token: \n'
        #'\n\t{0}'.format(url))

#webbrowser.open(url)
#pincode = raw_input('\nEnter your pincode? ')

#print('\nGenerating and signing request for an access token...\n')

#oauth_client = OAuth1Session(consumer_key, client_secret=consumer_secret,
                                #resource_owner_key=resp.get('oauth_token'),
                                #resource_owner_secret=resp.get('oauth_token_secret'),
                                #verifier=pincode)
#try:
    #resp = oauth_client.fetch_access_token(ACCESS_TOKEN_URL)
#except ValueError as e:
    #raise 'Invalid response from Twitter requesting temp token: {0}'.format(e)

#access_token_key = resp.get('oauth_token')
#access_token_secret = resp.get('oauth_token_secret')

api = twitter.Api(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            application_only_auth=True
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
