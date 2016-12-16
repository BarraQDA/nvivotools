#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import sys
import unicodecsv
import urllib,urllib2,json,re,datetime,sys,cookielib
from pyquery import PyQuery
from dateutil import parser as dateparser

parser = argparse.ArgumentParser(description='Scrape twitter feed using pyquery.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-u', '--user', type=str, 				help='Twitter username to match.')
parser.add_argument('--since', type=str, 					help='Lower bound search date (YYYY-MM-DD).')
parser.add_argument('--until', type=str, 					help='Upper bound search date (YYYY-MM-DD).')
parser.add_argument('-l', '--language', type=str, 			help='Language code to filter.')
parser.add_argument('-q', '--query', type=str, 				help='Search string to filter twitter content.')
parser.add_argument('-t', '--top', action='store_true', 	help='Search for top tweets.')
parser.add_argument('--limit', type=int, 					help='Limit number of tweets to output.')
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
	# Real all lines to find last id
	lastid = None
	lastdate = None
	for row in inreader:
		lastid = row['id']
		lastdate = row['date']

	if args.until is None and lastdate is not None:
		args.until = dateparser.parse(lastdate).strftime("%Y-%m-%d")
		sys.stderr.write("Continuing scrape from: " + args.until + '\n')

	csvwriter=unicodecsv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
	overlap = False
else:
	sys.stderr.write("Creating new output file...\n")
	fieldnames = [ 'username', 'date', 'retweets', 'favorites', 'text', 'lang', 'geo', 		'mentions', 'hashtags', 'id', 'permalink']
	csvwriter=unicodecsv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
	csvwriter.writeheader()
	overlap = True

refreshCursor = ''

results = []
resultsAux = []
cookieJar = cookielib.CookieJar()

if args.user is not None and (args.user.startswith("\'") or args.user.startswith("\"")) and (args.user.endswith("\'") or args.user.endswith("\"")):
	args.user = args.user[1:-1]

active = True
freshtweets = False
dateSec = None
abortAfter = 2
abortCount = 0

while active:
	if args.top:
		url = "https://twitter.com/i/search/timeline?q=%s&src=typd&max_position=%s"
	else:
		url = "https://twitter.com/i/search/timeline?f=tweets&q=%s&src=typd&max_position=%s"

	urlGetData = ''
	if args.language is not None:
		urlGetData += ' lang:' + args.language

	if args.user:
		urlGetData += ' from:' + args.user

	if args.since:
		urlGetData += ' since:' + args.since

	if args.until:
		urlGetData += ' until:' + args.until

	if args.query:
		urlGetData += ' ' + args.query

	url = url % (urllib.quote(urlGetData), refreshCursor)

	headers = [
		('Host', "twitter.com"),
		('User-Agent', "Mozilla/5.0 (Windows NT 6.1; Win64; x64)"),
		('Accept', "application/json, text/javascript, */*; q=0.01"),
		('Accept-Language', "de,en-US;q=0.7,en;q=0.3"),
		('X-Requested-With', "XMLHttpRequest"),
		('Referer', url),
		('Connection', "keep-alive")
	]

	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookieJar))
	opener.addheaders = headers

	try:
		dataJson = json.loads(opener.open(url).read())
	except:
		raise
		sys.stderr.write("Twitter weird response. Try to see on browser: " + url + '\n')
		dataJson = None


	tweets = None
	if dataJson is not None and len(dataJson['items_html'].strip()) > 0:
		refreshCursor = dataJson['min_position']
		tweets = PyQuery(dataJson['items_html'])('div.js-stream-tweet')

	if tweets is None or len(tweets) == 0:
		if not freshtweets or dateSec is None:
			abortCount += 1
			if abortCount == abortAfter:
				break
			sys.stderr.write("Retrying...\n")

		freshtweets = False
		# Set 'until' criterion to date of last retrieved tweet
		if dateSec is not None:
			args.until = datetime.datetime.utcfromtimestamp(dateSec).strftime("%Y-%m-%d")
			sys.stderr.write("Setting until criteria to " + args.until + '\n')

		refreshCursor = ''
		overlap = False
		continue

	for tweetHTML in tweets:
		tweetPQ = PyQuery(tweetHTML)

		# Get timestamp to help resume
		dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"));

		# Skip retweets
		retweet = tweetPQ("span.js-retweet-text").text()
		if retweet != '':
			continue

		# Ignore non-descending tweet IDs. This can happen when we do a restart
		# following search exhaustion.
		id = tweetPQ.attr("data-tweet-id");
		if lastid is not None and id >= lastid:
			overlap = True
			if freshtweets:
				raise "Out of order tweets!"

			continue

		freshtweets = True
		abortCount = 0

		if not overlap:
			overlap = True
			sys.stderr.write("Possible missing tweets...\n")
			# Add an empty tweet to signal possible missing tweets
			csvwriter.writerow({})

		usernameTweet = tweetPQ("span.username.js-action-profile-name b").text();
		lang = tweetPQ("p.js-tweet-text").attr("lang")
		txt = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text().replace('# ', '#').replace('@ ', '@'));
		retweets = int(tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""));
		favorites = int(tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""));
		permalink = tweetPQ.attr("data-permalink-path");

		geo = ''
		geoSpan = tweetPQ('span.Tweet-geo')
		if len(geoSpan) > 0:
			geo = geoSpan.attr('title')

		tweet = {}
		tweet['id'] = id
		tweet['lang'] = lang
		tweet['permalink'] = 'https://twitter.com' + permalink
		tweet['username'] = usernameTweet
		tweet['text'] = txt
		tweet['date'] = datetime.datetime.fromtimestamp(dateSec)
		tweet['retweets'] = retweets
		tweet['favorites'] = favorites
		tweet['mentions'] = " ".join(re.compile('(@\\w*)').findall(tweet['text']))
		tweet['hashtags'] = " ".join(re.compile('(#\\w*)').findall(tweet['text']))
		tweet['geo'] = geo

		csvwriter.writerow(tweet)

		if args.limit is not None and len(results) >= args.limit:
			active = False
			break

outfile.close()
