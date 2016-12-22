import urllib,urllib2,json,re,datetime,sys,cookielib
from pyquery import PyQuery
from dateutil import parser as dateparser

class TwitterFeed(object):
	def __init__(self, language=None, user=None, since=None, until=None, query=None):
		self.url = 'https://twitter.com/i/search/timeline?f=tweets&q='
		self.url += (' lang:' + language) if language is not None else ''
		self.url += (' from:' + user)     if user     is not None else ''
		self.url += (' since:' + since)   if since    is not None else ''
		self.url += (' until:' + until)   if until    is not None else ''
		self.url += (' ' + query)         if query    is not None else ''
		self.url += '&src=typd&max_position='
		self.position = ''
		self.opener = None
		self.cookieJar = cookielib.CookieJar()
		self.tweets = None

	def next(self):
		if self.opener is None:
			self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookieJar))
			opener.addheaders = [
				('Host', "twitter.com"),
				('User-Agent', "Mozilla/5.0 (Windows NT 6.1; Win64; x64)"),
				('Accept', "application/json, text/javascript, */*; q=0.01"),
				('Accept-Language', "de,en-US;q=0.7,en;q=0.3"),
				('X-Requested-With', "XMLHttpRequest"),
				('Referer', url),
				('Connection', "keep-alive")
			]

		while True:
			if self.tweets is None:
				try:
					dataJson = json.loads(opener.open(self.url + self.position).read())
					if dataJson is not None and len(dataJson['items_html'].strip()) > 0:
						self.position = dataJson['min_position']
						self.tweets = PyQuery(dataJson['items_html'])('div.js-stream-tweet')
				except:
					sys.stderr.write("Twitter weird response. Try to see on browser: " + url + '\n')
					raise

			if self.tweets is None:
				raise StopIteration

			try:
				tweetPQ = PyQuery(next(self.tweets))
			except StopIteration:
				self.tweets = None
				continue

			# Skip retweets
			retweet = tweetPQ("span.js-retweet-text").text()
			if retweet != '':
				continue

			# Build tweet as dictionary
			ret = {}

			ret['id']        = tweetPQ.attr("data-tweet-id");
			ret['date']      = datetime.datetime.utcfromtimestamp(
									int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"))).strftime("%Y-%m-%d %H:%M:%S")
			ret['user']      = tweetPQ("span.username.js-action-profile-name b").text();
			ret['lang']      = tweetPQ("p.js-tweet-text").attr("lang")
			ret['text']      = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text().replace('# ', '#').replace('@ ', '@'));
			ret['retweets']  = int(tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""));
			ret['favorites'] = int(tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""));
			ret['permalink'] = 'https://twitter.com' + tweetPQ.attr("data-permalink-path");

			geoSpan = tweetPQ('span.Tweet-geo')
			ret['geo'] = geoSpan.attr('title') if len(geoSpan) > 0 else ''

			ret['mentions']  = " ".join(re.compile('(@\\w*)').findall(ret['text']))
			ret['hashtags']  = " ".join(re.compile('(#\\w*)').findall(ret['text']))

			return ret
