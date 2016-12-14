#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import sys
import unicodecsv
import string
import unicodedata
import pymp

parser = argparse.ArgumentParser(description='Word proximity calculator.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('--textblob', action='store_true', help='Use textblob for analysis')

parser.add_argument('-j', '--jobs', type=int, help='Number of parallel tasks, default is number of CPUs')

parser.add_argument('-k', '--keyword', type=str, help='Key word for search.')
parser.add_argument('-t', '--threshold', type=float,
                    help='Threshold value for word to be output, default=100')
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

if args.keyword is None:
	raise("Keyword must be provided.")

keywordlc = args.keyword.lower()

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Open output file already so we catch file error before doing all the hard work
if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')


from nltk.corpus import stopwords
stop = set(stopwords.words('english'))

if args.textblob:
    # We want to catch handles and hashtags so need to manage punctuation manually
    from textblob import TextBlob
    from nltk.tokenize import RegexpTokenizer
    tokenizer=RegexpTokenizer(r'https?://[^"\' ]+|[@|#]?\w+')

# Separators are all symbols and punctuation except hashtag ('#') and mention ('@')
#separators = u''.join(set(unichr(i) for i in range(sys.maxunicode) if unicodedata.category(unichr(i))[0] in ['P','Z','S']) - set([u'#', u'@']))

inreader=unicodecsv.DictReader(infile)
rows = [dict(row) for row in inreader]
rowcount = len(rows)
mergedscore = pymp.shared.dict()
with pymp.Parallel(args.jobs) as p:
	score = {}
	for rowindex in p.range(0, rowcount):
		if args.textblob:
			textblob = TextBlob(rows[rowindex]['text'], tokenizer=tokenizer)
			wordlist = textblob.tokens
		else:
			wordlist = rows[rowindex]['text'].split()

		keywordindices = [index for index,word in enumerate(wordlist)
								if keywordlc in word.lower()]
		if len(keywordindices) > 0:
			if args.textblob:
				wordproximity = [(word.lemmatize().lower(), min([abs(index - keywordindex) for keywordindex in keywordindices]))]
			else:
				wordproximity = [(word.lower(), min([abs(index - keywordindex) for keywordindex in keywordindices]))
								for index,word in enumerate(wordlist) if word.lower() not in stop]

			for word,proximity in wordproximity:
				if proximity > 0:
					#wordscore = 1.0
					wordscore = 1.0 / proximity
					if word not in score.keys():
						score[word] = wordscore
					else:
						score[word] += wordscore

	with p.lock:
		# Accessing shared dict is really slow so do everything to minimise
		mergedscorekeys = mergedscore.keys()
		for word in score.keys():
			if word in mergedscorekeys:
				mergedscore[word] += score[word]
			else:
				mergedscore[word]  = score[word]
				mergedscorekeys += {word}

sortedscore = sorted([{'word': word, 'score':mergedscore[word]}
                                for word in mergedscore.keys()
                                if mergedscore[word] >= args.threshold or 0],
                           key=lambda item: item['score'],
                           reverse=True)

if args.limit != 0:
    sortedscore = sortedscore[0:args.limit]

outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=['word', 'score'])
outunicodecsv.writeheader()
outunicodecsv.writerows(sortedscore)
outfile.close()
