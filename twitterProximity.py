#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import sys
import unicodecsv
import string
import unicodedata

parser = argparse.ArgumentParser(description='Word proximity calculator.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-k', '--keyword', type=str, help='Key word for search.')
parser.add_argument('-t', '--threshold', type=float,
                    help='Threshold value for word to be output')
parser.add_argument('-l', '--limit', type=int, default=100,
                    help='Limit number of words to output')
parser.add_argument('-o', '--outfile', type=str, nargs='?',
                    help='Output file name, otherwise use stdout.')

parser.add_argument('infile', type=str, nargs='?',
                    help='Input CSV file, if missing use stdin.')

args = parser.parse_args()

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

# We want to catch handles and hashtags so need to manage puncutation manually
from nltk.tokenize import RegexpTokenizer
tokenizer=RegexpTokenizer(r'https?://[^"\' ]+|[@|#]?\w+')

#punctuationtable = dict.fromkeys(i for i in range(sys.maxunicode)
                                 #if unicodedata.category(unichr(i)).startswith(u'P'))

inreader=unicodecsv.DictReader(infile)
rows = [dict(row) for row in inreader]
score = pymp.shared.dict()
for rowindex in range(0, len(rows)):
	#textblob = TextBlob(rows[rowindex]['text'], tokenizer=tokenizer)
	#wordlist = textblob.tokens
	wordlist = rows[rowindex]['text'].split()

	keywordindices = [index for index,word in enumerate(wordlist)
								if keywordlc in word.lower()]
	if len(keywordindices) > 0:
		#wordproximity = [(word.lemmatize().lower(), min([abs(index - keywordindex) for keywordindex in keywordindices]))
		wordproximity = [(word.lower(), min([abs(index - keywordindex) for keywordindex in keywordindices]))
							for index,word in enumerate(wordlist) if word.lower() not in stop]
		for word,proximity in wordproximity:
			if proximity > 0:
				wordscore = 1.0
				#wordscore = 1.0 / proximity
				if word not in score.keys():
					score[word] = wordscore
				else:
					score[word] += wordscore

sortedscore = sorted([{'word': word, 'score':score[word]}
                                for word in score.keys()
                                if score[word] >= args.threshold or 0],
                           key=lambda item: item['score'],
                           reverse=True)

if args.limit != 0:
    sortedscore = sortedscore[0:args.limit]

outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=['word', 'score'])
outunicodecsv.writeheader()
outunicodecsv.writerows(sortedscore)
outfile.close()
