#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import sys
import unicodecsv
from textblob import TextBlob
import string
import unicodedata

parser = argparse.ArgumentParser(description='Mass twitter search.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-w', '--word', type=str, help='Base word.')
parser.add_argument('-t', '--threshold', type=float,
                    help='Threshold value for word to be output')
parser.add_argument('-l', '--limit', type=int, default=100,
                    help='Limit number of words to output')
parser.add_argument('-o', '--outfile', type=str, nargs='?',
                    help='Output file name, otherwise use stdout.')

parser.add_argument('infile', type=str, nargs='?',
                    help='Input CSV file, if missing use stdin.')

args = parser.parse_args()

wordlc = args.word.lower()

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Open output file already so we catch file error before doing all the hard work
if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

dictionary = {}

from nltk.corpus import stopwords
stop = set(stopwords.words('english'))

# We want to catch handles and hashtags so need to manage puncutation manually
from nltk.tokenize import RegexpTokenizer
tokenizer=RegexpTokenizer(r'https?://[^"\' ]+|[@|#]?\w+')

punctuationtable = dict.fromkeys(i for i in range(sys.maxunicode)
                                 if unicodedata.category(unichr(i)).startswith(u'P'))

inreader=unicodecsv.DictReader(infile)
for row in inreader:
    textblob = TextBlob(row['Text'], tokenizer=tokenizer)

    wordproximity = [(word.lemmatize().lower(), min([abs(index - foundindex)
                    for foundindex,foundword in enumerate(textblob.tokens)
                    if wordlc in foundword.lower()]))
                        for index,word in enumerate(textblob.tokens) if word.lower() not in stop]
    for word,proximity in wordproximity:
        if proximity > 0:

            if word not in dictionary.keys():
                dictionary[word] = 0

            dictionary[word] += 1.0 / proximity

sorteddictionary = sorted([{'word': word, 'proximity':dictionary[word]}
                                for word in dictionary.keys()
                                if dictionary[word] >= args.threshold or 0],
                           key=lambda item: item['proximity'],
                           reverse=True)

if args.limit != 0:
    sorteddictionary = sorteddictionary[0:args.limit]

outunicodecsv=unicodecsv.DictWriter(outfile, quoting=unicodecsv.QUOTE_ALL,
                                    fieldnames=['word', 'proximity'])
outunicodecsv.writeheader()
outunicodecsv.writerows(sorteddictionary)
outfile.close()
