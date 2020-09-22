#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016, 2020 Jonathan Schultz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
import argparse
from NVivoNorm import NVivoNorm
from sqlalchemy import *
from textblob import TextBlob
import uuid
import datetime


exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())


parser = argparse.ArgumentParser(description='Analyse source text.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-l', '--limit', type=int, default=0,
                    help="Limit number of sources to process")
parser.add_argument('-t', '--threshold', type=int, default=0,
                    help="Miniumum number of occurrences for inclusion")

parser.add_argument('nvpn', type=str,
                    help="Normalised project file to analyse")

args = parser.parse_args()

try:
    norm = NVivoNorm(args.nvpn)
    norm.begin()

    user = norm.con.execute(select([norm.Project.c.ModifiedBy.label('User')])).first()['User']
    now = datetime.datetime.now()

    sources = [dict(row) for row in norm.con.execute(select([
            norm.Source.c.Id,
            norm.Source.c.Name,
            norm.Source.c.Content
        ]).where(
            norm.Source.c.Content.isnot(None)
        ))]
    lemmafrequency = {}
    for source in sources:
        if args.verbosity > 2:
            print("Reading source: " + source['Name'])
        content = TextBlob(source['Content'])
        noun_phrases = content.lower().noun_phrases
        lemmas = noun_phrases.lemmatize()
        for lemma in lemmas:
            if lemma in lemmafrequency.keys():
                lemmafrequency[lemma] += 1
            else:
                lemmafrequency[lemma] = 1

        if args.limit > 0:
            args.limit -= 1
            if args.limit == 0:
                break

    lemmanode = {}
    
    nounPhraseNodeCategory = norm.con.execute(select([
            norm.NodeCategory.c.Id
        ]).where(
            norm.NodeCategory.c.Name == 'Noun Phrases'
        )).first()
    if nounPhraseNodeCategory is not None:
        nounPhraseNodeCategoryId = nounPhraseNodeCategory['Id']
        if args.verbosity > 1:
            print("Found Noun Phrases node category ID ", nounPhraseNodeCategoryId)

        for node in norm.con.execute(select([norm.Node.c.Id, norm.Node.c.Name]).
                                    where(norm.Node.c.Category == nounPhraseNodeCategoryId)):
            lemmanode[node['Name']] = node['Id']
    else:
        if args.verbosity > 1:
            print("Creating Noun Phrases node category")
        nounPhraseNodeCategoryId = uuid.uuid4()
        norm.con.execute(norm.NodeCategory.insert().values({
                'Id': nounPhraseNodeCategoryId,
                'Name': 'Noun Phrases',
                'CreatedBy': user,
                'CreatedDate': now,
                'ModifiedBy': user,
                'ModifiedDate': now
            }))

    for lemma in lemmafrequency.keys():
        if (args.threshold == 0 or lemmafrequency[lemma] >= args.threshold) and not lemmanode.get(lemma):
            if args.verbosity > 1:
                print("Creating node: " + lemma)
            lemmanode[lemma] = uuid.uuid4()
            norm.con.execute(norm.Node.insert().values({
                    'Id': lemmanode[lemma],
                    'Category': nounPhraseNodeCategoryId,
                    'Name': lemma,
                    'Aggregate': False,
                    'CreatedBy': user,
                    'CreatedDate': now,
                    'ModifiedBy': user,
                    'ModifiedDate': now
                }))

    for source in sources:
        if args.verbosity > 2:
            print("Processing source: " + source['Name'])
        content = TextBlob(source['Content'])
        for sentence in content.lower().sentences:
            lemmas = sentence.noun_phrases.lemmatize()
            for lemma in lemmas:
                if lemma in lemmafrequency.keys() and (args.threshold == 0 or lemmafrequency[lemma] >= args.threshold):
                    if args.verbosity > 2:
                        print("    Inserting tagging: " + str(sentence.start) + ':' + str(sentence.end - 1))
                    norm.con.execute(norm.Tagging.insert().values({
                            'Id':           uuid.uuid4(),
                            'Source':       source['Id'],
                            'Node':         lemmanode[lemma],
                            'Fragment':     str(sentence.start) + ':' + str(sentence.end - 1),
                            'CreatedBy':    user,
                            'CreatedDate':  now,
                            'ModifiedBy':   user,
                            'ModifiedDate': now
                        }))

    norm.commit()

except:
    raise
    norm.rollback()

finally:
    del norm
