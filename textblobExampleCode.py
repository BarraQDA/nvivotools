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
from sqlalchemy import *
from sqlalchemy import exc
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

parser.add_argument('indb', type=str,
                    help="Normalised project file to analyse")

args = parser.parse_args()

from textblob import TextBlob

try:
    normdb = create_engine(args.indb)
    normmd = MetaData(bind=normdb)
    normmd.reflect(normdb)

    normcon = normdb.connect()
    normtr = normcon.begin()

    normProject = normmd.tables.get('Project')
    user = normcon.execute(select([normProject.c.ModifiedBy.label('User')])).first()['User']
    now = datetime.datetime.now()

    normSource = normmd.tables.get('Source')
    sources = [dict(row) for row in normcon.execute(select([
            normSource.c.Id,
            normSource.c.Name,
            normSource.c.Content
        ]).where(
            normSource.c.Content.isnot(None)
        ))]
    lemmafrequency = {}
    for source in sources:
        if args.verbosity > 1:
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

    normNode = normmd.tables.get('Node')
    normTagging = normmd.tables.get('Tagging')
    nounPhraseNode = normcon.execute(select([
            normNode.c.Id
        ]).where(
            normNode.c.Name == bindparam('Name')
        ), {
            'Name': 'Noun Phrases'
        }).first()
    if nounPhraseNode is not None:
        if args.verbosity > 1:
            print("Found Noun Phrases node")
        nounPhrasesNodeId = nounPhraseNode['Id']
        for node in normcon.execute(select([
                normNode.c.Id
            ]).where(
                normNode.c.Parent == bindparam('Parent')
            ), {
                'Parent': nounPhrasesNodeId
            }):
            normcon.execute(normTagging.delete(
                normTagging.c.Node == bindparam('Node')
            ), {
                'Node': node['Id']
            })
        normcon.execute(normNode.delete(
                normNode.c.Parent == bindparam('Id')
            ), {
                'Id': nounPhrasesNodeId
            })

    else:
        if args.verbosity > 1:
            print("Creating Noun Phrases node")
        nounPhrasesNodeId = uuid.uuid4()
        normcon.execute(normNode.insert().values({
                'Id': nounPhrasesNodeId,
                'Name': 'Noun Phrases',
                'Aggregate': True,
                'CreatedBy': user,
                'CreatedDate': now,
                'ModifiedBy': user,
                'ModifiedDate': now
            }))

    lemmanode = {}
    for lemma in lemmafrequency.keys():
        if args.threshold == 0 or lemmafrequency[lemma] >= args.threshold:
            if args.verbosity > 1:
                print("Creating node: " + lemma)
            lemmanode[lemma] = uuid.uuid4()
            normcon.execute(normNode.insert().values({
                    'Id': lemmanode[lemma],
                    'Parent': nounPhrasesNodeId,
                    'Name': lemma,
                    'Aggregate': False,
                    'CreatedBy': user,
                    'CreatedDate': now,
                    'ModifiedBy': user,
                    'ModifiedDate': now
                }))

    for source in sources:
        if args.verbosity > 1:
            print("Processing source: " + source['Name'])
        content = TextBlob(source['Content'])
        for sentence in content.lower().sentences:
            lemmas = sentence.noun_phrases.lemmatize()
            for lemma in lemmas:
                if lemma in lemmafrequency.keys() and (args.threshold == 0 or lemmafrequency[lemma] >= args.threshold):
                    if args.verbosity > 2:
                        print("    Inserting tagging: " + str(sentence.start) + ':' + str(sentence.end - 1))
                    normcon.execute(normTagging.insert().values({
                            'Id':           uuid.uuid4(),
                            'Source':       source['Id'],
                            'Node':         lemmanode[lemma],
                            'Fragment':     str(sentence.start) + ':' + str(sentence.end - 1),
                            'CreatedBy':    user,
                            'CreatedDate':  now,
                            'ModifiedBy':   user,
                            'ModifiedDate': now
                        }))

    normtr.commit()
    normtr = None
    normcon.close()
    normdb.dispose()


except:
    raise
