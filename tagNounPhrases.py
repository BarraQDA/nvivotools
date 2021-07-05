#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Jonathan Schultz
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

from argrecord import ArgumentHelper, ArgumentRecorder
import os
import sys
from NVivoNorm import NVivoNorm
from sqlalchemy import *
from textblob import TextBlob
import uuid
from datetime import datetime

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

def tagNounPhrases(arglist=None):

    parser = ArgumentRecorder(description='Analyse source text.')

    parser.add_argument(        'file',    type = str, input=True, output=True,
                                           help = 'Normalised NVivo (.nvpn) project file')            
    parser.add_argument('-u', '--user',    type=str,
                                           help='User name, default is project "modified by".')

    parser.add_argument('-sc', '--source-category', type=str,
                                                    help='Category of sources to process')
    parser.add_argument('-t', '--threshold',        type=int, default=0,
                                                    help="Miniumum number of occurrences for inclusion")

    parser.add_argument('-v', '--verbosity', type=int, default=1, private=True)
    parser.add_argument('-l', '--limit',     type=int, default=0,
                                             help="Limit number of sources to process")
    parser.add_argument('--logfile',         type=str, private=True,
                                             help="Logfile, default is <file>.log")
    parser.add_argument('--no-logfile',      action='store_true', 
                                             help='Do not output a logfile')

    args = parser.parse_args(arglist)

    if not args.no_logfile:
        logfilename = args.file.rsplit('.',1)[0] + '.log'
        incomments = ArgumentHelper.read_comments(logfilename) or ArgumentHelper.separator()
        logfile = open(logfilename, 'w')
        parser.write_comments(args, logfile, incomments=incomments)
        logfile.close()

    try:
        norm = NVivoNorm(args.file)
        norm.begin()

        datetimeNow = datetime.utcnow()
        
        if args.user:
            userRecord = norm.con.execute(select([
                    norm.User.c.Id
                ]).where(
                    norm.User.c.Name == bindparam('Name')
                ), {
                    'Name': args.user
                }).first()
            if userRecord:
                userId = userRecord['Id']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': args.user
                    })
        else:
            projectRecord = norm.con.execute(select([
                    norm.Project.c.ModifiedBy
                ])).first()
            if projectRecord:
                userId = projectRecord['ModifiedBy']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': u"Default User"
                    })
                norm.con.execute(norm.Project.insert(), {
                    'Version':      '0.2',
                    'Title':        "Created by NVivotools http://barraqda.org/nvivotools/",
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                })
        
        sourceSelect = select([
                            norm.Source.c.Name,
                            norm.Source.c.Content,
                            norm.Source.c.Id
                         ])
        if args.source_category:
            sourceCategoryNode = norm.con.execute(
                            select([norm.SourceCategory.c.Id]).
                            where(norm.SourceCategory.c.Name == bindparam('Category')),
                            { 'Category':  args.source_category }).first()
            if sourceCategoryNode:
                sourceCategoryId = sourceCategoryNode['Id']
            else:
                raise RuntimeError("Source category: ", args.source_category, " does not exist.")
                
            sourceSelect = sourceSelect.where(
                norm.Source.c.Category == bindparam('Category')
            )
        else:
            sourceCategoryId = None

        if args.limit:
            sourceSelect = sourceSelect.limit(args.limit)

        lemmaFrequency = {}
        for sourceRow in norm.con.execute(sourceSelect, { 'Category':  sourceCategoryId }):
            if args.verbosity > 1:
                print("Source: ", sourceRow['Name'])
            if sourceRow['Content']:
                content = TextBlob(sourceRow['Content'])
                nounPhrases = content.lower().noun_phrases
                lemmas = nounPhrases.lemmatize()
                for lemma in lemmas:
                    lemmaFrequency[lemma] = lemmaFrequency.get(lemma, 0) + 1

        lemmaNode = {}
        
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
                lemmaNode[node['Name']] = node['Id']
        else:
            if args.verbosity > 1:
                print("Creating Noun Phrases node category")
            nounPhraseNodeCategoryId = uuid.uuid4()
            norm.con.execute(norm.NodeCategory.insert().values({
                    'Id': nounPhraseNodeCategoryId,
                    'Name': 'Noun Phrases',
                    'CreatedBy': userId,
                    'CreatedDate': datetimeNow,
                    'ModifiedBy': userId,
                    'ModifiedDate': datetimeNow
                }))

        for lemma in lemmaFrequency.keys():
            if (args.threshold == 0 or lemmaFrequency[lemma] >= args.threshold) and not lemmaNode.get(lemma):
                if args.verbosity > 1:
                    print("Creating node: " + lemma)
                lemmaNode[lemma] = uuid.uuid4()
                norm.con.execute(norm.Node.insert().values({
                        'Id': lemmaNode[lemma],
                        'Category': nounPhraseNodeCategoryId,
                        'Name': lemma,
                        'Aggregate': False,
                        'CreatedBy': userId,
                        'CreatedDate': datetimeNow,
                        'ModifiedBy': userId,
                        'ModifiedDate': datetimeNow
                    }))

        for sourceRow in norm.con.execute(sourceSelect, { 'Category':  sourceCategoryId }):
            if args.verbosity > 1:
                print("Processing source: " + sourceRow['Name'])
            if sourceRow['Content']:
                content = TextBlob(sourceRow['Content'])
                for sentence in content.lower().sentences:
                    lemmas = sentence.noun_phrases.lemmatize()
                    for lemma in lemmas:
                        if lemma in lemmaFrequency.keys() and (args.threshold == 0 or lemmaFrequency[lemma] >= args.threshold):
                            if args.verbosity > 2:
                                print("    Inserting tagging: " + str(sentence.start) + ':' + str(sentence.end - 1))
                            norm.con.execute(norm.Tagging.insert().values({
                                    'Id':           uuid.uuid4(),
                                    'Source':       sourceRow['Id'],
                                    'Node':         lemmaNode[lemma],
                                    'Fragment':     str(sentence.start) + ':' + str(sentence.end - 1),
                                    'CreatedBy':    userId,
                                    'CreatedDate':  datetimeNow,
                                    'ModifiedBy':   userId,
                                    'ModifiedDate': datetimeNow
                                }))

        norm.commit()

    except:
        raise
        norm.rollback()

    finally:
        del norm

if __name__ == '__main__':
    tagNounPhrases(None)
