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
from datetime import datetime
import re
import uuid

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

def tagSpeakers(arglist=None):

    parser = ArgumentRecorder(description='Insert or update source in normalised file.')

    parser.add_argument(        'file',    type = str, input=True, output=True,
                                           help = 'Normalised NVivo (.nvpn) project file')            
    parser.add_argument('-u', '--user',    type=str,
                                           help='User name, default is project "modified by".')

    parser.add_argument('-sc', '--source-category', type=str,
                                                    help='Category of sources to process')
    parser.add_argument('-nc', '--node-category',   type=str,
                                                    help='Category of nodes to process')

    parser.add_argument('-v', '--verbosity', type=int, default=1, private=True)
    parser.add_argument('-l', '--limit',     type=int,
                                             help='Limit number of lines from table file')
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

        nodeSelect = select([
                            norm.Node.c.Name,
                            norm.Node.c.Id
                         ])
        if args.node_category:
            nodeCategoryId = norm.con.execute(
                            select([norm.NodeCategory.c.Id]).
                            where(norm.NodeCategory.c.Name == bindparam('Category')),
                            { 'Category':  args.node_category }).first()['Id']
            
            nodeSelect = nodeSelect.where(
                norm.Node.c.Category == bindparam('Category')
            )
        else:
            nodeCategoryId = None
            
        speakers = { row['Name']: row['Id'] for row in norm.con.execute(nodeSelect, { 'Category':  nodeCategoryId }) }

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

        speakerPattern = re.compile(r'^(.+?):\s*(.*?)$', re.MULTILINE)
        taggingRows = []
        for sourceRow in norm.con.execute(sourceSelect, { 'Category':  sourceCategoryId }):
            if args.verbosity > 1:
                print("Source: ", sourceRow['Name'])
            speakerMatches = speakerPattern.finditer(sourceRow['Content'])
            for speakerMatch in speakerMatches:
                speakerName = speakerMatch.group(1)
                speakerId = speakers.get(speakerName, None)
                if speakerId:
                    fragment = str(speakerMatch.start(2)) + ':' + str(speakerMatch.end(2))
                    taggingRows.append({
                            'Id':           uuid.uuid4(),
                            'Source':       sourceRow['Id'],
                            'Node':         speakerId,
                            'Fragment':     fragment,
                            'CreatedBy':    userId,
                            'CreatedDate':  datetimeNow,
                            'ModifiedBy':   userId,
                            'ModifiedDate': datetimeNow
                        })
                    
        if taggingRows:
            norm.con.execute(norm.Tagging.insert(), taggingRows)

        if args.verbosity > 1:
            print("Inserted", len(taggingRows), "taggings.", file=sys.stderr)

        norm.commit()

    except:
        raise
        norm.rollback()

    finally:
        del norm

if __name__ == '__main__':
    tagSpeakers(None)
