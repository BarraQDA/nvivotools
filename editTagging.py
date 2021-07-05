#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016-7 Jonathan Schultz
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

from __future__ import print_function
import os
import sys
import argparse
from NVivoNorm import NVivoNorm
import csv
from sqlalchemy import *
from datetime import datetime
import uuid
import re

def add_arguments(parser):
    parser.description = "Insert or update taggings in normalised file.'"

    generalgroup = parser.add_argument_group('General')
    generalgroup.add_argument('-o', '--outfile', type=str, required=True,
                                                 help='Output normalised NVivo (.norm) file')
    generalgroup.add_argument(        'infile',  type=str, nargs='?',
                                                 help='Input CSV file containing tagging info')
    generalgroup.add_argument('-u', '--user',    type=str,
                                                 help='User name, default is project "modified by".')

    tagginggroup = parser.add_argument_group('Tagging')
    tagginggroup.add_argument('-s', '--source',      type=str)
    tagginggroup.add_argument('-sc', '--source-category', type=str)
    tagginggroup.add_argument('-n', '--node',        type=str,
                              help="Multiple nodes may be specified, separated by line terminator")
    tagginggroup.add_argument('-f', '--fragment',    type=str)
    tagginggroup.add_argument('-m', '--memo',        type=str)

    tagginggroup.add_argument('-p', '--prelude',     type=str, nargs="*", help='Python code to execute before processing')
    tagginggroup.add_argument('-t', '--tagging',     type=str,
                               help="Tagging to define. Can be either a string representing a string slice, or Python code that returns list of taggings as dictionary with keys 'Node', 'Memo' and/or 'Fragment'")

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity', type=int, default=1)
    advancedgroup.add_argument('--no-comments',     action='store_true',
                                                    help='Do not produce a comments logfile')

    parser.set_defaults(func=editTagging)
    parser.set_defaults(build_comments=build_comments)
    parser.set_defaults(hiddenargs=['hiddenargs', 'verbosity', 'no_comments'])

def parse_arguments():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    return vars(parser.parse_args())

def build_comments(kwargs):
    comments = ((' ' + kwargs['outfile'] + ' ') if kwargs['outfile'] else '').center(80, '#') + '\n'
    comments += '# ' + os.path.basename(__file__) + '\n'
    hiddenargs = kwargs['hiddenargs'] + ['hiddenargs', 'func', 'build_comments']
    for argname, argval in kwargs.items():
        if argname not in hiddenargs:
            if type(argval) == str:
                comments += '#     --' + argname + '="' + argval + '"\n'
            elif type(argval) == bool:
                if argval:
                    comments += '#     --' + argname + '\n'
            elif type(argval) == list:
                for valitem in argval:
                    if type(valitem) == str:
                        comments += '#     --' + argname + '="' + valitem + '"\n'
                    else:
                        comments += '#     --' + argname + '=' + str(valitem) + '\n'
            elif argval is not None:
                comments += '#     --' + argname + '=' + str(argval) + '\n'

    return comments

def editTagging(outfile, infile, user,
                source, node, fragment, memo,
                source_category, prelude, tagging,
                verbosity, no_comments,
                comments, **dummy):

    try:

        norm = None
        if prelude:
            if verbosity >= 1:
                print("Executing prelude code.", file=sys.stderr)

            exec(os.linesep.join(prelude), globals())

        if tagging:
            exec("\
def evaltagging(sourceRow, csvRow):\n\
    return " + tagging, globals())

        # Read and skip comments at start of CSV file.
        csvcomments = ''
        if infile:
            csvFile = open(infile, 'r')

            while True:
                line = csvFile.readline()
                if line[:1] == '#':
                    csvcomments += line
                else:
                    csvfieldnames = next(csv.reader([line]))
                    break

        if not no_comments:
            logfilename = outfile.rsplit('.',1)[0] + '.log'
            if os.path.isfile(logfilename):
                incomments = open(logfilename, 'r').read()
            else:
                incomments = ''
            logfile = open(logfilename, 'w')
            logfile.write(comments.encode('utf8'))
            logfile.write(csvcomments)
            logfile.write(incomments)
            logfile.close()

        norm = NVivoNorm(outfile)
        norm.begin()

        datetimeNow = datetime.utcnow()

        if user:
            user = norm.con.execute(select([
                    norm.User.c.Id
                ]).where(
                    norm.User.c.Name == bindparam('Name')
                ), {
                    'Name': user
                }).first()
            if user:
                userId = user['Id']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': user
                    })
        else:
            project = norm.con.execute(select([
                    norm.Project.c.ModifiedBy
                ])).first()
            if project:
                userId = project['ModifiedBy']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': "Default User"
                    })
                norm.con.execute(norm.Project.insert(), {
                    'Version': '0.2',
                    'Title': "Created by NVivotools http://barraqda.org/nvivotools/",
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                })

        if infile:
            csvRows = csv.DictReader(csvFile, fieldnames=csvfieldnames)
        else:
            csvRows = [{}]

        fragmentregex = re.compile(r'(?P<start>[0-9]+):(?P<end>[0-9]+)')

        inrowcount = 0
        taggingRows = []
        for csvRow in csvRows:
            csvRow = dict(csvRow)
            sourceSel = select([
                            norm.Source.c.Id,
                            norm.Source.c.Name,
                            norm.SourceCategory.c.Name.label('Category'),
                            norm.Source.c.Content
                        ]).select_from(
                            norm.Source.outerjoin(norm.SourceCategory,
                            norm.SourceCategory.c.Id == norm.Source.c.Category)
                        )
            sourceParams = {}
            source          = csvRow.get('Source') or source
            source_category = csvRow.get('Source Category') or source_category
            node            = csvRow.get('Node')            or node
            fragment        = csvRow.get('Fragment')        or fragment
            memo            = csvRow.get('Memo')            or memo

            if source:
                sourceSel = sourceSel.where(norm.Source.c.Name == bindparam('Source'))
                sourceParams.update({'Source': source})
            if source_category:
                sourceSel = sourceSel.where(and_(
                    norm.Source.c.Category == norm.SourceCategory.c.Id,
                    norm.SourceCategory.c.Name == bindparam('SourceCategory')
                ))
                sourceParams.update({'SourceCategory': source_category})

            sourceRows = norm.con.execute(sourceSel, {'Source': source})
            for sourceRow in sourceRows:
                if tagging:
                    taggingList = evaltagging(sourceRow, csvRow)
                else:
                    taggingList = [{'Node': node, 'Fragment': fragment, 'Memo': memo}]

                for taggingItem in taggingList:
                    fragmentmatch = fragmentregex.match(taggingItem['Fragment'])
                    if fragmentmatch:
                        fragmentstart = int(fragmentmatch.group('start'))
                        fragmentend   = int(fragmentmatch.group('end'))

                    if not fragmentmatch or fragmentstart > len(sourceRow['Content']) or fragmentend < fragmentstart or fragmentend > len(sourceRow['Content']):
                        print(sourceRow['Content'], len(sourceRow['Content']))
                        raise RuntimeError("Illegal fragment specification: " + taggingItem['Fragment'] + " Content length is " + str(len(sourceRow['Content'])))

                    if taggingItem.get('Node'):
                        for nodeItem in taggingItem['Node'].splitlines():
                            nodeRec = norm.con.execute(select([
                                        norm.Node.c.Id,
                                    ]).where(
                                        norm.Node.c.Name == bindparam('Node')
                                    ), {
                                        'Node': nodeItem
                                    }).first()
                            if nodeRec is None:
                                raise RuntimeError("Node: " + nodeItem + " not found.")

                            taggingRows.append({
                                    'Id':           uuid.uuid4(),
                                    'Source':       sourceRow['Id'],
                                    'Node':         nodeRec['Id'],
                                    'Fragment':     taggingItem['Fragment'],
                                    'Memo':         taggingItem.get('Memo'),
                                    'CreatedBy':    userId,
                                    'CreatedDate':  datetimeNow,
                                    'ModifiedBy':   userId,
                                    'ModifiedDate': datetimeNow
                                })
                    elif taggingItem.get('Memo'):
                        taggingRows.append({
                                'Id':           uuid.uuid4(),
                                'Source':       sourceRow['Id'],
                                'Node':         None,
                                'Fragment':     taggingItem['Fragment'],
                                'Memo':         taggingItem['Memo'],
                                'CreatedBy':    userId,
                                'CreatedDate':  datetimeNow,
                                'ModifiedBy':   userId,
                                'ModifiedDate': datetimeNow
                            })

        if taggingRows:
            norm.con.execute(norm.Tagging.insert(), taggingRows)

        if verbosity >= 1:
            print("Inserted", len(taggingRows), "taggings.", file=sys.stderr)

        norm.commit()

    except:
        raise
        norm.rollback()

    finally:
        del norm

def main():
    kwargs = parse_arguments()
    kwargs['comments'] = build_comments(kwargs)
    kwargs['func'](**kwargs)

if __name__ == '__main__':
    main()
