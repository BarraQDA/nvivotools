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
import unicodecsv
from sqlalchemy import *
import re
from dateutil import parser as dateparser
from datetime import date, time, datetime
from distutils import util
import uuid

def add_arguments(parser):
    parser.description = "Insert or update tagging in normalised file.'"

    generalgroup = parser.add_argument_group('General')
    generalgroup.add_argument('-o', '--outfile', type=str, required=True,
                                                 help='Output normalised NVivo (.norm) file')
    generalgroup.add_argument(        'infile',  type=str, nargs='?',
                                                 help='Input CSV file containing tagging info')
    generalgroup.add_argument('-u', '--user',    type=lambda s: unicode(s, 'utf8'),
                                                 help='User name, default is project "modified by".')

    functiongroup = parser.add_argument_group('Function tagging')
    functiongroup.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    functiongroup.add_argument('-s', '--source',      type=lambda s: unicode(s, 'utf8'))
    functiongroup.add_argument('-sc', '--source-category', type=lambda s: unicode(s, 'utf8'))
    functiongroup.add_argument('-n', '--node',        type=lambda s: unicode(s, 'utf8'))
    functiongroup.add_argument('-t', '--tagging',     type=lambda s: unicode(s, 'utf8'),
                               help="Python script that returns list of taggings as dictionary")

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-l', '--limit',     type=int, help='Limit number of CSV rows to process')
    advancedgroup.add_argument('-v', '--verbosity', type=int, default=1)
    advancedgroup.add_argument(      '--dry-run',   action='store_true',
                                                    help='Print but do not execute command')
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
    for argname, argval in kwargs.iteritems():
        if argname not in hiddenargs:
            if type(argval) == str or type(argval) == unicode:
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
                prelude, source, source_category, node, tagging,
                limit, verbosity, dry_run, no_comments,
                comments, **dummy):

    try:

        # Read and skip comments at start of CSV file.
        csvcomments = ''
        if infile:
            csvFile = file(infile, 'r')

            while True:
                line = csvFile.readline()
                if line[:1] == '#':
                    csvcomments += line
                else:
                    csvfieldnames = next(unicodecsv.reader([line]))
                    break

        if prelude:
            if verbosity >= 1:
                print("Executing prelude code.", file=sys.stderr)

            exec(os.linesep.join(prelude), globals())

        if tagging:
            exec("\
def evaltagging(sourceRec, csvRow):\n\
    return " + tagging, globals())

        if not no_comments:
            logfilename = outfile.rsplit('.',1)[0] + '.log'
            if os.path.isfile(logfilename):
                incomments = open(logfilename, 'r').read()
            else:
                incomments = ''
            logfile = open(logfilename, 'w')
            logfile.write(comments)
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
            csvRows = unicodecsv.DictReader(csvFile, fieldnames=csvfieldnames)
        else:
            # Dummy input CSV file
            csvRows = [{}]

        inrowcount = 0
        taggingRecs = []
        for csvRow in csvRows:
            if limit and inrowcount == limit:
                break
            inrowcount += 1

            csvRow = dict(csvRow)
            sourceSel = select([norm.Source.c.Id, norm.Source.c.Content])
            sourceParams = {}
            csvRow['source'] = csvRow.get('source') or source
            csvRow['source category'] = csvRow.get('source category') or source_category
            csvRow['node'] = csvRow.get('node') or node
            if csvRow['source']:
                sourceSel = sourceSel.where(norm.Source.c.Name == bindparam('Source'))
                sourceParams.update({'Source': csvRow['source']})
            if csvRow['source category']:
                sourceSel = sourceSel.where(and_(
                    norm.Source.c.Category == norm.SourceCategory.c.Id,
                    norm.SourceCategory.c.Name == bindparam('SourceCategory')
                ))
                sourceParams.update({'SourceCategory': csvRow['source category']})

            sourceRecs = norm.con.execute(sourceSel, {'Source': source})
            for sourceRec in sourceRecs:
                taggings = evaltagging(sourceRec, csvRow)
                for tagging in taggings:
                    nodeRecord = None
                    if tagging.get('Node'):
                        nodeRec = norm.con.execute(select([
                                    norm.Node.c.Id,
                                ]).where(
                                    norm.Node.c.Name == bindparam('Node')
                                ), {
                                    'Node': tagging['Node']
                                }).first()
                        if nodeRec is None:
                            raise RuntimeError("Node: " + tagging['Node'] + " not found.")

                        taggingRecs.append({
                                'Id':           uuid.uuid4(),
                                'Source':       sourceRec['Id'],
                                'Node':         nodeRec['Id'],
                                'Fragment':     tagging['Fragment'],
                                'Memo':         tagging.get('Memo'),
                                'CreatedBy':    userId,
                                'CreatedDate':  datetimeNow,
                                'ModifiedBy':   userId,
                                'ModifiedDate': datetimeNow
                            })
                    elif taggingRow['Memo']:
                        taggingRecs.append({
                                'Id':           uuid.uuid4(),
                                'Source':       sourceRecord['Id'],
                                'Node':         None,
                                'Fragment':     tagging['Fragment'],
                                'Memo':         tagging['Memo'],
                                'CreatedBy':    userId,
                                'CreatedDate':  datetimeNow,
                                'ModifiedBy':   userId,
                                'ModifiedDate': datetimeNow
                            })

        if dry_run:
            print(taggingRecs)
        else:
            if taggingRecs:
                norm.con.execute(norm.Tagging.insert(), taggingRecs)

            norm.commit()

    except:
        raise
        norm.rollback()

    #finally:
        #del norm

def main():
    kwargs = parse_arguments()
    kwargs['comments'] = build_comments(kwargs)
    kwargs['func'](**kwargs)

if __name__ == '__main__':
    main()
