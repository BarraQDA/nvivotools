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

    singlegroup = parser.add_argument_group('Single tagging')
    singlegroup.add_argument('-s', '--source',      type=lambda s: unicode(s, 'utf8'))
    singlegroup.add_argument('-n', '--node',        type=lambda s: unicode(s, 'utf8'))
    singlegroup.add_argument('-f', '--fragment',    type=str)
    singlegroup.add_argument('-m', '--memo',        type=lambda s: unicode(s, 'utf8'))

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
                source, node, fragment, memo,
                verbosity, no_comments,
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

        if not no_comments:
            logfilename = outfile.rsplit('.',1)[0] + '.log'
            if os.path.isfile(logfilename):
                incomments = open(logfilename, 'r').read()
            else:
                incomments = ''
            logfile = open(logfilename, 'w')
            logfile.write(comments)
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
            csvreader=unicodecsv.DictReader(csvFile, fieldnames=csvfieldnames)
            taggingRows = []
            for row in csvreader:
                taggingRow = dict(row)
                taggingRow['Source']   = taggingRow.get('Source',   source)
                taggingRow['Node']     = taggingRow.get('Node',     node)
                taggingRow['Fragment'] = taggingRow.get('Fragment', fragment)
                taggingRow['Memo']     = taggingRow.get('Memo',     memo)
                taggingRows.append(taggingRow)
        else:
            taggingRows = [{
                'Source':   source,
                'Node':     node,
                'Fragment': fragment,
                'Memo':     memo
            }]

        taggings = []
        for taggingRow in taggingRows:
            source = taggingRow['Source']
            if source:
                sourceRecord = norm.con.execute(select([
                            norm.Source.c.Id
                        ]).where(
                            norm.Source.c.Name == bindparam('Source')
                        ), {
                            'Source': source
                        }).first()
                if sourceRecord is None:
                    raise RuntimeError("Source: " + source + " not found.")

                nodeRecord = None
                if taggingRow['Node']:
                    for node in taggingRow['Node'].splitlines():
                        nodeRecord = norm.con.execute(select([
                                    norm.Node.c.Id
                                ]).where(
                                    norm.Node.c.Name == bindparam('Node')
                                ), {
                                    'Node': node
                                }).first()
                        if nodeRecord is None:
                            raise RuntimeError("Node: " + node + " not found.")

                        taggings.append({
                                'Id':           uuid.uuid4(),
                                'Source':       sourceRecord['Id'],
                                'Node':         nodeRecord['Id'],
                                'Fragment':     taggingRow['Fragment'],
                                'Memo':         taggingRow['Memo'],
                                'CreatedBy':    userId,
                                'CreatedDate':  datetimeNow,
                                'ModifiedBy':   userId,
                                'ModifiedDate': datetimeNow
                            })
                elif taggingRow['Memo']:
                    taggings.append({
                            'Id':           uuid.uuid4(),
                            'Source':       sourceRecord['Id'],
                            'Node':         None,
                            'Fragment':     taggingRow['Fragment'],
                            'Memo':         taggingRow['Memo'],
                            'CreatedBy':    userId,
                            'CreatedDate':  datetimeNow,
                            'ModifiedBy':   userId,
                            'ModifiedDate': datetimeNow
                        })

        if taggings:
            norm.con.execute(norm.Tagging.insert(), taggings)

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
