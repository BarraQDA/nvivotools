#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016 Jonathan Schultz
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
import argparse
from NVivoNorm import NVivoNorm
from sqlalchemy import *
import re
import csv
import shutil

def queryTagging(arglist=None):

    parser = ArgumentRecorder(description="Query taggings in a normalised file.")

    generalgroup = parser.add_argument_group('General')
    generalgroup.add_argument(      'file',      type=str,
                                                 help='Normalised NVivo (.nvpn) file')
    generalgroup.add_argument('-o', '--outfile', type=str,
                                                 help='Output CSV file')
    generalgroup.add_argument('-s',  '--source',          type=str)
    generalgroup.add_argument('-sc', '--source-category', type=str)
    generalgroup.add_argument('-n',  '--node', nargs='*', type=str)
    generalgroup.add_argument('-nc', '--node-category',   type=str)

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity', type=int, default=1, private=True)
    advancedgroup.add_argument('-l', '--limit',     type=int, default=0,
                                                    help="Limit number of sources to process")
    advancedgroup.add_argument('--no-comments',     action='store_true', 
                                                    help='Do not output comments in header of output file')

    args = parser.parse_args(arglist)
    
    try:
        norm = NVivoNorm(args.file)
        norm.begin()

        sourcesel = select([
                norm.Source.c.Name.label('Source'),
                norm.Node.c.Name.label('Node'),
                norm.Source.c.Content,
                norm.Tagging.c.Fragment,
                norm.Tagging.c.Memo
            ]).where(
                norm.Source.c.Id == norm.Tagging.c.Source,
            ).select_from(
                norm.Tagging.outerjoin(
                    norm.Node,
                    norm.Tagging.c.Node == norm.Node.c.Id)
            )
        params = {}

        if args.source:
            sourcesel = sourcesel.where(
                norm.Source.c.Name == bindparam('Source')
            )
            params.update({'Source': args.source})

        if args.source_category:
            sourcesel = sourcesel.where(and_(
                norm.Source.c.Category == norm.SourceCategory.c.Id,
                norm.SourceCategory.c.Name == bindparam('SourceCategory')
            ))
            params.update({'SourceCategory': args.source_category})

        tagginglist = []
        if args.node_category:
            sourceselnodecat = sourcesel.where(and_(
                norm.Node.c.Category == norm.NodeCategory.c.Id,
                norm.NodeCategory.c.Name == bindparam('NodeCategory')
            ))
            params.update({'NodeCategory': args.node_category})
            tagginglist.append([dict(row) for row in norm.con.execute(sourceselnodecat, params)])

        if args.node:
            sourceselnode = sourcesel.where(and_(
                norm.Tagging.c.Node == norm.Node.c.Id,
                norm.Node.c.Name == bindparam('Node')
            ))
            for nodeiter in args.node:
                params.update({'Node': nodeiter})

                tagginglist.append([dict(row) for row in norm.con.execute(sourceselnode, params)])
        elif not args.node_category:
            tagginglist = [[dict(row) for row in norm.con.execute(sourcesel, params)]]

        fragmentregex = re.compile(r'(?P<start>[0-9]+):(?P<end>[0-9]+)')
        for taggings in tagginglist:
            for tagging in taggings:
                matchfragment = fragmentregex.match(tagging['Fragment'])
                tagging['Start'] = int(matchfragment.group('start'))
                tagging['End']   = int(matchfragment.group('end'))

        def sortandmergetagginglist(tagginglist):
            tagginglist.sort(key = lambda tagging: (tagging['Source'], tagging['NodeTuple'], tagging['Start'], tagging['End']))
            idx = 0
            while idx < len(tagginglist) - 1:
                if  tagginglist[idx]['Source']    == tagginglist[idx+1]['Source']    \
                and tagginglist[idx]['NodeTuple'] == tagginglist[idx+1]['NodeTuple'] \
                and tagginglist[idx]['End'] >= tagginglist[idx+1]['Start']:
                    tagginglist[idx]['End'] = max(tagginglist[idx]['End'], tagginglist[idx+1]['End'])
                    del tagginglist[idx+1]
                else:
                    idx += 1

        intersection = tagginglist[0]
        for tagging in intersection:
            if tagging.get('Node'):
                tagging['NodeTuple'] = (tagging['Node'],)
            else:
                tagging['NodeTuple'] = ()

        sortandmergetagginglist(intersection)
        for taggings in tagginglist[1:]:
            idx = 0
            newintersection = []
            for intagging in intersection:
                for tagging in taggings:
                    if tagging['Source'] == intagging['Source']:
                        newstart = max(tagging['Start'], intagging['Start'])
                        newend   = min(tagging['End'],   intagging['End'])
                        if newend >= newstart:
                            newintersection.append({'Source': tagging['Source'],
                                                    'NodeTuple': (tagging['Node'],) +  intagging['NodeTuple'],
                                                    'Content': tagging['Content'],
                                                    'Start': newstart,
                                                    'End':   newend})

            intersection = newintersection
            sortandmergetagginglist(intersection)

        if args.outfile:
            if os.path.exists(args.outfile):
                shutil.move(args.outfile, args.outfile + '.bak')

            csvfile = open(args.outfile, 'w')
        else:
            csvfile = sys.stdout

        if not args.no_comments:
            parser.write_comments(args, csvfile, incomments=ArgumentHelper.separator())

        csvwriter = csv.DictWriter(csvfile,
                                   fieldnames=['Source', 'Node', 'Memo', 'Text', 'Fragment'],
                                   extrasaction='ignore',
                                   lineterminator=os.linesep,
                                   quoting=csv.QUOTE_NONNUMERIC)

        csvwriter.writeheader()

        for tagging in intersection:
            tagging['Fragment'] = str(tagging['Start']) + ':' + str(tagging['End'])
            tagging['Text'] = tagging['Content'][tagging['Start']-1:tagging['End']]
            tagging['Node'] = os.linesep.join(nodeiter for nodeiter in tagging['NodeTuple'])

        csvwriter.writerows(intersection)
        csvfile.close()

    except:
        raise

    finally:
        del norm

if __name__ == '__main__':
    queryTagging(None)
