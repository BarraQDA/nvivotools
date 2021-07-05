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

from __future__ import print_function
import os
import sys
import argparse
from sqlalchemy import *
from sqlalchemy import exc
import re

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())


parser = argparse.ArgumentParser(description='Extract tagging from normalised file.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-l', '--limit', type=int, default=0,
                    help="Limit number of sources to process")
parser.add_argument('-n', '--node', type=str, nargs='?',
                    help="Name of node")
parser.add_argument('-s', '--source', type=str, nargs='?',
                    help="Name of source")

parser.add_argument('infile', type=str,
                    help="Normalised project file to analyse")

args = parser.parse_args()

from textblob import TextBlob

try:
    normdb = create_engine('sqlite:///' + args.infile)
    normmd = MetaData(bind=normdb)

    normNode = Table('Node', normmd, autoload=True)
    normSource = Table('Source', normmd, autoload=True)
    normTagging = Table('Tagging', normmd, autoload=True)

    sel = select([
            normTagging.c.Fragment,
            normNode.c.Name.label('NodeName'),
            normSource.c.Name.label('SourceName'),
            normSource.c.Content
        ]).where(and_(
            normTagging.c.Node == normNode.c.Id,
            normSource.c.Id == normTagging.c.Source
        ))
    if args.node is not None:
        sel = sel.where(
                normNode.c.Name == bindparam('NodeName')
            )
    if args.source is not None:
        sel = sel.where(
            normSource.c.Name == bindparam('SourceName')
        )

    taggings = normdb.execute(sel, {
            'NodeName':   args.node,
            'SourceName': args.source
        })

    for tagging in taggings:
        print("Node: " + tagging['NodeName'] + " Source: " + tagging['SourceName'] + "[" + tagging['Fragment'] + "]", file=sys.stderr)

        matchfragment = re.match("([0-9]+):([0-9]+)(?:,([0-9]+)(?::([0-9]+))?)?", tagging['Fragment'])
        if matchfragment is None:
            print("WARNING: Unrecognised tagging fragment", file=sys.stderr)
        else:
            print(tagging['Content'][int(matchfragment.group(1)):int(matchfragment.group(2))+1], file=sys.stderr)

        print("", file=sys.stderr)

    normdb.dispose()


except:
    raise
