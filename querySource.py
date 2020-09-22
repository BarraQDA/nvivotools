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

import os
import sys
import argparse
from NVivoNorm import NVivoNorm
from sqlalchemy import *
import re
import csv
import shutil

def add_arguments(parser):
    parser.description = "Query sources in a normalised file."

    generalgroup = parser.add_argument_group('General')
    generalgroup.add_argument(      'infile',   type=str,
                                                help='Input normalised NVivo (.nvpn) file')
    generalgroup.add_argument('-o', '--outfile', type=str,
                                                 help='Output file')
    generalgroup.add_argument('-s',  '--source', type=str)
    generalgroup.add_argument('-c', '--category', type=str)

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity',  type=int, default=1)
    advancedgroup.add_argument('--no-comments', action='store_true', help='Do not produce a comments logfile')

    parser.set_defaults(func=querySource)
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

def querySource(infile, outfile,
                 source, category,
                 verbosity, no_comments,
                 comments, **dummy):

    try:
        norm = NVivoNorm(infile)
        norm.begin()

        sourcesel = select([
                norm.Source.c.Name,
                norm.Source.c.Description,
                norm.SourceCategory.c.Name.label('Category'),
                norm.Source.c.Color,
                norm.Source.c.Content
            ]).select_from(
                norm.Source.outerjoin(norm.SourceCategory,
                norm.SourceCategory.c.Id == norm.Source.c.Category)
            )
        params = {}

        if source:
            sourcesel = sourcesel.where(
                norm.Source.c.Name == bindparam('Source')
            )
            params.update({'Source': source})

        if category:
            sourcesel = sourcesel.where(and_(
                norm.Source.c.Category == norm.SourceCategory.c.Id,
                norm.SourceCategory.c.Name == bindparam('SourceCategory')
            ))
            params.update({'SourceCategory': category})

        if outfile:
            if os.path.exists(outfile):
                shutil.move(outfile, outfile + '.bak')

            csvfile = open(outfile, 'w')
        else:
            csvfile = sys.stdout

        if not no_comments:
            csvfile.write(comments)
            csvfile.write('#' * 80 + '\n')

        csvwriter = csv.DictWriter(csvfile,
                                          fieldnames=['Name', 'Description', 'Content', 'Category', 'Color'],
                                          extrasaction='ignore',
                                          lineterminator=os.linesep,
                                          quoting=csv.QUOTE_NONNUMERIC)

        csvwriter.writeheader()

        for source in norm.con.execute(sourcesel, params):
            csvwriter.writerow(dict(source))

        csvfile.close()

    except:
        raise

    finally:
        del norm

def main():
    kwargs = parse_arguments()
    kwargs['comments'] = build_comments(kwargs)
    kwargs['func'](**kwargs)

if __name__ == '__main__':
    main()
