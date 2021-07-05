#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Jonathan Schultz
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
import fnmatch
from NVivoNorm import NVivoNorm
from sqlalchemy import *

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

def saveSources(arglist):

    parser = argparse.ArgumentParser(description='Save sources from a normalised NVivo file',
                                    fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)
    parser.add_argument('--no-comments', action='store_true', help='Do not produce a comments logfile')

    parser.add_argument('-s', '--source',  type=str, default = '%',
                                           help='Source or name or pattern')

    parser.add_argument('-p', '--path', type=str, default='.',
                        help='Output file directory')
    parser.add_argument('infile',  type=str,
                        help='Input normalised file')


    args = parser.parse_args()
    hiddenargs = ['verbosity']

    try:
        if not args.no_comments:
            logfilename = os.path.join(args.path, 'saveSources.log')

            comments = (' ' + args.path + ' ').center(80, '#') + '\n'
            comments += '# ' + os.path.basename(sys.argv[0]) + '\n'
            arglist = args.__dict__.keys()
            for arg in arglist:
                if arg not in hiddenargs:
                    val = getattr(args, arg)
                    if type(val) == str:
                        comments += '#     --' + arg + '="' + val + '"\n'
                    elif type(val) == bool:
                        if val:
                            comments += '#     --' + arg + '\n'
                    elif type(val) == list:
                        for valitem in val:
                            if type(valitem) == str:
                                comments += '#     --' + arg + '="' + valitem + '"\n'
                            else:
                                comments += '#     --' + arg + '=' + str(valitem) + '\n'
                    elif val is not None:
                        comments += '#     --' + arg + '=' + str(val) + '\n'

            with open(logfilename, 'w') as logfile:
                logfile.write(comments)

        norm = NVivoNorm(args.infile)
        norm.begin()

        query = select([norm.Source.c.Name, norm.Source.c.Object, norm.Source.c.ObjectType]).where(
                        norm.Source.c.Name.like(literal(args.source)))
        for row in norm.con.execute(query):
            outfile = open(os.path.join(args.path, row.Name + '.' + row.ObjectType.lower()), 'wb')
            outfile.write(row.Object)
            outfile.close()

    except:
        raise
        norm.rollback()
        del norm

if __name__ == '__main__':
    saveSources(None)
