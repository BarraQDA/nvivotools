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
from dateutil import parser as dateparser
from datetime import date, time, datetime
from distutils import util
import uuid

def add_arguments(parser):
    parser.description = "Insert or update source category in normalised file."

    generalgroup = parser.add_argument_group('General')
    generalgroup.add_argument('-o', '--outfile',     type=str, required=True,
                                                     help='Output normalised NVivo (.norm) file')
    generalgroup.add_argument('-n', '--name',        type=str)
    generalgroup.add_argument('-d', '--description', type=str)
    generalgroup.add_argument('-u', '--user',        type=str,
                              help = 'User name, default is project "modified by".')

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity',  type=int, default=1)
    advancedgroup.add_argument('--no-comments', action='store_true', help='Do not produce a comments logfile')

    parser.set_defaults(func=editSourceCategory)
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

def editSourceCategory(outfile, name, description, user,
                     verbosity, no_comments,
                     comments, **dummy):

    try:
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

        if user is not None:
            userRecord = norm.con.execute(select([
                    norm.User.c.Id
                ]).where(
                    norm.User.c.Name == bindparam('Name')
                ), {
                    'Name': user
                }).first()
            if userRecord is not None:
                userId = userRecord['Id']
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
                        'Name': u"Default User"
                    })
                norm.con.execute(norm.Project.insert(), {
                    'Version': u'0.2',
                    'Title': '',
                    'Description':  u"Created by NVivotools http://barraqda.org/nvivotools/",
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                })

        catRecord = norm.con.execute(select([
                    norm.SourceCategory.c.Id
                ]).where(
                    norm.SourceCategory.c.Name == bindparam('Name')
                ), {
                    'Name': name
                }).first()
        catId = uuid.uuid4() if catRecord is None else catRecord['Id']

        datetimeNow = datetime.utcnow()

        attColumns = {
                'Id':           catId,
                '_Id':          catId,
                'Name':         name,
                'Description':  description,
                'ModifiedBy':   userId,
                'ModifiedDate': datetimeNow
            }
        if catRecord:
            norm.con.execute(norm.SourceCategory.update(
                    norm.SourceCategory.c.Id == bindparam('_Id')),
                    attColumns)
        else:
            attColumns.update({
                'CreatedBy':    userId,
                'CreatedDate':  datetimeNow,
            })
            norm.con.execute(norm.SourceCategory.insert(), attColumns)

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
