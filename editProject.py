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

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

def editProject(arglist):

    parser = argparse.ArgumentParser(description='Insert or update project in normalised file.')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)

    parser.add_argument('-t', '--title',       type=str)
    parser.add_argument('-d', '--description', type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-u', '--user',        type = lambda s: unicode(s, 'utf8'),
                        help='User, default is first user from user table')

    parser.add_argument('normFile', type=str)

    args = parser.parse_args()

    try:
        norm = NVivoNorm(args.normFile)
        norm.begin()

        if args.user is not None:
            user = norm.con.execute(select([
                    norm.User.c.Id
                ]).where(
                    norm.User.c.Name == bindparam('Name')
                ), {
                    'Name': args.user
                }).first()
            if user is not None:
                userId = user['Id']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': args.user
                    })
        else:
            user = norm.con.execute(select([
                    norm.User.c.Id
                ])).first()
            if user is not None:
                userId = user['Id']
            else:
                raise RuntimeError("No user on command line or user file")

        project = norm.con.execute(select([
                    norm.Project.c.Title
                ])).first()

        datetimeNow = datetime.now()

        projectColumns = {'Version': '0.2'}
        if args.title is not None:
            projectColumns.update({'Title': args.title})
        if args.description is not None:
            projectColumns.update({'Description': args.description})
        if project is None:
            projectColumns.update({'CreatedBy':   userId})
            projectColumns.update({'CreatedDate': datetimeNow})
        projectColumns.update({'ModifiedBy':   userId})
        projectColumns.update({'ModifiedDate': datetimeNow})

        if project is None:    # New project
            norm.con.execute(norm.Project.insert(), projectColumns)
        else:
            norm.con.execute(norm.Project.update(), projectColumns)

        norm.commit()
        del norm

    except:
        raise
        norm.rollback()
        del norm

if __name__ == '__main__':
    editProject(None)
