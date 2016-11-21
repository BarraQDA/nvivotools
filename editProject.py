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
from sqlalchemy import *
from sqlalchemy import exc
import re
from dateutil import parser as dateparser
from datetime import date, time, datetime
from distutils import util
import uuid

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())


parser = argparse.ArgumentParser(description='Insert or update project in normalised file.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)

parser.add_argument('-t', '--title',       type=str)
parser.add_argument('-d', '--description', type = str)
parser.add_argument('-u', '--user',        type = str,
                    help='User, default is first user from user table')

parser.add_argument('normFile', type=str)

args = parser.parse_args()

try:
    normdb  = create_engine('sqlite:///' + args.normFile)
    normmd  = MetaData(bind=normdb)
    normcon = normdb.connect()
    normtr  = normcon.begin()

    normUser          = Table('User',          normmd, autoload=True)
    normProject       = Table('Project',       normmd, autoload=True)

    if args.user is not None:
        user = normcon.execute(select([
                normUser.c.Id
            ]).where(
                normUser.c.Name == bindparam('Name')
            ), {
                'Name': args.user
            }).first()
        if user is not None:
            userId = user['Id']
        else:
            userId = uuid.uuid4()
            normcon.execute(normUser.insert(), {
                    'Id':   userId,
                    'Name': args.user
                })
    else:
        user = normcon.execute(select([
                normUser.c.Id
            ])).first()
        if user is not None:
            userId = user['Id']
        else:
            raise RuntimeError("No user on command line or user file")

    project = normcon.execute(select([
                normProject.c.Title
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
        normcon.execute(normProject.insert(), projectColumns)
    else:
        normcon.execute(normProject.update(), projectColumns)

    normtr.commit()
    normtr = None
    normcon.close()
    normdb.dispose()


except:
    raise
    if not normtr is None:
        normtr.rollback()
    normdb.dispose()
