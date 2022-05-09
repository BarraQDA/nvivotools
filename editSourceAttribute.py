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
from datetime import date, time, datetime
from distutils import util
import uuid

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())


parser = argparse.ArgumentParser(description='Insert or update source attribute in normalised file.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)

parser.add_argument('-n', '--name',        type = str)
parser.add_argument('-d', '--description', type = str)
parser.add_argument('-t', '--type',        choices=["text", "integer", "decimal", "datetime", "date", "time", "boolean"])
parser.add_argument('-l', '--length',      type = int)
parser.add_argument('-u', '--user',        type = str,
                    help = 'User name, default is project "modified by".')

parser.add_argument('normFile', type=str)

args = parser.parse_args()

try:
    normdb  = create_engine('sqlite:///' + args.normFile)
    normmd  = MetaData(bind=normdb)
    normcon = normdb.connect()
    normtr  = normcon.begin()

    normUser            = Table('User',          normmd, autoload=True)
    normProject         = Table('Project',       normmd, autoload=True)
    normSourceAttribute = Table('SourceAttribute', normmd, autoload=True)

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
            normocon.execute(normUser.insert(), {
                    'Id':   userId,
                    'Name': args.user
                })
    else:
        project = normcon.execute(select([
                normProject.c.ModifiedBy
            ])).first()
        userId = project['ModifiedBy']

    att = normcon.execute(select([
                normSourceAttribute.c.Id
            ]).where(
                normSourceAttribute.c.Name == bindparam('Name')
            ), {
                'Name': args.name
            }).first()
    Id = uuid.uuid4() if att is None else att['Id']

    datetimeNow = datetime.utcnow()

    attColumns = {
            'Id':           Id,
            '_Id':          Id,
            'Name':         args.name,
            'Description':  args.description,
            'Type':         args.type.title(),
            'Length':       args.length,
            'CreatedBy':    userId,
            'CreatedDate':  datetimeNow,
            'ModifiedBy':   userId,
            'ModifiedDate': datetimeNow
        }
    if att is None:    # New category
        normcon.execute(normSourceAttribute.insert(), attColumns)
    else:
        normcon.execute(normSourceAttribute.update(
                normSourceAttribute.c.Id == bindparam('_Id')),
                attColumns)

    normtr.commit()
    normtr = None
    normcon.close()
    normdb.dispose()


except:
    raise
    if not normtr is None:
        normtr.rollback()
    normdb.dispose()
