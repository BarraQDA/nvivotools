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
from datetime import datetime
import uuid

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())


parser = argparse.ArgumentParser(description='Insert user into normalised file.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)

parser.add_argument('-n', '--name',       type = str)

parser.add_argument('normFile', type=str)

args = parser.parse_args()

try:
    normdb  = create_engine('sqlite:///' + args.normFile)
    normmd  = MetaData(bind=normdb)
    normcon = normdb.connect()
    normtr  = normcon.begin()

    normUser            = Table('User', normmd, autoload=True)

    Id = uuid.uuid4()

    datetimeNow = datetime.utcnow()

    userColumns = {
            'Id':           Id,
            'Name':         args.name,
        }
    normcon.execute(normUser.insert(), userColumns)

    normtr.commit()
    normtr = None
    normcon.close()
    normdb.dispose()

except:
    raise
    if not normtr is None:
        normtr.rollback()
    normdb.dispose()
