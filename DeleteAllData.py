#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import exc, TypeDecorator, CHAR, String, create_engine, MetaData, bindparam
from sqlalchemy.engine import reflection
import warnings
import sys
import os
import argparse
import uuid

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

try:
    parser = argparse.ArgumentParser(description='Delete all data leaving only database structure.')

    parser.add_argument('database', type=str,
                        help='Path of database from which to delete data.')

    args = parser.parse_args()

    mssql.ischema_names['xml'] = UUID

    databasedb = create_engine(args.database)
    databasemd = MetaData(bind=databasedb)
    databasemd.reflect(databasedb)

    for table in reversed(databasemd.sorted_tables):
        databasedb.execute(table.delete())

except exc.SQLAlchemyError:
    raise
