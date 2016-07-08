#!/usr/bin/python
# -*- coding: utf-8 -*-

from sqlalchemy import exc, TypeDecorator, CHAR, String, create_engine, MetaData, bindparam
from sqlalchemy.engine import reflection
import warnings
import sys
import os
import argparse
import uuid

execfile(os.path.dirname(os.path.realpath(__file__)) + '/' + 'NVivoTypes.py')

class UUID(TypeDecorator):
    """Platform-independent UUID type.

    Uses Postgresql's UUID type, otherwise uses
    CHAR(36), storing as stringified hex values.

    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return str(uuid.UUID(value)).upper()
            else:
                return str(value).upper()

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return uuid.UUID(value)

parser = argparse.ArgumentParser(description='Delete all data leaving only database structure.')

parser.add_argument('database', type=str,
                    help='Path of database from which to delete data.')

args = parser.parse_args()

try:
    # Hide warning message over unrecognised xml columns
    #warnings.filterwarnings("ignore", category=exc.SAWarning, message='Did not recognize type \'xml\'.*', module='sqlalchemy')

    mssql.ischema_names['xml'] = UUID

    databasedb = create_engine(args.database)
    databasemd = MetaData(bind=databasedb)
    databasemd.reflect(databasedb)

    for table in reversed(databasemd.sorted_tables):
        databasedb.execute(table.delete())

except exc.SQLAlchemyError:
    raise
