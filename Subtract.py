#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlalchemy
from sqlalchemy import *
from sqlalchemy import exc
import warnings
import sys
import os
import argparse

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

parser = argparse.ArgumentParser(description='Subtract the contents of one database from another.')

parser.add_argument('minuend', type=str,
                    help='Path of database from which to subtract contents.')
parser.add_argument('subtrahend', type=str,
                    help='Path of database whose contents are to be subracted from the first database.')

args = parser.parse_args()

try:
    # Hide warning message over unrecognised xml columns
    warnings.filterwarnings("ignore", category=exc.SAWarning, message='Did not recognize type \'xml\'.*', module='sqlalchemy')

    minuenddb = create_engine(args.minuend)
    minuendmd = MetaData(bind=minuenddb)
    minuendmd.reflect(minuenddb)

    subtrahenddb = create_engine(args.subtrahend)
    subtrahendmd = MetaData(bind=subtrahenddb)
    subtrahendmd.reflect(subtrahenddb)

    for minuendtable in minuendmd.tables.values():
        subtrahendtable = minuendmd.tables[minuendtable.name]
        if subtrahendtable != None:
#            print subtrahendtable.select()
            subtrahendrows = subtrahenddb.execute(subtrahendtable.select())
            subtrahendrows = [dict(row) for row in subtrahendrows]

            minuendrows = minuenddb.execute(minuendtable.select())
            minuendrows = [dict(row) for row in minuendrows]

            differencerows = [ x for x in minuendrows if not x in subtrahendrows ]
            if len(differencerows) > 0:
                print "-------------- " + minuendtable.name + "-------------- " 
            for row in differencerows:
                print row

# All done.

except exc.SQLAlchemyError:
    raise
