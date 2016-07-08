#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlalchemy
from sqlalchemy import *
from sqlalchemy import exc
from sqlalchemy.databases import mssql, sqlite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
import warnings
import sys
import os
import argparse
import uuid

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


@compiles(UNIQUEIDENTIFIER, 'sqlite')
def compile_UNIQUEIDENTIFIER_mssql_sqlite(element, compiler, **kw):
    """ Handles mssql UNIQUEIDENTIFIER datatype as UUID in SQLite """
    try:
        length = element.length
    except:
        length = None
    element.length = 64 # @note: 36 should be enough, but see the link below

    # @note: since SA-0.9 all string types have collation, which are not
    # really compatible between databases, so use default one
    element.collation = None

    res = "UUID"
    if length:
        element.length = length
    return res

parser = argparse.ArgumentParser(description='Subtract the contents of one database from another.')

parser.add_argument('minuend', type=str,
                    help='Path of database from which to subtract contents.')
parser.add_argument('subtrahend', type=str,
                    help='Path of database whose contents are to be subracted from the first database.')
parser.add_argument('difference', type=str, nargs='?',
                    help='Path of database to be populated with the difference.')

args = parser.parse_args()

try:
    mssql.ischema_names['xml'] = String
    
    sqlite.ischema_names['UNIQUEIDENTIFIER'] = UUID

    minuenddb = create_engine(args.minuend)
    minuendmd = MetaData()
    minuendmd.reflect(minuenddb)

    subtrahenddb = create_engine(args.subtrahend)
    subtrahendmd = MetaData()
    subtrahendmd.reflect(subtrahenddb)

    if args.difference != None:
        differencedb = create_engine(args.difference)
        differencemd = MetaData()
        differencemd.reflect(differencedb)

    for minuendtable in minuendmd.sorted_tables:
        if args.difference != None:
            if minuendtable.name not in differencemd.tables.keys():
                print "Creating table: " + minuendtable.name
                minuendtable.create(differencedb)

    for minuendtable in minuendmd.sorted_tables:
        subtrahendtable = minuendmd.tables[minuendtable.name]
        if subtrahendtable != None:
            subtrahendrows = subtrahenddb.execute(subtrahendtable.select())
            subtrahendrows = [dict(row) for row in subtrahendrows]

            minuendrows = minuenddb.execute(minuendtable.select())
            minuendrows = [dict(row) for row in minuendrows]

            differencerows = [ x for x in minuendrows if not x in subtrahendrows ]
            
            if len(differencerows) > 0:
                if args.difference != None:
                    differencedb.execute(minuendtable.insert(), differencerows)
                else:
                    print "-------------- " + minuendtable.name + "-------------- " 
                    for row in differencerows:
                        print row

# All done.

except exc.SQLAlchemyError:
    raise
