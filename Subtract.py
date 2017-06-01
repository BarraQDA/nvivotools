#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from sqlalchemy import exc, TypeDecorator, CHAR, String, create_engine, MetaData, bindparam
from sqlalchemy.engine import reflection
import warnings
import sys
import os
import argparse
import uuid

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

try:

    parser = argparse.ArgumentParser(description='Subtract the contents of one database from another.')

    parser.add_argument('-r', '--reverse', action='store_true',
                        help='Reverse the subtraction, that is swap the minuent and subtrahend.')
    parser.add_argument('-i', '--ignore', type=str, nargs='?',
                        help='Comma-separated list of columns to ignore when doing record comparison.')

    parser.add_argument('minuend', type=str,
                        help='Path of database from which to subtract contents.')
    parser.add_argument('subtrahend', type=str,
                        help='Path of database whose contents are to be subracted from the first database.')
    parser.add_argument('difference', type=str, nargs='?',
                        help='Path of database to be populated with the difference.')

    args = parser.parse_args()

    if args.reverse:
        args.minuend, args.subtrahend = args.subtrahend, args.minuend

    if args.ignore != None:
        ignorecols = args.ignore.split(",")
    else:
        ignorecols = []

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
        differenceconn = differencedb.connect()
        differencetrans = differenceconn.begin()
        inspector = reflection.Inspector.from_engine(differencedb)

    for minuendtable in minuendmd.sorted_tables:
        if args.difference != None:
            if minuendtable.name not in differencemd.tables.keys():
                print("Creating table: " + minuendtable.name, file=sys.stderr)
                minuendtable.create(differenceconn)

    for minuendtable in minuendmd.sorted_tables:
        subtrahendtable = subtrahendmd.tables[minuendtable.name]
        if subtrahendtable != None:
            subtrahendrows = subtrahenddb.execute(subtrahendtable.select())
            subtrahendrows = [dict(row) for row in subtrahendrows]
            for row in subtrahendrows:
                for ignorecolumn in ignorecols:
                    row[ignorecolumn] = None

            minuendrows = minuenddb.execute(minuendtable.select())
            minuendrows = [dict(row) for row in minuendrows]
            for row in minuendrows:
                for ignorecolumn in ignorecols:
                    row[ignorecolumn] = None

            differencerows = [ x for x in minuendrows if not x in subtrahendrows ]

            if len(differencerows) > 0:
                if args.difference != None:
                    print("Finding foreign key references for table " + minuendtable.name, file=sys.stderr)
                    for fk in inspector.get_foreign_keys(minuendtable.name):
                        if not fk['name']:
                            continue

                        #print("   " + fk['name'], file=sys.stderr)
                        fkreferredtable = minuendmd.tables[fk['referred_table']]
                        fkselect = fkreferredtable.select()
                        for referred_column, constrained_column in zip(fk['referred_columns'], fk['constrained_columns']):
                            fkselect = fkselect.where(fkreferredtable.c[referred_column]  == bindparam(constrained_column))

                        #print(fkselect, file=sys.stderr)

                        fkrows = []
                        fkexists = []
                        for differencerow in differencerows:
                            fkrow = minuenddb.execute(fkselect, differencerow)
                            fkrows += [dict(row) for row in fkrow if not dict(row) in fkrows]

                            fkexist = differenceconn.execute(fkselect, differencerow)
                            fkexists += [dict(row) for row in fkexist if not dict(row) in fkexists]

                        fkinsert = [ x for x in fkrows if not x in fkexists ]
                        if len(fkinsert) > 0:
                            differencereferredtable = differencemd.tables[fk['referred_table']]
                            #print( "fkinsert: " + str(fkinsert))                            differenceconn.execute(differencereferredtable.insert(), fkinsert, file=sys.stderr)

                    differenceconn.execute(minuendtable.insert(), differencerows)
                else:
                    print("-------------- " + minuendtable.name + " --------------", file=sys.stderr)
                    for row in differencerows:
                        print(row, file=sys.stderr)

# All done.

    if args.difference != None:
        differencetrans.commit()

except exc.SQLAlchemyError:
    raise
