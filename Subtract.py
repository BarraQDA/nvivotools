#!/usr/bin/python
# -*- coding: utf-8 -*-

from sqlalchemy import exc, TypeDecorator, CHAR, String, create_engine, MetaData, bindparam
from sqlalchemy.engine import reflection
import warnings
import sys
import os
import argparse
import uuid

exec(open(os.path.dirname(os.path.realpath(__file__)) + '/' + 'NVivoTypes.py').read())

try:

    parser = argparse.ArgumentParser(description='Subtract the contents of one database from another.')

    parser.add_argument('-r', '--reverse', action='store_true',
                        help='Reverse the subtraction, that is swap the minuent and subtrahend.')

    parser.add_argument('minuend', type=str,
                        help='Path of database from which to subtract contents.')
    parser.add_argument('subtrahend', type=str,
                        help='Path of database whose contents are to be subracted from the first database.')
    parser.add_argument('difference', type=str, nargs='?',
                        help='Path of database to be populated with the difference.')

    args = parser.parse_args()

    if args.reverse:
        args.minuend, args.subtrahend = args.subtrahend, args.minuend

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
                print("Creating table: " + minuendtable.name)
                minuendtable.create(differenceconn)

    for minuendtable in minuendmd.sorted_tables:
        subtrahendtable = subtrahendmd.tables[minuendtable.name]
        if subtrahendtable != None:
            subtrahendrows = subtrahenddb.execute(subtrahendtable.select())
            subtrahendrows = [dict(row) for row in subtrahendrows]

            minuendrows = minuenddb.execute(minuendtable.select())
            minuendrows = [dict(row) for row in minuendrows]

            differencerows = [ x for x in minuendrows if not x in subtrahendrows ]

            if len(differencerows) > 0:
                if args.difference != None:
                    print("Finding foreign key references for table " + minuendtable.name)
                    for fk in inspector.get_foreign_keys(minuendtable.name):
                        if not fk['name']:
                            continue

                        #print("   " + fk['name'])
                        fkreferredtable = minuendmd.tables[fk['referred_table']]
                        fkselect = fkreferredtable.select()
                        for referred_column, constrained_column in zip(fk['referred_columns'], fk['constrained_columns']):
                            fkselect = fkselect.where(fkreferredtable.c[referred_column]  == bindparam(constrained_column))

                        #print(fkselect)

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
                            #print( "fkinsert: " + str(fkinsert))                            differenceconn.execute(differencereferredtable.insert(), fkinsert)

                    differenceconn.execute(minuendtable.insert(), differencerows)
                else:
                    print("-------------- " + minuendtable.name + " --------------")
                    for row in differencerows:
                        print(row)

# All done.

    if args.difference != None:
        differencetrans.commit()

except exc.SQLAlchemyError:
    raise
