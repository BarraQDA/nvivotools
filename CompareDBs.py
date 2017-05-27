#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from sqlalchemy import *
from sqlalchemy import exc
import warnings
import sys
import os
import argparse
import re
from dateutil import parser as dateparser
import datetime
from pytimeparse.timeparse import timeparse

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

db = None
con = None
tr = None
try:
    parser = argparse.ArgumentParser(description='Compare two databases.')

    parser.add_argument('db1',       type=str,
                        help='SQLAlchemy path of database from which to delete data.')
    parser.add_argument('db2',       type=str,
                        help='SQLAlchemy path of database from which to delete data.')

    parser.add_argument('--table',   type=str,
                        help='Table to compare, otherwise all tables.')
    parser.add_argument('--ignore',  type=str, nargs='*',
                        help='Field names to ignore in comparison')


    args = parser.parse_args()

    db1 = create_engine(args.db1)
    md1 = MetaData(bind=db1)
    md1.reflect(db1)
    con1 = db1.connect()

    db2 = create_engine(args.db2)
    md2 = MetaData(bind=db2)
    md2.reflect(db2)
    con2 = db2.connect()

    if args.table:
        tables = [table for table in md1.sorted_tables if table.name == args.table]
    else:
        tables = md1.sorted_tables

    args.ignore = args.ignore or []

    for table in tables:
        if table.name in ['Annotation', 'Category', 'ExtendedItem', 'Item', 'NodeReference', 'Project',  'Role', 'Source', 'UserProfile']:
            print("Table:", table.name)

            rows1 = [dict(row) for row in con1.execute(select(table.columns))]
            rows2 = [dict(row) for row in con2.execute(select(table.columns))]
            keynames = ([column.name for column in table.primary_key.columns])

            for row1 in rows1:
                keyval = tuple(row1[keyname] for keyname in keynames)
                #print(keyval)
                row2match = [row for row in rows2 if tuple(row[keyname] for keyname in keynames) == keyval]
                if len(row2match) > 1:
                    print("PRIMARY KEY ERROR")
                elif len(row2match) == 1:
                    row2 = row2match[0]
                    collist = []
                    for key in row1.keys():
                        if key not in args.ignore and row1[key] != row2[key]:
                            collist.append(key)
                    if collist:
                        print ("Records:", keyval, " differ in column:", collist)

            for row1 in rows1:
                keyval = tuple(row1[keyname] for keyname in keynames)
                row2match = [row for row in rows2 if tuple(row[keyname] for keyname in keynames) == keyval]
                if len(row2match) == 0:
                    print("Only in args.db1:", row1)

            for row2 in rows2:
                keyval = tuple(row2[keyname] for keyname in keynames)
                row1match = [row for row in rows1 if tuple(row[keyname] for keyname in keynames) == keyval]
                if len(row1match) == 0:
                    print("Only in args.db2:", row2)


except:
    raise
