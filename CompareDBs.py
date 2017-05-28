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
    parser = argparse.ArgumentParser(description='Compare two NVivo databases, attempting to match records which may have different UUID keys.')

    parser.add_argument('db1',       type=str,
                        help='SQLAlchemy path of database from which to delete data.')
    parser.add_argument('db2',       type=str,
                        help='SQLAlchemy path of database from which to delete data.')

    args = parser.parse_args()

    db1 = create_engine(args.db1)
    md1 = MetaData(bind=db1)
    md1.reflect(db1)
    con1 = db1.connect()

    db2 = create_engine(args.db2)
    md2 = MetaData(bind=db2)
    md2.reflect(db2)
    con2 = db2.connect()

    def buildTableMatchDicts(tableName, matchCols):
        table1 = Table(tableName, md1, autoload=True)
        table2 = Table(tableName, md2, autoload=True)
        rows1 = [dict(row) for row in con1.execute(select(table1.columns))]
        rows2 = [dict(row) for row in con2.execute(select(table2.columns))]
        key1Cols = ([column.name for column in table1.primary_key.columns])
        key2Cols = ([column.name for column in table2.primary_key.columns])
        for row1 in rows1:
            match1Set = set(row['Id'] for row in rows2 if tuple(row[col] for col in matchCols) == tuple(row1[col] for col in matchCols))
            oldMatch1Set = dict1.get(row1['Id'])
            if oldMatch1Set is not None:
                match1Set &= oldMatch1Set
            if len(match1Set) == 0:
                print("Zero matches for", tableName, "key", {col:row1[col] for col in matchCols})
            else:
                match1Set -= {row1['Id']}
                if len(match1Set):
                    dict1[row1['Id']] = match1Set

        for row2 in rows2:
            match2Set = set(row['Id'] for row in rows1 if tuple(row[col] for col in matchCols) == tuple(row2[col] for col in matchCols))
            oldMatch2Set = dict2.get(row2['Id'])
            if oldMatch2Set is not None:
                match2Set &= oldMatch2Set
            match2Set -= {row2['Id']}
            if len(match2Set):
                dict2[row2['Id']] = match2Set

    print("1", args.db1)
    print("2", args.db2)

    # Build match dicts
    dict1 = {}
    dict2 = {}
    buildTableMatchDicts('UserProfile', ['Name'])
    buildTableMatchDicts('Project', [])
    buildTableMatchDicts('Item', ['TypeId', 'Name'])

    tables = ['Annotation', 'Category', 'ExtendedItem', 'Item', 'NodeReference', 'Project',  'Role', 'Source', 'UserProfile']
    ignore = ['CreatedBy', 'CreatedDate', 'ModifiedBy', 'ModifiedDate', 'RevisionId']
    for tableName in tables:
        table = next(table for table in md1.sorted_tables if table.name == tableName)
        print("Table:", table.name)

        rows1 = [dict(row) for row in con1.execute(select(table.columns))]
        rows2 = [dict(row) for row in con2.execute(select(table.columns))]
        keyColNames = ([column.name for column in table.primary_key.columns])

        for row1 in rows1:
            keyVals = [{keyColName:row1[keyColName] for keyColName in keyColNames}]
            for keyColName in keyColNames:
                if keyColName[-2:] == 'Id':
                    keyColValues = dict1.get(row1[keyColName])
                    if keyColValues is not None:
                        newKeyVals = []
                        for keyColValue in keyColValues:
                            for keyVal in keyVals:
                                keyVal.update({keyColName:keyColValue})
                            newKeyVals = newKeyVals + keyVals
                        keyVals = newKeyVals

            for keyVal in keyVals:
                row2Match = [row for row in rows2 if tuple(row[keyColName] for keyColName in keyColNames) == tuple(keyVal[keyColName] for keyColName in keyColNames)]
                if len(row2Match) > 1:
                    print("PRIMARY KEY ERROR")
                elif len(row2Match) == 1:
                    row2 = row2Match[0]
                    row2['_matched'] = True
                    diffCols = []
                    for col in row1.keys():     # NB dict not DB key
                        if col[-2:] != 'Id' and col not in ignore and row1[col] != row2[col]:
                            diffCols.append(col)
                    if diffCols:
                        print ("    Key:", {keyColName:keyVal[keyColName] for keyColName in keyColNames})
                        for diffCol in diffCols:
                            print("        ", diffCol)
                            print("            1:", row1[diffCol])
                            print("            2:", row2[diffCol])

                    break
            else:
                print("Only in 1:", {keyColName:row1[keyColName] for keyColName in keyColNames})

        for row2 in rows2:
            if not row2.get('_matched'):
                print("Only in 2:", {keyColName:row2[keyColName] for keyColName in keyColNames})


except:
    raise
