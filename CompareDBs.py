#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Jonathan Schultz
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

from __future__ import print_function
import NVivo
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
from xml.dom.minidom import *

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

db = None
con = None
tr = None
try:
    parser = argparse.ArgumentParser(description='Compare two NVivo databases, attempting to match records which may have different UUID keys.')

    parser.add_argument('db1',       type=str,
                        help='Filename or SQLAlchemy path of first database.')
    parser.add_argument('db2',       type=str,
                        help='Filename or SQLAlchemy path of second database.')

    parser.add_argument('--tables',  type=str, nargs='*', default=['Annotation', 'Category', 'ExtendedItem', 'Item', 'NodeReference', 'Project',  'Role', 'Source', 'UserProfile'],
                        help='List of tables to compare, otherwise all main NVivo tables.')
    parser.add_argument('--ignore',  type=str, nargs='*', default=['CreatedBy', 'CreatedDate', 'ModifiedBy', 'ModifiedDate', 'RevisionId'],
                        help='Fields to ignore in comparison, otherwise creation and modification info only.')

    args = parser.parse_args()

    if '://' not in args.db1:
        args.db1 = NVivo.mount(args.db1)

    db1 = create_engine(args.db1)
    md1 = MetaData(bind=db1)
    md1.reflect(db1)
    con1 = db1.connect()

    if '://' not in args.db2:
        args.db2 = NVivo.mount(args.db2)

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
            match1Set = set(row2['Id'] for row2 in rows2 if all((row2[col] in dict1.get(row1[col])) if isinstance(row1[col], uuid.UUID) else row2[col] == row1[col] for col in matchCols))
            oldMatch1Set = dict1.get(row1['Id'])
            if oldMatch1Set is not None:
                match1Set &= oldMatch1Set
            if len(match1Set) == 0:
                print("Zero matches for", tableName, "key", {col:row1[col] for col in matchCols})
                for col in matchCols:
                    if isinstance(row1[col], uuid.UUID):
                        print(row1[col], dict1.get(row1[col]))
            else:
                match1Set -= {row1['Id']}
                if len(match1Set):
                    dict1[row1['Id']] = match1Set

        for row2 in rows2:
            match2Set = set(row1['Id'] for row1 in rows1 if all((row1[col] in dict2.get(row2[col])) if isinstance(row2[col], uuid.UUID) else row1[col] == row2[col] for col in matchCols))
            oldMatch2Set = dict2.get(row2['Id'])
            if oldMatch2Set is not None:
                match2Set &= oldMatch2Set
            match2Set -= {row2['Id']}
            if len(match2Set):
                dict2[row2['Id']] = match2Set

    # Modified from https://stackoverflow.com/questions/321795/comparing-xml-in-a-unit-test-in-python
    def compareElements(e1, e2, path):
        if e1.tagName!=e2.tagName:
            return path+'/'+e1.tagname + ' != ' + path+'/'+e2.tagname

        path += '/' + e1.tagName
        for a1, a2 in zip(sorted(e1.attributes.items()), sorted(e2.attributes.items())):
            if a1[0] != a2[0]:
                return path+':'+a1[0] + ' != ' + path+':'+a2[0]
            elif a1[0] != 'Guid':
                if a1[1] != a2[1]:
                    return path+':'+a1[0]+'='+a1[1] + ' != ' + path+':'+a2[0]+'='+a2[1]
            else:
                if a2[1] not in dict.get(a1[1]):
                    return path+':'+a1[0]+'='+a1[1] + ' != ' + path+':'+a2[0]+'='+a2[1]

        if len(e1.childNodes)!=len(e2.childNodes):
            tag1 = [c.tagName for c in e1.childNodes]
            tag2 = [c.tagName for c in e2.childNodes]
            for t in tag1:
                if t not in tag2:
                    return path+'/'+t + ' missing from 2'
            for t in tag2:
                if t not in tag1:
                    return path+'/'+t + ' missing from 1'

        for c1, c2 in zip(e1.childNodes, e2.childNodes):
            if c1.nodeType!=c2.nodeType:
                return path+'/'+c1.tagName + ' differs from ' + path+'/'+c2.tagName
            if c1.nodeType==c1.TEXT_NODE and c1.data!=c2.data:
                return path+'/'+c1.tagName + ' differs from ' + path+'/'+c2.tagName
            if c1.nodeType==c1.ELEMENT_NODE:
                ret = compareElements(c1, c2, path)
                if ret:
                    return ret
        return None

    def compareTable(tableName, extraColumns):
        table = next(table for table in md1.sorted_tables if table.name == tableName)
        print("Table:", table.name)

        rows1 = [dict(row) for row in con1.execute(select(table.columns))]
        rows2 = [dict(row) for row in con2.execute(select(table.columns))]
        keyColNames = [column.name for column in table.primary_key.columns]

        for row1 in rows1:
            keyVals = [{keyColName:row1[keyColName] for keyColName in keyColNames}]
            for keyColName in keyColNames:
                if isinstance(row1[keyColName], uuid.UUID):
                    keyColValues = dict1.get(row1[keyColName])
                    if keyColValues is not None:
                        newKeyVals = []
                        for keyColValue in keyColValues:
                            for keyVal in keyVals:
                                keyVal[keyColName] = keyColValue
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
                        if col in ['Properties', 'Layout'] and row1[col] and row2[col]:
                            xml1 = parseString(row1[col])
                            xml2 = parseString(row2[col])
                            diff = compareElements(xml1.documentElement, xml2.documentElement, '')
                            if diff:
                                diffCols.append((col, diff))
                        elif (not isinstance(row1[col], uuid.UUID)) and col not in args.ignore and row1[col] != row2[col]:
                            diffCols.append((col, None))
                    if diffCols:
                        keyDict = {}
                        for keyColName in keyColNames:
                            keyDict[keyColName] = row1[keyColName]
                            if isinstance(row1[keyColName], uuid.UUID):
                                keyDict[keyColName + ' Name'] = item1.get(row1[keyColName], '')
                        print ("    Key:", keyDict, ("Name: " + row1['Name']) if 'Name' in row1.keys() and 'Name' not in diffCols else "")
                        for diffCol in diffCols:
                            print("        ", diffCol[0])
                            if diffCol[1]:
                                print("            ", diffCol[1])
                            else:
                                print("            1:", str(row1[diffCol[0]])[0:256])
                                print("            2:", str(row2[diffCol[0]])[0:256])

                    break
            else:
                keyDict = {}
                for keyColName in keyColNames:
                    keyDict[keyColName] = row1[keyColName]
                    if isinstance(row1[keyColName], uuid.UUID):
                        keyDict[keyColName + ' Name'] = item1.get(row1[keyColName], '')
                print("Only in 1:", keyDict, {colName:row1[colName] for colName in extraColumns})

        for row2 in rows2:
            if not row2.get('_matched'):
                keyDict = {}
                for keyColName in keyColNames:
                    keyDict[keyColName] = row1[keyColName]
                    if isinstance(row1[keyColName], uuid.UUID):
                        keyDict[keyColName + ' Name'] = item2.get(row2[keyColName], '')
                print("Only in 2:", keyDict, {colName:row2[colName] for colName in extraColumns})


    print("1", args.db1)
    print("2", args.db2)

    # Build item dicts

    item1 = {row['Id']:row['Name'] for row in con1.execute(select(Table('Item', md1, autoload=True).columns))}
    item2 = {row['Id']:row['Name'] for row in con2.execute(select(Table('Item', md1, autoload=True).columns))}

    # Build match dicts
    dict1 = {}
    dict2 = {}
    buildTableMatchDicts('UserProfile', ['Name'])
    buildTableMatchDicts('Project', [])
    buildTableMatchDicts('Item', ['TypeId', 'Name', 'HierarchicalName'])
    buildTableMatchDicts('NodeReference', ['Node_Item_Id', 'Source_Item_Id'])

    compareTable('Annotation', [])
    compareTable('Category', [])
    compareTable('ExtendedItem', [])
    compareTable('Item', ['TypeId', 'Name'])
    compareTable('NodeReference', ['StartX', 'LengthX'])
    compareTable('Project', [])
    compareTable('Role', [])
    compareTable('Source', [])
    compareTable('UserProfile', ['Name'])



except:
    raise
