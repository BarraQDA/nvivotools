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
from dateutil import parser as dateparser
from datetime import date, time, datetime
from distutils import util
import uuid
import codecs
import unicodecsv

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())


parser = argparse.ArgumentParser(description='Load source data and attributes from CSV.',
                                 fromfile_prefix_chars='@')

parser.add_argument('-v', '--verbosity',  type=int, default=1)

parser.add_argument('csvFile',  type = str, help = 'Source file name')
parser.add_argument('normFile', type = str, help = 'Normalised file name')

parser.add_argument('-u', '--user',        type = lambda s: unicode(s, 'utf8'),
                    help = 'User name, default is project "modified by".')

parser.add_argument('--createCategory', action='store_true')
parser.add_argument('--createAttribute', action='store_true')

args = parser.parse_args()

try:
    if args.csvFile is None:
        csvFile = sys.stdin
    else:
        csvFile = file(args.csvFile, 'r')
        # detect file encoding
        #raw = file(args.csvFile, 'rb').read(32) # at most 32 bytes are returned
        #encoding = chardet.detect(raw)['encoding']
        #csvFile = codecs.open(args.csvFile, 'r', encoding=encoding).read().encode('utf-8')

    # Skip comments at start of csvFile.
    incomments = ''
    while True:
        line = csvFile.readline()
        if line[:1] == '#':
            incomments += line
        else:
            csvfieldnames = next(unicodecsv.reader([line]))
            break

    csvreader=unicodecsv.DictReader(csvFile, fieldnames=csvfieldnames)
    rows = [dict(row) for row in csvreader]

    normdb  = create_engine('sqlite:///' + args.normFile)
    normmd  = MetaData(bind=normdb)
    normcon = normdb.connect()
    normtr  = normcon.begin()

    normUser            = Table('User',            normmd, autoload=True)
    normProject         = Table('Project',         normmd, autoload=True)
    normSource          = Table('Source',          normmd, autoload=True)
    normSourceCategory  = Table('SourceCategory',  normmd, autoload=True)
    normSourceAttribute = Table('SourceAttribute', normmd, autoload=True)
    normSourceValue     = Table('SourceValue',     normmd, autoload=True)

    datetimeNow = datetime.now()

    if args.user:
        user = normcon.execute(select([
                normUser.c.Id
            ]).where(
                normUser.c.Name == bindparam('Name')
            ), {
                'Name': args.user
            }).first()
        if user:
            userId = user['Id']
        else:
            userId = uuid.uuid4()
            normocon.execute(normUser.insert(), {
                    'Id':   userId,
                    'Name': args.user
                })
    else:
        project = normcon.execute(select([
                normProject.c.ModifiedBy
            ])).first()
        userId = project['ModifiedBy']

    sourceAttributes = {}
    for attributeName in csvfieldnames:
        if attributeName in ['Name', 'Description', 'Category', 'Color', 'Text']:
            continue

        sourceattribute = normcon.execute(select([
                normSourceAttribute.c.Id,
                normSourceAttribute.c.Type,
                normSourceAttribute.c.Length
            ]).where(
                normSourceAttribute.c.Name == bindparam('Name')
            ), {
                'Name': attributeName
            }).first()
        if sourceattribute:
            sourceAttributes[attributeName] = {
                'Id':     sourceattribute['Id'],
                'Type':   sourceattribute['Type'],
                'Length': sourceattribute['Length']
            }
        elif args.createAttribute:
            attributeId = uuid.uuid4()
            typeInteger = True
            typeDecimal = True
            typeDateTime = True
            typeDate = True
            typeTime = True
            typeBoolean = True
            attributeLength = 0
            for row in rows:
                value = row[attributeName]
                attributeLength = max(attributeLength, len(value))
                try:
                    int(value)
                except ValueError:
                    typeInteger = False
                try:
                    float(value)
                except ValueError:
                    typeDecimal = False
                try:
                    datetime = dateparser.parse(value, default=datetime.min)
                    if datetime.hour or datetime.minute:
                        typeDate = False
                    # Assume date being min means taken from default, ie not specified in datetime
                    if datetime.date() != datetime.min.date():
                        typeTime = False
                except ValueError:
                    typeDateTime = False
                    typeDate = False
                    typeTime = False
                if not value.lower() in {'true', 'false'}:
                    typeBoolean = False
            if typeInteger:
                attributeType = 'integer'
            elif typeDecimal:
                attributeType = 'decimal'
            elif typeBoolean:
                attributeType = 'boolean'
            elif typeDate:
                attributeType = 'date'
            elif typeTime:
                attributeType = 'time'
            elif typeDateTime:
                attributeType = 'datetime'
            else:
                attributeType = 'text'

            normcon.execute(normSourceAttribute.insert(), {
                'Id':           attributeId,
                'Name':         attributeName,
                'Description':  "Created by CSV2Sources",
                'Type':         attributeType,
                'Length':       attributeLength,
                'CreatedBy':    userId,
                'CreatedDate':  datetimeNow,
                'ModifiedBy':   userId,
                'ModifiedDate': datetimeNow
            })
            sourceAttributes[attributeName] = {
                'Id':           attributeId,
                'Type':         attributeType,
                'Length':       attributeLength,
            }
        else:
            raise RuntimeError("Source attribute: " + attributeName + " not found.")


    rowNum = 0
    for row in rows:
        rowNum += 1

        categoryName = row.get('Category')
        categoryId = None
        if categoryName is not None:
            category = normcon.execute(select([
                    normSourceCategory.c.Id
                ]).where(
                    normSourceCategory.c.Name == bindparam('SourceCategory')
                ), {
                    'SourceCategory': categoryName
                }).first()

            if category is not None:
                categoryId = category['Id']
            elif args.createCategory:
                categoryId = uuid.uuid4()
                normcon.execute(normSourceCategory.insert(), {
                    'Id':           categoryId,
                    'Name':         categoryName,
                    'Description':  "Created by CSV2Sources",
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                    })
            else:
                raise RuntimeError("Source category: " + categoryName + " not found.")

        sourceName = row.get('Name') or str(rowNum)

        source = normcon.execute(select([
                    normSource.c.Id
                ]).where(
                    normSource.c.Name == bindparam('Name')
                ), {
                    'Name': sourceName
                }).first()
        sourceId = source['Id'] if source else uuid.uuid4()

        sourceValues = []
        for attributeName, attributeSource in sourceAttributes.iteritems():
            attributeId     = attributeSource['Id']
            attributeType   = attributeSource['Type']
            attributeLength = attributeSource['Length']
            attributeValue  = row[attributeName]

            if attributeType == 'text':
                if attributeLength and len(attributeValue) > attributeLength:
                    raise RuntimeError("Value: " + attributeValue + " longer than attribute length")
            elif attributeType == 'integer':
                attributeValue = int(attributeValue)
            elif attributeType == 'decimal':
                attributeValue = float(attributeValue)
            elif attributeType == 'datetime':
                attributeValue = datetime.isoformat(dateparser.parse(attributeValue))
            elif attributeType == 'date':
                attributeValue = date.isoformat(dateparser.parse(attributeValue))
            elif attributeType == 'time':
                attributeValue = time.isoformat(dateparser.parse(attributeValue).time())
            elif attributeType == 'boolean':
                attributeValue = str(bool(util.strtobool(attributeValue)))
            else:
                raise RuntimeError("Unknown attribute type: " + attributeType)

            sourceValues.append({
                    'Source':       sourceId,
                    '_Source':      sourceId,
                    'Attribute':    attributeId,
                    '_Attribute':   attributeId,
                    'Value':        attributeValue,
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                })

        sourceColumns = {
                'Id':           sourceId,
                '_Id':          sourceId,
                'Name':         sourceName,
                'Description':  "Created by CSV2Sources",
                'ModifiedBy':   userId,
                'ModifiedDate': datetimeNow
            }
        sourceColumns['Color'] = row.get('Color')

        if row.get('Text'):
            sourceColumns['ObjectType'] = 'TXT'

            # detect file encoding
            raw = file(args.source, 'rb').read(32) # at most 32 bytes are returned
            encoding = chardet.detect(raw)['encoding']

            sourceColumns['Object'] = codecs.open(args.source, 'r', encoding=encoding).read().encode('utf-8')
            sourceColumns['Content'] = sourceColumns['Object']

        if source is None:    # New source
            sourceColumns.update({
                'CreatedBy':    userId,
                'CreatedDate':  datetimeNow,
            })
            normcon.execute(normSource.insert(), sourceColumns)
            if len(sourceValues) > 0:
                normcon.execute(normSourceValue.insert(), sourceValues)
        else:
            normcon.execute(normSource.update(
                    normSource.c.Id == bindparam('_Id')),
                    sourceColumns)
            for sourceValue in sourceValues:
                normcon.execute(normSourceValue.delete(and_(
                    normSourceValue.c.Source    == bindparam('_Source'),
                    normSourceValue.c.Attribute == bindparam('_Attribute'),
                )), sourceValues)
                normcon.execute(normSourceValue.insert(), sourceValues)

    normtr.commit()
    normtr = None
    normcon.close()
    normdb.dispose()


except:
    raise
    if not normtr is None:
        normtr.rollback()
    normdb.dispose()
