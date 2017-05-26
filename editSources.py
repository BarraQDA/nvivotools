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
from NVivoNorm import NVivoNorm
import unicodecsv
from sqlalchemy import *
import re
from dateutil import parser as dateparser
from datetime import date, time, datetime
from distutils import util
import uuid
import chardet
import codecs
import tempfile
from subprocess import Popen, PIPE

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

def editSources(arglist):

    parser = argparse.ArgumentParser(description='Insert or update source in normalised file.',
                                    fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)

    parser.add_argument('-i', '--infile', type = str, help = 'CSV file containing source information')

    parser.add_argument('-n', '--name',        type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-d', '--description', type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-c', '--category',    type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-a', '--attributes',  type = str, action='append', help='Attributes in format name:value')
    parser.add_argument(      '--color',       type = str)
    parser.add_argument('-s', '--source',      type = str,
                        help = 'Source file name')
    parser.add_argument('-t', '--text',        type = str,
                        help = 'Source text')

    parser.add_argument('-u', '--user',        type = lambda s: unicode(s, 'utf8'),
                        help = 'User name, default is project "modified by".')

    parser.add_argument('normFile', type=str)

    args = parser.parse_args()

    try:
        norm = NVivoNorm(args.normFile)
        norm.begin()

        datetimeNow = datetime.utcnow()

        if args.user:
            user = norm.con.execute(select([
                    norm.User.c.Id
                ]).where(
                    norm.User.c.Name == bindparam('Name')
                ), {
                    'Name': args.user
                }).first()
            if user:
                userId = user['Id']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': args.user
                    })
        else:
            project = norm.con.execute(select([
                    norm.Project.c.ModifiedBy
                ])).first()
            if project:
                userId = project['ModifiedBy']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': "Default User"
                    })
                norm.con.execute(norm.Project.insert(), {
                    'Version': '0.2',
                    'Title': "Created by NVivotools http://barraqda.org/nvivotools/",
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                })

        if args.infile:
            csvFile = file(args.infile, 'r')

            # Skip comments at start of CSV file.
            incomments = ''
            while True:
                line = csvFile.readline()
                if line[:1] == '#':
                    incomments += line
                else:
                    csvfieldnames = next(unicodecsv.reader([line]))
                    break

            csvreader=unicodecsv.DictReader(csvFile, fieldnames=csvfieldnames)
            sourceRows = []
            for row in csvreader:
                sourceRow = dict(row)
                sourceRow['Name']        = sourceRow.get('Name',        args.name)
                sourceRow['Description'] = sourceRow.get('Description', args.description)
                sourceRow['Category']    = sourceRow.get('Category',    args.category)
                sourceRow['Color']       = sourceRow.get('Color',       args.color)
                sourceRow['Source']      = sourceRow.get('Source',      args.source)
                sourceRow['Text']        = sourceRow.get('Text',        args.text)
                sourceRows.append(sourceRow)

            colNames = csvfieldnames
        else:
            sourceRows = [{
                'Name':        args.name,
                'Description': args.description,
                'Category':    args.category,
                'Color':       args.color,
                'Source':      args.source,
                'Text':        args.text
            }]
            colNames = ['Name', 'Description', 'Category', 'Color', 'Source', 'Text']

        # Fill in attributes from command-line
        if args.attributes:
            for attribute in args.attributes:
                attMatch = re.match("(?P<attname>[^:]+):(?P<attvalue>.+)?", attribute)
                if not parseattribute:
                    raise RuntimeError("Incorrect attribute format " + attribute)

                colnames.append(attName)
                for sourceRow in sourceRows:
                    attName  = attMatch.group('attname')
                    attValue = attMatch.group('attvalue')
                    sourceRow[attName] = sourceRow.get(attName, attValue)

        sourceAttributes = {}
        for attributeName in colNames:
            # Skip reserved attribute names
            if attributeName in ['Name', 'Description', 'Category', 'Color', 'Source', 'Text']:
                continue

            # Determine whether attribute is already defined
            sourceattribute = norm.con.execute(select([
                    norm.SourceAttribute.c.Id,
                    norm.SourceAttribute.c.Type,
                    norm.SourceAttribute.c.Length
                ]).where(
                    norm.SourceAttribute.c.Name == bindparam('Name')
                ), {
                    'Name': attributeName
                }).first()

            if sourceattribute:
                sourceAttributes[attributeName] = {
                    'Id':     sourceattribute['Id'],
                    'Type':   sourceattribute['Type'],
                    'Length': sourceattribute['Length']
                }
            else:
                attributeId = uuid.uuid4()
                typeInteger = True
                typeDecimal = True
                typeDateTime = True
                typeDate = True
                typeTime = True
                typeBoolean = True
                attributeLength = 0
                for sourceRow in sourceRows:
                    attributeValue = sourceRow[attributeName]
                    attributeLength = max(attributeLength, len(attributeValue))
                    try:
                        int(attributeValue)
                    except ValueError:
                        typeInteger = False
                    try:
                        float(attributeValue)
                    except ValueError:
                        typeDecimal = False
                    try:
                        datetimeval = dateparser.parse(attributeValue, default=datetime.min)
                        if datetimeval.hour or datetimeval.minute:
                            typeDate = False
                        # Assume date being min means taken from default, ie not specified in datetime
                        if datetimeval.date() != datetime.min.date():
                            typeTime = False
                    except ValueError:
                        typeDateTime = False
                        typeDate = False
                        typeTime = False
                    if not attributeValue.lower() in {'true', 'false'}:
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

                norm.con.execute(norm.SourceAttribute.insert(), {
                    'Id':           attributeId,
                    'Name':         attributeName,
                    'Description':  "Created by NVivotools http://barraqda.org/nvivotools/",
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

        rowNum = 0
        sourcesToInsert      = []
        sourceValuesToInsert = []
        for sourceRow in sourceRows:
            rowNum += 1

            categoryName = sourceRow.get('Category')
            categoryId = None
            if categoryName is not None:
                category = norm.con.execute(select([
                        norm.SourceCategory.c.Id
                    ]).where(
                        norm.SourceCategory.c.Name == bindparam('SourceCategory')
                    ), {
                        'SourceCategory': categoryName
                    }).first()

                if category is not None:
                    categoryId = category['Id']
                else:
                    categoryId = uuid.uuid4()
                    norm.con.execute(norm.SourceCategory.insert(), {
                        'Id':           categoryId,
                        'Name':         categoryName,
                        'Description':  "Created by NVivotools http://barraqda.org/nvivotools/",
                        'CreatedBy':    userId,
                        'CreatedDate':  datetimeNow,
                        'ModifiedBy':   userId,
                        'ModifiedDate': datetimeNow
                        })

            sourceName        = sourceRow.get('Name') or str(rowNum)
            sourceDescription = sourceRow.get('Description') or "Created by NVivotools http://barraqda.org/nvivotools/"

            source = norm.con.execute(select([
                        norm.Source.c.Id
                    ]).where(
                        norm.Source.c.Name == bindparam('Name')
                    ), {
                        'Name': sourceName
                    }).first()
            sourceId = source['Id'] if source else uuid.uuid4()

            sourceValues = []
            for attributeName, attributeSource in sourceAttributes.iteritems():
                attributeId     = attributeSource['Id']
                attributeType   = attributeSource['Type']
                attributeLength = attributeSource['Length']
                attributeValue  = sourceRow[attributeName]

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

            normSourceRow = {
                    'Id':           sourceId,
                    '_Id':          sourceId,
                    'Name':         sourceName,
                    'Description':  sourceDescription,
                    'Category':     categoryId,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                }
            normSourceRow['Color'] = sourceRow.get('Color')

            if sourceRow.get('Text'):
                normSourceRow['ObjectType'] = 'TXT'

                normSourceRow['Object']  = buffer(sourceRow['Text'].encode('utf-8'))
                normSourceRow['Content'] = normSourceRow['Object']

            elif sourceRow.get('Source'):
                normSourceRow['ObjectType'] = 'TXT'

                # detect file encoding
                raw = file(sourcerow['Source'], 'rb').read(32) # at most 32 bytes are returned
                encoding = chardet.detect(raw)['encoding']

                normSourceRow['Object'] = codecs.open(sourcerow['Source'], 'r', encoding=encoding).read().encode('utf-8')
                normSourceRow['Content'] = normSourceRow['Object']

            # Skip source without an object
            if not normSourceRow.get('ObjectType'):
                continue

            if source is None:    # New source
                normSourceRow.update({
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                })
                sourcesToInsert.append(normSourceRow)
                sourceValuesToInsert += sourceValues
            else:
                norm.con.execute(norm.Source.update(
                        norm.Source.c.Id == bindparam('_Id')),
                        normSourceRow)
                for sourceValue in sourceValues:
                    norm.con.execute(norm.SourceValue.delete(and_(
                        norm.SourceValue.c.Source    == bindparam('_Source'),
                        norm.SourceValue.c.Attribute == bindparam('_Attribute'),
                    )), sourceValues)

                sourceValuesToInsert += sourceValues

        if sourcesToInsert:
            norm.con.execute(norm.Source.insert(), sourcesToInsert)
        if sourceValuesToInsert:
            norm.con.execute(norm.SourceValue.insert(), sourceValuesToInsert)

        norm.commit()
        del norm

    except:
        raise
        norm.rollback()
        del norm

if __name__ == '__main__':
    editSources(None)
