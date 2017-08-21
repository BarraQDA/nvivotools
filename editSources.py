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

from __future__ import print_function
import os
import sys
import argparse
from NVivoNorm import NVivoNorm
import unicodecsv
import glob
from sqlalchemy import *
import re
from dateutil import parser as dateparser
from datetime import date, time, datetime
from distutils import util
import uuid
import chardet
import codecs
import tempfile

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

def editSource(sourceRows, args, norm):
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
    sourceNodeId = {}
    for colName in sourceRows[0].keys():
        # Does column define an attribute?
        if ((not args.columns or colName in args.columns)
            and colName
            and colName not in ['Name', 'Description', 'Category', 'Color', 'ObjectFile', 'Text'] + args.exclude + args.textcolumns):

            # Determine whether attribute is already defined
            sourceattribute = norm.con.execute(select([
                    norm.SourceAttribute.c.Id,
                    norm.SourceAttribute.c.Type,
                    norm.SourceAttribute.c.Length
                ]).where(
                    norm.SourceAttribute.c.Name == bindparam('Name')
                ), {
                    'Name': colName
                }).first()

            if sourceattribute:
                sourceAttributes[colName] = {
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
                    attributeValue = sourceRow[colName]
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
                    'Name':         colName,
                    'Description':  u"Created by NVivotools http://barraqda.org/nvivotools/",
                    'Type':         unicode(attributeType),
                    'Length':       attributeLength,
                    'CreatedBy':    args.userId,
                    'CreatedDate':  args.datetimeNow,
                    'ModifiedBy':   args.userId,
                    'ModifiedDate': args.datetimeNow
                })
                sourceAttributes[colName] = {
                    'Id':           attributeId,
                    'Type':         attributeType,
                    'Length':       attributeLength,
                }

        # Does column define a node?
        elif colName in args.textcolumns and len(args.textcolumns) > 1:

            node = norm.con.execute(select([
                    norm.Node.c.Id
                ]).where(
                    norm.Node.c.Name == bindparam('Name')
                ), {
                    'Name': colName
                }).first()
            if node:
                nodeId = node['Id']
            else:
                nodeId = uuid.uuid4()
                norm.con.execute(norm.Node.insert(), {
                    'Id':           nodeId,
                    'Name':         unicode(colName),
                    'Description':  u"Created by NVivotools http://barraqda.org/nvivotools/",
                    'CreatedBy':    args.userId,
                    'CreatedDate':  args.datetimeNow,
                    'ModifiedBy':   args.userId,
                    'ModifiedDate': args.datetimeNow
                })

            sourceNodeId[colName] = nodeId

    rowNum = 0
    sourcesToInsert      = []
    sourceValuesToInsert = []
    digits = len(str(len(sourceRows)))
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
                    'Description':  u"Created by NVivotools http://barraqda.org/nvivotools/",
                    'CreatedBy':    args.userId,
                    'CreatedDate':  args.datetimeNow,
                    'ModifiedBy':   args.userId,
                    'ModifiedDate': args.datetimeNow
                    })

        sourceName        = sourceRow.get('Name') or unicode(rowNum).zfill(digits)
        sourceDescription = sourceRow.get('Description') or u"Created by NVivotools http://barraqda.org/nvivotools/"

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
                    'Value':        unicode(attributeValue),
                    'CreatedBy':    args.userId,
                    'CreatedDate':  args.datetimeNow,
                    'ModifiedBy':   args.userId,
                    'ModifiedDate': args.datetimeNow
                })

        normSourceRow = {
                'Id':           sourceId,
                '_Id':          sourceId,
                'Name':         sourceName,
                'Description':  sourceDescription,
                'Category':     categoryId,
                'ModifiedBy':   args.userId,
                'ModifiedDate': args.datetimeNow
            }
        normSourceRow['Color'] = sourceRow.get('Color')

        ObjectFile = sourceRow.get('ObjectFile')
        if ObjectFile:
            dummy, ObjectFileExt = os.path.splitext(ObjectFile)
            ObjectFileExt = ObjectFileExt[1:].upper()
            normSourceRow['ObjectType'] = unicode(ObjectFileExt)
            if ObjectFileExt == 'TXT':
                # detect file encoding
                raw = file(ObjectFile, 'rb').read(32) # at most 32 bytes are returned
                encoding = chardet.detect(raw)['encoding']

                normSourceRow['Content'] = codecs.open(ObjectFile, 'r', encoding=encoding).read().encode('utf-8')
            else:
                normSourceRow['Object'] = open(ObjectFile, 'rb').read()
        else:
            normSourceRow['ObjectType'] = u'TXT'
            normSourceRow['Content'] = sourceRow.get('Text') or u''

        if normSourceRow.get('ObjectType') == u'TXT':

            for textColumn in args.textcolumns:
                normSourceText = sourceRow.get(textColumn) or u''
                if normSourceText:
                    normSourceText += u'\n'

                    if normSourceRow['Content']:
                        normSourceRow['Content'] += u'\n\n'

                    # If more than one text column then make header in content and tag text
                    if len(args.textcolumns) > 1:
                        normSourceRow['Content'] += textColumn + u'\n\n'

                        start  = len(normSourceRow['Content']) + 1
                        end    = start + len(normSourceText) - 1
                        nodeId    = sourceNodeId[textColumn]
                        taggingId = uuid.uuid4()
                        norm.con.execute(norm.Tagging.insert(), {
                                'Id':           taggingId,
                                'Source':       sourceId,
                                'Node':         nodeId,
                                'Fragment':     unicode(start) + u':' + unicode(end),
                                'Memo':         None,
                                'CreatedBy':    args.userId,
                                'CreatedDate':  args.datetimeNow,
                                'ModifiedBy':   args.userId,
                                'ModifiedDate': args.datetimeNow
                            })

                    normSourceRow['Content'] += normSourceText

            normSourceRow['Object'] = bytearray(normSourceRow['Content'], 'utf-8')

        if source is None:    # New source
            normSourceRow.update({
                'CreatedBy':    args.userId,
                'CreatedDate':  args.datetimeNow,
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


def editSources(arglist):

    parser = argparse.ArgumentParser(description='Insert or update source in normalised file.',
                                    fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)

    parser.add_argument('-l', '--limit',   type=int, help='Limit number of lines from input file')

    parser.add_argument('-C', '--columns', type = str, nargs = '*',
                                           help = 'Columns from input CSV file to include as attributes')
    parser.add_argument(      '--exclude', type = str, nargs = '*', default = [],
                                           help = 'Columns from input CSV file to exclude as attributes')
    parser.add_argument('--textcolumns',   type = str, nargs = '*', default = [],
                                           help = 'Columns from input CSV file to include as coded text')

    parser.add_argument('-n', '--name',        type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-d', '--description', type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-c', '--category',    type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-a', '--attributes',  type = str, action='append', help='Attributes in format name:value')
    parser.add_argument(      '--color',       type = str)
    parser.add_argument('-t', '--text',        type = str, help = 'Source text')

    parser.add_argument('-u', '--user',        type = lambda s: unicode(s, 'utf8'),
                                               help = 'User name, default is project "modified by".')

    parser.add_argument('--no-comments', action='store_true', help='Do not produce a comments logfile')

    parser.add_argument('-o', '--outfile',  type=str, required=True,
                        help='Output normalised NVivo (.norm) file')
    parser.add_argument(        'infile',   type = str, nargs = '*',
                                            help = 'Input CSV or source content filename pattern')

    args = parser.parse_args()
    hiddenargs = ['verbosity']

    try:

        if not args.no_comments:
            logfilename = args.outfile.rsplit('.',1)[0] + '.log'

            comments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
            comments += '# ' + os.path.basename(sys.argv[0]) + '\n'
            arglist = args.__dict__.keys()
            for arg in arglist:
                if arg not in hiddenargs:
                    val = getattr(args, arg)
                    if type(val) == str or type(val) == unicode:
                        comments += '#     --' + arg + '="' + val + '"\n'
                    elif type(val) == bool:
                        if val:
                            comments += '#     --' + arg + '\n'
                    elif type(val) == list:
                        for valitem in val:
                            if type(valitem) == str:
                                comments += '#     --' + arg + '="' + valitem + '"\n'
                            else:
                                comments += '#     --' + arg + '=' + str(valitem) + '\n'
                    elif val is not None:
                        comments += '#     --' + arg + '=' + str(val) + '\n'

            logfile = open(logfilename, 'w')
            logfile.write(comments)

        norm = NVivoNorm(args.outfile)
        norm.begin()

        # Put timestamp and user ID into args so editSource can access them.

        args.datetimeNow = datetime.utcnow()

        if args.user:
            user = norm.con.execute(select([
                    norm.User.c.Id
                ]).where(
                    norm.User.c.Name == bindparam('Name')
                ), {
                    'Name': args.user
                }).first()
            if user:
                args.userId = user['Id']
            else:
                args.userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   args.userId,
                        'Name': args.user
                    })
        else:
            project = norm.con.execute(select([
                    norm.Project.c.ModifiedBy
                ])).first()
            if project:
                args.userId = project['ModifiedBy']
            else:
                args.userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   args.userId,
                        'Name': u"Default User"
                    })
                norm.con.execute(norm.Project.insert(), {
                    'Version': u'0.2',
                    'Title': unicode(args.infile),
                    'Description':  u"Created by NVivotools http://barraqda.org/nvivotools/",
                    'CreatedBy':    args.userId,
                    'CreatedDate':  args.datetimeNow,
                    'ModifiedBy':   args.userId,
                    'ModifiedDate': args.datetimeNow
                })

        for infilepattern in args.infile:
            for infilename in glob.glob(infilepattern):
                if args.verbosity >= 2:
                    print("Loading " + infilename, file=sys.stderr)

                infilebasename, infiletype = os.path.splitext(os.path.basename(infilename))
                infiletype = infiletype[1:].upper()

                if infiletype == 'CSV':
                    incomments = ''
                    csvFile = file(infilename, 'rU')

                    # Skip comments at start of CSV file.
                    while True:
                        line = csvFile.readline()
                        if line[:1] == '#':
                            incomments += line
                        else:
                            csvfieldnames = next(unicodecsv.reader([line]))
                            break

                    if not args.no_comments:
                        logfile.write(incomments)

                    csvreader=unicodecsv.DictReader(csvFile, fieldnames=csvfieldnames)
                    sourceRows = []
                    for row in csvreader:
                        sourceRow = dict(row)
                        sourceRow['Name']        = sourceRow.get('Name',        args.name)
                        sourceRow['Description'] = sourceRow.get('Description', args.description or u"Created by NVivotools http://barraqda.org/nvivotools/")
                        sourceRow['Category']    = sourceRow.get('Category',    args.category)
                        sourceRow['Color']       = sourceRow.get('Color',       args.color)
                        sourceRow['Text']        = sourceRow.get('Text',        args.text)
                        sourceRows.append(sourceRow)

                        if args.limit and len(sourceRows) == args.limit:
                            break

                else:
                    sourceRows = [{
                        'Name':        args.name or infilebasename,
                        'Description': args.description or u"Created by NVivotools http://barraqda.org/nvivotools/",
                        'Category':    args.category,
                        'Color':       args.color,
                        'ObjectFile':  infilename,
                    }]

                editSource(sourceRows, args, norm)

        if not args.infile:
            sourceRows = [{
                'Name':        args.name,
                'Description': args.description or u"Created by NVivotools http://barraqda.org/nvivotools/",
                'Category':    args.category,
                'Color':       args.color,
                'Text':        args.text
            }]
            editSource(sourceRows, args, norm)


        norm.commit()
        del norm

    except:
        raise
        norm.rollback()
        del norm

if __name__ == '__main__':
    editSources(None)
