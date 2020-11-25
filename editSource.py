#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Jonathan Schultz
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

from argrecord import ArgumentHelper, ArgumentRecorder
import os
import sys
from NVivoNorm import NVivoNorm
import csv
import glob
from sqlalchemy import *
import re
from dateutil import parser as dateparser
from datetime import date, time, datetime
from distutils import util
import uuid
import chardet
import codecs
import subprocess
import tempfile
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import StringIO

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

def editSource(arglist=None):

    parser = ArgumentRecorder(description='Insert or update source in normalised file.')

    generalGroup = parser.add_argument_group('General')
    generalGroup.add_argument('-o', '--outfile', type=str, required=True, output=True,
                                                 help='Output normalised NVivo (.nvpn) file')
    generalGroup.add_argument(        'infile',  type = str, nargs = '*', input=True,
                                                 help = 'Source content filename pattern')
    generalGroup.add_argument('-u', '--user',    type=str,
                                                 help='User name, default is project "modified by".')

    singleGroup = parser.add_argument_group('Single source')
    singleGroup.add_argument('-n', '--name',        type = str)
    singleGroup.add_argument('-d', '--description', type = str)
    singleGroup.add_argument('-c', '--category',    type = str)
    singleGroup.add_argument('-a', '--attributes',  type = str, action='append', help='Attributes in format name:value')
    singleGroup.add_argument(      '--color',       type = str)
    singleGroup.add_argument(      '--text',        type = str, help = 'Source text')

    tableGroup = parser.add_argument_group('Table of sources')
    tableGroup.add_argument('-t', '--table',   type=str, input=True,
                                               help='CSV file containing table of sources with their attributes')
    tableGroup.add_argument('-N', '--namecol', type=str, default='Name',
                                               help='Column to use for source name.')
    tableGroup.add_argument('-T', '--textcol', type=str,
                                               help='Column to use for source content text.')
    tableGroup.add_argument('-F', '--filenamecol', type=str, default='Filename',
                                               help='Column to use for source content file name.')
    tableGroup.add_argument('-D', '--filedir', type=str, default='.',
                                               help='Directory where source content file is located.')
    tableGroup.add_argument('-C', '--columns', type = str, nargs = '*',
                                               help = 'Columns from input CSV file to include as attributes, default is all')
    tableGroup.add_argument(      '--exclude', type = str, nargs = '*', default = [],
                                               help = 'Columns from input CSV file to exclude as attributes')
    tableGroup.add_argument('--textcolumns',   type = str, nargs = '*', default = [],
                                               help = 'Columns from input CSV file to include as coded text')

    advancedGroup = parser.add_argument_group('Advanced')
    advancedGroup.add_argument('-e', '--encoding',  type=str,
                                                    help='File encoding; if not specified then we will try to detect.')
    advancedGroup.add_argument('-v', '--verbosity', type=int, default=1, private=True)
    advancedGroup.add_argument('-l', '--limit',     type=int,
                                                    help='Limit number of lines from table file')
    advancedGroup.add_argument('--logfile',         type=str, private=True,
                                                    help="Logfile, default is <outfile>.log")
    advancedGroup.add_argument('--no-logfile',      action='store_true',
                                                    help='Do not output a logfile')

    args = parser.parse_args(arglist)

    if args.textcol and args.filenamecol:
        raise RuntimeError("Only one of 'textcol' and 'filenamecol' may be specified")

    if not args.no_logfile:
        logfilename = args.outfile.rsplit('.',1)[0] + '.log'
        incomments = ArgumentHelper.read_comments(logfilename) or ArgumentHelper.separator()
        logfile = open(logfilename, 'w')
        parser.write_comments(args, logfile, incomments=incomments)

    try:

        norm = NVivoNorm(args.outfile)
        norm.begin()

        datetimeNow = datetime.utcnow()

        if args.user:
            userRecord = norm.con.execute(select([
                    norm.User.c.Id
                ]).where(
                    norm.User.c.Name == bindparam('Name')
                ), {
                    'Name': args.user
                }).first()
            if userRecord:
                userId = userRecord['Id']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': args.user
                    })
        else:
            projectRecord = norm.con.execute(select([
                    norm.Project.c.ModifiedBy
                ])).first()
            if projectRecord:
                userId = projectRecord['ModifiedBy']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': u"Default User"
                    })
                norm.con.execute(norm.Project.insert(), {
                    'Version':      '0.2',
                    'Title':        "Created by NVivotools http://barraqda.org/nvivotools/",
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                })

        sourceRows = []
        for infilePattern in args.infile:
            for infilename in glob.glob(infilePattern):
                if args.verbosity >= 2:
                    print("Loading " + infilename, file=sys.stderr)

                infileBasename, infileType = os.path.splitext(os.path.basename(infilename))
                infileType = infileType[1:].upper()

                sourceRows.append({
                    'Name':        args.name or infileBasename,
                    'Description': args.description or u"Created by NVivotools http://barraqda.org/nvivotools/",
                    'Category':    args.category,
                    'Color':       args.color,
                    'ObjectFile':  infilename,
                })

        if args.table:
            incomments = ''
            tableFile = open(args.table, 'r')

            # Skip comments at start of CSV file.
            while True:
                line = tableFile.readline()
                if line[:1] == '#':
                    incomments += line
                else:
                    tableFieldnames = next(csv.reader([line]))
                    break

            if not args.no_logfile:
                logfile.write(incomments)

            tableFieldnames = [fieldname if fieldname != args.namecol else 'Name' for fieldname in tableFieldnames]
            tableReader=csv.DictReader(tableFile, fieldnames=tableFieldnames)
            for row in tableReader:
                sourceRow = dict(row)
                sourceRow['Description'] = sourceRow.get('Description', args.description or u"Created by NVivotools http://barraqda.org/nvivotools/")
                sourceRow['Category']    = sourceRow.get('Category',    args.category)
                sourceRow['Color']       = sourceRow.get('Color',       args.color)
                if args.textcol or args.text:
                    sourceRow['Text']        = sourceRow.get(args.textcol, args.text)
                else:
                    sourceRow['ObjectFile']  = sourceRow.get(args.filenamecol)

                sourceRows.append(sourceRow)

                if args.limit and len(sourceRows) == args.limit:
                    break

            colNames = tableFieldnames

        elif not args.infile:
            sourceRows = [{
                'Name':        args.name,
                'Description': args.description or u"Created by NVivotools http://barraqda.org/nvivotools/",
                'Category':    args.category,
                'Color':       args.color,
                'Text':        args.text
            }]

        # Define text column node category
        if len(args.textcolumns) > 1:
            textColNodeCategory = norm.con.execute(select([
                    norm.NodeCategory.c.Id
                ]).where(
                    norm.NodeCategory.c.Name == 'Text Columns'
                )).first()
            if textColNodeCategory is not None:
                textColNodeCategoryId = textColNodeCategory['Id']
                if args.verbosity > 1:
                    print("Found Text Columns node category ID ", textColNodeCategoryId)
            else:
                if args.verbosity > 1:
                    print("Creating node category")
                textColNodeCategoryId = uuid.uuid4()
                norm.con.execute(norm.NodeCategory.insert().values({
                        'Id': textColNodeCategoryId,
                        'Name': 'Text Columns',
                        'Description':  u"Created by NVivotools http://barraqda.org/nvivotools/",
                        'CreatedBy':    userId,
                        'CreatedDate':  datetimeNow,
                        'ModifiedBy':   userId,
                        'ModifiedDate': datetimeNow
                    }))

        # Fill in attributes
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
                        except:
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
                        'Type':         attributeType,
                        'Length':       attributeLength,
                        'CreatedBy':    userId,
                        'CreatedDate':  datetimeNow,
                        'ModifiedBy':   userId,
                        'ModifiedDate': datetimeNow
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
                    ]).where(and_(
                        norm.Node.c.Name == bindparam('Name'),
                        norm.Node.c.Category == textColNodeCategoryId
                    )), {
                        'Name': colName
                    }).first()
                if node:
                    nodeId = node['Id']
                else:
                    nodeId = uuid.uuid4()
                    norm.con.execute(norm.Node.insert(), {
                        'Id':           nodeId,
                        'Name':         colName,
                        'Description':  u"Created by NVivotools http://barraqda.org/nvivotools/",
                        'Category':     textColNodeCategoryId,
                        'CreatedBy':    userId,
                        'CreatedDate':  datetimeNow,
                        'ModifiedBy':   userId,
                        'ModifiedDate': datetimeNow
                    })

                sourceNodeId[colName] = nodeId

        rowNum = 0
        sourcesToInsert      = []
        sourceValuesToInsert = []
        digits = len(str(len(sourceRows)))
        unoconvcmd = None
        for sourceRow in sourceRows:
            rowNum += 1

            categoryName = sourceRow.get('Category')
            categoryId = None
            if categoryName is not None:
                categoryRecord = norm.con.execute(select([
                        norm.SourceCategory.c.Id
                    ]).where(
                        norm.SourceCategory.c.Name == bindparam('SourceCategory')
                    ), {
                        'SourceCategory': categoryName
                    }).first()

                if categoryRecord is not None:
                    categoryId = categoryRecord['Id']
                else:
                    categoryId = uuid.uuid4()
                    norm.con.execute(norm.SourceCategory.insert(), {
                        'Id':           categoryId,
                        'Name':         categoryName,
                        'Description':  u"Created by NVivotools http://barraqda.org/nvivotools/",
                        'CreatedBy':    userId,
                        'CreatedDate':  datetimeNow,
                        'ModifiedBy':   userId,
                        'ModifiedDate': datetimeNow
                        })

            sourceName        = sourceRow.get('Name') or str(rowNum).zfill(digits)
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
            for attributeName, attributeSource in sourceAttributes.items():
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

            ObjectFile = sourceRow.get('ObjectFile')
            if ObjectFile:
                ObjectPath = os.path.join(args.filedir, ObjectFile)
                dummy, ObjectFileExt = os.path.splitext(ObjectFile)
                ObjectFileExt = ObjectFileExt[1:].upper()
                normSourceRow['ObjectType'] = ObjectFileExt
                if ObjectFileExt == 'TXT':
                    # Detect file encoding if not specified
                    if not args.encoding:
                        raw = open(ObjectPath, 'rb').read(32) # at most 32 bytes are returned
                        args.encoding = chardet.detect(raw)['encoding']

                    normSourceRow['Content'] = codecs.open(ObjectPath, 'r', encoding=args.encoding).read()
                    if args.encoding != 'utf8':
                        normSourceRow['Content'] = normSourceRow['Content'].encode('utf8')
                else:
                    normSourceRow['Object'] = open(ObjectPath, 'rb').read()

                    if ObjectFileExt in {'DOCX', 'DOC', 'ODT', 'TXT', 'RTF'}:

                        # Hack to remove hidden characters from RTF
                        if ObjectFileExt == 'RTF':
                            rtfFile = open(ObjectPath, 'r')
                            tempFilename = tempfile.mktemp() + '.rtf'
                            tempFile = open(tempFilename, 'w')
                            hiddenPattern = re.compile(r'{[^}].*\\v\\[^}]+}')
                            for rtfLine in rtfFile.readlines():
                                tempFile.write(hiddenPattern.sub('', rtfLine))

                            tempFile.close()
                            ObjectPath = tempFilename

                        # Look for unoconvcmd just once
                        if unoconvcmd is None:
                            if args.verbosity > 1:
                                print("Searching for unoconv executable.", file=sys.stderr)
                            # Look first on path for OS installed version, otherwise use our copy
                            for path in os.environ["PATH"].split(os.pathsep):
                                unoconvpath = os.path.join(path, 'unoconv')
                                if os.path.exists(unoconvpath):
                                    if os.access(unoconvpath, os.X_OK) and '' in os.environ.get("PATHEXT", "").split(os.pathsep):
                                        unoconvcmd = [unoconvpath]
                                    else:
                                        unoconvcmd = ['python', unoconvpath]
                                    break
                            if unoconvcmd is None:
                                unoconvpath = os.path.join(NVivo.helperpath + 'unoconv')
                                if os.path.exists(unoconvpath):
                                    if os.access(unoconvpath, os.X_OK) and '' in os.environ.get("PATHEXT", "").split(os.pathsep):
                                        unoconvcmd = [unoconvpath]
                                    else:
                                        unoconvcmd = ['python', unoconvpath]
                            if unoconvcmd is None:
                                raise RuntimeError("Can't find unoconv on path. Please refer to the NVivotools README file.")

                        tmptextfilename = tempfile.mktemp() + '.txt'
                        cmd = unoconvcmd + ['--format=text', '--output=' + tmptextfilename, ObjectPath]
                        if args.verbosity > 1:
                            print("Running: ", cmd)
                        p = subprocess.run(cmd, stderr=subprocess.PIPE, text=True)
                        err = p.stderr
                        if err:
                            print("Command: ", cmd)
                            raise RuntimeError(err)

                        normSourceRow['Content'] = codecs.open(tmptextfilename, 'r', 'utf-8-sig').read()
                        os.remove(tmptextfilename)

                        if ObjectFileExt == 'RTF':
                            os.remove(ObjectPath)

                    elif ObjectFileExt == 'PDF':

                        rsrcmgr = PDFResourceManager()
                        retstr = StringIO()
                        laparams = LAParams()
                        device = TextConverter(rsrcmgr, retstr, laparams=laparams)

                        pdffileptr = open(ObjectPath, 'rb')
                        interpreter = PDFPageInterpreter(rsrcmgr, device)
                        pagenos = set()
                        pdfpages = PDFPage.get_pages(pdffileptr, password='', check_extractable=True)
                        pdfstr = ''
                        for pdfpage in pdfpages:
                            interpreter.process_page(pdfpage)
                            pagestr = str(retstr.getvalue())
                            pagestr = re.sub('(?<!\n)\n(?!\n)', ' ', pagestr).replace('\n\n', '\n').replace('\x00','')
                            retstr.truncate(0)
                            pdfstr += pagestr

                        normSourceRow['Content'] = pdfstr
                        pdffileptr.close()

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
                                    'Fragment':     str(start) + u':' + str(end),
                                    'Memo':         None,
                                    'CreatedBy':    userId,
                                    'CreatedDate':  datetimeNow,
                                    'ModifiedBy':   userId,
                                    'ModifiedDate': datetimeNow
                                })

                        normSourceRow['Content'] += normSourceText

                normSourceRow['Object'] = bytearray(normSourceRow['Content'], 'utf8')

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

    except:
        raise
        norm.rollback()

    finally:
        del norm
        if not args.no_logfile:
            logfile.close()


if __name__ == '__main__':
    editSource(None)
