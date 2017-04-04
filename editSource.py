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
import chardet
import codecs
import tempfile
from subprocess import Popen, PIPE

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())


parser = argparse.ArgumentParser(description='Insert or update source in normalised file.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)

parser.add_argument('-n', '--name',        type = lambda s: unicode(s, 'utf8'))
parser.add_argument('-d', '--description', type = lambda s: unicode(s, 'utf8'))
parser.add_argument('-c', '--category',    type = lambda s: unicode(s, 'utf8'))
parser.add_argument('-a', '--attribute',   action='append')
parser.add_argument(      '--color',       type = str)
parser.add_argument('-s', '--source',      type = str,
                    help = 'Source file name')

parser.add_argument('-u', '--user',        type = lambda s: unicode(s, 'utf8'),
                    help = 'User name, default is project "modified by".')

parser.add_argument('normFile', type=str)

args = parser.parse_args()

try:
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

    categoryId = None
    if args.category is not None:
        category = normcon.execute(select([
                normSourceCategory.c.Id
            ]).where(
                normSourceCategory.c.Name == bindparam('SourceCategory')
            ), {
                'SourceCategory': args.category
            }).first()

        if category is None:
            raise RuntimeError("Source category: " + args.category + " not found.")

        categoryId = category['Id']

    if args.user is not None:
        user = normcon.execute(select([
                normUser.c.Id
            ]).where(
                normUser.c.Name == bindparam('Name')
            ), {
                'Name': args.user
            }).first()
        if user is not None:
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

    source = normcon.execute(select([
                normSource.c.Id
            ]).where(
                normSource.c.Name == bindparam('Name')
            ), {
                'Name': args.name
            }).first()
    Id = uuid.uuid4() if source is None else source['Id']

    datetimeNow = datetime.now()

    sourceValues = []
    if args.attribute is not None:
        for attribute in args.attribute:
            parseattribute = re.match("([^:]+):(.+)?", attribute)
            if parseattribute is None:
                raise RuntimeError("Incorrect attribute format " + attribute)

            attributeName, attributeValue = parseattribute.group(1), parseattribute.group(2)
            sourceattribute = normcon.execute(select([
                    normSourceAttribute.c.Id,
                    normSourceAttribute.c.Type,
                    normSourceAttribute.c.Length
                ]).where(
                    normSourceAttribute.c.Name == bindparam('Name')
                ), {
                    'Name': attributeName
                }).first()
            if sourceattribute is None:
                raise RuntimeError("Source attribute: " + attributeName + " not found.")

            attributeId     = sourceattribute['Id']
            attributeType   = sourceattribute['Type']
            attributeLength = sourceattribute['Length']

            if attributeType == 'Text':
                if attributeLength and len(attributeValue) > attributeLength:
                    raise RuntimeError("Value: " + attributeValue + " longer than attribute length")
            elif attributeType == 'Integer':
                int(attributeValue)
            elif attributeType == 'Decimal':
                float(attributeValue)
            elif attributeType == 'Datetime':
                attributeValue = datetime.isoformat(dateparser.parse(attributeValue))
            elif attributeType == 'Date':
                attributeValue = date.isoformat(dateparser.parse(attributeValue))
            elif attributeType == 'Time':
                attributeValue = time.isoformat(dateparser.parse(attributeValue).time())
            elif attributeType == 'Boolean':
                attributeValue = str(bool(util.strtobool(attributeValue)))
            else:
                raise RuntimeError("Unknown attribute type: " + attributeType)

            sourceValues += [{
                    'Source':         Id,
                    '_Source':        Id,
                    'Attribute':    attributeId,
                    '_Attribute':   attributeId,
                    'Value':        attributeValue,
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                }]

    sourceColumns = {
            'Id':           Id,
            '_Id':          Id,
            'CreatedBy':    userId,
            'CreatedDate':  datetimeNow,
            'ModifiedBy':   userId,
            'ModifiedDate': datetimeNow
        }
    if categoryId:
        sourceColumns['Category'] = categoryId
    if args.name:
        sourceColumns['Name'] = args.name
    if args.description:
        sourceColumns['Description']  = args.description
    if args.color:
        sourceColumns['Color'] = args.color
    if args.source:
        sourceColumns['ObjectType'] = os.path.splitext(args.source)[1][1:].upper()

        if sourceColumns['ObjectType'] == 'TXT':
            # detect file encoding
            raw = file(args.source, 'rb').read(32) # at most 32 bytes are returned
            encoding = chardet.detect(raw)['encoding']

            sourceColumns['Object'] = codecs.open(args.source, 'r', encoding=encoding).read().encode('utf-8')
            sourceColumns['Content'] = sourceColumns['Object']
        else:
            sourceColumns['Object'] = file(args.source, 'rb').read()

            # This code copied from NVivo.py
            if args.verbosity > 1:
                print("Searching for unoconv executable.")
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
                unoconvpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'unoconv')
                if os.path.exists(unoconvpath):
                    if os.access(unoconvpath, os.X_OK) and '' in os.environ.get("PATHEXT", "").split(os.pathsep):
                        unoconvcmd = [unoconvpath]
                    else:
                        unoconvcmd = ['python', unoconvpath]
            if unoconvcmd is None:
                raise RuntimeError("Can't find unoconv on path. Please refer to the NVivotools README file.")

            # Use unoconv to convert to text
            tmpfilename = tempfile.mktemp()
            if sourceColumns['ObjectType'] == 'TXT':
                sourceColumns['Content'] = sourceColumns['Object']
            else:
                tmpfile = file(tmpfilename + '.' + sourceColumns['ObjectType'], 'wb')

                tmpfile.write(sourceColumns['Object'])
                tmpfile.close()

                p = Popen(unoconvcmd + ['--format=text', tmpfilename + '.' + sourceColumns['ObjectType']], stderr=PIPE)
                err = p.stderr.read()
                if err != '':
                    raise RuntimeError(err)

                # Read text output from unocode, then massage it by first dropping a final line
                # terminator, then changing to Windows (CRLF) or Mac (LFLF) line terminators
                sourceColumns['Content'] = codecs.open(tmpfilename + '.txt', 'r', 'utf-8-sig').read()
                os.remove(tmpfilename + '.txt')
                os.remove(tmpfilename + '.' + sourceColumns['ObjectType'])

    if source is None:    # New source
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
