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

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())


parser = argparse.ArgumentParser(description='Insert or update node in normalised file.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)

parser.add_argument('-n', '--name',        type = str)
parser.add_argument('-d', '--description', type = str)
parser.add_argument('-c', '--category',    type = str)
parser.add_argument('-a', '--attribute',   action='append')
parser.add_argument('-p', '--parent',      type = str)
parser.add_argument(      '--color',       type = str)
parser.add_argument(      '--aggregate',   action = 'store_true')

parser.add_argument('-u', '--user',        type = str,
                    help = 'User name, default is project "modified by".')

parser.add_argument('normFile', type=str)

args = parser.parse_args()

try:
    normdb  = create_engine('sqlite:///' + args.normFile)
    normmd  = MetaData(bind=normdb)
    normcon = normdb.connect()
    normtr  = normcon.begin()

    normUser          = Table('User',          normmd, autoload=True)
    normProject       = Table('Project',       normmd, autoload=True)
    normNode          = Table('Node',          normmd, autoload=True)
    normNodeCategory  = Table('NodeCategory',  normmd, autoload=True)
    normNodeAttribute = Table('NodeAttribute', normmd, autoload=True)
    normNodeValue     = Table('NodeValue',     normmd, autoload=True)

    categoryId = None
    if args.category is not None:
        category = normcon.execute(select([
                normNodeCategory.c.Id
            ]).where(
                normNodeCategory.c.Name == bindparam('NodeCategory')
            ), {
                'NodeCategory': args.category
            }).first()

        if category is None:
            raise RuntimeError("Node category: " + args.category + " not found.")

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

    node = normcon.execute(select([
                normNode.c.Id
            ]).where(
                normNode.c.Name == bindparam('Name')
            ), {
                'Name': args.name
            }).first()
    Id = uuid.uuid4() if node is None else node['Id']

    datetimeNow = datetime.now()

    nodeValues = []
    if args.attribute is not None:
        for attribute in args.attribute:
            parseattribute = re.match("([^:]+):(.+)?", attribute)
            if parseattribute is None:
                raise RuntimeError("Incorrect attribute format " + attribute)

            attributeName, attributeValue = parseattribute.group(1), parseattribute.group(2)
            nodeattribute = normcon.execute(select([
                    normNodeAttribute.c.Id,
                    normNodeAttribute.c.Type,
                    normNodeAttribute.c.Length
                ]).where(
                    normNodeAttribute.c.Name == bindparam('Name')
                ), {
                    'Name': attributeName
                }).first()
            if nodeattribute is None:
                raise RuntimeError("Node attribute: " + attributeName + " not found.")

            attributeId     = nodeattribute['Id']
            attributeType   = nodeattribute['Type']
            attributeLength = nodeattribute['Length']

            if attributeType == 'Text':
                if len(attributeValue) > attributeLength:
                    raise RuntimeError("Value: " + attributeValue + " longer than attribute length")
            elif attributeType == 'Integer':
                int(attributeValue)
            elif attributeType == 'Decimal':
                float(attributeValue)
            elif attributeType == 'Datetime':
                attributeValue = date.strftime(dateparser.parse(attributeValue), '%Y-%m-%d %H:%M:%SZ')
            elif attributeType == 'Date':
                attributeValue = date.strftime(dateparser.parse(attributeValue), '%Y-%m-%d 00:00:00Z')
            elif attributeType == 'Time':
                attributeValue = time.strftime(dateparser.parse(attributeValue).time(), '%H:%M:%S')
            elif attributeType == 'Boolean':
                attributeValue = str(bool(util.strtobool(attributeValue)))
            else:
                raise RuntimeError("Unknown attribute type: " + attributeType)

            nodeValues += [{
                    'Node':         Id,
                    '_Node':        Id,
                    'Attribute':    attributeId,
                    '_Attribute':   attributeId,
                    'Value':        attributeValue,
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                }]

    parentId = None
    if args.parent is not None:
        parent = normcon.execute(select([
                normNode.c.Id
            ]).where(
                normNode.c.Name == bindparam('Name')
            ), {
                'Name': args.parent
            }).first()
        if parent is None:
            raise RuntimeError('Parent node "' + args.parent + '" does not exist.')
        parentId = parent['Id']

    nodeColumns = {
            'Id':           Id,
            '_Id':          Id,
            'Parent':       parentId,
            'Category':     categoryId,
            'Name':         args.name,
            'Description':  args.description,
            'Color':        args.color,
            'Aggregate':    args.aggregate,
            'CreatedBy':    userId,
            'CreatedDate':  datetimeNow,
            'ModifiedBy':   userId,
            'ModifiedDate': datetimeNow
        }
    if node is None:    # New node
        normcon.execute(normNode.insert(), nodeColumns)
        if len(nodeValues) > 0:
            normcon.execute(normNodeValue.insert(), nodeValues)
    else:
        normcon.execute(normNode.update(
                normNode.c.Id == bindparam('_Id')),
                nodeColumns)
        for nodeValue in nodeValues:
            normcon.execute(normNodeValue.delete(and_(
                normNodeValue.c.Node      == bindparam('_Node'),
                normNodeValue.c.Attribute == bindparam('_Attribute'),
            )), nodeValues)
            normcon.execute(normNodeValue.insert(), nodeValues)


    normtr.commit()
    normtr = None
    normcon.close()
    normdb.dispose()


except:
    raise
    if not normtr is None:
        normtr.rollback()
    normdb.dispose()
