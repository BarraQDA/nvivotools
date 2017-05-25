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

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

def editNodes(arglist):

    parser = argparse.ArgumentParser(description='Insert or update node in normalised file.')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)

    parser.add_argument('-i', '--infile', type = str, help = 'CSV file containing node information')

    parser.add_argument('-n', '--name',        type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-d', '--description', type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-c', '--category',    type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-p', '--parent',      type = lambda s: unicode(s, 'utf8'))
    parser.add_argument('-a', '--attributes',  type = str, action='append', help='Attributes in format name:value')
    parser.add_argument(      '--color',       type = str)
    parser.add_argument(      '--aggregate',   action = 'store_true')

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
                        'Name': "NVivotools"
                    })
                norm.con.execute(norm.Project.insert(), {
                    'Version': '0.2',
                    'Title': "Created by NVivotools",
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
            nodeRows = []
            for row in csvreader:
                nodeRow = dict(row)
                nodeRow['Name']        = nodeRow.get('Name',        args.name)
                nodeRow['Description'] = nodeRow.get('Description', args.description)
                nodeRow['Category']    = nodeRow.get('Category',    args.category)
                nodeRow['Parent']      = nodeRow.get('Parent',      args.parent)
                nodeRow['Aggregate']   = nodeRow.get('Aggregate',   args.aggregate)
                nodeRow['Category']    = nodeRow.get('Category',    args.category)
                nodeRow['Color']       = nodeRow.get('Color',       args.color)
                nodeRows.append(nodeRow)

            colNames = csvfieldnames
        else:
            nodeRows = [{
                'Name':        args.name,
                'Description': args.description,
                'Category':    args.category,
                'Color':       args.color
            }]
            colNames = ['Name', 'Description', 'Category', 'Color']

        # Fill in attributes from command-line
        if args.attributes:
            for attribute in args.attributes:
                attMatch = re.match("(?P<attname>[^:]+):(?P<attvalue>.+)?", attribute)
                if not parseattribute:
                    raise RuntimeError("Incorrect attribute format " + attribute)

                colnames.append(attName)
                for nodeRow in nodeRows:
                    attName  = attMatch.group('attname')
                    attValue = attMatch.group('attvalue')
                    nodeRow[attName] = nodeRow.get(attName, attValue)

        nodeAttributes = {}
        for attributeName in colNames:
            # Skip reserved attribute names
            if attributeName in ['Name', 'Description', 'Category', 'Parent', 'Aggregate', 'Color']:
                continue

            # Determine whether attribute is already defined
            nodeattribute = norm.con.execute(select([
                    norm.NodeAttribute.c.Id,
                    norm.NodeAttribute.c.Type,
                    norm.NodeAttribute.c.Length
                ]).where(
                    norm.NodeAttribute.c.Name == bindparam('Name')
                ), {
                    'Name': attributeName
                }).first()

            if nodeattribute:
                nodeAttributes[attributeName] = {
                    'Id':     nodeattribute['Id'],
                    'Type':   nodeattribute['Type'],
                    'Length': nodeattribute['Length']
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
                for nodeRow in nodeRows:
                    attributeValue = nodeRow[attributeName]
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

                norm.con.execute(norm.NodeAttribute.insert(), {
                    'Id':           attributeId,
                    'Name':         attributeName,
                    'Description':  "Created by NVivotools",
                    'Type':         attributeType,
                    'Length':       attributeLength,
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                })
                nodeAttributes[attributeName] = {
                    'Id':           attributeId,
                    'Type':         attributeType,
                    'Length':       attributeLength,
                }

        rowNum = 0
        nodesToInsert      = []
        nodeValuesToInsert = []
        for nodeRow in nodeRows:
            rowNum += 1

            categoryName = nodeRow.get('Category')
            categoryId = None
            if categoryName is not None:
                category = norm.con.execute(select([
                        norm.NodeCategory.c.Id
                    ]).where(
                        norm.NodeCategory.c.Name == bindparam('NodeCategory')
                    ), {
                        'NodeCategory': categoryName
                    }).first()

                if category is not None:
                    categoryId = category['Id']
                else:
                    categoryId = uuid.uuid4()
                    norm.con.execute(norm.NodeCategory.insert(), {
                        'Id':           categoryId,
                        'Name':         categoryName,
                        'Description':  "Created by NVivotools",
                        'CreatedBy':    userId,
                        'CreatedDate':  datetimeNow,
                        'ModifiedBy':   userId,
                        'ModifiedDate': datetimeNow
                        })

            parentName = nodeRow.get('Parent')
            parentId = None
            if (parentName or '') != '':
                parentIdList = [nodeToInsert['Id'] for nodeToInsert in nodesToInsert if nodeToInsert['Name'] == parentName]
                if parentIdList:
                    parentId = parentIdList[0]
                else:
                    parent = norm.con.execute(select([
                            norm.Node.c.Id
                        ]).where(
                            norm.Node.c.Name == bindparam('Name')
                        ), {
                            'Name': parentName
                        }).first()

                    if parent is not None:
                        parentId = parent['Id']
                    else:
                        parentId = uuid.uuid4()
                        norm.con.execute(norm.Node.insert(), {
                            'Id':           parentId,
                            'Name':         parentName,
                            'Description':  "Created by NVivotools",
                            'CreatedBy':    userId,
                            'CreatedDate':  datetimeNow,
                            'ModifiedBy':   userId,
                            'ModifiedDate': datetimeNow
                        })

            nodeName        = nodeRow.get('Name')        or str(rowNum)
            nodeDescription = nodeRow.get('Description')
            nodeAggregate   = nodeRow.get('Aggregate')

            node = norm.con.execute(select([
                        norm.Node.c.Id
                    ]).where(
                        norm.Node.c.Name == bindparam('Name')
                    ), {
                        'Name': nodeName
                    }).first()
            nodeId = node['Id'] if node else uuid.uuid4()

            nodeValues = []
            for attributeName, attributeNode in nodeAttributes.iteritems():
                attributeId     = attributeNode['Id']
                attributeType   = attributeNode['Type']
                attributeLength = attributeNode['Length']
                attributeValue  = nodeRow[attributeName]

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

                nodeValues.append({
                        'Node':       nodeId,
                        '_Node':      nodeId,
                        'Attribute':    attributeId,
                        '_Attribute':   attributeId,
                        'Value':        attributeValue,
                        'CreatedBy':    userId,
                        'CreatedDate':  datetimeNow,
                        'ModifiedBy':   userId,
                        'ModifiedDate': datetimeNow
                    })

            normNodeRow = {
                    'Id':           nodeId,
                    '_Id':          nodeId,
                    'Name':         nodeName,
                    'Description':  nodeDescription,
                    'Category':     categoryId,
                    'Parent':       parentId,
                    'Aggregate':    nodeAggregate,
                    'ModifiedBy':   userId,
                    'ModifiedDate': datetimeNow
                }
            normNodeRow['Color'] = nodeRow.get('Color')

            if node is None:    # New node
                normNodeRow.update({
                    'CreatedBy':    userId,
                    'CreatedDate':  datetimeNow,
                })
                nodesToInsert.append(normNodeRow)
                nodeValuesToInsert += nodeValues
            else:
                norm.con.execute(norm.Node.update(
                        norm.Node.c.Id == bindparam('_Id')),
                        normNodeRow)
                for nodeValue in nodeValues:
                    norm.con.execute(norm.NodeValue.delete(and_(
                        norm.NodeValue.c.Node    == bindparam('_Node'),
                        norm.NodeValue.c.Attribute == bindparam('_Attribute'),
                    )), nodeValues)

                nodeValuesToInsert += nodeValues

        if nodesToInsert:
            norm.con.execute(norm.Node.insert(), nodesToInsert)
        if nodeValuesToInsert:
            norm.con.execute(norm.NodeValue.insert(), nodeValuesToInsert)

        norm.commit()
        del norm

    except:
        raise
        norm.rollback()
        del norm

if __name__ == '__main__':
    editNodes(None)
