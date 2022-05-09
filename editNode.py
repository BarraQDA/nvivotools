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
from sqlalchemy import *
import re
from dateutil import parser as dateparser
from datetime import date, time, datetime
from distutils import util
import uuid

def editNode(arglist=None):
    
    parser = ArgumentRecorder(description='Insert or update node in normalised file.')

    generalGroup = parser.add_argument_group('General')
    generalGroup.add_argument('-o', '--outfile', type=str, required=True, output=True,
                                                 help='Output normalised NVivo (.nvpn) file')
    generalGroup.add_argument('-u', '--user',    type=str,
                                                 help='User name, default is project "modified by".')

    singleGroup = parser.add_argument_group('Single node')
    singleGroup.add_argument('-n', '--name',        type = str)
    singleGroup.add_argument('-d', '--description', type = str)
    singleGroup.add_argument('-c', '--category',    type = str)
    singleGroup.add_argument('-p', '--parent',      type = str)
    singleGroup.add_argument('-a', '--attributes',  type = str, action='append', help='Attributes in format name:value')
    singleGroup.add_argument(      '--color',       type = str)
    singleGroup.add_argument(      '--aggregate',   action = 'store_true')

    tableGroup = parser.add_argument_group('Table of nodes')
    tableGroup.add_argument('-t', '--table',        type=str, input=True,
                                                    help='CSV file containing table of nodes with their attributes')
    tableGroup.add_argument('-N', '--namecol',      type=str, default='Name',
                                                    help='Column to use for node name.')
    
    advancedGroup = parser.add_argument_group('Advanced')
    advancedGroup.add_argument('-v', '--verbosity', type=int, default=1,
                                                    private=True)
    advancedGroup.add_argument('--logfile',         type=str, private=True,
                                                    help="Logfile, default is <outfile>.log")
    advancedGroup.add_argument('--no-logfile',      action='store_true', 
                                                    help='Do not output a logfile')

    args = parser.parse_args(arglist)

    if not args.no_logfile:
        logFilename = args.outfile.rsplit('.',1)[0] + '.log'
        incomments = ArgumentHelper.read_comments(logFilename) or ArgumentHelper.separator()
        logfile = open(logFilename, 'w')
        parser.write_comments(args, logfile, incomments=incomments)
        logfile.close()

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

        if args.table:
            tableFile = open(args.table, 'r')
            tableFieldnames = next(csv.reader([next(tableFile)]))
            tableFieldnames = [fieldname if fieldname != args.namecol else 'Name' for fieldname in tableFieldnames]
            tableReader=csv.DictReader(tableFile, fieldnames=tableFieldnames)
            nodeRows = []
            for row in tableReader:
                nodeRow = dict(row)
                nodeRow['Description'] = nodeRow.get('Description', args.description or u"Created by NVivotools http://barraqda.org/nvivotools/")
                nodeRow['Category']    = nodeRow.get('Category',    args.category)
                nodeRow['Parent']      = nodeRow.get('Parent',      args.parent)
                nodeRow['Aggregate']   = nodeRow.get('Aggregate',   args.aggregate)
                nodeRow['Color']       = nodeRow.get('Color',       args.color)
                nodeRows.append(nodeRow)

            colNames = tableFieldnames
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

                colNames.append(attName)
                for nodeRow in nodeRows:
                    attName  = attMatch.group('attname')
                    attValue = attMatch.group('attvalue')
                    nodeRow[attName] = nodeRow.get(attName, attValue)

        nodeAttributeRecords = {}
        for attributeName in colNames:
            # Skip reserved attribute names
            if attributeName in ['Name', 'Description', 'Category', 'Parent', 'Aggregate', 'Color']:
                continue

            # Determine whether attribute is already defined
            nodeAttributeRecord = norm.con.execute(select([
                    norm.NodeAttribute.c.Id,
                    norm.NodeAttribute.c.Type,
                    norm.NodeAttribute.c.Length
                ]).where(
                    norm.NodeAttribute.c.Name == bindparam('Name')
                ), {
                    'Name': attributeName
                }).first()

            if nodeAttributeRecord:
                nodeAttributeRecords[attributeName] = {
                    'Id':     nodeAttributeRecord['Id'],
                    'Type':   nodeAttributeRecord['Type'],
                    'Length': nodeAttributeRecord['Length']
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

                norm.con.execute(norm.NodeAttribute.insert(), {
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
                nodeAttributeRecords[attributeName] = {
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
                categoryRecord = norm.con.execute(select([
                        norm.NodeCategory.c.Id
                    ]).where(
                        norm.NodeCategory.c.Name == bindparam('NodeCategory')
                    ), {
                        'NodeCategory': categoryName
                    }).first()

                if categoryRecord is not None:
                    categoryId = categoryRecord['Id']
                else:
                    categoryId = uuid.uuid4()
                    norm.con.execute(norm.NodeCategory.insert(), {
                        'Id':           categoryId,
                        'Name':         categoryName,
                        'Description':  "Created by NVivotools http://barraqda.org/nvivotools/",
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
                    parentRecord = norm.con.execute(select([
                            norm.Node.c.Id
                        ]).where(
                            norm.Node.c.Name == bindparam('Name')
                        ), {
                            'Name': parentName
                        }).first()

                    if parentRecord is not None:
                        parentId = parentRecord['Id']
                    else:
                        parentId = uuid.uuid4()
                        norm.con.execute(norm.Node.insert(), {
                            'Id':           parentId,
                            'Name':         parentName,
                            'Description':  "Created by NVivotools http://barraqda.org/nvivotools/",
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
            for attributeName, attributeNode in nodeAttributeRecords.items():
                attributeId     = attributeNode['Id']
                attributeType   = attributeNode['Type']
                attributeLength = attributeNode['Length']
                attributeValue  = nodeRow[attributeName]

                if not attributeValue:
                    continue

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

    except:
        raise
        norm.rollback()

    finally:
        del norm

if __name__ == '__main__':
    editNode(None)
