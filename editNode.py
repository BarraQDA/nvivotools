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

def add_arguments(parser):
    parser.description = "Insert or update node in normalised file."

    generalgroup = parser.add_argument_group('General')
    generalgroup.add_argument('-o', '--outfile', type=str, required=True,
                                                 help='Output normalised NVivo (.norm) file')
    generalgroup.add_argument(        'infile',  type=str, nargs = '*',
                                                 help='Input CSV file containing node info')
    generalgroup.add_argument('-u', '--user',    type=lambda s: unicode(s, 'utf8'),
                                                 help='User name, default is project "modified by".')

    singlegroup = parser.add_argument_group('Single node')
    singlegroup.add_argument('-n', '--name',        type = lambda s: unicode(s, 'utf8'))
    singlegroup.add_argument('-d', '--description', type = lambda s: unicode(s, 'utf8'))
    singlegroup.add_argument('-c', '--category',    type = lambda s: unicode(s, 'utf8'))
    singlegroup.add_argument('-p', '--parent',      type = lambda s: unicode(s, 'utf8'))
    singlegroup.add_argument('-a', '--attributes',  type = str, action='append', help='Attributes in format name:value')
    singlegroup.add_argument(      '--color',       type = str)
    singlegroup.add_argument(      '--aggregate',   action = 'store_true')

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity', type=int, default=1)
    advancedgroup.add_argument('--no-comments',     action='store_true',
                                                    help='Do not produce a comments logfile')

    parser.set_defaults(func=editNode)
    parser.set_defaults(build_comments=build_comments)
    parser.set_defaults(hiddenargs=['hiddenargs', 'verbosity', 'no_comments'])

def parse_arguments():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    return vars(parser.parse_args())

def build_comments(kwargs):
    comments = ((' ' + kwargs['outfile'] + ' ') if kwargs['outfile'] else '').center(80, '#') + '\n'
    comments += '# ' + os.path.basename(__file__) + '\n'
    hiddenargs = kwargs['hiddenargs'] + ['hiddenargs', 'func', 'build_comments']
    for argname, argval in kwargs.iteritems():
        if argname not in hiddenargs:
            if type(argval) == str or type(argval) == unicode:
                comments += '#     --' + argname + '="' + argval + '"\n'
            elif type(argval) == bool:
                if argval:
                    comments += '#     --' + argname + '\n'
            elif type(argval) == list:
                for valitem in argval:
                    if type(valitem) == str:
                        comments += '#     --' + argname + '="' + valitem + '"\n'
                    else:
                        comments += '#     --' + argname + '=' + str(valitem) + '\n'
            elif argval is not None:
                comments += '#     --' + argname + '=' + str(argval) + '\n'

    return comments

def editNode(outfile, infile, user,
              name, description, category, parent, attributes, color, aggregate,
              verbosity, no_comments,
              comments, **dummy):

    try:

        # Read and skip comments at start of CSV file.
        csvcomments = ''
        if infile:
            csvFile = file(infile, 'r')

            while True:
                line = csvFile.readline()
                if line[:1] == '#':
                    csvcomments += line
                else:
                    csvfieldnames = next(unicodecsv.reader([line]))
                    break

        if not no_comments:
            logfilename = outfile.rsplit('.',1)[0] + '.log'
            if os.path.isfile(logfilename):
                incomments = open(logfilename, 'r').read()
            else:
                incomments = ''
            logfile = open(logfilename, 'w')
            logfile.write(comments)
            logfile.write(csvcomments)
            logfile.write(incomments)
            logfile.close()

        norm = NVivoNorm(outfile)
        norm.begin()

        datetimeNow = datetime.utcnow()

        if user:
            user = norm.con.execute(select([
                    norm.User.c.Id
                ]).where(
                    norm.User.c.Name == bindparam('Name')
                ), {
                    'Name': user
                }).first()
            if user:
                userId = user['Id']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': user
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

        if infile:
            csvreader=unicodecsv.DictReader(csvFile, fieldnames=csvfieldnames)
            nodeRows = []
            for row in csvreader:
                nodeRow = dict(row)
                nodeRow['Name']        = nodeRow.get('Name',        name).strip()
                nodeRow['Description'] = nodeRow.get('Description', description)
                nodeRow['Category']    = nodeRow.get('Category',    category)
                nodeRow['Parent']      = nodeRow.get('Parent',      parent).strip()
                nodeRow['Aggregate']   = nodeRow.get('Aggregate',   aggregate)
                nodeRow['Category']    = nodeRow.get('Category',    category)
                nodeRow['Color']       = nodeRow.get('Color',       color)
                nodeRows.append(nodeRow)

            colNames = csvfieldnames
        else:
            nodeRows = [{
                'Name':        name.strip(),
                'Description': description,
                'Category':    category,
                'Color':       color
            }]
            colNames = ['Name', 'Description', 'Category', 'Color']

        # Fill in attributes from command-line
        if attributes:
            for attribute in attributes:
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
                    'Description':  "Created by NVivotools http://barraqda.org/nvivotools/",
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
            for attributeName, attributeNode in nodeAttributes.iteritems():
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

def main():
    kwargs = parse_arguments()
    kwargs['comments'] = build_comments(kwargs)
    kwargs['func'](**kwargs)

if __name__ == '__main__':
    main()
