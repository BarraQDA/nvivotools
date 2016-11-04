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

from builtins import chr
from sqlalchemy import *
from sqlalchemy import exc
from xml.dom.minidom import *
import warnings
import sys
import os
import codecs
from subprocess import Popen, PIPE
import argparse
import uuid
import re
import zlib
import datetime
from dateutil import parser as dateparser
from PIL import Image
import tempfile
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

# Generic merge/overwrite/replace function
def merge_overwrite_or_replace(conn, table, columns, data, operation, verbosity):
    if verbosity > 1:
        print("merge_overwrite_or_replace('" + table.name + "'," + repr(columns) + "," + repr(data) + ",'" + operation + "')")
    newids = [{column:row[column] for column in columns} for row in data]
    curids = [{column:row[column] for column in columns}
                for row in conn.execute(select([table.c[column] for column in columns]))]
    if verbosity > 1:
        print("newids " + repr(newids))
        print("curids " + repr(curids))

    if operation == 'replace':
        idstodelete = [id for id in curids if not id in newids]
        if len(idstodelete) > 0:
            if verbosity > 1:
                print("    deleting " + repr(idstodelete))
            delete = table.delete()
            for column in columns:
                if column == 'Id':  # Hack to catch reserved word disallowed in bindparam
                    for id in idstodelete:
                        id['_' + column] = id[column]
                    delete = delete.where(table.c[column] == bindparam('_' + column))
                else:
                    delete = delete.where(table.c[column] == bindparam(column))
            conn.execute(delete, idstodelete)

    if operation == 'overwrite' or operation == 'replace':
        rowstoupdate = [row for row in data if {column:row[column] for column in columns} in curids]
        if len(rowstoupdate) > 0:
            if verbosity > 1:
                print("    updating " + repr(rowstoupdate))
            update = table.update()
            for column in columns:
                if column == 'Id':  # Hack to catch reserved word disallowed in bindparam
                    for id in rowstoupdate:
                        id['_' + column] = id[column]
                    update = update.where(table.c[column] == bindparam('_' + column))
                else:
                    update = update.where(table.c[column] == bindparam(column))
            conn.execute(update, rowstoupdate)

    rowstoinsert = [row for row in data if not {column:row[column] for column in columns} in curids]
    if len(rowstoinsert) > 0:
        if verbosity > 1:
            print("    inserting " + repr(rowstoinsert))
        conn.execute(table.insert(), rowstoinsert)

def Normalise(args):
    # Initialise DB variables so exception handlers don't freak out
    nvivodb = None
    normdb = None
    normtr = None

    try:
        if args.indb != '-':
            nvivodb = create_engine(args.indb)
            nvivomd = MetaData(bind=nvivodb)
            nvivomd.reflect(nvivodb)

            nvivoAnnotation    = nvivomd.tables.get('Annotation')
            nvivoExtendedItem  = nvivomd.tables.get('ExtendedItem')
            nvivoItem          = nvivomd.tables.get('Item')
            nvivoNodeReference = nvivomd.tables.get('NodeReference')
            nvivoProject       = nvivomd.tables.get('Project')
            nvivoRole          = nvivomd.tables.get('Role')
            nvivoSource        = nvivomd.tables.get('Source')
            nvivoUserProfile   = nvivomd.tables.get('UserProfile')
        else:
            nvivodb = None

        if args.outdb is None:
            args.outdb = args.indb.rsplit('.',1)[0] + '.norm'
        normdb = create_engine(args.outdb)
        normmd = MetaData(bind=normdb)

        if args.structure:
            normmd.drop_all(normdb)
            for table in reversed(normmd.sorted_tables):
                normmd.remove(table)

# Create the normalised database structure
        normUser = normmd.tables.get('User') or Table('User', normmd,
            Column('Id',            UUID(),         primary_key=True),
            Column('Name',          String(256)))

        normProject = normmd.tables.get('Project') or Table('Project', normmd,
            Column('Title',         String(256),                            nullable=False),
            Column('Description',   String(2048)),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id"),  nullable=False),
            Column('CreatedDate',   DateTime,                               nullable=False),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id"),  nullable=False),
            Column('ModifiedDate',  DateTime,                               nullable=False))

        normSource = normmd.tables.get('Source') or Table('Source', normmd,
            Column('Id',            UUID(),         primary_key=True),
            Column('Category',      UUID()),
            Column('Name',          String(256)),
            Column('Description',   String(512)),
            Column('Color',         Integer),
            Column('Content',       String(16384)),
            Column('ObjectType',    String(256)),
            Column('SourceType',    Integer),
            Column('Object',        LargeBinary,    nullable=False),
            Column('Thumbnail',     LargeBinary),
            #Column('Waveform',      LargeBinary,    nullable=False),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

        normSourceCategory = normmd.tables.get('SourceCategory') or Table('SourceCategory', normmd,
            Column('Id',            UUID(),         primary_key=True),
            Column('Name',          String(256)),
            Column('Description',   String(512)),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

        normTagging = normmd.tables.get('Tagging') or Table('Tagging', normmd,
            Column('Source',        UUID(),         ForeignKey("Source.Id")),
            Column('Node',          UUID(),         ForeignKey("Node.Id")),
            Column('Fragment',      String(256)),
            Column('Memo',          String(256)),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

        normNode = normmd.tables.get('Node') or Table('Node', normmd,
            Column('Id',            UUID(),         primary_key=True),
            Column('Parent',        UUID(),         ForeignKey("Node.Id")),
            Column('Category',      UUID(),         ForeignKey("NodeCategory.Id")),
            Column('Name',          String(256)),
            Column('Description',   String(512)),
            Column('Color',         Integer),
            Column('Aggregate',     Boolean),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

        normNodeCategory = normmd.tables.get('NodeCategory') or Table('NodeCategory', normmd,
            Column('Id',            UUID(),         primary_key=True),
            Column('Name',          String(256)),
            Column('Description',   String(512)),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

        normSourceAttribute = normmd.tables.get('SourceAttribute') or Table('SourceAttribute', normmd,
            Column('Source',        UUID(),         ForeignKey("Source.Id"),    primary_key=True),
            Column('Name',          String(256),                                primary_key=True),
            Column('Value',         String(256)),
            Column('Type',          String(16)),
            Column('Length',        Integer),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

        normNodeAttribute = normmd.tables.get('NodeAttribute') or Table('NodeAttribute', normmd,
            Column('Node',          UUID(),         ForeignKey("Node.Id"),      primary_key=True),
            Column('Name',          String(256),                                primary_key=True),
            Column('Value',         String(256)),
            Column('Type',          String(16)),
            Column('Length',        Integer),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

        if args.structure:
            normmd.create_all(normdb)

        if nvivodb is None:     # that is, if all we are doing is making an empty norm file
            return

        normcon = normdb.connect()
        normtr = normcon.begin()

# Users
        if args.users != 'skip':
            if args.verbosity > 0:
                print("Normalising users")

            users = [dict(row) for row in nvivodb.execute(select([
                    nvivoUserProfile.c.Id,
                    nvivoUserProfile.c.Name]
                ))]

            merge_overwrite_or_replace(normcon, normUser, ['Id'], users, args.users, args.verbosity)

# Project
        if args.project != 'skip':
            if args.verbosity > 0:
                print("Normalising project")

            project = dict(nvivodb.execute(select([
                    nvivoProject.c.Title,
                    nvivoProject.c.Description,
                    nvivoProject.c.CreatedBy,
                    nvivoProject.c.CreatedDate,
                    nvivoProject.c.ModifiedBy,
                    nvivoProject.c.ModifiedDate
                ])).fetchone())
            if args.windows:
                project['Title']       = u''.join(map(lambda ch: chr(ord(ch) - 0x377), project['Title']))
                project['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), project['Description']))

            # SQLAlchemy should probably handle this...
            if not isinstance(project['CreatedDate'], datetime.datetime):
                project['CreatedDate'] = dateparser.parse(project['CreatedDate'])
            if not isinstance(project['ModifiedDate'], datetime.datetime):
                project['ModifiedDate'] = dateparser.parse(project['ModifiedDate'])

            normcon.execute(normProject.delete())
            normcon.execute(normProject.insert(), project)

# Node Categories
        if args.node_categories != 'skip':
            if args.verbosity > 0:
                print("Normalising node categories")

            nodecategories = [dict(row) for row in nvivodb.execute(select([
                    nvivoItem.c.Id,
                    nvivoItem.c.Name,
                    nvivoItem.c.Description,
                    nvivoItem.c.CreatedBy,
                    nvivoItem.c.CreatedDate,
                    nvivoItem.c.ModifiedBy,
                    nvivoItem.c.ModifiedDate]
                ).where(
                    nvivoItem.c.TypeId == literal_column('52')
                ))]

            for nodecategory in nodecategories:
                if args.windows:
                    nodecategory['Name']        = u''.join(map(lambda ch: chr(ord(ch) - 0x377), nodecategory['Name']))
                    nodecategory['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), nodecategory['Description']))

                if not isinstance(nodecategory['CreatedDate'], datetime.datetime):
                    nodecategory['CreatedDate'] = dateparser.parse(nodecategory['CreatedDate'])
                if not isinstance(nodecategory['ModifiedDate'], datetime.datetime):
                    nodecategory['ModifiedDate'] = dateparser.parse(nodecategory['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normNodeCategory, ['Id'], nodecategories, args.node_categories, args.verbosity)

# Nodes
        if args.nodes != 'skip':
            if args.verbosity > 0:
                print("Normalising nodes")

            nvivoCategoryRole = nvivoRole.alias(name='CategoryRole')
            nvivoParentRole   = nvivoRole.alias(name='ParentRole')

            nodes = [dict(row) for row in nvivodb.execute(select([
                    nvivoItem.c.Id,
                    nvivoCategoryRole.c.Item2_Id.label('Category'),
                    nvivoItem.c.Name,
                    nvivoItem.c.Description,
                    nvivoItem.c.ColorArgb.label('Color'),
                    nvivoItem.c.Aggregate,
                    nvivoItem.c.CreatedBy,
                    nvivoItem.c.CreatedDate,
                    nvivoItem.c.ModifiedBy,
                    nvivoItem.c.ModifiedDate,
                    nvivoParentRole.c.Item1_Id.label('Parent')]
                ).where(
                    nvivoItem.c.TypeId == literal_column('16'),
                ).select_from(nvivoItem.outerjoin(
                    nvivoCategoryRole,
                and_(
                    nvivoCategoryRole.c.TypeId == literal_column('14'),
                    nvivoCategoryRole.c.Item1_Id == nvivoItem.c.Id)
                ).outerjoin(
                    nvivoParentRole,
                and_(
                    nvivoParentRole.c.TypeId == literal_column('1'),
                    nvivoParentRole.c.Item2_Id == nvivoItem.c.Id
                ))))]
            for node in nodes:
                if args.windows:
                    node['Name']        = u''.join(map(lambda ch: chr(ord(ch) - 0x377), node['Name']))
                    node['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), node['Description']))

                if not isinstance(node['CreatedDate'], datetime.datetime):
                    node['CreatedDate'] = dateparser.parse(node['CreatedDate'])
                if not isinstance(node['ModifiedDate'], datetime.datetime):
                    node['ModifiedDate'] = dateparser.parse(node['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normNode, ['Id'], nodes, args.nodes, args.verbosity)

# Node attributes
        if args.node_attributes != 'skip':
            if args.verbosity > 0:
                print("Normalising node attributes")

            nvivoNodeItem     = nvivoItem.alias(name='NodeItem')
            nvivoNameItem     = nvivoItem.alias(name='NameItem')
            nvivoNameRole     = nvivoRole.alias(name='NameRole')
            nvivoValueItem    = nvivoItem.alias(name='ValueItem')
            nvivoValueRole    = nvivoRole.alias(name='ValueRole')

            nodeattrs = [dict(row) for row in nvivodb.execute(select([
                    nvivoNodeItem.c.Id.label('Node'),
                    nvivoNameItem.c.Name.label('Name'),
                    nvivoValueItem.c.Name.label('Value'),
                    nvivoValueItem.c.CreatedBy,
                    nvivoValueItem.c.CreatedDate,
                    nvivoValueItem.c.ModifiedBy,
                    nvivoValueItem.c.ModifiedDate,
                    nvivoNameRole.c.TypeId.label('NameRoleTypeId'),
                    nvivoValueRole.c.TypeId.label('ValueRoleTypeId'),
                    nvivoExtendedItem.c.Properties]
                ).where(and_(
                    nvivoNodeItem.c.TypeId==literal_column('16'),
                    nvivoNodeItem.c.Id == nvivoValueRole.c.Item1_Id,
                    nvivoValueRole.c.TypeId == literal_column('7'),
                    nvivoValueItem.c.Id == nvivoValueRole.c.Item2_Id,
                    nvivoNameRole.c.Item2_Id == nvivoValueRole.c.Item2_Id,
                    nvivoNameRole.c.TypeId == literal_column('6'),
                    nvivoNameItem.c.Id == nvivoNameRole.c.Item1_Id,
                    nvivoValueItem.c.Name != literal_column("'Unassigned'"),
                    nvivoExtendedItem.c.Item_Id == nvivoNameItem.c.Id
                )))]
            for nodeattr in nodeattrs:
                properties = parseString(nodeattr['Properties'])
                for property in properties.documentElement.getElementsByTagName('Property'):
                    if property.getAttribute('Key') == 'DataType':
                        nodeattr['Type'] = DataTypeName.get(int(property.getAttribute('Value')), property.getAttribute('Value'))
                    elif property.getAttribute('Key') == 'Length':
                        nodeattr['Length'] = int(property.getAttribute('Value'))

            for nodeattr in nodeattrs:
                if args.windows:
                    nodeattr['Name']  = u''.join(map(lambda ch: chr(ord(ch) - 0x377), nodeattr['Name']))
                    nodeattr['Value'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), nodeattr['Value']))

                if not isinstance(nodeattr['CreatedDate'], datetime.datetime):
                    nodeattr['CreatedDate'] = dateparser.parse(nodeattr['CreatedDate'])
                if not isinstance(nodeattr['ModifiedDate'], datetime.datetime):
                    nodeattr['ModifiedDate'] = dateparser.parse(nodeattr['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normNodeAttribute, ['Node', 'Name'], nodeattrs, args.node_attributes, args.verbosity)

# Source categories
        if args.source_categories != 'skip':
            if args.verbosity > 0:
                print("Normalising source categories")

            sourcecats  = [dict(row) for row in nvivodb.execute(select([
                    nvivoItem.c.Id,
                    nvivoItem.c.Name,
                    nvivoItem.c.Description,
                    nvivoItem.c.CreatedBy,
                    nvivoItem.c.CreatedDate,
                    nvivoItem.c.ModifiedBy,
                    nvivoItem.c.ModifiedDate]
                ).where(
                    nvivoItem.c.TypeId == literal_column('51')
                ))]
            for sourcecat in sourcecats:
                if args.windows:
                    sourcecat['Name']        = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourcecat['Name']))
                    sourcecat['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourcecat['Description']))

                if not isinstance(sourcecat['CreatedDate'], datetime.datetime):
                    sourcecat['CreatedDate'] = dateparser.parse(sourcecat['CreatedDate'])
                if not isinstance(sourcecat['ModifiedDate'], datetime.datetime):
                    sourcecat['ModifiedDate'] = dateparser.parse(sourcecat['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normSourceCategory, ['Id'], sourcecats, args.source_categories, args.verbosity)

# Sources
        if args.sources != 'skip':
            if args.verbosity > 0:
                print("Normalising sources")

            nvivoCategoryRole = nvivoRole.alias(name='CategoryRole')
            nvivoParentRole   = nvivoRole.alias(name='ParentRole')

            sources = [dict(row) for row in nvivodb.execute(select([
                    nvivoItem.c.Id,
                    nvivoCategoryRole.c.Item2_Id.label('Category'),
                    nvivoItem.c.Name,
                    nvivoItem.c.Description,
                    nvivoItem.c.ColorArgb.label('Color'),
                    nvivoSource.c.TypeId.label('ObjectTypeId'),
                    nvivoSource.c.Object,
                    nvivoSource.c.PlainText,
                    nvivoSource.c.MetaData,
                    nvivoSource.c.Thumbnail,
                    nvivoSource.c.Waveform,
                    nvivoItem.c.TypeId.label('SourceType'),
                    nvivoItem.c.CreatedBy,
                    nvivoItem.c.CreatedDate,
                    nvivoItem.c.ModifiedBy,
                    nvivoItem.c.ModifiedDate]
                ).where(
                    nvivoItem.c.Id == nvivoSource.c.Item_Id
                ).select_from(nvivoItem.outerjoin(
                    nvivoCategoryRole,
                and_(
                    nvivoCategoryRole.c.TypeId == literal_column('14'),
                    nvivoCategoryRole.c.Item1_Id == nvivoItem.c.Id)
                )))]
            for source in sources:
                if args.windows:
                    source['Name']        = u''.join(map(lambda ch: chr(ord(ch) - 0x377), source['Name']))
                    source['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), source['Description']))

                if source['PlainText'] != None:
                    #source['Content'] = source['PlainText'].replace ('\\n', os.linesep * int(2 / len(os.linesep)))
                    source['Content'] = source['PlainText']
                else:
                    source['Content'] = None

                source['ObjectType'] = ObjectTypeName.get(source['ObjectTypeId'], str(source['ObjectTypeId']))

                if source['ObjectType'] == 'DOC':
                    # Look for ODT signature from NVivo for Mac files
                    if source['Object'][0:4] != 'PK\x03\x04':
                        source['ObjectType'] = 'ODT'
                    else:
                        try:
                            ## Try zlib decompression without header
                            source['Object'] = zlib.decompress(source['Object'], -15)
                        except Exception:
                            pass

                if not isinstance(source['CreatedDate'], datetime.datetime):
                    source['CreatedDate'] = dateparser.parse(source['CreatedDate'])
                if not isinstance(source['ModifiedDate'], datetime.datetime):
                    source['ModifiedDate'] = dateparser.parse(source['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normSource, ['Id'], sources, args.sources, args.verbosity)

# Source attributes
        if args.source_attributes != 'skip':
            if args.verbosity > 0:
                print("Normalising source attributes")

            nvivoNameItem     = nvivoItem.alias(name='NameItem')
            nvivoNameRole     = nvivoRole.alias(name='NameRole')
            nvivoValueItem    = nvivoItem.alias(name='ValueItem')
            nvivoValueRole    = nvivoRole.alias(name='ValueRole')

            sourceattrs  = [dict(row) for row in nvivodb.execute(select([
                    nvivoSource.c.Item_Id.label('Source'),
                    nvivoNameItem.c.Name,
                    nvivoValueItem.c.Name.label('Value'),
                    nvivoValueItem.c.CreatedBy,
                    nvivoValueItem.c.CreatedDate,
                    nvivoValueItem.c.ModifiedBy,
                    nvivoValueItem.c.ModifiedDate,
                    nvivoExtendedItem.c.Properties]
                ).where(and_(
                    nvivoSource.c.Item_Id == nvivoValueRole.c.Item1_Id,
                    nvivoValueRole.c.TypeId == literal_column('7'),
                    nvivoValueItem.c.Id == nvivoValueRole.c.Item2_Id,
                    nvivoNameRole.c.Item2_Id == nvivoValueRole.c.Item2_Id,
                    nvivoNameRole.c.TypeId == literal_column('6'),
                    nvivoNameItem.c.Id == nvivoNameRole.c.Item1_Id,
                    nvivoValueItem.c.Name != literal_column("'Unassigned'"),
                    nvivoExtendedItem.c.Item_Id == nvivoNameItem.c.Id
                )))]
            for sourceattr in sourceattrs:
                properties = parseString(sourceattr['Properties'])
                for property in properties.documentElement.getElementsByTagName('Property'):
                    if property.getAttribute('Key') == 'DataType':
                        sourceattr['Type'] = DataTypeName.get(int(property.getAttribute('Value')), property.getAttribute('Value'))
                    elif property.getAttribute('Key') == 'Length':
                        sourceattr['Length'] = int(property.getAttribute('Value'))

            for sourceattr in sourceattrs:
                if args.windows:
                    sourceattr['Name']  = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourceattr['Name']))
                    sourceattr['Value'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourceattr['Value']))

                if not isinstance(sourceattr['CreatedDate'], datetime.datetime):
                    sourceattr['CreatedDate'] = dateparser.parse(sourceattr['CreatedDate'])
                if not isinstance(sourceattr['ModifiedDate'], datetime.datetime):
                    sourceattr['ModifiedDate'] = dateparser.parse(sourceattr['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normSourceAttribute, ['Source', 'Name'], sourceattrs, args.source_attributes, args.verbosity)

# Tagging
        if args.taggings != 'skip':
            if args.verbosity > 0:
                print("Normalising taggings")

            taggings  = [dict(row) for row in nvivodb.execute(select([
                    nvivoNodeReference.c.Source_Item_Id.label('Source'),
                    nvivoNodeReference.c.Node_Item_Id.label('Node'),
                    nvivoNodeReference.c.StartX,
                    nvivoNodeReference.c.LengthX,
                    nvivoNodeReference.c.StartY,
                    nvivoNodeReference.c.LengthY,
                    nvivoNodeReference.c.StartZ,
                    nvivoNodeReference.c.CreatedBy,
                    nvivoNodeReference.c.CreatedDate,
                    nvivoNodeReference.c.ModifiedBy,
                    nvivoNodeReference.c.ModifiedDate,
                    nvivoItem.c.TypeId]
                ).where(and_(
                    #nvivoNodeReference.c.ReferenceTypeId == literal_column('0'),
                    nvivoItem.c.Id == nvivoNodeReference.c.Node_Item_Id,
                    nvivoItem.c.TypeId == literal_column('16')
                )))]
            for tagging in taggings:
                # JS: Should be able to do this in select statement - figure out how!
                if tagging['StartZ'] != None:
                    next
                tagging['Fragment'] = str(tagging['StartX']) + ':' + str(tagging['StartX'] + tagging['LengthX'] - 1)
                if tagging['StartY'] != None:
                    tagging['Fragment'] += ',' + str(tagging['StartY'])
                    if tagging['LengthY'] > 0:
                        tagging['Fragment'] += ':' + str(tagging['StartY'] + tagging['LengthY'] - 1)

                if not isinstance(tagging['CreatedDate'], datetime.datetime):
                    tagging['CreatedDate'] = dateparser.parse(tagging['CreatedDate'])
                if not isinstance(tagging['ModifiedDate'], datetime.datetime):
                    tagging['ModifiedDate'] = dateparser.parse(tagging['ModifiedDate'])

            # This could be improved - maybe use tagging Id?
            merge_overwrite_or_replace(normcon, normTagging, ['Source', 'Node', 'Fragment'], taggings, args.taggings, args.verbosity)

# Annotations
        if args.annotations != 'skip':
            if args.verbosity > 0:
                print("Normalising annotations")

            annotations  = [dict(row) for row in nvivodb.execute(select([
                    nvivoAnnotation.c.Item_Id.label('Source'),
                    nvivoAnnotation.c.Text.label('Memo'),
                    nvivoAnnotation.c.StartText.label('StartX')   if args.mac else nvivoAnnotation.c.StartX,
                    nvivoAnnotation.c.LengthText.label('LengthX') if args.mac else nvivoAnnotation.c.LengthX,
                    nvivoAnnotation.c.StartY,
                    nvivoAnnotation.c.LengthY,
                    nvivoAnnotation.c.CreatedBy,
                    nvivoAnnotation.c.CreatedDate,
                    nvivoAnnotation.c.ModifiedBy,
                    nvivoAnnotation.c.ModifiedDate
                ]))]

            for annotation in annotations:
                annotation['Node'] = None
                annotation['Fragment'] = str(annotation['StartX']) + ':' + str(annotation['StartX'] + annotation['LengthX'] - 1);
                if annotation['StartY'] != None:
                    annotation['Fragment'] += ',' + str(annotation['StartY'])
                    if annotation['LengthY'] > 0:
                        annotation['Fragment'] += ':' + str(annotation['StartY'] + annotation['LengthY'] - 1)

                if not isinstance(annotation['CreatedDate'], datetime.datetime):
                    annotation['CreatedDate'] = dateparser.parse(annotation['CreatedDate'])
                if not isinstance(annotation['ModifiedDate'], datetime.datetime):
                    annotation['ModifiedDate'] = dateparser.parse(annotation['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normTagging, ['Source', 'Node', 'Fragment'], annotations, args.annotations, args.verbosity)

# All done.
        normtr.commit()
        normtr = None
        normdb.dispose()

        nvivodb.dispose()

    except:
        if not normtr is None:
            normtr.rollback()
        raise

######################################################################################

def Denormalise(args):
    # Initialise DB variables so exception handlers don't freak out
    normdb = None
    nvivodb = None
    nvivotr = None

    try:
        normdb = create_engine(args.indb)
        normmd = MetaData(bind=normdb)
        normmd.reflect(normdb)

        normUser            = normmd.tables.get('User')
        normProject         = normmd.tables.get('Project')
        normSource          = normmd.tables.get('Source')
        normSourceCategory  = normmd.tables.get('SourceCategory')
        normTagging         = normmd.tables.get('Tagging')
        normNode            = normmd.tables.get('Node')
        normNodeCategory    = normmd.tables.get('NodeCategory')
        normSourceAttribute = normmd.tables.get('SourceAttribute')
        normNodeAttribute   = normmd.tables.get('NodeAttribute')

        if args.outdb is None:
            args.outdb = args.indb.rsplit('.',1)[0] + '.nvivo'

        #nvivodb = create_engine(args.outdb, deprecate_large_types=True)
        nvivodb = create_engine(args.outdb)
        nvivomd = MetaData(bind=nvivodb)
        nvivomd.reflect(nvivodb)
        nvivocon = nvivodb.connect()
        nvivotr = nvivocon.begin()
        mssql = nvivodb.dialect.name == 'mssql'

        nvivoUserProfile = Table('UserProfile', nvivomd,
            Column('Id',            UUID(),         primary_key=True),
            extend_existing=True)

        nvivoProject       = nvivomd.tables.get('Project')
        nvivoRole          = nvivomd.tables.get('Role')
        nvivoItem          = nvivomd.tables.get('Item')
        nvivoExtendedItem  = nvivomd.tables.get('ExtendedItem')
        nvivoCategory      = nvivomd.tables.get('Category')
        nvivoSource        = nvivomd.tables.get('Source')
        nvivoNodeReference = nvivomd.tables.get('NodeReference')
        nvivoAnnotation    = nvivomd.tables.get('Annotation')

# Users
        if args.users != 'skip':
            if args.verbosity > 0:
                print("Denormalising users")

            users = [dict(row) for row in normdb.execute(select([
                    normUser.c.Id,
                    normUser.c.Name]
                ))]
            for user in users:
                user['Initials'] = u''.join(word[0].upper() for word in user['Name'].split())

            merge_overwrite_or_replace(nvivocon, nvivoUserProfile, ['Id'], users, args.users, args.verbosity)

# Project
        # First read existing NVivo project record to test that it is there and extract
        # Unassigned and Not Applicable field labels.
        nvivoproject = nvivocon.execute(select([nvivoProject.c.UnassignedLabel,
                                                nvivoProject.c.NotApplicableLabel])).fetchone()
        if nvivoproject is None:
            raise RuntimeError("""
    NVivo file contains no project record. Begin denormalisation with an
    existing project or stock empty project.
    """)
        else:
            unassignedLabel    = nvivoproject['UnassignedLabel']
            notapplicableLabel = nvivoproject['NotApplicableLabel']
            if args.windows:
                unassignedLabel    = u''.join(map(lambda ch: chr(ord(ch) + 0x377), unassignedLabel))
                notapplicableLabel = u''.join(map(lambda ch: chr(ord(ch) + 0x377), notapplicableLabel))

        if args.project != 'skip':
            print("Denormalising project")

            project = dict(normdb.execute(select([
                    normProject.c.Title,
                    normProject.c.Description,
                    normProject.c.CreatedBy,
                    normProject.c.CreatedDate,
                    normProject.c.ModifiedBy,
                    normProject.c.ModifiedDate
                ])).fetchone())
            project['Description'] = project['Description'] or u''
            if args.windows:
                project['Title']       = u''.join(map(lambda ch: chr(ord(ch) + 0x377), project['Title']))
                project['Description'] = u''.join(map(lambda ch: chr(ord(ch) + 0x377), project['Description']))

            if args.project == 'overwrite':
                nvivocon.execute(nvivoProject.update(), project)

        # Function to handle node or source categories

        def skip_merge_or_overwrite_categories(normtable, itemtype, name, operation):
            if operation != 'skip':
                if args.verbosity > 0:
                    print('Denormalising ' + name + ' categories')
                # Look up head category
                headcategoryname = unicode(name + ' Classifications')
                print repr(headcategoryname)
                if args.windows:
                    headcategoryname = u''.join(map(lambda ch: chr(ord(ch) + 0x377), headcategoryname))

                headcategory = nvivocon.execute(select([
                        nvivoItem.c.Id
                    ]).where(and_(
                        nvivoItem.c.TypeId == literal_column('0'),
                        nvivoItem.c.Name   == literal_column("'" + headcategoryname + "'"),
                        nvivoItem.c.System == True
                    ))).fetchone()
                if headcategory is None:
                    raise RuntimeError("""
        NVivo file contains no head """ + name + """ category.
        """)
                else:
                    print "Found head"
                categories = [dict(row) for row in normdb.execute(select([
                        normtable.c.Id,
                        normtable.c.Name,
                        normtable.c.Description,
                        normtable.c.CreatedBy,
                        normtable.c.CreatedDate,
                        normtable.c.ModifiedBy,
                        normtable.c.ModifiedDate
                    ]))]
                for category in categories:
                    category['Id']            = category['Id']          or uuid.uuid4()
                    category['_Id']           = category['Id']      # So we can bind parameter
                    category['Description']   = category['Description'] or u''
                    category['PlainTextName'] = category['Name']
                    if args.windows:
                        category['Name']        = u''.join(map(lambda ch: chr(ord(ch) + 0x377), category['Name']))
                        category['Description'] = u''.join(map(lambda ch: chr(ord(ch) + 0x377), category['Description']))

                newids = [{'_Id':row['Id']} for row in categories]
                curids = [{'_Id':row['Id']} for row in nvivocon.execute(select([
                        nvivoItem.c.Id
                    ]).where(
                        nvivoItem.c.TypeId == literal_column(itemtype)
                    ))]
                if args.verbosity > 1:
                    print("newids " + repr(newids))
                    print("curids " + repr(curids))

                if operation == 'overwrite':
                    rowstoupdate = [row for row in categories if {'_Id':row['Id']} in curids]
                    if len(rowstoupdate) > 0:
                        nvivocon.execute(nvivoItem.update(
                            nvivoItem.c.Id == bindparam('_Id')), rowstoupdate)

                rowstoinsert = [row for row in categories if not {'_Id':row['Id']} in curids]
                if len(rowstoinsert) > 0:
                    nvivocon.execute(nvivoItem.insert().values({
                            'Id':       bindparam('_Id'),
                            'TypeId':   literal_column(itemtype),
                            'System':   literal_column('0'),
                            'ReadOnly': literal_column('0'),
                            'InheritPermissions': literal_column('1')
                        }), rowstoinsert)
                    nvivocon.execute(nvivoRole.insert().values({
                            'Item1_Id': literal_column("'" + str(headcategory['Id']) + "'"),
                            'Item2_Id': bindparam('_Id'),
                            'TypeId':   literal_column('0')
                        }), rowstoinsert)
                    nvivocon.execute(nvivoExtendedItem.insert().values({
                            'Item_Id': bindparam('_Id'),
                            'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="EndNoteReferenceType" Value="-1" /></Properties>\'')
                        }), rowstoinsert)
                    # Insert empty category layout - we'll finish this record later
                    nvivocon.execute(nvivoCategory.insert().values({
                                'Item_Id': bindparam('_Id'),
                                'Layout' : literal_column('\'\'')
                        }), rowstoinsert)


# Node Categories
        skip_merge_or_overwrite_categories(normNodeCategory, '52', 'Node', args.node_categories)

# Nodes
        if args.nodes != 'skip':
            if args.verbosity > 0:
                print("Denormalising nodes")

            # Look up head node
            headnodename = 'Nodes'
            if args.windows:
                headnodename = u''.join(map(lambda ch: chr(ord(ch) + 0x377), headnodename))

            headnode = nvivocon.execute(select([
                    nvivoItem.c.Id
                ]).where(and_(
                    nvivoItem.c.TypeId == literal_column('0'),
                    nvivoItem.c.Name == literal_column("'" + headnodename + "'"),
                    nvivoItem.c.System == True
                ))).fetchone()
            if headnode is None:
                raise RuntimeError("""
    NVivo file contains no head node.
    """)

            nodes = [dict(row) for row in normdb.execute(select([
                    normNode.c.Id,
                    normNode.c.Parent,
                    normNode.c.Category,
                    normNode.c.Name,
                    normNode.c.Description,
                    normNode.c.Color,
                    normNode.c.Aggregate,
                    normNode.c.CreatedBy,
                    normNode.c.CreatedDate,
                    normNode.c.ModifiedBy,
                    normNode.c.ModifiedDate
                ]))]

            tag = 0
            for node in nodes:
                node['Id']            = node['Id']          or uuid.uuid4()
                node['_Id']           = node['Id']      # So we can bind parameter
                node['Description']   = node['Description'] or u''
                node['PlainTextName'] = node['Name']
                if args.windows:
                    node['Name']        = u''.join(map(lambda ch: chr(ord(ch) + 0x377), node['Name']))
                    node['Description'] = u''.join(map(lambda ch: chr(ord(ch) + 0x377), node['Description']))

            def tagchildnodes(TopParent, Parent, AggregateList, depth):
                tag = depth << 16
                for node in nodes:
                    if node['Parent'] == Parent:
                        node['RoleTag'] = tag
                        tag += 1
                        node['AggregateList'] = [node['Id']] + AggregateList
                        if Parent is None:
                            node['TopParent'] = node['Id']
                        else:
                            node['TopParent'] = TopParent

                        if node['Aggregate'] is None:
                            node['Aggregate'] = False

                        if node['Aggregate']:
                            tagchildnodes(node['TopParent'], node['Id'], node['AggregateList'], depth+1)
                        else:
                            tagchildnodes(node['TopParent'], node['Id'], [], depth+1)

            tagchildnodes(None, None, [], 0)
            aggregatepairs = []
            for node in nodes:
                for dest in node['AggregateList']:
                    aggregatepairs += [{ 'Id': node['Id'], 'Ancestor': dest }]

            newids = [{'_Id':row['Id']} for row in nodes]
            curids = [{'_Id':row['Id']} for row in nvivocon.execute(select([
                    nvivoItem.c.Id
                ]).where(
                    nvivoItem.c.TypeId == literal_column('16')
                ))]
            if args.verbosity > 1:
                print("newids " + repr(newids))
                print("curids " + repr(curids))

            nodestoinsert = [node for node in nodes if not {'_Id':node['Id']} in curids]
            if len(nodestoinsert) > 0:
                nvivocon.execute(nvivoItem.insert().values({
                        'TypeId':   literal_column('16'),
                        'ColorArgb': bindparam('Color'),
                        'System':   literal_column('0'),
                        'ReadOnly': literal_column('0'),
                        'InheritPermissions': literal_column('1')
                    }), nodestoinsert)
                nvivocon.execute(nvivoRole.insert().values({
                        'Item1_Id': literal_column("'" + str(headnode['Id']) + "'"),
                        'Item2_Id': bindparam('Id'),
                        'TypeId':   literal_column('0')
                    }), nodestoinsert)
                nvivocon.execute(nvivoRole.insert().values({
                        'Item1_Id': bindparam('TopParent'),
                        'Item2_Id': bindparam('Id'),
                        'TypeId':   literal_column('2'),
                        'Tag':      bindparam('RoleTag')
                    }), nodestoinsert)

                nodeswithparent   = [dict(row) for row in nodestoinsert if row['Parent']   != None]
                nodeswithcategory = [dict(row) for row in nodestoinsert if row['Category'] != None]
                if len(nodeswithcategory) > 0:
                    nvivocon.execute(nvivoRole.insert().values({
                            'Item1_Id': bindparam('Id'),
                            'Item2_Id': bindparam('Category'),
                            'TypeId':   literal_column('14')
                        }), nodeswithcategory)
                if len(nodeswithparent) > 0:
                    nvivocon.execute(nvivoRole.insert().values({
                            'Item1_Id': bindparam('Parent'),
                            'Item2_Id': bindparam('Id'),
                            'TypeId':   literal_column('1')
                        }), nodeswithparent)
                if len(aggregatepairs) > 0:
                    nvivocon.execute(nvivoRole.insert().values({
                            'Item1_Id': bindparam('Ancestor'),
                            'Item2_Id': bindparam('Id'),
                            'TypeId':   literal_column('15')
                        }), aggregatepairs)

        # Function to handle node or source attributes

        def skip_merge_or_overwrite_attributes(selection, items, operation):
            attributes = [dict(row) for row in normdb.execute(selection)]

            # This monster query looks up the node/source category, then does outer joins to find whether
            # the attribute has already been defined, what its value is, and whether the new value
            # has been defined. It also finds the highest tag for both Category Attributes and
            # Values, as this is needed to create a new attribute or value.
            nvivoCategoryRole  = nvivoRole.alias(name='CategoryRole')
            nvivoCategoryAttributeRole = nvivoRole.alias(name='CategoryAttributeRole')
            nvivoCategoryAttributeItem = nvivoItem.alias(name='CategoryAttributeItem')
            nvivoCategoryAttributeExtendedItem = nvivoExtendedItem.alias(name='CategoryAttributeExtendedItem')
            nvivoNewValueRole = nvivoRole.alias(name='NewValueRole')
            nvivoNewValueItem = nvivoItem.alias(name='NewValueItem')
            nvivoExistingValueRole = nvivoRole.alias(name='ExistingValueRole')
            nvivoValueRole = nvivoRole.alias(name='ValueRole')
            nvivoCountAttributeRole = nvivoRole.alias(name='CountAttributeRole')
            nvivoCountValueRole = nvivoRole.alias(name='CountValueRole')

            sel = select([
                    nvivoCategoryRole.c.Item2_Id.label('CategoryId'),
                    nvivoCategoryAttributeItem.c.Id.label('AttributeId'),
                    func.CONVERT(literal_column('VARCHAR(MAX)'),nvivoCategoryAttributeExtendedItem.c.Properties)
                        if mssql
                        else nvivoCategoryAttributeExtendedItem.c.Properties,
                    nvivoNewValueItem.c.Id.label('NewValueId'),
                    nvivoValueRole.c.Item2_Id.label('ExistingValueId'),
                    func.max(nvivoCountAttributeRole.c.Tag).label('MaxAttributeTag'),
                    func.max(nvivoCountValueRole.c.Tag).label('MaxValueTag')
                ]).where(and_(
                    nvivoCategoryRole.c.TypeId   == literal_column('14'),
                    nvivoCategoryRole.c.Item1_Id == bindparam('Item')
                )).group_by(nvivoCategoryRole.c.Item2_Id) \
                .group_by(nvivoCategoryAttributeItem.c.Id) \
                .group_by(
                    func.CONVERT(literal_column('VARCHAR(MAX)'),nvivoCategoryAttributeExtendedItem.c.Properties)
                    if mssql
                    else nvivoCategoryAttributeExtendedItem.c.Properties) \
                .group_by(nvivoNewValueItem.c.Id) \
                .group_by(nvivoValueRole.c.Item2_Id) \
                .select_from(nvivoCategoryRole.outerjoin(
                    nvivoCategoryAttributeRole.join(
                            nvivoCategoryAttributeItem.join(
                                    nvivoCategoryAttributeExtendedItem,
                                    nvivoCategoryAttributeExtendedItem.c.Item_Id == nvivoCategoryAttributeItem.c.Id
                                ), and_(
                            nvivoCategoryAttributeItem.c.Id == nvivoCategoryAttributeRole.c.Item1_Id,
                            nvivoCategoryAttributeItem.c.Name == bindparam('Name')
                    )), and_(
                        nvivoCategoryAttributeRole.c.TypeId == literal_column('13'),
                        nvivoCategoryAttributeRole.c.Item2_Id == nvivoCategoryRole.c.Item2_Id
                )).outerjoin(
                    nvivoNewValueRole.join(
                        nvivoNewValueItem, and_(
                        nvivoNewValueItem.c.Id == nvivoNewValueRole.c.Item2_Id,
                        nvivoNewValueItem.c.Name == bindparam('Value')
                    )), and_(
                        nvivoNewValueRole.c.TypeId == literal_column('6'),
                        nvivoNewValueRole.c.Item1_Id == nvivoCategoryAttributeItem.c.Id
                )).outerjoin(
                    nvivoExistingValueRole.join(
                        nvivoValueRole, and_(
                            nvivoValueRole.c.Item2_Id == nvivoExistingValueRole.c.Item2_Id,
                            nvivoValueRole.c.TypeId == literal_column('7'),
                            nvivoValueRole.c.Item1_Id == bindparam('Item')
                        )), and_(
                            nvivoExistingValueRole.c.TypeId == literal_column('6'),
                            nvivoExistingValueRole.c.Item1_Id == nvivoCategoryAttributeItem.c.Id
                )).outerjoin(
                    nvivoCountAttributeRole, and_(
                        nvivoCountAttributeRole.c.TypeId == literal_column('13'),
                        nvivoCountAttributeRole.c.Item2_Id == nvivoCategoryRole.c.Item2_Id
                )).outerjoin(
                    nvivoCountValueRole, and_(
                        nvivoCountValueRole.c.TypeId == literal_column('6'),
                        nvivoCountValueRole.c.Item1_Id == nvivoCategoryAttributeRole.c.Item1_Id
                ))
                )

            addedattributes = []
            for attribute in attributes:
                item = next(item for item in items if item['Id'] == attribute['Item'])
                attribute['ItemName']          = item['Name']
                attribute['PlainTextItemName'] = item['Name']
                attribute['PlainTextName']     = attribute['Name']
                attribute['PlainTextValue']    = attribute['Value']
                if args.windows:
                    attribute['Name']  = u''.join(map(lambda ch: chr(ord(ch) + 0x377), attribute['Name']))
                    attribute['Value'] = u''.join(map(lambda ch: chr(ord(ch) + 0x377), attribute['Value']))

                newattributes = [dict(row) for row in nvivocon.execute(sel, attribute)]
                if len(newattributes) == 0:    # Item has no category
                    print("WARNING: Item '" + attribute['PlainTextItemName'] + "' has no category. NVivo cannot record attributes'")
                else:
                    attribute.update(newattributes[0])
                    if attribute['Type'] is None:
                        if attribute['Properties'] != None:
                            properties = parseString(attribute['Properties'])
                            for property in properties.documentElement.getElementsByTagName('Property'):
                                if property.getAttribute('Key') == 'DataType':
                                    attribute['Type'] = DataTypeName.get(int(property.getAttribute('Value')), property.getAttribute('Value'))
                                elif property.getAttribute('Key') == 'Length':
                                    attribute['Length'] = int(property.getAttribute('Value'))
                        else:
                            attribute['Type'] = 'Text'

                    attributecurrentlyexists = (attribute['AttributeId'] != None)
                    if not attributecurrentlyexists:
                        if args.verbosity > 1:
                            print("Creating attribute '" + attribute['PlainTextName'] + "/" + attribute['PlainTextName'] + "' for item '" + attribute['PlainTextItemName'] + "'")
                        attribute['AttributeId'] = uuid.uuid4()
                        if attribute['MaxAttributeTag'] is None:
                            attribute['NewAttributeTag'] = 0
                        else:
                            attribute['NewAttributeTag'] = attribute['MaxAttributeTag'] + 1
                        nvivocon.execute(nvivoItem.insert().values({
                                'Id':       bindparam('AttributeId'),
                                'Name':     bindparam('Name'),
                                'Description': literal_column("''"),
                                'TypeId':   literal_column('20'),
                                'System':   literal_column('0'),
                                'ReadOnly': literal_column('0'),
                                'InheritPermissions': literal_column('1')
                            }), attribute)
                        nvivocon.execute(nvivoRole.insert().values({
                                'Item1_Id': bindparam('AttributeId'),
                                'Item2_Id': bindparam('CategoryId'),
                                'TypeId':   literal_column('13'),
                                'Tag':      bindparam('NewAttributeTag')
                            }), attribute)
                        if attribute['Type'] in DataTypeName.values():
                            datatype = DataTypeName.keys()[DataTypeName.values().index(attribute['Type'])]
                        else:
                            datatype = 0;
                        nvivocon.execute(nvivoExtendedItem.insert().values({
                                'Item_Id': bindparam('AttributeId'),
                                'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="DataType" Value="' + str(datatype) + '" /><Property Key="Length" Value="0" /><Property Key="EndNoteFieldTypeId" Value="-1" /></Properties>\'')
                        }), attribute)
                        # Create unassigned and not applicable attribute values
                        attribute['ValueId'] = uuid.uuid4()
                        # Save the attribute and 'Unassigned' value so that it can be filled in for all
                        # items of the present category.
                        addedattributes.append({ 'CategoryId':     attribute['CategoryId'],
                                                'AttributeId':    attribute['AttributeId'],
                                                'DefaultValueId': attribute['ValueId'] })
                        attribute['Unassigned'] = unassignedLabel
                        nvivocon.execute(nvivoItem.insert().values({
                                'Id':       bindparam('ValueId'),
                                'Name':     bindparam('Unassigned'),
                                'Description': literal_column("''"),
                                'TypeId':   literal_column('21'),
                                'System':   literal_column('1'),
                                'ReadOnly': literal_column('0'),
                                'InheritPermissions': literal_column('1'),
                                'ColorArgb': literal_column('0')
                            }), attribute)
                        nvivocon.execute(nvivoRole.insert().values({
                                'Item1_Id': bindparam('AttributeId'),
                                'Item2_Id': bindparam('ValueId'),
                                'TypeId':   literal_column('6'),
                                'Tag':      literal_column('0')
                            }), attribute )
                        nvivocon.execute(nvivoExtendedItem.insert().values({
                                'Item_Id': bindparam('ValueId'),
                                'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="IsDefault" Value="True"/></Properties>\'')
                        }), attribute)
                        attribute['ValueId'] = uuid.uuid4()
                        attribute['NotApplicable'] = notapplicableLabel
                        nvivocon.execute(nvivoItem.insert().values({
                                'Id':       bindparam('ValueId'),
                                'Name':     bindparam('NotApplicable'),
                                'Description': literal_column("''"),
                                'TypeId':   literal_column('21'),
                                'System':   literal_column('1'),
                                'ReadOnly': literal_column('0'),
                                'InheritPermissions': literal_column('1'),
                                'ColorArgb': literal_column('0')
                            }), attribute)
                        nvivocon.execute(nvivoRole.insert().values({
                                'Item1_Id': bindparam('AttributeId'),
                                'Item2_Id': bindparam('ValueId'),
                                'TypeId':   literal_column('6'),
                                'Tag':      literal_column('1')
                            }), attribute )
                        nvivocon.execute(nvivoExtendedItem.insert().values({
                                'Item_Id': bindparam('ValueId'),
                                'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="IsDefault" Value="False"/></Properties>\'')
                        }), attribute)
                        attribute['MaxValueTag'] = 1

                    if operation == 'overwrite' or not attributecurrentlyexists:
                        if attribute['NewValueId'] is None:
                            if args.verbosity > 1:
                                print("Creating value '" + attribute['PlainTextValue'] + "' for attribute '" + attribute['PlainTextName'] + "' for item '" + attribute['PlainTextItemName'] + "'")
                            attribute['NewValueId']  = uuid.uuid4()
                            attribute['NewValueTag'] = attribute['MaxValueTag'] + 1
                            nvivocon.execute(nvivoItem.insert().values({
                                    'Id':       bindparam('NewValueId'),
                                    'Name':     bindparam('Value'),
                                    'Description': literal_column("''"),
                                    'TypeId':   literal_column('21'),
                                    'System':   literal_column('0'),
                                    'ReadOnly': literal_column('0'),
                                    'InheritPermissions': literal_column('1')
                                }), attribute )
                            nvivocon.execute(nvivoRole.insert().values({
                                    'Item1_Id': bindparam('AttributeId'),
                                    'Item2_Id': bindparam('NewValueId'),
                                    'TypeId':   literal_column('6'),
                                    'Tag':      bindparam('NewValueTag')
                                }), attribute )
                            nvivocon.execute(nvivoExtendedItem.insert().values({
                                    'Item_Id': bindparam('NewValueId'),
                                    'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="IsDefault" Value="False"/></Properties>\'')
                            }), attribute)

                        if attribute['NewValueId'] != attribute['ExistingValueId']:
                            if attribute['ExistingValueId'] != None:
                                if args.verbosity > 1:
                                    print("Removing existing value of attribute '" + attribute['PlainTextName'] + "' for item '" + attribute['PlainTextItemName'] + "'")
                                nvivocon.execute(nvivoRole.delete().values({
                                        'Item1_Id': bindparam('Item'),
                                        'Item2_Id': bindparam('ExistingValueId'),
                                        'TypeId':   literal_column('7')
                                    }), attribute )
                            if args.verbosity > 1:
                                print("Assigning value '" + attribute['PlainTextValue'] + "' for attribute '" + attribute['PlainTextName'] + "' for item '" + attribute['PlainTextItemName'] + "'")
                            nvivocon.execute(nvivoRole.insert().values({
                                    'Item1_Id': bindparam('Item'),
                                    'Item2_Id': bindparam('NewValueId'),
                                    'TypeId':   literal_column('7')
                                }), attribute )

            # Fill in default ('Undefined') value for all items lacking an attribute value
            sel = select([
                    nvivoCategoryRole.c.Item1_Id.label('_Id'),
                    func.count(nvivoExistingValueRole.c.Item1_Id).label('ValueCount')
                ]).where(and_(
                    nvivoCategoryRole.c.TypeId   == literal_column('14'),
                    nvivoCategoryRole.c.Item2_Id == bindparam('CategoryId')
                )).group_by(nvivoCategoryRole.c.Item1_Id)\
                .select_from(nvivoCategoryRole.outerjoin(
                    nvivoExistingValueRole.join(
                        nvivoValueRole, and_(
                            nvivoValueRole.c.Item2_Id == nvivoExistingValueRole.c.Item2_Id,
                            nvivoValueRole.c.TypeId == literal_column('7')
                        )), and_(
                            nvivoExistingValueRole.c.TypeId == literal_column('6'),
                            nvivoExistingValueRole.c.Item1_Id == bindparam('AttributeId'),
                            nvivoValueRole.c.Item1_Id == nvivoCategoryRole.c.Item1_Id
                )))

            categorysel = select([nvivoCategory.c.Layout]).where(
                                nvivoCategory.c.Item_Id == bindparam('CategoryId'))
            for addedattribute in addedattributes:
                # Set value of undefined attribute to 'Unassigned'
                attributes = [dict(row) for row in nvivocon.execute(sel, addedattribute)]
                for attribute in attributes:
                    if attribute['ValueCount'] == 0:
                        attribute.update(addedattribute)
                        #print(attribute)
                        nvivocon.execute(nvivoRole.insert().values({
                                'Item1_Id': bindparam('_Id'),
                                'Item2_Id': bindparam('DefaultValueId'),
                                'TypeId':   literal_column('7')
                            }), attribute )

# Node attributes
        if args.node_attributes != 'skip':
            if args.verbosity > 0:
                print("Denormalising node attributes")

            selection = select([
                    normNodeAttribute.c.Node.label('Item'),
                    normNodeAttribute.c.Name,
                    normNodeAttribute.c.Type,
                    normNodeAttribute.c.Value,
                    normNodeAttribute.c.CreatedBy,
                    normNodeAttribute.c.CreatedDate,
                    normNodeAttribute.c.ModifiedBy,
                    normNodeAttribute.c.ModifiedDate
                ])

            nodes = [dict(row) for row in nvivocon.execute(select([
                    nvivoItem.c.Id,
                    nvivoItem.c.Name,
                ]).where(
                    nvivoItem.c.TypeId == literal_column('16')
                ))]

            skip_merge_or_overwrite_attributes(selection, nodes, args.node_attributes)

        # Function to handle node or source category records
        def rebuild_category_records(itemtype):
            categories = [dict(row) for row in nvivocon.execute(select([
                    nvivoItem.c.Id.label('CategoryId')
                ]).where(
                    nvivoItem.c.TypeId == literal_column(itemtype)
                ))]

            for category in categories:
                doc = Document()
                layout = doc.createElement('CategoryLayout')
                layout.setAttribute('xmlns', 'http://qsr.com.au/XMLSchema.xsd')

                items = [dict(row) for row in nvivocon.execute(select([
                        nvivoRole.c.Item1_Id.label('Id')
                    ]).where(and_(
                        nvivoRole.c.Item2_Id == bindparam('CategoryId'),
                        nvivoRole.c.TypeId   == literal_column('0')
                    )), category)]
                index = 0
                for item in items:
                    row = layout.appendChild(doc.createElement('Row'))
                    row.setAttribute('Guid',   str(item['Id']).lower())
                    row.setAttribute('Id',     str(index))
                    row.setAttribute('OrderId',str(index))
                    row.setAttribute('Hidden', 'false')
                    row.setAttribute('Size',   '-1')
                    index += 1

                attributes = [dict(row) for row in nvivocon.execute(select([
                        nvivoRole.c.Item1_Id.label('AttributeId')
                    ]).where(and_(
                        nvivoRole.c.Item2_Id == bindparam('CategoryId'),
                        nvivoRole.c.TypeId   == literal_column('13')
                    )), category)]
                index = 0
                for attribute in attributes:
                    column = layout.appendChild(doc.createElement('Column'))
                    column.setAttribute('Guid', str(attribute['AttributeId']).lower())
                    column.setAttribute('Id',     str(index))
                    column.setAttribute('OrderId',str(index))
                    column.setAttribute('Hidden', 'false')
                    column.setAttribute('Size',   '-1')
                    index += 1

                el = layout.appendChild(doc.createElement('SortedColumn'))
                el.setAttribute('Ascending', 'true')
                el.appendChild(doc.createTextNode('-1'))
                layout.appendChild(doc.createElement('RecordHeaderWidth')).appendChild(doc.createTextNode('100'))
                layout.appendChild(doc.createElement('ShowRowIDs')).appendChild(doc.createTextNode('true'))
                layout.appendChild(doc.createElement('ShowColumnIDs')).appendChild(doc.createTextNode('true'))
                layout.appendChild(doc.createElement('Transposed')).appendChild(doc.createTextNode('false'))
                layout.appendChild(doc.createElement('NameSource')).appendChild(doc.createTextNode('1'))
                layout.appendChild(doc.createElement('RowsUserOrdered')).appendChild(doc.createTextNode('false'))
                layout.appendChild(doc.createElement('ColumnsUserOrdered')).appendChild(doc.createTextNode('true'))

                category['Layout'] = layout.toxml()

            nvivocon.execute(nvivoCategory.update(
                    nvivoCategory.c.Item_Id == bindparam('CategoryId')
                ), categories)


        # Node category layouts
        if args.nodes != 'skip' or args.node_categories != 'skip' or args.node_attributes != 'skip':
            rebuild_category_records('52')

# Source categories
        skip_merge_or_overwrite_categories(normSourceCategory, '51', 'Source', args.source_categories)

# Sources
        if args.sources != 'skip':
            if args.verbosity > 0:
                print("Denormalising sources")

            # Look up head source
            sel = select([nvivoItem.c.Id]).where(and_(
                nvivoItem.c.TypeId == literal_column('0'),
                nvivoItem.c.Name == literal_column("'Internals'"),
                nvivoItem.c.System == True))
            headsource = nvivocon.execute(select([
                    nvivoItem.c.Id
                ]).where(and_(
                    nvivoItem.c.TypeId == literal_column('0'),
                    nvivoItem.c.Name == literal_column("'Internals'"),
                    nvivoItem.c.System == True
                ))).fetchone()
            if headsource is None:
                raise RuntimeError("""
    NVivo file contains no head Internal source.
    """)

            sources = [dict(row) for row in normdb.execute(select([
                        normSource.c.Id.label('Item_Id'),
                        normSource.c.Category,
                        normSource.c.Name,
                        normSource.c.Description,
                        normSource.c.Color,
                        normSource.c.Content,
                        normSource.c.ObjectType.label('ObjectTypeName'),
                        normSource.c.SourceType,
                        normSource.c.Object,
                        normSource.c.Thumbnail,
                        normSource.c.CreatedBy,
                        normSource.c.CreatedDate,
                        normSource.c.ModifiedBy,
                        normSource.c.ModifiedDate
                    ]))]
            extendeditems = []

            for source in sources:
                source['Item_Id']       = source['Item_Id']     or uuid.uuid4()
                source['Description']   = source['Description'] or u''
                source['PlainTextName'] = source['Name']
                if args.windows:
                    source['Name']        = u''.join(map(lambda ch: chr(ord(ch) + 0x377), source['Name']))
                    source['Description'] = u''.join(map(lambda ch: chr(ord(ch) + 0x377), source['Description']))

                source['PlainText'] = source['Content']
                source['MetaData']  = None

                # Do our best to imitate NVivo's treatment of sources. In particular, generating
                # the PlainText column is very tricky. If it was already filled in the Object column
                # of the normalised file then use that value instead.
                if source['ObjectTypeName'] == 'PDF':
                    source['SourceType'] = 34
                    source['LengthX'] = 0

                    doc = Document()
                    pages = doc.createElement("PdfPages")
                    pages.setAttribute("xmlns", "http://qsr.com.au/XMLSchema.xsd")

                    # Write out PDF object into a temporary file, then read it using pdfminer
                    tmpfilename = tempfile.mktemp()
                    tmpfileptr  = file(tmpfilename, 'wb')
                    tmpfileptr.write(source['Object'])
                    tmpfileptr.close()
                    rsrcmgr = PDFResourceManager()
                    retstr = StringIO()
                    laparams = LAParams()
                    device = TextConverter(rsrcmgr, retstr, codec='utf-8', laparams=laparams)
                    tmpfileptr = file(tmpfilename, 'rb')
                    interpreter = PDFPageInterpreter(rsrcmgr, device)
                    pagenos = set()
                    pdfpages = PDFPage.get_pages(tmpfileptr, password='', check_extractable=True)
                    pageoffset = 0
                    pdfstr = u''
                    for pdfpage in pdfpages:
                        mediabox   = pdfpage.attrs['MediaBox']

                        interpreter.process_page(pdfpage)
                        pageelement = pages.appendChild(doc.createElement("PdfPage"))
                        pageelement.setAttribute("PageLength", str(retstr.tell()))
                        pageelement.setAttribute("PageOffset", str(pageoffset))
                        pageelement.setAttribute("PageWidth",  str(int(mediabox[2] - mediabox[0])))
                        pageelement.setAttribute("PageHeight", str(int(mediabox[3] - mediabox[1])))

                        pagestr = unicode(retstr.getvalue(), 'utf-8')
                        pagestr = re.sub('(?<!\n)\n(?!\n)', ' ', pagestr).replace('\n\n', '\n')
                        retstr.truncate(0)
                        pdfstr += pagestr
                        pageoffset += len(pagestr)

                    tmpfileptr.close()
                    os.remove(tmpfilename)

                    if source['PlainText'] is None:
                        source['PlainText'] = pdfstr

                    paragraphs = doc.createElement("Paragraphs")
                    paragraphs.setAttribute("xmlns", "http://qsr.com.au/XMLSchema.xsd")
                    start = 0
                    while start < len(source['PlainText']):
                        end = source['PlainText'].find('\n', start)
                        if end == -1:
                            end = len(source['PlainText']) - 1
                        para = paragraphs.appendChild(doc.createElement("Para"))
                        para.setAttribute("Pos", str(start))
                        para.setAttribute("Len", str(end - start + 1))
                        para.setAttribute("Style", "")
                        start = end + 1

                    source['MetaData'] = paragraphs.toxml() + pages.toxml()

                    # Would be good to work out how NVivo calculates the PDF checksum
                    extendeditems.append({'Item_Id':    source['Item_Id'],
                                        'Properties': '<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="PDFChecksum" Value="0"/><Property Key="PDFPassword" Value=""/></Properties>'})
                elif source['ObjectTypeName'] in {'DOC', 'ODT', 'TXT'}:
                    if source['SourceType'] is None:
                        source['SourceType'] = 2  # or 3 or 4?

                    tmpfilename = tempfile.mktemp()
                    tmpfile = file(tmpfilename + '.' + source['ObjectTypeName'], 'wb')
                    tmpfile.write(source['Object'])
                    tmpfile.close()

                    # Look for unoconv script or executable. Could this be made simpler?
                    unoconvcmd = None
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
                        raise RuntimeError("""
    Can't find unoconv on path. Please refer to the NVivotools README file.
    """)
                    if source['PlainText'] is None:
                        # Use unoconv to convert to text
                        p = Popen(unoconvcmd + ['--format=text', tmpfilename + '.' + source['ObjectTypeName']], stderr=PIPE)
                        err = p.stderr.read()
                        if err != '':
                            raise RuntimeError(err)

                        # Read text output from unocode, then massage it by first dropping a final line
                        # terminator, then changing to Windows (CRLF) or Mac (LFLF) line terminators
                        source['PlainText'] = codecs.open(tmpfilename + '.txt', 'r', 'utf-8-sig').read()
                        if source['PlainText'].endswith('\n'):
                            source['PlainText'] = source['PlainText'][:-1]
                        source['PlainText'] = source['PlainText'].replace('\n', '\n\n' if args.mac else '\r\n')

                        os.remove(tmpfilename + '.txt')

                    # Convert object to DOC/ODT if isn't already
                    if source['ObjectTypeName'] != ('ODT' if args.mac else 'DOC'):
                        destformat = 'odt' if args.mac else 'doc'
                        p = Popen(unoconvcmd + ['--format=' + destformat, tmpfilename + '.' + source['ObjectTypeName']], stderr=PIPE)
                        err = p.stderr.read()
                        if err != '':
                            err = "unoconv invocation error:\n" + err
                            raise RuntimeError(err)

                        source['Object'] = file(tmpfilename + '.' + destformat, 'rb').read()

                        os.remove(tmpfilename + '.' + destformat)

                    os.remove(tmpfilename + '.' + source['ObjectTypeName'])

                    # Hack so that right object type code is found later
                    source['ObjectTypeName'] = 'DOC'

                    if args.mac:
                        source['LengthX'] = len(source['PlainText'].replace(u' ', u''))
                    else:
                        # Compress doc object without header using compression level 6
                        compressor = zlib.compressobj(6, zlib.DEFLATED, -15)
                        source['Object'] = compressor.compress(source['Object']) + compressor.flush()

                        source['LengthX'] = 0

                        doc = Document()
                        settings = doc.createElement("DisplaySettings")
                        settings.setAttribute("xmlns", "http://qsr.com.au/XMLSchema.xsd")
                        settings.setAttribute("InputPosition", "0")
                        source['MetaData'] = settings.toxml()

                        paragraphs = doc.createElement("Paragraphs")
                        paragraphs.setAttribute("xmlns", "http://qsr.com.au/XMLSchema.xsd")
                        start = 0
                        while start < len(source['PlainText']):
                            end = source['PlainText'].find('\n', start)
                            if end == -1:
                                end = len(source['PlainText']) - 1
                            para = paragraphs.appendChild(doc.createElement("Para"))
                            para.setAttribute("Pos", str(start))
                            para.setAttribute("Len", str(end - start + 1))
                            para.setAttribute("Style", "Text Body")
                            start = end + 1

                        source['MetaData'] = paragraphs.toxml() + settings.toxml()

                # Note that NVivo 10 for Mac doesn't support images
                elif source['ObjectTypeName'] == 'JPEG':
                    source['SourceType'] = 33
                    image = Image.open(StringIO(source['Object']))
                    source['LengthX'], source['LengthY'] = image.size
                    image.thumbnail((200,200))
                    thumbnail = StringIO()
                    image.save(thumbnail, format='BMP')
                    source['Thumbnail'] = thumbnail.getvalue()
                    source['Properties'] = '<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"/>'

                    extendeditems.append({'Item_Id':source['Item_Id'],
                                        'Properties': '<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="PictureRotation" Value="0"/><Property Key="PictureBrightness" Value="0"/><Property Key="PictureContrast" Value="0"/><Property Key="PictureQuality" Value="0"/></Properties>'})
                #elif source['ObjectTypeName'] == 'MP3':
                    #source['LengthX'] = length of recording in milliseconds
                    #source['Waveform'] = waveform of recording, one byte per centisecond
                #elif source['ObjectTypeName'] == 'WMV':
                    #source['LengthX'] = length of recording in milliseconds
                    #source['Waveform'] = waveform of recording, one byte per centisecond
                else:
                    source['LengthX'] = 0

                # Lookup object type from name
                if source['ObjectTypeName'] in ObjectTypeName.values():
                    source['ObjectType'] = ObjectTypeName.keys()[ObjectTypeName.values().index(source['ObjectTypeName'])]
                else:
                    source['ObjectType'] = int(source['ObjectTypeName'])

            newids = [{'Item_Id':row['Item_Id']} for row in sources]
            curids = [{'Item_Id':row['Item_Id']} for row in nvivocon.execute(select([
                    nvivoSource.c.Item_Id
                ]))]
            if args.verbosity > 1:
                print("newids " + repr(newids))
                print("curids " + repr(curids))

            if args.sources == 'overwrite' or args.sources == 'replace':
                sourcestoupdate = [source for source in sources if {'Item_Id':source['Item_Id']} in curids]
                if len(sourcestoupdate) > 0:
                    if args.verbosity > 1:
                        print("    updating " + repr(sourcestoupdate))
                    nvivocon.execute(nvivoItem.update(
                            nvivoItem.c.Id == bindparam('Item_Id')).values({
                            'TypeId':   bindparam('SourceType'),
                            'ColorArgb': bindparam('Color'),
                        }), sourcestoupdate)
                    nvivocon.execute(nvivoSource.update(
                            nvivoSource.c.Item_Id == bindparam('Item_Id')).values({
                            'TypeId':   bindparam('ObjectType'),
                            # This work-around is specific to MSSQL
                            'Object':   func.CONVERT(literal_column('VARBINARY(MAX)'),
                                                    bindparam('Object'))
                                        if mssql
                                        else bindparam('Object'),
                            'Thumbnail': func.CONVERT(literal_column('VARBINARY(MAX)'),
                                                    bindparam('Thumbnail'))
                                        if mssql
                                        else bindparam('Thumbnail'),
                        }), sourcestoupdate)

            sourcestoinsert = [source for source in sources if not {'Item_Id':source['Item_Id']} in curids]
            if len(sourcestoinsert) > 0:
                if args.verbosity > 1:
                    print("    inserting " + repr(sourcestoinsert))
                nvivocon.execute(nvivoItem.insert().values({
                        'Id':       bindparam('Item_Id'),
                        'TypeId':   bindparam('SourceType'),
                        'ColorArgb': bindparam('Color'),
                        'System':   literal_column('0'),
                        'ReadOnly': literal_column('0'),
                        'InheritPermissions': literal_column('1')
                    }), sourcestoinsert)
                nvivocon.execute(nvivoSource.insert().values({
                        'TypeId':   bindparam('ObjectType'),
                        # This work-around is specific to MSSQL
                        'Object':   func.CONVERT(literal_column('VARBINARY(MAX)'),
                                                bindparam('Object'))
                                    if mssql
                                    else bindparam('Object'),
                        'Thumbnail': func.CONVERT(literal_column('VARBINARY(MAX)'),
                                                bindparam('Thumbnail'))
                                    if mssql
                                    else bindparam('Thumbnail'),
                    }), sourcestoinsert)
                nvivocon.execute(nvivoRole.insert().values({
                        'Item1_Id': literal_column("'" + str(headsource['Id']) + "'"),
                        'Item2_Id': bindparam('Item_Id'),
                        'TypeId':   literal_column('0')
                    }), sourcestoinsert)

            sourcestoinsertwithcategory = [dict(row) for row in sourcestoinsert if row['Category'] != None]
            if len(sourcestoinsertwithcategory) > 0:
                nvivocon.execute(nvivoRole.insert().values({
                        'Item1_Id': bindparam('Item_Id'),
                        'Item2_Id': bindparam('Category'),
                        'TypeId':   literal_column('14')
                    }), sourcestoinsertwithcategory)

            # Now deal with extended items.
            if len(extendeditems) > 0:
                newids = [{'Item_Id':row['Item_Id']} for row in extendeditems]
                curids = [{'Item_Id':row['Item_Id']} for row in nvivocon.execute(select([
                        nvivoExtendedItem.c.Item_Id
                    ]))]
                if args.verbosity > 1:
                    print("newids " + repr(newids))
                    print("curids " + repr(curids))
                if args.sources == 'overwrite':
                    extendeditemstoupdate = [row for row in extendeditems if {'Item_Id':source['Item_Id']} in curids]
                    if len(extendeditemstoupdate) > 0:
                        nvivocon.execute(nvivoExtendedItem.update(
                                nvivoExtendedItem.c.Item_Id == bindparam('Item_Id')
                            ), extendeditemstoinsert)

                extendeditemstoinsert = [row for row in extendeditems if not {'Item_Id':source['Item_Id']} in curids]
                if len(extendeditemstoinsert) > 0:
                    nvivocon.execute(nvivoExtendedItem.insert(), extendeditemstoinsert)

# Source attributes
        if args.source_attributes != 'skip':
            if args.verbosity > 0:
                print("Denormalising source attributes")

            selection = select([
                    normSourceAttribute.c.Source.label('Item'),
                    normSourceAttribute.c.Name,
                    normSourceAttribute.c.Type,
                    normSourceAttribute.c.Value,
                    normSourceAttribute.c.CreatedBy,
                    normSourceAttribute.c.CreatedDate,
                    normSourceAttribute.c.ModifiedBy,
                    normSourceAttribute.c.ModifiedDate
                ])
            sources = [dict(row) for row in nvivocon.execute(select([
                    nvivoSource.c.Item_Id.label('Id'),
                    nvivoItem.  c.Name
                ]).where(
                    nvivoItem.  c.Id == nvivoSource.c.Item_Id
                ))]

            skip_merge_or_overwrite_attributes(selection, sources, args.node_attributes)

        # Source category layouts
        if args.sources != 'skip' or args.source_categories != 'skip' or args.source_attributes != 'skip':
            rebuild_category_records('51')

# Taggings and annotations
        if args.taggings != 'skip':
            if args.verbosity > 0:
                print("Denormalising taggings")

            sources = [dict(row) for row in nvivocon.execute(select([
                    nvivoSource.c.Item_Id,
                    nvivoSource.c.PlainText
                ]))]

            taggings = [dict(row) for row in normdb.execute(select([
                    normTagging.c.Source,
                    normSource.c.ObjectType,
                    normSource.c.Name.label('SourceName'),
                    normTagging.c.Node,
                    normTagging.c.Memo,
                    normTagging.c.Fragment,
                    normTagging.c.CreatedBy,
                    normTagging.c.CreatedDate,
                    normTagging.c.ModifiedBy,
                    normTagging.c.ModifiedDate
                ]).where(
                    normSource.c.Id == normTagging.c.Source
                ))]

            nvivotaggings    = []
            nvivoannotations = []
            for tagging in taggings[:]:
                matchfragment = re.match("([0-9]+):([0-9]+)(?:,([0-9]+)(?::([0-9]+))?)?", tagging['Fragment'])
                if matchfragment is None:
                    print("WARNING: Unrecognised tagging fragment: " + tagging['Fragment'] + " for Source: " + tagging['SourceName'] )
                    taggings.remove(tagging)
                    continue

                tagging['StartX']  = int(matchfragment.group(1))
                tagging['LengthX'] = int(matchfragment.group(2)) - int(matchfragment.group(1)) + 1
                tagging['StartY']  = None
                tagging['LengthY'] = None
                startY = matchfragment.group(3)
                if startY != None:
                    tagging['StartY'] = int(startY)
                    endY = matchfragment.group(4)
                    if endY != None:
                        tagging['LengthY'] = int(endY) - tagging['StartY'] + 1

                if args.mac:
                    source = next(source for source in sources if source['Item_Id'] == tagging['Source'])
                    if source['PlainText'] != None:
                        tagging['StartText']  = tagging['StartX']  - source['PlainText'][0:tagging['StartX']].count(' ')
                        tagging['LengthText'] = tagging['LengthX'] - source['PlainText'][tagging['StartX']:tagging['StartX']+tagging['LengthX']+1].count(' ')
                    else:
                        tagging['StartText']  = tagging['StartX']
                        tagging['LengthText'] = tagging['LengthX']

                if tagging['ObjectType'] == 'JPEG':
                    tagging['ReferenceTypeId'] = 2
                    tagging['ClusterId']       = 0
                else:
                    tagging['ReferenceTypeId'] = 0

                tagging['Id'] = uuid.uuid4()  # Not clear what purpose this field serves

                if tagging['Node']:
                    nvivotaggings += [tagging]
                else:
                    nvivoannotations += [tagging]

            if len(nvivotaggings) > 0:
                nvivocon.execute(nvivoNodeReference.insert().values({
                            'Node_Item_Id':     bindparam('Node'),
                            'Source_Item_Id':   bindparam('Source')
                    }), nvivotaggings)
            if len(nvivoannotations) > 0:
                nvivocon.execute(nvivoAnnotation.insert().values({
                            'Item_Id':          bindparam('Source'),
                            'Text':             bindparam('Memo')
                    }), nvivoannotations)

# All done.
        nvivotr.commit()
        nvivotr = None
        nvivodb.dispose()

        normdb.dispose()

    except:
        if not nvivotr is None:
            nvivotr.rollback()
        raise
