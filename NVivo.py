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
from datetime import date, time, datetime
from dateutil import parser as dateparser
from PIL import Image
import tempfile
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO
from distutils import util

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

# Generic merge/overwrite/replace function
def merge_overwrite_or_replace(conn, table, columns, data, operation, verbosity):
    newids = [{column:row[column] for column in columns} for row in data]
    curids = [{column:row[column] for column in columns}
                for row in conn.execute(select([table.c[column] for column in columns]))]

    if operation == 'replace':
        idstodelete = [id for id in curids if not id in newids]
        if len(idstodelete) > 0:
            delete = table.delete()
            for column in columns:
                for id in idstodelete:
                    id['_' + column] = id[column]
                delete = delete.where(table.c[column] == bindparam('_' + column))
            conn.execute(delete, idstodelete)

    if operation == 'overwrite' or operation == 'replace':
        rowstoupdate = [row for row in data if {column:row[column] for column in columns} in curids]
        if len(rowstoupdate) > 0:
            update = table.update()
            for column in columns:
                for id in rowstoupdate:
                    id['_' + column] = id[column]
                update = update.where(table.c[column] == bindparam('_' + column))
            conn.execute(update, rowstoupdate)

    rowstoinsert = [row for row in data if not {column:row[column] for column in columns} in curids]
    if len(rowstoinsert) > 0:
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

            nvivoAnnotation    = Table('Annotation',    nvivomd, autoload=True)
            nvivoCategory      = Table('Category',      nvivomd, autoload=True)
            nvivoExtendedItem  = Table('ExtendedItem',  nvivomd, autoload=True)
            nvivoItem          = Table('Item',          nvivomd, autoload=True)
            nvivoNodeReference = Table('NodeReference', nvivomd, autoload=True)
            nvivoProject       = Table('Project',       nvivomd, autoload=True)
            nvivoRole          = Table('Role',          nvivomd, autoload=True)
            nvivoSource        = Table('Source',        nvivomd, autoload=True)
            nvivoUserProfile   = Table('UserProfile',   nvivomd, autoload=True)
        else:
            nvivodb = None

        if args.outdb is None:
            args.outdb = args.indb.rsplit('.',1)[0] + '.norm'
        normdb = create_engine(args.outdb)
        normmd = MetaData(bind=normdb)

# Create the normalised database structure
        try:
            normUser = Table('User', normmd, autoload=True)
        except exc.NoSuchTableError:
            normUser = Table('User', normmd,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          String(256)))
            normUser.create(normdb)

        try:
            normProject = Table('Project', normmd, autoload=True)
        except exc.NoSuchTableError:
            normProject = Table('Project', normmd,
                Column('Version',       String(16)),
                Column('Title',         String(256),                            nullable=False),
                Column('Description',   String(2048)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id"),  nullable=False),
                Column('CreatedDate',   DateTime,                               nullable=False),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id"),  nullable=False),
                Column('ModifiedDate',  DateTime,                               nullable=False))
            normProject.create(normdb)

        try:
            normNodeCategory = Table('NodeCategory', normmd, autoload=True)
        except exc.NoSuchTableError:
            normNodeCategory = Table('NodeCategory', normmd,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          String(256)),
                Column('Description',   String(512)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            normNodeCategory.create(normdb)

        try:
            normNode = Table('Node', normmd, autoload=True)
        except exc.NoSuchTableError:
            normNode = Table('Node', normmd,
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
            normNode.create(normdb)

        try:
            normNodeAttribute = Table('NodeAttribute', normmd, autoload=True)
        except exc.NoSuchTableError:
            normNodeAttribute = Table('NodeAttribute', normmd,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          String(256)),
                Column('Description',   String(512)),
                Column('Type',          String(16)),
                Column('Length',        Integer),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            normNodeAttribute.create(normdb)

        try:
            normNodeValue = Table('NodeValue', normmd, autoload=True)
        except exc.NoSuchTableError:
            normNodeValue = Table('NodeValue', normmd,
                Column('Node',          UUID(),         ForeignKey("Node.Id"),      primary_key=True),
                Column('Attribute',     UUID(),         ForeignKey("NodeAttribute.Id"),
                                                                                    primary_key=True),
                Column('Value',         String(256)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            normNodeValue.create(normdb)

        try:
            normSourceCategory = Table('SourceCategory', normmd, autoload=True)
        except exc.NoSuchTableError:
            normSourceCategory = Table('SourceCategory', normmd,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          String(256)),
                Column('Description',   String(512)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            normSourceCategory.create(normdb)

        try:
            normSource = Table('Source', normmd, autoload=True)
        except exc.NoSuchTableError:
            normSource = Table('Source', normmd,
                Column('Id',            UUID(),         primary_key=True),
                Column('Category',      UUID(),         ForeignKey("SourceCategory.Id")),
                Column('Name',          String(256)),
                Column('Description',   String(512)),
                Column('Color',         Integer),
                Column('Content',       String(16384)),
                Column('ObjectType',    String(256)),
                Column('SourceType',    Integer),
                Column('Object',        LargeBinary),
                Column('Thumbnail',     LargeBinary),
            #Column('Waveform',      LargeBinary,    nullable=False),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            normSource.create(normdb)

        try:
            normSourceAttribute = Table('SourceAttribute', normmd, autoload=True)
        except exc.NoSuchTableError:
            normSourceAttribute = Table('SourceAttribute', normmd,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          String(256)),
                Column('Description',   String(512)),
                Column('Type',          String(16)),
                Column('Length',        Integer),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            normSourceAttribute.create(normdb)

        try:
            normSourceValue = Table('SourceValue', normmd, autoload=True)
        except exc.NoSuchTableError:
            normSourceValue = Table('SourceValue', normmd,
                Column('Source',        UUID(),         ForeignKey("Source.Id"),    primary_key=True),
                Column('Attribute',     UUID(),         ForeignKey("SourceAttribute.Id"),
                                                                                    primary_key=True),
                Column('Value',         String(256)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            normSourceValue.create(normdb)

        try:
            normTagging = Table('Tagging', normmd, autoload=True)
        except exc.NoSuchTableError:
            normTagging = Table('Tagging', normmd,
                Column('Id',            UUID(),         primary_key=True),
                Column('Source',        UUID(),         ForeignKey("Source.Id")),
                Column('Node',          UUID(),         ForeignKey("Node.Id")),
                Column('Fragment',      String(256)),
                Column('Memo',          String(256)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            normTagging.create(normdb)

        if nvivodb is None:     # that is, if all we are doing is making an empty norm file
            normdb.dispose()
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
                    nvivoProject.c.UnassignedLabel,
                    nvivoProject.c.NotApplicableLabel,
                    nvivoProject.c.CreatedBy,
                    nvivoProject.c.CreatedDate,
                    nvivoProject.c.ModifiedBy,
                    nvivoProject.c.ModifiedDate
                ])).first())

            unassignedlabel    = project['UnassignedLabel']
            notapplicablelabel = project['NotApplicableLabel']
            if args.windows:
                project['Title']       = u''.join(map(lambda ch: chr(ord(ch) - 0x377), project['Title']))
                project['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), project['Description']))
                unassignedlabel    = u''.join(map(lambda ch: chr(ord(ch) + 0x377), unassignedlabel))
                notapplicablelabel = u''.join(map(lambda ch: chr(ord(ch) + 0x377), notapplicablelabel))

            # SQLAlchemy should probably handle this...
            if not isinstance(project['CreatedDate'], datetime):
                project['CreatedDate'] = dateparser.parse(project['CreatedDate'])
            if not isinstance(project['ModifiedDate'], datetime):
                project['ModifiedDate'] = dateparser.parse(project['ModifiedDate'])

            normcon.execute(normProject.delete())
            normcon.execute(normProject.insert().values({
                    'Version': '0.2'
                }), project)

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

                if not isinstance(nodecategory['CreatedDate'], datetime):
                    nodecategory['CreatedDate'] = dateparser.parse(nodecategory['CreatedDate'])
                if not isinstance(nodecategory['ModifiedDate'], datetime):
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

                if not isinstance(node['CreatedDate'], datetime):
                    node['CreatedDate'] = dateparser.parse(node['CreatedDate'])
                if not isinstance(node['ModifiedDate'], datetime):
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

            nodeattrvalues = [dict(row) for row in nvivodb.execute(select([
                    nvivoNodeItem.c.Id.label('Node'),
                    nvivoNameItem.c.Id.label('Attribute'),
                    nvivoNameItem.c.Name,
                    nvivoNameItem.c.Description,
                    nvivoNameItem.c.CreatedBy.label('AttrCreatedBy'),
                    nvivoNameItem.c.CreatedDate.label('AttrCreatedDate'),
                    nvivoNameItem.c.ModifiedBy.label('AttrModifiedBy'),
                    nvivoNameItem.c.ModifiedDate.label('AttrModifiedDate'),
                    nvivoValueItem.c.Name.label('Value'),
                    nvivoValueItem.c.CreatedBy,
                    nvivoValueItem.c.CreatedDate,
                    nvivoValueItem.c.ModifiedBy,
                    nvivoValueItem.c.ModifiedDate,
                    nvivoExtendedItem.c.Properties]
                ).where(and_(
                    nvivoNodeItem.c.TypeId==literal_column('16'),
                    nvivoNodeItem.c.Id == nvivoValueRole.c.Item1_Id,
                    nvivoValueRole.c.TypeId == literal_column('7'),
                    nvivoValueItem.c.Id == nvivoValueRole.c.Item2_Id,
                    nvivoNameRole.c.Item2_Id == nvivoValueRole.c.Item2_Id,
                    nvivoNameRole.c.TypeId == literal_column('6'),
                    nvivoNameItem.c.Id == nvivoNameRole.c.Item1_Id,
                    nvivoValueItem.c.Name != bindparam('UnassignedLabel'),
                    nvivoExtendedItem.c.Item_Id == nvivoNameItem.c.Id
                )).order_by(
                    nvivoNameItem.c.Id
                ),
                    {'UnassignedLabel':unassignedlabel}
                )]
            lastattribute = None
            nodeattrs = []
            for nodeattrvalue in nodeattrvalues:
                nodeattrvalue['PlainTextName'] = nodeattrvalue['Name']
                if args.windows:
                    nodeattrvalue['Name']  = u''.join(map(lambda ch: chr(ord(ch) - 0x377), nodeattrvalue['Name']))
                    nodeattrvalue['Value'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), nodeattrvalue['Value']))
                if not isinstance(nodeattrvalue['AttrCreatedDate'], datetime):
                    nodeattrvalue['AttrCreatedDate'] = dateparser.parse(nodeattrvalue['AttrCreatedDate'])
                if not isinstance(nodeattrvalue['AttrModifiedDate'], datetime):
                    nodeattrvalue['AttrModifiedDate'] = dateparser.parse(nodeattrvalue['AttrModifiedDate'])
                if not isinstance(nodeattrvalue['CreatedDate'], datetime):
                    nodeattrvalue['CreatedDate'] = dateparser.parse(nodeattrvalue['CreatedDate'])
                if not isinstance(nodeattrvalue['ModifiedDate'], datetime):
                    nodeattrvalue['ModifiedDate'] = dateparser.parse(nodeattrvalue['ModifiedDate'])

                if nodeattrvalue['Attribute'] != lastattribute:
                    lastattribute = nodeattrvalue['Attribute']
                    attrtype = None
                    attrlength = None
                    for property in parseString(nodeattrvalue['Properties']).documentElement.getElementsByTagName('Property'):
                        if property.getAttribute('Key') == 'DataType':
                            attrtype = DataTypeName.get(int(property.getAttribute('Value')), property.getAttribute('Value'))
                        elif property.getAttribute('Key') == 'Length':
                            attrlength = int(property.getAttribute('Value'))
                            if attrlength == 0:
                                attrlength = None

                    # Check for existing attribute with same name
                    existingattributes = [attr for attr in nodeattrs if attr['Id'] == nodeattrvalue['Attribute']]
                    if len(existingattributes) > 0:
                        existingattribute = existingattributes[0]
                        if existingattribute['Type'] != attrtype or existingattribute['Length'] != attrlength:
                            raise RuntimeError("ERROR: Attribute " + nodeattrvalue['PlainTextName'] + " is multiply defined with different type or length.")
                    else:
                        nodeattrs += [{
                            'Id':            nodeattrvalue['Attribute'],
                            'Name':          nodeattrvalue['Name'],
                            'PlainTextName': nodeattrvalue['PlainTextName'],
                            'Description':   nodeattrvalue['Description'],
                            'Type':          attrtype,
                            'Length':        attrlength,
                            'CreatedBy':     nodeattrvalue['AttrCreatedBy'],
                            'CreatedDate':   nodeattrvalue['AttrCreatedDate'],
                            'ModifiedBy':    nodeattrvalue['AttrModifiedBy'],
                            'ModifiedDate':  nodeattrvalue['AttrModifiedDate']
                        }]

            merge_overwrite_or_replace(normcon, normNodeAttribute, ['Id'], nodeattrs, args.node_attributes, args.verbosity)
            merge_overwrite_or_replace(normcon, normNodeValue, ['Node', 'Attribute'], nodeattrvalues, args.node_attributes, args.verbosity)

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

                if not isinstance(sourcecat['CreatedDate'], datetime):
                    sourcecat['CreatedDate'] = dateparser.parse(sourcecat['CreatedDate'])
                if not isinstance(sourcecat['ModifiedDate'], datetime):
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

                if source['PlainText'] is not None:
                    source['Content'] = source['PlainText']
                else:
                    source['Content'] = None

                source['ObjectType'] = ObjectTypeName.get(source['ObjectTypeId'], str(source['ObjectTypeId']))

                if source['ObjectType'] == 'DOC':
                    # Look for ODT signature from NVivo for Mac files
                    if source['Object'][0:4] == 'PK\x03\x04':
                        source['ObjectType'] = 'ODT'
                    else:
                        try:
                            ## Try zlib decompression without header
                            source['Object'] = zlib.decompress(source['Object'], -15)
                        except Exception:
                            pass

                if not isinstance(source['CreatedDate'], datetime):
                    source['CreatedDate'] = dateparser.parse(source['CreatedDate'])
                if not isinstance(source['ModifiedDate'], datetime):
                    source['ModifiedDate'] = dateparser.parse(source['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normSource, ['Id'], sources, args.sources, args.verbosity)

# Source attributes
        if args.source_attributes != 'skip':
            if args.verbosity > 0:
                print("Normalising source attributes")

            nvivoNameItem  = nvivoItem.alias(name='NameItem')
            nvivoNameRole  = nvivoRole.alias(name='NameRole')
            nvivoValueItem = nvivoItem.alias(name='ValueItem')
            nvivoValueRole = nvivoRole.alias(name='ValueRole')

            sourceattrvalues  = [dict(row) for row in nvivodb.execute(select([
                    nvivoSource.c.Item_Id.label('Source'),
                    nvivoNameItem.c.Id.label('Attribute'),
                    nvivoNameItem.c.Name,
                    nvivoNameItem.c.Description,
                    nvivoNameItem.c.CreatedBy.label('AttrCreatedBy'),
                    nvivoNameItem.c.CreatedDate.label('AttrCreatedDate'),
                    nvivoNameItem.c.ModifiedBy.label('AttrModifiedBy'),
                    nvivoNameItem.c.ModifiedDate.label('AttrModifiedDate'),
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
                    nvivoValueItem.c.Name != bindparam('UnassignedLabel'),
                    nvivoExtendedItem.c.Item_Id == nvivoNameItem.c.Id
                )).order_by(
                    nvivoNameItem.c.Id
                ),
                    {'UnassignedLabel':unassignedlabel}
                )]
            lastattribute = None
            sourceattrs = []
            for sourceattrvalue in sourceattrvalues:
                sourceattrvalue['PlainTextName'] = sourceattrvalue['Name']
                if args.windows:
                    sourceattrvalue['Name']  = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourceattrvalue['Name']))
                    sourceattrvalue['Value'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourceattrvalue['Value']))
                if not isinstance(sourceattrvalue['AttrCreatedDate'], datetime):
                    sourceattrvalue['AttrCreatedDate'] = dateparser.parse(sourceattrvalue['AttrCreatedDate'])
                if not isinstance(sourceattrvalue['AttrModifiedDate'], datetime):
                    sourceattrvalue['AttrModifiedDate'] = dateparser.parse(sourceattrvalue['AttrModifiedDate'])
                if not isinstance(sourceattrvalue['CreatedDate'], datetime):
                    sourceattrvalue['CreatedDate'] = dateparser.parse(sourceattrvalue['CreatedDate'])
                if not isinstance(sourceattrvalue['ModifiedDate'], datetime):
                    sourceattrvalue['ModifiedDate'] = dateparser.parse(sourceattrvalue['ModifiedDate'])

                if sourceattrvalue['Attribute'] != lastattribute:
                    lastattribute = sourceattrvalue['Attribute']
                    attrtype = None
                    attrlength = None
                    for property in parseString(sourceattrvalue['Properties']).documentElement.getElementsByTagName('Property'):
                        if property.getAttribute('Key') == 'DataType':
                            attrtype = DataTypeName.get(int(property.getAttribute('Value')), property.getAttribute('Value'))
                        elif property.getAttribute('Key') == 'Length':
                            attrlength = int(property.getAttribute('Value'))
                            if attrlength == 0:
                                attrlength = None

                    # Check for existing attribute with same name
                    existingattributes = [attr for attr in sourceattrs if attr['Id'] == sourceattrvalue['Attribute']]
                    if len(existingattributes) > 0:
                        existingattribute = existingattributes[0]
                        if existingattribute['Type'] != attrtype or existingattribute['Length'] != attrlength:
                            raise RuntimeError("ERROR: Attribute " + sourceattrvalue['PlainTextName'] + " is multiply defined with different type or length.")
                    else:
                        sourceattrs += [{
                            'Id':            sourceattrvalue['Attribute'],
                            'Name':          sourceattrvalue['Name'],
                            'PlainTextName': sourceattrvalue['PlainTextName'],
                            'Description':   sourceattrvalue['Description'],
                            'Type':          attrtype,
                            'Length':        attrlength,
                            'CreatedBy':     sourceattrvalue['AttrCreatedBy'],
                            'CreatedDate':   sourceattrvalue['AttrCreatedDate'],
                            'ModifiedBy':    sourceattrvalue['AttrModifiedBy'],
                            'ModifiedDate':  sourceattrvalue['AttrModifiedDate']
                        }]

            merge_overwrite_or_replace(normcon, normSourceAttribute, ['Id'], sourceattrs, args.source_attributes, args.verbosity)
            merge_overwrite_or_replace(normcon, normSourceValue, ['Source', 'Attribute'], sourceattrvalues, args.source_attributes, args.verbosity)

# Tagging
        if args.taggings != 'skip':
            if args.verbosity > 0:
                print("Normalising taggings")

            taggings  = [dict(row) for row in nvivodb.execute(select([
                    nvivoNodeReference.c.Id,
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
                    nvivoItem.c.TypeId == literal_column('16'),
                    nvivoNodeReference.c.StartZ.is_(None)
                )))]
            for tagging in taggings:
                tagging['Fragment'] = str(tagging['StartX']) + ':' + str(tagging['StartX'] + tagging['LengthX'] - 1)
                if tagging['StartY'] is not None:
                    tagging['Fragment'] += ',' + str(tagging['StartY'])
                    if tagging['LengthY'] > 0:
                        tagging['Fragment'] += ':' + str(tagging['StartY'] + tagging['LengthY'] - 1)

                if not isinstance(tagging['CreatedDate'], datetime):
                    tagging['CreatedDate'] = dateparser.parse(tagging['CreatedDate'])
                if not isinstance(tagging['ModifiedDate'], datetime):
                    tagging['ModifiedDate'] = dateparser.parse(tagging['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normTagging, ['Id'], taggings, args.taggings, args.verbosity)

# Annotations
        if args.annotations != 'skip':
            if args.verbosity > 0:
                print("Normalising annotations")

            annotations  = [dict(row) for row in nvivodb.execute(select([
                    nvivoAnnotation.c.Id,
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
                if annotation['StartY'] is not None:
                    annotation['Fragment'] += ',' + str(annotation['StartY'])
                    if annotation['LengthY'] > 0:
                        annotation['Fragment'] += ':' + str(annotation['StartY'] + annotation['LengthY'] - 1)

                if not isinstance(annotation['CreatedDate'], datetime):
                    annotation['CreatedDate'] = dateparser.parse(annotation['CreatedDate'])
                if not isinstance(annotation['ModifiedDate'], datetime):
                    annotation['ModifiedDate'] = dateparser.parse(annotation['ModifiedDate'])

            merge_overwrite_or_replace(normcon, normTagging, ['Id'], annotations, args.annotations, args.verbosity)

# All done.
        normtr.commit()
        normtr = None
        normcon.close()
        normdb.dispose()

        nvivodb.dispose()

    except:
        raise
        if not normtr is None:
            normtr.rollback()
        normdb.dispose()
        nvivodb.dispose()

######################################################################################

def Denormalise(args):
    # Initialise DB variables so exception handlers don't freak out
    normdb = None
    nvivodb = None
    nvivotr = None

    try:
        normdb = create_engine(args.indb)
        normmd = MetaData(bind=normdb)

        normUser            = Table('User',            normmd, autoload=True)
        normProject         = Table('Project',         normmd, autoload=True)
        normSource          = Table('Source',          normmd, autoload=True)
        normSourceCategory  = Table('SourceCategory',  normmd, autoload=True)
        normTagging         = Table('Tagging',         normmd, autoload=True)
        normNode            = Table('Node',            normmd, autoload=True)
        normNodeCategory    = Table('NodeCategory',    normmd, autoload=True)
        normSourceAttribute = Table('SourceAttribute', normmd, autoload=True)
        normSourceValue     = Table('SourceValue',     normmd, autoload=True)
        normNodeAttribute   = Table('NodeAttribute',   normmd, autoload=True)
        normNodeValue       = Table('NodeValue',       normmd, autoload=True)

        if args.outdb is None:
            args.outdb = args.indb.rsplit('.',1)[0] + '.nvivo'

        nvivodb = create_engine(args.outdb)
        nvivomd = MetaData(bind=nvivodb)

        nvivoAnnotation    = Table('Annotation',    nvivomd, autoload=True)
        nvivoCategory      = Table('Category',      nvivomd, autoload=True)
        nvivoExtendedItem  = Table('ExtendedItem',  nvivomd, autoload=True)
        nvivoItem          = Table('Item',          nvivomd, autoload=True)
        nvivoNodeReference = Table('NodeReference', nvivomd, autoload=True)
        nvivoProject       = Table('Project',       nvivomd, autoload=True)
        nvivoRole          = Table('Role',          nvivomd, autoload=True)
        nvivoSource        = Table('Source',        nvivomd, autoload=True)
        nvivoUserProfile   = Table('UserProfile',   nvivomd, autoload=True)

        nvivocon = nvivodb.connect()
        nvivotr = nvivocon.begin()
        mssql = nvivodb.dialect.name == 'mssql'

# Load project record to extract the default users
        project = dict(normdb.execute(select([
                normProject.c.Version.label('NVivotoolsVersion'),   # Don't overwrite NVivo's version
                normProject.c.Title,
                normProject.c.Description,
                normProject.c.CreatedBy,
                normProject.c.CreatedDate,
                normProject.c.ModifiedBy,
                normProject.c.ModifiedDate
            ])).first())

        if project['NVivotoolsVersion'] != '0.2':
            raise RuntimeError("Incompatible version of normalised file: " + project['NVivotoolsVersion'])

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

            if args.users == 'replace':
                newids = [row['Id'] for row in users]
                curids = [row['Id'] for row in nvivocon.execute(select([nvivoUserProfile.c.Id]))]
                idstodelete = [{'_Id':id} for id in curids if not id in newids]

                # First create the new users
                merge_overwrite_or_replace(nvivocon, nvivoUserProfile, ['Id'], users, 'overwrite', args.verbosity)

                # Then replace every reference to a user to be deleted
                for table in nvivomd.sorted_tables:
                    userCreatedBy  = table.c.get('CreatedBy')
                    userModifiedBy = table.c.get('ModifiedBy')

                    if userCreatedBy is not None:
                        for idtodelete in idstodelete:
                            nvivocon.execute(table.update(
                                    userCreatedBy == bindparam('_Id')
                                ).values({
                                    'CreatedBy':   bindparam('CreatedBy'),
                                    'CreatedDate': bindparam('CreatedDate')
                                }), {
                                    '_Id':         idtodelete['_Id'],
                                    'CreatedBy':   project['CreatedBy'],
                                    'CreatedDate': project['CreatedDate']
                                })
                    if userModifiedBy is not None:
                        for idtodelete in idstodelete:
                            nvivocon.execute(table.update(
                                    userModifiedBy == bindparam('_Id')
                                ).values({
                                    'ModifiedBy':   bindparam('ModifiedBy'),
                                    'ModifiedDate': bindparam('ModifiedDate')
                                }), {
                                    '_Id':          idtodelete['_Id'],
                                    'ModifiedBy':   project['ModifiedBy'],
                                    'ModifiedDate': project['ModifiedDate']
                                })

                    # Finally the users can be deleted
                    if len(idstodelete) > 0:
                        nvivocon.execute(nvivoUserProfile.delete(
                                    nvivoUserProfile.c.Id == bindparam('_Id')
                                ),  idstodelete)
            else:
                merge_overwrite_or_replace(nvivocon, nvivoUserProfile, ['Id'], users, args.users, args.verbosity)

# Project
        # Read unassigned and not applicable labels from existing NVivo project record.
        nvivoproject = nvivocon.execute(select([nvivoProject.c.UnassignedLabel,
                                                nvivoProject.c.NotApplicableLabel])).first()

        unassignedlabel    = nvivoproject['UnassignedLabel']
        notapplicablelabel = nvivoproject['NotApplicableLabel']
        if args.windows:
            unassignedlabel    = u''.join(map(lambda ch: chr(ord(ch) + 0x377), unassignedlabel))
            notapplicablelabel = u''.join(map(lambda ch: chr(ord(ch) + 0x377), notapplicablelabel))

        if args.project != 'skip':
            print("Denormalising project")

            project['Description'] = project['Description'] or u''
            if args.windows:
                project['Title']       = u''.join(map(lambda ch: chr(ord(ch) + 0x377), project['Title']))
                project['Description'] = u''.join(map(lambda ch: chr(ord(ch) + 0x377), project['Description']))

            if args.project == 'overwrite':
                nvivocon.execute(nvivoProject.update(), project)

        # Basic lookup item name function. Use for debug messages.
        itemsel = select([
                nvivoItem.c.Name
            ]).where(
                nvivoItem.c.Id == bindparam('Id')
            )

        def itemname(id):
            res = nvivocon.execute(itemsel, {'Id':id}).first()
            if res is not None:
                res = res['Name']
                if args.windows:
                    res = u''.join(map(lambda ch: chr(ord(ch) - 0x377), res))
            else:
                res = u''
            return res

        # Function to handle node or source categories

        def skip_merge_or_overwrite_categories(normtable, itemtype, name, operation):
            if operation != 'skip':
                if args.verbosity > 0:
                    print('Denormalising ' + name.lower() + ' categories')
                # Look up head category
                headcategoryname = name.title() + u' Classifications'
                if args.windows:
                    headcategoryname = u''.join(map(lambda ch: chr(ord(ch) + 0x377), headcategoryname))

                headcategory = nvivocon.execute(select([
                        nvivoItem.c.Id
                    ]).where(and_(
                        nvivoItem.c.TypeId == literal_column('0'),
                        nvivoItem.c.Name   == bindparam('Name'),
                        nvivoItem.c.System == True
                    )),
                        {'Name':headcategoryname}
                    ).first()
                if headcategory is None:
                    raise RuntimeError("NVivo file contains no head " + name + " category.")
                else:
                    if args.verbosity > 1:
                        print("Found head " + name + " category Id: " + str(headcategory['Id']))
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
        skip_merge_or_overwrite_categories(normNodeCategory, '52', 'case' if args.nvivoversion == '11' else 'node', args.node_categories)

# Nodes
        if args.nodes != 'skip':
            if args.verbosity > 0:
                print("Denormalising nodes")

            # Look up head node
            headnodename = u'Nodes'
            if args.windows:
                headnodename = u''.join(map(lambda ch: chr(ord(ch) + 0x377), headnodename))

            headnode = nvivocon.execute(select([
                    nvivoItem.c.Id
                ]).where(and_(
                    nvivoItem.c.TypeId == literal_column('0'),
                    nvivoItem.c.Name == bindparam('Name'),
                    nvivoItem.c.System == True
                )),
                    {'Name':headnodename}
                ).first()
            if headnode is None:
                raise RuntimeError("NVivo file contains no head node.")
            else:
                if args.verbosity > 1:
                    print("Found head node Id: " + str(headnode['Id']))

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
                node['Color'] = node['Color'] or 0
                if args.mac:
                    node['HierarchicalName'] = node['Name']
                    parent = node['Parent']
                    while parent is not None:
                        parentnode = normdb.execute(select([
                                normNode.c.Name,
                                normNode.c.Parent
                            ]).where(
                                normNode.c.Id == bindparam('Parent')
                            ), {
                                'Parent': parent
                            }).first()
                        node['HierarchicalName'] = parentnode['Name'] + u'\\' + node['HierarchicalName']
                        parent = parentnode['Parent']
                    node['HierarchicalName'] = u'Nodes\\\\' + node['HierarchicalName']

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

            newids = [{'_Id':row['Id']} for row in nodes]
            curids = [{'_Id':row['Id']} for row in nvivocon.execute(select([
                    nvivoItem.c.Id
                ]).where(
                    nvivoItem.c.TypeId == literal_column('16')
                ))]

            nodestoinsert = [node for node in nodes if not {'_Id':node['Id']} in curids]
            if args.verbosity > 1:
                for node in nodestoinsert:
                    print "Inserting node: " + node['PlainTextName']

            tagchildnodes(None, None, [], 0)
            aggregatepairs = []
            for node in nodestoinsert:
                for dest in node['AggregateList']:
                    aggregatepairs += [{ 'Id': node['Id'], 'Ancestor': dest }]

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

                nodeswithparent   = [dict(row) for row in nodestoinsert if row['Parent']   is not None]
                nodeswithcategory = [dict(row) for row in nodestoinsert if row['Category'] is not None]
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

        def skip_merge_or_overwrite_attributes(attributes, values, name, operation):

            nvivoCategoryRole  = nvivoRole.alias(name='CategoryRole')
            nvivoCategoryAttributeRole = nvivoRole.alias(name='CategoryAttributeRole')
            nvivoAttributeItem = nvivoItem.alias(name='AttributeItem')
            nvivoNewValueRole = nvivoRole.alias(name='NewValueRole')
            nvivoNewValueItem = nvivoItem.alias(name='NewValueItem')
            nvivoExistingValueRole = nvivoRole.alias(name='ExistingValueRole')
            nvivoValueRole = nvivoRole.alias(name='ValueRole')
            nvivoCountAttributeRole = nvivoRole.alias(name='CountAttributeRole')
            nvivoCountValueRole = nvivoRole.alias(name='CountValueRole')

            itemsel = select([
                    nvivoCategoryRole.c.Item2_Id.label('Category')
                ]).where(and_(
                    nvivoItem.c.Id == bindparam('Item')
                )).select_from(
                    nvivoItem.outerjoin(
                    nvivoCategoryRole, and_(
                    nvivoCategoryRole.c.TypeId   == literal_column('14'),
                    nvivoCategoryRole.c.Item1_Id == nvivoItem.c.Id
                )))

            # Build dictionary of item categories
            uniqueitems = set(value['Item'] for value in values)
            itemcategory = {}
            for item in uniqueitems:
                itemselresult = nvivocon.execute(itemsel, {'Item':item}).first()
                itemcategory[item] = itemselresult['Category']
            for value in values:
                value['Category'] = itemcategory[value['Item']]
                if value['Category'] is None:
                    print("WARNING: " + name + " '" + itemname(value['Item']) + "' has no category, attribute '" + attribute['PlainTextName'] + "' cannot be stored.")
                    continue

            attrsel = select([
                    func.max(nvivoCountAttributeRole.c.Tag).label('MaxAttributeTag')
                ]).where(and_(
                    nvivoCountAttributeRole.c.TypeId == literal_column('13'),
                    nvivoCountAttributeRole.c.Item2_Id == bindparam('Category')
                ))

            valuesel = select([
                    nvivoNewValueItem.c.Id.label('NewValueId'),
                    nvivoValueRole.c.Item2_Id.label('ExistingValueId'),
                ]).where(and_(
                    nvivoCategoryAttributeRole.c.Item1_Id == bindparam('Attribute'),
                    nvivoCategoryAttributeRole.c.TypeId == literal_column('13'),
                    nvivoCategoryAttributeRole.c.Item2_Id == bindparam('Category')
                )).select_from(
                    nvivoCategoryAttributeRole.outerjoin(
                    nvivoNewValueRole.join(
                        nvivoNewValueItem, and_(
                        nvivoNewValueItem.c.Id == nvivoNewValueRole.c.Item2_Id,
                        nvivoNewValueItem.c.Name == bindparam('Value')
                    )), and_(
                        nvivoNewValueRole.c.TypeId == literal_column('6'),
                        nvivoNewValueRole.c.Item1_Id == nvivoCategoryAttributeRole.c.Item1_Id
                )).outerjoin(
                    nvivoExistingValueRole.join(
                        nvivoValueRole, and_(
                        nvivoValueRole.c.Item2_Id == nvivoExistingValueRole.c.Item2_Id,
                        nvivoValueRole.c.TypeId == literal_column('7'),
                        nvivoValueRole.c.Item1_Id == bindparam('Item')
                    )), and_(
                        nvivoExistingValueRole.c.TypeId == literal_column('6'),
                        nvivoExistingValueRole.c.Item1_Id == nvivoCategoryAttributeRole.c.Item1_Id
                )))

            maxvaluesel = select([
                    func.max(nvivoCountValueRole.c.Tag).label('MaxValueTag')
                ]).where(and_(
                    nvivoCountValueRole.c.TypeId == literal_column('6'),
                    nvivoCountValueRole.c.Item1_Id == bindparam('Attribute')
                ))

            missingvaluesel = select([
                    nvivoCategoryRole.c.Item1_Id.label('Item')
                ]).where(and_(
                    nvivoCategoryRole.c.TypeId   == literal_column('14'),
                    nvivoCategoryRole.c.Item2_Id == bindparam('Category')
                )).group_by(
                    nvivoCategoryRole.c.Item1_Id
                ).having(
                    func.count(nvivoValueRole.c.Item1_Id) == 0
                ).select_from(
                    nvivoCategoryRole.outerjoin(
                    nvivoValueRole.join(
                        nvivoExistingValueRole, and_(
                            nvivoExistingValueRole.c.Item2_Id == nvivoValueRole.c.Item2_Id,
                            nvivoExistingValueRole.c.TypeId == literal_column('7')
                        )), and_(
                        nvivoValueRole.c.TypeId == literal_column('6'),
                        nvivoValueRole.c.Item1_Id == bindparam('Attribute'),
                        nvivoExistingValueRole.c.Item1_Id == nvivoCategoryRole.c.Item1_Id
                )))

            for attribute in attributes:
                attribute['PlainTextName'] = attribute['Name']
                if args.windows:
                    attribute['Name'] = u''.join(map(lambda ch: chr(ord(ch) + 0x377), attribute['Name']))

            maxattributetags = {}
            maxvaluetags = {}
            addedattributes = []
            for value in values:
                attribute = next(attribute for attribute in attributes if attribute['Id'] == value['Attribute'])
                if attribute['Type'] in DataTypeName.values():
                    datatype = DataTypeName.keys()[DataTypeName.values().index(attribute['Type'])]
                else:
                    datatype = 0;

                if attribute['Type'] == 'Boolean':
                    if util.strtobool(value['Value']):
                        value['Value'] = u'1' if args.mac else u'True'
                    else:
                        value['Value'] = u'0' if args.mac else u'False'
                elif attribute['Type'] == 'Datetime':
                    if args.mac:
                        value['Value'] = unicode(date.strftime(dateparser.parse(value['Value']), '%Y-%m-%dT%H:%M:%S'))
                    else:
                        value['Value'] = unicode(date.strftime(dateparser.parse(value['Value']), '%Y-%m-%d %H:%M:%SZ'))
                elif attribute['Type'] == 'Date':
                    if args.mac:
                        value['Value'] = unicode(date.strftime(dateparser.parse(value['Value']), '%Y-%m-%d'))
                    else:
                        value['Value'] = unicode(date.strftime(dateparser.parse(value['Value']), '%Y-%m-%d 00:00:00Z'))
                elif attribute['Type'] == 'Time':
                    value['Value'] = unicode(time.strftime(dateparser.parse(value['Value']).time(), '%H:%M:%S'))

                value['PlainTextValue'] = value['Value']
                if args.windows:
                    value['Value'] = u''.join(map(lambda ch: chr(ord(ch) + 0x377), value['Value']))

                curvalues = [dict(row) for row in nvivocon.execute(valuesel, value)]
                if len(curvalues) > 1:
                    raise RuntimeError("ERROR: Sanity check!")
                elif len(curvalues) == 1:  # Attribute exists
                    valuestatus = dict(curvalues[0])
                else:  # Attribute does not exist
                    valuestatus = {
                        'NewValueId':None,
                        'ExistingValueId':None,
                    }

                    if value['Category'] not in maxattributetags.keys():
                        maxattributetags[value['Category']] = nvivocon.execute(attrsel, {'Category':value['Category']}).first()['MaxAttributeTag'] or -1
                    maxattributetags[value['Category']] += 1

                    # NVivo doesn't like attributes being shared across categories, so test whether
                    # attribute has already been created.
                    curitem = nvivocon.execute(select([
                            nvivoItem.c.Name
                        ]).where(
                            nvivoItem.c.Id == bindparam('Id')
                        ), attribute).first()
                    if curitem is not None:
                        # Need to adjust every instance of this attribute/category combination
                        if args.verbosity > 1:
                            print("Duplicating " + name + " attribute '" + attribute['PlainTextName'] + "' for category '" + itemname(value['Category']) + "' with tag: " + str(maxattributetags[value['Category']]))
                        attribute = attribute.copy()
                        attributes += [attribute]
                        attribute['Id'] = uuid.uuid4()
                        existingattr = value['Attribute']
                        for valueiter in values:
                            if valueiter['Attribute'] == existingattr and valueiter['Category'] == value['Category']:
                                valueiter['Attribute'] = attribute['Id']
                    else:
                        if args.verbosity > 1:
                            print("Creating " + name + " attribute '" + attribute['PlainTextName'] + "' for category '" + itemname(value['Category']) + "' with tag: " + str(maxattributetags[value['Category']]))

                    nvivocon.execute(nvivoItem.insert().values({
                            'Id':       bindparam('Id'),
                            'Name':     bindparam('Name'),
                            'Description': literal_column("''"),
                            'TypeId':   literal_column('20'),
                            'System':   literal_column('0'),
                            'ReadOnly': literal_column('0'),
                            'InheritPermissions': literal_column('1')
                        }), attribute)

                    attribute['Tag'] = maxattributetags[value['Category']]
                    attribute['Category'] = value['Category']
                    maxvaluetags[(value['Category'], attribute['Id'])] = 1

                    nvivocon.execute(nvivoRole.insert().values({
                            'Item1_Id': bindparam('Id'),
                            'Item2_Id': bindparam('Category'),
                            'TypeId':   literal_column('13'),
                            'Tag':      bindparam('Tag')
                        }), attribute)
                    nvivocon.execute(nvivoExtendedItem.insert().values({
                            'Item_Id': bindparam('Id'),
                            'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="DataType" Value="' + str(datatype) + '" /><Property Key="Length" Value="0" /><Property Key="EndNoteFieldTypeId" Value="-1" /></Properties>\'')
                    }), attribute)

                    # Create unassigned and not applicable attribute values
                    attribute['UnassignedValueId'] = uuid.uuid4()
                    attribute['Unassigned'] = unassignedlabel
                    nvivocon.execute(nvivoItem.insert().values({
                            'Id':       bindparam('UnassignedValueId'),
                            'Name':     bindparam('Unassigned'),
                            'Description': literal_column("''"),
                            'TypeId':   literal_column('21'),
                            'System':   literal_column('1'),
                            'ReadOnly': literal_column('0'),
                            'InheritPermissions': literal_column('1'),
                            'ColorArgb': literal_column('0')
                        }), attribute)
                    nvivocon.execute(nvivoRole.insert().values({
                            'Item1_Id': bindparam('Id'),
                            'Item2_Id': bindparam('UnassignedValueId'),
                            'TypeId':   literal_column('6'),
                            'Tag':      literal_column('0')
                        }), attribute )
                    nvivocon.execute(nvivoExtendedItem.insert().values({
                            'Item_Id': bindparam('UnassignedValueId'),
                            'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="IsDefault" Value="True"/></Properties>\'')
                    }), attribute)

                    # Save the attribute and 'Unassigned' value so that it can be filled in later
                    # for all items of the present category.
                    addedattributes.append({'Category':       attribute['Category'],
                                            'Attribute':      attribute['Id'],
                                            'DefaultValueId': attribute['UnassignedValueId'] })

                    attribute['NotApplicableValueId'] = uuid.uuid4()
                    attribute['NotApplicable'] = notapplicablelabel
                    nvivocon.execute(nvivoItem.insert().values({
                            'Id':       bindparam('NotApplicableValueId'),
                            'Name':     bindparam('NotApplicable'),
                            'Description': literal_column("''"),
                            'TypeId':   literal_column('21'),
                            'System':   literal_column('1'),
                            'ReadOnly': literal_column('0'),
                            'InheritPermissions': literal_column('1'),
                            'ColorArgb': literal_column('0')
                        }), attribute)
                    nvivocon.execute(nvivoRole.insert().values({
                            'Item1_Id': bindparam('Id'),
                            'Item2_Id': bindparam('NotApplicableValueId'),
                            'TypeId':   literal_column('6'),
                            'Tag':      literal_column('1')
                        }), attribute )
                    nvivocon.execute(nvivoExtendedItem.insert().values({
                            'Item_Id': bindparam('NotApplicableValueId'),
                            'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="IsDefault" Value="False"/></Properties>\'')
                    }), attribute)

                    # Boolean values also need True and False values to be created
                    if attribute['Type'] == 'Boolean':
                        attribute['FalseValueId'] = uuid.uuid4()
                        attribute['False']        = u'0' if args.mac else u'False'
                        attribute['TrueValueId']  = uuid.uuid4()
                        attribute['True']         = u'1' if args.mac else u'True'
                        if args.windows:
                            attribute['True']  = u''.join(map(lambda ch: chr(ord(ch) + 0x377), attribute['True']))
                            attribute['False'] = u''.join(map(lambda ch: chr(ord(ch) + 0x377), attribute['False']))

                        nvivocon.execute(nvivoItem.insert().values({
                                'Id':       bindparam('FalseValueId'),
                                'Name':     bindparam('False'),
                                'Description': literal_column("''"),
                                'TypeId':   literal_column('21'),
                                'System':   literal_column('1'),
                                'ReadOnly': literal_column('0'),
                                'InheritPermissions': literal_column('1'),
                                'ColorArgb': literal_column('0')
                            }), attribute)
                        nvivocon.execute(nvivoRole.insert().values({
                                'Item1_Id': bindparam('Id'),
                                'Item2_Id': bindparam('FalseValueId'),
                                'TypeId':   literal_column('6'),
                                'Tag':      literal_column('2')
                            }), attribute )
                        nvivocon.execute(nvivoExtendedItem.insert().values({
                                'Item_Id': bindparam('FalseValueId'),
                                'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="IsDefault" Value="False"/></Properties>\'')
                        }), attribute)

                        nvivocon.execute(nvivoItem.insert().values({
                                'Id':       bindparam('TrueValueId'),
                                'Name':     bindparam('True'),
                                'Description': literal_column("''"),
                                'TypeId':   literal_column('21'),
                                'System':   literal_column('1'),
                                'ReadOnly': literal_column('0'),
                                'InheritPermissions': literal_column('1'),
                                'ColorArgb': literal_column('0')
                            }), attribute)
                        nvivocon.execute(nvivoRole.insert().values({
                                'Item1_Id': bindparam('Id'),
                                'Item2_Id': bindparam('TrueValueId'),
                                'TypeId':   literal_column('6'),
                                'Tag':      literal_column('3')
                            }), attribute )
                        nvivocon.execute(nvivoExtendedItem.insert().values({
                                'Item_Id': bindparam('TrueValueId'),
                                'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="IsDefault" Value="False"/></Properties>\'')
                        }), attribute)

                        # Assign boolean value to one of the two possibilities
                        if util.strtobool(value['PlainTextValue']):
                            valuestatus['NewValueId'] = attribute['TrueValueId']
                        else:
                            valuestatus['NewValueId'] = attribute['FalseValueId']

                    # Catch 'Unassigned' and 'Not Applicable' values manually
                    if value['Value'] == unassignedlabel:
                        valuestatus['NewValueId'] = attribute['UnassignedValueId']
                    elif value['Value'] == notapplicablelabel:
                        valuestatus['NewValueId'] = attribute['NotApplicableValueId']

                # Create new value if required
                if operation == 'overwrite' or valuestatus['ExistingValueId'] is None:
                    if valuestatus['NewValueId'] is None:
                        categoryattribute = (value['Category'], value['Attribute'])
                        # First time we have met this attribute?
                        if categoryattribute not in maxvaluetags.keys():
                            maxvalues = [dict(row) for row in nvivocon.execute(maxvaluesel, valuestatus)]
                            if len(maxvalues) != 1:
                                raise RuntimeError("ERROR: Sanity check!")
                            maxvaluetags[categoryattribute] = maxvalues[0]['MaxValueTag']

                        maxvaluetags[categoryattribute] = (maxvaluetags[categoryattribute] or -1) + 1
                        value['Tag'] = maxvaluetags[categoryattribute]
                        if args.verbosity > 1:
                            print("Creating value '" + value['PlainTextValue'] + "' for " + name + " attribute '" + attribute['PlainTextName'] + "' with tag: "+ str(value['Tag']))

                        value['Id']  = uuid.uuid4()
                        nvivocon.execute(nvivoItem.insert().values({
                                'Id':       bindparam('Id'),
                                'Name':     bindparam('Value'),
                                'Description': literal_column("''"),
                                'TypeId':   literal_column('21'),
                                'System':   literal_column('0'),
                                'ReadOnly': literal_column('0'),
                                'InheritPermissions': literal_column('1')
                            }), value )
                        value['Attribute'] = value['Attribute']
                        nvivocon.execute(nvivoRole.insert().values({
                                'Item1_Id': bindparam('Attribute'),
                                'Item2_Id': bindparam('Id'),
                                'TypeId':   literal_column('6'),
                                'Tag':      bindparam('Tag')
                            }), value )
                        nvivocon.execute(nvivoExtendedItem.insert().values({
                                'Item_Id': bindparam('Id'),
                                'Properties': literal_column('\'<Properties xmlns="http://qsr.com.au/XMLSchema.xsd"><Property Key="IsDefault" Value="False"/></Properties>\'')
                        }), value)

                        valuestatus['NewValueId'] = value['Id']

                    # Assign value to attribute
                    if valuestatus['ExistingValueId'] != valuestatus['NewValueId']:
                        value.update(valuestatus)
                        if valuestatus['ExistingValueId'] is not None:
                            if args.verbosity > 1:
                                print("Deassigning existing value '" + itemname(value['ExistingValueId']) + "' from " + name + " attribute '" + attribute['PlainTextName']  + "' of " + name + " '" + itemname(value['Item']) + "'")
                            nvivocon.execute(nvivoRole.delete(and_(
                                    nvivoRole.c.Item1_Id == bindparam('Item'),
                                    nvivoRole.c.Item2_Id == bindparam('ExistingValueId'),
                                    nvivoRole.c.TypeId   ==   literal_column('7')
                                )), value )

                        if args.verbosity > 1:
                            print("Assigning value '" + value['PlainTextValue'] + "' to " + name + " attribute '" + attribute['PlainTextName']  + "' of " + name + " '" + itemname(value['Item']) + "'")
                        nvivocon.execute(nvivoRole.insert().values({
                                'Item1_Id': bindparam('Item'),
                                'Item2_Id': bindparam('NewValueId'),
                                'TypeId':   literal_column('7')
                            }), value )

            # Now fill in default ('Undefined') for new attributes
            for addedattribute in addedattributes:
                # Set value of undefined attribute to 'Unassigned'
                attributes = [dict(row) for row in nvivocon.execute(missingvaluesel, addedattribute)]
                if len(attributes) > 0 and args.verbosity > 1:
                    print("Assigning default value '" + itemname(addedattribute['DefaultValueId']) + "' to attribute '" + itemname(addedattribute['Attribute']) + "' of " + str(len(attributes)) + " " + name + "(s).")
                for attribute in attributes:
                    attribute.update(addedattribute)

                if len(attributes) > 0:
                    addedattribute.update(attribute)
                    nvivocon.execute(nvivoRole.insert().values({
                            'Item1_Id': bindparam('Item'),
                            'Item2_Id': bindparam('DefaultValueId'),
                            'TypeId':   literal_column('7')
                        }), attributes )

# Node attributes
        if args.node_attributes != 'skip':
            if args.verbosity > 0:
                print("Denormalising node attributes")

            attributes = [dict(row) for row in normdb.execute(select([
                    normNodeAttribute.c.Id,
                    normNodeAttribute.c.Name,
                    normNodeAttribute.c.Description,
                    normNodeAttribute.c.Type,
                    normNodeAttribute.c.CreatedBy,
                    normNodeAttribute.c.CreatedDate,
                    normNodeAttribute.c.ModifiedBy,
                    normNodeAttribute.c.ModifiedDate
                ]))]
            values = [dict(row) for row in normdb.execute(select([
                    normNodeValue.c.Node.label('Item'),
                    normNodeValue.c.Attribute,
                    normNodeValue.c.Value,
                    normNodeValue.c.CreatedBy,
                    normNodeValue.c.CreatedDate,
                    normNodeValue.c.ModifiedBy,
                    normNodeValue.c.ModifiedDate
                ]))]

            skip_merge_or_overwrite_attributes(attributes, values, 'node', args.node_attributes)

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
                        nvivoRole.c.Item1_Id.label('Id')
                    ]).where(and_(
                        nvivoRole.c.Item2_Id == bindparam('CategoryId'),
                        nvivoRole.c.TypeId   == literal_column('13')
                    )), category)]
                index = 0
                for attribute in attributes:
                    column = layout.appendChild(doc.createElement('Column'))
                    column.setAttribute('Guid', str(attribute['Id']).lower())
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

            if len(categories) > 0:
                nvivocon.execute(nvivoCategory.update(
                        nvivoCategory.c.Item_Id == bindparam('CategoryId')
                    ), categories)

        # Node category layouts
        if args.nodes != 'skip' or args.node_categories != 'skip' or args.node_attributes != 'skip':
            rebuild_category_records('52')

# Source categories
        skip_merge_or_overwrite_categories(normSourceCategory, '51', 'source', args.source_categories)

# Function to massage source data
        def massagesource(source):
            if args.verbosity > 1:
                print("Source: " + source['Name'])
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
                if source['Object'] is not None:
                    tmpfile = file(tmpfilename + '.' + source['ObjectTypeName'], 'wb')
                    tmpfile.write(source['Object'])
                elif source['ObjectTypeName'] == 'TXT' and source['PlainText'] is not None:
                    tmpfile = codecs.open(tmpfilename + '.' + source['ObjectTypeName'], 'w', 'utf-8-sig')
                    tmpfile.write(source['PlainText'])
                else:
                    raise RuntimeError("Source '" + source['PlainTextName'] + "' is missing")

                tmpfile.close()

                # Look for unoconvcmd just once
                if massagesource.unoconvcmd is None:
                    if args.verbosity > 1:
                        print("Searching for unoconv executable.")
                    # Look first on path for OS installed version, otherwise use our copy
                    for path in os.environ["PATH"].split(os.pathsep):
                        unoconvpath = os.path.join(path, 'unoconv')
                        if os.path.exists(unoconvpath):
                            if os.access(unoconvpath, os.X_OK) and '' in os.environ.get("PATHEXT", "").split(os.pathsep):
                                massagesource.unoconvcmd = [unoconvpath]
                            else:
                                massagesource.unoconvcmd = ['python', unoconvpath]
                            break
                    if massagesource.unoconvcmd is None:
                        unoconvpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'unoconv')
                        if os.path.exists(unoconvpath):
                            if os.access(unoconvpath, os.X_OK) and '' in os.environ.get("PATHEXT", "").split(os.pathsep):
                                massagesource.unoconvcmd = [unoconvpath]
                            else:
                                massagesource.unoconvcmd = ['python', unoconvpath]
                    if massagesource.unoconvcmd is None:
                        raise RuntimeError("Can't find unoconv on path. Please refer to the NVivotools README file.")

                if source['PlainText'] is None:
                    if source['ObjectTypeName'] == 'TXT':
                        source['PlainText'] = codecs.open(tmpfilename + '.TXT', 'r', 'utf-8-sig').read()
                    else:
                        # Use unoconv to convert to text
                        p = Popen(massagesource.unoconvcmd + ['--format=text', tmpfilename + '.' + source['ObjectTypeName']], stderr=PIPE)
                        err = p.stderr.read()
                        if err != '':
                            raise RuntimeError(err)

                        # Read text output from unocode, then massage it by first dropping a final line
                        # terminator, then changing to Windows (CRLF) or Mac (LFLF) line terminators
                        source['PlainText'] = codecs.open(tmpfilename + '.txt', 'r', 'utf-8-sig').read()
                        os.remove(tmpfilename + '.txt')

                    if source['PlainText'].endswith('\n'):
                        source['PlainText'] = source['PlainText'][:-1]
                    source['PlainText'] = source['PlainText'].replace('\n', '\n\n' if args.mac else '\r\n')

                # Convert object to DOC/ODT if isn't already
                source['Object'] = ''
                if source['ObjectTypeName'] != ('ODT' if args.mac else 'DOC'):
                    destformat = 'odt' if args.mac else 'doc'
                    p = Popen(massagesource.unoconvcmd + ['--format=' + destformat, tmpfilename + '.' + source['ObjectTypeName']], stderr=PIPE)
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

        # Unitialise static variable
        massagesource.unoconvcmd = None

# Sources
        if args.sources != 'skip':
            if args.verbosity > 0:
                print("Denormalising sources")

            # Look up head source
            headsourcename = u'Internals'
            if args.windows:
                headsourcename = u''.join(map(lambda ch: chr(ord(ch) + 0x377), headsourcename))
            headsource = nvivocon.execute(select([
                    nvivoItem.c.Id
                ]).where(and_(
                    nvivoItem.c.TypeId == literal_column('0'),
                    nvivoItem.c.Name == bindparam('Name'),
                    nvivoItem.c.System == True
                )),
                    {'Name':headsourcename}
                ).first()
            if headsource is None:
                raise RuntimeError("NVivo file contains no head Internal source.")
            else:
                if args.verbosity > 1:
                    print("Found head source Id: " + str(headsource['Id']))

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

            newids = [{'Item_Id':row['Item_Id']} for row in sources]
            curids = [{'Item_Id':row['Item_Id']} for row in nvivocon.execute(select([
                    nvivoSource.c.Item_Id
                ]))]

            if args.sources == 'overwrite' or args.sources == 'replace':
                sourcestoupdate = [source for source in sources if {'Item_Id':source['Item_Id']} in curids]

                for source in sourcestoupdate:
                    massagesource(source)

                if len(sourcestoupdate) > 0:
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
            for source in sourcestoinsert:
                massagesource(source)

            if len(sourcestoinsert) > 0:
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

            sourcestoinsertwithcategory = [dict(row) for row in sourcestoinsert if row['Category'] is not None]
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

            attributes = [dict(row) for row in normdb.execute(select([
                    normSourceAttribute.c.Id,
                    normSourceAttribute.c.Name,
                    normSourceAttribute.c.Description,
                    normSourceAttribute.c.Type,
                    normSourceAttribute.c.CreatedBy,
                    normSourceAttribute.c.CreatedDate,
                    normSourceAttribute.c.ModifiedBy,
                    normSourceAttribute.c.ModifiedDate
                ]))]
            values = [dict(row) for row in normdb.execute(select([
                    normSourceValue.c.Source.label('Item'),
                    normSourceValue.c.Attribute,
                    normSourceValue.c.Value,
                    normSourceValue.c.CreatedBy,
                    normSourceValue.c.CreatedDate,
                    normSourceValue.c.ModifiedBy,
                    normSourceValue.c.ModifiedDate
                ]))]

            skip_merge_or_overwrite_attributes(attributes, values, 'source', args.source_attributes)

        # Source category layouts
        if args.sources != 'skip' or args.source_categories != 'skip' or args.source_attributes != 'skip':
            rebuild_category_records('51')

# Taggings and annotations
        if args.taggings != 'skip' or args.annotations != 'skip':
            if args.verbosity > 0:
                print("Denormalising taggings and/or annotations")

            sources = [dict(row) for row in nvivocon.execute(select([
                    nvivoSource.c.Item_Id,
                    nvivoSource.c.PlainText
                ]).where(
                    nvivoItem.c.Id == nvivoSource.c.Item_Id
                ))]

            taggings = [dict(row) for row in normdb.execute(select([
                    normTagging.c.Id,
                    normTagging.c.Source,
                    normSource.c.ObjectType,
                    normTagging.c.Node,
                    normTagging.c.Memo.label('Text'),
                    normTagging.c.Fragment,
                    normTagging.c.CreatedBy,
                    normTagging.c.CreatedDate,
                    normTagging.c.ModifiedBy,
                    normTagging.c.ModifiedDate,
                ]).where(
                    normSource.c.Id == normTagging.c.Source
                ))]

            nvivotaggings    = []
            nvivoannotations = []
            for tagging in taggings[:]:
                tagging['ClusterId'] = None
                matchfragment = re.match("([0-9]+):([0-9]+)(?:,([0-9]+)(?::([0-9]+))?)?", tagging['Fragment'])
                if matchfragment is None:
                    print("WARNING: Unrecognised tagging fragment: " + tagging['Fragment'] + " for Source: " + itemname(tagging['Source']) )
                    taggings.remove(tagging)
                    continue

                tagging['StartX']  = int(matchfragment.group(1))
                tagging['LengthX'] = int(matchfragment.group(2)) - int(matchfragment.group(1)) + 1
                tagging['StartY']  = None
                tagging['LengthY'] = None
                tagging['StartZ']  = 0
                startY = matchfragment.group(3)
                if startY is not None:
                    tagging['StartY'] = int(startY)
                    endY = matchfragment.group(4)
                    if endY is not None:
                        tagging['LengthY'] = int(endY) - tagging['StartY'] + 1

                # On Mac need to remove white space (but not non-breaking spaces) from startX
                # andLengthX to calculate StartText and LengthText
                if args.mac:
                    source = next(source for source in sources if source['Item_Id'] == tagging['Source'])
                    if source['PlainText'] is not None:
                        tagging['StartText']  = tagging['StartX'] - sum(c.isspace() and c != u'\xa0'
                                                for c in source['PlainText'][0:tagging['StartX']])
                        tagging['LengthText'] = tagging['LengthX'] - sum(c.isspace() and c != u'\xa0'
                                                for c in source['PlainText'][tagging['StartX']:tagging['StartX']+tagging['LengthX']])
                    else:
                        tagging['StartText']  = tagging['StartX']
                        tagging['LengthText'] = tagging['LengthX']

                if tagging['ObjectType'] == 'JPEG':
                    tagging['ReferenceTypeId'] = 2
                    tagging['ClusterId']       = 0
                else:
                    tagging['ReferenceTypeId'] = 0

                matchedtagging = False
                if tagging['Node']:
                    tagging['Node_Item_Id'] = tagging['Node']
                    tagging['Source_Item_Id'] = tagging['Source']
                    nvivotaggings += [tagging]
                else:
                    tagging['Item_Id'] = tagging['Source']
                    nvivoannotations += [tagging]

            if args.taggings != 'skip':
                merge_overwrite_or_replace(nvivocon, nvivoNodeReference, ['Id'], nvivotaggings, args.taggings, args.verbosity)
            if args.annotations != 'skip':
                merge_overwrite_or_replace(nvivocon, nvivoAnnotation, ['Id'], nvivoannotations, args.annotations, args.verbosity)

# All done.
        nvivotr.commit()
        nvivotr = None
        nvivocon.close()
        nvivodb.dispose()

        normdb.dispose()

    except:
        raise
        if not nvivotr is None:
            nvivotr.rollback()
        nvivodb.dispose()
        normdb.dispose()
