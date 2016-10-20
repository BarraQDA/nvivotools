#!/usr/bin/python
# -*- coding: utf-8 -*-
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
import argparse
import uuid
import zlib
import datetime
from dateutil import parser as dateparser

exec(open(os.path.dirname(os.path.realpath(__file__)) + '/' + 'NVivoTypes.py').read())

try:
    parser = argparse.ArgumentParser(description='Normalise an offloaded NVivo project.')
    parser.add_argument('-w', '--windows', action='store_true',
                        help='Correct NVivo for Windows string coding. Use if names and descriptions appear wierd.')
    parser.add_argument('-m', '--mac',  action='store_true',
                        help='Use NVivo for Mac database format.')
    parser.add_argument('-s', '--structure', action='store_true',
                        help='Replace existing table structures.')

    table_choices = ["", "skip", "replace", "merge"]
    parser.add_argument('-p', '--project', choices=table_choices, default="replace",
                        help='Project action.')
    parser.add_argument('-nc', '--node-categories', choices=table_choices, default="replace",
                        help='Node category action.')
    parser.add_argument('-n', '--nodes', choices=table_choices, default="replace",
                        help='Node action.')
    parser.add_argument('-na', '--node-attributes', choices=table_choices, default="replace",
                        help='Node attribute table action.')
    parser.add_argument('-sc', '--source-categories', choices=table_choices, default="replace",
                        help='Source category action.')
    parser.add_argument('--sources', choices=table_choices, default="replace",
                        help='Source action.')
    parser.add_argument('-sa', '--source-attributes', choices=table_choices, default="replace",
                        help='Source attribute action.')
    parser.add_argument('-t', '--taggings', choices=table_choices, default="replace",
                        help='Tagging action.')
    parser.add_argument('-a', '--annotations', choices=table_choices, default="replace",
                        help='Annotation action.')
    parser.add_argument('-u', '--users', choices=table_choices, default="replace",
                        help='User action.')

    parser.add_argument('infile', type=str,
                        help='SQLAlchemy path of input NVivo database or "-" to create empty project.')
    parser.add_argument('outfile', type=str, nargs='?',
                        help='SQLAlchemy path of output normalised database.')

    args = parser.parse_args()

    if args.infile != '-':
        nvivodb = create_engine(args.infile)
        nvivomd = MetaData(bind=nvivodb)
        nvivomd.reflect(nvivodb)

        nvivoAnnotation    = nvivomd.tables['Annotation']
        nvivoExtendedItem  = nvivomd.tables['ExtendedItem']
        nvivoItem          = nvivomd.tables['Item']
        nvivoNodeReference = nvivomd.tables['NodeReference']
        nvivoProject       = nvivomd.tables['Project']
        nvivoRole          = nvivomd.tables['Role']
        nvivoSource        = nvivomd.tables['Source']
        nvivoUserProfile   = nvivomd.tables['UserProfile']
    else:
        nvivodb = None

    if args.outfile is None:
        args.outfile = args.infile.rsplit('.',1)[0] + '.norm'
    normdb = create_engine(args.outfile)
    normmd = MetaData(bind=normdb)

    if args.structure:
        normmd.drop_all(normdb)
        for table in reversed(normmd.sorted_tables):
            normmd.remove(table)

    # Create the normalised database structure
    normUser = Table('User', normmd,
        Column('Id',            UUID(),         primary_key=True),
        Column('Name',          String(256)))

    normProject = Table('Project', normmd,
        Column('Title',         String(256),                            nullable=False),
        Column('Description',   String(2048)),
        Column('CreatedBy',     UUID(),         ForeignKey("User.Id"),  nullable=False),
        Column('CreatedDate',   DateTime,                               nullable=False),
        Column('ModifiedBy',    UUID(),         ForeignKey("User.Id"),  nullable=False),
        Column('ModifiedDate',  DateTime,                               nullable=False))

    normSource = Table('Source', normmd,
        Column('Id',            UUID(),         primary_key=True),
        Column('Category',      UUID()),
        Column('Name',          String(256)),
        Column('Description',   String(512)),
        Column('Color',         Integer),
        Column('Content',       String(16384)),
        Column('ObjectType',    String(256)),
        Column('SourceType',    Integer),
        Column('Object',        LargeBinary,    nullable=False),
        Column('Thumbnail',     LargeBinary,    nullable=False),
        #Column('Waveform',      LargeBinary,    nullable=False),
        Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
        Column('CreatedDate',   DateTime),
        Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
        Column('ModifiedDate',  DateTime))

    normSourceCategory = Table('SourceCategory', normmd,
        Column('Id',            UUID(),         primary_key=True),
        Column('Name',          String(256)),
        Column('Description',   String(512)),
        Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
        Column('CreatedDate',   DateTime),
        Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
        Column('ModifiedDate',  DateTime))

    normTagging = Table('Tagging', normmd,
        Column('Source',        UUID(),         ForeignKey("Source.Id")),
        Column('Node',          UUID(),         ForeignKey("Node.Id")),
        Column('Fragment',      String(256)),
        Column('Memo',          String(256)),
        Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
        Column('CreatedDate',   DateTime),
        Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
        Column('ModifiedDate',  DateTime))

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

    normNodeCategory = Table('NodeCategory', normmd,
        Column('Id',            UUID(),         primary_key=True),
        Column('Name',          String(256)),
        Column('Description',   String(512)),
        Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
        Column('CreatedDate',   DateTime),
        Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
        Column('ModifiedDate',  DateTime))

    normSourceAttribute = Table('SourceAttribute', normmd,
        Column('Source',        UUID(),         ForeignKey("Source.Id"),    primary_key=True),
        Column('Name',          String(256),                                primary_key=True),
        Column('Value',         String(256)),
        Column('Type',          String(16)),
        Column('Length',        Integer),
        Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
        Column('CreatedDate',   DateTime),
        Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
        Column('ModifiedDate',  DateTime))

    normNodeAttribute = Table('NodeAttribute', normmd,
        Column('Node',          UUID(),         ForeignKey("Node.Id"),      primary_key=True),
        Column('Name',          String(256),                                primary_key=True),
        Column('Value',         String(256)),
        Column('Type',          String(16)),
        Column('Length',        Integer),
        Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
        Column('CreatedDate',   DateTime),
        Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
        Column('ModifiedDate',  DateTime))

    normmd.create_all(normdb)

    if nvivodb == None:
        sys.exit()

# Users
    if args.users != 'skip':
        print("Normalising users")

        sel = select([nvivoUserProfile.c.Id,
                      nvivoUserProfile.c.Name])

        users = [dict(row) for row in nvivodb.execute(sel)]

        if args.users == 'replace':
            normdb.execute(normUser.delete())
        elif args.users == 'merge':
            normdb.execute(normUser.delete(),
                           users)

        if len(users) > 0:
            normdb.execute(normUser.insert(), users)

# Project
    if args.project != 'skip':
        print("Normalising project")

        sel = select([nvivoProject.c.Title,
                      nvivoProject.c.Description,
                      nvivoProject.c.CreatedBy,
                      nvivoProject.c.CreatedDate,
                      nvivoProject.c.ModifiedBy,
                      nvivoProject.c.ModifiedDate])
        projects = [dict(row) for row in nvivodb.execute(sel)]
        for project in projects:
            if args.windows:
                project['Title']       = u''.join(map(lambda ch: chr(ord(ch) - 0x377), project['Title']))
                project['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), project['Description']))

            # SQLAlchemy should probably handle this...
            if not isinstance(project['CreatedDate'], datetime.datetime):
                project['CreatedDate'] = dateparser.parse(project['CreatedDate'])
            if not isinstance(project['ModifiedDate'], datetime.datetime):
                project['ModifiedDate'] = dateparser.parse(project['ModifiedDate'])

        normdb.execute(normProject.delete())
        normdb.execute(normProject.insert(), projects)

# Node Categories
    if args.node_categories != 'skip':
        print("Normalising node categories")

        sel = select([nvivoItem.c.Id,
                      nvivoItem.c.Name,
                      nvivoItem.c.Description,
                      nvivoItem.c.CreatedBy,
                      nvivoItem.c.CreatedDate,
                      nvivoItem.c.ModifiedBy,
                      nvivoItem.c.ModifiedDate])
        sel = sel.where(nvivoItem.c.TypeId == literal_column('52'))
        nodecategories = [dict(row) for row in nvivodb.execute(sel)]
        for nodecategory in nodecategories:
            if args.windows:
                nodecategory['Name']        = u''.join(map(lambda ch: chr(ord(ch) - 0x377), nodecategory['Name']))
                nodecategory['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), nodecategory['Description']))

            if not isinstance(nodecategory['CreatedDate'], datetime.datetime):
                nodecategory['CreatedDate'] = dateparser.parse(nodecategory['CreatedDate'])
            if not isinstance(nodecategory['ModifiedDate'], datetime.datetime):
                nodecategory['ModifiedDate'] = dateparser.parse(nodecategory['ModifiedDate'])

        if args.node_categories == 'replace':
            normdb.execute(normNodeCategory.delete())
        elif args.node_categories == 'merge':
            normdb.execute(normNodeCategory.delete(normNodeCategory.c.Id == bindparam('Id')),
                           nodecategories)

        if len(nodecategories) > 0:
            normdb.execute(normNodeCategory.insert(), nodecategories)

# Nodes
    if args.nodes != 'skip':
        print("Normalising nodes")

        nvivoCategoryRole = nvivoRole.alias(name='CategoryRole')
        nvivoParentRole   = nvivoRole.alias(name='ParentRole')

        sel = select([
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
              ).where(or_(
                    nvivoItem.c.TypeId == literal_column('16'),
                    nvivoItem.c.TypeId == literal_column('62'))
              ).select_from(nvivoItem.outerjoin(
                    nvivoCategoryRole, and_(
                    nvivoCategoryRole.c.TypeId == literal_column('14'),
                    nvivoCategoryRole.c.Item1_Id == nvivoItem.c.Id)
              ).outerjoin(
                    nvivoParentRole, and_(
                    nvivoParentRole.c.TypeId == literal_column('1'),
                    nvivoParentRole.c.Item2_Id == nvivoItem.c.Id)))

        nodes = [dict(row) for row in nvivodb.execute(sel)]
        for node in nodes:
            if args.windows:
                node['Name']        = u''.join(map(lambda ch: chr(ord(ch) - 0x377), node['Name']))
                node['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), node['Description']))

            if not isinstance(node['CreatedDate'], datetime.datetime):
                node['CreatedDate'] = dateparser.parse(node['CreatedDate'])
            if not isinstance(node['ModifiedDate'], datetime.datetime):
                node['ModifiedDate'] = dateparser.parse(node['ModifiedDate'])

        if args.nodes == 'replace':
            normdb.execute(normNode.delete())
        elif args.nodes == 'merge':
            normdb.execute(normNode.delete(normNode.c.Id == bindparam('Id')),
                           nodes)

        if len(nodes) > 0:
            normdb.execute(normNode.insert(), nodes)

# Node attributes
    if args.node_attributes != 'skip':
        print("Normalising node attributes")

        nvivoNodeItem     = nvivoItem.alias(name='NodeItem')
        nvivoNameItem     = nvivoItem.alias(name='NameItem')
        nvivoNameRole     = nvivoRole.alias(name='NameRole')
        nvivoValueItem    = nvivoItem.alias(name='ValueItem')
        nvivoValueRole    = nvivoRole.alias(name='ValueRole')

        sel = select([nvivoNodeItem.c.Id.label('Node'),
                      nvivoNameItem.c.Name.label('Name'),
                      nvivoValueItem.c.Name.label('Value'),
                      nvivoValueItem.c.CreatedBy,
                      nvivoValueItem.c.CreatedDate,
                      nvivoValueItem.c.ModifiedBy,
                      nvivoValueItem.c.ModifiedDate,
                      nvivoNameRole.c.TypeId.label('NameRoleTypeId'),
                      nvivoValueRole.c.TypeId.label('ValueRoleTypeId'),
                      nvivoExtendedItem.c.Properties])
        sel = sel.where(and_(
                      or_(nvivoNodeItem.c.TypeId==literal_column('16'), nvivoNodeItem.c.TypeId==literal_column('62')),
                      nvivoNodeItem.c.Id == nvivoValueRole.c.Item1_Id,
                      nvivoValueRole.c.TypeId == literal_column('7'),
                      nvivoValueItem.c.Id == nvivoValueRole.c.Item2_Id,
                      nvivoNameRole.c.Item2_Id == nvivoValueRole.c.Item2_Id,
                      nvivoNameRole.c.TypeId == literal_column('6'),
                      nvivoNameItem.c.Id == nvivoNameRole.c.Item1_Id,
                      nvivoValueItem.c.Name != literal_column("'Unassigned'"),
                      nvivoExtendedItem.c.Item_Id == nvivoNameItem.c.Id
                      ))
        nodeattrs = [dict(row) for row in nvivodb.execute(sel)]
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

        if args.node_attributes == 'replace':
            normdb.execute(normNodeAttribute.delete())
        elif args.node_attributes == 'merge':
            normdb.execute(normNodeAttribute.delete(
                           and_(normNodeAttribute.c.Node == bindparam('Node'),
                                normNodeAttribute.c.Name == bindparam('Name'))),
                           nodeattrs)

        if len(nodeattrs) > 0:
            normdb.execute(normNodeAttribute.insert(), nodeattrs)

# Source categories
    if args.source_categories != 'skip':
        print("Normalising source categories")

        sel = select([nvivoItem.c.Id,
                      nvivoItem.c.Name,
                      nvivoItem.c.Description,
                      nvivoItem.c.CreatedBy,
                      nvivoItem.c.CreatedDate,
                      nvivoItem.c.ModifiedBy,
                      nvivoItem.c.ModifiedDate])
        sel = sel.where(nvivoItem.c.TypeId == literal_column('51'))
        sourcecats  = [dict(row) for row in nvivodb.execute(sel)]
        for sourcecat in sourcecats:
            if args.windows:
                sourcecat['Name']        = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourcecat['Name']))
                sourcecat['Description'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourcecat['Description']))

            if not isinstance(sourcecat['CreatedDate'], datetime.datetime):
                sourcecat['CreatedDate'] = dateparser.parse(sourcecat['CreatedDate'])
            if not isinstance(sourcecat['ModifiedDate'], datetime.datetime):
                sourcecat['ModifiedDate'] = dateparser.parse(sourcecat['ModifiedDate'])

        if args.source_categories == 'replace':
            normdb.execute(normSourceCategory.delete())
        elif args.source_categories == 'merge':
            normdb.execute(normSourceCategory.delete(normSourceCategory.c.Id == bindparam('Id')),
                           sourcecategories)

        if len(sourcecats) > 0:
            normdb.execute(normSourceCategory.insert(), sourcecats)

# Sources
    if args.sources != 'skip':
        print("Normalising sources")

        nvivoCategoryRole = nvivoRole.alias(name='CategoryRole')
        nvivoParentRole   = nvivoRole.alias(name='ParentRole')

        sel = select([
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
                    nvivoCategoryRole, and_(
                    nvivoCategoryRole.c.TypeId == literal_column('14'),
                    nvivoCategoryRole.c.Item1_Id == nvivoItem.c.Id)
            ))

        sources = [dict(row) for row in nvivodb.execute(sel)]
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
                else
                    try:
                        ## Try zlib decompression without header
                        source['Object'] = zlib.decompress(source['Object'], -15)
                    except Exception:
                        pass

            if not isinstance(source['CreatedDate'], datetime.datetime):
                source['CreatedDate'] = dateparser.parse(source['CreatedDate'])
            if not isinstance(source['ModifiedDate'], datetime.datetime):
                source['ModifiedDate'] = dateparser.parse(source['ModifiedDate'])

        if args.sources == 'replace':
            normdb.execute(normSource.delete())
        elif args.sources == 'merge':
            normdb.execute(normSource.delete(normSource.c.Id == bindparam('Id')),
                           sources)

        if len(sources) > 0:
            normdb.execute(normSource.insert(), sources)

# Source attributes
    if args.source_attributes != 'skip':
        print("Normalising source attributes")

        nvivoNameItem     = nvivoItem.alias(name='NameItem')
        nvivoNameRole     = nvivoRole.alias(name='NameRole')
        nvivoValueItem    = nvivoItem.alias(name='ValueItem')
        nvivoValueRole    = nvivoRole.alias(name='ValueRole')

        sel = select([nvivoSource.c.Item_Id.label('Source'),
                      nvivoNameItem.c.Name,
                      nvivoValueItem.c.Name.label('Value'),
                      nvivoValueItem.c.CreatedBy,
                      nvivoValueItem.c.CreatedDate,
                      nvivoValueItem.c.ModifiedBy,
                      nvivoValueItem.c.ModifiedDate,
                      nvivoExtendedItem.c.Properties])
        sel = sel.where(and_(
                      nvivoSource.c.Item_Id == nvivoValueRole.c.Item1_Id,
                      nvivoValueRole.c.TypeId == literal_column('7'),
                      nvivoValueItem.c.Id == nvivoValueRole.c.Item2_Id,
                      nvivoNameRole.c.Item2_Id == nvivoValueRole.c.Item2_Id,
                      nvivoNameRole.c.TypeId == literal_column('6'),
                      nvivoNameItem.c.Id == nvivoNameRole.c.Item1_Id,
                      nvivoValueItem.c.Name != literal_column("'Unassigned'"),
                      nvivoExtendedItem.c.Item_Id == nvivoNameItem.c.Id
                    ))
        sourceattrs  = [dict(row) for row in nvivodb.execute(sel)]
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

        if args.source_attributes == 'replace':
            normdb.execute(normSourceAttribute.delete())
        elif args.source_attributes == 'merge':
            normdb.execute(normSourceAttribute.delete(
                           and_(normSourceAttribute.c.Source == bindparam('Source'),
                                normSourceAttribute.c.Name == bindparam('Name'))),
                           sourceattrs)

        if len(sourceattrs) > 0:
            normdb.execute(normSourceAttribute.insert(), sourceattrs)

# Tagging
    if args.taggings != 'skip':
        print("Normalising taggings")

        sel = select([nvivoNodeReference.c.Source_Item_Id.label('Source'),
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
                      nvivoItem.c.TypeId])
        sel = sel.where(and_(
                      #nvivoNodeReference.c.ReferenceTypeId == literal_column('0'),
                      nvivoItem.c.Id == nvivoNodeReference.c.Node_Item_Id,
                      or_(
                        nvivoItem.c.TypeId == literal_column('16'),
                        nvivoItem.c.TypeId == literal_column('62'))))

        taggings  = [dict(row) for row in nvivodb.execute(sel)]
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

        if args.taggings == 'replace':
            normdb.execute(normTagging.delete(
                                normTagging.c.Node   != None))
        elif args.taggings == 'merge':
            normdb.execute(normTagging.delete(
                           and_(normSource.c.Source  == bindparam('Source'),
                                normSource.c.Node    == bindparam('Node'),
                                normSource.c.StartX  == bindparam('StartX'),
                                normSource.c.LengthX == bindparam('LengthX'))),
                           sources)

        if len(taggings) > 0:
            normdb.execute(normTagging.insert(), taggings)

# Annotations
    if args.annotations != 'skip':
        print("Normalising annotations")

        # Mac versions label some columns a bit differently here. Go figure...
        sel = select([nvivoAnnotation.c.Item_Id.label('Source'),
                        nvivoAnnotation.c.Text.label('Memo'),
                        nvivoAnnotation.c.StartText.label('StartX')   if args.mac else nvivoAnnotation.c.StartX,
                        nvivoAnnotation.c.LengthText.label('LengthX') if args.mac else nvivoAnnotation.c.LengthX,
                        nvivoAnnotation.c.StartY,
                        nvivoAnnotation.c.LengthY,
                        nvivoAnnotation.c.CreatedBy,
                        nvivoAnnotation.c.CreatedDate,
                        nvivoAnnotation.c.ModifiedBy,
                        nvivoAnnotation.c.ModifiedDate])

        annotations  = [dict(row) for row in nvivodb.execute(sel)]
        for annotation in annotations:
            annotation['Fragment'] = str(annotation['StartX']) + ':' + str(annotation['StartX'] + annotation['LengthX'] - 1);
            if annotation['StartY'] != None:
                annotation['Fragment'] += ',' + str(annotation['StartY'])
                if annotation['LengthY'] > 0:
                    annotation['Fragment'] += ':' + str(annotation['StartY'] + annotation['LengthY'] - 1)

            if not isinstance(annotation['CreatedDate'], datetime.datetime):
                annotation['CreatedDate'] = dateparser.parse(annotation['CreatedDate'])
            if not isinstance(annotation['ModifiedDate'], datetime.datetime):
                annotation['ModifiedDate'] = dateparser.parse(annotation['ModifiedDate'])

        if args.annotations == 'replace':
            normdb.execute(normTagging.delete(
                                normTagging.c.Node   == None))
        elif args.annotations == 'merge':
            normdb.execute(normTagging.delete(
                           and_(normSource.c.Source  == bindparam('Source'),
                                normSource.c.Node    == None,
                                normSource.c.StartX  == bindparam('StartX'),
                                normSource.c.LengthX == bindparam('LengthX'))),
                           sources)

        if len(annotations) > 0:
            normdb.execute(normTagging.insert(), annotations)

except exc.SQLAlchemyError:
    raise
