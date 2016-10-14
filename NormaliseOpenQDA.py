#!/usr/bin/python
# -*- coding: utf-8 -*-

from builtins import chr
from sqlalchemy import *
from sqlalchemy import exc
import warnings
import sys
import os
import argparse
import uuid
import datetime
import urllib2

exec(open(os.path.dirname(os.path.realpath(__file__)) + '/' + 'NVivoTypes.py').read())

try:
    parser = argparse.ArgumentParser(description='Normalise an offloaded NVivo project.')
    parser.add_argument('-s', '--structure', action='store_true',
                        help='Replace existing table structures.')

    table_choices = ["", "skip", "replace", "merge"]
    parser.add_argument('-n', '--nodes', choices=table_choices, default="replace",
                        help='Node action.')
    parser.add_argument('-na', '--node-attributes', choices=table_choices, default="replace",
                        help='Node attribute table action.')
    parser.add_argument('--sources', choices=table_choices, default="replace",
                        help='Source action.')
    parser.add_argument('-sa', '--source-attributes', choices=table_choices, default="replace",
                        help='Source attribute action.')
    parser.add_argument('-t', '--codings', choices=table_choices, default="replace",
                        help='Tagging action.')
    parser.add_argument('-u', '--users', choices=table_choices, default="replace",
                        help='User action.')

    parser.add_argument('infile', type=str,
                        help='SQLAlchemy path of input OpenQDA database or "-" to create empty project.')
    parser.add_argument('outfile', type=str, nargs='?',
                        help='SQLAlchemy path of output normalised database.')

    args = parser.parse_args()

    if args.infile != '-':
        oqdadb = create_engine(args.infile)
        oqdamd = MetaData(bind=oqdadb)
        oqdamd.reflect(oqdadb)
    else:
        oqdadb = None

    if args.outfile is None:
        args.outfile = args.infile.rsplit('.',1)[0] + '.norm'
    normdb = create_engine(args.outfile)
    normmd = MetaData(bind=normdb)
    normmd.reflect(normdb)

    if args.structure:
        normmd.drop_all(normdb)
        for table in reversed(normmd.sorted_tables):
            normmd.remove(table)

    # Create the normalised database structure
    normUser = normmd.tables.get('User')
    if normUser == None:
        normUser = Table('User', normmd,
            Column('Id',            UUID(),         primary_key=True),
            Column('Name',          String(256)))

    normProject = normmd.tables.get('Project')
    if normProject == None:
        normProject = Table('Project', normmd,
            Column('Title',         String(256),                            nullable=False),
            Column('Description',   String(2048)),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id"),  nullable=False),
            Column('CreatedDate',   DateTime,                               nullable=False),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id"),  nullable=False),
            Column('ModifiedDate',  DateTime,                               nullable=False))

    normSource = normmd.tables.get('Source')
    if normSource == None:
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

    normSourceCategory = normmd.tables.get('SourceCategory')
    if normSourceCategory == None:
        normSourceCategory = Table('SourceCategory', normmd,
            Column('Id',            UUID(),         primary_key=True),
            Column('Name',          String(256)),
            Column('Description',   String(512)),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

    normTagging = normmd.tables.get('Tagging')
    if normTagging == None:
        normTagging = Table('Tagging', normmd,
            Column('Source',        UUID(),         ForeignKey("Source.Id")),
            Column('Node',          UUID(),         ForeignKey("Node.Id")),
            Column('Fragment',      String(256)),
            Column('Memo',          String(256)),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

    normNode = normmd.tables.get('Node')
    if normNode == None:
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

    normNodeCategory = normmd.tables.get('NodeCategory')
    if normNodeCategory == None:
        normNodeCategory = Table('NodeCategory', normmd,
            Column('Id',            UUID(),         primary_key=True),
            Column('Name',          String(256)),
            Column('Description',   String(512)),
            Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
            Column('CreatedDate',   DateTime),
            Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
            Column('ModifiedDate',  DateTime))

    normSourceAttribute = normmd.tables.get('SourceAttribute')
    if normSourceAttribute == None:
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

    normNodeAttribute = normmd.tables.get('NodeAttribute')
    if normNodeAttribute == None:
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

    if oqdadb == None:
        sys.exit()

# OpenQDA tables
    oqdaattributes      = oqdamd.tables['attributes']
    oqdacodes           = oqdamd.tables['codes']
    oqdaimageAttributes = oqdamd.tables['imageAttributes']
    oqdaimageCoding     = oqdamd.tables['imageCoding']
    oqdaimages          = oqdamd.tables['images']

# Users
    if args.users != 'skip':
        print("Normalising users")

        # Collect unique users from images and imageCoding tables
        userlist = [row.owner for row in oqdadb.execute(select([oqdaimageCoding.c.owner]))] + \
                   [row.owner for row in oqdadb.execute(select([oqdaimages.c.owner]))]
        users = {}
        for user in set(userlist):
            userid = uuid.uuid4()
            users[user] = userid
            normdb.execute(normUser.insert(), {'Id':userid, 'Name':user})

        defaultuserid = users[userlist[1]]

# Project
    #if args.project != 'skip':
    if True:
        print("Normalising project")

        # Get earliest and latest timestamp
        dateset = set([row.date for row in oqdadb.execute(select([oqdaimageCoding.c.date]))] +
                      [row.date for row in oqdadb.execute(select([oqdaimages.c.date]))])

        normdb.execute(normProject.insert(), {'Title'       : 'OpenQDA Project',
                                              'Description' : 'Exported from ' + args.infile,
                                              'CreatedBy'   : defaultuserid,
                                              'CreatedDate' : min(dateset),
                                              'ModifiedBy'  : defaultuserid,
                                              'ModifiedDate': max(dateset)})

# Nodes
    if args.nodes != 'skip':
        print("Normalising nodes")

        sel = select([oqdacodes.c.id,
                      oqdacodes.c.name,
                      oqdacodes.c.memo,
                      oqdacodes.c.color])

        codes = [dict(row) for row in oqdadb.execute(sel)]
        codeuuid = {}
        for code in codes:
            code['uuid']         = uuid.uuid4()
            code['CreatedBy']    = defaultuserid
            code['CreatedDate']  = min(dateset)
            code['ModifiedBy']   = defaultuserid
            code['ModifiedDate'] = max(dateset)
            codeuuid[code['id']] = code['uuid']

        if len(codes) > 0:
            normdb.execute(normNode.insert().values({
                    'Id':bindparam('uuid'),
                    'Name': bindparam('name'),
                    'Description': bindparam('memo'),
                    'Color': bindparam('color'),
                }), codes)

# Sources
    if args.sources != 'skip':
        print("Normalising sources")

        sel = select([oqdaimages.c.id,
                      oqdaimages.c.name,
                      oqdaimages.c.owner,
                      oqdaimages.c.date,
                      oqdaimages.c.status,
                      oqdaimages.c.memo])

        # Prepare http voodoo
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        top_level_url = "http://www.mais.mat.br/webQDA/"
        password_mgr.add_password(None, top_level_url, 'leo', 'research1717')
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)
        opener = urllib2.build_opener(handler)

        sources = [dict(row) for row in oqdadb.execute(sel)]
        sourceuuid = {}
        for source in sources:
            source['uuid'] = uuid.uuid4()
            source['Type'] = 'JPEG'
            print("Downloading " + source['name'])
            opener.open('http://www.mais.mat.br/webQDA/images/' + source['name'])
            urllib2.install_opener(opener)
            response = urllib2.urlopen('http://www.mais.mat.br/webQDA/images/' + source['name'])
            source['Object'] = response.read()
            source['CreatedBy']    = users[source['owner']]
            source['CreatedDate']  = source['date']
            source['ModifiedBy']   = users[source['owner']]
            source['ModifiedDate'] = source['date']
            sourceuuid[source['id']] = source['uuid']

        if len(sources) > 0:
            normdb.execute(normSource.insert().values({
                    'Id':bindparam('uuid'),
                    'Name': bindparam('name'),
                    'Description': bindparam('memo'),
                    'Color': bindparam('color'),
                }), sources)

        sys.exit()


    if args.node_attributes != 'skip':
        print("Normalising node attributes")

        sel = select([oqdaattributes.c.id,
                      oqdaattributes.c.name,
                      oqdaattributes.c.memo])

        attributes = [dict(row) for row in oqdadb.execute(sel)]
        for attribute in attributes:
            attribute['uuid']         = codeuuid[attribute['id']]
            attribute['Length']       = len(attribute['memo'])
            attribute['CreatedBy']    = defaultuserid
            attribute['CreatedDate']  = min(dateset)
            attribute['ModifiedBy']   = defaultuserid
            attribute['ModifiedDate'] = max(dateset)

        if len(attributes) > 0:
            normdb.execute(normNodeAttribute.insert().values({
                    'Node': bindparam('uuid'),
                    'Name': bindparam('name'),
                    'Value':bindparam('memo'),
                    'Type': literal_column('Text'),
                }), nodeattrs)

        sys.exit()


# Source attributes
    if args.source_attributes != 'skip':
        print("Normalising source attributes")

        oqdaItem         = oqdamd.tables['Item']
        oqdaRole         = oqdamd.tables['Role']
        oqdaSource       = oqdamd.tables['Source']
        oqdaNameItem     = oqdaItem.alias(name='NameItem')
        oqdaNameRole     = oqdaRole.alias(name='NameRole')
        oqdaValueItem    = oqdaItem.alias(name='ValueItem')
        oqdaValueRole    = oqdaRole.alias(name='ValueRole')
        oqdaExtendedItem = oqdamd.tables['ExtendedItem'].alias(name='ExtendedNameItem')

        sel = select([oqdaSource.c.Item_Id.label('Source'),
                      oqdaNameItem.c.Name,
                      oqdaValueItem.c.Name.label('Value'),
                      oqdaValueItem.c.CreatedBy,
                      oqdaValueItem.c.CreatedDate,
                      oqdaValueItem.c.ModifiedBy,
                      oqdaValueItem.c.ModifiedDate,
                      oqdaExtendedItem.c.Properties])
        sel = sel.where(and_(
                      oqdaSource.c.Item_Id == oqdaValueRole.c.Item1_Id,
                      oqdaValueRole.c.TypeId == literal_column('7'),
                      oqdaValueItem.c.Id == oqdaValueRole.c.Item2_Id,
                      oqdaNameRole.c.Item2_Id == oqdaValueRole.c.Item2_Id,
                      oqdaNameRole.c.TypeId == literal_column('6'),
                      oqdaNameItem.c.Id == oqdaNameRole.c.Item1_Id,
                      oqdaValueItem.c.Name != literal_column('\'Unassigned\''),
                      oqdaExtendedItem.c.Item_Id == oqdaNameItem.c.Id
                    ))
        sourceattrs  = [dict(row) for row in oqdadb.execute(sel)]
        for sourceattr in sourceattrs:
            properties = parseString(sourceattr['Properties'])
            for property in properties.documentElement.getElementsByTagName('Property'):
                if property.getAttribute('Key') == 'DataType':
                    sourceattr['Type'] = DataTypeName.get(int(property.getAttribute('Value')), property.getAttribute('Value'))
                elif property.getAttribute('Key') == 'Length':
                    sourceattr['Length'] = int(property.getAttribute('Value'))

        if args.windows:
            for sourceattr in sourceattrs:
                sourceattr['Name']  = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourceattr['Name']))
                sourceattr['Value'] = u''.join(map(lambda ch: chr(ord(ch) - 0x377), sourceattr['Value']))

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

        oqdaNodeReference = oqdamd.tables['NodeReference']

        sel = select([oqdaNodeReference.c.Source_Item_Id.label('Source'),
                      oqdaNodeReference.c.Node_Item_Id.label('Node'),
                      oqdaNodeReference.c.StartX,
                      oqdaNodeReference.c.LengthX,
                      oqdaNodeReference.c.StartY,
                      oqdaNodeReference.c.LengthY,
                      oqdaNodeReference.c.StartZ,
                      oqdaNodeReference.c.CreatedBy,
                      oqdaNodeReference.c.CreatedDate,
                      oqdaNodeReference.c.ModifiedBy,
                      oqdaNodeReference.c.ModifiedDate,
                      oqdaItem.c.TypeId])
        sel = sel.where(and_(
                      #oqdaNodeReference.c.ReferenceTypeId == literal_column('0'),
                      oqdaItem.c.Id == oqdaNodeReference.c.Node_Item_Id,
                      or_(
                        oqdaItem.c.TypeId == literal_column('16'),
                        oqdaItem.c.TypeId == literal_column('62'))))

        taggings  = [dict(row) for row in oqdadb.execute(sel)]
        for tagging in taggings:
            # JS: Should be able to do this in select statement - figure out how!
            if tagging['StartZ'] != None:
                next
            tagging['Fragment'] = str(tagging['StartX']) + ':' + str(tagging['StartX'] + tagging['LengthX'] - 1)
            if tagging['StartY'] != None:
                tagging['Fragment'] += ',' + str(tagging['StartY'])
                if tagging['LengthY'] > 0:
                    tagging['Fragment'] += ':' + str(tagging['StartY'] + tagging['LengthY'] - 1)

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

        oqdaAnnotation = oqdamd.tables['Annotation']

        sel = select([oqdaAnnotation.c.Item_Id.label('Source'),
                      oqdaAnnotation.c.Text.label('Memo'),
                      oqdaAnnotation.c.StartX,
                      oqdaAnnotation.c.LengthX,
                      oqdaAnnotation.c.StartY,
                      oqdaAnnotation.c.LengthY,
                      oqdaAnnotation.c.CreatedBy,
                      oqdaAnnotation.c.CreatedDate,
                      oqdaAnnotation.c.ModifiedBy,
                      oqdaAnnotation.c.ModifiedDate])

        annotations  = [dict(row) for row in oqdadb.execute(sel)]
        for annotation in annotations:
            annotation['Fragment'] = str(annotation['StartX']) + ':' + str(annotation['StartX'] + annotation['LengthX'] - 1);
            if annotation['StartY'] != None:
                annotation['Fragment'] += ',' + str(annotation['StartY'])
                if annotation['LengthY'] > 0:
                    annotation['Fragment'] += ':' + str(annotation['StartY'] + annotation['LengthY'] - 1)

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
