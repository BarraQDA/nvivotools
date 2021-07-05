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

from __future__ import print_function
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
import webcolors

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

try:
    parser = argparse.ArgumentParser(description='Normalise an offloaded NVivo project.')

    table_choices = ["", "skip", "merge", "overwrite", "replace"]
    parser.add_argument('-n', '--nodes', choices=table_choices, default="merge",
                        help='Node action.')
    parser.add_argument('-na', '--node-attributes', choices=table_choices, default="merge",
                        help='Node attribute table action.')
    parser.add_argument('--sources', choices=table_choices, default="merge",
                        help='Source action.')
    parser.add_argument('-sa', '--source-attributes', choices=table_choices, default="merge",
                        help='Source attribute action.')
    parser.add_argument('-t', '--taggings', choices=table_choices, default="merge",
                        help='Tagging action.')
    parser.add_argument('-u', '--users', choices=table_choices, default="merge",
                        help='User action.')

    parser.add_argument('-U', '--user', type=str,
                        help='Username for fetching images')
    parser.add_argument('-P', '--password', type=str,
                        help='Password for fetching images')
    parser.add_argument('--url', type=str, default='http://localhost/images/',
                        help='URL for fetching images')

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
        print("Normalising users", file=sys.stderr)

        # Collect unique users from images and imageCoding tables
        userlist = [row.owner for row in oqdadb.execute(select([oqdaimageCoding.c.owner]))] + \
                   [row.owner for row in oqdadb.execute(select([oqdaimages.c.owner]))]
        users = {}
        for user in set(userlist):
            userid = uuid.uuid4()
            users[user] = userid
            normdb.execute(normUser.insert(), {
                        'Id'  : userid,
                        'Name': user})

        defaultuserid = users[userlist[1]]

# Project
    #if args.project != 'skip':
    if True:
        print("Normalising project", file=sys.stderr)

        # Get earliest and latest timestamp
        dateset = set([row.date for row in oqdadb.execute(select([oqdaimageCoding.c.date]))] +
                      [row.date for row in oqdadb.execute(select([oqdaimages.c.date]))])

        normdb.execute(normProject.insert(), {
                        'Title'       : 'OpenQDA Project',
                        'Description' : 'Exported from ' + args.infile,
                        'CreatedBy'   : defaultuserid,
                        'CreatedDate' : min(dateset),
                        'ModifiedBy'  : defaultuserid,
                        'ModifiedDate': max(dateset)})

# Nodes
    if args.nodes != 'skip':
        print("Normalising nodes", file=sys.stderr)

        sel = select([oqdacodes.c.id,
                      oqdacodes.c.name,
                      oqdacodes.c.memo,
                      oqdacodes.c.color])

        codes = [dict(row) for row in oqdadb.execute(sel)]
        codeuuid = {}
        for code in codes:
            code['uuid']         = uuid.uuid4()
            code['Color']        = int(webcolors.name_to_hex(code['color'])[1:], 16)
            code['CreatedBy']    = defaultuserid
            code['CreatedDate']  = min(dateset)
            code['ModifiedBy']   = defaultuserid
            code['ModifiedDate'] = max(dateset)
            codeuuid[code['id']] = code['uuid']

        if len(codes) > 0:
            normdb.execute(normNode.insert().values({
                    'Id'         : bindparam('uuid'),
                    'Name'       : bindparam('name'),
                    'Description': bindparam('memo')
                }), codes)

# Sources
    if args.sources != 'skip':
        print("Normalising sources", file=sys.stderr)

        # Create dummy source category
        sourcecatuuid = uuid.uuid4()
        normdb.execute(normSourceCategory.insert(), {
                    'Id'          : sourcecatuuid,
                    'Name'        : 'OpenQDA image',
                    'CreatedBy'   : defaultuserid,
                    'CreatedDate' : min(dateset),
                    'ModifiedBy'  : defaultuserid,
                    'ModifiedDate': max(dateset)})


        sel = select([oqdaimages.c.id,
                      oqdaimages.c.name,
                      oqdaimages.c.owner,
                      oqdaimages.c.date,
                      oqdaimages.c.status,
                      oqdaimages.c.memo])

        # Prepare http voodoo
        if args.user != None:
            password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
            top_level_url = args.url
            password_mgr.add_password(None, top_level_url, args.user, args.password)
            handler = urllib2.HTTPBasicAuthHandler(password_mgr)
            opener = urllib2.build_opener(handler)

        sources = [dict(row) for row in oqdadb.execute(sel)]
        sourceuuid = {}
        for source in sources:
            source['uuid']         = uuid.uuid4()
            source['Category']     = sourcecatuuid
            source['ObjectType']   = 'JPEG'
            print("Downloading " + source['name'], file=sys.stderr)
            if args.user != None:
                opener.open(args.url + source['name'])
                urllib2.install_opener(opener)
            response = urllib2.urlopen(args.url + source['name'])
            source['Object']       = response.read()
            source['Thumbnail']    = ''
            source['CreatedBy']    = users[source['owner']]
            source['CreatedDate']  = source['date']
            source['ModifiedBy']   = users[source['owner']]
            source['ModifiedDate'] = source['date']
            sourceuuid[source['id']] = source['uuid']

        if len(sources) > 0:
            normdb.execute(normSource.insert().values({
                    'Id'         : bindparam('uuid'),
                    'Name'       : bindparam('name'),
                    'Description': bindparam('memo')
                }), sources)

# Source attributes
    if args.source_attributes != 'skip':
        print("Normalising source attributes", file=sys.stderr)

        sel = select([oqdaimageAttributes.c.images_id,
                      oqdaimageAttributes.c.value,
                      oqdaattributes.c.name,
                      oqdaattributes.c.memo]).where(
                      oqdaattributes.c.id == oqdaimageAttributes.c.attributes_id)

        attributes = [dict(row) for row in oqdadb.execute(sel)]
        for attribute in attributes:
            attribute['Source']       = sourceuuid[attribute['images_id']]
            attribute['Type']         = 'Text'
            attribute['Length']       = len(attribute['value'])
            attribute['CreatedBy']    = defaultuserid
            attribute['CreatedDate']  = min(dateset)
            attribute['ModifiedBy']   = defaultuserid
            attribute['ModifiedDate'] = max(dateset)

        if len(attributes) > 0:
            normdb.execute(normSourceAttribute.insert().values({
                    'Name' : bindparam('name'),
                    'Value': bindparam('value')
                }), attributes)

# Tagging
    if args.taggings != 'skip':
        print("Normalising taggings", file=sys.stderr)

        sel = select([oqdaimageCoding.c.images_id,
                      oqdaimageCoding.c.codes_id,
                      oqdaimageCoding.c.x1,
                      oqdaimageCoding.c.y1,
                      oqdaimageCoding.c.x2,
                      oqdaimageCoding.c.y2,
                      oqdaimageCoding.c.owner,
                      oqdaimageCoding.c.date,
                      oqdaimageCoding.c.memo])

        taggings  = [dict(row) for row in oqdadb.execute(sel)]
        for tagging in taggings:
            tagging['Fragment']     = str(tagging['x1']) + ':' + str(tagging['x2']) + ',' + str(tagging['y1']) + ':' + str(tagging['y2'])
            tagging['Source']       = sourceuuid[tagging['images_id']]
            tagging['Node']         = codeuuid[tagging['codes_id']]
            tagging['CreatedBy']    = users[tagging['owner']]
            tagging['CreatedDate']  = tagging['date']
            tagging['ModifiedBy']   = users[tagging['owner']]
            tagging['ModifiedDate'] = tagging['date']

        if len(taggings) > 0:
            normdb.execute(normTagging.insert().values({
                    'Memo': bindparam('memo')
                }), taggings)

except exc.SQLAlchemyError:
    raise
