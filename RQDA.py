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

from sqlalchemy import *
from sqlalchemy import exc
import os
import datetime

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

def Denormalise(args):
    # Initialise DB variables so exception handlers don't freak out
    normdb = None
    rqdadb = None
    rqdatr = None

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
            args.outdb = args.indb.rsplit('.',1)[0] + '.rqda'


        rqdadb = create_engine(args.outdb)
        rqdamd = MetaData(bind=rqdadb)
        rqdamd.reflect(rqdadb)

# Create the RQDA database structure if it doesn't already exist
        rqdaproject = rqdamd.tables.get('project')
        if rqdaproject is None:
            rqdaproject = Table('project', rqdamd,
                Column('databaseversion', Text()),
                Column('date',            Text()),
                Column('dateM',           Text()),
                Column('memo',            Text()),
                Column('about',           Text()),
                Column('imageDir',        Text()))

        rqdasource = rqdamd.tables.get('source')
        if rqdasource is None:
            rqdasource = Table('source', rqdamd,
                Column('name',   Text()),
                Column('id',     Integer),
                Column('file',   Text()),
                Column('memo',   Text()),
                Column('owner',  Text()),
                Column('date',   Text()),
                Column('dateM',  Text()),
                Column('status', Integer))

        rqdafileAttr = rqdamd.tables.get('fileAttr')
        if rqdafileAttr is None:
            rqdafileAttr = Table('fileAttr', rqdamd,
                Column('variable', Text()),
                Column('value',    Text()),
                Column('fileID',   Integer),
                Column('date',     Text()),
                Column('dateM',    Text()),
                Column('owner',    Text()),
                Column('status',   Integer))

        rqdafilecat = rqdamd.tables.get('filecat')
        if rqdafilecat is None:
            rqdafilecat = Table('filecat', rqdamd,
                Column('name', Text()),
                Column('fid', Integer),
                Column('catid', Integer),
                Column('owner', Text()),
                Column('date', Text()),
                Column('dateM', Text()),
                Column('memo', Text()),
                Column('status', Integer))

        rqdaannotation = rqdamd.tables.get('annotation')
        if rqdaannotation is None:
            rqdaannotation = Table('annotation', rqdamd,
                Column('fid',        Integer),
                Column('position',   Integer),
                Column('annotation', Text()),
                Column('owner',      Text()),
                Column('date',       Text()),
                Column('dateM',      Text()),
                Column('status',     Integer))

        rqdaattributes = rqdamd.tables.get('attributes')
        if rqdaattributes is None:
            rqdaattributes = Table('attributes', rqdamd,
                Column('name',   Text()),
                Column('status', Integer),
                Column('date',   Text()),
                Column('dateM',  Text()),
                Column('owner',  Text()),
                Column('memo',   Text()),
                Column('class',  Text()))

        rqdacaseAttr = rqdamd.tables.get('caseAttr')
        if rqdacaseAttr is None:
            rqdacaseAttr = Table('caseAttr', rqdamd,
                Column('variable', Text()),
                Column('value',    Text()),
                Column('caseID',   Integer),
                Column('date',     Text()),
                Column('dateM',    Text()),
                Column('owner',    Text()),
                Column('status',   Integer))

        rqdacaselinkage = rqdamd.tables.get('caselinkage')
        if rqdacaselinkage is None:
            rqdacaselinkage = Table('caselinkage', rqdamd,
                Column('caseid',   Integer),
                Column('fid',      Integer),
                Column('selfirst', Float()),
                Column('selend',   Float()),
                Column('status',   Integer),
                Column('owner',    Text()),
                Column('date',     Text()),
                Column('memo',     Text()))

        rqdacases = rqdamd.tables.get('cases')
        if rqdacases is None:
            rqdacases = Table('cases', rqdamd,
                Column('name',   Text()),
                Column('memo',   Text()),
                Column('owner',  Text()),
                Column('date',   Text()),
                Column('dateM',  Text()),
                Column('id',     Integer),
                Column('status', Integer))

        rqdacodecat = rqdamd.tables.get('codecat')
        if rqdacodecat is None:
            rqdacodecat = Table('codecat', rqdamd,
                Column('name',   Text()),
                Column('cid',    Integer),
                Column('catid',  Integer),
                Column('owner',  Text()),
                Column('date',   Text()),
                Column('dateM',  Text()),
                Column('memo',   Text()),
                Column('status', Integer))

        rqdacoding = rqdamd.tables.get('coding')
        if rqdacoding is None:
            rqdacoding = Table('coding', rqdamd,
                Column('cid',      Integer),
                Column('fid',      Integer),
                Column('seltext',  Text()),
                Column('selfirst', Float()),
                Column('selend',   Float()),
                Column('status',   Integer),
                Column('owner',    Text()),
                Column('date',     Text()),
                Column('memo',     Text()))

        rqdacoding2 = rqdamd.tables.get('coding2')
        if rqdacoding2 is None:
            rqdacoding2 = Table('coding2', rqdamd,
                Column('cid',      Integer),
                Column('fid',      Integer),
                Column('seltext',  Text()),
                Column('selfirst', Float()),
                Column('selend',   Float()),
                Column('status',   Integer),
                Column('owner',    Text()),
                Column('date',     Text()),
                Column('memo',     Text()))

        rqdafreecode = rqdamd.tables.get('freecode')
        if rqdafreecode is None:
            rqdafreecode = Table('freecode', rqdamd,
                Column('name',   Text()),
                Column('memo',   Text()),
                Column('owner',  Text()),
                Column('date',   Text()),
                Column('dateM',  Text()),
                Column('id',     Integer),
                Column('status', Integer),
                Column('color',  Text()))

        rqdaimage = rqdamd.tables.get('image')
        if rqdaimage is None:
            rqdaimage = Table('image', rqdamd,
                Column('name',   Text()),
                Column('id',     Integer),
                Column('date',   Text()),
                Column('dateM',  Text()),
                Column('owner',  Text()),
                Column('status', Integer))

        rqdaimageCoding = rqdamd.tables.get('imageCoding')
        if rqdaimageCoding is None:
            rqdaimageCoding = Table('imageCoding', rqdamd,
                Column('cid',    Integer),
                Column('iid',    Integer),
                Column('x1',     Integer),
                Column('y1',     Integer),
                Column('x2',     Integer),
                Column('y2',     Integer),
                Column('memo',   Text()),
                Column('date',   Text()),
                Column('dateM',  Text()),
                Column('owner',  Text()),
                Column('status', Integer))

        rqdajournal = rqdamd.tables.get('journal')
        if rqdajournal is None:
            rqdajournal = Table('journal', rqdamd,
                Column('name',    Text()),
                Column('journal', Text()),
                Column('date',    Text()),
                Column('dateM',   Text()),
                Column('owner',   Text()),
                Column('status',  Integer))

        rqdatreecode = rqdamd.tables.get('treecode')
        if rqdatreecode is None:
            rqdatreecode = Table('treecode', rqdamd,
                Column('cid', Integer),
                Column('catid',  Integer),
                Column('date',   Text()),
                Column('dateM',  Text()),
                Column('memo',   Text()),
                Column('status', Integer),
                Column('owner',  Text()))

        rqdatreefile = rqdamd.tables.get('treefile')
        if rqdatreefile is None:
            rqdatreefile = Table('treefile', rqdamd,
                Column('fid',    Integer),
                Column('catid',  Integer),
                Column('date',   Text()),
                Column('dateM',  Text()),
                Column('memo',   Text()),
                Column('status', Integer),
                Column('owner',  Text()))

        rqdamd.create_all(rqdadb)

        if normdb is None:     # that is, if all we are doing is making an empty RQDA file
            return

        rqdacon = rqdadb.connect()
        rqdatr=rqdacon.begin()

# Project
        if args.project != 'skip':
            if args.verbosity > 0:
                print("Converting project")

            curproject = rqdacon.execute(select([rqdaproject.c.memo])).first()
            if curproject is not None and args.project == 'overwrite':
                rqdacon.execute(rqdaproject.delete())
                curproject = None

            if curproject is None:
                project = dict(normdb.execute(select([
                        normProject.c.Title,
                        normProject.c.Description.label('memo'),
                        normProject.c.CreatedDate,
                        normProject.c.ModifiedDate
                    ])).first())
                project['About'] = project['Title'] +' Imported by NVivotools ' + datetime.datetime.now().strftime('%c')

                project['date']  = project['CreatedDate'].strftime('%c')
                project['dateM'] = project['ModifiedDate'].strftime('%c')
                rqdacon.execute(rqdaproject.insert().values({
                        'databaseversion': literal_column("'DBVersion:0.1'"),
                        'about' :          bindparam('About')
                    }), project)

# Source categories
        if args.source_categories != 'skip':
            if args.verbosity > 0:
                print("Converting source categories")

            sourcecats  = [dict(row) for row in normdb.execute(select([
                    normSourceCategory.c.Id,
                    normSourceCategory.c.Name.label('name'),
                    normSourceCategory.c.Description.label('memo'),
                    normUser.c.Name.label('owner'),
                    normSourceCategory.c.CreatedDate,
                    normSourceCategory.c.ModifiedDate
                ]).where(
                    normUser.c.Id == normSourceCategory.c.CreatedBy
                ))]

            filecatid = 1 # This isn't quite right for merging...
            filecatmap = []
            for sourcecat in sourcecats:
                sourcecat['catid'] = filecatid
                filecatid += 1
                sourcecat['date']   = sourcecat['CreatedDate'].strftime('%c')
                sourcecat['dateM']  = sourcecat['ModifiedDate'].strftime('%c')
                sourcecat['status'] = 1

            merge_overwrite_or_replace(rqdacon, rqdafilecat, ['catid'], sourcecats, args.source_categories, args.verbosity)

# Source attributes
        if args.source_attributes != 'skip':
            if args.verbosity > 0:
                print("Converting source attributes")

            sourceattrs  = [dict(row) for row in normdb.execute(select([
                    normSourceCategory.c.Id,
                    normSourceCategory.c.Name.label('name'),
                    normSourceCategory.c.Description.label('memo'),
                    normUser.c.Name.label('owner'),
                    normSourceCategory.c.CreatedDate,
                    normSourceCategory.c.ModifiedDate
                ]).where(
                    normUser.c.Id == normSourceCategory.c.CreatedBy
                ))]



# All done.
        rqdatr.commit()
        rqdatr = None
        rqdacon.close()
        rqdadb.dispose()

        normdb.dispose()

    except:
        raise
        if not normtr is None:
            normtr.rollback()
        normdb.dispose()
        normdb.dispose()
