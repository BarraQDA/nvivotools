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
from sqlalchemy import *
from sqlalchemy import exc
import os
import sys
import re
from datetime import date, time, datetime
from dateutil import parser as dateparser
from distutils import util

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

def Norm2RQDA(args):
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
        normSourceAttribute = normmd.tables.get('SourceAttribute')
        normSourceValue     = normmd.tables.get('SourceValue')
        normTagging         = normmd.tables.get('Tagging')
        normNode            = normmd.tables.get('Node')
        normNodeCategory    = normmd.tables.get('NodeCategory')
        normNodeAttribute   = normmd.tables.get('NodeAttribute')
        normNodeValue       = normmd.tables.get('NodeValue')

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
                print("Converting project", file=sys.stderr)

            curproject = rqdacon.execute(select([rqdaproject.c.memo])).first()
            if curproject is not None and args.project == 'overwrite':
                rqdacon.execute(rqdaproject.delete())
                curproject = None

            if curproject is None:
                project = dict(normdb.execute(select([
                        normProject.c.Title,
                        normProject.c.Description.label('memo'),
                        normUser.c.Name.label('owner'),
                        normProject.c.CreatedDate,
                        normProject.c.ModifiedDate
                    ]).where(
                        normUser.c.Id == normSourceCategory.c.CreatedBy
                    )).first())
                project['About'] = project['Title'] +' Imported by NVivotools ' + datetime.now().strftime('%c')

                project['date']  = project['CreatedDate'].strftime('%c')
                project['dateM'] = project['ModifiedDate'].strftime('%c')
                rqdacon.execute(rqdaproject.insert().values({
                        'databaseversion': literal_column("'DBVersion:0.1'"),
                        'about' :          bindparam('About')
                    }), project)

# Source categories
        if args.source_categories != 'skip':
            if args.verbosity > 0:
                print("Converting source categories", file=sys.stderr)

            sourcecats  = [dict(row) for row in normdb.execute(select([
                    normSourceCategory.c.Id.label('Uuid'),
                    normSourceCategory.c.Name.label('name'),
                    normSourceCategory.c.Description.label('memo'),
                    normUser.c.Name.label('owner'),
                    normSourceCategory.c.CreatedDate,
                    normSourceCategory.c.ModifiedDate
                ]).where(
                    normUser.c.Id == normSourceCategory.c.CreatedBy
                ))]

            lastid = 1
            filecatid = {}
            for sourcecat in sourcecats:
                sourcecat['catid'] = lastid
                filecatid[sourcecat['Uuid']] = lastid
                lastid += 1
                sourcecat['date']   = sourcecat['CreatedDate']. strftime('%c')
                sourcecat['dateM']  = sourcecat['ModifiedDate'].strftime('%c')
                sourcecat['status'] = 1

            if len(sourcecats) > 0:
                rqdacon.execute(rqdafilecat.insert(), sourcecats)

# Sources
        if args.sources != 'skip':
            if args.verbosity > 0:
                print("Converting sources", file=sys.stderr)

            sources = [dict(row) for row in normdb.execute(select([
                    normSource.c.Id.label('Uuid'),
                    normSource.c.Category,
                    normSource.c.Name.label('name'),
                    normSource.c.Description.label('memo'),
                    normSource.c.Content.label('file'),
                    normUser.c.Name.label('owner'),
                    normSource.c.CreatedDate,
                    normSource.c.ModifiedDate
                ]).where(and_(
                    normSource.c.Content.isnot(None),
                    normUser.c.Id == normSource.c.CreatedBy
                )))]

            lastid = 1
            sourceid = {}
            sourcename = {}
            sourcetext = {}
            for source in sources:
                source['id'] = lastid
                sourceid[source['Uuid']] = lastid
                sourcename[source['Uuid']] = source['name']
                sourcetext[source['Uuid']] = source['file']
                lastid += 1
                source['date']   = source['CreatedDate']. strftime('%c')
                source['dateM']  = source['ModifiedDate'].strftime('%c')
                source['status'] = 1


            if len(sources) > 0:
                rqdacon.execute(rqdasource.insert(), sources)

            # Map NVivo classifications to RQDA categories
            if args.source_categories != 'skip':
                treefiles = []
                for source in sources:
                    if source['Category'] is not None:
                        treefiles += [{
                                'fid':    source['id'],
                                'catid':  filecatid[source['Category']],
                                'date':   source['date'],
                                'dateM':  source['dateM'],
                                'memo':   None,
                                'status': 1,
                                'owner':  source['owner']
                            }]

                if len(treefiles) > 0:
                    rqdacon.execute(rqdatreefile.insert(), treefiles)

        def create_or_test_attribute(value):
            attrclass = None
            if value['Type'] == 'Text':
                attrclass = 'character'
            elif value['Type'] == 'Integer':
                attrclass = 'numeric'
            elif value['Type'] == 'Decimal':
                attrclass = 'numeric'
            elif value['Type'] == 'DateTime':
                attrclass = 'numeric'
                value['value'] = unicode(date.strftime(dateparser.parse(value['value']), '%Y%m%d%H%M%S'))
            elif value['Type'] == 'Date':
                attrclass = 'numeric'
                value['value'] = unicode(date.strftime(dateparser.parse(value['value']), '%Y%m%d'))
            elif value['Type'] == 'Time':
                attrclass = 'numeric'
                value['value'] = unicode(time.strftime(dateparser.parse(value['value']).time(), '%H%M%S'))
            elif value['Type'] == 'Boolean':
                attrclass = 'numeric'
                value['value'] = util.strtobool(value['value'])

            attribute = rqdacon.execute(select([
                    rqdaattributes.c['class']
                ]).where(
                    rqdaattributes.c.name == bindparam('variable')
                ), value).first()
            if attribute is None:
                rqdacon.execute(rqdaattributes.insert(), {
                        'name':   value['variable'],
                        'class':  attrclass,
                        'status': 1,
                        'date':   value['AttributeCreatedDate']. strftime('%c'),
                        'dateM':  value['AttributeModifiedDate'].strftime('%c'),
                        'owner':  value['attributeowner']
                    })
            else:
                if attribute['class'] != attrclass:
                    print("WARNING: Inconsistent type for attribute: " + value['variable'], file=sys.stderr)

# Source attributes
        if args.source_attributes != 'skip':
            if args.verbosity > 0:
                print("Converting source attributes", file=sys.stderr)

            normAttributeUser = normUser.alias(name='AttributeUser')
            sourcevalues = [dict(row) for row in normdb.execute(select([
                    normSourceValue.c.Source.label('SourceUuid'),
                    normSourceAttribute.c.Name.label('variable'),
                    normSourceAttribute.c.Type,
                    normAttributeUser.c.Name.label('attributeowner'),
                    normSourceAttribute.c.CreatedDate.label('AttributeCreatedDate'),
                    normSourceAttribute.c.ModifiedDate.label('AttributeModifiedDate'),
                    normSourceValue.c.Value.label('value'),
                    normUser.c.Name.label('owner'),
                    normSourceValue.c.CreatedDate,
                    normSourceValue.c.ModifiedDate
                ]).where(and_(
                    normSource.c.Id == normSourceValue.c.Source,
                    normSource.c.Content.isnot(None),
                    normSourceAttribute.c.Id == normSourceValue.c.Attribute,
                    normUser.c.Id == normSourceValue.c.CreatedBy,
                    normAttributeUser.c.Id == normSourceAttribute.c.CreatedBy
                )))]

            for sourcevalue in sourcevalues:
                sourcevalue['fileID'] = sourceid[sourcevalue['SourceUuid']]
                sourcevalue['date']   = sourcevalue['CreatedDate']. strftime('%c')
                sourcevalue['dateM']  = sourcevalue['ModifiedDate'].strftime('%c')
                sourcevalue['status'] = 1

                create_or_test_attribute(sourcevalue)

            if len(sourcevalues) > 0:
                rqdacon.execute(rqdafileAttr.insert(), sourcevalues)

# Node categories
        if args.node_categories != 'skip':
            if args.verbosity > 0:
                print("Converting node categories", file=sys.stderr)

            codecats  = [dict(row) for row in normdb.execute(select([
                    normNodeCategory.c.Id.label('Uuid'),
                    normNodeCategory.c.Name.label('name'),
                    normNodeCategory.c.Description.label('memo'),
                    normUser.c.Name.label('owner'),
                    normNodeCategory.c.CreatedDate,
                    normNodeCategory.c.ModifiedDate
                ]).where(
                    normUser.c.Id == normNodeCategory.c.CreatedBy
                ))]

            lastid = 1
            codecatid = {}
            codecatname = {}
            for codecat in codecats:
                codecat['catid'] = lastid
                codecatid[codecat['Uuid']] = lastid
                codecatname[codecat['Uuid']] = codecat['name']
                lastid += 1
                codecat['date']   = codecat['CreatedDate']. strftime('%c')
                codecat['dateM']  = codecat['ModifiedDate'].strftime('%c')
                codecat['status'] = 1

            if len(codecats) > 0:
                rqdacon.execute(rqdacodecat.insert(), codecats)

# Nodes
        if args.nodes != 'skip':
            if args.verbosity > 0:
                print("Converting nodes", file=sys.stderr)

            # Nodes without any attributes are mapped to RQDA codes, those with attributes
            # are mapped to RQDA cases.
            nodes = [dict(row) for row in normdb.execute(select([
                    normNode.c.Id.label('Uuid'),
                    normNode.c.Category,
                    normNode.c.Name.label('name'),
                    normNode.c.Description.label('memo'),
                    normUser.c.Name.label('owner'),
                    normNode.c.CreatedDate,
                    normNode.c.ModifiedDate,
                    func.count(normNodeValue.c.Node).label('ValueCount')
                ]).where(
                    normUser.c.Id == normNode.c.CreatedBy
                ).group_by(
                    normNode.c.Id
                ).select_from(
                    normNode.outerjoin(
                    normNodeValue,
                    normNodeValue.c.Node == normNode.c.Id
                )))]

            lastcodeid = 1
            lastcaseid = 1
            codeid = {}
            caseid = {}
            codes = []
            cases = []
            for node in nodes:
                if node['ValueCount'] == 0:
                    node['id'] = lastcodeid
                    codeid[node['Uuid']] = lastcodeid
                    lastcodeid += 1
                    node['date']   = node['CreatedDate']. strftime('%c')
                    node['dateM']  = node['ModifiedDate'].strftime('%c')
                    node['status'] = 1
                    codes += [node]
                else:
                    node['id'] = lastcaseid
                    caseid[node['Uuid']] = lastcaseid
                    lastcaseid += 1
                    node['date']   = node['CreatedDate']. strftime('%c')
                    node['dateM']  = node['ModifiedDate'].strftime('%c')
                    node['status'] = 1
                    cases += [node]

            if len(codes) > 0:
                rqdacon.execute(rqdafreecode.insert(), codes)

            if len(cases) > 0:
                rqdacon.execute(rqdacases.insert(), cases)

            # Map NVivo classifications to RQDA categories
            if args.node_categories != 'skip':
                treecodes = []
                for code in codes:
                    if code['Category'] is not None:
                        treecodes += [{
                                'cid':    code['id'],
                                'catid':  codecatid[code['Category']],
                                'date':   code['date'],
                                'dateM':  code['dateM'],
                                'memo':   None,
                                'status': 1,
                                'owner':  code['owner']
                            }]

                if len(treecodes) > 0:
                    rqdacon.execute(rqdatreecode.insert(), treecodes)

                # Make an attribute for NVivo Classification
                casecatattrs = []
                for case in cases:
                    if case['Category'] is not None:
                        value = {
                                'Type':                  'Text',
                                'variable':              'Category',
                                'value':                 codecatname[case['Category']],
                                'AttributeCreatedDate':  project['CreatedDate'],
                                'AttributeModifiedDate': project['ModifiedDate'],
                                'attributeowner':        project['owner']
                            }
                        create_or_test_attribute(value)
                        casecatattrs += [{
                                'variable': u'Category',
                                'value':    codecatname[case['Category']],
                                'caseID':   case['id'],
                                'date':     case['date'],
                                'dateM':    case['dateM'],
                                'status':   1,
                                'owner':    case['owner']
                            }]
                if len(casecatattrs) > 0:
                    rqdacon.execute(rqdacaseAttr.insert(), casecatattrs)



            if args.node_attributes != 'skip':
                normAttributeUser = normUser.alias(name='AttributeUser')
                casevalues = [dict(row) for row in normdb.execute(select([
                        normNodeValue.c.Node.label('NodeUuid'),
                        normNodeAttribute.c.Name.label('variable'),
                        normNodeAttribute.c.Type,
                        normAttributeUser.c.Name.label('attributeowner'),
                        normNodeAttribute.c.CreatedDate.label('AttributeCreatedDate'),
                        normNodeAttribute.c.ModifiedDate.label('AttributeModifiedDate'),
                        normNodeValue.c.Value.label('value'),
                        normUser.c.Name.label('owner'),
                        normNodeValue.c.CreatedDate,
                        normNodeValue.c.ModifiedDate
                    ]).where(and_(
                        normNodeAttribute.c.Id == normNodeValue.c.Attribute,
                        normUser.c.Id == normNodeValue.c.CreatedBy,
                        normAttributeUser.c.Id == normNodeAttribute.c.CreatedBy
                    )))]

                for casevalue in casevalues:
                    casevalue['caseID'] = caseid[casevalue['NodeUuid']]
                    casevalue['date']   = casevalue['CreatedDate']. strftime('%c')
                    casevalue['dateM']  = casevalue['ModifiedDate'].strftime('%c')
                    casevalue['status'] = 1
                    create_or_test_attribute(casevalue)

                if len(casevalues) > 0:
                    rqdacon.execute(rqdacaseAttr.insert(), casevalues)

# Tagging
        if args.taggings != 'skip':
            if args.verbosity > 0:
                print("Converting taggings", file=sys.stderr)

            taggings = [dict(row) for row in normdb.execute(select([
                    normTagging.c.Id,
                    normTagging.c.Source.label('SourceUuid'),
                    normTagging.c.Node,
                    normTagging.c.Memo.label('memo'),
                    normTagging.c.Fragment,
                    normUser.c.Name.label('owner'),
                    normTagging.c.CreatedDate,
                    normTagging.c.ModifiedDate
                ]).where(and_(
                    normSource.c.Id == normTagging.c.Source,
                    normSource.c.Content.isnot(None),
                    normUser.c.Id == normTagging.c.CreatedBy
                )))]

            annotations = []
            codings = []
            caselinkages = []
            for tagging in taggings:
                tagging['date']   = tagging['CreatedDate']. strftime('%c')
                tagging['dateM']  = tagging['ModifiedDate'].strftime('%c')
                tagging['status'] = 1
                matchfragment = re.match("([0-9]+):([0-9]+)(?:,([0-9]+)(?::([0-9]+))?)?", tagging['Fragment'])
                if matchfragment is None:
                    print("WARNING: Unrecognised tagging fragment: " + tagging['Fragment'] + " for Source: " + sourcename[tagging['SourceUuid']], file=sys.stderr)
                    continue

                if tagging['Node'] is None:
                    tagging['annotation'] = tagging['memo']
                    tagging['fid']        = sourceid[tagging['SourceUuid']]
                    tagging['position']   = int(matchfragment.group(1))
                    annotations += [tagging]
                elif tagging['Node'] in codeid.keys():
                    tagging['cid']      = codeid[tagging['Node']]
                    tagging['fid']      = sourceid[tagging['SourceUuid']]
                    tagging['selfirst'] = int(matchfragment.group(1))
                    tagging['selend']   = int(matchfragment.group(2))
                    tagging['seltext']  = sourcetext[tagging['SourceUuid']][tagging['selfirst']:tagging['selend']+1]
                    codings += [tagging]
                else:
                    tagging['caseid']   = caseid[tagging['Node']]
                    tagging['fid']      = sourceid[tagging['SourceUuid']]
                    tagging['selfirst'] = int(matchfragment.group(1))
                    tagging['selend']   = int(matchfragment.group(2))
                    caselinkages += [tagging]

            if len(annotations) > 0:
                rqdacon.execute(rqdaannotation.insert(), annotations)
            if len(codings) > 0:
                rqdacon.execute(rqdacoding.insert(), codings)
            if len(caselinkages) > 0:
                rqdacon.execute(rqdacaselinkage.insert(), caselinkages)

# All done.
        rqdatr.commit()
        rqdatr = None
        rqdacon.close()
        rqdadb.dispose()

        normdb.dispose()

    except:
        raise
        if not rqdatr is None:
            rqdatr.rollback()
        rqdadb.dispose()
        normdb.dispose()

###############################################################################

def RQDA2Norm(args):
    # Initialise DB variables so exception handlers don't freak out
    rqdadb = None
    normdb = None
    normtr = None

    try:
        if args.indb != '-':
            rqdadb = create_engine(args.indb)
            rqdamd = MetaData(bind=rqdadb)

            rqdaproject     = Table('project',     rqdamd, autoload=True)
            rqdasource      = Table('source',      rqdamd, autoload=True)
            rqdafileAttr    = Table('fileAttr',    rqdamd, autoload=True)
            rqdafilecat     = Table('filecat',     rqdamd, autoload=True)
            rqdaannotation  = Table('annotation',  rqdamd, autoload=True)
            rqdaattributes  = Table('attributes',  rqdamd, autoload=True)
            rqdacaseAttr    = Table('caseAttr',    rqdamd, autoload=True)
            rqdacaselinkage = Table('caselinkage', rqdamd, autoload=True)
            rqdacases       = Table('cases',       rqdamd, autoload=True)
            rqdacodecat     = Table('codecat',     rqdamd, autoload=True)
            rqdacoding      = Table('coding',      rqdamd, autoload=True)
            rqdafreecode    = Table('freecode',    rqdamd, autoload=True)
            rqdajournal     = Table('journal',     rqdamd, autoload=True)
            rqdatreecode    = Table('treecode',    rqdamd, autoload=True)
            rqdatreefile    = Table('treefile',    rqdamd, autoload=True)
        else:
            rqdadb = None

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

        if rqdadb is None:     # that is, if all we are doing is making an empty norm file
            normdb.dispose()
            return

        normcon = normdb.connect()
        normtr = normcon.begin()

# Function to find or create users
        def find_or_create_user(name):
            user = normcon.execute(select([
                    normUser.c.Id
                ]).where(
                    normUser.c.Name == bindparam('Name')
                ), {
                    'Name': name
                }).first()
            if user is not None:
                return user['Id']
            else:
                userid = uuid.uuid4()
                normcon.execute(normUser.insert(), {
                        'Id': userid,
                        'Name': name
                    })
                return userid

# Project
        if args.project != 'skip':
            if args.verbosity > 0:
                print("Converting project", file=sys.stderr)

            project = dict(rqdadb.execute(select([
                    rqdaproject.c.databaseversion,
                    rqdaproject.c.date,
                    rqdaproject.c.dateM,
                    rqdaproject.c.memo.label('Description'),
                    rqdaproject.c.about.label('Title'),
                    rqdaproject.c.imageDir
                ])).first())

            if project['databaseversion'] != 'DBVersion:0.1':
                raise RuntimeError("Incompatible version of RQDA file: " + project['databaseversion'])

            project['CreatedBy']    = find_or_create_user('Default User')
            project['CreatedDate']  = dateparser.parse(project['date'])
            project['ModifiedBy']   = project['CreatedBy']
            project['ModifiedDate'] = dateparser.parse(project['dateM'])

            normcon.execute(normProject.delete())
            normcon.execute(normProject.insert().values({
                    'Version': '0.2'
                }), project)

# Source categories
        if args.source_categories != 'skip':
            if args.verbosity > 0:
                print("Converting source categories", file=sys.stderr)

            sourcecats  = [dict(row) for row in rqdadb.execute(select([
                    rqdafilecat.c.catid,
                    rqdafilecat.c.name.label('Name'),
                    rqdafilecat.c.memo.label('Description'),
                    rqdafilecat.c.owner,
                    rqdafilecat.c.date,
                    rqdafilecat.c.dateM
                ]).where(
                    rqdafilecat.c.status == literal_column('1')
                ))]

            sourcecatuuid = {}
            for sourcecat in sourcecats:
                sourcecat['Id'] = uuid.uuid4()
                sourcecatuuid[sourcecat['catid']] = sourcecat['Id']
                sourcecat['CreatedBy']    = find_or_create_user(sourcecat['owner'])
                sourcecat['CreatedDate']  = dateparser.parse(sourcecat['date'])
                sourcecat['ModifiedBy']   = sourcecat['CreatedBy']
                sourcecat['ModifiedDate'] = dateparser.parse(sourcecat['dateM'])

            if len(sourcecats) > 0:
                normcon.execute(normSourceCategory.insert(), sourcecats)

# Sources
        if args.sources != 'skip':
            if args.verbosity > 0:
                print("Converting sources", file=sys.stderr)

            sources = [dict(row) for row in rqdadb.execute(select([
                    rqdasource.c.id.label('fid'),
                    rqdasource.c.name.label('Name'),
                    rqdasource.c.memo.label('Description'),
                    rqdasource.c.file.label('Content'),
                    rqdasource.c.owner,
                    rqdasource.c.date,
                    rqdasource.c.dateM
                ]).where(
                    rqdasource.c.status == literal_column('1')
                ))]

            sourceuuid = {}
            for source in sources:
                source['Id'] = uuid.uuid4()
                sourceuuid[source['fid']] = source['Id']
                source['ObjectType'] = 'TXT'
                #source['Object'] = buffer(source['Content'])  # SQLite complains here!!!
                source['CreatedBy']    = find_or_create_user(source['owner'])
                source['CreatedDate']  = dateparser.parse(source['date'])
                source['ModifiedBy']   = source['CreatedBy']
                source['ModifiedDate'] = dateparser.parse(source['dateM'])
                source['Category']     = None
                sourcecats = [dict(row) for row in rqdadb.execute(select([
                        rqdatreefile.c.catid
                    ]).where(
                        rqdatreefile.c.fid == bindparam('fid')
                    ), {
                        'fid': source['fid']
                    })]
                if len(sourcecats) > 1:
                    print("WARNING: Source: " + source['Name'] + " belongs to more than one category. Only first category will be retained", file=sys.stderr)
                if len(sourcecats) > 0:
                    source['Category'] = sourcecatuuid[sourcecats[0]['catid']]

            if len(sources) > 0:
                normcon.execute(normSource.insert(), sources)

# Source attributes
        if args.source_attributes != 'skip':
            if args.verbosity > 0:
                print("Converting source attributes", file=sys.stderr)

            sourcevalues = [dict(row) for row in rqdadb.execute(select([
                    rqdafileAttr.c.variable.label('Name'),
                    rqdafileAttr.c.value.label('Value'),
                    rqdafileAttr.c.fileID.label('fid'),
                    rqdafileAttr.c.owner,
                    rqdafileAttr.c.date,
                    rqdafileAttr.c.dateM,
                    rqdaattributes.c['class'],
                    rqdaattributes.c.memo.label('Description'),
                    rqdaattributes.c.owner.label('attributeowner'),
                    rqdaattributes.c.date.label('attributedate'),
                    rqdaattributes.c.dateM.label('attributedateM')
                ]).where(and_(
                    rqdafileAttr.c.status == literal_column('1'),
                    rqdaattributes.c.name == rqdafileAttr.c.variable
                )))]

            sourceattributeuuid = {}
            for sourcevalue in sourcevalues:
                sourcevalue['Source']       = sourceuuid[sourcevalue['fid']]
                sourcevalue['CreatedBy']    = find_or_create_user(sourcevalue['owner'])
                sourcevalue['CreatedDate']  = dateparser.parse(sourcevalue['date'])
                sourcevalue['ModifiedBy']   = sourcevalue['CreatedBy']
                sourcevalue['ModifiedDate'] = dateparser.parse(sourcevalue['dateM'])

                if sourcevalue['Name'] not in sourceattributeuuid.keys():
                    sourceattribute = normcon.execute(select([
                            normSourceAttribute.c.Id
                        ]).where(
                            normSourceAttribute.c.Name == bindparam('Name')
                        ), sourcevalue).first()
                    if sourceattribute is None:
                        sourceattributeuuid[sourcevalue['Name']] = uuid.uuid4()
                        sourcevalue['AttributeOwner']    = find_or_create_user(sourcevalue['attributeowner'])
                        sourcevalue['AttributeCreatedDate']  = dateparser.parse(sourcevalue['attributedate'])
                        sourcevalue['AttributeModifiedDate'] = dateparser.parse(sourcevalue['attributedateM'])

                        normcon.execute(normSourceAttribute.insert().values({
                                'Id': sourceattributeuuid[sourcevalue['Name']],
                                'Type': 'Decimal' if sourcevalue['class'] == 'numeric' else 'Text',
                                'CreatedBy': sourcevalue['AttributeOwner'],
                                'CreatedDate': sourcevalue['AttributeCreatedDate'],
                                'ModifiedBy': sourcevalue['AttributeOwner'],
                                'ModifiedDate': sourcevalue['AttributeModifiedDate']
                            }), sourcevalue)
                    else:
                        sourceattributeuuid[sourcevalue['Name']] = sourceattribute['Id']

                sourcevalue['Attribute'] = sourceattributeuuid[sourcevalue['Name']]

            if len(sourcevalues) > 0:
                normcon.execute(normSourceValue.insert(), sourcevalues)

# Node categories
        if args.node_categories != 'skip':
            if args.verbosity > 0:
                print("Converting node categories", file=sys.stderr)

            nodecats  = [dict(row) for row in rqdadb.execute(select([
                    rqdacodecat.c.catid,
                    rqdacodecat.c.name.label('Name'),
                    rqdacodecat.c.memo.label('Description'),
                    rqdacodecat.c.owner,
                    rqdacodecat.c.date,
                    rqdacodecat.c.dateM
                ]).where(
                    rqdacodecat.c.status == literal_column('1')
                ))]

            nodecatuuid = {}
            for nodecat in nodecats:
                nodecat['Id'] = uuid.uuid4()
                nodecatuuid[nodecat['catid']] = nodecat['Id']
                nodecat['CreatedBy']    = find_or_create_user(nodecat['owner'])
                nodecat['CreatedDate']  = dateparser.parse(nodecat['date'])
                nodecat['ModifiedBy']   = nodecat['CreatedBy']
                nodecat['ModifiedDate'] = dateparser.parse(nodecat['dateM'])

            if len(nodecats) > 0:
                normcon.execute(normNodeCategory.insert(), nodecats)

# Nodes
        if args.nodes != 'skip':
            if args.verbosity > 0:
                print("Converting nodes", file=sys.stderr)

            nodes  = [dict(row) for row in rqdadb.execute(select([
                    rqdafreecode.c.id.label('cid'),
                    rqdafreecode.c.name.label('Name'),
                    rqdafreecode.c.memo.label('Description'),
                    rqdafreecode.c.owner,
                    rqdafreecode.c.date,
                    rqdafreecode.c.dateM
                ]).where(
                    rqdafreecode.c.status == literal_column('1')
                ))]

            nodeuuid = {}
            for node in nodes:
                node['Id'] = uuid.uuid4()
                nodeuuid[node['cid']] = node['Id']
                node['CreatedBy']    = find_or_create_user(node['owner'])
                node['CreatedDate']  = dateparser.parse(node['date'])
                node['ModifiedBy']   = node['CreatedBy']
                node['ModifiedDate'] = dateparser.parse(node['dateM'])
                node['Category']     = None
                nodecats = [dict(row) for row in rqdadb.execute(select([
                        rqdatreecode.c.catid
                    ]).where(
                        rqdatreecode.c.cid == bindparam('cid')
                    ), {
                        'cid': node['cid']
                    })]
                if len(nodecats) > 1:
                    print("WARNING: Node: " + node['Name'] + " belongs to more than one category. Only first category will be retained", file=sys.stderr)
                if len(nodecats) > 0:
                    node['Category'] = nodecatuuid[nodecats[0]['catid']]

            if len(nodes) > 0:
                normcon.execute(normNode.insert(), nodes)

# Cases
        if args.cases != 'skip':
            if args.verbosity > 0:
                print("Converting cases", file=sys.stderr)

            cases  = [dict(row) for row in rqdadb.execute(select([
                    rqdacases.c.id.label('caseid'),
                    rqdacases.c.name.label('Name'),
                    rqdacases.c.memo.label('Description'),
                    rqdacases.c.owner,
                    rqdacases.c.date,
                    rqdacases.c.dateM
                ]).where(
                    rqdacases.c.status == literal_column('1')
                ))]

            caseuuid = {}
            for case in cases:
                case['Id'] = uuid.uuid4()
                caseuuid[case['caseid']] = case['Id']
                case['CreatedBy']    = find_or_create_user(case['owner'])
                case['CreatedDate']  = dateparser.parse(case['date'])
                case['ModifiedBy']   = case['CreatedBy']
                case['ModifiedDate'] = dateparser.parse(case['dateM'])

            if len(cases) > 0:
                normcon.execute(normNode.insert(), cases)

# Case attributes
        if args.case_attributes != 'skip':
            if args.verbosity > 0:
                print("Converting case attributes", file=sys.stderr)

            casevalues = [dict(row) for row in rqdadb.execute(select([
                    rqdacaseAttr.c.variable.label('Name'),
                    rqdacaseAttr.c.value.label('Value'),
                    rqdacaseAttr.c.caseID.label('caseid'),
                    rqdacaseAttr.c.owner,
                    rqdacaseAttr.c.date,
                    rqdacaseAttr.c.dateM,
                    rqdaattributes.c['class'],
                    rqdaattributes.c.memo.label('Description'),
                    rqdaattributes.c.owner.label('attributeowner'),
                    rqdaattributes.c.date.label('attributedate'),
                    rqdaattributes.c.dateM.label('attributedateM')
                ]).where(and_(
                    rqdacaseAttr.c.status == literal_column('1'),
                    rqdaattributes.c.name == rqdacaseAttr.c.variable
                )))]

            caseattributeuuid = {}
            for casevalue in casevalues[:]: # Take copy of list so we can remove items
                casevalue['Node']                  = caseuuid[casevalue['caseid']]
                casevalue['CreatedBy']             = find_or_create_user(casevalue['owner'])
                casevalue['CreatedDate']           = dateparser.parse(casevalue['date'])
                casevalue['ModifiedBy']            = casevalue['CreatedBy']
                casevalue['ModifiedDate']          = dateparser.parse(casevalue['dateM'])
                casevalue['AttributeOwner']        = find_or_create_user(casevalue['attributeowner'])
                casevalue['AttributeCreatedDate']  = dateparser.parse(casevalue['attributedate'])
                casevalue['AttributeModifiedDate'] = dateparser.parse(casevalue['attributedateM'])

                # Catch reserved attribute 'Category'
                if casevalue['Name'] == 'Category':
                    # See if the category already exists
                    nodecat = normcon.execute(select([
                            normNodeCategory.c.Id
                        ]).where(
                            normNodeCategory.c.Name == bindparam('Name')
                        ), {
                            'Name': casevalue['Value']
                        }).first()
                    if nodecat is not None:
                        catuuid = nodecat['Id']
                    else:
                        catuuid = uuid.uuid4()
                        normcon.execute(normNodeCategory.insert(), {
                                'Id':           catuuid,
                                'Name':         casevalue['Value'],
                                'Description':  'Category created from RQDA node attribute',
                                'CreatedBy':    casevalue['AttributeOwner'],
                                'CreatedDate':  casevalue['AttributeCreatedDate'],
                                'ModifiedBy':   casevalue['AttributeOwner'],
                                'ModifiedDate': casevalue['AttributeModifiedDate']
                            })

                    normcon.execute(normNode.update(
                            normNode.c.Id == bindparam('Node')
                        ), {
                            'Node':     caseuuid[casevalue['caseid']],
                            'Category': nodecat['Id']
                        })

                    casevalues.remove(casevalue)
                else:
                    if casevalue['Name'] not in caseattributeuuid.keys():
                        caseattribute = normcon.execute(select([
                                normNodeAttribute.c.Id
                            ]).where(
                                normNodeAttribute.c.Name == bindparam('Name')
                            ), casevalue).first()
                        if caseattribute is None:
                            caseattributeuuid[casevalue['Name']] = uuid.uuid4()

                            normcon.execute(normNodeAttribute.insert().values({
                                    'Id': caseattributeuuid[casevalue['Name']],
                                    'Type': 'Decimal' if casevalue['class'] == 'numeric' else 'Text',
                                    'CreatedBy': casevalue['AttributeOwner'],
                                    'CreatedDate': casevalue['AttributeCreatedDate'],
                                    'ModifiedBy': casevalue['AttributeOwner'],
                                    'ModifiedDate': casevalue['AttributeModifiedDate']
                                }), casevalue)
                        else:
                            caseattributeuuid[casevalue['Name']] = caseattribute['Id']

                    casevalue['Attribute'] = caseattributeuuid[casevalue['Name']]

            if len(casevalues) > 0:
                normcon.execute(normNodeValue.insert(), casevalues)

# Annotations, codings and case linkages
        if args.taggings != 'skip':
            if args.verbosity > 0:
                print("Converting annotations, codings and case linkages", file=sys.stderr)

            taggings = []

            annotations = [dict(row) for row in rqdadb.execute(select([
                    rqdaannotation.c.fid,
                    rqdaannotation.c.position.label('StartX'),
                    rqdaannotation.c.annotation.label('Memo'),
                    rqdaannotation.c.owner,
                    rqdaannotation.c.date,
                    rqdaannotation.c.dateM
                ]).where(
                    rqdaannotation.c.status == literal_column('1')
                ))]
            for annotation in annotations:
                annotation.update({
                        'Node': None,
                        'EndX': annotation['StartX']
                    })
                taggings += [annotation]

            codings = [dict(row) for row in rqdadb.execute(select([
                    rqdacoding.c.cid,
                    rqdacoding.c.fid,
                    rqdacoding.c.selfirst.label('StartX'),
                    rqdacoding.c.selend.label('EndX'),
                    rqdacoding.c.memo.label('Memo'),
                    rqdacoding.c.owner,
                    rqdacoding.c.date
                ]).where(
                    rqdacoding.c.status == literal_column('1')
                ))]
            for coding in codings:
                coding.update({
                        'Node':  nodeuuid[coding['cid']],
                        'dateM': coding['date']
                    })
                taggings += [coding]

            caselinkages = [dict(row) for row in rqdadb.execute(select([
                    rqdacaselinkage.c.caseid,
                    rqdacaselinkage.c.fid,
                    rqdacaselinkage.c.selfirst.label('StartX'),
                    rqdacaselinkage.c.selend.label('EndX'),
                    rqdacaselinkage.c.memo.label('Memo'),
                    rqdacaselinkage.c.owner,
                    rqdacaselinkage.c.date
                ]).where(
                    rqdacaselinkage.c.status == literal_column('1')
                ))]
            for caselinkage in caselinkages:
                caselinkage.update({
                        'Node':  caseuuid[caselinkage['caseid']],
                        'dateM': caselinkage['date']
                    })
                taggings += [caselinkage]

            for tagging in taggings:
                tagging['Id']           = uuid.uuid4()
                tagging['Source']       = sourceuuid[tagging['fid']]
                tagging['Fragment']     = str(int(tagging['StartX'])) + ':' + str(int(tagging['EndX']))
                tagging['CreatedBy']    = find_or_create_user(tagging['owner'])
                tagging['CreatedDate']  = dateparser.parse(tagging['date'])
                tagging['ModifiedBy']   = tagging['CreatedBy']
                tagging['ModifiedDate'] = dateparser.parse(tagging['dateM'])

            if len(taggings) > 0:
                normcon.execute(normTagging.insert(), taggings)

# All done.
        normtr.commit()
        normtr = None
        normcon.close()
        normdb.dispose()

        rqdadb.dispose()

    except:
        raise
        if not normtr is None:
            normtr.rollback()
        normdb.dispose()
        rqdadb.dispose()
