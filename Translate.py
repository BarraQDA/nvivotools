#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import *
from sqlalchemy import exc
from sqlalchemy.engine import reflection
import warnings
import sys
import os
import argparse
import uuid

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

try:
    parser = argparse.ArgumentParser(description='Translate NVivo encoded strings.')

    parser.add_argument('-r', '--reverse', action='store_true',
                        help='Reverse translate, that is from clear text to NVivo encoded strings.')
    parser.add_argument('infile', type=str,
                        help='SQLAlchemy path of NVivo database.')

    args = parser.parse_args()

    nvivodb = create_engine(args.infile)
    nvivomd = MetaData(bind=nvivodb)
    nvivomd.reflect(nvivodb)

    if args.reverse:
        charoffset = +0x377
    else:
        charoffset = -0x377

    nvivoProject = nvivomd.tables.get('Project')
    projectSel = select([
            nvivoProject.c.Id.label('b_Id'),    # For some reason SQLAlchemy won't allow 'Id'
            nvivoProject.c.Title,
            nvivoProject.c.Description
        ])
    projectRows = [ dict(row) for row in nvivodb.execute(projectSel) ]
    updateSql = update(nvivoProject) \
                .where(nvivoProject.c.Id == bindparam('b_Id')) \
                .values(Title = bindparam('Title')) \
                .values(Description = bindparam('Description'))
    for project in projectRows:
        project['Title']       = u''.join(map(lambda ch: unichr(ord(ch) + charoffset), project['Title']))
        project['Description'] = u''.join(map(lambda ch: unichr(ord(ch) + charoffset), project['Description']))
        nvivodb.execute(updateSql, project)

    nvivoItem = nvivomd.tables.get('Item')
    itemSel = select([
            nvivoItem.c.Id.label('b_Id'),    # For some reason SQLAlchemy won't allow 'Id'
            nvivoItem.c.Name,
            nvivoItem.c.Description
        ])
    itemRows = [ dict(row) for row in nvivodb.execute(itemSel) ]
    updateSql = update(nvivoItem) \
                .where(nvivoItem.c.Id == bindparam('b_Id')) \
                .values(Name = bindparam('Name')) \
                .values(Description = bindparam('Description'))
    for item in itemRows:
        item['Name']       = u''.join(map(lambda ch: unichr(ord(ch) + charoffset), item['Name']))
        item['Description'] = u''.join(map(lambda ch: unichr(ord(ch) + charoffset), item['Description']))
        nvivodb.execute(updateSql, item)

except exc.SQLAlchemyError:
    raise
