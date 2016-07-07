#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
import sys

import sqlalchemy
from sqlalchemy import *
from sqlalchemy import exc
import warnings
import sys
import os
import argparse

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
    print updateSql
    for project in projectRows:
        project['Title']       = ''.join(map(lambda ch: unichr(ord(ch) + charoffset), project['Title']))
        project['Description'] = ''.join(map(lambda ch: unichr(ord(ch) + charoffset), project['Description']))
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
    print updateSql
    for item in itemRows:
        item['Name']       = ''.join(map(lambda ch: unichr(ord(ch) + charoffset), item['Name']))
        item['Description'] = ''.join(map(lambda ch: unichr(ord(ch) + charoffset), item['Description']))
        nvivodb.execute(updateSql, item)

    sys.exit()
    print "Translating " + sys.argv[1]
    con = sqlite3.connect(sys.argv[1])
    con.row_factory = sqlite3.Row

    cur = con.cursor()
    old_rows = cur.execute('SELECT Title, Description, Id FROM Project')
    new_rows = []
    for row in old_rows:
        new_rows.append ( { "Title"       : translate(row["Title"]),
                            "Description" : translate(row["Description"]),
                            "Id"          : row["Id"] } )

    cur.executemany('UPDATE Project SET Title=:Title, Description=:Description WHERE Id=:Id', new_rows)

    cur = con.cursor()
    old_rows = cur.execute('SELECT Name, Description, Id FROM Item')
    new_rows = []
    for row in old_rows:
        new_rows.append ( { "Name"        : translate(row["Name"]),
                            "Description" : translate(row["Description"]),
                            "Id"          : row["Id"] } )

    cur.executemany('UPDATE Item SET Name=:Name, Description=:Description WHERE Id=:Id', new_rows)

    con.commit()
    cur.close()

    con.close()

except exc.SQLAlchemyError:
    raise
