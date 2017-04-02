#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import *
from sqlalchemy import exc
import warnings
import sys
import os
import argparse
import uuid
from dateutil import parser as dateparser
import datetime

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

db = None
con = None
tr = None
try:
    parser = argparse.ArgumentParser(description='Adjust all CreatedDate/ModifiedDate columns in a database before a specified date by a given number of days.')

    parser.add_argument('db', type=str,
                        help='SQLAlchemy path of database from which to delete data.')
    parser.add_argument('--before', type=str, required=True,
                        help='Adjust all records with created date before this date.')
    parser.add_argument('--adjust', type=int, required=True,
                        help='Number of days to adjust dates.')

    args = parser.parse_args()

    args.before = dateparser.parse(args.before).date().isoformat()
    adjust = datetime.timedelta(days=args.adjust)

    mssql.ischema_names['xml'] = UUID

    db = create_engine(args.db)
    md = MetaData(bind=db)
    md.reflect(db)
    con = db.connect()
    tr = con.begin()

    for table in md.sorted_tables:
        CreatedDate  = table.c.get('CreatedDate')
        ModifiedDate = table.c.get('ModifiedDate')
        if CreatedDate is not None:
            print ("Table " + table.name)
            # Prepend columns with '_' to avoid bindparam conflict error with reserved names
            rows = [{'_'+key:value for key,value in dict(row).iteritems()} for row in con.execute(
                select(
                    table.primary_key.columns + [CreatedDate, ModifiedDate]).where(
                    table.c.CreatedDate  <= args.before))]

            keycondition = [column == bindparam('_'+column.name) for column in table.primary_key.columns]

            print ("Updating " + str(len(rows)) + " rows.")
            for row in rows:
                row['_ModifiedDate'] += adjust
                row['_CreatedDate']  += adjust

                con.execute(table.update(
                    and_(*keycondition)).values({
                        'CreatedDate':  bindparam('_CreatedDate'),
                        'ModifiedDate': bindparam('_ModifiedDate')}), row)

    tr.commit()

except:
    raise
    if tr:
        tr.rollback()

finally:
    if con:
        con.close()
    if db:
        db.dispose()
