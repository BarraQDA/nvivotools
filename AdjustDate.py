#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import NVivo
from sqlalchemy import *
from sqlalchemy import exc
import warnings
import sys
import os
import argparse
import re
from dateutil import parser as dateparser
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

db = None
con = None
tr = None
try:
    parser = argparse.ArgumentParser(description='Adjust all CreatedDate/ModifiedDate columns in a database before a specified date by a given number of days. Simultateouly correct ModifiedDate to be no earlier than CreatedDate.')

    parser.add_argument('db',        type=str,
                        help='SQLAlchemy path of database from which to delete data.')
    parser.add_argument('--before',  type=str, required=True,
                        help='Adjust all records with created date before this date.')
    parser.add_argument('--adjust',  type=str, required=True,
                        help='Time delta to adjust time forward by, for example "3 days 2 hours"')

    parser.add_argument('--dry-run',         action='store_true', help='Print but do not execute command')

    args = parser.parse_args()

    before = dateparser.parse(args.before)
    adjust = timedelta(seconds=timeparse(args.adjust))

    if '://' not in args.db:
        args.db = NVivo.mount(args.db)

    db = create_engine(args.db)
    md = MetaData(bind=db)
    md.reflect(db)
    con = db.connect()
    tr = con.begin()

    datetimeNow = datetime.utcnow()

    for table in md.sorted_tables:
        CreatedDate  = table.c.get('CreatedDate')
        ModifiedDate = table.c.get('ModifiedDate')
        if CreatedDate is not None and ModifiedDate is not None:
            # Prepend columns with '_' to avoid bindparam conflict error with reserved names
            rows = [{'_'+key:value for key,value in dict(row).items()} for row in con.execute(
                select(
                    table.primary_key.columns + [CreatedDate, ModifiedDate]).where(or_(
                    table.c.CreatedDate <= bindparam('Before'),
                    table.c.CreatedDate >  table.c.ModifiedDate)), {
                        'Before': before
                    })]

            keycondition = [column == bindparam('_'+column.name) for column in table.primary_key.columns]

            print ("Table " + table.name + " Updating " + str(len(rows)) + " rows.")
            for row in rows:
                if type(row['_CreatedDate']) == str:    # ???
                    createdDate  = dateparser.parse(row['_CreatedDate'])
                    modifiedDate = dateparser.parse(row['_ModifiedDate'])
                else:
                    createdDate  = row['_CreatedDate']
                    modifiedDate = row['_ModifiedDate']
                if createdDate <= before:
                    createdDate  += adjust
                if modifiedDate <= before:
                    modifiedDate += adjust
                if createdDate > datetimeNow or modifiedDate > datetimeNow:
                    print("WARNING: future date", file=sys.stderr)

                if createdDate > modifiedDate:
                    print("WARNING: created date", createdDate, "later than modified date", modifiedDate, file=sys.stderr)
                    #row['_ModifiedDate'] = row['_CreatedDate']

                row['_CreatedDate']  = createdDate
                row['_ModifiedDate'] = modifiedDate

                if not args.dry_run:
                    con.execute(table.update(
                        and_(*keycondition)).values({
                            'CreatedDate':  bindparam('_CreatedDate'),
                            'ModifiedDate': bindparam('_ModifiedDate')}), row)

    if not args.dry_run:
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
