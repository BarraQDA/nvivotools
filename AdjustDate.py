#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import *
from sqlalchemy import exc
import warnings
import sys
import os
import argparse
import re
from dateutil import parser as dateparser
import datetime

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
                        help='Time delta to adjust time forward by. Format is "<w>w <d>d <h>h <m>m <s>s"')
    parser.add_argument('--dry-run', action='store_true', help='Print but do not execute command')

    args = parser.parse_args()

    args.before = dateparser.parse(args.before)

    adjust = re.search(r'(?:(?P<weeks>\d+?)w)?\s*(?:(?P<days>\d+?)d)?\s*(?:(?P<hours>\d+?)h)?\s*(?:(?P<minutes>\d+?)m)?\s*((?P<seconds>\d+?)s)?', args.adjust, re.UNICODE | re.IGNORECASE)
    adjust = datetime.timedelta(weeks=int(adjust.group('weeks') or '0'),
                                days=int(adjust.group('days') or '0'),
                                hours=int(adjust.group('hours') or '0'),
                                minutes=int(adjust.group('minutes') or '0'),
                                seconds=int(adjust.group('seconds') or '0'))

    db = create_engine(args.db)
    md = MetaData(bind=db)
    md.reflect(db)
    con = db.connect()
    tr = con.begin()

    for table in md.sorted_tables:
        CreatedDate  = table.c.get('CreatedDate')
        ModifiedDate = table.c.get('ModifiedDate')
        if CreatedDate is not None and ModifiedDate is not None:
            print ("Table " + table.name)
            # Prepend columns with '_' to avoid bindparam conflict error with reserved names
            rows = [{'_'+key:value for key,value in dict(row).iteritems()} for row in con.execute(
                select(
                    table.primary_key.columns + [CreatedDate, ModifiedDate]).where(or_(
                    table.c.CreatedDate <= args.before,
                    table.c.CreatedDate >  table.c.ModifiedDate)))]

            keycondition = [column == bindparam('_'+column.name) for column in table.primary_key.columns]

            print ("Updating " + str(len(rows)) + " rows.")
            if not args.dry_run:
                for row in rows:
                    if row['_CreatedDate'] <= args.before:
                        row['_CreatedDate']  += adjust
                    if row['_ModifiedDate'] <= args.before:
                        row['_ModifiedDate'] += adjust
                    if row['_CreatedDate'] > row['_ModifiedDate']:
                        row['_ModifiedDate'] = row['_CreatedDate']

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
