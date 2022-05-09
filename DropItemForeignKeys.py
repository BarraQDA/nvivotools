#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import exc, TypeDecorator, CHAR, String, create_engine, MetaData, bindparam
from sqlalchemy.engine import reflection
import warnings
import sys
import os
import argparse
import uuid
from sqlalchemy.schema import (
    MetaData,
    Table,
    DropTable,
    ForeignKeyConstraint,
    DropConstraint,
    )

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

try:
    parser = argparse.ArgumentParser(description='Drop certain foreign keys.')
    parser.add_argument('database', type=str)
    args = parser.parse_args()

    databasedb = create_engine(args.database)
    databaseconn = databasedb.connect()
    databasetrans = databaseconn.begin()

    inspector = reflection.Inspector.from_engine(databasedb)
    databasemd = MetaData()

    all_fks = []

    for table_name in inspector.get_table_names():
        #if table_name != 'Item' and table_name != 'Project':
            #continue

        fks = []
        for fk in inspector.get_foreign_keys(table_name):
            if not fk['name']:
                continue
            fks.append(
                ForeignKeyConstraint((),(),name=fk['name'])
                )
        t = Table(table_name,databasemd,*fks)
        all_fks.extend(fks)

    for fkc in all_fks:
        databaseconn.execute(DropConstraint(fkc))

    databasetrans.commit()

except exc.SQLAlchemyError:
    raise
