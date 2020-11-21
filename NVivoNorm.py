#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Jonathan Schultz
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

import os
from sqlalchemy import *
from sqlalchemy import exc
import uuid

exec(open(os.path.dirname(os.path.realpath(__file__)) + os.path.sep + 'DataTypes.py').read())

class NVivoNorm(object):

    def __init__(self, path):
        try:
            self.db  = create_engine('sqlite:///' + path)
            self.md  = MetaData(bind=self.db)
            self.con = self.db.connect()
            self.tr  = None
        except:
            raise

        # Load or create the database
        try:
            self.User = Table('User', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.User = Table('User', self.md,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          UnicodeText(256)))
            self.User.create(self.db)

        try:
            self.Project = Table('Project', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.Project = Table('Project', self.md,
                Column('Version',       UnicodeText(16)),
                Column('Title',         UnicodeText(256),                            nullable=False),
                Column('Description',   UnicodeText(2048)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id"),  nullable=False),
                Column('CreatedDate',   DateTime,                               nullable=False),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id"),  nullable=False),
                Column('ModifiedDate',  DateTime,                               nullable=False))
            self.Project.create(self.db)

        try:
            self.NodeCategory = Table('NodeCategory', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.NodeCategory = Table('NodeCategory', self.md,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          UnicodeText(256)),
                Column('Description',   UnicodeText(512)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            self.NodeCategory.create(self.db)

        try:
            self.Node = Table('Node', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.Node = Table('Node', self.md,
                Column('Id',            UUID(),         primary_key=True),
                Column('Parent',        UUID(),         ForeignKey("Node.Id")),
                Column('Category',      UUID(),         ForeignKey("NodeCategory.Id")),
                Column('Name',          UnicodeText(256)),
                Column('Description',   UnicodeText(512)),
                Column('Color',         Integer),
                Column('Aggregate',     Boolean),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            self.Node.create(self.db)

        try:
            self.NodeAttribute = Table('NodeAttribute', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.NodeAttribute = Table('NodeAttribute', self.md,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          UnicodeText(256)),
                Column('Description',   UnicodeText(512)),
                Column('Type',          UnicodeText(16)),
                Column('Length',        Integer),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            self.NodeAttribute.create(self.db)

        try:
            self.NodeValue = Table('NodeValue', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.NodeValue = Table('NodeValue', self.md,
                Column('Node',          UUID(),         ForeignKey("Node.Id"),      primary_key=True),
                Column('Attribute',     UUID(),         ForeignKey("NodeAttribute.Id"),
                                                                                    primary_key=True),
                Column('Value',         UnicodeText(256)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            self.NodeValue.create(self.db)

        try:
            self.SourceCategory = Table('SourceCategory', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.SourceCategory = Table('SourceCategory', self.md,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          UnicodeText(256)),
                Column('Description',   UnicodeText(512)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            self.SourceCategory.create(self.db)

        try:
            self.Source = Table('Source', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.Source = Table('Source', self.md,
                Column('Id',            UUID(),         primary_key=True),
                Column('Category',      UUID(),         ForeignKey("SourceCategory.Id")),
                Column('Name',          UnicodeText(256)),
                Column('Description',   UnicodeText(512)),
                Column('Color',         Integer),
                Column('Content',       UnicodeText(16384)),
                Column('ObjectType',    UnicodeText(256)),
                Column('SourceType',    Integer),
                Column('Object',        LargeBinary),
                Column('Thumbnail',     LargeBinary),
            #Column('Waveform',      LargeBinary,    nullable=False),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            self.Source.create(self.db)

        try:
            self.SourceAttribute = Table('SourceAttribute', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.SourceAttribute = Table('SourceAttribute', self.md,
                Column('Id',            UUID(),         primary_key=True),
                Column('Name',          UnicodeText(256)),
                Column('Description',   UnicodeText(512)),
                Column('Type',          UnicodeText(16)),
                Column('Length',        Integer),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            self.SourceAttribute.create(self.db)

        try:
            self.SourceValue = Table('SourceValue', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.SourceValue = Table('SourceValue', self.md,
                Column('Source',        UUID(),         ForeignKey("Source.Id"),    primary_key=True),
                Column('Attribute',     UUID(),         ForeignKey("SourceAttribute.Id"),
                                                                                    primary_key=True),
                Column('Value',         UnicodeText(256)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            self.SourceValue.create(self.db)

        try:
            self.Tagging = Table('Tagging', self.md, autoload=True)
        except exc.NoSuchTableError:
            self.Tagging = Table('Tagging', self.md,
                Column('Id',            UUID(),         primary_key=True),
                Column('Source',        UUID(),         ForeignKey("Source.Id")),
                Column('Node',          UUID(),         ForeignKey("Node.Id")),
                Column('Fragment',      UnicodeText(256)),
                Column('Memo',          UnicodeText(256)),
                Column('CreatedBy',     UUID(),         ForeignKey("User.Id")),
                Column('CreatedDate',   DateTime),
                Column('ModifiedBy',    UUID(),         ForeignKey("User.Id")),
                Column('ModifiedDate',  DateTime))
            self.Tagging.create(self.db)

    def __del__(self):
        if self.tr:
            self.tr.rollback()
        #self.con.close()
        self.db.dispose()

    def begin(self):
        self.tr  = self.con.begin()

    def commit(self):
        if self.tr:
            self.tr.commit()
            self.tr = None

    def rollback(self):
        if self.tr:
            self.tr.rollback()
            self.tr = None
