#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Jonathan Schultz
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

from argrecord import ArgumentHelper, ArgumentRecorder
import os
import sys
from NVivoNorm import NVivoNorm
from sqlalchemy import *
import re
from dateutil import parser as dateparser
from datetime import date, time, datetime
from distutils import util
import uuid

def editProject(arglist=None):

    parser = ArgumentRecorder(description='Insert or update project in normalised file.')

    generalgroup = parser.add_argument_group('General')
    generalgroup.add_argument('-o', '--outfile',     type=str, required=True, output=True,
                                                     help='Output normalised NVivo (.norm) file')
    generalgroup.add_argument('-t', '--title',       type=str,
                                                     required=True)
    generalgroup.add_argument('-d', '--description', type=str)
    generalgroup.add_argument('-u', '--user',        type=str,
                              help='User, default is first user from user table')

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity',  type=int, default=1,
                                                     private=True)
    advancedgroup.add_argument('--logfile',          type=str, help="Logfile, default is <outfile>.log",
                                                     private=True)
    advancedgroup.add_argument('--no-logfile',       action='store_true', help='Do not output a logfile')

    args = parser.parse_args(arglist)

    if not args.no_logfile:
        logfilename = args.outfile.rsplit('.',1)[0] + '.log'
        incomments = ArgumentHelper.read_comments(logfilename) or ArgumentHelper.separator()
        logfile = open(logfilename, 'w')
        parser.write_comments(args, logfile, incomments=incomments)
        logfile.close()

    try:

        norm = NVivoNorm(args.outfile)
        norm.begin()

        if args.user:
            userRecord = norm.con.execute(select([
                    norm.User.c.Id
                ]).where(
                    norm.User.c.Name == bindparam('Name')
                ), {
                    'Name': args.user
                }).first()
            if userRecord:
                userId = userRecord['Id']
            else:
                userId = uuid.uuid4()
                norm.con.execute(norm.User.insert(), {
                        'Id':   userId,
                        'Name': args.user
                    })
        else:
            userRecord = norm.con.execute(select([
                    norm.User.c.Id
                ])).first()
            if userRecord is not None:
                userId = userRecord['Id']
            else:
                raise RuntimeError("No user on command line or user file")

        project = norm.con.execute(select([
                    norm.Project.c.Title
                ])).first()

        datetimeNow = datetime.utcnow()

        projectColumns = {'Version': u'0.2'}
        if args.title:
            projectColumns.update({'Title': args.title})
        if args.description:
            projectColumns.update({'Description': args.description})
        projectColumns.update({'ModifiedBy':   userId,
                               'ModifiedDate': datetimeNow})

        if project is None:    # New project
            projectColumns.update({'CreatedBy':   userId,
                                   'CreatedDate': datetimeNow})
            norm.con.execute(norm.Project.insert(), projectColumns)
        else:
            norm.con.execute(norm.Project.update(), projectColumns)

        norm.commit()

    except:
        raise
        norm.rollback()

    finally:
        del norm

if __name__ == '__main__':
    editProject(None)
