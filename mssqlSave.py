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

from __future__ import print_function
import argparse
from mssqlTools import mssqlAPI

def mssqlSave(arglist):
    parser = argparse.ArgumentParser(description='Detach a database and save database file from an MS SQL Server instance.')

    parser.add_argument('-v', '--verbosity', type=int, default=1)

    parser.add_argument('-nv', '--nvivoversion', choices=["10", "11"], default="10",
                        help='NVivo version (10 or 11)')

    parser.add_argument('-S', '--server', type=str,
                        help="IP address/name of Microsoft SQL Server")
    parser.add_argument('-P', '--port', type=int,
                        help="Port of Microsoft SQL Server")
    parser.add_argument('-i', '--instance', type=str,
                        help="Microsoft SQL Server instance")

    parser.add_argument('filename', type=str,
                        help="File to save as")
    parser.add_argument('dbname',   type=str,
                        help="Name to assign database")

    args = parser.parse_args(arglist)

    api = mssqlAPI(args.server,
                   args.port,
                   args.instance,
                   version = ('MSSQL12' if args.nvivoversion == '11' else 'MSSQL10_50'),
                   verbosity = args.verbosity)

    api.save(args.filename, args.dbname)

if __name__ == '__main__':
    mssqlSave(None)
