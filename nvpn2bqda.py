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

import os
import sys
from argrecord import ArgumentHelper, ArgumentRecorder
from NVivoNorm import NVivoNorm
from sqlalchemy import *
import re
import csv
import shutil
import re

def nvpn2bqda(arglist=None):
    parser = ArgumentRecorder(description='Export NVPN to BarraQDA project.',
                              fromfile_prefix_chars='@')

    parser.description = "Export NVPN to BarraQDA project."

    generalgroup = parser.add_argument_group('General')
    generalgroup.add_argument('-ob', '--outbase', type=str, help='Output base path')
    generalgroup.add_argument('infile',   type=str, help='Input NVPN file', input=True)

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity',  type=int, default=1, private=True)
    advancedgroup.add_argument('--logfile',          type=str, help="Logfile")

    args = parser.parse_args(arglist)

    if args.logfile:
        logfile = open(logfilename, 'w')
        parser.write_comments(args, logfile, incomments=ArgumentHelper.separator())
        logfile.close()

    try:
        norm = NVivoNorm(args.infile)
        norm.begin()
        
        docdata = os.path.join(args.outbase, 'docdata')
        if not os.path.exists(docdata):
            os.mkdir(docdata)

        sourcesel = select([
                norm.Source.c.Id,
                norm.Source.c.Name,
                norm.Source.c.ObjectType,
                norm.Source.c.Object
            ])
        nodesel = select([
                norm.Node.c.Id,
                norm.Node.c.Name,
            ])
        nodeattrsel = select([
                norm.NodeAttribute.c.Name.label('Attribute'),
                norm.NodeValue.c.Value
            ]).select_from(
                norm.NodeAttribute.join(
                norm.NodeValue,
                norm.NodeValue.c.Attribute == norm.NodeAttribute.c.Id
            )).where(norm.NodeValue.c.Node == bindparam('Node'))
        taggingsel = select([
                norm.Tagging.c.Node,
                norm.Tagging.c.Fragment,
                norm.Tagging.c.Memo,
                norm.Tagging.c.ModifiedDate,
                norm.Tagging.c.CreatedDate,
                norm.User.c.Name.label('User')
            ]).select_from(
                norm.Tagging.join(
                norm.User,
                norm.User.c.Id == norm.Tagging.c.ModifiedBy
            )).where(
                norm.Tagging.c.Source == bindparam('Source'),
            )                  
        fragmentregex = re.compile(r'(?P<start>[0-9]+):(?P<end>[0-9]+)')
                               
        for source in norm.con.execute(sourcesel):
            sourcefilename = os.path.join(args.outbase, source['Name'] + '.' + source['ObjectType'].lower())
            sourcefile = open(sourcefilename, 'wb')
            sourcefile.write(source['Object'])
            sourcefile.close()
            docfilename = os.path.join(docdata, str(len(source['Object'])) + '.' + source['Name'] + '.' + source['ObjectType'].lower() + '.xml')
            docfile = open(docfilename, 'w')
            docfile.write("""<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE documentInfo>
<documentInfo url=\"""" + os.path.realpath(sourcefilename) + """\">
 <QDA>
""")
            for node in norm.con.execute(nodesel):
                docfile.write("  <node name=\"" + node['Name'] + "\" uniqueName=\"okular-{" + str(node['Id']) + "}\"")
                anyattr = False
                for nodeattr in norm.con.execute(nodeattrsel, { 'Node': node['Id'] }):
                    if not anyattr:
                        anyattr = True
                        docfile.write(">\n")
                        docfile.write("   <attribute name=\"" + nodeattr['Attribute'] + "\" value=\"" + nodeattr['Value'] + "\"/>\n")
                if anyattr:
                    docfile.write("  </node>\n")
                else:
                    docfile.write("/>\n")
            docfile.write(" </QDA>\n")

            anytagging = False
            for tagging in norm.con.execute(taggingsel, { 'Source': source['Id'] }):
                matchfragment = fragmentregex.match(tagging['Fragment'])
                start = int(matchfragment.group('start')) - 1
                end   = int(matchfragment.group('end'))
                if not anytagging:
                    anytagging = True
                    docfile.write("""
 <pageList>
  <page number="0">
   <taggingList>""")
                docfile.write("""
    <tagging type="1">
     <base author=\"""" + tagging['User'] + "\" modifyDate=\"" + str(tagging['ModifiedDate']) + "\" creationDate=\"" + str(tagging['CreatedDate']) + "\" node=\"okular-{" + str(tagging['Node']) + "}\" />\n")
                docfile.write("     <textref o=\"" + str(start) + "\" l=\"" + str(end-start+1) + """"/>
    </tagging>""")
            if anytagging:
                docfile.write("""
   </taggingList>
  </page>
 </pageList>""")

            docfile.write("""
</documentInfo>""")
            
    except:
        raise

    finally:
        del norm

if __name__ == '__main__':
    nvpn2bqda(None)
