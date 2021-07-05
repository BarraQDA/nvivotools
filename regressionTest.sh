#!/bin/sh
#
# Copyright 2016 Jonathan Schultz
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

dir=`dirname $0`

$dir/NormaliseDB.py - sqlite:///$1

$dir/editProject.py --title "Test project" --description "This test project is for regression testing NVivotools" --user "Regression Tester" $1

$dir/editNodeCategory.py --name "Node cat one" --description "First of two node categories" $1
$dir/editNodeCategory.py --name "Node cat two" --description "Second of two node categories" $1

$dir/editNodeAttribute.py --name "String attribute" --type text --length 64 $1
$dir/editNodeAttribute.py --name "Integer attribute" --type integer $1
$dir/editNodeAttribute.py --name "Decimal attribute" --type decimal $1
$dir/editNodeAttribute.py --name "DateTime attribute" --type datetime $1
$dir/editNodeAttribute.py --name "Date attribute" --type date $1
$dir/editNodeAttribute.py --name "Time attribute" --type time $1
$dir/editNodeAttribute.py --name "Boolean attribute" --type boolean $1

$dir/editNode.py --name "Node with attributes" --description "Node in first category testing attributes" --category "Node cat one" --attribute "String attribute:string value" --attribute "Integer attribute:17" --attribute "Decimal attribute:3.1415" --attribute "DateTime attribute:2000-01-01 00:01" --attribute "Date attribute:10 Dec 1967" --attribute "Time attribute:16:20" --attribute "Boolean attribute:false" $1

$dir/editNode.py --name "Top level node no aggregate"    $1
$dir/editNode.py --name "Second level node" --description "Child of no aggregate top node" --parent "Top level node no aggregate" $1
$dir/editNode.py --name "Second second level node" --description "Second child of no aggregate top node" --parent "Top level node no aggregate" $1
$dir/editNode.py --name "Third level node" --parent "Second second level node" $1

$dir/editNode.py --name "Top level node with aggregate" --aggregate $1
$dir/editNode.py --name "Another second level node" --description "Child of with aggregate top node" --parent "Top level node with aggregate" $1

$dir/editSourceCategory.py --name "Source cat one" --description "First of two source categories" $1
$dir/editSourceCategory.py --name "Source cat two" --description "Second of two source categories" $1

$dir/editSourceAttribute.py --name "String attribute" --type text --length 64 $1
$dir/editSourceAttribute.py --name "Integer attribute" --type integer $1
$dir/editSourceAttribute.py --name "Decimal attribute" --type decimal $1
$dir/editSourceAttribute.py --name "DateTime attribute" --type datetime $1
$dir/editSourceAttribute.py --name "Date attribute" --type date $1
$dir/editSourceAttribute.py --name "Time attribute" --type time $1
$dir/editSourceAttribute.py --name "Boolean attribute" --type boolean $1

echo "Hello world" > source.txt

$dir/editSource.py --name "Source with attributes" --description "Source in first category testing attributes" --category "Source cat one" --attribute "String attribute:string value" --attribute "Integer attribute:17" --attribute "Decimal attribute:3.1415" --attribute "DateTime attribute:2000-01-01 00:01" --attribute "Date attribute:10 Dec 1967" --attribute "Time attribute:16:20" --attribute "Boolean attribute:false" --source source.txt $1

$dir/editTagging.py --node "Third level node" --source "Source with attributes" --fragment "0:4" $1



