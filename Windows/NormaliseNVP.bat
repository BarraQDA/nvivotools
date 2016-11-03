@echo off

rem Copyright 2016 Jonathan Schultz
rem
rem This program is free software: you can redistribute it and/or modify
rem it under the terms of the GNU General Public License as published by
rem the Free Software Foundation, either version 3 of the License, or
rem (at your option) any later version.
rem
rem This program is distributed in the hope that it will be useful,
rem but WITHOUT ANY WARRANTY; without even the implied warranty of
rem MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
rem GNU General Public License for more details.
rem
rem You should have received a copy of the GNU General Public License
rem along with this program.  If not, see <http://www.gnu.org/licenses/>.
rem
rem Wrapper for NormaliseNVPX.py
rem Sets environment to run SQL Anywhere client on Linux or OSX then calls
rem NormaliseNVPX.py in the same directory as this script.

call mssqlAttach %1 nvivo
python NormaliseNVP.py --windows mssql+pymssql://nvivotools:nvivotools@%COMPUTERNAME%/nvivo "sqlite:///%2"
call mssqlDrop nvivo
