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

set preferredinstances=QSRNVIVO10 QSRNVIVO1 SQLEXPRESS MSSQLSERVER

setlocal EnableDelayedExpansion
set first=
for /f "skip=2 tokens=1 delims= " %%i in ('reg query "HKLM\Software\Microsoft\Microsoft SQL Server\Instance Names\SQL"') do (
rem for /f "skip=2 tokens=1 delims= " %%i in ('type instances.txt') do (
    set found%%i=1
    if not defined first (
        set first="%%i"
    )
)

set instance=
for %%i in (%preferredinstances%) do (
    if not defined instance if "!found%%i!" == "1" (
        set instance=%%i
    )
)
if not defined instance (
    set instance=%first%
)
echo %instance%
endlocal
