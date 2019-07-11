#!/bin/bash
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

set > /tmp/sqlanyenv.1

if [ "$(uname)" = "Linux" ]; then
    sqlanywhere=${sqlanywhere:-/opt}
elif [ "$(uname)" = "Darwin" ]; then
    sqlanywhere=${sqlanywhere:-/Applications}
fi

# Hacky way of choosing which SQLAnywhere installation to use: sort in
# reverse order so SQLAnywhere installations take precedence over NVivo
# installations and 64 bit over 32 bit. But only stop when we find a
# dynamic executable.
IFS=$'\n'
for dbeng in `find $sqlanywhere -name dbeng[0-9][0-9] | sort -r`; do
    source=$(echo $dbeng | awk -F/ '{print $3}')
    version=$(echo $dbeng | awk -F/ '{print $NF}' | sed 's/dbeng\(..\)/\1/')
    arch=$(echo $dbeng | awk -F/ '{print $(NF-1)}')
    bits=$(echo $arch | sed 's/bin\([0-9]*\).*/\1/')
    binpath=`dirname $dbeng`
    libpath=`dirname $binpath`/lib$bits
    [[ "${arch: -1}" == "s" ]] && static=1 || static=0
    [[ $source =~ NVivo ]] && nvivo=1 || nvivo=0
    [[ $nvivo ]] && nvivoversion=$(echo $source | sed 's/NVivo\(.*\)\.app/\1/' | xargs)
    [[ $static == 0 ]] && break
done
unset IFS
if [[ "$source" != "" ]]; then
    echo "Found SQLAnywhere installation at `dirname "$dbeng"`" > /dev/stderr
else
    echo "Could not find SQLAnywhere installation" > /dev/stderr
    exit 1
fi

# The version of SQLAnywhere bundled with NVivo is very difficult to work with
# as its libraries contain references to @rpath. In addition, dlopen() doesn't
# seem to respect DYLD_LIBRARY_PATH and friends. But it will find a library
# in the current working directory. So this hack makes a copy of
# libdbcapi_r.dylib, modifies its embedded rpath, tells sqlanydb to use
# the modified library, and flags that the current working directory much
# be changed prior to loading sqlanydb. Whew!
if [[ $nvivo == 1 && $static == 0 ]]; then
    if test -f "$libpath"/libdbcapi_r.dylib; then
        cp -p "$libpath"/libdbcapi_r.dylib "$libpath"/libdbcapi_r.rpath.dylib
        chmod +w "$libpath"/libdbcapi_r.rpath.dylib
        install_name_tool -add_rpath "$libpath"/ "$libpath"/libdbcapi_r.rpath.dylib
        SQLANY_API_DLL=libdbcapi_r.rpath.dylib
        CHDIR="$libpath"
    fi
else
# As of High Sierra we have another problem - changes to DYLD_LIBRARY_PATH are dumped by the
# System Integrity Protection system. We hack our way around this problem by changing the current
# working directory to the one containing the dynamic libraries.
    CHDIR=$libpath
fi

if test -f "$binpath"/sa_config.sh; then
    . "$binpath"/sa_config.sh
fi

set > /tmp/sqlanyenv.2
comm -13 <(sort /tmp/sqlanyenv.1) <(sort /tmp/sqlanyenv.2)
