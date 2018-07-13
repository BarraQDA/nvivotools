#!/bin/sh
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
    for SQLANYWHERE in `ls -d /opt/sqlanywhere?? 2>/dev/null`; do
        if test -f $SQLANYWHERE/bin64/sa_config.sh; then
            . $SQLANYWHERE/bin64/sa_config.sh
        elif test -f $SQLANYWHERE/bin32/sa_config.sh; then
            . $SQLANYWHERE/bin32/sa_config.sh
        fi
    done
elif [ "$(uname)" = "Darwin" ]; then
    SQLANYWHERE=/Applications/NVivo.app/Contents/SQLAnywhere
    # The version of SQLAnywhere bundled with NVivo is very difficult to work with
    # as its libraries contain references to @rpath. In addition, dlopen() doesn't
    # seem to respect DYLD_LIBRARY_PATH and friends. But it will find a library
    # in the current working directory. So this hack makes a copy of
    # libdbcapi_r.dylib, modifies its embedded rpath, tells sqlanydb to use
    # the modified library, and flags that the current working directory much
    # be changed prior to loading sqlanydb. Whew!
    if test -d "$SQLANYWHERE"; then
        if test -f $SQLANYWHERE/lib64/libdbcapi_r.dylib; then
            cp -p $SQLANYWHERE/lib64/libdbcapi_r.dylib $SQLANYWHERE/lib64/libdbcapi_r.rpath.dylib
            chmod +w $SQLANYWHERE/lib64/libdbcapi_r.rpath.dylib
            install_name_tool -add_rpath $SQLANYWHERE/lib64/ $SQLANYWHERE/lib64/libdbcapi_r.rpath.dylib
            export SQLANY_API_DLL=libdbcapi_r.rpath.dylib
            export CHDIR=$SQLANYWHERE/lib64/
        elif test -f $SQLANYWHERE/lib32/libdbcapi_r.dylib; then
            cp -p $SQLANYWHERE/lib32/libdbcapi_r.dylib $SQLANYWHERE/lib32/libdbcapi_r.rpath.dylib
            chmod +w $SQLANYWHERE/lib32/libdbcapi_r.rpath.dylib
            install_name_tool -add_rpath $SQLANYWHERE/lib32/ $SQLANYWHERE/lib32/libdbcapi_r.rpath.dylib
            export SQLANY_API_DLL=libdbcapi_r.rpath.dylib
            export CHDIR=$SQLANYWHERE/lib32/
        fi
    else
        for SQLANYWHERE in `ls -d /Applications/SQLAnywhere??/System 2>/dev/null`; do
            if test -f $SQLANYWHERE/bin64/sa_config.sh; then
                . $SQLANYWHERE/bin64/sa_config.sh
            elif test -f $SQLANYWHERE/bin32/sa_config.sh; then
                . $SQLANYWHERE/bin32/sa_config.sh
            fi
        done
    fi
fi

set > /tmp/sqlanyenv.2
diff /tmp/sqlanyenv.1 /tmp/sqlanyenv.2 | grep "^>" | awk '{print $2}'