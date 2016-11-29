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

# Wrapper for NormaliseNVPX.py
# Sets environment to run SQL Anywhere client on Linux or OSX then calls
# NormaliseNVPX.py in the same directory as this script.

if [ "$(uname)" = "Linux" ]; then
    for SQLANYWHERE in `ls -d /opt/sqlanywhere?? 2>/dev/null`; do
        if test -f $SQLANYWHERE/bin64/sa_config.sh; then
            found=1
            . $SQLANYWHERE/bin64/sa_config.sh
        elif test -f $SQLANYWHERE/bin32/sa_config.sh; then
            found=1
            . $SQLANYWHERE/bin32/sa_config.sh
        fi
    done
elif [ "$(uname)" = "Darwin" ]; then
    SQLANYWHERE=/Applications/NVivo.app/Contents/SQLAnywhere
    if test -d "$SQLANYWHERE"; then
        if test -d $SQLANYWHERE/lib64; then
            found=1
            export DYLD_LIBRARY_PATH=$SQLANYWHERE/lib64
        elif test -d $SQLANYWHERE/lib32; then
            found=1
            export DYLD_LIBRARY_PATH=$SQLANYWHERE/lib32
        fi
    else
        for SQLANYWHERE in `ls -d /Applications/SQLAnywhere??/System 2>/dev/null`; do
            if test -f $SQLANYWHERE/bin64/sa_config.sh; then
                found=1
                . $SQLANYWHERE/bin64/sa_config.sh
                break
            elif test -f $SQLANYWHERE/bin32/sa_config.sh; then
                found=1
                . $SQLANYWHERE/bin32/sa_config.sh
                break
            fi
        done
    fi
fi
if [ "$found" != "1" ]; then
    echo "No SQLAnywhere instance found"
    exit
fi

# Call python explicitly here, otherwise env will drop our environment.
python `dirname $0`/RQDA2NVPX.py --cmdline "$@"
