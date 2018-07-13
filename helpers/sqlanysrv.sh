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

if [ "$(uname)" = "Linux" ]; then
    for SQLANYWHERE in `ls -d /opt/sqlanywhere?? 2>/dev/null`; do
        if test -f $SQLANYWHERE/bin64/sa_config.sh; then
            . $SQLANYWHERE/bin64/sa_config.sh
            $SQLANYWHERE/bin64/dbeng?? "$@"
            exit
        elif test -f $SQLANYWHERE/bin32/sa_config.sh; then
            . $SQLANYWHERE/bin32/sa_config.sh
            $SQLANYWHERE/bin32/dbeng?? "$@"
            exit
        fi
    done
elif [ "$(uname)" = "Darwin" ]; then
    SQLANYWHERE=/Applications/NVivo.app/Contents/SQLAnywhere
    if test -d "$SQLANYWHERE"; then
        if test -d $SQLANYWHERE/bin64/; then
            $SQLANYWHERE/bin64/dbeng?? "$@"
            exit
        elif test -d $SQLANYWHERE/bin32/; then
            $SQLANYWHERE/bin32/dbeng?? "$@"
            exit
        fi
    else
        for SQLANYWHERE in `ls -d /Applications/SQLAnywhere??/System 2>/dev/null`; do
            if test -f $SQLANYWHERE/bin64/sa_config.sh; then
                . $SQLANYWHERE/bin64/sa_config.sh
                $SQLANYWHERE/bin64/dbeng?? "$@"
                exit
            elif test -f $SQLANYWHERE/bin32/sa_config.sh; then
                . $SQLANYWHERE/bin32/sa_config.sh
                $SQLANYWHERE/bin32/dbeng?? "$@"
                exit
            fi
        done
    fi
fi
echo "No SQLAnywhere instance found"
