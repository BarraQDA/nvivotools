#!/bin/sh

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
        if test -d $SQLANYWHERE/bin64 && test -f $SQLANYWHERE/bin64/dbeng??; then
            DYLD_LIBRARY_PATH=$SQLANYWHERE/lib64/ $SQLANYWHERE/bin64/dbeng?? "$@"
            exit
        elif test -d $SQLANYWHERE/bin32 && test -f $SQLANYWHERE/bin32/dbeng??; then
            DYLD_LIBRARY_PATH=$SQLANYWHERE/lib32/ $SQLANYWHERE/bin32/dbeng?? "$@"
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
