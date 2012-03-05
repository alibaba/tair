#! /bin/bash

usage()
{
    echo -e "\tusage:
      -h, \t print this help.
      -c, \t configure server, in terms of ip:port.
      -g, \t group name.
      -a, \t print area stat.
      -s, \t print server stat.
      -p, \t print ping time, the response time.
      "
}

sstat() {
    grep ':.*:' stat.log |\
        gawk '{
            sstat[$1, $4] += $5;
        }END{
            for (key in sstat) {
                split(key, sk, SUBSEP);
                print sk[1],sk[2], sstat[key]
            }
        }' | sort -g |\
    gawk '{
        if (sstat[$1] ~ /^[.:0-9]*$/) {
            sstat[$1] = ( sstat[$1] " " $2 ":" $3 );
        } else {
            sstat[$1] = ( sstat[$1] ", " $2 ":" $3 );
        }
    }END{
        for (key in sstat) {
            print key, sstat[key];
        }
    }'
}

ping() {
    grep ' *[.0-9]*:[0-9]* *[0-9]*\.[0-9]*' stat.log | sed 's/^ *//' | sed 's/  */\tping:/'
}

astat() {
    grep '^[0-9]\+ ' stat.log |\
        gawk '{
            if (sstat[$1] ~ /^[0-9]+$/) {
                sstat[$1] = ( sstat[$1] " " $2 ":" $3 );
            } else {
                sstat[$1] = ( sstat[$1] ", " $2 ":" $3 );
            }
        }END{
            for (key in sstat) {
                if (sstat[key] ~ /itemCount:0/)
                    continue;
                print key, sstat[key];
            }
        }' | sort -g
}
isall=1
issstat=
isastat=
isping=
while getopts ":hc:g:asp" opt
do
    case $opt in
        h)
            usage
            exit 0
        ;;
        c)
            cs=$OPTARG
        ;;
        g)
            grp=$OPTARG
        ;;
        a)
            isastat=1
            isall=0
        ;;
        s)
            issstat=1
            isall=0
        ;;
        p)
            isping=1
            isall=0
        ;;
        :)
            echo -e "\t-$OPTARG requires an argument"
            exit 1
        ;;
        \?)
            echo -e "\t-$OPTARG: unknown option"
            exit 1
        ;;
    esac
done

[ "$cs" = "" ] && usage && exit
[ "$grp" = "" ] && usage && exit

sbin/tairclient -c $cs -g $grp 2>stat.log <<EOF
stat
exit
EOF

[ "$isall" = 1 ] && astat && sstat && ping && exit 0
[ "$isastat" = 1 ] && astat
[ "$issstat" = 1 ] && sstat
[ "$isping" = 1 ] && ping

exit 0
