#!/bin/bash

case $1 in
shut)
    ssh root@localhost "/sbin/iptables -A OUTPUT -d 10.232.4.17 -p tcp --dport 5368 -j DROP "
    ssh root@localhost "/sbin/iptables -A INPUT -s 10.232.4.17 -p tcp --dport 5368 -j DROP "
    ssh root@localhost "/sbin/iptables -A OUTPUT -d 10.232.4.17 -p tcp --dport 5361 -j DROP "
    ssh root@localhost "/sbin/iptables -A INPUT -s 10.232.4.17 -p tcp --dport 5368 -j DROP "
    ssh root@localhost "/sbin/iptables -A OUTPUT -d 10.232.4.18 -p tcp --dport 5361 -j DROP "
    ssh root@localhost "/sbin/iptables -A INPUT -s 10.232.4.18 -p tcp --dport 5368 -j DROP "
    ssh root@localhost "/sbin/iptables -A OUTPUT -d 10.232.4.19 -p tcp --dport 5361 -j DROP "
    ssh root@localhost "/sbin/iptables -A INPUT -s 10.232.4.19 -p tcp --dport 5368 -j DROP "
    ssh root@localhost "/sbin/iptables-save">/dev/null 2>&1
    echo 0
    ;;
recover)
    ssh root@localhost "/sbin/iptables -F"
    echo 0
    ;;
*)
    echo -1
    ;;
esac
exit 0
