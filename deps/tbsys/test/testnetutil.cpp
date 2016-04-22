/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong
 *
 */

#include <tbnetutil.h>
#include <ext/hash_map>
#include <set>
#include <stdio.h>
#include <stdint.h>

using namespace __gnu_cxx;
using namespace std; 

using namespace tbsys;

int _UUID_Count = time(NULL);
int _UUID_HostIp = 0;

uint64_t newUUID()
{
    uint64_t id = (_UUID_HostIp & 0xFFFFFF00);
    id <<= 8;
    id |= getpid();
    id <<= 24;
    id |= (++_UUID_Count & 0xFFFFFF);
    return id;
}

void test1()
{
    set<uint64_t, ipaddr_less> m;
    uint64_t x;
    int port = 1234;
    x = CNetUtil::strToAddr("192.168.207.11", port); m.insert(x);
    x = CNetUtil::strToAddr("192.168.207.12", port); m.insert(x);
    x = CNetUtil::strToAddr("192.168.207.12", port+1); m.insert(x);
    x = CNetUtil::strToAddr("192.168.207.12", port-1); m.insert(x);
    x = CNetUtil::strToAddr("192.168.207.191", port); m.insert(x);
    x = CNetUtil::strToAddr("172.168.207.192", port); m.insert(x);
    x = CNetUtil::strToAddr("172.168.207.12", port); m.insert(x);
    x = CNetUtil::strToAddr("172.169.207.12", port); m.insert(x);
    x = CNetUtil::strToAddr("172.168.207.13", port); m.insert(x);
    x = CNetUtil::strToAddr("182.168.207.11", port); m.insert(x);
    x = CNetUtil::strToAddr("182.168.207.12", port); m.insert(x);
    x = CNetUtil::strToAddr("182.169.207.12", port); m.insert(x);
    x = CNetUtil::strToAddr("182.168.207.13", port); m.insert(x);
    set<uint64_t>::iterator it;
    for(it=m.begin(); it!=m.end(); ++it) {
       fprintf(stderr, "%s\n", CNetUtil::addrToString(*it).c_str()); 
    }
}


int main(int argc, char *argv[])
{
    //test1();
    char *ip = "192.168.207.158:2089";
    int port = 2071;
    uint32_t ipx = CNetUtil::getLocalAddr(NULL);
    unsigned char *bytes = (unsigned char *) &ipx;
    ipx<<=8;
    int index = 1;
    while(ipx!=0) {
    fprintf(stderr, "ip: ");
    for(int i=index; i<4; i++) fprintf(stderr, "%d.", bytes[i]);
    fprintf(stderr, "\n");
    ipx<<=8;
    index++;
    }
    _UUID_HostIp = ipx;
    uint32_t ipx1 = CNetUtil::getAddr("192.168.207.157");
    fprintf(stderr, "ipx: %X, ipx1: %X\n", ipx, ipx1);
    uint64_t uuid;
    for(int i=0; i<10; i++) {
     uuid = newUUID();
    fprintf(stderr, "uuid: %llu, %llX\n", uuid, uuid);
    } 

    uint64_t x = CNetUtil::strToAddr(ip, port);
    uint64_t x1 = CNetUtil::ipToAddr(ipx, port);
        
    fprintf(stderr, "x: %llu, %llu %s\n", x, x1, CNetUtil::addrToString(x).c_str());
    
    return 0;
}

