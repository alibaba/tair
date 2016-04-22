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

#ifndef TBSYS_NETUTIL_H_
#define TBSYS_NETUTIL_H_

#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <netdb.h>
#include <sys/time.h>
#include <net/if.h>
#include <inttypes.h>
#include <sys/types.h>
#include <linux/unistd.h>
#include <string>

//using namespace std;

namespace tbsys {
/** 
* @brief 按ip地址大小比较 
*/
struct ipaddr_less {
    bool operator()(const uint64_t a, const uint64_t b) const {
        uint64_t a1 = ((a & 0xFF) << 24) | ((a & 0xFF00) << 8) | ((a & 0xFF0000) >> 8) | ((a & 0xFF000000) >> 24);
        a1 <<= 32; a1 |= ((a>>32) & 0xffff);
        uint64_t b1 = ((b & 0xFF) << 24) | ((b & 0xFF00) << 8) | ((b & 0xFF0000) >> 8) | ((b & 0xFF000000) >> 24);
        b1 <<= 32; b1 |= ((b>>32) & 0xffff);
        return (a1<b1);
    }
};

/** 
 * @brief IP地址转换类
 */
class CNetUtil {
public:
    /**
     * 得到本机ip
     */
    static uint32_t getLocalAddr(const char *dev_name);
    /**
     * ip是本机ip地址, true - 是, false - 不是
     */
    static bool isLocalAddr(uint32_t ip, bool loopSkip = true);
    /**
     * 把字符串的ip转成int
     * 如 xxx.xxx.xxx.xxx => ??????
     */
    static uint32_t getAddr(const char *ip);
    /**
     * 把uint64转成字符串
     */
    static std::string addrToString(uint64_t ipport);
    /**
     * 把ip,port转成uint64_t
     */
    static uint64_t strToAddr(const char *ip, int port);
    /**
     * 把ip,port转成uint64_t
     */
    static uint64_t ipToAddr(uint32_t ip, int port);
};

}

#endif
