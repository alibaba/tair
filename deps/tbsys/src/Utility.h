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

#ifndef TBSYS_UTILITY_H_
#define TBSYS_UTILITY_H_

#include <string.h>
#include <stdlib.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <arpa/inet.h>

namespace tbsys
{
#define MAX_STR_FIELD_NUM 32
#define SEC2USEC 1000000
#define USEC2NSEC 1000

#ifdef __cplusplus
extern "C"
{
#endif // __cplusplus

int getAbsPath(const char *pszPath, char *pszBuf, int iBufLen);
int checkCreateDir(const char *pszPath);
int checkCreateLink(const char *pszPath, const char *pszLink, int iRecreate);
int strJoin(char *pszDst, size_t sizeDst, char **ppszField, size_t sizeField, const char *pszSep);
// 不能多线程调用
int getHostIP(char *pszAddr, unsigned uiAddrLen);
int getExe(char *pszExe, unsigned uiExeLen);
int getExeRoot(char *pszExeRoot, unsigned uiExePathLen);

/**
 * 获取大于某个值得最小的2的n次方的整数
 * @param uiValue(uint32_t): 要求的值的下限
 * @see 
 * @return result
 */
static inline uint32_t guint32p2(uint32_t uiValue)
{
    uiValue |= (uiValue >> 1);
    uiValue |= (uiValue >> 2);
    uiValue |= (uiValue >> 4);
    uiValue |= (uiValue >> 8);
    uiValue |= (uiValue >> 16);
    return uiValue + 1;
}

/**
 * 把64位主机字节序整数转换成64位网络字节序整数
 * @param ull(uint64_t): 64位主机字节序整数
 * @see 
 * @return 64位网络字节序整数
 */
static inline uint64_t htonll(uint64_t ull)
{
    if (1 != htonl(1))
    {
        uint64_t ullRet = 0;
        char *pSrc = (char *)&ull;
        char *pDst = (char *)&ullRet;
        pDst[0] = pSrc[7];
        pDst[1] = pSrc[6];
        pDst[2] = pSrc[5];
        pDst[3] = pSrc[4];
        pDst[4] = pSrc[3];
        pDst[5] = pSrc[2];
        pDst[6] = pSrc[1];
        pDst[7] = pSrc[0];
        return ullRet;
    }
    return ull;
}

/**
 * 把64位网络字节序整数转换成64位主机字节序整数
 * @param ull(uint64_t): 64位网络字节序整数
 * @see 
 * @return 64位主机字节序整数
 */
static inline uint64_t ntohll(uint64_t ull)
{
    if (1 != ntohl(1))
    {
        uint64_t ullRet = 0;
        char *pSrc = (char *)&ull;
        char *pDst = (char *)&ullRet;
        pDst[0] = pSrc[7];
        pDst[1] = pSrc[6];
        pDst[2] = pSrc[5];
        pDst[3] = pSrc[4];
        pDst[4] = pSrc[3];
        pDst[5] = pSrc[2];
        pDst[6] = pSrc[1];
        pDst[7] = pSrc[0];
        return ullRet;
    }
    return ull;
}

/**
 * 把相对现在的一个时间片断转换成将来的绝对时间
 * @param pts(struct timespec *): 输出参数，将来的绝对时间
 * @param uiUSec(unsigned): 相对于现在的未来的微秒数
 * @see 
 * @return 0(成功)
 * @return -1(失败)
 */
static inline int getFutureAbsTS(struct timespec *pts, unsigned uiUSec)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    tv.tv_usec += uiUSec % SEC2USEC;
    if (tv.tv_usec >= SEC2USEC)
    {
        pts->tv_sec = tv.tv_sec + uiUSec / SEC2USEC + 1;
        pts->tv_nsec = (tv.tv_usec - SEC2USEC) * 1000;
    }
    else
    {
        pts->tv_sec = tv.tv_sec + uiUSec / SEC2USEC;
        pts->tv_nsec = tv.tv_usec * USEC2NSEC;
    }
    return 0;
}

#ifdef __cplusplus
}
#endif // __cplusplus

}//end namespace
#endif // UTILITY_H_
