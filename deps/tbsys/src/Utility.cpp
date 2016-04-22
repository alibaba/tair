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

#include <netdb.h>
#include "PublicDefine.h"
#include "Utility.h"

namespace tbsys
{
/**
 * 把一个路径转换成规范的绝对路径
 * @param pszPath(const char *): 原始路径
 * @param pszBuf(char *): 绝对路径存储内存地址
 * @param iBufLen(int): 绝对路径存储内存长度
 * @see 
 * @return 0(成功)
 * @return -1(失败)
 */
int getAbsPath(const char *pszPath, char *pszBuf, int iBufLen)
{
    int iBuf = 0;
    int iBufLeftLen = iBufLen;
    int iNameLen = 0;
    int iContinue = 1;
    const char *pcSubPathBegin = pszPath;
    const char *pcSubPathEnd = NULL;
    if ((NULL == pszPath) || ('\0' == pszPath[0]) || (iBufLeftLen < 2))
    {
        return -1;
    }

    if (pszPath[0] != '/')
    {
        if (NULL == getcwd(pszBuf, iBufLeftLen))
        {
            return -1;
        }
        iBuf = strlen(pszBuf);
        if ('/' == pszBuf[iBuf - 1])
        {
            --iBuf;
            pszBuf[iBuf] = '\0';
        }
        iBufLeftLen -= iBuf;
    }
    else
    {
        pszBuf[0] = '\0';
        ++pcSubPathBegin;
    }

    while (iContinue)
    {
        switch (pcSubPathBegin[0])
        {
            case '\0':
            {
                pszBuf[iBuf] = '\0';
                iContinue = 0;
                continue;
            }
            case '/':
            {
                ++pcSubPathBegin;
                continue;
            }
            case '.':
            {
                switch (pcSubPathBegin[1])
                {
                    case '\0':
                    {
                        pszBuf[iBuf] = '\0';
                        iContinue = 0;
                        continue;
                    }
                    case '/':
                    {
                        pszBuf[iBuf] = '\0';
                        pcSubPathBegin += 2;
                        continue;
                    }
                    case '.':
                    {
                        switch (pcSubPathBegin[2])
                        {
                            case '\0':
                            {
                                for (; iBuf >= 0; --iBuf)
                                {
                                    if ('/' == pszBuf[iBuf])
                                    {
                                        pszBuf[iBuf] = '\0';
                                        break;
                                    }
                                }
                                iContinue = 0;
                                continue;
                            }
                            case '/':
                            {
                                for (; iBuf >= 0; --iBuf)
                                {
                                    if ('/' == pszBuf[iBuf])
                                    {
                                        pszBuf[iBuf] = '\0';
                                        break;
                                    }
                                }
                                pcSubPathBegin += 3;
                                iBuf = (iBuf > 0)?iBuf:0;
                                iBufLeftLen = iBufLen - iBuf;
                                continue;
                            }
                            default:
                            {

                            }
                        }
                    }
                    default:
                    {

                    }
                }
            }
            default:
            {

            }
        }
        pszBuf[iBuf] = '/';
        ++iBuf;
        iBufLeftLen = iBufLen - iBuf;
        pcSubPathEnd = strstr(pcSubPathBegin, "/");
        if (pcSubPathEnd)
        {
            iNameLen = pcSubPathEnd - pcSubPathBegin;
            if (iBufLeftLen <= iNameLen)
            {
                return -1;
            }
            memcpy(pszBuf + iBuf, pcSubPathBegin, iNameLen);
            iBuf += iNameLen;
            pcSubPathBegin = pcSubPathEnd + 1;
        }
        else
        {
            if (iBufLeftLen < (int)strlen(pcSubPathBegin))
            {
                return -1;
            }
            strcpy(pszBuf + iBuf, pcSubPathBegin);
            return 0;
        }
    }

    if (pszBuf[0] != '/')
    {
        pszBuf[0] = '/';
        pszBuf[1] = '\0';
    }
    return 0;
}

/**
 * 检查并创建一个目录或一个文件的父目录
 * @param pszPath(const char *): 要检查的路径
 * @see 
 * @return 0(成功)
 * @return -1(失败)
 */
int checkCreateDir(const char *pszPath)
{
    struct stat statbuf;

    char szFileCopy[MAX_FILENAME_LEN];
    char szDir[MAX_FILENAME_LEN];
    int iDirLen = MAX_FILENAME_LEN;
    char *apszField[MAX_STR_FIELD_NUM];
    int iFieldNum = 0;
    char *pcFieldBegin = NULL;
    char *pcFieldEnd = NULL;
    int iLastIsFile = 0;
    int iLen = 0;
    int i = 0;

    if (pszPath == NULL)
    {
        return -1;
    }

    iLen = strlen(pszPath);
    if ((iLen < 1) || (iLen >= MAX_FILENAME_LEN))
    {
        return -1;
    }

    /*
     * 最后不以斜杠结尾，是文件
     */
    if (pszPath[iLen - 1] != '/')
    {
        iLastIsFile = 1;
    }

    strncpy(szFileCopy, pszPath, MAX_FILENAME_LEN);
    szFileCopy[MAX_FILENAME_LEN - 1] = '\0';

    /*
     * 解析成文件夹名数组
     */
    pcFieldEnd = szFileCopy;
    iFieldNum = 0;
    // 如果是绝对路径
    if (pcFieldEnd[0] == '/')
    {
        // 去除所有的多余的斜杠
        for (; *pcFieldEnd; ++pcFieldEnd)
        {
            if ('/' == *pcFieldEnd)
            {
                *pcFieldEnd = '\0';
                continue;
            }
            break;
        }
        pcFieldBegin = pcFieldEnd;
        pcFieldEnd = strchr(pcFieldBegin, '/');
        // 没有文件夹部分，则返回错误
        if (NULL == pcFieldEnd)
        {
            return -1;
        }
        --pcFieldBegin;
        *pcFieldBegin = '/';
        *pcFieldEnd = '\0';
        ++pcFieldEnd;
        apszField[0] = pcFieldBegin;
        iFieldNum = 1;
        
    }

    for (; iFieldNum <= MAX_STR_FIELD_NUM; ++iFieldNum)
    {
        if (pcFieldEnd)
        {
            // 去除所有的多余的斜杠
            for (; *pcFieldEnd; ++pcFieldEnd)
            {
                if ('/' == *pcFieldEnd)
                {
                    *pcFieldEnd = '\0';
                    continue;
                }
                break;
            }
            // 没有下一个有效的文件名，则跳出
            if ('\0' == *pcFieldEnd)
            {
                break;
            }
        }
        else
        {
            // 不存在下一个文件名，则跳出
            break;
        }
        pcFieldBegin = pcFieldEnd;
        apszField[iFieldNum] = pcFieldBegin;
        pcFieldEnd = strchr(pcFieldBegin, '/');
    }

    // 结束符设置，如果当前字段太多，则退出
    if (iFieldNum > MAX_STR_FIELD_NUM)
    {
        return -1;
    }

    if (iLastIsFile)
    {
        --iFieldNum;
        apszField[iFieldNum] = NULL;
    }
    for (i = 0; i < iFieldNum; i++)
    {
        /*
         * 如果存在点开头的文件夹，则退出
         */
        if (apszField[i][0] == '.')
        {
            return -2;
        }

        /*
         * 保证每一层目录都存在
         */
        if (strJoin(szDir, iDirLen, apszField, i + 1, "/") < 0)
        {
            return -1;
        }

        if (stat(szDir, &statbuf) == -1)
        {
            // 错误不是文件夹不存在或者创建失败则返回错误
            if ((errno != ENOENT) || (mkdir(szDir, 0755) == -1))
            {
                return -1;
            }
        }
        else if (!S_ISDIR(statbuf.st_mode))
        {
            return -1;
        }
    }

    return 0;
}

int checkCreateLink(const char *pszPath, const char *pszLink, int iRecreate)
{
    int iRes = 0;
    JUST_RETURN((NULL == pszPath) || ('\0' == pszPath[0]) || (NULL == pszLink) || ('\0' == pszLink[0]), -1);
    iRes = access(pszPath, R_OK);
    JUST_RETURN(-1 == iRes, -1);
    errno = 0;
    iRes = access(pszLink, R_OK);
    if (0 == iRes)
    {
        char szBuf[MAX_FILENAME_LEN];
        iRes = readlink(pszLink, szBuf, MAX_FILENAME_LEN - 1);
        JUST_RETURN(-1 == iRes, -1);
        szBuf[iRes] = '\0';
        JUST_RETURN(0 == strcmp(pszPath, szBuf), 0);
        JUST_RETURN(0 == iRecreate, -1);
        unlink(pszLink);
    }
    else
    {
        JUST_RETURN(errno != ENOENT, -1);
    }

    iRes = checkCreateDir(pszLink);
    JUST_RETURN(-1 == iRes, -1);
    iRes = symlink(pszPath, pszLink);
    JUST_RETURN(-1 == iRes, -1);
    return 0;
}

/**
 * 连接一组字符串
 * @param pszDst(char *): 目的字符串存储内存地址
 * @param sizeDst(size_t): 目的字符串存储内存长度
 * @param ppszField(char **): 要链接的字符串数组
 * @param sizeField(size_t): 要链接的字符串数目
 * @param pszSep(const char *): 链接字符串
 * @see 
 * @return 0(成功)
 * @return -1(失败)
 */
int strJoin(char *pszDst, size_t sizeDst, char **ppszField, size_t sizeField, const char *pszSep)
{
    int iFieldNum = 0;
    int iSepLen = 0;
    int i = 0;

    memset(pszDst, '\0', sizeDst);

    if (sizeField <= 0)
        return -1;

    iSepLen = 0;
    if (pszSep != NULL)
        iSepLen = strlen(pszSep);

    for (i = 0; (size_t)i < sizeField; i++)
    {
        if ((i > 0) && (iSepLen > 0))
        {
            if (sizeDst <= (size_t)iSepLen)
                return -1;

            strncat(pszDst, pszSep, sizeDst);
            sizeDst -= iSepLen;
        }

        iFieldNum = strlen(ppszField[i]);

        if (sizeDst <= (size_t)iFieldNum)
            return -1;

        strncat(pszDst, ppszField[i], sizeDst);
        sizeDst -= iFieldNum;
    }

    return (strlen(pszDst));
}

// 不能多线程调用
/**
 * 获取本机的ip地址
 * @param pszAddr(char *): 存储字符串格式ip地址的内存地址
 * @param uiAddrLen(unsigned): 存储字符串格式ip地址的内存长度
 * @see 
 * @return 0(成功)
 * @return -1(失败)
 */
int getHostIP(char *pszAddr, unsigned uiAddrLen)
{
    int iRes = 0;
    char szHostName[HOST_NAME_MAX];
    iRes = gethostname(szHostName, HOST_NAME_MAX);
    if (-1 == iRes)
    {
        return -1;
    }
    struct hostent *pHE = gethostbyname(szHostName);
    if (NULL == pHE)
    {
        return -1;
    }
    if ((pHE->h_length != sizeof(in_addr_t))
        || (NULL == pHE->h_addr_list)
        || (NULL == pHE->h_addr_list[0]))
    {
        return -1;
    }
    
    if (NULL == inet_ntop(AF_INET, pHE->h_addr_list[0], pszAddr, uiAddrLen))
    {
        return -1;
    }
    return 0;
}

/**
 * 获取进程的二进制文件绝对路径
 * @param pszExe(char *): 路径存放内存地址
 * @param uiExeLen(unsigned): 路径存放内存长度
 * @see 
 * @return 0(成功)
 * @return -1(失败)
 */
int getExe(char *pszExe, unsigned uiExeLen)
{
    char szExe[MAX_FILENAME_LEN] = "";
    int iRes = snprintf(szExe, MAX_FILENAME_LEN, "/proc/%u/exe", getpid());
    if ((iRes < 0) || (iRes >= MAX_FILENAME_LEN))
    {
        return -1;
    }
    iRes = readlink(szExe, pszExe, uiExeLen);
    if ((iRes < 0) || ((unsigned)iRes >= uiExeLen))
    {
        return -1;
    }
    return 0;
}

/**
 * 获取进程的二进制文件绝对根目录
 * @param pszExe(char *): 根目录存放内存地址
 * @param uiExeLen(unsigned): 根目录存放内存长度
 * @see 
 * @return 0(成功)
 * @return -1(失败)
 */
int getExeRoot(char *pszExeRoot, unsigned uiExePathLen)
{
    int i = 0;
    int iRes = getExe(pszExeRoot, uiExePathLen);
    if (-1 == iRes)
    {
        return -1;
    }
    for (i = uiExePathLen - 1; i > 0; --i)
    {
        if ('/' == pszExeRoot[i])
        {
            pszExeRoot[i] = '\0';
            break;
        }
    }
    return 0;
}

}//end namespace
/*
int main(int argc, char *argv[])
{
    if (-1 == checkCreateLink(argv[1], argv[2], (3 == argc)?0:1))
    {
        printf("check failed!\n");
    }
    else
    {
        printf("check OK.\n");
    }
    return 0;
}
*/
