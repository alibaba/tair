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

#include "fileutil.h"

namespace tbsys {
    /** 
     * 创建多级目录, 在父目录不存在一同创建
     */
    bool CFileUtil::mkdirs(char *szDirPath) 
    {
        struct stat stats;
        if (stat (szDirPath, &stats) == 0 && S_ISDIR (stats.st_mode)) 
            return true;
    
        mode_t umask_value = umask (0);
        umask (umask_value);
        mode_t mode = (S_IRWXUGO & (~ umask_value)) | S_IWUSR | S_IXUSR;
        
        char *slash = szDirPath;
    	while (*slash == '/')
    	    slash++;
        
        while (1)
    	{
    	    slash = strchr (slash, '/');
    	    if (slash == NULL)
    	        break;
    	        
            *slash = '\0';
            int ret = mkdir(szDirPath, mode);
            *slash++ = '/';
            if (ret && errno != EEXIST) {
                return false;
            }
            
    	    while (*slash == '/')
    	        slash++;
        }
        if (mkdir(szDirPath, mode)) {
            return false;
        }
        return true;
    }

    // 是否为目录
    bool CFileUtil::isDirectory(const char *szDirPath)
    {
        struct stat stats;
        if (lstat (szDirPath, &stats) == 0 && S_ISDIR (stats.st_mode))
            return true;
        return false;
    }

    // 是否为islnk
    bool CFileUtil::isSymLink(const char *szDirPath)
    {
        struct stat stats;
        if (lstat (szDirPath, &stats) == 0 && S_ISLNK (stats.st_mode)) 
            return true;
        return false; 
    }
}

//////////////////END
