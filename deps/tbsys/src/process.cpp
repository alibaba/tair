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

#include "process.h"

namespace tbsys {
    
    /**
     * 起一个守护进程, 返回守护进程ID
     */
    int CProcess::startDaemon(const char *szPidFile, const char *szLogFile)
    {
        if (getppid() == 1) {
            return 0;
        }
    
        int pid = fork();
        if (pid < 0) exit(1);
        if (pid > 0) return pid;

        writePidFile(szPidFile);

        int fd =open("/dev/null", 0);
        if (fd != -1) {
            dup2(fd, 0);
            close(fd);
        }
        
        TBSYS_LOGGER.setFileName(szLogFile);
                
        return pid;
    }
    
    /**
     * 写pid文件
     */
    void CProcess::writePidFile(const char *szPidFile)
    {
        char            str[32];
        int lfp = open(szPidFile, O_WRONLY|O_CREAT|O_TRUNC, 0600);
        if (lfp < 0) exit(1);
        if (lockf(lfp, F_TLOCK, 0) < 0) {
            fprintf(stderr, "Can't Open Pid File: %s", szPidFile);
            exit(0);
        }
        sprintf(str, "%d\n", getpid());
        ssize_t len = strlen(str);
        ssize_t ret = write(lfp, str, len);
        if (ret != len ) {
            fprintf(stderr, "Can't Write Pid File: %s", szPidFile);
            exit(0);
        }
        close(lfp);
    }
        
    /**
     * 判断进程是否还活着
     * 
     * 活着: 非0
     * 死掉: 0
     */
    int CProcess::existPid(const char *szPidFile)
    {
        char buffer[64], *p;
        int otherpid = 0, lfp;
        lfp = open(szPidFile, O_RDONLY, 0);
        if (lfp >= 0) {
            read(lfp, buffer, 64);
            close(lfp);
            buffer[63] = '\0';
            p = strchr(buffer, '\n');
            if (p != NULL)
                *p = '\0';
            otherpid = atoi(buffer);
        }
        if (otherpid > 0) {
            if (kill(otherpid, 0) != 0) {
                otherpid = 0;
            }
        }
        return otherpid;
    }
}

/*END*/

