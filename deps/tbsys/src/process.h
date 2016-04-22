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

#ifndef TBSYS_PROCESS_H
#define TBSYS_PROCESS_H

#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <tblog.h>

namespace tbsys {
    
	/** 
	 * @brief 进程以daemon方法启动时简单封装
	 */
    class CProcess {

    public:
        // 起一个daemon
        static int startDaemon(const char *szPidFile, const char *szLogFile);
        // 进程是不是存在
        static int existPid(const char *szPidFile);
        // 写PID文件
        static void writePidFile(const char *szPidFile);
    };
}
#endif 

/*END*/
