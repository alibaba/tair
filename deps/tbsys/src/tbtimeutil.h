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

#ifndef TBSYS_TIMEUTIL_H_
#define TBSYS_TIMEUTIL_H_

#include <stdint.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <string.h>

namespace tbsys {

	/** 
	 * @brief linux时间操作简单的封装
	 */
class CTimeUtil {
public:
    /**
     * 得到当前时间
     */
    static int64_t getTime();
    /**
     * 得到单调递增的时间
     */
    static int64_t getMonotonicTime();
    /**
     * 把int转成20080101101010的格式
     */ 
    static char *timeToStr(time_t t, char *dest);
    /**
     * 把字节串转成时间(当地时间)
     */
    static int strToTime(char *str);
};

}

#endif
