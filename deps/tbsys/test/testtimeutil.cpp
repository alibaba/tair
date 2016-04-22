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

#include <tbtimeutil.h>
#include <ext/hash_map>
#include <set>

using namespace tbsys;

int main(int argc, char *argv[])
{
    int i=0;
    printf("i:%d\n", i);
    i = CTimeUtil::strToTime("20090101121212");
    i = CTimeUtil::strToTime("20090101121212");
    i = CTimeUtil::strToTime("20090101121212");
    i = CTimeUtil::strToTime("20090101121212");
    i = CTimeUtil::strToTime("20090101121212");
    printf("i:%d\n", i);
    return 0;
}

