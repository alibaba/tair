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

#include <fileutil.h>

using namespace tbsys;

int main(int argc, char *argv[])
{
    char str[1024];
    strcpy(str, "/tmp/src/lcp/.svn/text-base");
    printf("ret: %d\n", tbsys::CFileUtil::mkdirs(str));
    
    strcpy(str, "/tmp/lcp/ltmain.sh");
    printf("is sym: ret: %d\n", tbsys::CFileUtil::isSymLink(str));
    return 0;
}

