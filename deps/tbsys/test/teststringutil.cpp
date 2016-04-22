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
#include <stdint.h>
#include <stringutil.h>

using namespace tbsys;

int main(int argc, char *argv[])
{

    char s[100];
    for(int i=0; i<10; i++) {
	int len = sprintf(s, "²âÊÔ_test_%d", i);
	fprintf(stderr, "%s => %u\n", s, CStringUtil::murMurHash(s, len));
    }
    uint64_t x = 1;
    for(int i=0; i<7; i++) {
	fprintf(stderr, "%ld => %s\n", x, CStringUtil::formatByteSize(x).c_str());
	x *= 1022;
    }

    return 0;
}

