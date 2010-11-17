/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 */
#include "define.hpp"
#include <stdlib.h>
#include <stdio.h>
int
main()
{
  printf("------------golable---------------------\n");
  printf("TAIR_MAX_KEY_SIZE=%d\n", TAIR_MAX_KEY_SIZE);
  printf("TAIR_MAX_DATA_SIZE=%d\n", TAIR_MAX_DATA_SIZE);
  printf("TAIR_MAX_AREA_COUNT=%d\n", TAIR_MAX_AREA_COUNT);
  printf
    ("TAIR_MAX_FILENAME_LEN=%d this value will be used to create position information \n",
     TAIR_MAX_FILENAME_LEN);
  printf("------------configerver----------------\n");
  printf
    ("TAIR_SERVER_DOWNTIME=%d this means how long configserver will confirm crash of dataserver \n",
     TAIR_SERVER_DOWNTIME);
  printf
    ("TAIR_POS_MASK=%p this value will be used to create position information \n",
     (char *) TAIR_POS_MASK);
  printf("------------duplicate----------------\n");
  printf
    ("MISECONDS_BEFOR_SEND_RETRY=%d this value will be used in duplicate thread \n",
     MISECONDS_BEFOR_SEND_RETRY);
  printf("SLEEP_MISECONDS=%d this value will be used in duplicate thread \n",
         SLEEP_MISECONDS);
  return 0;
}
