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
 *   MaoQi <maoqi@taobao.com>
 *
 */
#ifndef CTAIR_CLIENT_API_H
#define CTAIR_CLIENT_API_H

#if __cplusplus
extern "C"
{
#endif

   typedef void* tair_handler;

   typedef struct _TairDataPair
   {
      size_t len;
      char*  data;
   }tair_data_pair;

   tair_handler tair_init();
   void tair_deinit(tair_handler handler);
   void tair_set_loglevel(const char* level);
   int tair_begin(tair_handler handler, const char *master_addr,const char *slave_addr, const char *group_name);
   int tair_put(tair_handler handler, int area, tair_data_pair* key, tair_data_pair* data, int expire, int version);
   int tair_get(tair_handler handler, int area, tair_data_pair* key, tair_data_pair* data);
   int tair_remove(tair_handler handler, int area, tair_data_pair* key);
//int tairRemoveArea(tair_handler handler, int area, int timeout);
   int tair_incr(tair_handler handler, int area, tair_data_pair* key, int count, int* ret_count, int init_value, int expire);
   int tair_decr(tair_handler handler, int area, tair_data_pair* key, int count, int* ret_count, int init_value, int expire);

#if __cplusplus
}
#endif
#endif
