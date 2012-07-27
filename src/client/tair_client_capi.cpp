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
#include "tair_client_api.hpp"
#include "tair_client_capi.hpp"

using namespace tair;

tair_handler tair_init()
{
   return new tair_client_api();
}

void tair_deinit(tair_handler handler)
{
   tair_client_api* tc = (tair_client_api*)handler;
   tc->close();
   delete tc;
}

void tair_set_loglevel(const char* level )
{
   TBSYS_LOGGER.setLogLevel((char*)level);
}

int tair_begin(tair_handler handler, const char *master_addr,const char *slave_addr, const char *group_name)
{
   if ( handler == 0
        || master_addr <= 0
        || group_name == 0 )
   {
      return EXIT_FAILURE;
   }
   tair_client_api* tc = (tair_client_api*)handler;
   return tc->startup(master_addr, slave_addr, group_name) ? EXIT_SUCCESS : EXIT_FAILURE;
}

int tair_put(tair_handler handler, int area, tair_data_pair* key, tair_data_pair* data, int expire, int version)
{
  return tair_put_with_cache(handler, area, key, data, expire, version, true);
}

int tair_put_with_cache(tair_handler handler, int area, tair_data_pair* key, tair_data_pair* data, int expire, int version, bool fill_cache)
{
   if ( handler == 0 )
      return EXIT_FAILURE;

   data_entry _key(key->data, key->len, true);
   data_entry _data(data->data, data->len, true);

   tair_client_api* tc = (tair_client_api*)handler;
   return tc->put(area, _key, _data, expire, version, fill_cache);
}

int tair_get(tair_handler handler, int area, tair_data_pair* key, tair_data_pair* data)
{
   if ( handler == 0 )
      return EXIT_FAILURE;

   data_entry _key(key->data, key->len, true);
   data_entry* _data = 0;

   tair_client_api* tc = (tair_client_api*)handler;
   int ret = EXIT_FAILURE;
   if ( (ret = tc->get(area, _key, _data)) < 0 )
      return ret;



   data->len = _data->get_size();
   data->data = (char*)malloc(data->len + 1);
   memset(data->data, 0, data->len + 1 );
   memcpy(data->data, _data->get_data(), data->len );


   delete _data;
   _data = 0;
   return EXIT_SUCCESS;
}

int tair_remove(tair_handler handler, int area, tair_data_pair* key)
{
   if ( handler == 0 )
      return EXIT_FAILURE;

   data_entry _key(key->data, key->len, true);
   tair_client_api* tc = (tair_client_api*)handler;

   return tc->remove(area, _key);
}
/*
  int tairRemoveArea(tair_handler handler, int area, int timeout)
  {
  if ( handler == 0 )
  return EXIT_FAILURE;

  tair_client_api* tc = (tair_client_api*)handler;

  return tc->removeArea(area, timeout);
  }*/


int tair_incr(tair_handler handler, int area, tair_data_pair* key, int count, int* ret_count, int init_value, int expire)
{
   if ( handler == 0 )
      return EXIT_FAILURE;

   data_entry _key(key->data, key->len, true);

   tair_client_api* tc = (tair_client_api*)handler;
    
   return tc->incr(area, _key, count, ret_count, init_value, expire);
}

int tair_decr(tair_handler handler, int area, tair_data_pair* key, int count, int* ret_count, int init_value, int expire)
{
   if ( handler == 0 )
      return EXIT_FAILURE;

   data_entry _key(key->data, key->len, true);

   tair_client_api* tc = (tair_client_api*)handler;
    
   return tc->decr(area, _key, count, ret_count, init_value, expire);
}

