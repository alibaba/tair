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
 *
 */
#ifndef BASE_PLUGIN_H
#define BASE_PLUGIN_H
#include "data_entry.hpp"
namespace tair {
   namespace plugin {
      using namespace tair::common;

      const int HOOK_POINT_REQUEST  = 1;
      const int HOOK_POINT_RESPONSE = 2;
      const int HOOK_POINT_ALL      = HOOK_POINT_REQUEST | HOOK_POINT_RESPONSE;

      const int PLUGIN_TYPE_SYSTEM  = 1;

      class base_plugin {
      public:


         virtual ~base_plugin()
          {
            return;
         }

         /*
          * return value 1 hook request 2 hook response 3 hook request and response
          */
         virtual int get_hook_point() = 0;

         /*
          * larger property will be excuted earier.
          */
         virtual int get_property() = 0;

         /*
          * reserved
          */
         virtual int get_plugin_type() = 0;

         /*
          * this will be called before the real operation is performed
          * parameters: call_type: put get remove and so on
          *       version, expired: these only useful when call_type is put
          *       exv: reserved
          * return value: if return value is minus, the process will be broken.
          */
         virtual int do_request(int call_type, int area, const data_entry* key, const data_entry* value, void* exv = NULL)
         {
            return 0;
         }
         /*
          * this will be called after the real operation is performed.
          */
         virtual void do_response(int rev, int call_type, int area, const data_entry* key, const data_entry* value, void* exv = NULL)
         {
            return;
         }
         virtual bool init()
         {
            return true;
         }
         virtual void clean()
          {
            return;
         }
      };
      typedef base_plugin* (create_t)();
      typedef void (destroy_t)(base_plugin*);
   }
}
#endif
