/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * sample_plugin.cpp is a example to show how to make a plugin
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "sample_plugin.hpp"
#include "tbsys.h"
namespace tair{
   namespace plugin{

      int sample_plugin::get_hook_point() {
         return HOOK_POINT_REQUEST;
      }
      int sample_plugin::get_property() {
         return 1001;
      }
      int sample_plugin::get_plugin_type() {
         return PLUGIN_TYPE_SYSTEM;
      }
      int sample_plugin::do_request(int call_type, int area, const data_entry* key, const data_entry* value, void* exv )
      {
         log_debug("sample_plugin::do_request call_type:%d area:%d TairDataEntry key:%p TairDataEntry value:%p",
                   call_type, area, key, value);

         return 0;
      }
      bool sample_plugin::init()
      {
         log_debug("sample_plugin::init()");
         return true;
      }
      void sample_plugin::clean()
      {
         log_debug("sample_plugin::clean()");
         return;
      }
      extern "C" base_plugin* create()
      {
         return new sample_plugin();
      }

      extern "C" void destroy (base_plugin* p)
      {
         delete p;
      }
   }
}
