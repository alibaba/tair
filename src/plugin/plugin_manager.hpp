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
#ifndef PLUGIN_MANAGER_H
#define PLUGIN_MANAGER_H
#include "tbsys.h"
#include "base_plugin.hpp"
#include "atomic.h"
#include <string>
#include <map>
namespace tair{
   namespace plugin {
      //do not derived from this class
      class plugin_handler {
      public:
         explicit plugin_handler(const std::string& dll_name);
         base_plugin* get_instance() const;
         std::string get_dll_name() const {
            return dll_name;
         }
         // the next 3 func need be protected by mutex
         bool load_dll();
         bool unload_dll();
         ~plugin_handler();
      private:
         std::string dll_name;
         void* dll_handler;
         create_t* create;
         destroy_t* destroy;
         base_plugin* instance;

      };

      class plugins_root { // this class for copy on write
      public:
         std::map<int, plugin_handler*> request_plugins;
         std::map<int, plugin_handler*> response_plugins;
      };
      class plugins_manager {
      public:
         plugins_manager(): root(NULL) {
         }
         ~plugins_manager();
         std::set<std::string> get_dll_names();
         bool chang_plugins_to(const std::set<std::string>& dll_names);
         // if a minus return value occured when we excute the request plugins, no response plugins will be performed.
         int do_request_plugins(int plugin_type, int call_type, int area, const data_entry* key, const data_entry* value, plugins_root*& root);
         // the last parameter root is the value got when the doRequestPlugIns be called.
         void do_response_plugins(int rev, int plugin_type, int call_type, int area, const data_entry* key, const data_entry* value, plugins_root*& root );
      public:
         bool add_plugins(const std::set<std::string>& dll_names);
         void remove_plugins (const std::set<std::string>& dll_names);
         std::set<int> get_properties();
      private:
         bool add_plugin(const std::string& dll_name, plugins_root* root);
         void clean_plugins(plugins_root*); //delete point

         plugins_root* root;
         tbsys::CThreadMutex mutex;
      };
   }}
#endif
