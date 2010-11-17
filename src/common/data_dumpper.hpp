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

#ifndef DATA_DUMP_H_
#define DATA_DUMP_H_
#include <iostream>
#include <string>
#include <tbsys.h>
#include "define.hpp"
namespace tair {

   class data_dumpper {
   public:
      data_dumpper():is_dump(true),fp(NULL),path(NULL),start_time(0),curr_time(0){}
      ~data_dumpper(){
         if(fp != NULL){
            fclose(fp);
            fp = NULL;
         }
      }
      static const int SECONDS_PER_FILE = 24 * 60 * 60;
      struct item {
         item():area(0),prop(0),prop_len(0),key(0),key_len(0),value(0),value_len(0){}
         int area;
         char *prop;
         int prop_len;

         char *key;
         int key_len;

         char *value;
         int value_len;
      };
      /*
       * area keylen value_len dumptime [prop_len]\n
       * key\n
       * value\n
       * [prop]\n
       *
       **/
      int dump(item *p_item);
   private:
      bool open_file();
      bool is_dump;
      FILE *fp;
      const char *path;
      time_t start_time;
      time_t curr_time;
   };

} /* tair */

#endif
