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
#include "data_dumpper.hpp"
#include "log.hpp"

namespace tair {

   int data_dumpper::dump(item *p_item)
   {
      if(!is_dump){
         return -1;
      }
      if(p_item == 0)
         return -1;
      if(p_item->key == 0 || p_item->value == 0 ||
         p_item->key_len == 0 || p_item->value_len == 0){
         return -1;
      }

      //record dump time
      time(&curr_time);

      if(start_time == 0){
         start_time = curr_time;
      }
      if( !open_file() ){
         return -1;
      }

      log_debug("\033[;31mDataDump::dump\033[0;m");
      uint32_t dump_time = static_cast<uint32_t>(curr_time);
      char header[64];
      int header_len = 0;
      int written = 0;
      if(p_item->prop_len <= 0){
         header_len = snprintf(header,sizeof(header),"%d %d %d %u\n",p_item->area,p_item->key_len,p_item->value_len,dump_time);
      }else{
         header_len = snprintf(header,sizeof(header),"%d %d %d %u %d\n",p_item->area,p_item->key_len,p_item->value_len,dump_time,p_item->prop_len);

      }

      if(fwrite(header,header_len,1,fp) != 1){
         return -1;
      }

      written += header_len;
      if(fwrite(p_item->key,p_item->key_len,1,fp) != 1){
         fseek(fp,-written,SEEK_CUR);
         return -1;
      }

      written += p_item->key_len;
      if(fwrite("\n",1,1,fp) != 1){
         fseek(fp,-written,SEEK_CUR);
         return -1;
      }
      written += 1;

      if(fwrite(p_item->value,p_item->value_len,1,fp) != 1){
         fseek(fp,-written,SEEK_CUR);
         return -1;
      }
      written += p_item->value_len;

      if(fwrite("\n",1,1,fp) != 1){
         fseek(fp,-written,SEEK_CUR);
         return -1;
      }
      written += 1;

      if(p_item->prop_len > 0 && p_item->prop != 0){
         if(fwrite(p_item->prop,p_item->prop_len,1,fp) != 1){
            fseek(fp,-written,SEEK_CUR);
            return -1;
         }
         written += p_item->prop_len;
         if(fwrite("\n",1,1,fp) != 1){
            fseek(fp,-written,SEEK_CUR);
            return -1;
         }

      }
      return 0;
   }

   bool data_dumpper::open_file()
   {
      if(fp != NULL && curr_time - start_time < SECONDS_PER_FILE){
         return true;
      }

      if(fp != NULL){ //change a new file
         fclose(fp);
         fp = NULL;
      }
      if(path == NULL){ //initialize
         path = TBSYS_CONFIG.getString(TAIRSERVER_SECTION,TAIR_EVICT_DATA_PATH);
         if(path == NULL){ //don't dump
            is_dump = false;
            return false;
         }
         assert(strlen(path) < 256); //we can control the length of path
         char dir_path[256];
         strncpy(dir_path,path,strlen(path));
         dir_path[strlen(path)] = '\0';
         //mkdirs will change the argument
         if(!tbsys::CFileUtil::mkdirs(dir_path) ){
            log_debug("mkdir %s failed.",path);
            is_dump = false;
            return false;
         }
      }
      struct tm tm;
      char file_name[256];
      ::localtime_r(&curr_time,&tm);
      snprintf(file_name,sizeof(file_name),"%s/evict_data.%02d",path,tm.tm_mday);
      log_debug("evict_data_file:%s",file_name);

      fp = fopen(file_name,"wb+");
      if(fp == NULL){
         log_debug("open file(%s) failed",file_name);
         return false;
      }
      start_time = curr_time;
      return true;
   }

} /* tair */
