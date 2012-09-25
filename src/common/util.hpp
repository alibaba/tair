/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * some commonly used util class
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_MHASH_HPP
#define TAIR_MHASH_HPP
#include <sys/time.h>
#include <sys/types.h>
#include <dirent.h>
#include <ctype.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <algorithm>
#include "hash.hpp"
#include "define.hpp"
#include "log.hpp"
namespace tair
{
   namespace util
   {
      class hash_util
      {
      public:
         static unsigned int mhash1(char *key, int size)
          {
            return mur_mur_hash2(key, size, 134217689);
         }

         static unsigned int mhash2(char *key, int size)
          {
            return mur_mur_hash2(key, size, 97);
         }

      };

      class string_util
      {
      public:

         static char *conv_show_string(char *str, int size, char *ret = NULL, int msize = 0) 
          {
            int index = 0;
            if (ret == NULL) {
               msize = size*3+5;
               ret = (char*) malloc(msize);
            }
            unsigned char *p = (unsigned char *)str;
            while (size-->0 && index<msize-4) {
               index += sprintf(ret+index, "\\%02X", *p);
               p ++;
            }
            ret[index] = '\0';
            return ret;
         }

         static char *bin2ascii(char *str, int size, char *ret = NULL, int msize = 0) {
           if (ret == NULL) {
             msize = size * 3 + 5;
             ret = (char*) malloc(msize);
           }
           unsigned char *p = (unsigned char*)str;
           int i = 0;
           while (size-- > 0 && i < msize - 4) {
             if (*p >= '!' && *p <= '~') { //~ printable, excluding space
               i += sprintf(ret + i, "%c", *p++);
             } else {
               i += sprintf(ret + i, "\\%02X", *p++);
             }
           }
           ret[i] = '\0';
           return ret;
         }
         static void conv_raw_string(const char *str, char *result, int *size)
         {
            int index = 0;
            const unsigned char *p = (const unsigned char *)str;
            while(*p && index<(*size)-1) {
               if (*p == '\\') {
                  int c1 = *(p+1);
                  int c2 = *(p+2);
                  if (c1==0 || c2==0) break;
                  if (isupper(c1)) c1 = tolower(c1);
                  int value = (c1 >= '0' && c1 <= '9' ? c1 - '0' : c1 - 'a' + 10) * 16;
                  if (isupper(c2)) c2 = tolower(c2);
                  value += c2 >= '0' && c2 <= '9' ? c2 - '0' : c2 - 'a' + 10;
                  result[index++] = (char)(value&0xff);
                  p += 2;
               } else {
                  result[index++] = *p;
               }
               p ++;
            }
            *size = index;
         }

         static unsigned int mur_mur_hash(const void *key, int len)
         {
            return mur_mur_hash2(key, len, 97);
         }

        static void split_str(const char* str, const char* delim, std::vector<std::string>& values)
        {
          if (NULL == str)
          {
            return;
          }
          if (NULL == delim)
          {
            values.push_back(str);
          }
          else
          {
            const char* pos = str + strspn(str, delim);
            size_t len = 0;
            while ((len = strcspn(pos, delim)) > 0)
            {
              values.push_back(std::string(pos, len));
              pos += len;
              pos += strspn(pos, delim);
            }
          }
        }
      };

     class time_util {
     public:
       static int get_time_range(const char* str, int32_t& min, int32_t& max)
         {
           bool ret = str != NULL;

           if (ret)
           {
             int32_t tmp_min = 0, tmp_max = 0;
             char buf[32];
             char* max_p = strncpy(buf, str, sizeof(buf));
             char* min_p = strsep(&max_p, "-~");

             if (min_p != NULL && min_p[0] != '\0')
             {
               tmp_min = atoi(min_p);
             }
             if (max_p != NULL && max_p[0] != '\0')
             {
               tmp_max = atoi(max_p);
             }

             if ((ret = tmp_min >= 0 && tmp_max >= 0))
             {
               min = tmp_min;
               max = tmp_max;
             }
           }

           return ret;
         }

       static bool is_in_range(int32_t min, int32_t max)
         {
           time_t t = time(NULL);
           struct tm *tm = localtime((const time_t*) &t);
           bool reverse = false;
           if (min > max)
           {
             std::swap(min, max);
             reverse = true;
           }
           bool in_range = tm->tm_hour >= min && tm->tm_hour <= max;
           return reverse ? !in_range : in_range;
         }

     };

      class local_server_ip {
      public:
         static uint64_t ip;
      };

      class file_util {
      public:
        static int change_conf(const char *group_file_name, const char *section_name, const char *key, const char *value);
      };

     class coding_util {
     public:
       static void encode_fixed32(char* buf, uint32_t value)
       {
         buf[0] = value & 0xff;
         buf[1] = (value >> 8) & 0xff;
         buf[2] = (value >> 16) & 0xff;
         buf[3] = (value >> 24) & 0xff;
       }

       static uint32_t decode_fixed32(const char* buf)
       {
         return ((static_cast<uint32_t>(static_cast<unsigned char>(buf[0])))
                 | (static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 8)
                 | (static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 16)
                 | (static_cast<uint32_t>(static_cast<unsigned char>(buf[3])) << 24));
       }

       static void encode_fixed64(char* buf, uint64_t value)
       {
         buf[0] = value & 0xff;
         buf[1] = (value >> 8) & 0xff;
         buf[2] = (value >> 16) & 0xff;
         buf[3] = (value >> 24) & 0xff;
         buf[4] = (value >> 32) & 0xff;
         buf[5] = (value >> 40) & 0xff;
         buf[6] = (value >> 48) & 0xff;
         buf[7] = (value >> 56) & 0xff;
       }

       static uint64_t decode_fixed64(const char* buf)
       {
         return (static_cast<uint64_t>(decode_fixed32(buf + 4)) << 32) | decode_fixed32(buf);
       }
     };

   }

}
#endif
