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
#include "hash.hpp"
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

      };

      class local_server_ip {
      public:
         static uint64_t ip;
      };
   }

}
#endif
