/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * get server table for a particular group 
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_GET_SERVER_TABLE_PACKET_H
#define TAIR_PACKET_GET_SERVER_TABLE_PACKET_H
#include "base_packet.hpp"
#include "get_group_packet.hpp"
namespace tair {
   class request_get_server_table : public request_get_group {
   public:
      request_get_server_table() : request_get_group()
      {
         setPCode(TAIR_REQ_GET_SVRTAB_PACKET);
      }

      request_get_server_table(request_get_server_table &packet) : request_get_group(packet)
      {
         setPCode(TAIR_REQ_GET_SVRTAB_PACKET);
      }
   };
   class response_get_server_table : public base_packet {
   public:
      response_get_server_table()
      {
         data = NULL;
         size = 0;
         type = 0;
         modified_time = 0;
         group_name[0] = '\0';
         setPCode(TAIR_RESP_GET_SVRTAB_PACKET);
      }

      ~response_get_server_table()
      {
         if (data) ::free(data);
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(modified_time);
         output->writeInt32(type);
         output->writeString(group_name);
         if (size>0) {
            unsigned long source_len = size;
            unsigned long dest_len = source_len;
            unsigned char *dest = (unsigned char*) malloc(dest_len);
            int ret = compress(dest, &dest_len, (unsigned char*)data, source_len);
            if (ret != Z_OK) {
               log_warn( "compress error");
            }

            if (ret == Z_OK && dest_len>0) {
               output->writeInt32(dest_len);
               output->writeInt32(size);
               output->writeBytes(dest, dest_len);
            } else {
               output->writeInt32(size);
               output->writeInt32(0);
               output->writeBytes(data, size);
            }
            ::free(dest);
         } else {
            output->writeInt32(size);
            output->writeInt32(0);
         }
         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 20) {
            log_warn( "buffer data too few.");
            return false;
         }
         modified_time = input->readInt32();
         type = input->readInt32();
         char *tmp = &group_name[0];
         input->readString(tmp, 64);

         size = input->readInt32();
         int orig_size = input->readInt32();
         if (input->getDataLen() < size) {
            log_warn( "buffer data too few.");
            return false;
         }
         if (size > 0) {
            unsigned char *src = (unsigned char*) malloc(size);
            unsigned long srcLen = size;
            input->readBytes((char*)src, size);
            if (orig_size>0) {
               unsigned long destLen = orig_size;
               data = (char*)malloc(destLen);
               if (uncompress((Bytef*)data, &destLen, (const Bytef*)src, srcLen) != Z_OK) {
                  log_warn( "uncompress");
                  ::free(data);
                  data = NULL;
                  size = 0;
               } else {
                  size = destLen;
               }
               ::free(src);
            } else {
               data = (char*)src;
            }
         }
         return true;
      }

      void set_data(const char *data_value, int size_value)
      {
         if (data) {
            ::free(data);
            data = NULL;
         }
         size = size_value;
         if (size > 0) {
            data = (char*)malloc(size);
            if (data != NULL) memcpy(data, data_value, size);
         }
      }
      // set_group_name
      void set_group_name(const char *group_name_value)
      {
         group_name[0] = '\0';
         if (group_name_value != NULL) {
            strncpy(group_name, group_name_value, 64);
            group_name[63] = '\0';
         }
      }
   public:
      char group_name[64];
      int type;
      int modified_time;
      int size;
      char *data;
   private:
      response_get_server_table(const response_get_server_table&);
   };
}
#endif
