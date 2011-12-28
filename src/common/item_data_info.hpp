/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this structure holds meta info of item
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_ITEM_DATA_INFO
#define TAIR_ITEM_DATA_INFO
#include "log.hpp"
namespace tair {
   typedef struct _item_meta {
      uint16_t magic;
      uint16_t checksum;
      uint16_t keysize; // key size max: 64KB
      uint16_t version;
      uint32_t padsize; // padding size: 64KB
      uint32_t valsize : 24; // value size
      uint8_t flag; // for extends
      uint32_t cdate; // item create time
      uint32_t mdate; // item last modified time
      uint32_t edate; // expire date

      void encode(tbnet::DataBuffer *output) const 
     {
         output->writeInt16(magic);
         output->writeInt16(checksum);
         output->writeInt16(keysize);
         output->writeInt16(version);
         output->writeInt32(padsize);
         output->writeInt32(valsize);
         output->writeInt8(flag);
         output->writeInt32(cdate);
         output->writeInt32(mdate);
         output->writeInt32(edate);
      }

      void decode(tbnet::DataBuffer *input)
      {
         magic = input->readInt16();
         checksum = input->readInt16();
         keysize = input->readInt16();
         version = input->readInt16();
         padsize = input->readInt32();
         valsize = input->readInt32() & 0xFFFFFF;
         flag = input->readInt8();
         cdate = input->readInt32();
         mdate = input->readInt32();
         edate = input->readInt32();
      }

      void log_self()
      {
         log_debug("meta info of data: keysize[%d], valsize[%d], padsize[%d], version[%d], flag[%d], cdate[%d], mdate[%d], edate[%d]", keysize, valsize, padsize, version, flag, cdate, mdate, edate);
      }
   } item_meta_info;

   static const size_t ITEM_HEADER_LEN = sizeof(item_meta_info);

   typedef struct _ItemData{
      item_meta_info header;
      char m_data[0];
   }item_data_info;
}
#endif
